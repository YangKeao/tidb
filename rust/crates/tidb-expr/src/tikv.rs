// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Experimental, copying adapter to TiKV's existing in-process RPN evaluator.
//!
//! Admission is intentionally narrower than coprocessor pushdown: only static,
//! side-effect-free, homogeneous numeric arithmetic/comparisons and byte length
//! are admitted. In particular, no lazy control flow, parameters, correlated
//! values, string comparisons, temporal functions, or implicit cross-domain
//! casts enter this path. A runtime error is never retried natively.

use std::collections::BTreeMap;

use prost::Message;
use tidb_chunk::chunk::Chunk;
use tidb_datatype::{Datum, EvalType, FieldType, FieldTypeCode};
use tidb_proto::tipb::{Expr as PbExpr, ExprType, ScalarFuncSig};
use tidb_query_expr::standalone::{Column as EngineColumn, PreparedExpression};

use crate::expression::Expression;
use crate::pushdown_catalog::{self, ColumnDescriptor};
use crate::{Columns, EvalError};

/// Statement settings understood by the embedded TiKV expression engine.
pub use tidb_query_expr::standalone::Context;

/// One admitted expression, with a compact, copying input-column map.
///
/// Keep this state in an execution instance, not the shared optimizer plan:
/// TiKV RPN metadata is Send but is not required to be Sync.
pub struct TikvExpression {
    prepared: PreparedExpression,
    inputs: Vec<(usize, FieldType)>,
    result_type: FieldType,
}

impl TikvExpression {
    /// Compile an expression once. `None` means admission/lowering declined it,
    /// before evaluating any row or changing the caller's diagnostics.
    pub fn compile(expression: &Expression, context: Context) -> Result<Option<Self>, EvalError> {
        if !admitted(expression) {
            return Ok(None);
        }
        let Some(result_type) = expression.static_type().cloned() else {
            return Ok(None);
        };
        let mut remapped = expression.clone();
        let mut inputs = Vec::new();
        let mut positions = BTreeMap::new();
        if !remap_columns(&mut remapped, &mut inputs, &mut positions) {
            return Ok(None);
        }
        let descriptor = |offset: u32| {
            let (_, field_type) = inputs.get(offset as usize)?;
            Some(ColumnDescriptor {
                tp: i32::from(field_type.code().mysql_type()),
                flag: field_type.flags(),
                flen: i32::try_from(field_type.flen()).ok()?,
                decimal: i32::try_from(field_type.decimal()).ok()?,
                charset: field_type.charset_name().to_owned(),
                collation: field_type.collation_name().to_owned(),
                elems: Vec::new(),
                array: false,
            })
        };
        let Some(encoded) = lower(&remapped, &descriptor) else {
            return Ok(None);
        };
        let Some(schema): Option<Vec<_>> = inputs
            .iter()
            .map(|(_, ty)| pushdown_catalog::field_type_to_pb(ty).map(|pb| pb.encode_to_vec()))
            .collect()
        else {
            return Ok(None);
        };
        // Lowering and the engine have independent capability sets. A compile
        // refusal is safe to keep native; unlike evaluation, compilation here
        // has no caller-visible warnings, session state, or input mutation.
        let Ok(prepared) = PreparedExpression::compile(&encoded.encode_to_vec(), &schema, context)
        else {
            return Ok(None);
        };
        Ok(Some(Self {
            prepared,
            inputs,
            result_type,
        }))
    }

    fn has_nonfinite_input(&self, input: &Chunk) -> bool {
        self.inputs.iter().any(|(index, ty)| {
            if ty.eval_type() != EvalType::Real || *index >= input.num_cols() {
                return false;
            }
            let column = input.column(*index);
            (0..input.num_rows()).any(|row| {
                let row = input.sel().map_or(row, |selection| selection[row]);
                row < column.rows() && !column.is_null(row) && !column.get_float64(row).is_finite()
            })
        })
    }

    /// Evaluate selected logical rows, including both directions of conversion.
    /// Output is dense in logical-row order; the input is not mutated.
    pub fn evaluate<C: Columns>(
        &mut self,
        context: &C,
        input: &Chunk,
    ) -> Result<Vec<Datum>, EvalError> {
        let mut columns = Vec::with_capacity(self.inputs.len());
        for (index, ty) in &self.inputs {
            if *index >= input.num_cols() {
                return Err(invalid("TiKV expression input column is out of bounds"));
            }
            columns.push(copy_column(input, *index, ty)?);
        }
        // copy_column already gathers Chunk.sel in order, including duplicates.
        let output = self
            .prepared
            .eval(&columns, input.num_rows(), None)
            .map_err(engine_error)?;
        for warning in output.warnings {
            context.append_warning(mysql_code(warning.code), &warning.message);
        }
        let result = match output.column {
            EngineColumn::Int(values) => values
                .into_iter()
                .map(|value| match value {
                    Some(value) if self.result_type.is_unsigned() => Datum::UInt(value as u64),
                    Some(value) => Datum::Int(value),
                    None => Datum::Null,
                })
                .collect(),
            EngineColumn::Real(values) => values
                .into_iter()
                .map(|value| value.map_or(Datum::Null, Datum::Real))
                .collect(),
            EngineColumn::Decimal(_) => {
                return Err(invalid("unadmitted decimal returned by TiKV expression"));
            }
            EngineColumn::Bytes(values) => values
                .into_iter()
                .map(|value| match value {
                    None => Datum::Null,
                    Some(value) => {
                        let mut datum = Datum::Null;
                        datum.set_string(value, self.result_type.collation());
                        datum
                    }
                })
                .collect(),
        };
        context.record_tikv_expression_rows(input.num_rows());
        Ok(result)
    }
}

fn mysql_code(code: i32) -> u16 {
    u16::try_from(code).unwrap_or(1105)
}

fn engine_error(error: tidb_query_expr::standalone::Error) -> EvalError {
    EvalError::ExternalEngine {
        code: mysql_code(error.code),
        message: error.message,
    }
}

fn invalid(message: &str) -> EvalError {
    EvalError::ExternalEngine {
        code: 1105,
        message: message.to_owned(),
    }
}

fn admitted_type(ty: &FieldType) -> bool {
    if ty.is_unsigned() || ty.is_array() {
        return false;
    }
    matches!(
        ty.code(),
        FieldTypeCode::Tiny
            | FieldTypeCode::Short
            | FieldTypeCode::Int24
            | FieldTypeCode::Long
            | FieldTypeCode::LongLong
            | FieldTypeCode::Double
            // Decimal is supported by the embedding API, but not automatically
            // admitted here: a text bridge cannot preserve hidden fractional
            // digits independently of resultFrac for arbitrary intermediates.
            | FieldTypeCode::Varchar
            | FieldTypeCode::VarString
            | FieldTypeCode::String
            | FieldTypeCode::Blob
            | FieldTypeCode::TinyBlob
            | FieldTypeCode::MediumBlob
            | FieldTypeCode::LongBlob
    )
}

fn admitted(expression: &Expression) -> bool {
    let Some(ty) = expression.static_type() else {
        return false;
    };
    if !admitted_type(ty) {
        return false;
    }
    match expression {
        Expression::Column(_) => true,
        Expression::Constant(value) => {
            value.deferred_expr.is_none()
                && value.param_marker.is_none()
                && matches!(
                    value.value,
                    Datum::Null
                        | Datum::Int(_)
                        | Datum::Real(_)
                        | Datum::String(_)
                        | Datum::Bytes(_)
                )
                && !matches!(value.value, Datum::Real(number) if !number.is_finite())
        }
        Expression::CorrelatedColumn(_) => false,
        Expression::ScalarFunction(function) => {
            if !function.args.iter().all(admitted) {
                return false;
            }
            let argument_type = function.args.first().and_then(Expression::static_type);
            let Some(argument_type) = argument_type else {
                return false;
            };
            match function.func_name.lowercase() {
                "length" | "octet_length" => {
                    function.args.len() == 1 && argument_type.eval_type() == EvalType::String
                }
                "abs" => {
                    function.args.len() == 1
                        && matches!(argument_type.eval_type(), EvalType::Int | EvalType::Real)
                        && ty.eval_type() == argument_type.eval_type()
                }
                "plus" | "minus" | "mul" | "eq" | "ne" | "lt" | "le" | "gt" | "ge" | "nulleq" => {
                    function.args.len() == 2
                        && matches!(argument_type.eval_type(), EvalType::Int | EvalType::Real)
                        && function.args[1]
                            .static_type()
                            .is_some_and(|right| right.eval_type() == argument_type.eval_type())
                }
                _ => false,
            }
        }
    }
}

// A local signature adapter, not an expansion of distributed pushdown policy.
// Go's typed arithmetic/comparison signatures distinguish the argument eval
// type; admission above excludes mixed domains and unsigned operands. Preserve
// every node's inferred FieldType, not just the root type.
fn lower(
    expression: &Expression,
    columns: &impl Fn(u32) -> Option<ColumnDescriptor>,
) -> Option<PbExpr> {
    let Expression::ScalarFunction(function) = expression else {
        return pushdown_catalog::expression_to_pb(expression, columns);
    };
    use EvalType::{Int as I, Real as R, String as S};
    use ScalarFuncSig::*;
    let input_type = function.args.first()?.static_type()?.eval_type();
    let sig = match (function.func_name.lowercase(), input_type) {
        ("plus", I) => PlusInt,
        ("minus", I) => MinusInt,
        ("mul", I) => MultiplyInt,
        ("plus", R) => PlusReal,
        ("minus", R) => MinusReal,
        ("mul", R) => MultiplyReal,
        ("eq", I) => EqInt,
        ("ne", I) => NeInt,
        ("lt", I) => LtInt,
        ("le", I) => LeInt,
        ("gt", I) => GtInt,
        ("ge", I) => GeInt,
        ("nulleq", I) => NullEqInt,
        ("eq", R) => EqReal,
        ("ne", R) => NeReal,
        ("lt", R) => LtReal,
        ("le", R) => LeReal,
        ("gt", R) => GtReal,
        ("ge", R) => GeReal,
        ("nulleq", R) => NullEqReal,
        ("abs", I) => AbsInt,
        ("abs", R) => AbsReal,
        ("length" | "octet_length", S) => Length,
        _ => return None,
    };
    Some(PbExpr {
        tp: Some(ExprType::ScalarFunc as i32),
        sig: Some(sig as i32),
        field_type: Some(pushdown_catalog::field_type_to_pb(
            expression.static_type()?,
        )?),
        children: function
            .args
            .iter()
            .map(|argument| lower(argument, columns))
            .collect::<Option<_>>()?,
        ..PbExpr::default()
    })
}

fn remap_columns(
    expression: &mut Expression,
    inputs: &mut Vec<(usize, FieldType)>,
    positions: &mut BTreeMap<usize, usize>,
) -> bool {
    match expression {
        Expression::Column(column) => {
            let Ok(original) = usize::try_from(column.index) else {
                return false;
            };
            let Some(ty) = column.get_static_type().cloned() else {
                return false;
            };
            let compact = if let Some(&compact) = positions.get(&original) {
                if inputs[compact].1 != ty {
                    return false;
                }
                compact
            } else {
                let compact = inputs.len();
                positions.insert(original, compact);
                inputs.push((original, ty));
                compact
            };
            column.index = compact as i64;
            true
        }
        Expression::ScalarFunction(function) => function
            .args
            .iter_mut()
            .all(|argument| remap_columns(argument, inputs, positions)),
        Expression::Constant(_) => true,
        Expression::CorrelatedColumn(_) => false,
    }
}

fn copy_column(input: &Chunk, index: usize, ty: &FieldType) -> Result<EngineColumn, EvalError> {
    let column = input.column(index);
    let physical = |row| input.sel().map_or(row, |selection| selection[row]);
    let rows = input.num_rows();
    if (0..rows).any(|row| physical(row) >= column.rows()) {
        return Err(invalid("TiKV expression selection is out of bounds"));
    }
    Ok(match ty.eval_type() {
        EvalType::Int => EngineColumn::Int(
            (0..rows)
                .map(|row| {
                    let row = physical(row);
                    (!column.is_null(row)).then(|| column.get_int64(row))
                })
                .collect(),
        ),
        EvalType::Real => EngineColumn::Real(
            (0..rows)
                .map(|row| {
                    let row = physical(row);
                    (!column.is_null(row)).then(|| column.get_float64(row))
                })
                .collect(),
        ),
        EvalType::String => EngineColumn::Bytes(
            (0..rows)
                .map(|row| {
                    let row = physical(row);
                    (!column.is_null(row)).then(|| column.get_bytes(row).to_vec())
                })
                .collect(),
        ),
        _ => return Err(invalid("unadmitted TiKV expression input type")),
    })
}

/// Execution-local compiled programs. Recompile when statement policy changes.
#[derive(Default)]
pub(crate) struct ProjectionCache {
    context: Option<Context>,
    programs: Vec<Option<TikvExpression>>,
}

impl ProjectionCache {
    pub(crate) fn prepare(
        &mut self,
        expressions: &[Expression],
        context: &Context,
    ) -> Result<(), EvalError> {
        if self.context.as_ref() == Some(context) {
            return Ok(());
        }
        let programs = expressions
            .iter()
            .map(|expression| TikvExpression::compile(expression, context.clone()))
            .collect::<Result<_, _>>()?;
        self.programs = programs;
        self.context = Some(context.clone());
        Ok(())
    }

    pub(crate) fn evaluate<C: Columns>(
        &mut self,
        index: usize,
        context: &C,
        input: &Chunk,
    ) -> Result<Option<Vec<Datum>>, EvalError> {
        let Some(program) = self.programs[index].as_mut() else {
            return Ok(None);
        };
        // The TiKV Real carrier rejects NaN. Leave nonfinite values native
        // before evaluation rather than retrying an engine error afterwards.
        if program.has_nonfinite_input(input) {
            return Ok(None);
        }
        program.evaluate(context, input).map(Some)
    }
}
