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

//! Experimental copying and borrowed-input adapters to TiKV's existing kernels.
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
use tidb_query_expr::standalone::{
    Column as EngineColumn, ColumnRef as EngineColumnRef, PreparedExpression, ScalarRef,
};

use crate::expression::Expression;
use crate::pushdown_catalog::{self, ColumnDescriptor};
use crate::{Columns, EvalError};

/// Statement settings understood by the embedded TiKV expression engine.
pub use tidb_query_expr::standalone::Context;

/// Explicit adapter choice for the experimental TiKV expression backend.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Backend {
    /// Original owned-column adapter, retained as the regression/benchmark control.
    #[default]
    Copying,
    /// Borrow input payloads and write engine results directly to an output column.
    Borrowed,
}

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

    /// Borrow input payloads and append to one initially empty, correctly typed
    /// output column. Unsupported loaders or aliased output use the copying
    /// adapter before any input guards are held. Errors reset partial borrowed
    /// output; they never cause replay. Only successful borrowed calls increment
    /// the borrowed-row counter. No input reference survives this method.
    pub fn evaluate_into<C: Columns>(
        &mut self,
        context: &C,
        input: &Chunk,
        output: &mut Chunk,
        output_index: usize,
    ) -> Result<(), EvalError> {
        if output_index >= output.num_cols()
            || self.inputs.iter().any(|(i, _)| *i >= input.num_cols())
        {
            return Err(invalid("TiKV expression column index is out of bounds"));
        }
        let output_shared = {
            let destination = output.column(output_index);
            if destination.rows() != 0 {
                return Err(invalid(
                    "borrowed expression output must be initially empty",
                ));
            }
            let layout_matches = match self.result_type.eval_type() {
                EvalType::Int | EvalType::Real => {
                    destination.is_fixed() && destination.type_size() == 8
                }
                EvalType::String => !destination.is_fixed(),
                _ => false,
            };
            if !layout_matches {
                return Err(invalid(
                    "borrowed expression output layout does not match its type",
                ));
            }
            destination.has_shared_mutable_storage()
        };
        let aliases_input = self
            .inputs
            .iter()
            .any(|(index, _)| input.columns_share_identity(*index, output, output_index));
        // Shared backing writes can detach, but that hides a payload copy. More
        // importantly, an identical Column owner cannot be write-locked while
        // its input read guard is alive. Decide fallback before taking guards.
        if !self.prepared.supports_borrowed() || aliases_input || output_shared {
            for value in self.evaluate(context, input)? {
                output.append_datum(output_index, &value);
            }
            return Ok(());
        }
        let row_count = input.physical_rows();
        let logical_rows = input.num_rows();
        // Distinct indexes can alias one Column owner. Lock that owner only
        // once; repeated read-lock acquisition can block behind a writer.
        let mut unique_indexes = Vec::new();
        let mut slots = Vec::with_capacity(self.inputs.len());
        for (index, _) in &self.inputs {
            let slot = unique_indexes
                .iter()
                .position(|other| input.columns_share_identity(*index, input, *other))
                .unwrap_or_else(|| {
                    unique_indexes.push(*index);
                    unique_indexes.len() - 1
                });
            slots.push(slot);
        }
        let columns: Vec<_> = unique_indexes
            .iter()
            .map(|index| input.column(*index))
            .collect();
        let views: Vec<_> = columns.iter().map(|column| column.read_view()).collect();
        let borrowed: Vec<_> = self
            .inputs
            .iter()
            .zip(slots)
            .map(|((_, ty), slot)| {
                let view = &views[slot];
                if view.rows() != row_count {
                    return Err(invalid("borrowed expression input row counts differ"));
                }
                let values = view.data();
                let validity = view.null_bitmap();
                match ty.eval_type() {
                    EvalType::Int if view.fixed_len() == Some(8) => {
                        Ok(EngineColumnRef::Int { values, validity })
                    }
                    EvalType::Real if view.fixed_len() == Some(8) => {
                        Ok(EngineColumnRef::Real { values, validity })
                    }
                    EvalType::String if view.fixed_len().is_none() => Ok(EngineColumnRef::Bytes {
                        values,
                        validity,
                        offsets: view.offsets(),
                    }),
                    _ => Err(invalid(
                        "borrowed expression input layout does not match its type",
                    )),
                }
            })
            .collect::<Result<_, _>>()?;
        let mut destination = output.column_mut(output_index);
        let expected = self.result_type.eval_type();
        let result = self
            .prepared
            .eval_borrowed(&borrowed, row_count, input.sel(), |value| {
                match value {
                    ScalarRef::Null => destination.append_null(),
                    ScalarRef::Int(value) if expected == EvalType::Int => {
                        destination.append_int64(value)
                    }
                    ScalarRef::Real(value) if expected == EvalType::Real => {
                        destination.append_float64(value)
                    }
                    ScalarRef::Bytes(value) if expected == EvalType::String => {
                        destination.append_bytes(value)
                    }
                    _ => {
                        return Err(tidb_query_expr::standalone::Error {
                            code: 1105,
                            message: "borrowed expression result type does not match output"
                                .to_owned(),
                        })
                    }
                }
                Ok(())
            });
        let diagnostics = match result {
            Ok(diagnostics) => diagnostics,
            Err(error) => {
                destination.reset();
                return Err(engine_error(error));
            }
        };
        drop(destination);
        // Release payload/owner guards before invoking arbitrary context hooks.
        drop(borrowed);
        drop(views);
        drop(columns);
        for warning in diagnostics.warnings {
            context.append_warning(mysql_code(warning.code), &warning.message);
        }
        context.record_tikv_expression_rows(logical_rows);
        context.record_tikv_borrowed_expression_rows(logical_rows);
        Ok(())
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

    pub(crate) fn evaluate_into<C: Columns>(
        &mut self,
        index: usize,
        context: &C,
        input: &Chunk,
        output: &mut Chunk,
        output_index: usize,
    ) -> Result<bool, EvalError> {
        let Some(program) = self.programs[index].as_mut() else {
            return Ok(false);
        };
        // Both adapters leave nonfinite values native BEFORE executing kernels.
        if program.has_nonfinite_input(input) {
            return Ok(false);
        }
        match context.tikv_expression_backend() {
            Backend::Borrowed if output.column(output_index).rows() == 0 => {
                program.evaluate_into(context, input, output, output_index)?;
            }
            Backend::Copying | Backend::Borrowed => {
                // The existing suite can append calculated expressions after a
                // prefix. Preserve it, including on errors, via owned staging;
                // the public direct borrowed API intentionally requires empty output.
                for value in &program.evaluate(context, input)? {
                    output.append_datum(output_index, value);
                }
            }
        }
        Ok(true)
    }
}
