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
//! Local admission is independent of distributed pushdown authorization. The
//! adapter reuses typed kernels across numeric, string, temporal, JSON and vector
//! families, but declines unrepresented session settings/effects and speculative
//! lazy branches. Owned input transport covers more types than borrowed loaders.
//! A runtime error is never retried natively.

use std::collections::BTreeMap;
use std::sync::Arc;

use prost::Message;
use tidb_chunk::chunk::Chunk;
use tidb_datatype::{Datum, EvalType, FieldType, FieldTypeCode};
use tidb_query_expr::standalone::{ColumnRef as EngineColumnRef, PreparedExpression, ScalarRef};

mod admission;
mod bridge;
mod lowering;

use bridge::copy_column;
use lowering::{admitted, lower};

use crate::expression::Expression;
use crate::pushdown_catalog::ColumnDescriptor;
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
/// The compiled program is immutable and `Send + Sync`, so it is shared through
/// [`ProjectionCache`] rather than owned per execution.
pub struct TikvExpression {
    prepared: PreparedExpression,
    inputs: Vec<(usize, FieldType)>,
    result_type: FieldType,
    wire_signatures: Vec<i32>,
}

impl TikvExpression {
    /// Compile an expression once. `None` means admission/lowering declined it,
    /// before evaluating any row or changing the caller's diagnostics.
    pub fn compile(expression: &Expression, context: Context) -> Result<Option<Self>, EvalError> {
        Ok(Self::compile_detailed(expression, context)?.ok())
    }

    /// Compile, reporting why the engine will not run this expression.
    ///
    /// `Ok(Ok(program))` means the engine owns it. `Ok(Err(reason))` is the
    /// adapter's decision, before any row is read, and the reason is what the
    /// fallback gate records.
    pub fn compile_detailed(
        expression: &Expression,
        context: Context,
    ) -> Result<Result<Self, FallbackReason>, EvalError> {
        if !admitted(expression) {
            return Ok(Err(FallbackReason::NotAdmitted));
        }
        let Some(result_type) = expression.static_type().cloned() else {
            return Ok(Err(FallbackReason::NotAdmitted));
        };
        let mut remapped = expression.clone();
        let mut inputs = Vec::new();
        let mut positions = BTreeMap::new();
        if !remap_columns(&mut remapped, &mut inputs, &mut positions) {
            return Ok(Err(FallbackReason::NotAdmitted));
        }
        let descriptor = |offset: u32| {
            let (_, field_type) = inputs.get(offset as usize)?;
            let wire_type = lowering::field_type_to_pb(field_type)?;
            Some(ColumnDescriptor {
                tp: i32::from(field_type.code().mysql_type()),
                flag: field_type.flags(),
                flen: wire_type.flen?,
                decimal: wire_type.decimal?,
                charset: field_type.charset_name().to_owned(),
                collation: field_type.collation_name().to_owned(),
                elems: field_type
                    .elems_snapshot()
                    .into_iter()
                    .map(|elem| elem.to_string())
                    .collect(),
                array: field_type.is_array(),
            })
        };
        let Some(encoded) = lower(&remapped, &descriptor) else {
            return Ok(Err(FallbackReason::NotAdmitted));
        };
        let Some(schema): Option<Vec<_>> = inputs
            .iter()
            .map(|(_, ty)| lowering::field_type_to_pb(ty).map(|pb| pb.encode_to_vec()))
            .collect()
        else {
            return Ok(Err(FallbackReason::NotAdmitted));
        };
        fn collect_signatures(expression: &tidb_proto::tipb::Expr, out: &mut Vec<i32>) {
            if expression.tp == Some(tidb_proto::tipb::ExprType::ScalarFunc as i32) {
                if let Some(signature) = expression.sig {
                    out.push(signature);
                }
            }
            for child in &expression.children {
                collect_signatures(child, out);
            }
        }
        let mut wire_signatures = Vec::new();
        collect_signatures(&encoded, &mut wire_signatures);
        wire_signatures.sort_unstable();
        wire_signatures.dedup();
        // Lowering and the engine have independent capability sets. A compile
        // refusal is safe to keep native; unlike evaluation, compilation here
        // has no caller-visible warnings, session state, or input mutation.
        let prepared = match PreparedExpression::compile(&encoded.encode_to_vec(), &schema, context)
        {
            Ok(prepared) => prepared,
            Err(error) => {
                // Set TIKV_EXPR_DEBUG_COMPILE to see which wire program the
                // engine refuses; the refusal itself is a silent native
                // fallback by design.
                if std::env::var_os("TIKV_EXPR_DEBUG_COMPILE").is_some() {
                    eprintln!("ENGINE-COMPILE-REJECT {expression:?} -> {error:?}");
                }
                return Ok(Err(FallbackReason::NotAdmitted));
            }
        };
        // A successful compile says nothing about laziness: an eager kernel and
        // a lazy kernel for the same signature compile identically, and running
        // the eager one would enter a branch MySQL never enters. Only a *mixed*
        // program is at risk, though: a program with no lazy node has nothing
        // that could skip a subtree, and an eager lazy-sensitive kernel there is
        // covered by its own leaf-only shape rule. So refuse exactly the mix of
        // a lazy node and an eager lazy-sensitive node. The admission table's
        // per-node shape rules remain the first gate; this is the
        // engine-enforced one.
        if prepared.has_lazy_nodes() && !prepared.eager_lazy_risk().is_empty() {
            return Ok(Err(FallbackReason::LazyRisk));
        }
        Ok(Ok(Self {
            prepared,
            inputs,
            result_type,
            wire_signatures,
        }))
    }

    /// Distinct protobuf function IDs submitted to the engine's builder.
    ///
    /// This is observability for coverage tests, not proof that every signature
    /// shape works or that every submitted node runs once per row (metadata
    /// constructors may precompute constants). Aliases can share an ID.
    #[must_use]
    pub fn wire_signatures(&self) -> &[i32] {
        &self.wire_signatures
    }

    fn requires_native_input(&self, input: &Chunk) -> bool {
        self.inputs
            .iter()
            .any(|(index, ty)| bridge::requires_native(input, *index, ty))
    }

    fn borrowed_layout(ty: &FieldType) -> bool {
        // A SET column's SQL eval family is String but its chunk cell is
        // `[bitmask][name]`, so the borrowed Bytes loader would feed those bytes
        // to a string kernel. ENUM and BIT have the same shape of problem.
        !matches!(
            ty.code(),
            FieldTypeCode::Float | FieldTypeCode::Bit | FieldTypeCode::Enum | FieldTypeCode::Set
        ) && matches!(
            ty.eval_type(),
            EvalType::Int | EvalType::Real | EvalType::String
        )
    }

    /// Evaluate selected logical rows, including both directions of conversion.
    /// Output is dense in logical-row order; the input is not mutated.
    ///
    /// Takes `&self`: the compiled program is immutable and shareable, and the
    /// engine allocates its per-call execution state internally.
    pub fn evaluate<C: Columns>(
        &self,
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
            .eval_shared(&columns, input.num_rows(), None)
            .map_err(engine_error)?;
        for warning in output.warnings {
            context.append_warning(mysql_code(warning.code), &warning.message);
        }
        let result = bridge::into_datums(output.column, &self.result_type)?;
        context.record_tikv_expression_rows(input.num_rows());
        Ok(result)
    }

    /// Borrow input payloads and append to one initially empty, correctly typed
    /// output column. Unsupported loaders or aliased output use the copying
    /// adapter before any input guards are held. Errors reset partial borrowed
    /// output; they never cause replay. Only successful borrowed calls increment
    /// the borrowed-row counter. No input reference survives this method.
    pub fn evaluate_into<C: Columns>(
        &self,
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
            let layout_matches =
                destination.type_size() == tidb_chunk::column::get_fixed_len(&self.result_type);
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
        if !self.prepared.supports_borrowed()
            || !Self::borrowed_layout(&self.result_type)
            || self.inputs.iter().any(|(_, ty)| !Self::borrowed_layout(ty))
            || aliases_input
            || output_shared
        {
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
        let result =
            self.prepared
                .eval_borrowed_shared(&borrowed, row_count, input.sel(), |value| {
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

/// Why an expression with an engine context still ran natively.
///
/// These are stable identifiers used by the removal gate, not diagnostics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FallbackReason {
    /// The admission table excludes this name, or this particular shape did
    /// not lower. Either way the decision was made before any kernel ran.
    NotAdmitted,
    /// The program compiled, but it mixes a lazy node with an eager
    /// lazy-sensitive node, so a branch MySQL never enters could run. Clearing
    /// this reason means making the remaining family lazy, not relaxing a gate.
    LazyRisk,
    /// The expression lowered, but this batch holds a payload the exact bridge
    /// cannot represent (non-finite real/vector, temporal JSON, ...).
    UnrepresentableInput,
}

impl FallbackReason {
    /// The stable identifier a gate compares against its exclusion list.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotAdmitted => "not-admitted",
            Self::LazyRisk => "lazy-risk",
            Self::UnrepresentableInput => "unrepresentable-input",
        }
    }
}

/// Execution-local compiled programs, cached on the shared program so every
/// projection worker of one plan reuses a single compilation instead of
/// compiling its own copy. Recompile when statement policy changes.
#[derive(Default)]
pub(crate) struct ProjectionCache {
    context: Option<Context>,
    programs: Arc<Vec<Result<Arc<TikvExpression>, FallbackReason>>>,
    /// How many times this cache has actually compiled. Stays at one for a
    /// fixed statement policy no matter how many suites or workers share it.
    compilations: u64,
}

impl ProjectionCache {
    /// Returns the compiled programs for `context`, compiling them once if the
    /// statement policy changed. The returned handle is shared and immutable,
    /// so callers evaluate outside this cache's lock.
    pub(crate) fn prepare(
        &mut self,
        expressions: &[Expression],
        context: &Context,
    ) -> Result<Arc<Vec<Result<Arc<TikvExpression>, FallbackReason>>>, EvalError> {
        if self.context.as_ref() != Some(context) {
            let programs = expressions
                .iter()
                .map(|expression| {
                    TikvExpression::compile_detailed(expression, context.clone())
                        .map(|outcome| outcome.map(Arc::new))
                })
                .collect::<Result<_, _>>()?;
            self.programs = Arc::new(programs);
            self.context = Some(context.clone());
            self.compilations += 1;
        }
        Ok(Arc::clone(&self.programs))
    }

    pub(crate) fn compilations(&self) -> u64 {
        self.compilations
    }
}

/// Run one already-compiled expression against a batch.
///
/// `Ok(None)` means the engine produced this expression's column.
/// `Ok(Some(reason))` means the caller must use the native evaluator, and
/// reports that decision so a gate can reject unlisted reasons.
pub(crate) fn evaluate_shared<C: Columns>(
    program: &TikvExpression,
    context: &C,
    input: &Chunk,
    output: &mut Chunk,
    output_index: usize,
) -> Result<Option<FallbackReason>, EvalError> {
    // Both adapters decline unrepresentable payloads BEFORE executing kernels
    // (nonfinite reals/vectors, temporal JSON and other exact-bridge limits).
    if program.requires_native_input(input) {
        return Ok(Some(FallbackReason::UnrepresentableInput));
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
    Ok(None)
}
