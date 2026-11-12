// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Shared pure projection lowering and execution, including multiple outputs,
//! literal/column roots and selected direct-column transport. Every calculated
//! output uses the shared RPN program; no value callback enters a native tree.
//! The legacy single-invocation consumer is retained only for adversarial tests.
//!
//! Native source metadata is deep-detached when EvaluatorProgram takes
//! ownership, then validated and sealed once during preparation.
//! Input values/column owners must also be alias-stable throughout the borrowed
//! invocation: native integer leaf phases and singleton reads need not hold the
//! same lock interval. These are owner preconditions, NOT atomic snapshots.
//! Logical source byte policies and actual retained value capacities are separate
//! from allocator peaks, immutable program heap and externally owned Chunk buffers.

use std::mem::size_of;
use std::sync::Arc;

use protobuf::ProtobufEnum;
use tidb_chunk::column::get_fixed_len;
use tidb_datatype::tikv_compat::value::{
    project_field_type, snapshot_field_type, to_scalar, BridgeError,
};
use tidb_datatype::{
    BinaryJSON, Datum, EvalType as NativeEvalType, FieldType, FieldTypeCode, FieldTypeFlags,
    MySqlDuration, Time, VectorFloat32,
};
use tidb_query_datatype::codec::data_type::{
    ChunkRef, ChunkedVec, ChunkedVecBytes, ChunkedVecJson, ChunkedVecSized,
    ChunkedVecVectorFloat32, DateTime as TikvDateTime, Decimal as TikvDecimal,
    Duration as TikvDuration, Int, Json as TikvJson, JsonType, Real, ScalarValue, VectorValue,
};
use tidb_query_datatype::codec::mysql::{
    deserialize_native_vector_float32, VectorFloat32 as TikvVectorFloat32,
};
use tidb_query_datatype::expr::EvalContext;
use tidb_query_datatype::EvalType as TikvEvalType;
use tidb_query_expr::local::{
    compile_batch, BatchFacts, CallMetadata, CompileLimits, ExecutionLimits, FunctionRef,
    HostCatalog, HostSignature, InputRow, LiteralKind, LocalBatchProgram, LocalCompileContext,
    LocalError, LocalExpr, LocalResult, LocalRuntimeServices, OrdinaryCallSite, OrdinarySourceId,
    SqlBatchHostOperation,
};

#[path = "batch_decimal_fast_source.rs"]
mod decimal_fast_source;
#[path = "batch_hosts.rs"]
mod hosts;
#[path = "batch_numeric_source.rs"]
mod numeric_source;
use numeric_source::PendingNumeric;
#[path = "batch_node_ref.rs"]
mod node_ref;
use node_ref::{BorrowedNodeKind, SourceNodeRef, SourceReference};
#[path = "batch_values.rs"]
mod native_values;
use hosts::{
    CompiledDiagnosticParent, CompiledNumericParent, NativeBatchHosts, NativeCachedList,
    NativeFinalState, NativeHostMetadata, NativeJsonSchema, NativeRawValueOperation,
    NativeStateful, NativeTypedContext, NativeTypedDescriptor, NativeValuePreparation, NumericPlan,
    NumericPlanBody, WindowArithmeticMetadata, WindowArithmeticSource,
};
use native_values::{native_payload_bytes, NativeValueOrigin, NativeValueRole, NativeValueStore};
use std::cell::RefCell;
use tidb_query_expr::local::LocalHostServices;

use crate::expr_collation::CollationInfo;
use crate::pushdown_catalog::CATALOG;
use crate::scalar_function::{coerce_function_result, ScalarFunction};

use super::{
    BatchRefusal, BatchScratchSnapshot, Chunk, Columns, EvalError, EvaluatorError,
    EvaluatorProgram, EvaluatorSuite, Expression,
};

#[cfg(test)]
use super::{BatchConsumer, BatchInvocation};

#[derive(Clone, Copy, Debug)]
pub(crate) struct BatchSourceLimits {
    pub(crate) tree: CompileLimits,
    // Logical snapshot payload/copy-group and flat-scaffolding policy. Int/NULL
    // literal payload is fixed-size and included in the bounded node records.
    pub(crate) max_metadata_bytes: usize,
}

#[derive(Debug)]
enum FailureKind {
    Admission(&'static str),
    Bridge(BridgeError),
    Local(LocalError),
    Native(EvaluatorError),
}

/// Opaque failure ownership across admission, bridge, local, and native paths.
#[derive(Debug)]
pub(crate) struct BatchFailure {
    kind: FailureKind,
}
impl BatchFailure {
    fn admission(message: &'static str) -> Self {
        Self {
            kind: FailureKind::Admission(message),
        }
    }
    pub(crate) fn local_error(&self) -> Option<&LocalError> {
        match &self.kind {
            FailureKind::Local(error) => Some(error),
            _ => None,
        }
    }

    pub(super) fn refusal(&self) -> BatchRefusal {
        match &self.kind {
            FailureKind::Admission(_) => BatchRefusal::Admission,
            FailureKind::Bridge(_) => BatchRefusal::Bridge,
            FailureKind::Local(LocalError::ResourceLimit(_)) => BatchRefusal::Resource,
            FailureKind::Local(
                LocalError::InvalidSpec(_)
                | LocalError::InvalidBatch(_)
                | LocalError::BindingContract(_)
                | LocalError::HostContract(_),
            ) => BatchRefusal::Contract,
            FailureKind::Local(LocalError::Evaluation(_)) => BatchRefusal::Evaluation,
            FailureKind::Native(EvaluatorError::Eval(EvalError::ExpressionRuntimeFailure(
                error,
            ))) if error.class() == crate::ExpressionRuntimeFailureClass::ResourceLimit => {
                BatchRefusal::Resource
            }
            FailureKind::Native(_) => BatchRefusal::Native,
        }
    }

    pub(super) fn into_evaluator_error(self) -> EvaluatorError {
        self.into_error_at(crate::ExpressionRuntimeFailurePhase::Invoke)
    }

    pub(super) fn into_prepare_error(self) -> EvaluatorError {
        self.into_error_at(crate::ExpressionRuntimeFailurePhase::Prepare)
    }

    fn into_error_at(self, phase: crate::ExpressionRuntimeFailurePhase) -> EvaluatorError {
        let local = match self.kind {
            FailureKind::Native(error) => return error,
            FailureKind::Local(error) => error,
            FailureKind::Admission(message) => LocalError::InvalidSpec(message.into()),
            FailureKind::Bridge(error) => LocalError::BindingContract(error.to_string()),
        };
        if let Some(disposition) = local.native_json_type_disposition() {
            use tidb_query_expr::local::NativeJsonTypeDisposition;
            let error = match disposition {
                NativeJsonTypeDisposition::EmptyText => crate::context::JsonError::EmptyText,
                NativeJsonTypeDisposition::InvalidText => crate::context::JsonError::InvalidText,
                NativeJsonTypeDisposition::InvalidUtf8 => {
                    return EvalError::Unsupported("invalid UTF-8 string datum").into();
                }
            };
            return EvalError::Json(error).into();
        }
        EvalError::ExpressionRuntimeFailure(crate::ExpressionRuntimeFailure::from_local_eval(
            local,
            Some(phase),
        ))
        .into()
    }
}
impl From<LocalError> for BatchFailure {
    fn from(error: LocalError) -> Self {
        Self {
            kind: FailureKind::Local(error),
        }
    }
}
impl From<BridgeError> for BatchFailure {
    fn from(error: BridgeError) -> Self {
        Self {
            kind: FailureKind::Bridge(error),
        }
    }
}
impl From<EvaluatorError> for BatchFailure {
    fn from(error: EvaluatorError) -> Self {
        Self {
            kind: FailureKind::Native(error),
        }
    }
}
impl From<EvalError> for BatchFailure {
    fn from(error: EvalError) -> Self {
        EvaluatorError::Eval(error).into()
    }
}
type Result<T> = std::result::Result<T, BatchFailure>;

fn resource(message: &'static str) -> LocalError {
    LocalError::ResourceLimit(message.into())
}
fn binding(message: &'static str) -> LocalError {
    LocalError::BindingContract(message.into())
}
fn array_bytes<T>(count: usize) -> LocalResult<usize> {
    count
        .checked_mul(size_of::<T>())
        .ok_or_else(|| resource("batch caller size overflow"))
}
fn reserve<T>(values: &mut Vec<T>, count: usize) -> LocalResult<()> {
    values
        .try_reserve(count)
        .map_err(|_| resource("batch caller reservation failed"))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NativeView {
    Typed,
    JsonBytes,
    ValueToken,
    DiagnosticValue,
    EmptyRowValue,
    EmptyRowTyped(NativeEvalType),
    RowTyped(NativeEvalType),
    DiagnosticTyped(NativeEvalType),
    DiagnosticConstantRoot(crate::scalar_function::dynamic_diagnostic::LiteralPolicy),
    RootValue,
    FunctionRoot,
    BulkValue(NativeEvalType),
    BulkPrepared(NativeEvalType),
    BulkRoot(NativeEvalType),
    DecimalFast,
}
impl NativeView {
    fn value_role(self) -> Option<NativeValueRole> {
        match self {
            Self::ValueToken
            | Self::EmptyRowValue
            | Self::EmptyRowTyped(_)
            | Self::DiagnosticValue
            | Self::DiagnosticConstantRoot(_)
            | Self::RowTyped(_)
            | Self::DiagnosticTyped(_)
            | Self::BulkValue(_)
            | Self::BulkPrepared(_) => Some(NativeValueRole::CharInput),
            Self::RootValue | Self::BulkRoot(_) => Some(NativeValueRole::Output),
            Self::FunctionRoot => Some(NativeValueRole::FunctionOutput),
            Self::DecimalFast => Some(NativeValueRole::DecimalFast),
            Self::Typed | Self::JsonBytes => None,
        }
    }
    fn diagnostic(self) -> bool {
        matches!(
            self,
            Self::DiagnosticValue | Self::DiagnosticConstantRoot(_) | Self::DiagnosticTyped(_)
        )
    }
    fn semantic_empty_row(self) -> bool {
        self.diagnostic() || matches!(self, Self::EmptyRowValue | Self::EmptyRowTyped(_))
    }
    fn bulk_target(self) -> Option<NativeEvalType> {
        match self {
            Self::BulkValue(target) | Self::BulkPrepared(target) | Self::BulkRoot(target) => {
                Some(target)
            }
            _ => None,
        }
    }
}

fn row_typed_context_target<'a>(
    node: impl Into<SourceNodeRef<'a>>,
    view: NativeView,
) -> Option<NativeEvalType> {
    let node = node.into();
    let target = match view {
        NativeView::RowTyped(target)
        | NativeView::DiagnosticTyped(target)
        | NativeView::EmptyRowTyped(target) => target,
        NativeView::BulkValue(target)
        | NativeView::BulkPrepared(target)
        | NativeView::BulkRoot(target) => {
            if matches!(view, NativeView::BulkPrepared(_))
                && matches!(
                    node.kind(),
                    BorrowedNodeKind::Column(_) | BorrowedNodeKind::Constant(_)
                )
            {
                return Some(target);
            }
            target
        }
        _ => return None,
    };
    (matches!(node.kind(), BorrowedNodeKind::CorrelatedColumn(_))
        || matches!(node.kind(), BorrowedNodeKind::Constant(c) if c.param_marker.is_some() || c.deferred_expr.is_some())).then_some(target)
}
fn typed_bulk_forward<'a>(node: impl Into<SourceNodeRef<'a>>, view: NativeView) -> bool {
    let node = node.into();
    view.bulk_target().is_some()
        && matches!(node.kind(), BorrowedNodeKind::Constant(c) if c.deferred_expr.is_some())
}
fn typed_cache_source<'a>(node: impl Into<SourceNodeRef<'a>>, view: NativeView) -> bool {
    let node = node.into();
    view.bulk_target().is_some()
        && row_typed_context_target(node, view).is_some()
        && !typed_bulk_forward(node, view)
        && !matches!(node.kind(), BorrowedNodeKind::Column(_))
}
fn context_child_view(view: NativeView) -> NativeView {
    if let Some(target) = view.bulk_target() {
        NativeView::BulkPrepared(target)
    } else if view.diagnostic() {
        NativeView::DiagnosticValue
    } else if view.semantic_empty_row() {
        NativeView::EmptyRowValue
    } else {
        NativeView::ValueToken
    }
}

fn window_arithmetic_operation(
    function: &ScalarFunction,
    view: NativeView,
) -> Option<SqlBatchHostOperation> {
    view.bulk_target()?;
    if !matches!(
        function.native_arithmetic_generic_mode(),
        crate::scalar_function::NativeArithmeticInputMode::Numeric(_)
    ) {
        return None;
    }
    Some(
        match native_arithmetic_operation(function.func_name.lowercase())? {
            SqlBatchHostOperation::AddNative => SqlBatchHostOperation::AddWindowNative,
            SqlBatchHostOperation::SubtractNative => SqlBatchHostOperation::SubtractWindowNative,
            SqlBatchHostOperation::MultiplyNative => SqlBatchHostOperation::MultiplyWindowNative,
            SqlBatchHostOperation::ModuloNative => SqlBatchHostOperation::ModuloWindowNative,
            SqlBatchHostOperation::DivideNative => SqlBatchHostOperation::DivideWindowNative,
            SqlBatchHostOperation::IntegerDivideNative => {
                SqlBatchHostOperation::IntegerDivideWindowNative
            }
            _ => return None,
        },
    )
}
fn window_unary_operation(
    function: &ScalarFunction,
    view: NativeView,
) -> Option<SqlBatchHostOperation> {
    let target = view.bulk_target()?;
    if function.args.len() != 1 {
        return None;
    }
    let result = function.get_static_type()?;
    Some(match (function.func_name.lowercase(), target) {
        ("abs", NativeEvalType::Int) if result.eval_type() == NativeEvalType::Int => {
            SqlBatchHostOperation::AbsIntWindowNative
        }
        ("cast_decimal", NativeEvalType::Decimal)
            if result.eval_type() == NativeEvalType::Decimal =>
        {
            SqlBatchHostOperation::CastDecimalWindowNative
        }
        ("cast_json", NativeEvalType::Json) if result.eval_type() == NativeEvalType::Json => {
            SqlBatchHostOperation::CastJsonWindowNative
        }
        _ => return None,
    })
}
fn window_value_operation(
    function: &ScalarFunction,
    view: NativeView,
) -> Option<SqlBatchHostOperation> {
    window_arithmetic_operation(function, view).or_else(|| window_unary_operation(function, view))
}

fn bulk_value_tree_complete(
    node: &Expression,
    view: NativeView,
    depth: usize,
    remaining: &mut usize,
) -> bool {
    if depth == 0 || *remaining == 0 {
        return true;
    }
    let Expression::ScalarFunction(function) = node else {
        return native_value_tree_complete(node, depth, remaining);
    };
    if view.bulk_target().is_none() {
        return native_value_tree_complete(node, depth, remaining);
    }
    if window_value_operation(function, view).is_none() {
        return native_value_tree_complete(node, depth, remaining);
    }
    *remaining -= 1;
    function.args.iter().enumerate().all(|(index, child)| {
        bulk_value_tree_complete(
            child,
            argument_view(function, view, index),
            depth - 1,
            remaining,
        )
    })
}
fn promoted_native_root_view(root: &Expression, limits: BatchSourceLimits) -> Option<NativeView> {
    let Expression::ScalarFunction(function) = root else {
        return None;
    };
    let row_control = native_json_schema_call(function)
        || native_final_state_call(function)
        || native_stateful_call(function)
        || native_cached_list_call(function)
        || matches!(
            function.func_name.lowercase(),
            "json_sum_crc32"
                | "str_to_date"
                | "timestampdiff"
                | "sig_timestampdiff"
                | "tidb_bounded_staleness"
        )
        || native_epoch_call(function).is_some()
        || native_tso_call(function).is_some()
        || native_clock_call(function).is_some()
        || native_calendar_call(function).is_some()
        || native_duration_construction_call(function).is_some()
        || native_vector_call(function).is_some()
        || native_crypto_call(function).is_some()
        || native_compression_call(function).is_some()
        || native_byte_unary_call(function).is_some()
        || native_misc_call(function).is_some()
        || native_identity_call(function)
        || native_grouping_call(function)
        || native_like_call(function)
        || native_concat_value_call(function)
        || native_extract_call(function).is_some()
        || native_date_arithmetic_call(function).is_some()
        || native_charset_operation(function.func_name.lowercase()).is_some()
        || native_unary_sign_operation(function.func_name.lowercase()).is_some()
        || (function.func_name.lowercase() == "cast_char"
            && !typed_source_tree_complete(root, NativeView::Typed, limits.tree.max_depth, &mut {
                limits.tree.max_nodes
            }))
        || native_bitwise_operation(function.func_name.lowercase()).is_some()
        || native_selection_operation(function).is_some()
        || native_comparison_operation(function.func_name.lowercase()).is_some()
        || matches!(
            function.func_name.lowercase(),
            "in" | "interval" | "greatest" | "least"
        );
    if native_arithmetic_operation(function.func_name.lowercase()).is_none()
        && !matches!(
            function.func_name.lowercase(),
            "abs" | "cast_decimal" | "cast_json"
        )
        && !row_control
    {
        return None;
    }
    let Some(field) = root.static_type() else {
        return None;
    };

    // Extending actual-value producers must not replace an already closed
    // typed tree merely because its formerly typed-only string child can now
    // also produce a value handle. Keep the established phases and call sites.
    if contains_early_string(root, limits.tree.max_depth, &mut { limits.tree.max_nodes })
        && typed_source_tree_complete(root, NativeView::Typed, limits.tree.max_depth, &mut {
            limits.tree.max_nodes
        })
    {
        return None;
    }
    if row_control {
        // Native controls/comparisons invoke their selected operands per row,
        // even when the outer batch flag is enabled. Eager typed descendants
        // can change demand and error identity without any arithmetic node.
        // A closed native-value subtree therefore keeps its genuine row domain
        // independently of the spelling or carrier types of its descendants.
        return native_value_tree_complete(root, limits.tree.max_depth, &mut {
            limits.tree.max_nodes
        })
        .then_some(NativeView::RootValue);
    }
    let target = field.eval_type();
    if crate::scalar_function::batch_supported(root, target) {
        let view = NativeView::BulkRoot(target);
        bulk_value_tree_complete(root, view, limits.tree.max_depth, &mut {
            limits.tree.max_nodes
        })
        .then_some(view)
    } else {
        native_value_tree_complete(root, limits.tree.max_depth, &mut { limits.tree.max_nodes })
            .then_some(NativeView::RootValue)
    }
}

// Native value roots use the checked output ABI. Ordinary representable
// literals keep their existing broadcast fast path. This inspects borrowed
// kinds/declarations, never converts a payload before the complete byte gate.
fn root_source_view(root: &Expression, limits: CompileLimits) -> NativeView {
    match root {
        Expression::ScalarFunction(function)
            if function.pb_lazy_values().is_some()
                || function.pb_boolean_values().is_some()
                || function.pb_binary_values().is_some()
                || native_pb_eager_values(function)
                || native_regexp_source_value(function, limits)
                || native_json_schema_call(function)
                || native_final_state_call(function)
                || native_stateful_call(function)
                || native_cached_list_call(function)
                || native_optional_selection_call(function)
                || native_optional_operator_call(function)
                || native_optional_grouping_call(function)
                || native_concat_value_call(function)
                || native_optional_wildcard_call(function)
                || native_optional_arithmetic_call(function)
                || native_optional_ordered_comparison_call(function)
                || native_optional_date_arithmetic_call(function)
                || native_weight_string_call(function)
                || crate::scalar_function::prefix_value::PrefixKind::for_function(function)
                    .is_some()
                || native_no_argument_header_call(function) =>
        {
            NativeView::RootValue
        }
        Expression::Column(_) | Expression::CorrelatedColumn(_) => NativeView::RootValue,
        Expression::ScalarFunction(f)
            if crate::scalar_function::eager_values::prefers_common_body(f)
                || (crate::scalar_function::eager_values::supports(f)
                    && (!typed_source_tree_complete(
                        root,
                        NativeView::Typed,
                        limits.max_depth,
                        &mut { limits.max_nodes },
                    ) || f.identity_constant_probe().is_some_and(|v| {
                        !matches!(
                            v,
                            Datum::Null
                                | Datum::Int(_)
                                | Datum::UInt(_)
                                | Datum::Real(_)
                                | Datum::Float32(_)
                                | Datum::String(_)
                                | Datum::Bytes(_)
                        )
                    }))) =>
        {
            NativeView::RootValue
        }
        Expression::ScalarFunction(function)
            if native_cast_name(function).is_some()
                && function.func_name.lowercase() != "cast_char" =>
        {
            NativeView::RootValue
        }
        Expression::ScalarFunction(function)
            if native_math_operation(function).is_some()
                && !(contains_native_arithmetic(root, limits.max_depth, &mut {
                    limits.max_nodes
                }) && typed_source_tree_complete(
                    root,
                    NativeView::Typed,
                    limits.max_depth,
                    &mut { limits.max_nodes },
                ))
                && native_value_tree_complete(root, limits.max_depth, &mut {
                    limits.max_nodes
                }) =>
        {
            NativeView::RootValue
        }
        Expression::ScalarFunction(function)
            if (native_selection_operation(function).is_some()
                || native_comparison_operation(function.func_name.lowercase()).is_some())
                && nullable_prefers_native(root, limits) =>
        {
            NativeView::RootValue
        }
        Expression::Constant(constant) => {
            let Some(value) = constant.literal_value() else {
                return NativeView::RootValue;
            };
            let Some(sql) = root.static_type() else {
                return NativeView::RootValue;
            };
            let ordinary = match (sql.eval_type(), value) {
                (_, Datum::Null) => true,
                (NativeEvalType::Int, Datum::Int(_)) => true,
                (NativeEvalType::Int, Datum::UInt(_)) => sql.is_unsigned(),
                (NativeEvalType::Real, Datum::Real(value)) => !value.is_nan(),
                (NativeEvalType::String, Datum::String(_) | Datum::Bytes(_)) => true,
                _ => false,
            };
            if ordinary && check_type(sql).is_ok() {
                NativeView::Typed
            } else {
                NativeView::RootValue
            }
        }
        _ => NativeView::Typed,
    }
}

fn contains_context_value(node: &Expression, depth: usize, remaining: &mut usize) -> bool {
    if depth == 0 || *remaining == 0 {
        return false;
    }
    *remaining -= 1;
    match node {
        Expression::Constant(c) => c.param_marker.is_some() || c.deferred_expr.is_some(),
        Expression::CorrelatedColumn(_) => true,
        Expression::ScalarFunction(f) => {
            native_json_schema_call(f)
                || native_final_state_call(f)
                || native_stateful_call(f)
                || native_cached_list_call(f)
                || f.args
                    .iter()
                    .any(|child| contains_context_value(child, depth - 1, remaining))
        }
        _ => false,
    }
}
// A zero-value-input property still has its original logical expression in
// error text. Survey that real metadata tree independently of the data graph.
fn contains_metadata_property(node: &Expression, depth: usize, remaining: &mut usize) -> bool {
    if depth == 0 || *remaining == 0 {
        return false;
    }
    *remaining -= 1;
    match node {
        Expression::ScalarFunction(function) => {
            if let Some(kind) =
                crate::scalar_function::header_value::HeaderKind::for_function(function)
            {
                return kind.property().is_some();
            }
            function
                .args
                .iter()
                .any(|child| contains_metadata_property(child, depth - 1, remaining))
        }
        Expression::Constant(c) => c
            .deferred_expr
            .as_deref()
            .is_some_and(|child| contains_metadata_property(child, depth - 1, remaining)),
        _ => false,
    }
}
// Hidden virtual columns keep their genuine expression only for numeric
// diagnostic rendering. Survey it separately from physical value demand so a
// parameter is requested only if the original overflow text demands it.
fn contains_hidden_virtual_diagnostic(
    node: &Expression,
    depth: usize,
    remaining: &mut usize,
) -> bool {
    if depth == 0 || *remaining == 0 {
        return true;
    } // the complete survey owns resource refusal
    *remaining -= 1;
    match node {
        Expression::Column(c) => c.is_hidden && c.virtual_expr.is_some(),
        Expression::ScalarFunction(f) => f
            .args
            .iter()
            .any(|child| contains_hidden_virtual_diagnostic(child, depth - 1, remaining)),
        Expression::Constant(c) => c
            .deferred_expr
            .as_deref()
            .is_some_and(|child| contains_hidden_virtual_diagnostic(child, depth - 1, remaining)),
        Expression::CorrelatedColumn(_) => false,
    }
}
fn context_template_declared(node: &Expression, depth: usize, remaining: &mut usize) -> bool {
    if depth == 0 || *remaining == 0 {
        return false;
    }
    *remaining -= 1;
    if node.static_type().is_none() {
        return false;
    }
    match node {
        Expression::Constant(c) => c
            .deferred_expr
            .as_deref()
            .is_none_or(|child| context_template_declared(child, depth - 1, remaining)),
        Expression::ScalarFunction(f) => f
            .args
            .iter()
            .all(|child| context_template_declared(child, depth - 1, remaining)),
        _ => true,
    }
}
#[derive(Clone, Copy, Debug)]
enum ContextTypeProbe {
    Parameter(i64),
    MissingConstantType,
}
impl ContextTypeProbe {
    fn run(self, ctx: &dyn Columns) -> std::result::Result<(), EvalError> {
        match self {
            Self::Parameter(order) => crate::context_value::probe_parameter_type(order, ctx),
            Self::MissingConstantType => Err(EvalError::Unsupported("constant has no result type")),
        }
    }
}
// Genuine batch-eligible Boolean integer results retain scalar argument
// demand and original 0/1/NULL presentation inside the native row collector.
fn native_boolean_row_batch(root: &Expression) -> bool {
    matches!(root, Expression::ScalarFunction(function)
        if function.pb_signature().is_none() && function.pb_origin().is_none()
            && matches!(function.func_name.lowercase(), "isnull" | "istrue" | "isfalse" | "isnottrue" | "isnotfalse" | "isunknown" | "isnotunknown" | "istrue_with_null" | "not")
            && crate::scalar_function::static_batch_target(root) == Some(NativeEvalType::Int))
}
// Generic integer comparisons retain their original scalar demand, including
// a declined integer fast probe. Neither row profile broadcasts its arguments.
fn native_integer_row_batch(root: &Expression) -> bool {
    native_boolean_row_batch(root)
        || matches!(root, Expression::ScalarFunction(function)
        if function.pb_signature().is_none() && function.pb_origin().is_none()
            && function.args.len() == 2
            && function.ret_type.as_ref().is_some_and(|field| field.code() == FieldTypeCode::LongLong)
            && function.args.iter().all(|argument| argument.static_type().is_some_and(|field| field.eval_type() == NativeEvalType::Int))
            && matches!(native_comparison_operation(function.func_name.lowercase()), Some(SqlBatchHostOperation::CompareNative(_))))
}

// A genuine native candidate outside the established Projection profiles.
// Reuse its actual native view instead of rejecting contextual arguments.
fn additional_context_native_view(
    root: &Expression,
    limits: BatchSourceLimits,
) -> Option<NativeView> {
    let Expression::ScalarFunction(function) = root else {
        return None;
    };
    if function.pb_signature().is_some()
        || function.pb_origin().is_some()
        || function.has_values_offset()
        || function.has_grouping_metadata()
    {
        return None;
    }
    if native_unary_sign_operation(function.func_name.lowercase()).is_some()
        || matches!(
            function.func_name.lowercase(),
            "abs"
                | "cast_decimal"
                | "cast_json"
                | "cast_char"
                | "plus"
                | "minus"
                | "mul"
                | "mod"
                | "div"
                | "intdiv"
        )
        || native_concat_row_batch(function)
        || native_string_row_batch(root)
        || native_regexp_row_batch(root)
        || native_integer_row_batch(root)
    {
        return None;
    }
    if !contains_context_value(root, limits.tree.max_depth, &mut { limits.tree.max_nodes }) {
        return None;
    }
    let Some(target) = crate::scalar_function::static_batch_target(root) else {
        return None;
    };
    Some(if crate::scalar_function::native_scalar_row_batch(root) {
        NativeView::RootValue
    } else {
        NativeView::BulkRoot(target)
    })
}
fn context_root_policy(
    root: &Expression,
    unordered: bool,
    limits: BatchSourceLimits,
) -> Result<(bool, bool, Option<ContextTypeProbe>)> {
    let shortcut = unordered && matches!(root,Expression::Constant(c) if c.deferred_expr.is_none());
    if !contains_context_value(root, limits.tree.max_depth, &mut { limits.tree.max_nodes }) {
        return Ok((shortcut, false, None));
    }
    if unordered && additional_context_native_view(root, limits).is_some() {
        return Ok((shortcut, false, None));
    }
    let mut probe = None;
    if unordered {
        if !matches!(
            root,
            Expression::Constant(_) | Expression::CorrelatedColumn(_)
        ) && !context_template_declared(root, limits.tree.max_depth, &mut {
            limits.tree.max_nodes
        }) && !native_value_tree_complete(root, limits.tree.max_depth, &mut {
            limits.tree.max_nodes
        }) {
            return Err(BatchFailure::admission(
                "contextual optional declarations need their explicit profile",
            ));
        }
        if matches!(root, Expression::ScalarFunction(function) if native_unary_sign_operation(function.func_name.lowercase()).is_none() && !matches!(function.func_name.lowercase(), "abs" | "cast_decimal" | "cast_json" | "cast_char" | "plus" | "minus" | "mul" | "mod" | "div" | "intdiv"))
            && crate::scalar_function::static_batch_target(root).is_some()
            && !matches!(root, Expression::ScalarFunction(function) if native_concat_row_batch(function))
            && !native_string_row_batch(root)
            && !native_regexp_row_batch(root)
            && !pb_value_row_batch(root)
            && !native_integer_row_batch(root)
        {
            return Err(BatchFailure::admission(
                "contextual typed root operand phases are not yet closed",
            ));
        }
        if let Expression::Constant(c) = root {
            let mut inner = root;
            while let Expression::Constant(next) = inner {
                let Some(child) = next.deferred_expr.as_deref() else {
                    break;
                };
                inner = child;
            }
            if matches!(inner, Expression::ScalarFunction(_)) {
                let possible = if c.param_marker.is_some() {
                    [
                        NativeEvalType::Int,
                        NativeEvalType::Real,
                        NativeEvalType::Decimal,
                        NativeEvalType::String,
                        NativeEvalType::Datetime,
                        NativeEvalType::Timestamp,
                        NativeEvalType::Duration,
                        NativeEvalType::Json,
                        NativeEvalType::VectorFloat32,
                    ]
                    .into_iter()
                    .any(|target| crate::scalar_function::batch_supported(root, target))
                } else {
                    c.ret_type.as_ref().is_some_and(|field| {
                        crate::scalar_function::batch_supported(root, field.eval_type())
                    })
                };
                if possible {
                    return Err(BatchFailure::admission(
                        "deferred typed root needs its original admission profile",
                    ));
                }
                probe = c
                    .param_marker
                    .map(|marker| ContextTypeProbe::Parameter(marker.order))
                    .or_else(|| {
                        c.ret_type
                            .is_none()
                            .then_some(ContextTypeProbe::MissingConstantType)
                    });
            }
        }
    }
    Ok((shortcut, !shortcut, probe))
}

fn contains_early_string(node: &Expression, depth: usize, remaining: &mut usize) -> bool {
    if depth == 0 || *remaining == 0 {
        return false;
    }
    *remaining -= 1;
    if let Expression::ScalarFunction(function) = node {
        if crate::scalar_function::early_string::EarlyString::supports(function) {
            return true;
        }
        return function
            .args
            .iter()
            .any(|child| contains_early_string(child, depth - 1, remaining));
    }
    false
}

fn contains_native_arithmetic(node: &Expression, depth: usize, remaining: &mut usize) -> bool {
    if depth == 0 || *remaining == 0 {
        return true;
    }
    *remaining -= 1;
    if let Expression::ScalarFunction(function) = node {
        if native_arithmetic_operation(function.func_name.lowercase()).is_some() {
            return true;
        }
        for child in &function.args {
            if contains_native_arithmetic(child, depth - 1, remaining) {
                return true;
            }
        }
    }
    false
}

// Preserve the existing compiled batch's operand phases when its whole
// source closure is proved. Native value islands extend missing producers;
// choosing them must not silently turn an eager batch into a row evaluation.
// This is borrowed metadata/kind inspection, not conversion before preflight.
fn typed_source_tree_complete(
    node: &Expression,
    view: NativeView,
    depth: usize,
    remaining: &mut usize,
) -> bool {
    if depth == 0 || *remaining == 0 {
        return false;
    }
    if view.value_role().is_some() {
        return native_value_tree_complete(node, depth, remaining);
    }
    *remaining -= 1;
    let Ok((field, _)) = parts(node, false) else {
        return false;
    };
    if check_type(field).is_err() {
        return false;
    }
    match node {
        Expression::Constant(value) => match (field.eval_type(), value.literal_value()) {
            (_, Some(Datum::Null)) => true,
            (NativeEvalType::Int, Some(Datum::Int(_))) => true,
            (NativeEvalType::Int, Some(Datum::UInt(_))) => field.is_unsigned(),
            (NativeEvalType::Real, Some(Datum::Real(value))) => !value.is_nan(),
            (NativeEvalType::Decimal, Some(Datum::Decimal(_))) => true,
            (NativeEvalType::String, Some(value)) => value.as_raw_bytes().is_some(),
            (NativeEvalType::Datetime | NativeEvalType::Timestamp, Some(Datum::Time(_))) => true,
            (NativeEvalType::Duration, Some(Datum::Duration(_))) => true,
            (NativeEvalType::Json, Some(Datum::Json(_))) => true,
            (NativeEvalType::VectorFloat32, Some(Datum::VectorFloat32(_))) => true,
            _ => false,
        },
        Expression::Column(_) => true,
        Expression::CorrelatedColumn(_) => false,
        Expression::ScalarFunction(function) => {
            if resolve_call_in_view(function, view).is_err() {
                return false;
            }
            for (index, child) in function.args.iter().enumerate() {
                if !typed_source_tree_complete(
                    child,
                    argument_view(function, view, index),
                    depth - 1,
                    remaining,
                ) {
                    return false;
                }
            }
            true
        }
    }
}

// Bounded metadata/kind inspection only: no payload conversion or detached copy
// precedes preflight's complete source byte gate. Existing closed Int/String
// controls keep their typed frame fast path.
fn nullable_prefers_native(root: &Expression, limits: CompileLimits) -> bool {
    let mut remaining = limits.max_nodes;
    if !nullable_needs_native(root, limits.max_depth, &mut remaining) {
        return false;
    }
    // Until every producer can yield native handles, retain already prepared
    // typed control trees rather than moving a supported IF/kernel child into
    // an incomplete native island.
    remaining = limits.max_nodes;
    native_value_tree_complete(root, limits.max_depth, &mut remaining)
}
fn native_value_tree_complete(node: &Expression, depth: usize, remaining: &mut usize) -> bool {
    if depth == 0 || *remaining == 0 {
        return true;
    } // full preflight owns the limit error
    *remaining -= 1;
    match node {
        Expression::Constant(c) => {
            if c.param_marker.is_none() {
                if let Some(child) = c.deferred_expr.as_deref() {
                    return native_value_tree_complete(child, depth.saturating_sub(1), remaining);
                }
            }
            true
        }
        Expression::Column(_) | Expression::CorrelatedColumn(_) => true,
        Expression::ScalarFunction(function) => {
            if native_no_argument_header_call(function) {
                return true;
            }
            if !native_value_call(function) {
                return false;
            }
            for child in function.args.iter().take(source_data_arity(function)) {
                // Stop the sibling iterator too: returning true from the
                // recursive leaf alone would still scan an oversized arity.
                if depth == 1 || *remaining == 0 {
                    return true;
                }
                if !native_value_tree_complete(child, depth - 1, remaining) {
                    return false;
                }
            }
            true
        }
    }
}
fn nullable_needs_native(node: &Expression, depth: usize, remaining: &mut usize) -> bool {
    if depth == 0 || *remaining == 0 {
        return true;
    }
    *remaining -= 1;
    let Some(field) = node.static_type() else {
        return false;
    };
    if check_type(field).is_err()
        || field.is_unsigned()
        || field.eval_type() == NativeEvalType::VectorFloat32
    {
        return true;
    }
    match node {
        Expression::Constant(value) => match (field.eval_type(), value.literal_value()) {
            (_, Some(Datum::Null)) | (NativeEvalType::Int, Some(Datum::Int(_))) => false,
            (NativeEvalType::Real, Some(Datum::Real(value))) => value.is_nan(),
            (NativeEvalType::String, Some(Datum::String(_) | Datum::Bytes(_) | Datum::Raw(_))) => {
                false
            }
            (NativeEvalType::Decimal, Some(Datum::Decimal(_)))
            | (NativeEvalType::Datetime | NativeEvalType::Timestamp, Some(Datum::Time(_)))
            | (NativeEvalType::Duration, Some(Datum::Duration(_)))
            | (NativeEvalType::Json, Some(Datum::Json(_))) => false,
            _ => true,
        },
        Expression::Column(_) => field.eval_type() == NativeEvalType::Real,
        Expression::ScalarFunction(function) => {
            if native_cast_name(function).is_some()
                || (function.func_name.lowercase() == "coalesce"
                    && matches!(
                        field.eval_type(),
                        NativeEvalType::Datetime
                            | NativeEvalType::Timestamp
                            | NativeEvalType::Duration
                    ))
            {
                return true;
            }
            for (index, child) in function.args.iter().enumerate() {
                if depth == 1 || *remaining == 0 {
                    return true;
                }
                let condition = matches!(
                    function.func_name.lowercase(),
                    "and" | "logicand" | "or" | "logicor"
                ) || native_boolean_operation(function.func_name.lowercase())
                    .is_some()
                    || native_comparison_operation(function.func_name.lowercase()).is_some()
                    || (function.func_name.lowercase() == "if" && index == 0)
                    || (function.func_name.lowercase() == "case"
                        && index % 2 == 0
                        && index + 1 < function.args.len());
                if condition
                    && child
                        .static_type()
                        .is_none_or(|field| field.eval_type() != NativeEvalType::Int)
                {
                    return true;
                }
                if nullable_needs_native(child, depth - 1, remaining) {
                    return true;
                }
            }
            false
        }
        Expression::CorrelatedColumn(_) => false,
    }
}

#[derive(Clone, Copy, Debug)]
enum CallTarget {
    Kernel(FunctionRef),
    Host(SqlBatchHostOperation),
}

#[derive(Debug)]
enum SourceKind {
    DecimalFastInput(decimal_fast_source::FastInput),
    NativeConstant {
        value: Datum,
    },
    Constant {
        value: ScalarValue,
    },
    Column {
        index: usize,
    },
    Call {
        target: CallTarget,
        arity: usize,
    },
    ContextValue {
        arity: usize,
    },
    TypedContext {
        arity: usize,
        target: NativeEvalType,
    },
}
fn source_value_failure<'a>(
    node: impl Into<SourceNodeRef<'a>>,
    view: NativeView,
    columns: usize,
) -> Option<&'static str> {
    let node = node.into();
    if view.value_role().is_none() || view.bulk_target().is_some() {
        return None;
    }
    if matches!(
        view,
        NativeView::RowTyped(_) | NativeView::DiagnosticTyped(_) | NativeView::EmptyRowTyped(_)
    ) && matches!(node.kind(),BorrowedNodeKind::Constant(c) if c.literal_value().is_some() && c.ret_type.is_none())
    {
        return Some("constant has no result type");
    }
    let BorrowedNodeKind::Column(column) = node.kind() else {
        return None;
    };
    let columns = if view.semantic_empty_row() {
        0
    } else {
        columns
    };
    match crate::column::column_read_index(column.ret_type.is_some(), column.index, columns) {
        Err(EvalError::Unsupported(message)) => Some(message),
        _ => None,
    }
}
fn budget_context_source(budget: &mut Budget, sql: Option<&FieldType>) -> LocalResult<()> {
    budget.array::<crate::context_value::ContextValue>(1)?;
    budget.array::<OrdinaryCallSite>(2)?;
    budget.array::<NativeHostMetadata>(1)?;
    budget.array::<(SqlBatchHostOperation, HostSignature)>(1)?;
    budget.array::<Option<Datum>>(1)?;
    if let Some(sql) = sql {
        budget.field(sql, 1)?;
    }
    budget.field(&native_bytes_type(), 2)
}
impl SourceKind {
    fn capture<'a>(
        node: impl Into<SourceNodeRef<'a>>,
        sql: Option<&FieldType>,
        view: NativeView,
        inputs: SourceInputs<'_>,
    ) -> Result<Self> {
        let node = node.into();
        if let Some(target) = inputs.typed_context_target(node, view) {
            return Ok(Self::TypedContext {
                target,
                arity: usize::from(
                    typed_bulk_forward(node, view)
                        || matches!(node.kind(), BorrowedNodeKind::Constant(c) if c.param_marker.is_none() && c.deferred_expr.is_some()),
                ),
            });
        }
        if source_value_failure(node, view, inputs.width()).is_some() {
            return Ok(Self::ContextValue { arity: 0 });
        }
        if matches!(node.kind(), BorrowedNodeKind::CorrelatedColumn(_))
            || matches!(node.kind(), BorrowedNodeKind::Constant(c) if c.param_marker.is_some() || c.deferred_expr.is_some())
        {
            if view.value_role().is_none() {
                return Err(BatchFailure::admission(
                    "contextual value requires its generic native view",
                ));
            }
            return Ok(Self::ContextValue {
                arity: usize::from(
                    matches!(node.kind(), BorrowedNodeKind::Constant(c) if c.param_marker.is_none() && c.deferred_expr.is_some()),
                ),
            });
        }
        if let BorrowedNodeKind::Constant(value) = node.kind() {
            if view.value_role().is_some() {
                return Ok(Self::NativeConstant {
                    value: value
                        .literal_value()
                        .ok_or_else(|| BatchFailure::admission("nonliteral native source"))?
                        .clone(),
                });
            }
        }
        if let BorrowedNodeKind::ScalarFunction(function) = node.kind() {
            let (target, arity) = resolve_call_in_view(function, view)?;
            return Ok(Self::Call { target, arity });
        }
        let sql = sql.ok_or_else(|| {
            BatchFailure::admission("SQL consumer requires its original declaration")
        })?;
        Ok(match node.kind() {
            BorrowedNodeKind::Constant(value) => Self::Constant {
                value: source_literal(
                    value
                        .literal_value()
                        .ok_or_else(|| BatchFailure::admission("nonliteral batch source"))?,
                    sql,
                    view,
                )?,
            },
            BorrowedNodeKind::Column(column) => Self::Column {
                index: column.index as usize,
            },
            BorrowedNodeKind::ScalarFunction(_) => {
                unreachable!("scalar call captured without inventing SQL metadata")
            }
            BorrowedNodeKind::CorrelatedColumn(_) => {
                return Err(BatchFailure::admission("correlated source"));
            }
        })
    }
}

#[derive(Debug)]
struct NodeRecord {
    sql: Option<FieldType>,
    kind: SourceKind,
    view: NativeView,
}

impl NodeRecord {
    fn declared_sql(&self) -> &FieldType {
        self.sql
            .as_ref()
            .expect("admitted SQL consumer requires its original declaration")
    }
}

struct BatchSource {
    // The actual native program owner, not another lowered row/D4 plan. Native
    // evaluation is never called through it by the mandatory consumer.
    owner: Arc<EvaluatorProgram>,
    // Address of the root owned by `owner`. The Arc pins the program and its
    // calculated Vec for this source's lifetime; the address is compared only
    // as an identity and is never dereferenced.
    #[cfg(test)]
    root_identity: usize,
    calculated_slot: usize,
    output_index: usize,
    #[cfg(test)]
    row_schema: Box<[FieldType]>,
    plan: SourcePlan,
}
impl std::ops::Deref for BatchSource {
    type Target = SourcePlan;
    fn deref(&self) -> &Self::Target {
        &self.plan
    }
}

// Detached source metadata and the shared expression program do not own a
// Projection suite. Caller-specific seals stay in their actual consumer.
struct SourcePlan {
    unit: u64,
    logical_metadata_bytes: usize,
    nodes: Box<[NodeRecord]>,
    bindings: Box<[usize]>, // slot -> all-node source ordinal
    expr: LocalExpr,
    schema: Box<[tipb::FieldType]>,
    facts: BatchFacts,
    hosts: Option<HostCatalog>,
    host_metadata: Box<[NativeHostMetadata]>,
    host_probes: Box<[Option<Datum>]>,
    needs_native_values: bool,
    constant_root_shortcut: bool,
    context_row_only: bool,
    context_policy: Option<Box<ContextSourcePolicy>>,
    limits: BatchSourceLimits,
}
struct ContextSourcePolicy {
    root_type_probe: Option<ContextTypeProbe>,
    diagnostic_slots: Box<[usize]>,
    diagnostic_parents: Box<[CompiledDiagnosticParent]>,
    numeric_parents: Box<[CompiledNumericParent]>,
    typed_cache_slots: usize,
}
struct PendingDiagnostic<'a> {
    ordinal: usize,
    recipe: Option<crate::scalar_function::dynamic_diagnostic::DiagnosticRecipe<'a>>,
    main_arity: usize,
    bindings: usize,
}
struct CheckedSource<'a, N = &'a Expression> {
    logical_metadata_bytes: usize,
    nodes: Vec<CheckedNode<'a, N>>,
    diagnostics: Vec<PendingDiagnostic<'a>>,
    numerics: Vec<PendingNumeric<'a>>,
}
impl<'a, N> std::ops::Deref for CheckedSource<'a, N> {
    type Target = [CheckedNode<'a, N>];
    fn deref(&self) -> &Self::Target {
        &self.nodes
    }
}
impl std::fmt::Debug for BatchSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchSource")
            .field("unit", &self.unit)
            .field("nodes", &self.nodes.len())
            .finish()
    }
}

struct CheckedNode<'a, N = &'a Expression> {
    node: N,
    sql: Option<&'a FieldType>,
    view: NativeView,
    subtree_end: usize,
    folds_to_constant: bool,
    math_render_bound: Option<usize>,
    numeric_render_bound: Option<usize>,
    numeric_format_failure: Option<[u8; 8]>,
}
impl<'a, N> CheckedNode<'a, N> {
    fn declared_sql(&self) -> &'a FieldType {
        self.sql
            .expect("admitted SQL consumer requires its original declaration")
    }
}

struct Budget {
    used: usize,
}
impl Budget {
    fn add(&mut self, bytes: usize) -> LocalResult<()> {
        self.used = self
            .used
            .checked_add(bytes)
            .ok_or_else(|| resource("batch caller source metadata policy exceeded"))?;
        Ok(())
    }
    fn array<T>(&mut self, count: usize) -> LocalResult<()> {
        self.add(array_bytes::<T>(count)?)
    }
    fn field(&mut self, field: &FieldType, copies: usize) -> LocalResult<()> {
        self.add(
            field
                .checked_snapshot_payload_bytes()
                .and_then(|size| size.checked_mul(copies))
                .ok_or_else(|| resource("batch caller FieldType payload overflow"))?,
        )
    }
}

fn budget_eager_metadata(
    budget: &mut Budget,
    function: &ScalarFunction,
    result: Option<&FieldType>,
) -> LocalResult<()> {
    let arity = function.args.len();
    budget.array::<crate::scalar_function::eager_values::EagerArgument>(arity)?;
    budget.array::<usize>(arity)?;
    budget.add(
        function
            .func_name
            .original()
            .len()
            .checked_mul(2)
            .ok_or_else(|| resource("eager name extent overflow"))?,
    )?;
    if let Some(result) = result {
        budget.field(result, 2)?;
    }
    for arg in &function.args {
        if let Some(field) = arg.static_type() {
            budget.field(field, 1)?;
        }
    }
    if crate::scalar_function::early_string::EarlyString::supports(function) {
        budget.array::<crate::scalar_function::early_string::EarlyString>(1)?;
    }
    if function.func_name.lowercase() == "locate" && arity == 3 {
        if let Some(field) = function.args[2].static_type() {
            budget.field(field, 1)?;
        }
    }
    if let Some(value) = function.identity_constant_probe() {
        budget.add(
            crate::tikv::identity_actual::copy_extent(value)
                .map_err(|_| resource("eager probe copy extent overflow"))?,
        )?;
    }
    Ok(())
}
fn budget_value_call_metadata(
    budget: &mut Budget,
    function: &ScalarFunction,
    result: Option<&FieldType>,
) -> LocalResult<()> {
    let arity = function.args.len();
    if function.pb_lazy_values().is_some() {
        budget.array::<hosts::NativePbLazy>(1)?;
        budget.array::<usize>(source_data_arity(function))?;
        if let Some(field) = result {
            budget.field(field, 1)?;
        }
        budget.add(function.func_name.lowercase().len())?;
        return Ok(());
    }
    if function.pb_boolean_values().is_some() {
        budget.array::<hosts::NativePbBoolean>(1)?;
        budget.array::<usize>(source_data_arity(function))?;
        if let Some(field) = result {
            budget.field(field, 1)?;
        }
        budget.add(function.func_name.lowercase().len())?;
        return Ok(());
    }
    if function.pb_binary_values().is_some() && !native_no_argument_header_call(function) {
        budget.array::<hosts::NativePbBinary>(1)?;
        budget.array::<usize>(source_data_arity(function))?;
        for argument in function.args.iter().take(2) {
            if let Some(field) = argument.static_type() {
                budget.field(field, 1)?;
            }
        }
        if let Some(field) = result {
            budget.field(field, 1)?;
        }
        budget.add(function.func_name.lowercase().len())?;
        return Ok(());
    }
    if native_pb_eager_values(function) {
        budget.array::<hosts::NativePbEagerValues>(1)?;
        if function.pb_cast_values().is_some() {
            budget.array::<hosts::PbCastKernel>(1)?;
            if let Some(field) = function.args.first().and_then(Expression::static_type) {
                budget.field(field, 1)?;
            }
        }
        if function.pb_json_signature().is_some() {
            budget.array::<crate::scalar_function::PbJsonValues>(1)?;
            budget.array::<Option<FieldType>>(arity)?;
            for argument in &function.args {
                if let Some(field) = argument.static_type() {
                    budget.field(field, 1)?;
                }
            }
        }
        budget.array::<usize>(arity)?;
        budget.add(function.func_name.lowercase().len())?;
        if let Some(field) = result {
            budget.field(field, 1)?;
        }
        return Ok(());
    }
    if native_regexp_call(function) {
        budget.array::<hosts::NativeRegexpValues>(1)?;
        budget.array::<usize>(arity)?;
        budget.array::<Option<FieldType>>(arity)?;
        for arg in &function.args {
            if let Some(field) = arg.static_type() {
                budget.field(field, 1)?;
            }
        }
        if let Some(field) = result {
            budget.field(field, 1)?;
        }
        return Ok(());
    }

    if native_weight_string_call(function) {
        budget.array::<hosts::NativeWeightString>(1)?;
        budget.array::<usize>(source_data_arity(function))?;
        if let Some(field) = function.args[0].static_type() {
            budget.field(field, 1)?;
        }
        if let Some(field) = result {
            budget.field(field, 1)?;
        }
        return Ok(());
    }

    if native_no_argument_header_call(function) {
        if let Some(result) = result {
            budget.field(result, 1)?;
        }
        return Ok(());
    }
    if native_optional_date_arithmetic_call(function) {
        if let Some(result) = result {
            budget.field(result, 1)?;
        }
        let (unit, _) =
            native_date_arithmetic_call(function).expect("checked optional date prefix");
        budget.add(unit.len())?;
        budget.add(unit.len())?;
        for arg in &function.args {
            if let Some(field) = arg.static_type() {
                budget.field(field, 1)?;
            }
        }
        return Ok(());
    }
    if native_optional_ordered_comparison_call(function) {
        if let Some(result) = result {
            budget.field(result, 1)?;
        }
        match function.func_name.lowercase() {
            "in" => {
                budget.array::<hosts::NativeInArgument>(function.args.len())?;
                for arg in &function.args {
                    if let Some(field) = arg.static_type() {
                        budget.field(field, 1)?;
                    }
                }
                let (keys, dynamic, _) = function.native_in_cache();
                budget.array::<usize>(dynamic.len())?;
                if let Some(keys) = keys {
                    budget.array::<(Vec<u8>, u64)>(
                        keys.capacity()
                            .checked_mul(4)
                            .ok_or_else(|| resource("IN key table allowance overflow"))?,
                    )?;
                    for key in keys {
                        budget.add(
                            key.len()
                                .checked_mul(2)
                                .ok_or_else(|| resource("IN key copy allowance overflow"))?,
                        )?;
                    }
                }
            }
            "interval" => {
                budget.array::<hosts::NativeIntervalArgument>(function.args.len())?;
                for arg in &function.args {
                    if let Some(field) = arg.static_type() {
                        budget.field(field, 1)?;
                    }
                }
            }
            "greatest" | "least" => {
                budget.array::<usize>(function.args.len())?;
                budget.array::<i64>(function.args.len())?;
            }
            _ => unreachable!("checked ordered comparison owner"),
        }
        return Ok(());
    }
    if native_optional_arithmetic_call(function) {
        if let Some(result) = result {
            budget.field(result, 1)?;
        }
        for arg in &function.args {
            if let Some(field) = arg.static_type() {
                budget.field(field, 1)?;
            }
        }
        return Ok(());
    }
    if native_concat_value_call(function) {
        if let Some(result) = result {
            budget.field(result, 1)?;
        }
        budget.array::<usize>(arity)?;
        return Ok(());
    }
    if native_optional_wildcard_call(function) {
        if let Some(result) = result {
            budget.field(result, 1)?;
        }
        if let Some(escape) = function.args[2].static_type() {
            budget.field(escape, 1)?;
        }
        budget.array::<tidb_query_expr::NativeLikeInvocation>(1)?;
        budget.array::<[usize; 3]>(1)?;
        return Ok(());
    }
    if native_optional_grouping_call(function) {
        if let Some(result) = result {
            budget.field(result, 1)?;
        }
        budget.array::<Option<std::sync::Arc<crate::grouping::GroupingMetadata>>>(1)?;
        return Ok(());
    }
    if native_json_schema_call(function) {
        budget.array::<NativeJsonSchema>(1)?;
        budget.array::<usize>(arity)?;
    } else if native_final_state_call(function) {
        budget.array::<NativeFinalState>(1)?;
        budget.array::<usize>(arity)?;
    } else if native_stateful_call(function) {
        budget.array::<NativeStateful>(1)?;
        budget.array::<usize>(arity)?;
        budget.array::<Option<FieldType>>(arity)?;
        if let Some(result) = result {
            budget.field(result, 1)?;
        }
        for arg in &function.args {
            if let Some(field) = arg.static_type() {
                budget.field(field, 1)?;
            }
        }
        return Ok(());
    } else if native_cached_list_call(function) {
        budget.array::<NativeCachedList>(1)?;
    } else {
        return budget_eager_metadata(budget, function, result);
    }
    if let Some(result) = result {
        budget.field(result, 1)?;
    }
    Ok(())
}
fn check_type(sql: &FieldType) -> Result<()> {
    const DOCUMENTED: u64 = (1 << 25) - 1;
    let excluded = u64::from(
        FieldTypeFlags::ENUM
            | FieldTypeFlags::SET
            | FieldTypeFlags::PARSE_TO_JSON
            | FieldTypeFlags::ENUM_SET_AS_INT,
    );
    let carrier_ok = match sql.eval_type() {
        NativeEvalType::Int => matches!(
            sql.code(),
            FieldTypeCode::Tiny
                | FieldTypeCode::Short
                | FieldTypeCode::Int24
                | FieldTypeCode::Long
                | FieldTypeCode::LongLong
        ),
        NativeEvalType::Real => sql.code() == FieldTypeCode::Double,
        NativeEvalType::Decimal => sql.code() == FieldTypeCode::NewDecimal,
        NativeEvalType::String => matches!(
            sql.code(),
            FieldTypeCode::Varchar
                | FieldTypeCode::VarString
                | FieldTypeCode::String
                | FieldTypeCode::TinyBlob
                | FieldTypeCode::MediumBlob
                | FieldTypeCode::LongBlob
                | FieldTypeCode::Blob
        ),
        NativeEvalType::Datetime => {
            matches!(
                sql.code(),
                FieldTypeCode::Date | FieldTypeCode::NewDate | FieldTypeCode::Datetime
            )
        }
        NativeEvalType::Timestamp => sql.code() == FieldTypeCode::Timestamp,
        NativeEvalType::Duration => sql.code() == FieldTypeCode::Duration,
        NativeEvalType::Json => sql.code() == FieldTypeCode::Json,
        NativeEvalType::VectorFloat32 => sql.code() == FieldTypeCode::VectorFloat32,
        _ => false,
    };
    if (sql.is_unsigned() && sql.eval_type() != NativeEvalType::Int)
        || !carrier_ok
        || sql.is_array()
        || sql.raw_flags() & (excluded | !DOCUMENTED) != 0
    {
        return Err(BatchFailure::admission(
            "batch source requires an actual closed native EvalType storage",
        ));
    }
    Ok(())
}
fn native_bytes_type() -> FieldType {
    FieldType::new(FieldTypeCode::Blob).with_collation(tidb_datatype::Collation::Binary)
}
// Borrowed logical payload policy, before any conversion/clone. Charge the
// source record, lowered literal and compiled literal copy groups. Decimal uses
// its source coefficient bytes conservatively (shared words may be smaller);
// fixed inline cells are already charged with the node scaffolding. This is not
// an allocator-capacity or statement peak-memory measurement.
fn literal_copy_budget(value: &Datum, sql: &FieldType) -> LocalResult<usize> {
    let bytes = match (sql.eval_type(), value) {
        (NativeEvalType::String, value) => value.as_raw_bytes().map_or(0, <[u8]>::len),
        (NativeEvalType::Decimal, Datum::Decimal(value)) => value.coefficient_bytes().len(),
        (NativeEvalType::Json, Datum::Json(value)) => value
            .value()
            .len()
            .checked_add(1)
            .ok_or_else(|| resource("native JSON literal size overflow"))?,
        (NativeEvalType::VectorFloat32, Datum::VectorFloat32(value)) => value
            .len()
            .checked_mul(size_of::<f32>())
            .ok_or_else(|| resource("native vector literal size overflow"))?,
        _ => 0,
    };
    bytes
        .checked_mul(3)
        .ok_or_else(|| resource("native literal copy size overflow"))
}

fn source_literal(value: &Datum, sql: &FieldType, view: NativeView) -> Result<ScalarValue> {
    if view.value_role().is_some() {
        return Err(binding("native value literals require a demanded source binding").into());
    }
    if view == NativeView::JsonBytes {
        return match (sql.eval_type(), value) {
            (NativeEvalType::Json, Datum::Null) => Ok(ScalarValue::Bytes(None)),
            (NativeEvalType::Json, Datum::Json(value)) => {
                Ok(ScalarValue::Bytes(Some(value.encoded())))
            }
            _ => Err(BatchFailure::admission(
                "native JSON byte view requires an actual JSON literal",
            )),
        };
    }
    strict_literal(value, sql)
}
fn argument_view(function: &ScalarFunction, view: NativeView, index: usize) -> NativeView {
    let selected = argument_view_inner(function, view, index);
    if view.diagnostic() {
        match selected {
            NativeView::RowTyped(target) => NativeView::DiagnosticTyped(target),
            _ => NativeView::DiagnosticValue,
        }
    } else if view.semantic_empty_row() {
        match selected {
            NativeView::RowTyped(target) => NativeView::EmptyRowTyped(target),
            _ => NativeView::EmptyRowValue,
        }
    } else {
        selected
    }
}
fn argument_view_inner(function: &ScalarFunction, view: NativeView, index: usize) -> NativeView {
    if native_json_schema_call(function)
        || native_final_state_call(function)
        || native_stateful_call(function)
    {
        return NativeView::ValueToken;
    }
    if native_cached_list_call(function) {
        return if index == 1
            && function.args[1].const_level() >= crate::expression::ConstLevel::ONLY_IN_CONTEXT
        {
            NativeView::EmptyRowValue
        } else {
            NativeView::ValueToken
        };
    }
    if let Some(operation) = window_unary_operation(function, view) {
        return NativeView::BulkValue(
            if operation == SqlBatchHostOperation::CastJsonWindowNative {
                function.args[0]
                    .static_type()
                    .expect("typed JSON source")
                    .eval_type()
            } else {
                NativeEvalType::Int
            },
        );
    }
    if window_arithmetic_operation(function, view).is_some() {
        let crate::scalar_function::NativeArithmeticInputMode::Numeric(domain) =
            function.native_arithmetic_generic_mode()
        else {
            unreachable!()
        };
        let argument = &function.args[index];
        return NativeView::BulkValue(crate::scalar_function::native_numeric_batch_source(
            argument.static_type().expect("typed arithmetic child"),
            crate::scalar_function::dynamic_diagnostic::saved_binary_literal(argument),
            domain,
        ));
    }
    // The selected PB scalar body has raw demands. Only the genuine bulk
    // branches above may choose the old arithmetic operand preparation.
    if let Some(domain) = pb_scalar_binary_domain(function, view) {
        return numeric_source::mode_view(
            function,
            index,
            crate::scalar_function::NativeArithmeticInputMode::Numeric(domain),
            view.diagnostic(),
        );
    }
    if let Some(kernel) = function
        .pb_cast_values()
        .filter(|_| function.args.len() == 1)
    {
        return NativeView::RowTyped(kernel.source());
    }
    if pb_value_owner(function) {
        return NativeView::ValueToken;
    }
    if native_arithmetic_operation(function.func_name.lowercase()).is_some()
        && function.args.len() == 2
        && view.value_role().is_some()
    {
        use crate::scalar_function::NativeArithmeticInputMode as Mode;
        return match function.native_arithmetic_mode() {
            Mode::TypedInteger => NativeView::RowTyped(NativeEvalType::Int),
            Mode::Numeric(target) => {
                NativeView::RowTyped(crate::scalar_function::native_numeric_batch_source(
                    function.args[index]
                        .static_type()
                        .expect("declared numeric source"),
                    crate::scalar_function::dynamic_diagnostic::saved_binary_literal(
                        &function.args[index],
                    ),
                    target,
                ))
            }
            Mode::Raw => NativeView::ValueToken,
        };
    }
    if function.func_name.lowercase() == "cast_json" && function.args.len() == 1 {
        if let Some(field) = function.args[0].static_type() {
            return NativeView::RowTyped(field.eval_type());
        }
    }
    if matches!(
        function.func_name.lowercase(),
        "json_sum_crc32"
            | "str_to_date"
            | "timestampdiff"
            | "sig_timestampdiff"
            | "tidb_bounded_staleness"
    ) || native_epoch_call(function).is_some()
        || native_tso_call(function).is_some()
        || native_clock_call(function).is_some()
        || native_calendar_call(function).is_some()
        || native_duration_construction_call(function).is_some()
        || native_vector_call(function).is_some()
        || (native_crypto_call(function).is_some() && view.value_role().is_some())
        || (native_compression_call(function).is_some() && view.value_role().is_some())
        || (native_byte_unary_call(function).is_some() && view.value_role().is_some())
        || (native_misc_call(function).is_some() && view.value_role().is_some())
        || (native_identity_call(function) && view.value_role().is_some())
        || native_grouping_call(function)
        || native_like_call(function)
        || native_extract_call(function).is_some()
        || native_date_arithmetic_call(function).is_some()
        || native_charset_operation(function.func_name.lowercase()).is_some()
    {
        // Native producers always consume authentic native source values;
        // a typed parent receives its original carrier from the same value.
        return NativeView::ValueToken;
    }
    if view.value_role().is_some() && native_value_call(function) {
        return NativeView::ValueToken;
    }
    if matches!(
        resolve_call(function),
        Ok((
            CallTarget::Host(SqlBatchHostOperation::CastCharNative { .. }),
            1
        ))
    ) {
        return NativeView::ValueToken;
    }
    if function.func_name.lowercase() == "json_type"
        && function.args.len() == 1
        && function.args[0]
            .static_type()
            .is_some_and(|field| field.eval_type() == NativeEvalType::Json)
    {
        NativeView::JsonBytes
    } else {
        NativeView::Typed
    }
}
fn strict_literal(value: &Datum, sql: &FieldType) -> Result<ScalarValue> {
    match (sql.eval_type(), value) {
        (NativeEvalType::Int, Datum::Null) => Ok(ScalarValue::Int(None)),
        (NativeEvalType::Int, Datum::Int(value)) => Ok(ScalarValue::Int(Some(*value))),
        (NativeEvalType::Int, Datum::UInt(value)) if sql.is_unsigned() => {
            Ok(ScalarValue::Int(Some(*value as i64)))
        }
        (NativeEvalType::Real, Datum::Null) => Ok(ScalarValue::Real(None)),
        (NativeEvalType::Real, Datum::Real(value)) => Real::new(*value)
            .map(|value| ScalarValue::Real(Some(value)))
            .map_err(|_| BatchFailure::admission("batch Real literal is NaN")),
        (NativeEvalType::Decimal, Datum::Null) => Ok(ScalarValue::Decimal(None)),
        (NativeEvalType::Decimal, Datum::Decimal(value)) => value
            .try_to_shared_math(1024 * 1024)
            .map(|value| ScalarValue::Decimal(Some(value)))
            .map_err(|_| BatchFailure::admission("batch Decimal literal is not representable")),
        (NativeEvalType::String, Datum::Null) => Ok(ScalarValue::Bytes(None)),
        (NativeEvalType::String, value) => value
            .as_raw_bytes()
            .map(|bytes| ScalarValue::Bytes(Some(bytes.to_vec())))
            .ok_or_else(|| {
                BatchFailure::admission(
                    "batch String literal does not contain a strict byte payload",
                )
            }),
        (NativeEvalType::Datetime | NativeEvalType::Timestamp, Datum::Null) => {
            Ok(ScalarValue::DateTime(None))
        }
        (NativeEvalType::Datetime | NativeEvalType::Timestamp, Datum::Time(value)) => Ok(
            ScalarValue::DateTime(Some(TikvDateTime::from_go_raw_like_go(value.go_raw()))),
        ),
        (NativeEvalType::Duration, Datum::Null) => Ok(ScalarValue::Duration(None)),
        (NativeEvalType::Duration, Datum::Duration(value)) => TikvDuration::from_native_raw_parts(
            value.nanoseconds(),
            i8::try_from(value.fsp())
                .map_err(|_| BatchFailure::admission("batch Duration FSP is out of range"))?,
        )
        .map(|value| ScalarValue::Duration(Some(value)))
        .map_err(|_| BatchFailure::admission("batch Duration literal is not representable")),
        (NativeEvalType::Json, Datum::Null) => Ok(ScalarValue::Json(None)),
        (NativeEvalType::Json, Datum::Json(value)) => JsonType::try_from(value.type_code())
            .map(|kind| ScalarValue::Json(Some(TikvJson::new(kind, value.value().to_vec()))))
            .map_err(|_| BatchFailure::admission("batch JSON literal has an unknown type code")),
        (NativeEvalType::VectorFloat32, Datum::Null) => Ok(ScalarValue::VectorFloat32(None)),
        (NativeEvalType::VectorFloat32, Datum::VectorFloat32(value)) => Ok(
            ScalarValue::VectorFloat32(Some(value.clone().into_wire_raw())),
        ),
        _ => Err(BatchFailure::admission(
            "batch source literal differs from its declared carrier",
        )),
    }
}

fn native_vector_from_tikv(value: TikvVectorFloat32) -> LocalResult<VectorFloat32> {
    let dimensions = u32::try_from(value.value.len() / size_of::<f32>())
        .map_err(|_| binding("batch VectorFloat32 output dimension is out of range"))?;
    let mut encoded = Vec::with_capacity(size_of::<u32>() + value.value.len());
    encoded.extend_from_slice(&dimensions.to_le_bytes());
    encoded.extend_from_slice(&value.value);
    let (value, suffix) = deserialize_native_vector_float32(&encoded)
        .map_err(|_| binding("batch VectorFloat32 output is not representable"))?;
    debug_assert!(suffix.is_empty());
    Ok(value)
}
fn parts(node: &Expression, allow_pb: bool) -> Result<(&FieldType, &CollationInfo)> {
    let (field, collation) = parts_optional(node, allow_pb)?;
    Ok((
        field.ok_or_else(|| BatchFailure::admission("missing batch declaration"))?,
        collation,
    ))
}
fn parts_optional<'a>(
    node: impl Into<SourceNodeRef<'a>>,
    allow_pb: bool,
) -> Result<(Option<&'a FieldType>, &'a CollationInfo)> {
    let node = node.into();
    let (field, collation, pb) = match node.kind() {
        BorrowedNodeKind::Constant(value) => (
            value.ret_type.as_ref(),
            &value.collation,
            value.pb_origin().is_some(),
        ),
        BorrowedNodeKind::Column(value) => (
            value.ret_type.as_ref(),
            &value.collation,
            value.pb_origin().is_some(),
        ),
        BorrowedNodeKind::ScalarFunction(value) => (
            value.ret_type.as_ref(),
            &value.collation,
            value.pb_origin().is_some(),
        ),
        BorrowedNodeKind::CorrelatedColumn(value) => (
            value.column.ret_type.as_ref(),
            &value.column.collation,
            value.column.pb_origin().is_some(),
        ),
    };
    if pb {
        if !allow_pb {
            return Err(BatchFailure::admission("PB state inside batch source"));
        }
    }
    Ok((field, collation))
}
fn check_batch_call(function: &ScalarFunction, view: NativeView) -> Result<()> {
    let expected_arity = resolve_call_in_view(function, view)?.1;
    if source_data_arity_in_view(function, view) != expected_arity
        || (function.pb_signature().is_some()
            && !pb_timestamp_diff(function)
            && !pb_value_owner(function))
        || (function.pb_origin().is_some()
            && !pb_timestamp_diff(function)
            && !pb_value_owner(function))
        || (function.has_values_offset() && !native_stateful_call(function))
        || (function.has_grouping_metadata() && !native_grouping_call(function))
    {
        return Err(BatchFailure::admission("outside native batch call closure"));
    }
    Ok(())
}
// The native CAST island is closed over existing plain value kernels. JSON
// additionally retains its typed source conversion and pre-argument admission
// in the descriptor-driven host, rather than using an untyped cast shortcut.
fn native_cast_name(function: &ScalarFunction) -> Option<&str> {
    let target = function.func_name.lowercase().strip_prefix("cast_")?;
    crate::scalar_function::prefix_value::ordinary_cast_target(target).then_some(target)
}

fn native_arithmetic_operation(name: &str) -> Option<SqlBatchHostOperation> {
    Some(match name {
        "plus" => SqlBatchHostOperation::AddNative,
        "minus" => SqlBatchHostOperation::SubtractNative,
        "mul" => SqlBatchHostOperation::MultiplyNative,
        "mod" => SqlBatchHostOperation::ModuloNative,
        "div" => SqlBatchHostOperation::DivideNative,
        "intdiv" => SqlBatchHostOperation::IntegerDivideNative,
        _ => return None,
    })
}
fn arithmetic_subject_required(function: &ScalarFunction, arg: &Expression) -> bool {
    (function.func_name.lowercase() == "intdiv"
        && arg
            .static_type()
            .is_some_and(|field| field.eval_type() == NativeEvalType::Real))
        || function.native_arithmetic_generic_mode()
            == crate::scalar_function::NativeArithmeticInputMode::Numeric(NativeEvalType::Decimal)
            && arg
                .static_type()
                .is_some_and(|field| field.eval_type() != NativeEvalType::Decimal)
            && !matches!(arg,Expression::Constant(constant) if matches!(constant.value,Datum::BinaryLiteral(_)))
}
fn native_math_operation(function: &ScalarFunction) -> Option<SqlBatchHostOperation> {
    (function.func_name.lowercase() == "abs").then_some(SqlBatchHostOperation::AbsNative)
}
fn native_comparison_kind(name: &str) -> Option<tidb_query_expr::ComparisonOp> {
    use tidb_query_expr::ComparisonOp;
    Some(match name {
        "eq" => ComparisonOp::Eq,
        "ne" => ComparisonOp::Ne,
        "lt" => ComparisonOp::Lt,
        "le" => ComparisonOp::Le,
        "gt" => ComparisonOp::Gt,
        "ge" => ComparisonOp::Ge,
        _ => return None,
    })
}
fn native_comparison_operation(name: &str) -> Option<SqlBatchHostOperation> {
    if name == "nullif" {
        Some(SqlBatchHostOperation::NullIfNative)
    } else if name == "nulleq" {
        Some(SqlBatchHostOperation::NullSafeEqNative)
    } else {
        native_comparison_kind(name).map(SqlBatchHostOperation::CompareNative)
    }
}
fn native_boolean_operation(name: &str) -> Option<SqlBatchHostOperation> {
    match name {
        "not" => Some(SqlBatchHostOperation::UnaryNotNative),
        "isnull" => Some(SqlBatchHostOperation::IsNullNative),
        "istrue" => Some(SqlBatchHostOperation::IsTrueNative),
        "isfalse" => Some(SqlBatchHostOperation::IsFalseNative),
        "isnottrue" => Some(SqlBatchHostOperation::IsNotTrueNative),
        "isnotfalse" => Some(SqlBatchHostOperation::IsNotFalseNative),
        "isunknown" => Some(SqlBatchHostOperation::IsUnknownNative),
        "isnotunknown" => Some(SqlBatchHostOperation::IsNotUnknownNative),
        "istrue_with_null" => Some(SqlBatchHostOperation::IsTrueWithNullNative),
        _ => None,
    }
}
fn native_selection_operation(function: &ScalarFunction) -> Option<SqlBatchHostOperation> {
    match function.func_name.lowercase() {
        "coalesce" => Some(SqlBatchHostOperation::CoalesceNative),
        "ifnull" => Some(SqlBatchHostOperation::IfNullNative),
        "if" => Some(SqlBatchHostOperation::IfNative),
        "case" => Some(SqlBatchHostOperation::CaseNative),
        "and" | "logicand" => Some(SqlBatchHostOperation::AndNative),
        "or" | "logicor" => Some(SqlBatchHostOperation::OrNative),
        "xor" => Some(SqlBatchHostOperation::XorNative),
        name => native_boolean_operation(name),
    }
}
fn native_unary_sign_operation(name: &str) -> Option<SqlBatchHostOperation> {
    use tidb_query_expr::impl_op::NativeUnarySign;
    Some(SqlBatchHostOperation::UnarySignNative(match name {
        "unaryplus" => NativeUnarySign::Plus,
        "unaryminus" => NativeUnarySign::Minus,
        _ => return None,
    }))
}
fn native_bitwise_operation(name: &str) -> Option<SqlBatchHostOperation> {
    use tidb_query_expr::impl_op::NativeBitwiseOp as B;
    Some(SqlBatchHostOperation::BitwiseNative(match name {
        "bitand" => B::And,
        "bitor" => B::Or,
        "bitxor" => B::Xor,
        "leftshift" => B::LeftShift,
        "rightshift" => B::RightShift,
        "bitneg" => B::Neg,
        _ => return None,
    }))
}
/// Native CHAR's existing raw-value signature families. This is an admission
/// fact, independent of which extra value producers the unified compiler gains.
fn native_character_value_argument(function: &ScalarFunction) -> bool {
    native_cast_name(function).is_some()
        || native_epoch_call(function).is_some()
        || native_tso_call(function).is_some()
        || native_clock_call(function).is_some()
        || native_calendar_call(function).is_some()
        || native_duration_construction_call(function).is_some()
        || native_vector_call(function).is_some()
        || native_crypto_call(function).is_some()
        || native_compression_call(function).is_some()
        || native_byte_unary_call(function).is_some()
        || native_misc_call(function).is_some()
        || native_identity_call(function)
        || native_grouping_call(function)
        || native_like_call(function)
        || matches!(
            function.func_name.lowercase(),
            "bitand"
                | "bitor"
                | "bitxor"
                | "leftshift"
                | "rightshift"
                | "bitneg"
                | "coalesce"
                | "ifnull"
                | "if"
                | "case"
                | "and"
                | "logicand"
                | "or"
                | "logicor"
                | "xor"
                | "not"
                | "isnull"
                | "istrue"
                | "isfalse"
                | "isnottrue"
                | "isnotfalse"
                | "isunknown"
                | "isnotunknown"
                | "istrue_with_null"
                | "eq"
                | "ne"
                | "lt"
                | "le"
                | "gt"
                | "ge"
                | "nulleq"
                | "nullif"
                | "abs"
                | "plus"
                | "minus"
                | "mul"
        )
}
fn pb_timestamp_diff(function: &ScalarFunction) -> bool {
    function.pb_signature() == Some(tidb_proto::tipb::ScalarFuncSig::TimestampDiff)
}
fn timestamp_diff_call(function: &ScalarFunction) -> bool {
    function.func_name.lowercase() == "timestampdiff" || pb_timestamp_diff(function)
}
fn native_like_call(function: &ScalarFunction) -> bool {
    matches!(function.func_name.lowercase(), "like" | "ilike") && function.args.len() == 3
}
fn native_grouping_call(function: &ScalarFunction) -> bool {
    function.func_name.lowercase() == "grouping" && function.args.len() == 1
}
fn native_vector_call(function: &ScalarFunction) -> Option<tidb_query_expr::NativeVectorOperation> {
    use tidb_query_expr::NativeVectorOperation as O;
    let op = match function.func_name.lowercase() {
        "vec_dims" => O::Dims,
        "vec_l1_distance" => O::L1Distance,
        "vec_l2_distance" => O::L2Distance,
        "vec_negative_inner_product" => O::NegativeInnerProduct,
        "vec_cosine_distance" => O::CosineDistance,
        "vec_l2_norm" => O::L2Norm,
        "vec_from_text" => O::FromText,
        "vec_as_text" => O::AsText,
        _ => return None,
    };
    (function.args.len() == op.arity()).then_some(op)
}
fn native_duration_construction_call(
    function: &ScalarFunction,
) -> Option<tidb_query_expr::NativeDurationConstructionOperation> {
    use tidb_query_expr::NativeDurationConstructionOperation as O;
    let op = match function.func_name.lowercase() {
        "addtime" | "subtime" if function.args.len() == 2 => {
            let kinds = [
                crate::time_fn::add_sub::kind_from_field_type(function.args[0].static_type()?),
                crate::time_fn::add_sub::kind_from_field_type(function.args[1].static_type()?),
            ];
            let row_path = function
                .args
                .iter()
                .all(|arg| matches!(arg, Expression::Constant(_)));
            if function.func_name.lowercase() == "subtime" {
                O::SubTime { kinds, row_path }
            } else {
                O::AddTime { kinds, row_path }
            }
        }
        "timestamp" if function.args.len() == 1 => O::Timestamp1,
        "timestamp" if function.args.len() == 2 => O::Timestamp2,
        "timestampadd" => O::TimestampAdd,
        "date_format" => O::DateFormat,
        "format_bytes" => O::FormatBytes,
        "format_nano_time" => O::FormatNanoTime,
        "microsecond" => O::Microsecond,
        "time" => O::Time,
        "timediff" => O::TimeDiff,
        "time_to_sec" => O::TimeToSec,
        "sec_to_time" => O::SecToTime,
        "time_format" => O::TimeFormat,
        "makedate" => O::MakeDate,
        "maketime" => O::MakeTime,
        "from_days" => O::FromDays,
        "period_add" => O::PeriodAdd,
        "period_diff" => O::PeriodDiff,
        "get_format" => O::GetFormat,
        "datediff" => O::DateDiff,
        "convert_tz" => O::ConvertTz,
        _ => return None,
    };
    (function.args.len() == op.arity()).then_some(op)
}
fn native_calendar_call(
    function: &ScalarFunction,
) -> Option<(tidb_query_expr::NativeCalendarOperation, usize)> {
    use tidb_query_expr::NativeCalendarOperation as O;
    let op = match function.func_name.lowercase() {
        "date" => O::Date,
        "year" => O::Year,
        "month" => O::Month,
        "day" => O::Day,
        "dayofmonth" => O::Day,
        "quarter" => O::Quarter,
        "hour" => O::Hour,
        "minute" => O::Minute,
        "second" => O::Second,
        "dayofweek" => O::DayOfWeek,
        "dayofyear" => O::DayOfYear,
        "weekday" => O::Weekday,
        "monthname" => O::MonthName,
        "dayname" => O::DayName,
        "weekofyear" => O::WeekOfYear,
        "week" => O::Week,
        "yearweek" => O::YearWeek,
        "last_day" => O::LastDay,
        "to_days" => O::ToDays,
        "to_seconds" => O::ToSeconds,
        _ => return None,
    };
    op.accepts_arity(function.args.len())
        .then_some((op, function.args.len()))
}
fn native_clock_call(
    function: &ScalarFunction,
) -> Option<(tidb_query_expr::NativeClockOperation, usize)> {
    use tidb_query_expr::NativeClockOperation as Op;
    let op = match function.func_name.lowercase() {
        "now" | "current_timestamp" | "localtime" | "localtimestamp" => Op::Now,
        "utc_timestamp" => Op::UtcTimestamp,
        "curdate" | "current_date" => Op::CurrentDate,
        "utc_date" => Op::UtcDate,
        "curtime" => Op::Curtime,
        "current_time" => Op::CurrentTime,
        "utc_time" => Op::UtcTime,
        "sysdate" => Op::Sysdate,
        _ => return None,
    };
    let arity = function.args.len();
    op.accepts_arity(arity).then_some((op, arity))
}
fn native_tso_call(function: &ScalarFunction) -> Option<tidb_query_expr::NativeTsoOperation> {
    use tidb_query_expr::NativeTsoOperation as Op;
    let op = match function.func_name.lowercase() {
        "tidb_current_tso" => Op::Current,
        "tidb_parse_tso" => Op::Physical,
        "tidb_parse_tso_logical" => Op::Logical,
        _ => return None,
    };
    (function.args.len() == op.arity()).then_some(op)
}
fn native_epoch_call(function: &ScalarFunction) -> Option<(bool, usize)> {
    let from = match function.func_name.lowercase() {
        "unix_timestamp" => false,
        "from_unixtime" => true,
        _ => return None,
    };
    let arity = function.args.len();
    (if from {
        (1..=2).contains(&arity)
    } else {
        arity <= 1
    })
    .then_some((from, arity))
}
#[derive(Clone, Copy)]
enum NativeExtractCall {
    Scalar,
    Composite(&'static str),
}
impl NativeExtractCall {
    fn unit(self) -> Option<&'static str> {
        match self {
            Self::Scalar => None,
            Self::Composite(unit) => Some(unit),
        }
    }
    fn arity(self) -> usize {
        if self.unit().is_some() {
            1
        } else {
            2
        }
    }
}
fn native_extract_call(function: &ScalarFunction) -> Option<NativeExtractCall> {
    let call = match function.func_name.lowercase() {
        "extract" => NativeExtractCall::Scalar,
        "year_month" => NativeExtractCall::Composite("YEAR_MONTH"),
        "day_hour" => NativeExtractCall::Composite("DAY_HOUR"),
        "day_minute" => NativeExtractCall::Composite("DAY_MINUTE"),
        "day_second" => NativeExtractCall::Composite("DAY_SECOND"),
        "day_microsecond" => NativeExtractCall::Composite("DAY_MICROSECOND"),
        "hour_minute" => NativeExtractCall::Composite("HOUR_MINUTE"),
        "hour_second" => NativeExtractCall::Composite("HOUR_SECOND"),
        "hour_microsecond" => NativeExtractCall::Composite("HOUR_MICROSECOND"),
        "minute_second" => NativeExtractCall::Composite("MINUTE_SECOND"),
        "minute_microsecond" => NativeExtractCall::Composite("MINUTE_MICROSECOND"),
        "second_microsecond" => NativeExtractCall::Composite("SECOND_MICROSECOND"),
        _ => return None,
    };
    (function.args.len() == call.arity()).then_some(call)
}
fn native_date_arithmetic_call(function: &ScalarFunction) -> Option<(&str, i64)> {
    if function.args.len() != 2 {
        return None;
    }
    let name = function.func_name.lowercase();
    name.strip_prefix("date_add_")
        .map(|unit| (unit, 1))
        .or_else(|| name.strip_prefix("date_sub_").map(|unit| (unit, -1)))
}
fn native_optional_date_arithmetic_call(function: &ScalarFunction) -> bool {
    function.pb_signature().is_none()
        && function.pb_origin().is_none()
        && native_date_arithmetic_call(function).is_some()
        && (function.get_static_type().is_none()
            || function.args.iter().any(|arg| arg.static_type().is_none()))
}

fn native_charset_operation(name: &str) -> Option<tidb_query_expr::NativeCharsetOperation> {
    use tidb_query_expr::NativeCharsetOperation as Op;
    Some(match name {
        "to_binary" => Op::ToBinary,
        "from_binary" => Op::FromBinary,
        "convert_using" => Op::ConvertUsing,
        _ => return None,
    })
}
fn native_crypto_call(function: &ScalarFunction) -> Option<tidb_query_expr::NativeCryptoOperation> {
    crate::tikv::crypto_value::operation(function.func_name.lowercase())
        .filter(|op| op.arity() == function.args.len())
}
fn native_compression_call(
    function: &ScalarFunction,
) -> Option<tidb_query_expr::NativeCompressionOperation> {
    (function.args.len() == 1)
        .then(|| crate::tikv::compression_value::operation(function.func_name.lowercase()))
        .flatten()
}
fn native_byte_unary_call(
    function: &ScalarFunction,
) -> Option<tidb_query_expr::NativeByteUnaryOperation> {
    (function.args.len() == 1)
        .then(|| crate::tikv::byte_unary_value::operation(function.func_name.lowercase()))
        .flatten()
}
fn native_misc_call(function: &ScalarFunction) -> Option<tidb_query_expr::NativeMiscOperation> {
    crate::tikv::misc_value::operation(function.func_name.lowercase())
        .filter(|op| op.accepts_arity(function.args.len()))
}
fn native_identity_call(function: &ScalarFunction) -> bool {
    matches!(
        (function.func_name.lowercase(), function.args.len()),
        ("any_value", 1) | ("name_const", 2)
    )
}
fn native_json_schema_call(function: &ScalarFunction) -> bool {
    function.func_name.lowercase() == "json_schema_valid"
        && function.pb_signature().is_none()
        && function.pb_origin().is_none()
}
fn native_final_state_call(function: &ScalarFunction) -> bool {
    matches!(
        function.func_name.lowercase(),
        "rand" | "tidb_is_ddl_owner" | "aes_encrypt" | "aes_decrypt"
    ) && function.pb_signature().is_none()
        && function.pb_origin().is_none()
}
fn native_stateful_call(function: &ScalarFunction) -> bool {
    crate::scalar_function::stateful::Operation::from_name(function.func_name.lowercase()).is_some()
        && function.pb_signature().is_none()
        && function.pb_origin().is_none()
}
fn native_cached_list_call(function: &ScalarFunction) -> bool {
    function.func_name.lowercase() == "find_in_set"
        && function.args.len() == 2
        && function.pb_signature().is_none()
        && function.pb_origin().is_none()
}
// Closed value owners retain actual optional metadata and their own demand
// policy, including the distinct stateful, schema and lazy AES continuations.
// The decoder's selected implementation is independent of display spelling.
fn pb_regexp_owner(function: &ScalarFunction) -> bool {
    function.pb_signature() == Some(tidb_proto::tipb::ScalarFuncSig::RegexpLikeSig)
}
fn pb_scalar_binary_domain(function: &ScalarFunction, view: NativeView) -> Option<NativeEvalType> {
    if pb_value_unary_view(function, view) || pb_value_arithmetic_view(function, view) {
        return None;
    }
    function
        .pb_binary_values()?
        .domain()
        .filter(|_| function.args.len() == 2)
}
fn native_pb_eager_values(function: &ScalarFunction) -> bool {
    function.pb_eager_values().is_some()
        || function.pb_json_signature().is_some()
        || function.pb_cast_values().is_some()
}
fn pb_value_owner(function: &ScalarFunction) -> bool {
    pb_regexp_owner(function)
        || function.pb_cast_values().is_some()
        || function.pb_lazy_values().is_some()
        || function.pb_boolean_values().is_some()
        || function.pb_binary_values().is_some()
        || function.pb_json_signature().is_some()
        || function
            .pb_eager_values()
            .is_some_and(|owner| Some(owner.signature()) == function.pb_signature())
}
// A real bulk consumer can select the old unary batch body even though the
// same PB expression has a scalar arity header. Retain both actual profiles.
fn pb_value_unary_view(function: &ScalarFunction, view: NativeView) -> bool {
    pb_value_owner(function) && window_unary_operation(function, view).is_some()
}
fn pb_value_unary_batch(root: &Expression) -> Option<NativeEvalType> {
    let Expression::ScalarFunction(function) = root else {
        return None;
    };
    if !pb_value_owner(function) {
        return None;
    }
    let target = crate::scalar_function::static_batch_target(root)?;
    pb_value_unary_view(function, NativeView::BulkRoot(target)).then_some(target)
}
fn pb_value_arithmetic_view(function: &ScalarFunction, view: NativeView) -> bool {
    pb_value_owner(function)
        && function.args.len() == 2
        && window_arithmetic_operation(function, view).is_some()
}
fn pb_value_arithmetic_batch(root: &Expression) -> Option<NativeEvalType> {
    let Expression::ScalarFunction(function) = root else {
        return None;
    };
    if !pb_value_owner(function) {
        return None;
    }
    let target = crate::scalar_function::static_batch_target(root)?;
    pb_value_arithmetic_view(function, NativeView::BulkRoot(target)).then_some(target)
}
fn pb_value_row_batch(root: &Expression) -> bool {
    matches!(root,Expression::ScalarFunction(function) if pb_value_owner(function)
        && !function.has_values_offset() && !function.has_grouping_metadata()
        && crate::scalar_function::static_batch_target(root).is_some())
}
// The original generic integer batch presents each scalar row as EvalInt
// after its own result conversion. Keep this consumer policy separate from PB.
fn pb_value_integer_batch(root: &Expression) -> bool {
    pb_value_row_batch(root)
        && !crate::scalar_function::native_scalar_row_batch(root)
        && root
            .static_type()
            .is_some_and(|field| field.eval_type() == NativeEvalType::Int)
}
fn native_regexp_call(function: &ScalarFunction) -> bool {
    (pb_regexp_owner(function)
        && crate::scalar_function::regexp_value::accepts_arity(function.args.len()))
        || function.pb_signature().is_none()
            && function.pb_origin().is_none()
            && matches!(
                (function.func_name.lowercase(), function.args.len()),
                ("regexp", 2) | ("regexp_like", 2 | 3)
            )
}
fn native_regexp_source_value(function: &ScalarFunction, limits: CompileLimits) -> bool {
    native_regexp_call(function)
        && (pb_regexp_owner(function) || function.func_name.lowercase() == "regexp"
            || !matches!(
                resolve_call(function),
                Ok((
                    CallTarget::Host(SqlBatchHostOperation::RegexpLike { .. }),
                    _
                ))
            )
            || function.args.iter().any(|arg| {
                contains_context_value(arg, limits.max_depth, &mut { limits.max_nodes })
                    || matches!(arg,Expression::Constant(c) if c.literal_value().is_some_and(|value| !matches!(value,Datum::Null|Datum::String(_)|Datum::Bytes(_))))
            }))
}
// The original registered REGEXP_LIKE integer batch collects scalar row
// results. Its input values are still demanded by the original lazy phases.
fn native_regexp_row_batch(root: &Expression) -> bool {
    matches!(root,Expression::ScalarFunction(function) if native_regexp_call(function)
        && function.func_name.lowercase()=="regexp_like" && !function.has_values_offset() && !function.has_grouping_metadata()
        && crate::scalar_function::static_batch_target(root)==Some(NativeEvalType::Int))
}
fn native_weight_string_call(function: &ScalarFunction) -> bool {
    crate::scalar_function::weight_value::supports(function)
}
fn source_data_arity_in_view(function: &ScalarFunction, view: NativeView) -> usize {
    if pb_value_unary_view(function, view) {
        1
    } else if pb_value_arithmetic_view(function, view) {
        2
    } else {
        source_data_arity(function)
    }
}
fn source_data_arity(function: &ScalarFunction) -> usize {
    if let Some(kernel) = function.pb_lazy_values() {
        return kernel.data_arity(function.args.len());
    }

    if let Some(kernel) = function.pb_boolean_values() {
        return kernel.data_arity(function.args.len());
    }

    if function
        .pb_binary_values()
        .is_some_and(|kernel| kernel.domain().is_none())
    {
        return function.args.len().min(2);
    }
    if native_weight_string_call(function) {
        crate::scalar_function::weight_value::data_arity(
            function.args.len(),
            function.args[0].static_type(),
        )
    } else {
        function.args.len()
    }
}
fn native_optional_value_call(function: &ScalarFunction) -> bool {
    function.pb_signature().is_none()
        && function.pb_origin().is_none()
        && (crate::scalar_function::eager_values::supports(function)
            || native_weight_string_call(function)
            || native_regexp_call(function)
            || crate::scalar_function::prefix_value::PrefixKind::for_function(function).is_some()
            || native_stateful_call(function)
            || native_final_state_call(function)
            || native_json_schema_call(function)
            || native_cached_list_call(function))
        && (function.get_static_type().is_none()
            || function.args.iter().any(|a| a.static_type().is_none()))
}
// Selection owns raw argument demands and optional result coercion. Protocol
// aliases that belong to the common scalar body retain that original owner.
fn native_optional_selection_call(function: &ScalarFunction) -> bool {
    function.pb_signature().is_none()
        && function.pb_origin().is_none()
        && !crate::scalar_function::eager_values::supports(function)
        && native_selection_operation(function).is_some_and(|op| {
            op != SqlBatchHostOperation::XorNative && op.accepts_arity(function.args.len())
        })
        && (function.get_static_type().is_none()
            || function.args.iter().any(|a| a.static_type().is_none()))
}
// These raw/comparison owners read genuine optional operand facts. Absence
// must remain distinct from a declared signed operand for literal coercion.
fn native_optional_operator_call(function: &ScalarFunction) -> bool {
    function.pb_signature().is_none()
        && function.pb_origin().is_none()
        && !crate::scalar_function::eager_values::supports(function)
        && native_unary_sign_operation(function.func_name.lowercase())
            .or_else(|| native_bitwise_operation(function.func_name.lowercase()))
            .or_else(|| {
                (function.func_name.lowercase() == "xor")
                    .then_some(SqlBatchHostOperation::XorNative)
            })
            .or_else(|| native_comparison_operation(function.func_name.lowercase()))
            .is_some_and(|op| op.accepts_arity(function.args.len()))
        && (function.get_static_type().is_none()
            || function.args.iter().any(|a| a.static_type().is_none()))
}
fn native_concat_value_call(function: &ScalarFunction) -> bool {
    function.pb_signature().is_none()
        && function.pb_origin().is_none()
        && match function.func_name.lowercase() {
            "concat" => !function.args.is_empty(),
            "concat_ws" => function.args.len() >= 2,
            _ => false,
        }
}
// The original batch entry keeps these argument demands row based and
// publishes their results only after the complete invocation succeeds.
fn native_concat_row_batch(function: &ScalarFunction) -> bool {
    native_concat_value_call(function)
        && !function.has_values_offset()
        && !function.has_grouping_metadata()
        && function
            .get_static_type()
            .is_some_and(|field| field.eval_type() == NativeEvalType::String)
        && batch_call_signature(function)
            .is_some_and(|signature| signature.output_type() == TikvEvalType::Bytes)
        && function
            .args
            .iter()
            .any(|arg| matches!(arg,Expression::Constant(c) if c.param_marker.is_some()))
        && function.args.iter().all(|arg| {
            arg.static_type()
                .is_some_and(|field| field.eval_type() == NativeEvalType::String)
                && match arg {
                    Expression::Constant(c) => c.deferred_expr.is_none(),
                    Expression::Column(_) => true,
                    _ => false,
                }
        })
}
// These original declared-string batch entries collect ordinary row results;
// lazy child choices remain inside their existing value owners. The genuine
// native batch eligibility also admits nested and deferred typed arguments.
fn native_string_row_batch(root: &Expression) -> bool {
    let Expression::ScalarFunction(function) = root else {
        return false;
    };
    matches!(
        function.func_name.lowercase(),
        "any_value" | "lower" | "upper" | "ifnull" | "coalesce"
    ) && function.pb_signature().is_none()
        && function.pb_origin().is_none()
        && !function.has_values_offset()
        && !function.has_grouping_metadata()
        && crate::scalar_function::static_batch_target(root) == Some(NativeEvalType::String)
}
// LIKE retains lazy raw operands and the actual optional escape cast fact.
fn native_optional_wildcard_call(function: &ScalarFunction) -> bool {
    native_like_call(function)
        && function.pb_signature().is_none()
        && function.pb_origin().is_none()
        && (function.get_static_type().is_none()
            || function.args.iter().any(|arg| arg.static_type().is_none()))
}
// GROUPING retains its raw-id continuation, including NULL before metadata.
fn native_optional_grouping_call(function: &ScalarFunction) -> bool {
    native_grouping_call(function)
        && function.pb_signature().is_none()
        && function.pb_origin().is_none()
        && (function.get_static_type().is_none() || function.args[0].static_type().is_none())
}
fn native_no_argument_header_call(function: &ScalarFunction) -> bool {
    crate::scalar_function::header_value::HeaderKind::for_function(function).is_some()
}
// Retain the actual optional declarations of this original arithmetic call.
fn native_optional_arithmetic_call(function: &ScalarFunction) -> bool {
    function.pb_signature().is_none()
        && function.pb_origin().is_none()
        && function.args.len() == 2
        && native_arithmetic_operation(function.func_name.lowercase()).is_some()
        && (function.get_static_type().is_none()
            || function.args.iter().any(|arg| arg.static_type().is_none()))
}
// These existing owners consume genuine optional fields in their own phases.
fn native_optional_ordered_comparison_call(function: &ScalarFunction) -> bool {
    function.pb_signature().is_none()
        && function.pb_origin().is_none()
        && match function.func_name.lowercase() {
            "in" | "interval" => function.args.len() >= 2,
            "greatest" | "least" => !function.args.is_empty(),
            _ => false,
        }
        && (function.get_static_type().is_none()
            || function.args.iter().any(|arg| arg.static_type().is_none()))
}
// Missing argument types can stop diagnostic text partway through its real
// demand. Keep that rendering in the existing owned numeric continuation.
fn native_missing_arithmetic_sources(function: &ScalarFunction) -> bool {
    native_optional_arithmetic_call(function)
        && function.args.iter().any(|arg| arg.static_type().is_none())
}
fn native_optional_declarations_call(function: &ScalarFunction) -> bool {
    function.pb_lazy_values().is_some()
        || function.pb_boolean_values().is_some()
        || function.pb_binary_values().is_some()
        || native_pb_eager_values(function)
        || native_optional_value_call(function)
        || native_optional_selection_call(function)
        || native_optional_operator_call(function)
        || native_optional_grouping_call(function)
        || native_optional_wildcard_call(function)
        || native_concat_value_call(function)
        || native_optional_arithmetic_call(function)
        || native_optional_ordered_comparison_call(function)
        || native_optional_date_arithmetic_call(function)
        || native_no_argument_header_call(function)
}
fn native_value_call(function: &ScalarFunction) -> bool {
    function.pb_lazy_values().is_some()
        || function.pb_boolean_values().is_some()
        || function.pb_binary_values().is_some()
        || native_pb_eager_values(function)
        || native_concat_value_call(function)
        || native_no_argument_header_call(function)
        || native_json_schema_call(function)
        || native_final_state_call(function)
        || native_stateful_call(function)
        || native_cached_list_call(function)
        || crate::scalar_function::eager_values::supports(function)
        || native_weight_string_call(function)
        || native_regexp_call(function)
        || crate::scalar_function::prefix_value::PrefixKind::for_function(function).is_some()
        || matches!(
            function.func_name.lowercase(),
            "json_sum_crc32"
                | "str_to_date"
                | "timestampdiff"
                | "sig_timestampdiff"
                | "tidb_bounded_staleness"
        )
        || native_epoch_call(function).is_some()
        || native_tso_call(function).is_some()
        || native_clock_call(function).is_some()
        || native_calendar_call(function).is_some()
        || native_duration_construction_call(function).is_some()
        || native_vector_call(function).is_some()
        || native_crypto_call(function).is_some()
        || native_compression_call(function).is_some()
        || native_byte_unary_call(function).is_some()
        || native_misc_call(function).is_some()
        || native_identity_call(function)
        || native_grouping_call(function)
        || native_like_call(function)
        || native_extract_call(function).is_some()
        || native_date_arithmetic_call(function).is_some()
        || native_charset_operation(function.func_name.lowercase()).is_some()
        || matches!(
            function.func_name.lowercase(),
            "in" | "interval" | "greatest" | "least"
        )
        || native_unary_sign_operation(function.func_name.lowercase()).is_some()
        || native_arithmetic_operation(function.func_name.lowercase()).is_some()
        || native_character_value_argument(function)
}
fn host_has_embedded_probe(operation: SqlBatchHostOperation) -> bool {
    matches!(
        operation,
        SqlBatchHostOperation::EagerValuesNative
            | SqlBatchHostOperation::CryptoNative(_)
            | SqlBatchHostOperation::CompressionNative(_)
            | SqlBatchHostOperation::ByteUnaryNative(_)
            | SqlBatchHostOperation::MiscNative { .. }
            | SqlBatchHostOperation::ValueIdentityNative { .. }
    )
}
fn source_prefix_probe<'a>(
    function: &'a ScalarFunction,
    operation: SqlBatchHostOperation,
) -> Option<&'a Datum> {
    if pb_value_owner(function) {
        return None;
    }
    if native_final_state_call(function)
        || native_weight_string_call(function)
        || native_regexp_call(function)
    {
        return function.identity_constant_probe();
    }
    if host_has_embedded_probe(operation)
        || crate::scalar_function::argument_policy::classify(function)
            == crate::scalar_function::argument_policy::ArgumentPolicy::BeforeNameProbe
    {
        None
    } else {
        function.identity_constant_probe()
    }
}
fn resolve_call_in_view(
    function: &ScalarFunction,
    view: NativeView,
) -> Result<(CallTarget, usize)> {
    if pb_value_unary_view(function, view) {
        return Ok((
            CallTarget::Host(
                window_unary_operation(function, view).expect("actual unary bulk consumer"),
            ),
            1,
        ));
    }
    if pb_value_arithmetic_view(function, view) {
        return Ok((
            CallTarget::Host(
                window_arithmetic_operation(function, view)
                    .expect("actual arithmetic bulk consumer"),
            ),
            2,
        ));
    }
    if view.value_role().is_some() && native_no_argument_header_call(function) {
        return Ok((
            CallTarget::Host(SqlBatchHostOperation::EagerValuesNative),
            0,
        ));
    }
    if view.value_role().is_some()
        && (function.pb_lazy_values().is_some()
            || function.pb_boolean_values().is_some()
            || function.pb_binary_values().is_some()
            || native_pb_eager_values(function)
            || native_regexp_call(function))
    {
        return Ok((
            CallTarget::Host(SqlBatchHostOperation::EagerValuesNative),
            source_data_arity(function),
        ));
    }
    if view.value_role().is_some() && native_weight_string_call(function) {
        return Ok((
            CallTarget::Host(SqlBatchHostOperation::EagerValuesNative),
            source_data_arity(function),
        ));
    }
    if view.value_role().is_some()
        && (native_optional_value_call(function)
            || native_weight_string_call(function)
            || crate::scalar_function::prefix_value::PrefixKind::for_function(function).is_some())
    {
        return Ok((
            CallTarget::Host(SqlBatchHostOperation::EagerValuesNative),
            function.args.len(),
        ));
    }
    let resolved = resolve_specific_call_in_view(function, view);
    let common = crate::scalar_function::eager_values::supports(function);
    // Protocol aliases are not scalar operator names. The original scalar
    // body eagerly demands their arguments and then reports Unsupported.
    let protocol_alias = common
        && crate::scalar_function::binary_op_for_name(function.func_name.lowercase()).is_none()
        && matches!(
            native_selection_operation(function),
            Some(SqlBatchHostOperation::AndNative | SqlBatchHostOperation::OrNative)
        );
    if protocol_alias && view.value_role().is_none() {
        return Err(BatchFailure::admission(
            "scalar protocol alias requires its original common body",
        ));
    }
    if view.value_role().is_some()
        && common
        && (protocol_alias
            || matches!(
                &resolved,
                Ok((CallTarget::Kernel(_), _))
                    | Err(BatchFailure {
                        kind: FailureKind::Admission(_)
                    })
            ))
    {
        if function.get_static_type().is_none()
            || function.args.iter().any(|a| a.static_type().is_none())
        {
            return Err(BatchFailure::admission(
                "common eager source requires original declarations",
            ));
        }
        return Ok((
            CallTarget::Host(SqlBatchHostOperation::EagerValuesNative),
            function.args.len(),
        ));
    }
    resolved
}
fn resolve_specific_call_in_view(
    function: &ScalarFunction,
    view: NativeView,
) -> Result<(CallTarget, usize)> {
    if (native_json_schema_call(function)
        || native_final_state_call(function)
        || native_stateful_call(function))
        && view.value_role().is_some()
    {
        return Ok((
            CallTarget::Host(SqlBatchHostOperation::EagerValuesNative),
            function.args.len(),
        ));
    }
    if native_cached_list_call(function) && view.value_role().is_some() {
        return Ok((
            CallTarget::Host(SqlBatchHostOperation::EagerValuesNative),
            2,
        ));
    }
    if crate::scalar_function::eager_values::prefers_common_body(function)
        && view.value_role().is_some()
    {
        if function.get_static_type().is_none()
            || function.args.iter().any(|a| a.static_type().is_none())
        {
            return Err(BatchFailure::admission(
                "eager values require original native declarations",
            ));
        }
        return Ok((
            CallTarget::Host(SqlBatchHostOperation::EagerValuesNative),
            function.args.len(),
        ));
    }

    if let Some(operation) = native_crypto_call(function).filter(|_| view.value_role().is_some()) {
        if function.get_static_type().is_none()
            || function.args.iter().any(|a| a.static_type().is_none())
        {
            return Err(BatchFailure::admission(
                "crypto requires original native declarations",
            ));
        }
        return Ok((
            CallTarget::Host(SqlBatchHostOperation::CryptoNative(operation)),
            function.args.len(),
        ));
    }
    if let Some(operation) =
        native_compression_call(function).filter(|_| view.value_role().is_some())
    {
        if function.get_static_type().is_none()
            || function.args.iter().any(|a| a.static_type().is_none())
        {
            return Err(BatchFailure::admission(
                "compression requires original native declarations",
            ));
        }
        return Ok((
            CallTarget::Host(SqlBatchHostOperation::CompressionNative(operation)),
            1,
        ));
    }
    if let Some(operation) =
        native_byte_unary_call(function).filter(|_| view.value_role().is_some())
    {
        if function.get_static_type().is_none()
            || function.args.iter().any(|a| a.static_type().is_none())
        {
            return Err(BatchFailure::admission(
                "byte unary requires original native declarations",
            ));
        }
        return Ok((
            CallTarget::Host(SqlBatchHostOperation::ByteUnaryNative(operation)),
            1,
        ));
    }
    if let Some(operation) = native_misc_call(function).filter(|_| view.value_role().is_some()) {
        if function.get_static_type().is_none()
            || function.args.iter().any(|a| a.static_type().is_none())
        {
            return Err(BatchFailure::admission(
                "misc requires original native declarations",
            ));
        }
        return Ok((
            CallTarget::Host(SqlBatchHostOperation::MiscNative {
                operation,
                arity: function.args.len() as u8,
            }),
            function.args.len(),
        ));
    }
    if native_identity_call(function) && view.value_role().is_some() {
        if function.get_static_type().is_none()
            || function.args.iter().any(|a| a.static_type().is_none())
        {
            return Err(BatchFailure::admission(
                "identity requires original native declarations",
            ));
        }
        return Ok((
            CallTarget::Host(SqlBatchHostOperation::ValueIdentityNative {
                name_const: function.func_name.lowercase() == "name_const",
            }),
            function.args.len(),
        ));
    }
    if native_concat_value_call(function) && view.value_role().is_some() {
        return Ok((
            CallTarget::Host(if function.func_name.lowercase() == "concat" {
                SqlBatchHostOperation::Concat
            } else {
                SqlBatchHostOperation::ConcatWs
            }),
            function.args.len(),
        ));
    }
    if native_like_call(function) {
        if view.value_role().is_none()
            || (!native_optional_wildcard_call(function)
                && (function.get_static_type().is_none()
                    || function.args.iter().any(|a| a.static_type().is_none())))
        {
            return Err(BatchFailure::admission(
                "LIKE requires original native declarations",
            ));
        }
        return Ok((
            CallTarget::Host(SqlBatchHostOperation::LikeNative {
                ilike: function.func_name.lowercase() == "ilike",
            }),
            3,
        ));
    }
    if native_grouping_call(function) {
        if view.value_role().is_none()
            || (!native_optional_grouping_call(function)
                && (function.get_static_type().is_none()
                    || function.args[0].static_type().is_none()))
        {
            return Err(BatchFailure::admission(
                "GROUPING requires original native declarations",
            ));
        }
        return Ok((CallTarget::Host(SqlBatchHostOperation::GroupingNative), 1));
    }
    if let Some(op) = native_vector_call(function) {
        if view.value_role().is_none()
            || function.get_static_type().is_none()
            || function.args.iter().any(|v| v.static_type().is_none())
        {
            return Err(BatchFailure::admission(
                "vector requires original native declarations",
            ));
        }
        return Ok((
            CallTarget::Host(SqlBatchHostOperation::VectorNative(op)),
            op.arity(),
        ));
    }
    if let Some(op) = native_duration_construction_call(function) {
        if view.value_role().is_none()
            || function.get_static_type().is_none()
            || function.args.iter().any(|v| v.static_type().is_none())
        {
            return Err(BatchFailure::admission(
                "duration/construction requires original native declarations",
            ));
        }
        return Ok((
            CallTarget::Host(SqlBatchHostOperation::DurationConstructionNative(op)),
            op.arity(),
        ));
    }
    if let Some((operation, arity)) = native_calendar_call(function) {
        if view.value_role().is_none()
            || function.get_static_type().is_none()
            || function.args.iter().any(|v| v.static_type().is_none())
        {
            return Err(BatchFailure::admission(
                "calendar requires original native declarations",
            ));
        }
        return Ok((
            CallTarget::Host(SqlBatchHostOperation::CalendarNative {
                operation,
                arity: arity as u8,
            }),
            arity,
        ));
    }
    if let Some((operation, arity)) = native_clock_call(function) {
        if view.value_role().is_none()
            || function.get_static_type().is_none()
            || function.args.iter().any(|v| v.static_type().is_none())
        {
            return Err(BatchFailure::admission(
                "clock requires original native value declarations",
            ));
        }
        return Ok((
            CallTarget::Host(SqlBatchHostOperation::ClockNative {
                operation,
                arity: arity as u8,
            }),
            arity,
        ));
    }
    if let Some(op) = native_tso_call(function) {
        if view.value_role().is_none()
            || function.get_static_type().is_none()
            || function.args.iter().any(|v| v.static_type().is_none())
        {
            return Err(BatchFailure::admission(
                "TSO requires original native value declarations",
            ));
        }
        return Ok((
            CallTarget::Host(SqlBatchHostOperation::TsoNative(op)),
            op.arity(),
        ));
    }
    if let Some((from, arity)) = native_epoch_call(function) {
        if view.value_role().is_none()
            || function.args.iter().any(|v| v.static_type().is_none())
            || function.get_static_type().is_none()
        {
            return Err(BatchFailure::admission(
                "timestamp source requires original native value declarations",
            ));
        }
        return Ok((
            CallTarget::Host(SqlBatchHostOperation::EpochNative {
                from,
                arity: arity as u8,
            }),
            arity,
        ));
    }
    if let Some(call) = native_extract_call(function) {
        if function.args.iter().any(|arg| arg.static_type().is_none()) {
            return Err(BatchFailure::admission(
                "EXTRACT source declarations absent",
            ));
        }
        let result = function
            .get_static_type()
            .ok_or_else(|| BatchFailure::admission("EXTRACT result declaration absent"))?;
        let typed = view.value_role().is_none();
        if typed && result.eval_type() != NativeEvalType::Int {
            return Err(BatchFailure::admission(
                "EXTRACT result carrier is not covered",
            ));
        }
        return Ok((
            CallTarget::Host(SqlBatchHostOperation::ExtractNative {
                typed,
                composite: call.unit().is_some(),
            }),
            call.arity(),
        ));
    }
    if native_date_arithmetic_call(function).is_some() {
        use tidb_query_expr::local::SqlTemporalOutput as Output;
        if view.value_role().is_none()
            && function.args.iter().any(|arg| arg.static_type().is_none())
        {
            return Err(BatchFailure::admission(
                "typed date arithmetic source declarations absent",
            ));
        }
        let result = function.get_static_type();
        let output = if view.value_role().is_some() {
            Output::Value
        } else {
            let result = result.ok_or_else(|| {
                BatchFailure::admission("typed date arithmetic result declaration absent")
            })?;
            match result.code() {
                FieldTypeCode::Date | FieldTypeCode::Datetime => Output::DateTime,
                FieldTypeCode::Duration => Output::Duration,
                _ if result.eval_type() == NativeEvalType::String => Output::Bytes,
                _ => {
                    return Err(BatchFailure::admission(
                        "date arithmetic result carrier is not covered",
                    ));
                }
            }
        };
        return Ok((
            CallTarget::Host(SqlBatchHostOperation::DateArithmeticNative { output }),
            2,
        ));
    }
    if function.func_name.lowercase() == "tidb_bounded_staleness" {
        if function.args.len() != 2
            || function.args.iter().any(|arg| arg.static_type().is_none())
            || !function.get_static_type().is_some_and(|field| {
                view.value_role().is_some()
                    || matches!(
                        field.eval_type(),
                        NativeEvalType::Datetime | NativeEvalType::Timestamp
                    )
            })
        {
            return Err(BatchFailure::admission(
                "bounded staleness requires complete declarations and exact arity",
            ));
        }
        return Ok((
            CallTarget::Host(SqlBatchHostOperation::BoundedStalenessNative {
                typed: view.value_role().is_none(),
            }),
            2,
        ));
    }
    if timestamp_diff_call(function) {
        if function.args.len() != 3
            || function.args.iter().any(|arg| arg.static_type().is_none())
            || !function.get_static_type().is_some_and(|field| {
                view.value_role().is_some() || field.eval_type() == NativeEvalType::Int
            })
        {
            return Err(BatchFailure::admission(
                "TIMESTAMPDIFF requires complete declarations and exact arity",
            ));
        }
        return Ok((
            CallTarget::Host(SqlBatchHostOperation::TimestampDiffNative {
                typed: view.value_role().is_none(),
                stop_on_null: pb_timestamp_diff(function),
            }),
            3,
        ));
    }
    if function.func_name.lowercase() == "str_to_date" {
        use tidb_query_expr::local::SqlTemporalOutput as Output;
        if function.args.len() != 2
            || function
                .args
                .iter()
                .any(|argument| argument.static_type().is_none())
        {
            return Err(BatchFailure::admission(
                "date source requires complete declarations and exact arity",
            ));
        }
        let result = function
            .get_static_type()
            .ok_or_else(|| BatchFailure::admission("date result declaration absent"))?;
        let output = if view.value_role().is_some() {
            Output::Value
        } else {
            match result.eval_type() {
                NativeEvalType::String => Output::Bytes,
                NativeEvalType::Datetime | NativeEvalType::Timestamp => Output::DateTime,
                NativeEvalType::Duration => Output::Duration,
                _ => {
                    return Err(BatchFailure::admission(
                        "date typed result family is outside its signature",
                    ));
                }
            }
        };
        return Ok((
            CallTarget::Host(SqlBatchHostOperation::StrToDateNative { output }),
            2,
        ));
    }
    if function.func_name.lowercase() == "json_sum_crc32" {
        if function.args.len() != 1
            || function.args[0].static_type().is_none()
            || !function.get_static_type().is_some_and(|field| {
                view.value_role().is_some() || field.eval_type() == NativeEvalType::Int
            })
        {
            return Err(BatchFailure::admission(
                "JSON checksum requires complete declarations and exact arity",
            ));
        }
        return Ok((
            CallTarget::Host(SqlBatchHostOperation::JsonSumCrc32Native {
                typed: view.value_role().is_none(),
            }),
            1,
        ));
    }
    if let Some(operation) = native_charset_operation(function.func_name.lowercase()) {
        if function.args.len() != operation.arity()
            || function.args.iter().any(|arg| arg.static_type().is_none())
            || !function.get_static_type().is_some_and(|field| {
                view.value_role().is_some() || field.eval_type() == NativeEvalType::String
            })
        {
            return Err(BatchFailure::admission(
                "charset source requires complete declarations and exact arity",
            ));
        }
        return Ok((
            CallTarget::Host(SqlBatchHostOperation::CharsetNative(operation)),
            function.args.len(),
        ));
    }
    if view.value_role().is_some() {
        if let Some(operation) = window_value_operation(function, view)
            .or_else(|| native_unary_sign_operation(function.func_name.lowercase()))
            .or_else(|| native_bitwise_operation(function.func_name.lowercase()))
            .or_else(|| native_selection_operation(function))
            .or_else(|| native_comparison_operation(function.func_name.lowercase()))
            .or_else(|| native_math_operation(function))
            .or_else(|| native_arithmetic_operation(function.func_name.lowercase()))
            .or_else(|| {
                (function.func_name.lowercase() == "in").then_some(SqlBatchHostOperation::InNative)
            })
            .or_else(|| {
                (function.func_name.lowercase() == "interval")
                    .then_some(SqlBatchHostOperation::IntervalNative)
            })
            .or_else(|| {
                matches!(function.func_name.lowercase(), "greatest" | "least")
                    .then_some(SqlBatchHostOperation::ExtremumNative)
            })
        {
            if !operation.accepts_arity(function.args.len())
                || ((function.get_static_type().is_none()
                    || function.args.iter().any(|arg| arg.static_type().is_none()))
                    && !native_optional_selection_call(function)
                    && !native_optional_operator_call(function)
                    && !native_optional_arithmetic_call(function)
                    && !native_optional_ordered_comparison_call(function))
            {
                return Err(BatchFailure::admission(
                    "native nullable control requires typed arguments and valid arity",
                ));
            }
            return Ok((CallTarget::Host(operation), function.args.len()));
        }
    }
    if view.value_role().is_some() && native_cast_name(function).is_some() {
        if function.args.len() != 1
            || function.get_static_type().is_none()
            || function.args[0].static_type().is_none()
        {
            return Err(BatchFailure::admission(
                "native CAST requires one typed argument and result",
            ));
        }
        return Ok((CallTarget::Host(SqlBatchHostOperation::CastNative), 1));
    }
    resolve_call(function)
}
#[derive(Clone, Copy)]
enum SignaturePurpose {
    NativeEligibility,
    CompiledSource,
}
fn resolve_call(function: &ScalarFunction) -> Result<(CallTarget, usize)> {
    resolve_call_for(function, SignaturePurpose::CompiledSource)
}
fn resolve_call_for(
    function: &ScalarFunction,
    purpose: SignaturePurpose,
) -> Result<(CallTarget, usize)> {
    let operation = match function.func_name.lowercase() {
        "concat" => Some(SqlBatchHostOperation::Concat),
        "concat_ws" => Some(SqlBatchHostOperation::ConcatWs),
        "cast_char" => {
            let len = function
                .get_static_type()
                .and_then(|field| u32::try_from(field.flen()).ok());
            if function
                .args
                .first()
                .and_then(Expression::static_type)
                .is_some_and(|field| {
                    field.eval_type() == NativeEvalType::Int && check_type(field).is_ok()
                        && function.args.first().is_some_and(|arg| match arg {
                            Expression::Constant(value) => matches!(value.literal_value(), Some(Datum::Null | Datum::Int(_)))
                                || matches!(value.literal_value(), Some(Datum::UInt(_)) if field.is_unsigned()),
                            Expression::ScalarFunction(function) if match purpose {
                                SignaturePurpose::NativeEligibility => native_character_value_argument(function),
                                SignaturePurpose::CompiledSource => native_value_call(function),
                            } => false,
                            _ => true,
                        })
                })
            {
                Some(SqlBatchHostOperation::CastCharInt { len })
            } else {
                Some(SqlBatchHostOperation::CastCharNative { len })
            }
        }
        "regexp_like" => Some(SqlBatchHostOperation::RegexpLike {
            case_insensitive: tidb_datatype::is_ci_collation(function.derived_collation().name()),
        }),
        _ => None,
    };
    if let Some(operation) = operation {
        let arity = function.args.len();
        let exact = operation.accepts_arity(arity)
            && function.get_static_type().is_some_and(|field| {
                field.eval_type()
                    == match operation {
                        SqlBatchHostOperation::RegexpLike { .. } => NativeEvalType::Int,
                        _ => NativeEvalType::String,
                    }
                    && !field.is_unsigned()
                    && (!matches!(operation, SqlBatchHostOperation::RegexpLike { .. })
                        || field.code() == FieldTypeCode::LongLong)
            })
            && function.args.iter().all(|arg| {
                arg.static_type().is_some_and(|field| {
                    if matches!(operation, SqlBatchHostOperation::CastCharNative { .. } | SqlBatchHostOperation::CastNative) {
                        matches!(arg, Expression::Column(_)) || matches!(arg, Expression::Constant(value) if value.literal_value().is_some()) || matches!(arg, Expression::ScalarFunction(function) if match purpose {
                            SignaturePurpose::NativeEligibility => native_character_value_argument(function),
                            SignaturePurpose::CompiledSource => native_value_call(function),
                        })
                    } else {
                        batch_eval_type(field.eval_type()) == operation.argument_type()
                            && (!field.is_unsigned() || field.eval_type() == NativeEvalType::Int)
                    }
                })
            });
        return exact
            .then_some((CallTarget::Host(operation), arity))
            .ok_or_else(|| {
                BatchFailure::admission(
                    "SQL host requires exact ready argument/result declarations",
                )
            });
    }
    // The compiled source graph gains the pure typed MONTH parent. Keep the
    // older native eligibility contract separate from source composition.
    if matches!(purpose, SignaturePurpose::NativeEligibility)
        && function.func_name.lowercase() == "month"
    {
        return Err(BatchFailure::admission(
            "MONTH is outside the legacy eligibility closure",
        ));
    }
    batch_fact(function).map(|(function, arity)| (CallTarget::Kernel(function), arity))
}
// Follows the registry's identity policy without evaluating the expression.
// Complete preflight bounds this path before snapshots/metadata are built.
fn native_int_is_unsigned(mut node: &Expression, limits: CompileLimits) -> Result<bool> {
    use tidb_query_expr::local::{batch_int_kind, BatchIntKind};
    for _ in 0..limits.max_depth {
        match node {
            Expression::Constant(value) => {
                return match value.literal_value() {
                    Some(Datum::Int(_)) => Ok(false),
                    Some(Datum::UInt(_)) => Ok(true),
                    Some(Datum::Null) => Ok(false),
                    _ => Err(BatchFailure::admission(
                        "integer result kind needs native value transport",
                    )),
                };
            }
            Expression::Column(column) => {
                return column
                    .get_static_type()
                    .map(FieldType::is_unsigned)
                    .ok_or_else(|| BatchFailure::admission("integer source has no declaration"));
            }
            Expression::ScalarFunction(function) => match resolve_call(function)?.0 {
                CallTarget::Kernel(function_ref) => match batch_int_kind(function_ref) {
                    Some(BatchIntKind::Signed) => return Ok(false),
                    Some(BatchIntKind::Unsigned) => return Ok(true),
                    Some(BatchIntKind::FirstArgument) => {
                        node = function
                            .args
                            .first()
                            .ok_or_else(|| binding("integer identity lost its argument"))?;
                    }
                    None => {
                        return Err(BatchFailure::admission(
                            "integer result kind has no registry policy",
                        ));
                    }
                },
                CallTarget::Host(operation) if operation.output_type() == TikvEvalType::Int => {
                    return Ok(false);
                }
                _ => {
                    return Err(BatchFailure::admission(
                        "integer result kind has no native policy",
                    ));
                }
            },
            Expression::CorrelatedColumn(_) => {
                return Err(BatchFailure::admission("correlated integer kind"));
            }
        }
    }
    Err(resource("integer kind metadata depth policy exceeded").into())
}

fn batch_fact(function: &ScalarFunction) -> Result<(FunctionRef, usize)> {
    let name = match function.func_name.lowercase() {
        "character_length" => "char_length",
        name => name,
    };
    let arithmetic = match name {
        "plus" => Some((tipb::ScalarFuncSig::PlusInt, 2)),
        "minus" => Some((tipb::ScalarFuncSig::MinusInt, 2)),
        "mul" => Some((tipb::ScalarFuncSig::MultiplyInt, 2)),
        "abs" => Some((tipb::ScalarFuncSig::AbsInt, 1)),
        _ => None,
    };
    if let Some((integer_signature, arity)) = arithmetic {
        let result_type = function.get_static_type().map(FieldType::eval_type);
        if result_type == Some(NativeEvalType::Int) {
            let exact = function.args.len() == arity
                && function
                    .get_static_type()
                    .is_some_and(|field| !field.is_unsigned())
                && function.args.iter().all(|arg| {
                    arg.static_type().is_some_and(|field| {
                        field.eval_type() == NativeEvalType::Int && !field.is_unsigned()
                    })
                });
            return exact
                .then_some((FunctionRef::TiPb(integer_signature), arity))
                .ok_or_else(|| {
                    BatchFailure::admission("integer batch call is not exact signed Int")
                });
        }
        if result_type == Some(NativeEvalType::Real) {
            let real_signature = match integer_signature {
                tipb::ScalarFuncSig::PlusInt => tipb::ScalarFuncSig::PlusReal,
                tipb::ScalarFuncSig::MinusInt => tipb::ScalarFuncSig::MinusReal,
                tipb::ScalarFuncSig::MultiplyInt => tipb::ScalarFuncSig::MultiplyReal,
                tipb::ScalarFuncSig::AbsInt => tipb::ScalarFuncSig::AbsReal,
                _ => unreachable!("closed arithmetic signature"),
            };
            let exact = function.args.len() == arity
                && function.args.iter().all(|arg| {
                    arg.static_type()
                        .is_some_and(|field| field.eval_type() == NativeEvalType::Real)
                });
            return exact
                .then_some((FunctionRef::TiPb(real_signature), arity))
                .ok_or_else(|| BatchFailure::admission("Real batch call is not exact Double"));
        }
        if result_type == Some(NativeEvalType::Decimal) {
            let decimal_signature = match integer_signature {
                tipb::ScalarFuncSig::PlusInt => tipb::ScalarFuncSig::PlusDecimal,
                tipb::ScalarFuncSig::MinusInt => tipb::ScalarFuncSig::MinusDecimal,
                tipb::ScalarFuncSig::MultiplyInt => tipb::ScalarFuncSig::MultiplyDecimal,
                tipb::ScalarFuncSig::AbsInt => tipb::ScalarFuncSig::AbsDecimal,
                _ => unreachable!("closed arithmetic signature"),
            };
            let exact = function.args.len() == arity
                && function.args.iter().all(|arg| {
                    arg.static_type()
                        .is_some_and(|field| field.eval_type() == NativeEvalType::Decimal)
                });
            return exact
                .then_some((FunctionRef::TiPb(decimal_signature), arity))
                .ok_or_else(|| {
                    BatchFailure::admission("Decimal batch call is not exact NewDecimal")
                });
        }
    }
    let ret = function
        .get_static_type()
        .ok_or_else(|| BatchFailure::admission("batch call has no return type"))?
        .eval_type();
    if name == "json_type" && function.args.len() == 1 && ret == NativeEvalType::String {
        let argument = &function.args[0];
        let operation = match argument.static_type().map(FieldType::eval_type) {
            Some(NativeEvalType::String) => {
                if let Expression::Constant(constant) = argument {
                    if !matches!(
                        constant.literal_value(),
                        Some(Datum::Null | Datum::String(_) | Datum::Bytes(_))
                    ) {
                        return Err(BatchFailure::admission(
                            "JSON_TYPE text source needs actual String/Bytes datum kind",
                        ));
                    }
                }
                tidb_query_expr::LocalFunctionId::JsonTypeTextSqlNative
            }
            Some(NativeEvalType::Json)
                if matches!(argument, Expression::Column(_) | Expression::Constant(_)) =>
            {
                tidb_query_expr::LocalFunctionId::JsonTypeBinarySqlNative
            }
            _ => {
                return Err(BatchFailure::admission(
                    "JSON_TYPE needs a ready string or raw native JSON leaf",
                ));
            }
        };
        return Ok((FunctionRef::Local(operation), 1));
    }
    let control = match name {
        "if" => Some("If"),
        "ifnull" => Some("IfNull"),
        "coalesce" => Some("Coalesce"),
        "case" => Some("CaseWhen"),
        "and" | "logicand" => Some("LogicalAnd"),
        "or" | "logicor" => Some("LogicalOr"),
        _ => None,
    };
    if let Some(control) = control {
        use tipb::ScalarFuncSig as Sig;
        let signature = match (control, ret) {
            ("If", NativeEvalType::Int) => Sig::IfInt,
            ("If", NativeEvalType::Real) => Sig::IfReal,
            ("If", NativeEvalType::Decimal) => Sig::IfDecimal,
            ("If", NativeEvalType::String) => Sig::IfString,
            ("If", NativeEvalType::Datetime) => Sig::IfTime,
            ("If", NativeEvalType::Timestamp) => Sig::IfTime,
            ("If", NativeEvalType::Duration) => Sig::IfDuration,
            ("If", NativeEvalType::Json) => Sig::IfJson,
            ("IfNull", NativeEvalType::Int) => Sig::IfNullInt,
            ("IfNull", NativeEvalType::Real) => Sig::IfNullReal,
            ("IfNull", NativeEvalType::Decimal) => Sig::IfNullDecimal,
            ("IfNull", NativeEvalType::String) => Sig::IfNullString,
            ("IfNull", NativeEvalType::Datetime) => Sig::IfNullTime,
            ("IfNull", NativeEvalType::Timestamp) => Sig::IfNullTime,
            ("IfNull", NativeEvalType::Duration) => Sig::IfNullDuration,
            ("IfNull", NativeEvalType::Json) => Sig::IfNullJson,
            ("Coalesce", NativeEvalType::Int) => Sig::CoalesceInt,
            ("Coalesce", NativeEvalType::Real) => Sig::CoalesceReal,
            ("Coalesce", NativeEvalType::Decimal) => Sig::CoalesceDecimal,
            ("Coalesce", NativeEvalType::String) => Sig::CoalesceString,
            ("Coalesce", NativeEvalType::Datetime) => Sig::CoalesceTime,
            ("Coalesce", NativeEvalType::Timestamp) => Sig::CoalesceTime,
            ("Coalesce", NativeEvalType::Duration) => Sig::CoalesceDuration,
            ("Coalesce", NativeEvalType::Json) => Sig::CoalesceJson,
            ("CaseWhen", NativeEvalType::Int) => Sig::CaseWhenInt,
            ("CaseWhen", NativeEvalType::Real) => Sig::CaseWhenReal,
            ("CaseWhen", NativeEvalType::Decimal) => Sig::CaseWhenDecimal,
            ("CaseWhen", NativeEvalType::String) => Sig::CaseWhenString,
            ("CaseWhen", NativeEvalType::Datetime) => Sig::CaseWhenTime,
            ("CaseWhen", NativeEvalType::Timestamp) => Sig::CaseWhenTime,
            ("CaseWhen", NativeEvalType::Duration) => Sig::CaseWhenDuration,
            ("CaseWhen", NativeEvalType::Json) => Sig::CaseWhenJson,
            ("LogicalAnd", NativeEvalType::Int) => Sig::LogicalAnd,
            ("LogicalOr", NativeEvalType::Int) => Sig::LogicalOr,
            _ => {
                return Err(BatchFailure::admission(
                    "control result needs a supported shared carrier",
                ));
            }
        };
        let function_ref = FunctionRef::TiPb(signature);
        let contract =
            tidb_query_expr::local::batch_signature_for_arity(function_ref, function.args.len())
                .ok_or_else(|| {
                    BatchFailure::admission("control arity is outside shared demand contract")
                })?;
        let exact = function
            .get_static_type()
            .is_some_and(|field| !field.is_unsigned())
            && function.args.iter().enumerate().all(|(index, arg)| {
                arg.static_type().is_some_and(|field| {
                    !field.is_unsigned()
                        && Some(batch_eval_type(field.eval_type())) == contract.argument_type(index)
                })
            });
        // Native COALESCE stamps temporal FSP on the selected payload.
        // Until that transform is a shared node, only admit already matching
        // metadata, including DATE/DATETIME/TIMESTAMP identity.
        let exact = exact
            && !(name == "coalesce"
                && matches!(
                    ret,
                    NativeEvalType::Datetime | NativeEvalType::Timestamp | NativeEvalType::Duration
                )
                && function.args.iter().any(|arg| {
                    arg.static_type().is_none_or(|field| {
                        function.get_static_type().is_none_or(|result| {
                            field.code() != result.code() || field.decimal() != result.decimal()
                        })
                    })
                }));
        return exact
            .then_some((function_ref, function.args.len()))
            .ok_or_else(|| {
                BatchFailure::admission(
                    "control arguments require explicit carrier/unsigned conversion",
                )
            });
    }
    if name == "nullif" && function.args.len() == 2 && ret == NativeEvalType::Int {
        let exact = function.args.iter().all(|arg| {
            arg.static_type().is_some_and(|field| {
                field.code() == FieldTypeCode::LongLong && !field.is_unsigned()
            })
        }) && function.get_static_type().is_some_and(|field| {
            !field.is_unsigned() && function.args[0].static_type() == Some(field)
        });
        return exact
            .then_some((
                FunctionRef::Local(tidb_query_expr::LocalFunctionId::NullIfIntSignedSigned),
                2,
            ))
            .ok_or_else(|| {
                BatchFailure::admission(
                    "NULLIF requires its exact signed first argument declaration",
                )
            });
    }
    if name == "strcmp" {
        let function_ref = FunctionRef::Local(tidb_query_expr::LocalFunctionId::StrcmpNative);
        let exact = ret == NativeEvalType::Int
            && function.args.len() == 2
            && function.args.iter().all(|arg| {
                arg.static_type()
                    .is_some_and(|field| field.eval_type() == NativeEvalType::String)
            })
            && tidb_query_expr::local::batch_signature(function_ref).is_some();
        return exact.then_some((function_ref, 2)).ok_or_else(|| {
            BatchFailure::admission(
                "STRCMP batch inputs require ready strings and native policy support",
            )
        });
    }
    if name == "any_value" && function.args.len() == 1 {
        let argument = function.args[0]
            .static_type()
            .ok_or_else(|| BatchFailure::admission("ANY_VALUE argument has no static type"))?
            .eval_type();
        if argument != ret {
            return Err(BatchFailure::admission(
                "ANY_VALUE argument/result carriers differ",
            ));
        }
        let signature = match ret {
            NativeEvalType::Int => tipb::ScalarFuncSig::IntAnyValue,
            NativeEvalType::Real => tipb::ScalarFuncSig::RealAnyValue,
            NativeEvalType::Decimal => tipb::ScalarFuncSig::DecimalAnyValue,
            NativeEvalType::String => tipb::ScalarFuncSig::StringAnyValue,
            NativeEvalType::Datetime | NativeEvalType::Timestamp => {
                tipb::ScalarFuncSig::TimeAnyValue
            }
            NativeEvalType::Duration => tipb::ScalarFuncSig::DurationAnyValue,
            NativeEvalType::Json => tipb::ScalarFuncSig::JsonAnyValue,
            NativeEvalType::VectorFloat32 => tipb::ScalarFuncSig::VectorFloat32AnyValue,
        };
        let function = FunctionRef::TiPb(signature);
        if tidb_query_expr::local::batch_signature(function).is_some() {
            return Ok((function, 1));
        }
    }
    let row = CATALOG
        .iter()
        .find(|row| {
            row.name == name
                && row.selector.len() == function.args.len()
                && row.ret == ret
                && row
                    .selector
                    .iter()
                    .zip(&function.args)
                    .all(|(pattern, arg)| {
                        arg.static_type().is_some_and(|field| {
                            pattern.eval.is_none_or(|eval| eval == field.eval_type())
                                && pattern
                                    .unsigned
                                    .is_none_or(|unsigned| unsigned == field.is_unsigned())
                                && pattern
                                    .binary_string
                                    .is_none_or(|binary| binary == field.is_binary_string())
                        })
                    })
        })
        .ok_or_else(|| BatchFailure::admission("missing exact batch catalog fact"))?;
    let signature = tipb::ScalarFuncSig::from_i32(row.sig as i32)
        .ok_or_else(|| BatchFailure::admission("unknown batch catalog signature"))?;
    let (arguments, result) = tidb_query_expr::local::batch_signature(FunctionRef::TiPb(signature))
        .ok_or_else(|| {
            BatchFailure::admission("catalog signature is outside the eager batch closure")
        })?;
    // Catalog selectors may permit frontend coercion (for example JSON to
    // String for IS NULL). This source path has no coercion node, so reject
    // that capability gap before handing an invalid spec to the compiler.
    if arguments.len() != function.args.len()
        || result != batch_eval_type(ret)
        || arguments.iter().zip(&function.args).any(|(expected, arg)| {
            arg.static_type()
                .is_none_or(|field| batch_eval_type(field.eval_type()) != *expected)
        })
    {
        return Err(BatchFailure::admission(
            "catalog batch call requires frontend argument conversion",
        ));
    }
    Ok((FunctionRef::TiPb(signature), function.args.len()))
}

fn batch_eval_type(native: NativeEvalType) -> TikvEvalType {
    match native {
        NativeEvalType::Int => TikvEvalType::Int,
        NativeEvalType::Real => TikvEvalType::Real,
        NativeEvalType::Decimal => TikvEvalType::Decimal,
        NativeEvalType::String => TikvEvalType::Bytes,
        NativeEvalType::Datetime | NativeEvalType::Timestamp => TikvEvalType::DateTime,
        NativeEvalType::Duration => TikvEvalType::Duration,
        NativeEvalType::Json => TikvEvalType::Json,
        NativeEvalType::VectorFloat32 => TikvEvalType::VectorFloat32,
    }
}

/// Resolves one SQL call through the same exact catalog fact and TiKV
/// capability registry used by preparation. The native eligibility pass uses
/// this view so adding a signature cannot leave its mandatory consumer behind.
pub(crate) fn batch_call_signature(
    function: &ScalarFunction,
) -> Option<tidb_query_expr::local::BatchCallSignature> {
    let (target, arity) = resolve_call_for(function, SignaturePurpose::NativeEligibility).ok()?;
    match target {
        CallTarget::Kernel(function) => {
            tidb_query_expr::local::batch_signature_for_arity(function, arity)
        }
        CallTarget::Host(operation) => {
            tidb_query_expr::local::BatchCallSignature::for_host(operation, arity)
        }
    }
}
fn projected(sql: &FieldType, new_collation: bool) -> Result<tipb::FieldType> {
    let collation = tidb_datatype::get_collation_by_name(sql.collation_name())
        .map_err(|_| BatchFailure::admission("unknown batch SQL collation name"))?;
    if collation.id <= 0 {
        return Err(BatchFailure::admission("nonpositive SQL collation ID"));
    }
    Ok(project_field_type(
        sql,
        if new_collation {
            -collation.id
        } else {
            collation.id
        },
    )?)
}

// Use the same demand recipe for declared and genuinely absent result types.
fn survey_common_math_diagnostic<'a, N: SourceReference<'a>>(
    function: &'a ScalarFunction,
    ordinal: usize,
    depth: usize,
    allow_pb: bool,
    limits: BatchSourceLimits,
    budget: &mut Budget,
    scheduled: &mut usize,
    pending: &mut Vec<(N, usize, NativeView, bool)>,
    diagnostics: &mut Vec<PendingDiagnostic<'a>>,
) -> Result<()> {
    let recipe = crate::scalar_function::dynamic_diagnostic::DynamicDiagnostic::survey_math(
        function,
        limits.tree.max_nodes,
        limits.max_metadata_bytes,
    )?;
    budget.add(recipe.bytes())?;
    budget.array::<PendingDiagnostic<'_>>(1)?;
    budget.array::<CompiledDiagnosticParent>(1)?;
    budget.array::<Option<crate::scalar_function::dynamic_diagnostic::DynamicDiagnostic>>(1)?;
    budget.array::<usize>(recipe.bindings().len())?;
    budget.array::<tipb::FieldType>(recipe.bindings().len())?;
    budget.array::<(N, usize, NativeView, bool)>(recipe.bindings().len())?;
    budget.field(&native_bytes_type(), recipe.bindings().len())?;
    *scheduled = (*scheduled)
        .checked_add(recipe.bindings().len())
        .filter(|n| *n <= limits.tree.max_nodes)
        .ok_or_else(|| resource("diagnostic mirror source node policy exceeded"))?;
    let child_depth = depth
        .checked_add(1)
        .filter(|n| *n <= limits.tree.max_depth)
        .ok_or_else(|| resource("diagnostic mirror source depth policy exceeded"))?;
    reserve(pending, recipe.bindings().len())?;
    for &child in recipe.bindings().iter().rev() {
        pending.push((
            child.into(),
            child_depth,
            NativeView::DiagnosticConstantRoot(recipe.policy()),
            allow_pb,
        ));
    }
    reserve(diagnostics, 1)?;
    diagnostics.push(PendingDiagnostic {
        ordinal,
        main_arity: function.args.len(),
        bindings: recipe.bindings().len(),
        recipe: Some(recipe),
    });
    Ok(())
}

// The detached numeric recipes preserve the same diagnostic re-demands for
// genuinely present and absent result declarations.
fn survey_arithmetic_diagnostic<'a, N: SourceReference<'a>>(
    function: &'a ScalarFunction,
    view: NativeView,
    ordinal: usize,
    depth: usize,
    allow_pb: bool,
    limits: BatchSourceLimits,
    budget: &mut Budget,
    scheduled: &mut usize,
    pending: &mut Vec<(N, usize, NativeView, bool)>,
    numerics: &mut Vec<PendingNumeric<'a>>,
) -> Result<()> {
    let numeric = numeric_source::survey::<N>(function, view, ordinal, limits, budget)?;
    *scheduled = (*scheduled)
        .checked_add(numeric.extras.len())
        .filter(|n| *n <= limits.tree.max_nodes)
        .ok_or_else(|| resource("numeric extra source node policy exceeded"))?;
    let child_depth = depth
        .checked_add(1)
        .filter(|n| *n <= limits.tree.max_depth)
        .ok_or_else(|| resource("numeric extra source depth policy exceeded"))?;
    reserve(pending, numeric.extras.len())?;
    for &(child, child_view) in numeric.extras.iter().rev() {
        pending.push((child.into(), child_depth, child_view, allow_pb));
    }
    reserve(numerics, 1)?;
    numerics.push(numeric);
    Ok(())
}

// Complete source and incoming schema byte gates precede EVERY equality and
// detached/projected/C snapshot. Borrowed temporary nodes have no executable
// children and are not a second interpreter. Stable owner interval is required.
#[cfg(test)]
fn preflight<'a>(
    root: &'a Expression,
    row_schema: &[FieldType],
    limits: BatchSourceLimits,
) -> Result<Vec<CheckedNode<'a>>> {
    preflight_view(root, row_schema, limits, None, false).map(|checked| checked.nodes)
}
#[cfg(test)]
fn preflight_view<'a>(
    root: &'a Expression,
    row_schema: &[FieldType],
    limits: BatchSourceLimits,
    view: Option<NativeView>,
    root_probe: bool,
) -> Result<CheckedSource<'a>> {
    preflight_source(
        root,
        SourceInputs::Projection(row_schema),
        limits,
        view,
        root_probe,
    )
}

#[derive(Clone, Copy)]
enum SourceInputs<'a> {
    Projection(&'a [FieldType]),
    PhysicalRow { width: usize },
    NativeBatch { width: usize },
}
impl<'a> SourceInputs<'a> {
    fn width(self) -> usize {
        match self {
            Self::Projection(fields) => fields.len(),
            Self::PhysicalRow { width } | Self::NativeBatch { width } => width,
        }
    }
    fn declarations(self) -> &'a [FieldType] {
        match self {
            Self::Projection(fields) => fields,
            Self::PhysicalRow { .. } | Self::NativeBatch { .. } => &[],
        }
    }
    fn typed_context_target<'n>(
        self,
        node: impl Into<SourceNodeRef<'n>>,
        view: NativeView,
    ) -> Option<NativeEvalType> {
        let node = node.into();
        row_typed_context_target(node, view).or_else(|| {
            if !matches!(self, Self::NativeBatch { .. }) {
                return None;
            }
            let BorrowedNodeKind::Column(column) = node.kind() else {
                return None;
            };
            if usize::try_from(column.index)
                .ok()
                .is_none_or(|i| i >= self.width())
            {
                // The original typed batch leaf reports its own index error
                // only when demanded; no replacement schema makes it valid.
                view.bulk_target()
            } else {
                None
            }
        })
    }
    fn declaration(self, index: usize) -> Option<&'a FieldType> {
        self.declarations().get(index)
    }
}

fn preflight_source<'a, N: SourceReference<'a>>(
    root: N,
    inputs: SourceInputs<'_>,
    limits: BatchSourceLimits,
    view: Option<NativeView>,
    root_probe: bool,
) -> Result<CheckedSource<'a, N>> {
    let mut budget = Budget { used: 0 };
    for field in inputs.declarations() {
        budget.field(field, 1)?;
    }
    let mut pending = Vec::new();
    let mut checked = Vec::new();
    let mut diagnostics = Vec::new();
    let mut numerics = Vec::new();
    let mut diagnostic_slots = 0usize;
    budget.array::<(N, usize, NativeView, bool)>(1)?;
    reserve(&mut pending, 1)?;
    pending.push((
        root,
        1usize,
        view.unwrap_or_else(|| root.default_view(limits.tree)),
        matches!(root.kind(), BorrowedNodeKind::ScalarFunction(function) if pb_timestamp_diff(function) || pb_value_owner(function)),
    ));
    let mut scheduled = 1usize;
    while let Some((node, depth, view, allow_pb)) = pending.pop() {
        let allow_pb = allow_pb
            || matches!(node.kind(), BorrowedNodeKind::ScalarFunction(function) if pb_timestamp_diff(function) || pb_value_owner(function));
        let (declaration, collation) = parts_optional(node, allow_pb)?;
        // Authenticate the original declaration before the optional-type arm.
        // A removed declaration is stale metadata, not a new untyped source.
        if allow_pb {
            let origin = match node.kind() {
                BorrowedNodeKind::Constant(value) => value.pb_origin(),
                BorrowedNodeKind::Column(value) => value.pb_origin(),
                BorrowedNodeKind::ScalarFunction(value) => value.pb_origin(),
                _ => None,
            };
            if let Some(origin) = origin {
                if !declaration.is_some_and(|field| origin.matches_effective_type(field)) {
                    return Err(BatchFailure::from(binding(
                        "PB effective declaration differs from its original snapshot",
                    )));
                }
            }
        }
        if let BorrowedNodeKind::ScalarFunction(function) = node.kind() {
            if pb_value_owner(function) {
                if let Some(origin) = function.pb_origin() {
                    if origin.expr_type != Some(tidb_proto::tipb::ExprType::ScalarFunc as i32)
                        || origin.signature
                            != function.pb_signature().map(|signature| signature as i32)
                        || origin.child_count != function.args.len()
                    {
                        return Err(BatchFailure::from(binding(
                            "PB selected implementation differs from its origin",
                        )));
                    }
                }
            }
        }
        // Header metadata has no value operands. Keep original logical arity
        // and attributes in its descriptor; unused row-value bindings are not
        // part of this data program. The real tree is still shape-bounded.
        if let BorrowedNodeKind::ScalarFunction(function) = node.kind() {
            if let Some(kind) = crate::scalar_function::header_value::HeaderKind::for_function(
                function,
            )
            .filter(|_| {
                !pb_value_unary_view(function, view) && !pb_value_arithmetic_view(function, view)
            }) {
                if view.value_role().is_none() {
                    return Err(BatchFailure::admission(
                        "metadata outcome needs its native value ABI",
                    ));
                }

                budget.field(&native_bytes_type(), 5)?;
                if let Some(field) = declaration {
                    budget.field(field, 2)?;
                }
                if pb_value_owner(function) {
                    if let Some(origin) = function.pb_origin() {
                        if let Some(original) = origin.effective_type_ref() {
                            budget.field(original, 1)?;
                        }
                        if !declaration.is_some_and(|field| origin.matches_effective_type(field)) {
                            return Err(BatchFailure::from(binding(
                                "PB header declaration differs from its original snapshot",
                            )));
                        }
                    }
                }
                budget.array::<CheckedNode<'_, N>>(1)?;
                budget.array::<NodeRecord>(1)?;
                budget.array::<BuildNode>(1)?;
                budget.array::<LocalExpr>(1)?;
                budget.array::<OrdinaryCallSite>(2)?;
                budget.array::<(SqlBatchHostOperation, HostSignature)>(1)?;
                budget.array::<NativeHostMetadata>(1)?;
                budget.array::<Option<Datum>>(1)?;
                budget.add(function.func_name.original().len())?;
                budget.add(function.func_name.lowercase().len())?;
                let (charset, collate) = collation.charset_and_collation();
                budget.add(charset.len())?;
                budget.add(collate.len())?;
                if let Some(property) = kind.property() {
                    budget.add(
                        property
                            .copy_extent(&function.args[0])
                            .checked_mul(2)
                            .ok_or_else(|| resource("property metadata result copy overflow"))?,
                    )?;
                }
                if view.diagnostic() {
                    budget.array::<usize>(1)?;
                    diagnostic_slots += 1;
                }
                reserve(&mut checked, 1)?;
                checked.push(CheckedNode {
                    node,
                    sql: declaration,
                    view,
                    subtree_end: 0,
                    folds_to_constant: false,
                    math_render_bound: None,
                    numeric_render_bound: None,
                    numeric_format_failure: None,
                });
                continue;
            }
        }

        if inputs.typed_context_target(node, view).is_some() {
            budget.array::<crate::typed_context_value::TypedContextValue>(1)?;
            budget.array::<NativeTypedDescriptor>(1)?;
            if typed_cache_source(node, view) {
                budget.array::<Option<Datum>>(1)?;
                budget.array::<bool>(1)?;
            }
        }

        let Some(sql) = declaration else {
            if view.value_role().is_none()
                || !matches!(
                    node.kind(),
                    BorrowedNodeKind::Constant(_)
                        | BorrowedNodeKind::CorrelatedColumn(_)
                        | BorrowedNodeKind::Column(_)
                ) && !matches!(node.kind(),BorrowedNodeKind::ScalarFunction(f) if native_optional_declarations_call(f))
            {
                return Err(BatchFailure::admission(
                    "optional SQL consumer needs its explicit profile",
                ));
            }
            budget.field(&native_bytes_type(), 3)?;
            budget.array::<CheckedNode<'_, N>>(1)?;
            budget.array::<NodeRecord>(1)?;
            budget.array::<BuildNode>(1)?;
            budget.array::<LocalExpr>(1)?;
            let (charset, collate) = collation.charset_and_collation();
            budget.add(charset.len())?;
            budget.add(collate.len())?;
            match node.kind() {
                BorrowedNodeKind::ScalarFunction(function)
                    if native_optional_declarations_call(function) =>
                {
                    check_batch_call(function, view)?;
                    let arity = source_data_arity_in_view(function, view);
                    budget.add(function.func_name.original().len())?;
                    budget.add(function.func_name.lowercase().len())?;
                    budget.array::<OrdinaryCallSite>(2)?;
                    budget.array::<(SqlBatchHostOperation, HostSignature)>(1)?;
                    budget.array::<NativeHostMetadata>(1)?;
                    budget.array::<Option<Datum>>(1)?;
                    if native_optional_selection_call(function) {
                        budget.array::<usize>(arity)?;
                    } else if !native_optional_operator_call(function) {
                        budget_value_call_metadata(&mut budget, function, None)?;
                    }
                    let (CallTarget::Host(operation), _) = resolve_call_in_view(function, view)?
                    else {
                        unreachable!("native optional value owner");
                    };
                    if let Some(probe) = source_prefix_probe(function, operation) {
                        budget.add(crate::tikv::identity_actual::copy_extent(probe).map_err(
                            |_| resource("optional source prefix probe copy overflow"),
                        )?)?;
                    }
                    if (!pb_value_owner(function)
                        || pb_value_unary_view(function, view)
                        || pb_value_arithmetic_view(function, view))
                        && matches!(
                            function.func_name.lowercase(),
                            "abs" | "cot" | "pow" | "power"
                        )
                        && function.args.iter().any(|child| {
                            contains_context_value(child, limits.tree.max_depth, &mut {
                                limits.tree.max_nodes
                            }) || contains_metadata_property(child, limits.tree.max_depth, &mut {
                                limits.tree.max_nodes
                            })
                        })
                    {
                        survey_common_math_diagnostic(
                            function,
                            checked.len(),
                            depth,
                            allow_pb,
                            limits,
                            &mut budget,
                            &mut scheduled,
                            &mut pending,
                            &mut diagnostics,
                        )?;
                    }

                    if native_optional_arithmetic_call(function)
                        && (native_missing_arithmetic_sources(function)
                            || function.args.iter().any(|child| {
                                contains_context_value(child, limits.tree.max_depth, &mut {
                                    limits.tree.max_nodes
                                }) || contains_metadata_property(
                                    child,
                                    limits.tree.max_depth,
                                    &mut { limits.tree.max_nodes },
                                ) || contains_hidden_virtual_diagnostic(
                                    child,
                                    limits.tree.max_depth,
                                    &mut { limits.tree.max_nodes },
                                )
                            }))
                    {
                        survey_arithmetic_diagnostic(
                            function,
                            view,
                            checked.len(),
                            depth,
                            allow_pb,
                            limits,
                            &mut budget,
                            &mut scheduled,
                            &mut pending,
                            &mut numerics,
                        )?;
                    }
                    budget.array::<tipb::FieldType>(arity)?;
                    for arg in &function.args {
                        if let Some(field) = arg.static_type() {
                            budget.field(field, 1)?;
                        }
                    }
                    scheduled = scheduled
                        .checked_add(arity)
                        .filter(|n| *n <= limits.tree.max_nodes)
                        .ok_or_else(|| resource("optional scalar source node policy exceeded"))?;
                    let child_depth = depth
                        .checked_add(1)
                        .filter(|d| *d <= limits.tree.max_depth)
                        .ok_or_else(|| resource("optional scalar depth policy exceeded"))?;
                    budget.array::<(N, usize, NativeView, bool)>(arity)?;
                    reserve(&mut pending, arity)?;
                    for (index, child) in function.args.iter().take(arity).enumerate().rev() {
                        pending.push((
                            child.into(),
                            child_depth,
                            argument_view(function, view, index),
                            allow_pb,
                        ));
                    }
                }
                BorrowedNodeKind::Constant(_)
                    if source_value_failure(node, view, inputs.width()).is_some() =>
                {
                    budget_context_source(&mut budget, None)?
                }
                BorrowedNodeKind::Constant(c) if c.literal_value().is_some() => {
                    if inputs.typed_context_target(node, view).is_some() {
                        budget_context_source(&mut budget, None)?;
                    }
                    budget.add(
                        native_payload_bytes(c.literal_value().unwrap())?
                            .checked_mul(3)
                            .ok_or_else(|| resource("native literal source copy size overflow"))?,
                    )?;
                    budget.array::<usize>(1)?;
                    budget.field(&native_bytes_type(), 2)?;
                }
                BorrowedNodeKind::Constant(c) => {
                    budget_context_source(&mut budget, None)?;
                    if c.param_marker.is_none() || typed_bulk_forward(node, view) {
                        if let Some(child) = c.deferred_expr.as_deref() {
                            scheduled = scheduled
                                .checked_add(1)
                                .filter(|n| *n <= limits.tree.max_nodes)
                                .ok_or_else(|| resource("deferred source node policy exceeded"))?;
                            let child_depth = depth
                                .checked_add(1)
                                .filter(|d| *d <= limits.tree.max_depth)
                                .ok_or_else(|| resource("deferred source depth policy exceeded"))?;
                            budget.array::<(N, usize, NativeView, bool)>(1)?;
                            budget.array::<tipb::FieldType>(1)?;
                            reserve(&mut pending, 1)?;
                            pending.push((
                                child.into(),
                                child_depth,
                                context_child_view(view),
                                allow_pb,
                            ));
                        }
                    }
                }
                BorrowedNodeKind::CorrelatedColumn(_) => budget_context_source(&mut budget, None)?,
                BorrowedNodeKind::Column(_)
                    if inputs.typed_context_target(node, view).is_some()
                        || source_value_failure(node, view, inputs.width()).is_some() =>
                {
                    budget_context_source(&mut budget, None)?
                }
                _ => {
                    return Err(BatchFailure::admission(
                        "optional leaf lacks its actual value/error producer",
                    ));
                }
            }
            if view.diagnostic()
                && (!matches!(node.kind(),BorrowedNodeKind::Constant(c) if c.literal_value().is_some())
                    || source_value_failure(node, view, inputs.width()).is_some())
            {
                budget.array::<usize>(1)?;
                diagnostic_slots += 1;
            }
            reserve(&mut checked, 1)?;
            checked.push(CheckedNode {
                node,
                sql: None,
                view,
                subtree_end: 0,
                folds_to_constant: matches!(node.kind(), BorrowedNodeKind::Constant(_)),
                math_render_bound: None,
                numeric_render_bound: None,
                numeric_format_failure: None,
            });
            continue;
        };
        if view.value_role().is_none() {
            check_type(sql)?;
        } else if !matches!(
            node.kind(),
            BorrowedNodeKind::Constant(_)
                | BorrowedNodeKind::Column(_)
                | BorrowedNodeKind::CorrelatedColumn(_)
        ) && !matches!(node.kind(), BorrowedNodeKind::ScalarFunction(function) if native_value_call(function))
        {
            // The source is valid SQL but this producer does not yet have a
            // native-value recipe. It is a migration capability gap, not a
            // forged binding or a failed execution. Keep actual provenance and
            // resource errors terminal at their separate checked boundaries.
            return Err(BatchFailure::admission(
                "native value producer is outside the current source closure",
            ));
        }
        budget.field(sql, 3)?; // native source, projected declaration, C snapshot
        if allow_pb {
            let origin = match node.kind() {
                BorrowedNodeKind::Constant(v) => v.pb_origin(),
                BorrowedNodeKind::Column(v) => v.pb_origin(),
                BorrowedNodeKind::ScalarFunction(v) => v.pb_origin(),
                _ => None,
            };
            if let Some(origin) = origin {
                if let Some(original) = origin.effective_type_ref() {
                    budget.field(original, 1)?;
                }
                if !origin.matches_effective_type(sql) {
                    return Err(BatchFailure::from(binding(
                        "PB effective declaration differs from its original snapshot",
                    )));
                }
            }
        }
        if view != NativeView::Typed {
            budget.field(&native_bytes_type(), 3)?;
        }
        budget.array::<CheckedNode<'_, N>>(1)?;
        budget.array::<NodeRecord>(1)?;
        budget.array::<BuildNode>(1)?;
        budget.array::<LocalExpr>(1)?;
        let (charset, collate) = collation.charset_and_collation();
        budget.add(charset.len())?;
        budget.add(collate.len())?;
        match node.kind() {
            BorrowedNodeKind::Constant(value) => {
                if value.param_marker.is_some() || value.deferred_expr.is_some() {
                    if view.value_role().is_none() {
                        return Err(BatchFailure::admission(
                            "contextual constant has no generic value view",
                        ));
                    }
                    budget_context_source(&mut budget, Some(sql))?;
                    if value.param_marker.is_none() || typed_bulk_forward(node, view) {
                        if let Some(child) = value.deferred_expr.as_deref() {
                            scheduled = scheduled
                                .checked_add(1)
                                .filter(|n| *n <= limits.tree.max_nodes)
                                .ok_or_else(|| resource("deferred source node policy exceeded"))?;
                            let child_depth = depth
                                .checked_add(1)
                                .filter(|d| *d <= limits.tree.max_depth)
                                .ok_or_else(|| resource("deferred source depth policy exceeded"))?;
                            budget.array::<(N, usize, NativeView, bool)>(1)?;
                            budget.array::<tipb::FieldType>(1)?;
                            reserve(&mut pending, 1)?;
                            pending.push((
                                child.into(),
                                child_depth,
                                context_child_view(view),
                                allow_pb,
                            ));
                        }
                    }
                } else {
                    if inputs.typed_context_target(node, view).is_some() {
                        budget_context_source(&mut budget, Some(sql))?;
                    }
                    let literal = value.literal_value().expect("plain literal source");
                    if view.value_role().is_some() {
                        budget.add(native_payload_bytes(literal)?.checked_mul(3).ok_or_else(
                            || resource("native literal source copy size overflow"),
                        )?)?;
                        budget.array::<usize>(1)?;
                        budget.field(&native_bytes_type(), 2)?;
                    } else {
                        budget.add(literal_copy_budget(literal, sql)?)?;
                    }
                }
            }
            BorrowedNodeKind::Column(column) => {
                if inputs.typed_context_target(node, view).is_some()
                    || source_value_failure(node, view, inputs.width()).is_some()
                {
                    budget_context_source(&mut budget, Some(sql))?;
                } else {
                    // Both scalar and typed Column reads use the physical
                    // cell. Planner annotations do not substitute its value;
                    // hidden virtual metadata remains available to diagnostics.
                    let index = usize::try_from(column.index)
                        .map_err(|_| BatchFailure::from(binding("negative batch column index")))?;
                    if index >= inputs.width() {
                        return Err(BatchFailure::from(binding(
                            "batch column outside row schema",
                        )));
                    }
                    budget.add(column.orig_name.len())?;
                    budget.array::<usize>(1)?;
                    budget.field(sql, 1)?; // projected binding declaration copy
                }
            }
            BorrowedNodeKind::ScalarFunction(function) => {
                if function
                    .args
                    .iter()
                    .any(|argument| argument.static_type().is_none())
                    && !native_optional_declarations_call(function)
                {
                    return Err(BatchFailure::admission(
                        "optional function argument needs its explicit profile",
                    ));
                }

                let arity = source_data_arity_in_view(function, view);
                let policy_nodes = usize::from(native_batch_policy(function).is_some());
                if policy_nodes != 0 {
                    budget.array::<NodeRecord>(1)?;
                    budget.array::<BuildNode>(1)?;
                    budget.array::<LocalExpr>(1)?;
                    budget.field(&native_policy_type(), 3)?;
                }
                scheduled = scheduled
                    .checked_add(arity.saturating_add(policy_nodes))
                    .filter(|nodes| *nodes <= limits.tree.max_nodes)
                    .ok_or_else(|| resource("batch source node policy exceeded"))?;
                budget.array::<(N, usize, NativeView, bool)>(arity)?;

                let selected_pb_numeric = pb_scalar_binary_domain(function, view).is_some();
                let contextual_numeric =
                    (native_arithmetic_operation(function.func_name.lowercase()).is_some()
                        || native_unary_sign_operation(function.func_name.lowercase()).is_some()
                        || matches!(
                            function.func_name.lowercase(),
                            "abs" | "cot" | "exp" | "pow" | "power"
                        ))
                        && (native_missing_arithmetic_sources(function)
                            || function.args.iter().any(|child| {
                                contains_context_value(child, limits.tree.max_depth, &mut {
                                    limits.tree.max_nodes
                                }) || contains_metadata_property(
                                    child,
                                    limits.tree.max_depth,
                                    &mut { limits.tree.max_nodes },
                                ) || (native_arithmetic_operation(function.func_name.lowercase())
                                    .is_some()
                                    && contains_hidden_virtual_diagnostic(
                                        child,
                                        limits.tree.max_depth,
                                        &mut { limits.tree.max_nodes },
                                    ))
                            }));
                if selected_pb_numeric
                    || (contextual_numeric
                        && (!pb_value_owner(function)
                            || pb_value_unary_view(function, view)
                            || pb_value_arithmetic_view(function, view))
                        && (!crate::scalar_function::eager_values::supports(function)
                            || matches!(
                                function.func_name.lowercase(),
                                "abs" | "cot" | "exp" | "pow" | "power"
                            ))
                        && function.func_name.lowercase() != "exp"
                        && !(view.value_role().is_some()
                            && native_unary_sign_operation(function.func_name.lowercase())
                                .is_some()))
                {
                    if !selected_pb_numeric
                        && matches!(
                            function.func_name.lowercase(),
                            "abs" | "cot" | "exp" | "pow" | "power"
                        )
                        && matches!(
                            resolve_call_in_view(function, view)?.0,
                            CallTarget::Host(
                                SqlBatchHostOperation::EagerValuesNative
                                    | SqlBatchHostOperation::AbsNative
                                    | SqlBatchHostOperation::AbsIntWindowNative
                            )
                        )
                    {
                        survey_common_math_diagnostic(
                            function,
                            checked.len(),
                            depth,
                            allow_pb,
                            limits,
                            &mut budget,
                            &mut scheduled,
                            &mut pending,
                            &mut diagnostics,
                        )?;
                    } else if (selected_pb_numeric
                        || matches!(
                            function.func_name.lowercase(),
                            "plus" | "minus" | "mul" | "mod" | "div" | "intdiv"
                        ))
                        && function.args.len() == 2
                        && view.value_role().is_some()
                    {
                        survey_arithmetic_diagnostic(
                            function,
                            view,
                            checked.len(),
                            depth,
                            allow_pb,
                            limits,
                            &mut budget,
                            &mut scheduled,
                            &mut pending,
                            &mut numerics,
                        )?;
                    } else {
                        return Err(BatchFailure::admission(
                            "dynamic numeric diagnostic re-demand is not yet closed",
                        ));
                    }
                }
                check_batch_call(function, view)?;
                budget.add(function.func_name.original().len())?;
                budget.add(function.func_name.lowercase().len())?;
                budget.array::<OrdinaryCallSite>(2)?; // initial and lowered source ordinals
                if matches!(resolve_call_in_view(function, view)?.0, CallTarget::Host(_)) {
                    budget.array::<(SqlBatchHostOperation, HostSignature)>(1)?;
                    budget.array::<NativeHostMetadata>(1)?;
                    budget.array::<Option<Datum>>(1)?;
                    let CallTarget::Host(operation) = resolve_call_in_view(function, view)?.0
                    else {
                        unreachable!()
                    };
                    if let Some(probe) = source_prefix_probe(function, operation) {
                        budget.add(
                            crate::tikv::identity_actual::copy_extent(probe)
                                .map_err(|_| resource("source prefix probe copy overflow"))?,
                        )?;
                    }
                    if function.func_name.lowercase() == "cast_char" {
                        budget.add(sql.charset_name().len())?;
                        if matches!(
                            resolve_call_in_view(function, view)?.0,
                            CallTarget::Host(SqlBatchHostOperation::CastCharNative { .. })
                        ) {
                            budget.field(
                                function.args[0]
                                    .static_type()
                                    .expect("checked native argument type"),
                                1,
                            )?;
                        }
                    }
                    if matches!(
                        resolve_call_in_view(function, view)?.0,
                        CallTarget::Host(SqlBatchHostOperation::CastNative)
                    ) {
                        budget.field(sql, 1)?;
                        budget.field(
                            function.args[0]
                                .static_type()
                                .expect("checked native cast argument"),
                            1,
                        )?;
                        budget.add(sql.charset_name().len())?;
                    }
                    if matches!(
                        resolve_call_in_view(function, view)?.0,
                        CallTarget::Host(
                            SqlBatchHostOperation::CoalesceNative
                                | SqlBatchHostOperation::IfNullNative
                                | SqlBatchHostOperation::IfNative
                                | SqlBatchHostOperation::CaseNative
                                | SqlBatchHostOperation::AndNative
                                | SqlBatchHostOperation::OrNative
                                | SqlBatchHostOperation::IsTrueWithNullNative
                                | SqlBatchHostOperation::UnaryNotNative
                                | SqlBatchHostOperation::IsNullNative
                                | SqlBatchHostOperation::IsTrueNative
                                | SqlBatchHostOperation::IsFalseNative
                                | SqlBatchHostOperation::IsNotTrueNative
                                | SqlBatchHostOperation::IsNotFalseNative
                                | SqlBatchHostOperation::IsUnknownNative
                                | SqlBatchHostOperation::IsNotUnknownNative
                        )
                    ) {
                        budget.array::<usize>(function.args.len())?;
                        budget.field(sql, 1)?;
                    }
                    if matches!(
                        resolve_call_in_view(function, view)?.0,
                        CallTarget::Host(
                            SqlBatchHostOperation::CompareNative(_)
                                | SqlBatchHostOperation::NullSafeEqNative
                                | SqlBatchHostOperation::NullIfNative
                        )
                    ) {
                        budget.field(sql, 1)?;
                    }
                    if matches!(
                        resolve_call_in_view(function, view)?.0,
                        CallTarget::Host(
                            SqlBatchHostOperation::AbsNative
                                | SqlBatchHostOperation::BitwiseNative(_)
                                | SqlBatchHostOperation::XorNative
                                | SqlBatchHostOperation::UnarySignNative(_)
                        )
                    ) {
                        budget.field(sql, 1)?;
                    }
                    if window_unary_operation(function, view).is_some() {
                        budget.field(sql, 1)?;
                        budget.field(
                            function.args[0]
                                .static_type()
                                .expect("checked unary source"),
                            1,
                        )?;
                    }
                    if matches!(
                        resolve_call_in_view(function, view)?.0,
                        CallTarget::Host(SqlBatchHostOperation::EagerValuesNative)
                    ) || native_optional_grouping_call(function)
                        || native_optional_wildcard_call(function)
                        || native_optional_arithmetic_call(function)
                        || native_optional_ordered_comparison_call(function)
                        || native_optional_date_arithmetic_call(function)
                        || (native_concat_value_call(function) && view.value_role().is_some())
                    {
                        budget_value_call_metadata(&mut budget, function, Some(sql))?;
                    }
                    // Optional value calls retain only their selected owner metadata.
                    // A name may also have a typed specialization whose required
                    // declarations were not selected by this call's source.
                    if !native_optional_declarations_call(function) {
                        if native_crypto_call(function).is_some() && view.value_role().is_some() {
                            budget.field(sql, 1)?;
                            budget.array::<usize>(arity)?;
                            budget.array::<Option<Datum>>(1)?;
                            if let Some(v) = function.identity_constant_probe() {
                                budget.add(
                                    crate::tikv::identity_actual::copy_extent(v)
                                        .map_err(|_| resource("crypto probe copy overflow"))?,
                                )?;
                            }
                        }
                        if native_compression_call(function).is_some()
                            && view.value_role().is_some()
                        {
                            budget.field(sql, 1)?;
                            budget.array::<Option<Datum>>(1)?;
                            if let Some(v) = function.identity_constant_probe() {
                                budget
                                    .add(crate::tikv::identity_actual::copy_extent(v).map_err(
                                        |_| resource("compression probe copy overflow"),
                                    )?)?;
                            }
                        }
                        if native_byte_unary_call(function).is_some() && view.value_role().is_some()
                        {
                            budget.field(sql, 1)?;
                            budget.array::<Option<Datum>>(1)?;
                            if let Some(v) = function.identity_constant_probe() {
                                budget
                                    .add(crate::tikv::identity_actual::copy_extent(v).map_err(
                                        |_| resource("byte unary probe copy overflow"),
                                    )?)?;
                            }
                        }
                        if native_misc_call(function).is_some() && view.value_role().is_some() {
                            budget.field(sql, 1)?;
                            budget.array::<usize>(arity)?;
                            budget.array::<Option<Datum>>(1)?;
                            if let Some(v) = function.identity_constant_probe() {
                                budget.add(
                                    crate::tikv::identity_actual::copy_extent(v)
                                        .map_err(|_| resource("misc probe copy overflow"))?,
                                )?;
                            }
                        }
                        if native_identity_call(function) && view.value_role().is_some() {
                            budget.field(sql, 1)?;
                            budget.array::<usize>(arity)?;
                            budget.array::<Option<Datum>>(1)?;
                            if let Some(value) = function.identity_constant_probe() {
                                budget.add(
                                    crate::tikv::identity_actual::copy_extent(value)
                                        .map_err(|_| resource("identity probe extent overflow"))?,
                                )?;
                            }
                        }
                        if native_like_call(function) {
                            budget.field(sql, 1)?;
                            budget.field(
                                function.args[2].static_type().expect("checked LIKE escape"),
                                1,
                            )?;
                            budget.add(
                                std::mem::size_of::<tidb_query_expr::NativeLikeInvocation>()
                                    + std::mem::size_of::<[usize; 3]>(),
                            )?;
                        }
                        if native_grouping_call(function) {
                            budget.field(sql, 1)?;
                            budget.add(std::mem::size_of::<
                                Option<std::sync::Arc<crate::grouping::GroupingMetadata>>,
                            >())?;
                        }
                        if let Some(op) = native_vector_call(function) {
                            budget.field(sql, 1)?;
                            budget.add(
                                op.arity()
                                    .checked_mul(std::mem::size_of::<usize>())
                                    .ok_or_else(|| resource("vector metadata overflow"))?,
                            )?;
                        }
                        if let Some(op) = native_duration_construction_call(function) {
                            budget.field(sql, 1)?;
                            for arg in &function.args {
                                budget.field(
                                    arg.static_type().expect("checked duration source"),
                                    1,
                                )?;
                            }
                            budget.add(
                                op.arity()
                                    .checked_mul(std::mem::size_of::<usize>())
                                    .ok_or_else(|| resource("duration source metadata overflow"))?,
                            )?;
                        }
                        if let Some((_, arity)) = native_calendar_call(function) {
                            budget.field(sql, 1)?;
                            budget.field(
                                function.args[0]
                                    .static_type()
                                    .expect("checked calendar source"),
                                1,
                            )?;
                            budget.add(
                                arity
                                    .checked_mul(std::mem::size_of::<usize>())
                                    .ok_or_else(|| resource("calendar metadata overflow"))?,
                            )?;
                        }
                        if native_clock_call(function).is_some() {
                            budget.field(sql, 1)?;
                        }
                        if native_tso_call(function).is_some() {
                            budget.field(sql, 1)?;
                            if let Some(arg) = function.args.first() {
                                budget.field(arg.static_type().expect("checked TSO source"), 1)?;
                            }
                        }
                        if let Some((_, arity)) = native_epoch_call(function) {
                            budget.add(
                                arity
                                    .checked_mul(std::mem::size_of::<usize>())
                                    .ok_or_else(|| resource("epoch node metadata overflow"))?,
                            )?;
                        }
                        if let Some(call) = native_extract_call(function) {
                            budget.add(
                                call.arity()
                                    .checked_mul(std::mem::size_of::<usize>())
                                    .ok_or_else(|| resource("EXTRACT node metadata overflow"))?,
                            )?;
                            if let Some(unit) = call.unit() {
                                budget.add(unit.len())?;
                            }
                            budget.field(
                                function.args[usize::from(call.unit().is_none())]
                                    .static_type()
                                    .expect("checked EXTRACT value source"),
                                1,
                            )?;
                        }
                        if native_date_arithmetic_call(function).is_some() {
                            let (unit, _) = native_date_arithmetic_call(function)
                                .expect("checked date arithmetic");
                            budget.add(unit.len())?;
                            budget.add(unit.len())?;
                            for arg in &function.args {
                                budget.field(
                                    arg.static_type().expect("checked date arithmetic argument"),
                                    1,
                                )?;
                            }
                        }
                        if matches!(
                            resolve_call_in_view(function, view)?.0,
                            CallTarget::Host(SqlBatchHostOperation::BoundedStalenessNative { .. })
                        ) {
                            for argument in &function.args {
                                budget.field(
                                    argument.static_type().expect("checked bounded argument"),
                                    1,
                                )?;
                            }
                        }
                        if native_arithmetic_operation(function.func_name.lowercase()).is_some() {
                            budget.field(sql, 1)?;
                            for arg in &function.args {
                                budget.field(
                                    arg.static_type().expect("checked arithmetic source"),
                                    1,
                                )?;
                            }
                        }
                        if matches!(
                            resolve_call_in_view(function, view)?.0,
                            CallTarget::Host(SqlBatchHostOperation::InNative)
                        ) {
                            budget.array::<hosts::NativeInArgument>(function.args.len())?;
                            budget.field(sql, 1)?;
                            for arg in &function.args {
                                budget.field(arg.static_type().expect("checked IN source"), 1)?;
                            }
                            let (keys, dynamic, _) = function.native_in_cache();
                            budget.array::<usize>(dynamic.len())?;
                            if let Some(keys) = keys {
                                // Conservative logical table/key duplication allowance;
                                // no whole allocator-memory guarantee is implied.
                                budget.array::<(Vec<u8>, u64)>(
                                    keys.capacity().checked_mul(4).ok_or_else(|| {
                                        resource("IN key table allowance overflow")
                                    })?,
                                )?;
                                for key in keys {
                                    budget.add(key.len().checked_mul(2).ok_or_else(|| {
                                        resource("IN key copy allowance overflow")
                                    })?)?;
                                }
                            }
                        }
                        if matches!(
                            resolve_call_in_view(function, view)?.0,
                            CallTarget::Host(SqlBatchHostOperation::IntervalNative)
                        ) {
                            budget.array::<hosts::NativeIntervalArgument>(function.args.len())?;
                            budget.field(sql, 1)?;
                            for arg in &function.args {
                                budget.field(
                                    arg.static_type().expect("checked INTERVAL source"),
                                    1,
                                )?;
                            }
                        }
                        if matches!(
                            resolve_call_in_view(function, view)?.0,
                            CallTarget::Host(SqlBatchHostOperation::ExtremumNative)
                        ) {
                            budget.array::<usize>(function.args.len())?;
                            budget.array::<i64>(function.args.len())?;
                            budget.field(sql, 1)?;
                        }
                        if matches!(
                            resolve_call_in_view(function, view)?.0,
                            CallTarget::Host(SqlBatchHostOperation::CharsetNative(_))
                        ) {
                            budget.array::<usize>(function.args.len())?;
                            budget.field(
                                function.args[0]
                                    .static_type()
                                    .expect("checked charset source"),
                                1,
                            )?;
                            budget.field(sql, 1)?;
                        }
                    }
                    budget.array::<tipb::FieldType>(function.args.len())?;
                    budget.field(sql, 1)?;
                    for argument in &function.args {
                        if let Some(field) = argument.static_type() {
                            budget.field(field, 1)?;
                        } else if !native_optional_declarations_call(function) {
                            return Err(BatchFailure::admission(
                                "host argument has no static declaration",
                            ));
                        }
                    }
                }
                let child_depth = depth
                    .checked_add(1)
                    .filter(|depth| *depth <= limits.tree.max_depth)
                    .ok_or_else(|| resource("batch source depth policy exceeded"))?;
                reserve(&mut pending, arity)?;
                for (index, child) in function.args.iter().take(arity).enumerate().rev() {
                    pending.push((
                        child.into(),
                        child_depth,
                        argument_view(function, view, index),
                        allow_pb,
                    ));
                }
            }
            BorrowedNodeKind::CorrelatedColumn(_) => {
                if view.value_role().is_none() {
                    return Err(BatchFailure::admission(
                        "correlated source requires its generic view",
                    ));
                }
                budget_context_source(&mut budget, Some(sql))?;
            }
        }
        if view.diagnostic()
            && (matches!(
                node.kind(),
                BorrowedNodeKind::ScalarFunction(_) | BorrowedNodeKind::CorrelatedColumn(_)
            ) || matches!(node.kind(),BorrowedNodeKind::Constant(c) if c.literal_value().is_none())
                || source_value_failure(node, view, inputs.width()).is_some())
        {
            budget.array::<usize>(1)?;
            diagnostic_slots += 1;
        }
        reserve(&mut checked, 1)?;
        checked.push(CheckedNode {
            node,
            sql: Some(sql),
            view,
            subtree_end: 0,
            folds_to_constant: false,
            math_render_bound: None,
            numeric_render_bound: None,
            numeric_format_failure: None,
        });
    }
    for index in (0..checked.len()).rev() {
        let mut end = index
            .checked_add(1)
            .ok_or_else(|| resource("source subtree ordinal overflow"))?;
        let mut math_children = Some(0usize);
        let mut numeric_children = Some(0usize);
        let mut numeric_child_failure = None;
        let mut folds_to_constant =
            matches!(checked[index].node.kind(), BorrowedNodeKind::Constant(_));
        if let BorrowedNodeKind::Constant(c) = checked[index].node.kind() {
            if (c.param_marker.is_none()
                || typed_bulk_forward(checked[index].node, checked[index].view))
                && c.deferred_expr.is_some()
            {
                end = checked
                    .get(end)
                    .ok_or_else(|| binding("deferred source child missing"))?
                    .subtree_end;
                math_children = None;
                numeric_children = None;
            }
        }
        if let BorrowedNodeKind::ScalarFunction(function) = checked[index].node.kind() {
            folds_to_constant = !crate::constant_fold::is_unfoldable_call(
                function.func_name.lowercase(),
                function.args.len(),
            );
            let header = native_no_argument_header_call(function)
                && !pb_value_unary_view(function, checked[index].view)
                && !pb_value_arithmetic_view(function, checked[index].view);
            if header {
                folds_to_constant = checked[index].node.folds_to_constant();
                if !function.args.is_empty() {
                    math_children = None;
                    numeric_children = None;
                }
            }
            for _ in 0..if header {
                0
            } else {
                source_data_arity_in_view(function, checked[index].view)
            } {
                math_children = match (
                    math_children,
                    checked
                        .get(end)
                        .ok_or_else(|| binding("math diagnostic source missing"))?
                        .math_render_bound,
                ) {
                    (Some(total), Some(bytes)) => Some(
                        total
                            .checked_add(bytes)
                            .ok_or_else(|| resource("math diagnostic byte size overflow"))?,
                    ),
                    _ => None,
                };
                if let Some(total) = numeric_children {
                    let child = checked
                        .get(end)
                        .ok_or_else(|| binding("numeric diagnostic source missing"))?;
                    if let Some(bits) = child.numeric_format_failure {
                        numeric_child_failure = Some(bits);
                        numeric_children = None;
                    } else if let Some(bytes) = child.numeric_render_bound {
                        numeric_children =
                            Some(total.checked_add(bytes).ok_or_else(|| {
                                resource("numeric diagnostic byte size overflow")
                            })?);
                    } else {
                        numeric_children = None;
                    }
                }
                folds_to_constant &= checked
                    .get(end)
                    .ok_or_else(|| binding("constant metadata source missing"))?
                    .folds_to_constant;
                end = checked
                    .get(end)
                    .ok_or_else(|| binding("source subtree is incomplete"))?
                    .subtree_end;
            }
        }
        if let Ok(position) = diagnostics.binary_search_by_key(&index, |p| p.ordinal) {
            for _ in 0..diagnostics[position].bindings {
                end = checked
                    .get(end)
                    .ok_or_else(|| binding("diagnostic mirror child absent"))?
                    .subtree_end;
            }
        }
        if let Ok(position) = numerics.binary_search_by_key(&index, |p| p.ordinal) {
            for _ in &numerics[position].extras {
                end = checked
                    .get(end)
                    .ok_or_else(|| binding("numeric extra child absent"))?
                    .subtree_end;
            }
        }
        if end <= index || end > checked.len() {
            return Err(binding("source subtree join differs").into());
        }
        checked[index].subtree_end = end;
        checked[index].folds_to_constant = folds_to_constant;
        checked[index].math_render_bound = checked[index]
            .node
            .math_bound(math_children)
            .map_err(resource)?;
        checked[index].numeric_format_failure =
            checked[index].node.numeric_failure(numeric_child_failure);
        checked[index].numeric_render_bound = checked[index]
            .node
            .numeric_bound(numeric_children)
            .map_err(resource)?;
    }
    // Charge every owned diagnostic copy group before any temporary literal
    // conversion, source capture, formatting or immutable metadata snapshot.
    for item in &checked {
        if item.view.value_role().is_some()
            && matches!(item.node.kind(), BorrowedNodeKind::ScalarFunction(function) if matches!(resolve_call_in_view(function,item.view)?.0,CallTarget::Host(SqlBatchHostOperation::AbsNative)))
        {
            if let Some(bytes) = item.math_render_bound {
                budget.add(
                    bytes
                        .checked_mul(4)
                        .ok_or_else(|| resource("math diagnostic copy allowance overflow"))?,
                )?;
            }
        }
    }
    // Common math owns only raw diagnostic facts at preparation. Visible
    // Decimal scale is charged only if an actual error demands its rendering.
    for (index, item) in checked.iter().enumerate() {
        let BorrowedNodeKind::ScalarFunction(f) = item.node.kind() else {
            continue;
        };
        if !matches!(
            f.func_name.lowercase(),
            "cot" | "exp" | "pow" | "power" | "abs"
        ) || item.math_render_bound.is_none()
            || !matches!(
                resolve_call_in_view(f, item.view)?.0,
                CallTarget::Host(SqlBatchHostOperation::EagerValuesNative)
            )
        {
            continue;
        }
        budget.array::<crate::scalar_function::math_diagnostic::MathDiagnosticPart>(
            item.subtree_end - index,
        )?;
        for source in &checked[index..item.subtree_end] {
            let bytes = match source.node.kind() {
                BorrowedNodeKind::Constant(c) => c
                    .literal_value()
                    .map(crate::tikv::identity_actual::copy_extent)
                    .transpose()
                    .map_err(|_| resource("diagnostic literal copy overflow"))?
                    .unwrap_or(0),
                BorrowedNodeKind::Column(c) => c.orig_name.len(),
                BorrowedNodeKind::CorrelatedColumn(c) => c.column.orig_name.len(),
                BorrowedNodeKind::ScalarFunction(f) => f.func_name.lowercase().len(),
            };
            budget.add(bytes)?;
        }
    }
    for (index, item) in checked.iter().enumerate() {
        let BorrowedNodeKind::ScalarFunction(function) = item.node.kind() else {
            continue;
        };
        if item.view.value_role().is_none()
            || native_arithmetic_operation(function.func_name.lowercase()).is_none()
            || (pb_value_owner(function) && !pb_value_arithmetic_view(function, item.view))
        {
            continue;
        }
        if native_missing_arithmetic_sources(function) {
            if function.func_name.lowercase() == "intdiv" {
                budget.array::<crate::scalar_function::NativeIntDivDiagnostics>(1)?;
                budget.add(512)?;
            }
            continue;
        }
        if let Some(bytes) = item.numeric_render_bound {
            budget.add(
                bytes
                    .checked_mul(8)
                    .ok_or_else(|| resource("arithmetic diagnostic copies overflow"))?,
            )?;
        }
        if function.func_name.lowercase() == "intdiv" {
            budget.array::<crate::scalar_function::NativeIntDivDiagnostics>(1)?;
            budget.add(512)?;
        }
        let mut child = index + 1;
        for arg in &function.args {
            let item = checked
                .get(child)
                .ok_or_else(|| binding("arithmetic diagnostic argument missing"))?;
            if arithmetic_subject_required(function, arg) {
                if let Some(bytes) = item.numeric_render_bound {
                    budget.add(bytes.checked_mul(4).ok_or_else(|| {
                        resource("numeric argument diagnostic copies overflow")
                    })?)?;
                }
            }
            if function.func_name.lowercase() == "intdiv" {
                if let Some(bytes) = item.numeric_render_bound {
                    budget.add(
                        bytes
                            .checked_mul(4)
                            .ok_or_else(|| resource("DIV argument diagnostic copies overflow"))?,
                    )?;
                }
            }
            child = item.subtree_end;
        }
    }
    if root_probe
        || diagnostic_slots != 0
        || !diagnostics.is_empty()
        || !numerics.is_empty()
        || checked
            .iter()
            .any(|item| typed_cache_source(item.node, item.view))
    {
        budget.array::<ContextSourcePolicy>(1)?;
    }
    // Even a temporary representability conversion must wait for the complete
    // source/schema/payload byte gate, including literals later in the graph.
    for item in &checked {
        if let BorrowedNodeKind::Constant(value) = item.node.kind() {
            if item.view.value_role().is_some() {
                continue;
            }
            source_literal(
                value.literal_value().expect("checked literal"),
                item.declared_sql(),
                item.view,
            )?;
        }
        if let BorrowedNodeKind::Column(column) = item.node.kind() {
            if row_typed_context_target(item.node, item.view).is_some()
                || source_value_failure(item.node, item.view, inputs.width()).is_some()
            {
                continue;
            }

            let Some(field) = inputs.declaration(column.index as usize) else {
                // A public Row carries storage and a physical index, not a
                // complete SQL schema. Its actual expression declaration is
                // the original Column.Eval decoding contract.
                continue;
            };
            // A native Datum producer decodes using its sealed expression
            // declaration, including a COLLATE overlay. Authenticate the actual
            // storage code/width independently; typed imports still require the
            // complete declaration below.
            if item.view.value_role().is_some()
                && field.code() == item.declared_sql().code()
                && get_fixed_len(field) == get_fixed_len(item.declared_sql())
            {
                continue;
            }
            if field.collation() != item.declared_sql().collation() || field != item.declared_sql()
            {
                return Err(BatchFailure::from(binding(
                    "batch binding differs from complete row declaration",
                )));
            }
        }
    }
    Ok(CheckedSource {
        logical_metadata_bytes: budget.used,
        nodes: checked,
        diagnostics,
        numerics,
    })
}

#[cfg(test)]
fn suite_shape(suite: &EvaluatorSuite) -> Result<&Expression> {
    let program = &suite.program;
    if program.calculated.len() != 1
        || program.calculated_output_indexes.len() != 1
        || program.calculated_output_indexes.first() != Some(&0)
        || !program.column_mapping.is_empty()
        || suite.column_swap_helper.is_some()
    {
        return Err(BatchFailure::admission(
            "private batch suite requires one calculation and no owner swap",
        ));
    }
    let root = &program.calculated[0];
    if !matches!(root, Expression::ScalarFunction(_)) {
        return Err(BatchFailure::admission(
            "private consumer requires a call root",
        ));
    }
    Ok(root)
}
enum BuildKind {
    Literal(ScalarValue),
    Input(usize),
    Host {
        slot: usize,
        arity: usize,
    },
    Call {
        function: FunctionRef,
        arity: usize,
        policy: Option<i64>,
    },
}
struct BuildNode {
    field: tipb::FieldType,
    kind: BuildKind,
}

fn native_policy_type() -> FieldType {
    FieldType::new(FieldTypeCode::LongLong)
        .with_flen(20)
        .with_decimal(0)
}

fn native_batch_policy(function: &ScalarFunction) -> Option<i64> {
    (function.func_name.lowercase() == "strcmp")
        .then(|| function.derived_collation().native_policy().tag())
}

// Policy constants add graph nodes but no SQL call/effect. Recompute graph
// ordinals while retaining each original call's immutable source identity.
fn lowered_call_sites(
    root: &LocalExpr,
    original: Vec<OrdinaryCallSite>,
) -> Result<Vec<OrdinaryCallSite>> {
    let mut sites = Vec::new();
    reserve(&mut sites, original.len())?;
    let mut original = original.into_iter();
    let mut pending = Vec::new();
    reserve(&mut pending, 1)?;
    pending.push(root);
    let mut ordinal = 0usize;
    while let Some(node) = pending.pop() {
        if let LocalExpr::Call { args, .. } | LocalExpr::HostCall { args, .. } = node {
            let source = original
                .next()
                .ok_or_else(|| binding("lowered call has no SQL source"))?
                .source();
            sites.push(OrdinaryCallSite::sql_native_batch(ordinal, source));
            reserve(&mut pending, args.len())?;
            pending.extend(args.iter().rev());
        }
        ordinal += 1;
    }
    if original.next().is_some() {
        return Err(binding("lowered graph lost a SQL call").into());
    }
    Ok(sites)
}

fn arithmetic_host_metadata<'a, N: SourceReference<'a>>(
    operation: SqlBatchHostOperation,
    sql: &ScalarFunction,
    item: &CheckedNode<'a, N>,
    ordinal: usize,
    checked: &[CheckedNode<'a, N>],
) -> Result<NativeHostMetadata> {
    let left = ordinal
        .checked_add(1)
        .ok_or_else(|| resource("arithmetic source ordinal overflow"))?;
    let right = checked
        .get(left)
        .ok_or_else(|| binding("arithmetic left source missing"))?
        .subtree_end;
    let nodes = [left, right];
    let owned_missing_sources = native_missing_arithmetic_sources(sql);
    let op = crate::scalar_function::binary_op_for_name(sql.func_name.lowercase())
        .ok_or_else(|| binding("arithmetic source operation missing"))?;
    let capture = |float| -> Result<Option<Box<str>>> {
        if owned_missing_sources {
            return Ok(None);
        }
        if let Some(bound) = item.numeric_render_bound {
            let text =
                crate::scalar_function::static_arithmetic_overflow_expression(sql, op, float);
            if text.as_ref().is_some_and(|text| text.len() > bound) {
                return Err(binding("arithmetic diagnostic exceeds its admitted bound").into());
            }
            Ok(text.map(String::into_boxed_str))
        } else {
            Ok(None)
        }
    };
    let mut subjects = [None, None];
    let mut subject_format_failures = [None, None];
    for index in 0..2 {
        let child = checked
            .get(nodes[index])
            .ok_or_else(|| binding("arithmetic child source missing"))?;
        if !owned_missing_sources && arithmetic_subject_required(sql, &sql.args[index]) {
            subject_format_failures[index] = child.numeric_format_failure;
            if let Some(bound) = child.numeric_render_bound {
                let text = crate::scalar_function::static_numeric_expression_text(
                    child
                        .node
                        .expression()
                        .ok_or_else(|| binding("numeric argument lost its expression owner"))?,
                    true,
                );
                if text.as_ref().is_some_and(|text| text.len() > bound) {
                    return Err(binding("numeric subject exceeds its admitted bound").into());
                }
                subjects[index] = text.map(String::into_boxed_str);
            }
        }
    }
    let batch = if operation.is_windowed() {
        let target = item
            .view
            .bulk_target()
            .ok_or_else(|| binding("window arithmetic target missing"))?;
        let crate::scalar_function::NativeArithmeticInputMode::Numeric(domain) =
            sql.native_arithmetic_generic_mode()
        else {
            return Err(binding("window arithmetic domain missing").into());
        };
        let sources = [0, 1].map(|index| match &sql.args[index] {
            _ if row_typed_context_target(&sql.args[index], checked[nodes[index]].view)
                .is_some() =>
            {
                WindowArithmeticSource::TypedReady
            }
            Expression::Constant(_) => WindowArithmeticSource::Literal,
            Expression::Column(_) => WindowArithmeticSource::Column,
            Expression::ScalarFunction(child)
                if checked[nodes[index]].view.bulk_target() == Some(NativeEvalType::Int)
                    && window_value_operation(child, checked[nodes[index]].view).is_some() =>
            {
                WindowArithmeticSource::IntegerReady
            }
            _ => WindowArithmeticSource::Computed,
        });
        Some(WindowArithmeticMetadata {
            domain,
            integer: target == NativeEvalType::Int
                && domain == NativeEvalType::Int
                && [0, 1].into_iter().all(|index| {
                    crate::scalar_function::native_numeric_batch_source(
                        checked[nodes[index]].declared_sql(),
                        crate::scalar_function::dynamic_diagnostic::saved_binary_literal(
                            &sql.args[index],
                        ),
                        domain,
                    ) == NativeEvalType::Int
                }),
            output_integer: target == NativeEvalType::Int,
            sources,
        })
    } else {
        None
    };
    let intdiv = if sql.func_name.lowercase() == "intdiv" {
        let mut arguments = [None, None];
        if !owned_missing_sources {
            for index in 0..2 {
                let child = &checked[nodes[index]];
                if let Some(bound) = child.numeric_render_bound {
                    let text = crate::scalar_function::static_numeric_argument_text(
                        sql,
                        &sql.args[index],
                        false,
                    );
                    if text.as_ref().is_some_and(|text| text.len() > bound) {
                        return Err(binding("DIV argument text exceeds admitted bound").into());
                    }
                    arguments[index] = text.map(String::into_boxed_str);
                }
            }
        }
        Some(Box::new(
            crate::scalar_function::NativeIntDivDiagnostics::new(
                sql,
                arguments,
                if owned_missing_sources {
                    [None, None]
                } else {
                    nodes.map(|node| checked[node].numeric_format_failure)
                },
            ),
        ))
    } else {
        None
    };
    Ok(NativeHostMetadata::Arithmetic {
                                    batch, operation,name:sql.func_name.lowercase().into(),result_type:item.sql.map(snapshot_field_type),
                                    argument_nodes:nodes,source_types:[checked[left].sql.map(snapshot_field_type),checked[right].sql.map(snapshot_field_type)],
                                    literal:[matches!(sql.args[0],Expression::Constant(_)),matches!(sql.args[1],Expression::Constant(_))],
                                    binary_literal:[0,1].map(|index|matches!(&sql.args[index],Expression::Constant(value) if matches!(value.value,Datum::BinaryLiteral(_)))),
                                    mode:sql.native_arithmetic_mode(),generic_mode:sql.native_arithmetic_generic_mode(),subjects,
                                    subject_format_failures,overflow_format_failure:if owned_missing_sources {None}else{item.numeric_format_failure},
                                    overflow_expressions:[capture(false)?,capture(true)?],intdiv,output_node:ordinal,
                                    output_role:item.view.value_role().ok_or_else(||binding("arithmetic result role missing"))?,
                                })
}

fn lower_view(
    suite: &EvaluatorSuite,
    calculated_slot: usize,
    row_schema: &[FieldType],
    new_collation: bool,
    unit: u64,
    limits: BatchSourceLimits,
    view: Option<NativeView>,
) -> Result<Arc<BatchSource>> {
    lower_view_policy(
        suite,
        calculated_slot,
        row_schema,
        new_collation,
        unit,
        limits,
        view,
        false,
    )
}
fn lower_view_policy(
    suite: &EvaluatorSuite,
    calculated_slot: usize,
    row_schema: &[FieldType],
    new_collation: bool,
    unit: u64,
    limits: BatchSourceLimits,
    view: Option<NativeView>,
    deferred_closed: bool,
) -> Result<Arc<BatchSource>> {
    let root = suite
        .program
        .calculated
        .get(calculated_slot)
        .ok_or_else(|| BatchFailure::admission("batch calculation slot is missing"))?;
    let plan = lower_source_plan(
        root,
        SourceInputs::Projection(row_schema),
        new_collation,
        unit,
        limits,
        view,
        SourceConsumer::Projection {
            unordered: suite.program.vectorizable,
            deferred_closed,
        },
    )?;
    #[cfg(test)]
    let detached = {
        let mut detached = Vec::new();
        reserve(&mut detached, row_schema.len())?;
        detached.extend(row_schema.iter().map(snapshot_field_type));
        detached
    };
    Ok(Arc::new(BatchSource {
        owner: Arc::clone(&suite.program),
        #[cfg(test)]
        root_identity: root as *const Expression as usize,
        calculated_slot,
        output_index: suite.program.calculated_output_indexes[calculated_slot],
        #[cfg(test)]
        row_schema: detached.into_boxed_slice(),
        plan,
    }))
}

#[derive(Clone, Copy)]
enum SourceConsumer {
    Projection {
        unordered: bool,
        deferred_closed: bool,
    },
    Scalar,
}
impl SourceConsumer {
    fn root_policy(
        self,
        root: SourceNodeRef<'_>,
        limits: BatchSourceLimits,
    ) -> Result<(bool, bool, Option<ContextTypeProbe>)> {
        if matches!(self, Self::Projection { .. }) && root.expression().is_none() {
            return Err(binding("Projection requires its actual expression root").into());
        }
        match self {
            Self::Projection {
                deferred_closed: true,
                ..
            } => Ok((false, true, None)),
            Self::Projection { unordered, .. } => context_root_policy(
                root.expression()
                    .ok_or_else(|| binding("Projection requires an expression root"))?,
                unordered,
                limits,
            ),
            Self::Scalar => Ok((false, true, None)),
        }
    }
}

fn lower_source_plan<'a, N: SourceReference<'a>>(
    root: N,
    inputs: SourceInputs<'_>,
    new_collation: bool,
    unit: u64,
    _limits: BatchSourceLimits,
    view: Option<NativeView>,
    consumer: SourceConsumer,
) -> Result<SourcePlan> {
    let limits = BatchSourceLimits {
        tree: CompileLimits::default(),
        max_metadata_bytes: usize::MAX,
    };

    let view = if root.function_root() {
        match view {
            None | Some(NativeView::FunctionRoot) => Some(NativeView::FunctionRoot),
            _ => return Err(binding("function root requires its own result view").into()),
        }
    } else {
        if view == Some(NativeView::FunctionRoot) {
            return Err(binding("expression root requires its enclosing result view").into());
        }
        view
    };

    let (constant_root_shortcut, context_row_only, root_type_probe) =
        consumer.root_policy(root.into(), limits)?;
    let mut checked = preflight_source(root, inputs, limits, view, root_type_probe.is_some())?;
    // These original batch entries collect ordinary row results before
    // publishing. Their argument demand and atomic output are separate policies.
    let row_value_batch = root.expression().is_some_and(|root| {
        matches!(root, Expression::ScalarFunction(function)
        if (function.func_name.lowercase() == "cast_char"
            && crate::scalar_function::static_batch_target(root).is_some())
            || native_concat_row_batch(function)
            || native_string_row_batch(root)
            || native_regexp_row_batch(root) || pb_value_row_batch(root)
            || native_integer_row_batch(root))
    });
    let context_row_only =
        context_row_only && checked[0].view.bulk_target().is_none() && !row_value_batch;
    let mut typed_cache_slots = 0usize;
    let mirrored = !checked.diagnostics.is_empty() || !checked.numerics.is_empty();
    let mut diagnostic_slots = Vec::new();
    let mut diagnostic_parents = Vec::new();
    let mut compiled_plans = Vec::new();
    reserve(&mut compiled_plans, checked.diagnostics.len())?;
    for pending in &mut checked.diagnostics {
        let (plan, _) = pending
            .recipe
            .take()
            .expect("surveyed diagnostic plan")
            .freeze()?;
        compiled_plans.push(Some(plan));
    }
    let mut numeric_parents = Vec::new();
    let mut numeric_plans = Vec::new();
    reserve(&mut numeric_plans, checked.numerics.len())?;
    for pending in &mut checked.numerics {
        numeric_plans.push(Some(pending.freeze()?));
    }
    let mut next_numeric = 0usize;
    let mut next_diagnostic = 0usize;
    let mut host_entries = Vec::new();
    let mut host_metadata = Vec::new();
    let mut host_probes = Vec::new();
    let mut nodes = Vec::new();
    let mut bindings = Vec::new();
    let mut schema = Vec::new();
    let mut sites = Vec::new();
    let mut build = Vec::new();
    reserve(&mut nodes, checked.len())?;
    reserve(&mut build, checked.len())?;
    for (ordinal, item) in checked.iter().enumerate() {
        let field = if item.view != NativeView::Typed {
            projected(&native_bytes_type(), new_collation)?
        } else {
            projected(item.declared_sql(), new_collation)?
        };
        let kind = SourceKind::capture(item.node, item.sql, item.view, inputs)?;
        let construction = match &kind {
            SourceKind::DecimalFastInput(_) => {
                return Err(binding("fast source entered the ordinary source factory").into());
            }
            SourceKind::Constant { value, .. } => BuildKind::Literal(value.clone()),
            SourceKind::Column { .. } | SourceKind::NativeConstant { .. } => {
                let slot = bindings.len();
                reserve(&mut bindings, 1)?;
                reserve(&mut schema, 1)?;
                bindings.push(ordinal);
                schema.push(field.clone());
                BuildKind::Input(slot)
            }
            SourceKind::TypedContext { arity, target } => {
                reserve(&mut sites, 1)?;
                sites.push(OrdinaryCallSite::sql_native_batch(
                    ordinal,
                    OrdinarySourceId::new(
                        unit,
                        if mirrored {
                            item.node.identity() as u64
                        } else {
                            u64::try_from(ordinal)
                                .map_err(|_| resource("typed source ID overflow"))?
                        },
                    ),
                ));
                let slot = host_entries.len();
                reserve(&mut host_entries, 1)?;
                reserve(&mut host_metadata, 1)?;
                reserve(&mut host_probes, 1)?;
                let mut arguments = Vec::new();
                reserve(&mut arguments, *arity)?;
                for _ in 0..*arity {
                    arguments.push(projected(&native_bytes_type(), new_collation)?);
                }
                host_metadata.push(NativeHostMetadata::TypedContext(NativeTypedContext {
                    descriptor: if typed_bulk_forward(item.node, item.view) {
                        let BorrowedNodeKind::Constant(c) = item.node.kind() else {
                            unreachable!()
                        };
                        match c.deferred_expr.as_deref().expect("real deferred child") {
                            Expression::ScalarFunction(function)
                                if *target == NativeEvalType::Int
                                    && window_value_operation(
                                        function,
                                        checked[ordinal + 1].view,
                                    )
                                    .is_none() =>
                            {
                                let field =
                                    snapshot_field_type(checked[ordinal + 1].declared_sql());
                                Some(Box::new(if matches!(item.view, NativeView::BulkRoot(_)) {
                                    NativeTypedDescriptor::NormalizeRoot {
                                        field,
                                        unsigned: item.sql.is_some_and(FieldType::is_unsigned),
                                    }
                                } else {
                                    NativeTypedDescriptor::Normalize { field }
                                }))
                            }
                            _ if *target == NativeEvalType::Int
                                && matches!(item.view, NativeView::BulkRoot(_)) =>
                            {
                                Some(Box::new(NativeTypedDescriptor::RootForward {
                                    unsigned: item.sql.is_some_and(FieldType::is_unsigned),
                                }))
                            }
                            _ => None,
                        }
                    } else {
                        Some(Box::new(match item.node.kind() {
                            BorrowedNodeKind::Constant(c) if c.literal_value().is_some() => {
                                NativeTypedDescriptor::Literal {
                                    value: c.literal_value().unwrap().clone(),
                                    field: item.sql.map(snapshot_field_type),
                                }
                            }
                            BorrowedNodeKind::Column(column) => NativeTypedDescriptor::Column {
                                index: column.index,
                                field: item.sql.map(snapshot_field_type),
                            },
                            _ => NativeTypedDescriptor::Context(
                                crate::typed_context_value::TypedContextValue::capture(
                                    item.node.expression().ok_or_else(|| {
                                        binding("typed leaf lost its expression owner")
                                    })?,
                                )
                                .ok_or_else(|| binding("typed context descriptor absent"))?,
                            ),
                        }))
                    },
                    target: *target,
                    cache_slot: if typed_cache_source(item.node, item.view) {
                        let slot = typed_cache_slots;
                        typed_cache_slots = typed_cache_slots
                            .checked_add(1)
                            .ok_or_else(|| resource("typed cache count overflow"))?;
                        Some(slot)
                    } else {
                        None
                    },
                    argument: (*arity == 1).then_some(ordinal + 1),
                    output_node: ordinal,
                    output_role: item
                        .view
                        .value_role()
                        .ok_or_else(|| binding("typed source role absent"))?,
                }));
                host_entries.push((
                    if typed_bulk_forward(item.node, item.view)
                        && matches!(
                            host_metadata.last(),
                            Some(NativeHostMetadata::TypedContext(meta)) if meta.window_forward()
                        )
                    {
                        SqlBatchHostOperation::EagerValuesWindowNative
                    } else {
                        SqlBatchHostOperation::EagerValuesNative
                    },
                    HostSignature {
                        arg_types: arguments.into_boxed_slice(),
                        return_type: field.clone(),
                    },
                ));
                host_probes.push(None);
                if item.view.diagnostic() {
                    reserve(&mut diagnostic_slots, 1)?;
                    diagnostic_slots.push(slot);
                }
                BuildKind::Host {
                    slot,
                    arity: *arity,
                }
            }
            SourceKind::ContextValue { arity } => {
                reserve(&mut sites, 1)?;
                sites.push(OrdinaryCallSite::sql_native_batch(
                    ordinal,
                    OrdinarySourceId::new(
                        unit,
                        if mirrored {
                            item.node.identity() as u64
                        } else {
                            u64::try_from(ordinal)
                                .map_err(|_| resource("context source ID overflow"))?
                        },
                    ),
                ));
                let slot = host_entries.len();
                reserve(&mut host_entries, 1)?;
                reserve(&mut host_metadata, 1)?;
                reserve(&mut host_probes, 1)?;
                let mut arguments = Vec::new();
                reserve(&mut arguments, *arity)?;
                for _ in 0..*arity {
                    arguments.push(projected(&native_bytes_type(), new_collation)?);
                }
                host_entries.push((
                    SqlBatchHostOperation::EagerValuesNative,
                    HostSignature {
                        arg_types: arguments.into_boxed_slice(),
                        return_type: field.clone(),
                    },
                ));
                host_metadata.push(NativeHostMetadata::ContextValue {
                    descriptor: Box::new(
                        if let Some(message) =
                            source_value_failure(item.node, item.view, inputs.width())
                        {
                            crate::context_value::ContextValue {
                                read: crate::context_value::ContextRead::Failure(message),
                                target: item.sql.map(snapshot_field_type),
                                convert_deferred: false,
                                root_broadcast: false,
                            }
                        } else {
                            crate::context_value::ContextValue::capture(
                                item.node.expression().ok_or_else(|| {
                                    binding("context leaf lost its expression owner")
                                })?,
                            )
                            .ok_or_else(|| binding("context descriptor absent"))?
                        },
                    ),
                    diagnostic: match item.view {
                        NativeView::DiagnosticConstantRoot(policy) => Some(policy),
                        _ => None,
                    },
                    argument: (*arity == 1).then_some(ordinal + 1),
                    output_node: ordinal,
                    output_role: item
                        .view
                        .value_role()
                        .ok_or_else(|| binding("context source output role absent"))?,
                });
                host_probes.push(None);
                if item.view.diagnostic() {
                    reserve(&mut diagnostic_slots, 1)?;
                    diagnostic_slots.push(slot);
                }
                BuildKind::Host {
                    slot,
                    arity: *arity,
                }
            }
            SourceKind::Call { target, arity } => {
                reserve(&mut sites, 1)?;
                let node = if mirrored {
                    item.node.identity() as u64
                } else {
                    u64::try_from(ordinal).map_err(|_| resource("batch source ID overflow"))?
                };
                sites.push(OrdinaryCallSite::sql_native_batch(
                    ordinal,
                    OrdinarySourceId::new(unit, node),
                ));
                match *target {
                    CallTarget::Kernel(function) => BuildKind::Call {
                        function,
                        arity: *arity,
                        policy: match item.node.kind() {
                            BorrowedNodeKind::ScalarFunction(sql) => native_batch_policy(sql),
                            _ => None,
                        },
                    },
                    CallTarget::Host(operation) => {
                        let BorrowedNodeKind::ScalarFunction(sql) = item.node.kind() else {
                            return Err(binding("host call lost its SQL source").into());
                        };
                        let mut arguments = Vec::new();
                        reserve(&mut arguments, *arity)?;
                        for argument in sql.args.iter().take(*arity) {
                            arguments.push(
                                if (!matches!(
                                    operation,
                                    SqlBatchHostOperation::Concat | SqlBatchHostOperation::ConcatWs
                                ) || item.view.value_role().is_some())
                                    && matches!(
                                        operation,
                                        SqlBatchHostOperation::CastCharNative { .. }
                                            | SqlBatchHostOperation::CastNative
                                            | SqlBatchHostOperation::CoalesceNative
                                            | SqlBatchHostOperation::IfNullNative
                                            | SqlBatchHostOperation::IfNative
                                            | SqlBatchHostOperation::CaseNative
                                            | SqlBatchHostOperation::AndNative
                                            | SqlBatchHostOperation::OrNative
                                            | SqlBatchHostOperation::IsTrueWithNullNative
                                            | SqlBatchHostOperation::UnaryNotNative
                                            | SqlBatchHostOperation::IsNullNative
                                            | SqlBatchHostOperation::IsTrueNative
                                            | SqlBatchHostOperation::IsFalseNative
                                            | SqlBatchHostOperation::IsNotTrueNative
                                            | SqlBatchHostOperation::IsNotFalseNative
                                            | SqlBatchHostOperation::IsUnknownNative
                                            | SqlBatchHostOperation::IsNotUnknownNative
                                            | SqlBatchHostOperation::CompareNative(_)
                                            | SqlBatchHostOperation::NullSafeEqNative
                                            | SqlBatchHostOperation::NullIfNative
                                            | SqlBatchHostOperation::InNative
                                            | SqlBatchHostOperation::IntervalNative
                                            | SqlBatchHostOperation::ExtremumNative
                                            | SqlBatchHostOperation::TimestampDiffNative { .. }
                                            | SqlBatchHostOperation::ClockNative { .. }
                                            | SqlBatchHostOperation::CalendarNative { .. }
                                            | SqlBatchHostOperation::DurationConstructionNative(_)
                                            | SqlBatchHostOperation::VectorNative(_)
                                            | SqlBatchHostOperation::CryptoNative(_)
                                            | SqlBatchHostOperation::EagerValuesNative
                                            | SqlBatchHostOperation::CompressionNative(_)
                                            | SqlBatchHostOperation::ByteUnaryNative(_)
                                            | SqlBatchHostOperation::MiscNative { .. }
                                            | SqlBatchHostOperation::ValueIdentityNative { .. }
                                            | SqlBatchHostOperation::Concat
                                            | SqlBatchHostOperation::ConcatWs
                                            | SqlBatchHostOperation::GroupingNative
                                            | SqlBatchHostOperation::LikeNative { .. }
                                            | SqlBatchHostOperation::TsoNative(_)
                                            | SqlBatchHostOperation::EpochNative { .. }
                                            | SqlBatchHostOperation::ExtractNative { .. }
                                            | SqlBatchHostOperation::DateArithmeticNative { .. }
                                            | SqlBatchHostOperation::BoundedStalenessNative { .. }
                                            | SqlBatchHostOperation::StrToDateNative { .. }
                                            | SqlBatchHostOperation::JsonSumCrc32Native { .. }
                                            | SqlBatchHostOperation::CharsetNative(_)
                                            | SqlBatchHostOperation::AbsNative
                                            | SqlBatchHostOperation::BitwiseNative(_)
                                            | SqlBatchHostOperation::XorNative
                                            | SqlBatchHostOperation::UnarySignNative(_)
                                            | SqlBatchHostOperation::AddNative
                                            | SqlBatchHostOperation::SubtractNative
                                            | SqlBatchHostOperation::MultiplyNative
                                            | SqlBatchHostOperation::ModuloNative
                                            | SqlBatchHostOperation::DivideNative
                                            | SqlBatchHostOperation::IntegerDivideNative
                                            | SqlBatchHostOperation::AddWindowNative
                                            | SqlBatchHostOperation::SubtractWindowNative
                                            | SqlBatchHostOperation::MultiplyWindowNative
                                            | SqlBatchHostOperation::ModuloWindowNative
                                            | SqlBatchHostOperation::DivideWindowNative
                                            | SqlBatchHostOperation::IntegerDivideWindowNative
                                            | SqlBatchHostOperation::AbsIntWindowNative
                                            | SqlBatchHostOperation::CastDecimalWindowNative
                                            | SqlBatchHostOperation::CastJsonWindowNative
                                    )
                                {
                                    projected(&native_bytes_type(), new_collation)?
                                } else {
                                    projected(
                                        argument.static_type().ok_or_else(|| {
                                            binding("host argument lost its type")
                                        })?,
                                        new_collation,
                                    )?
                                },
                            );
                        }
                        let diagnostic = checked
                            .diagnostics
                            .get(next_diagnostic)
                            .filter(|p| p.ordinal == ordinal);
                        let numeric = checked
                            .numerics
                            .get(next_numeric)
                            .filter(|p| p.ordinal == ordinal);
                        let mirror_count = diagnostic.map_or(0, |p| p.bindings)
                            + numeric.map_or(0, |p| p.extras.len());
                        if mirror_count != 0 || diagnostic.is_some() {
                            reserve(&mut arguments, mirror_count)?;
                            for _ in 0..mirror_count {
                                arguments.push(projected(&native_bytes_type(), new_collation)?);
                            }
                        }
                        let slot = host_entries.len();
                        reserve(&mut host_entries, 1)?;
                        reserve(&mut host_metadata, 1)?;
                        reserve(&mut host_probes, 1)?;
                        host_probes.push(source_prefix_probe(sql, operation).cloned());
                        if item.view.diagnostic() {
                            reserve(&mut diagnostic_slots, 1)?;
                            diagnostic_slots.push(slot);
                        }
                        if let Some(pending) = numeric {
                            let mut child = ordinal + 1;
                            let mut producers = Vec::new();
                            reserve(&mut producers, 2 + pending.extras.len())?;
                            for _ in 0..2 + pending.extras.len() {
                                producers.push(child);
                                child = checked[child].subtree_end;
                            }
                            let preparation = pending.requests.map(|requests| {
                                requests
                                    .map(|i| numeric_source::preparation(&checked[producers[i]]))
                            });
                            reserve(&mut numeric_parents, 1)?;
                            numeric_parents.push(CompiledNumericParent {
                                slot,
                                arguments: producers.into_boxed_slice(),
                                requests: pending.requests,
                                preparation,
                                plans: numeric_plans[next_numeric]
                                    .take()
                                    .expect("compiled numeric plans"),
                            });
                            next_numeric += 1;
                        }
                        if let Some(pending) = diagnostic {
                            let mut child = ordinal + 1;
                            for _ in 0..pending.main_arity {
                                child = checked[child].subtree_end;
                            }
                            let mut mirrors = Vec::new();
                            reserve(&mut mirrors, pending.bindings)?;
                            for _ in 0..pending.bindings {
                                mirrors.push(child);
                                child = checked[child].subtree_end;
                            }
                            reserve(&mut diagnostic_parents, 1)?;
                            diagnostic_parents.push(CompiledDiagnosticParent {
                                slot,
                                main_arity: pending.main_arity,
                                arguments: mirrors.into_boxed_slice(),
                                plan: compiled_plans[next_diagnostic]
                                    .take()
                                    .expect("compiled diagnostic plan"),
                            });
                            next_diagnostic += 1;
                        }
                        let child_nodes = |count| -> LocalResult<Vec<usize>> {
                            let mut arguments = Vec::new();
                            reserve(&mut arguments, count)?;
                            let mut child = ordinal
                                .checked_add(1)
                                .ok_or_else(|| resource("host child ordinal overflow"))?;
                            for _ in 0..count {
                                arguments.push(child);
                                child = checked
                                    .get(child)
                                    .ok_or_else(|| binding("host child absent"))?
                                    .subtree_end;
                            }
                            Ok(arguments)
                        };
                        host_metadata.push(match operation {
                            SqlBatchHostOperation::Concat | SqlBatchHostOperation::ConcatWs if item.view.value_role().is_some() => {
                                let arguments = child_nodes(*arity)?;
                                NativeHostMetadata::ConcatValues {
                                    arguments: arguments.into_boxed_slice(),
                                    kind: if operation == SqlBatchHostOperation::Concat { tidb_query_expr::local::ConcatKind::Concat } else { tidb_query_expr::local::ConcatKind::ConcatWs },
                                    result_type: item.sql.map(snapshot_field_type),
                                    output_node: ordinal,
                                    output_role: item.view.value_role().expect("checked CONCAT value view"),
                                }
                            }
                            SqlBatchHostOperation::LikeNative { .. } => {
                                let mut arguments = [0; 3];
                                let mut child = ordinal
                                    .checked_add(1)
                                    .ok_or_else(|| resource("LIKE child overflow"))?;
                                for argument in &mut arguments {
                                    *argument = child;
                                    child = checked
                                        .get(child)
                                        .ok_or_else(|| binding("LIKE child missing"))?
                                        .subtree_end;
                                }
                                NativeHostMetadata::Like {
                                    arguments,
                                    escape_type: sql.args[2].static_type().map(snapshot_field_type),
                                    invocation: sql.batch_like_invocation(),
                                    result_type: item.sql.map(snapshot_field_type),
                                    output_node: ordinal,
                                    output_role: item
                                        .view
                                        .value_role()
                                        .ok_or_else(|| binding("LIKE result role missing"))?,
                                }
                            }
                            SqlBatchHostOperation::EagerValuesNative if sql.pb_lazy_values().is_some() => {
                                let kernel=sql.pb_lazy_values().expect("actual lazy owner");
                                debug_assert_eq!(Some(kernel.signature()),sql.pb_signature());
                                let arguments = child_nodes(source_data_arity(sql))?;
                                NativeHostMetadata::PbLazy(Box::new(hosts::NativePbLazy {
                                    kernel,arguments:arguments.into_boxed_slice(),logical_arity:sql.args.len(),
                                    name:sql.func_name.lowercase().into(),result_type:item.sql.map(snapshot_field_type),
                                    integer_batch_result:item.view==NativeView::BulkRoot(NativeEvalType::Int),
                                    output_node:ordinal,output_role:item.view.value_role().ok_or_else(||binding("PB lazy output view absent"))?,
                                }))
                            }
                            SqlBatchHostOperation::EagerValuesNative if sql.pb_boolean_values().is_some() => {
                                let kernel=sql.pb_boolean_values().expect("actual Boolean owner");
                                debug_assert_eq!(Some(kernel.signature()),sql.pb_signature());
                                let arguments = child_nodes(source_data_arity(sql))?;
                                NativeHostMetadata::PbBoolean(Box::new(hosts::NativePbBoolean {
                                    kernel,arguments:arguments.into_boxed_slice(),logical_arity:sql.args.len(),
                                    name:sql.func_name.lowercase().into(),result_type:item.sql.map(snapshot_field_type),
                                    integer_batch_result:item.view==NativeView::BulkRoot(NativeEvalType::Int),
                                    output_node:ordinal,output_role:item.view.value_role().ok_or_else(||binding("PB Boolean output view absent"))?,
                                }))
                            }
                            SqlBatchHostOperation::EagerValuesNative if sql.pb_binary_values().is_some() && !native_no_argument_header_call(sql) => {
                                let kernel=sql.pb_binary_values().expect("actual selected binary owner");
                                debug_assert_eq!(Some(kernel.signature()),sql.pb_signature());
                                let arguments = child_nodes(source_data_arity(sql))?;
                                NativeHostMetadata::PbBinary(Box::new(hosts::NativePbBinary {
                                    kernel,logical_arity:sql.args.len(),arguments:arguments.into_boxed_slice(),
                                    source_types:[0,1].map(|i|sql.args.get(i).and_then(Expression::static_type).map(snapshot_field_type)),
                                    binary_literal:[0,1].map(|i|sql.args.get(i).is_some_and(crate::scalar_function::dynamic_diagnostic::saved_binary_literal)),
                                    operands:kernel.domain().map(|_|crate::ops::Operands::of(&sql.args[0],&sql.args[1]).facts()),
                                    name:sql.func_name.lowercase().into(),collation:sql.derived_collation(),result_type:item.sql.map(snapshot_field_type),
                                    integer_batch_result:item.view==NativeView::BulkRoot(NativeEvalType::Int),output_node:ordinal,output_role:item.view.value_role().ok_or_else(||binding("PB binary output view absent"))?,
                                }))
                            }
                            SqlBatchHostOperation::EagerValuesNative if native_pb_eager_values(sql) && !native_no_argument_header_call(sql) => {
                                let arguments = child_nodes(*arity)?;
                                let kernel=if let Some(kernel)=sql.pb_cast_values(){
    let child=checked.get(ordinal+1).ok_or_else(||binding("PB cast child absent"))?;
    hosts::PbValueKernel::Cast(Box::new(hosts::PbCastKernel{kernel,source_type:sql.args[0].static_type().map(snapshot_field_type),input:numeric_source::preparation(child)}))
}else if let Some(kernel)=sql.pb_eager_values(){hosts::PbValueKernel::Eager(kernel)}else{hosts::PbValueKernel::Json(Box::new(sql.pb_json_values().expect("actual selected JSON owner")))};
                                debug_assert_eq!(Some(kernel.signature()),sql.pb_signature());
                                NativeHostMetadata::PbEagerValues(Box::new(hosts::NativePbEagerValues {
                                    arguments:arguments.into_boxed_slice(), kernel,
                                    name:sql.func_name.lowercase().into(), result_type:item.sql.map(snapshot_field_type),
                                    integer_batch_result:item.view==NativeView::BulkRoot(NativeEvalType::Int),
                                    output_node:ordinal,output_role:item.view.value_role().ok_or_else(||binding("PB eager output view absent"))?,
                                }))
                            }
                            SqlBatchHostOperation::EagerValuesNative if native_regexp_call(sql) => {
                                let arguments = child_nodes(*arity)?;
                                NativeHostMetadata::RegexpValues(Box::new(hosts::NativeRegexpValues{
                                    arguments:arguments.into_boxed_slice(),source_types:sql.args.iter().map(|arg|arg.static_type().map(snapshot_field_type)).collect(),
                                    operator:!pb_regexp_owner(sql) && sql.func_name.lowercase()=="regexp",protobuf_result:pb_regexp_owner(sql),integer_batch_result:pb_regexp_owner(sql) && item.view==NativeView::BulkRoot(NativeEvalType::Int),invocation:sql.batch_regexp_like_invocation(),collation:sql.derived_collation(),
                                    result_type:item.sql.map(snapshot_field_type),output_node:ordinal,output_role:item.view.value_role().ok_or_else(||binding("regexp output view absent"))?,
                                }))
                            }
                            SqlBatchHostOperation::EagerValuesNative if native_weight_string_call(sql) => {
                                let arguments = child_nodes(*arity)?;
                                NativeHostMetadata::WeightString(Box::new(hosts::NativeWeightString {
                                    arguments:arguments.into_boxed_slice(),logical_arity:sql.args.len(),
                                    source_type:sql.args[0].static_type().map(snapshot_field_type),
                                    explicit_collation:crate::collation_derive::coercibility_of(&sql.args[0])==crate::expr_collation::Coercibility::EXPLICIT,
                                    result_type:item.sql.map(snapshot_field_type),output_node:ordinal,
                                    output_role:item.view.value_role().ok_or_else(||binding("weight output view absent"))?,
                                }))
                            }
                            SqlBatchHostOperation::EagerValuesNative if native_no_argument_header_call(sql) => {
                                NativeHostMetadata::HeaderOutcome {
                                    kind: crate::scalar_function::header_value::HeaderKind::for_function(sql).expect("checked original header"),
                                    logical_arity: sql.args.len(),
                                    property_value: crate::scalar_function::header_value::HeaderKind::for_function(sql).and_then(|kind|kind.property()).map(|kind|kind.capture(&sql.args[0])),
                                    result_type: item.sql.map(snapshot_field_type),
                                    output_node: ordinal,
                                    output_role: item.view.value_role().ok_or_else(||binding("header output view missing"))?,
                                }
                            }
                            SqlBatchHostOperation::EagerValuesNative if native_json_schema_call(sql) => {
                                let arguments = child_nodes(*arity)?;
                                NativeHostMetadata::JsonSchema(Box::new(NativeJsonSchema{
                                    arguments:arguments.into_boxed_slice(),
                                    cacheable:sql.args.first().is_some_and(|a|a.const_level()>=crate::expression::ConstLevel::ONLY_IN_CONTEXT),
                                    cache:sql.shared_json_schema_cache(),result_type:item.sql.map(snapshot_field_type),output_node:ordinal,
                                    output_role:item.view.value_role().ok_or_else(||binding("schema output view missing"))?,
                                }))
                            }
                            SqlBatchHostOperation::EagerValuesNative if native_final_state_call(sql) => {
                                let arguments = child_nodes(*arity)?;
                                NativeHostMetadata::FinalState(Box::new(NativeFinalState {
                                    arguments:arguments.into_boxed_slice(),
                                    name:match sql.func_name.lowercase() { "rand"=>"rand", "tidb_is_ddl_owner"=>"tidb_is_ddl_owner", "aes_encrypt"=>"aes_encrypt", "aes_decrypt"=>"aes_decrypt", _=>unreachable!() },
                                    function_key:std::ptr::from_ref(sql) as usize,
                                    constant_seed:matches!(sql.args.first(),Some(Expression::Constant(_))),
                                    result_type:item.sql.map(snapshot_field_type), output_node:ordinal,
                                    output_role:item.view.value_role().ok_or_else(||binding("final state output role absent"))?,
                                }))
                            }
                            SqlBatchHostOperation::EagerValuesNative if native_stateful_call(sql) => {
                                let arguments = child_nodes(*arity)?;
                                NativeHostMetadata::Stateful(Box::new(NativeStateful {
                                    repeated_subtree: if sql.func_name.lowercase()=="benchmark" && *arity==2 { Some(arguments[1]..checked[arguments[1]].subtree_end) } else { None },
                                    arguments:arguments.into_boxed_slice(),
                                    descriptor:crate::scalar_function::stateful::Descriptor::capture(sql).expect("stateful descriptor"),
                                    result_type:item.sql.map(snapshot_field_type),output_node:ordinal,
                                    output_role:item.view.value_role().ok_or_else(||binding("stateful result role absent"))?,
                                }))
                            }
                            SqlBatchHostOperation::EagerValuesNative if native_cached_list_call(sql) => {
                                let second = checked.get(ordinal + 1).ok_or_else(|| binding("cached needle absent"))?.subtree_end;
                                NativeHostMetadata::CachedList(Box::new(NativeCachedList {
                                    arguments: [ordinal + 1, second],
                                    cacheable: sql.args[1].const_level() >= crate::expression::ConstLevel::ONLY_IN_CONTEXT,
                                    cache: sql.shared_find_in_set_cache(),
                                    collation: sql.derived_collation(),
                                    result_type: item.sql.map(snapshot_field_type),
                                    output_node: ordinal,
                                    output_role: item.view.value_role().ok_or_else(|| binding("cached list output role absent"))?,
                                }))
                            }
                            SqlBatchHostOperation::EagerValuesNative=>{
                                let arguments = child_nodes(*arity)?;
                                NativeHostMetadata::EagerValues{
                                    arguments:arguments.into_boxed_slice(),
                                    kernel:crate::scalar_function::eager_values::EagerValueKernel::capture(sql).with_math_diagnostic(
                                        if matches!(sql.func_name.lowercase(),"abs"|"cot"|"exp"|"pow"|"power") && item.math_render_bound.is_some() {
                                            crate::scalar_function::math_diagnostic::MathDiagnostic::capture(sql,item.subtree_end-ordinal)
                                        } else {None}),
                                    probe:sql.identity_constant_probe().cloned(),
                                    result_type:item.sql.map(snapshot_field_type),output_node:ordinal,
                                    output_role:item.view.value_role().ok_or_else(||binding("eager output view absent"))?,
                                }
                            }
                            operation @ (SqlBatchHostOperation::CryptoNative(_)
                            | SqlBatchHostOperation::CompressionNative(_)
                            | SqlBatchHostOperation::ByteUnaryNative(_)
                            | SqlBatchHostOperation::MiscNative { .. }
                            | SqlBatchHostOperation::ValueIdentityNative { .. }
                            | SqlBatchHostOperation::GroupingNative
                            | SqlBatchHostOperation::VectorNative(_)
                            | SqlBatchHostOperation::DurationConstructionNative(_)
                            | SqlBatchHostOperation::CalendarNative { .. }
                            | SqlBatchHostOperation::ClockNative { .. }
                            | SqlBatchHostOperation::TsoNative(_)
                            | SqlBatchHostOperation::EpochNative { .. }
                            | SqlBatchHostOperation::ExtractNative { .. }
                            | SqlBatchHostOperation::TimestampDiffNative { .. }
                            | SqlBatchHostOperation::BoundedStalenessNative { .. }
                            | SqlBatchHostOperation::StrToDateNative { .. }) => {
                                use hosts::{NativeArgumentKernel as K, NativeArgumentNodes as N};
                                let source_type = |i: usize| snapshot_field_type(sql.args[i].static_type().expect("checked native argument type"));
                                let kernel = match operation {
                                    SqlBatchHostOperation::CryptoNative(op) => K::Crypto { operation: op, probe: sql.identity_constant_probe().cloned() },
                                    SqlBatchHostOperation::CompressionNative(op) => K::Compression { operation: op, probe: sql.identity_constant_probe().cloned() },
                                    SqlBatchHostOperation::ByteUnaryNative(op) => K::ByteUnary { operation: op, probe: sql.identity_constant_probe().cloned() },
                                    SqlBatchHostOperation::MiscNative { operation: op, .. } => K::Misc { operation: op, probe: sql.identity_constant_probe().cloned() },
                                    SqlBatchHostOperation::ValueIdentityNative { name_const } => K::Identity { name_const: name_const, probe: sql.identity_constant_probe().cloned() },
                                    SqlBatchHostOperation::GroupingNative => K::Grouping(sql.shared_grouping_metadata()),
                                    SqlBatchHostOperation::VectorNative(op) => K::Vector(op),
                                    SqlBatchHostOperation::DurationConstructionNative(operation) => {
                                        let mut types = Vec::new();
                                        reserve(&mut types, operation.arity())?;
                                        types.extend((0..sql.args.len()).map(source_type));
                                        K::DurationConstruction { operation, argument_types: types.into_boxed_slice() }
                                    }
                                    SqlBatchHostOperation::CalendarNative { operation, .. } => K::Calendar { operation, source_type: source_type(0) },
                                    SqlBatchHostOperation::ClockNative { operation, .. } => K::Clock { operation },
                                    SqlBatchHostOperation::TsoNative(operation) => K::Tso { operation, argument_type: sql.args.first().map(|_| source_type(0)) },
                                    SqlBatchHostOperation::EpochNative { from, .. } => K::Epoch { from },
                                    SqlBatchHostOperation::TimestampDiffNative { stop_on_null, .. } => K::TimestampDiff { stop_on_null, argument_types: std::array::from_fn(source_type) },
                                    SqlBatchHostOperation::ExtractNative { composite, .. } => K::Extract {
                                        source_type: source_type(usize::from(!composite)),
                                        unit: native_extract_call(sql).ok_or_else(|| binding("EXTRACT source keyword lost"))?.unit().map(Into::into),
                                    },
                                    SqlBatchHostOperation::BoundedStalenessNative { .. } => K::BoundedStaleness { argument_types: std::array::from_fn(source_type) },
                                    SqlBatchHostOperation::StrToDateNative { output } => K::StrToDate { output },
                                    _ => unreachable!(),
                                };
                                let mut child = ordinal.checked_add(1).ok_or_else(|| resource("native argument ordinal overflow"))?;
                                let mut next_node = || -> LocalResult<usize> {
                                    let node = child;
                                    child = checked.get(child).ok_or_else(|| binding("native argument child absent"))?.subtree_end;
                                    Ok(node)
                                };
                                let arguments = match *arity {
                                    0 => N::Empty,
                                    1 => N::One(next_node()?),
                                    2 => N::Two([next_node()?, next_node()?]),
                                    3 => N::Three([next_node()?, next_node()?, next_node()?]),
                                    _ => {
                                        let mut nodes = Vec::new();
                                        reserve(&mut nodes, *arity)?;
                                        for _ in 0..*arity { nodes.push(next_node()?); }
                                        N::Many(nodes.into_boxed_slice())
                                    }
                                };
                                let result_type = if matches!(kernel, K::Grouping(_)) {
                                    item.sql.map(snapshot_field_type)
                                } else { Some(snapshot_field_type(item.declared_sql())) };
                                let output_role = item.view.value_role();
                                if output_role.is_none() && !matches!(kernel, K::TimestampDiff { .. } | K::Extract { .. } | K::BoundedStaleness { .. } | K::StrToDate { .. }) {
                                    return Err(binding("native argument output view missing").into());
                                }
                                NativeHostMetadata::Arguments(hosts::NativeArguments {
                                    arguments, kernel, result_type, output_node: ordinal, output_role,
                                })
                            }
                            SqlBatchHostOperation::DateArithmeticNative { output } => {
                                let first = ordinal
                                    .checked_add(1)
                                    .ok_or_else(|| resource("date arithmetic ordinal overflow"))?;
                                let second = checked
                                    .get(first)
                                    .ok_or_else(|| binding("date arithmetic first source absent"))?
                                    .subtree_end;
                                let (unit, sign) =
                                    native_date_arithmetic_call(sql).ok_or_else(|| {
                                        binding("date arithmetic source keyword lost")
                                    })?;
                                let result_fsp = crate::tikv::date_add_result_fsp_checked(
                                    unit,
                                    sql.args[0].static_type(),
                                    sql.args[1].static_type(),
                                )?;
                                NativeHostMetadata::Arguments(hosts::NativeArguments { arguments: ([first, second]).into(), kernel: hosts::NativeArgumentKernel::DateArithmetic { argument_types: std::array::from_fn(|i|sql.args[i].static_type().map(snapshot_field_type)), unit: unit.into(), sign: sign, result_fsp: result_fsp, output: output }, result_type: item.sql.map(snapshot_field_type), output_node: ordinal, output_role: item.view.value_role() })
                            }
                            SqlBatchHostOperation::JsonSumCrc32Native { .. } => {
                                NativeHostMetadata::Arguments(hosts::NativeArguments { arguments: (ordinal.checked_add(1).ok_or_else(|| {
                                        resource("JSON checksum argument ordinal overflow")
                                    })?).into(), kernel: hosts::NativeArgumentKernel::JsonSumCrc32, result_type: Some(snapshot_field_type(item.declared_sql())), output_node: ordinal, output_role: item.view.value_role() })
                            }
                            SqlBatchHostOperation::CharsetNative(operation) => {
                                let mut arguments = Vec::new();
                                reserve(&mut arguments, *arity)?;
                                let mut child = ordinal
                                    .checked_add(1)
                                    .ok_or_else(|| resource("charset child ordinal overflow"))?;
                                let source = checked
                                    .get(child)
                                    .ok_or_else(|| binding("charset source absent"))?;
                                let source_type = snapshot_field_type(source.declared_sql());
                                for _ in 0..*arity {
                                    let source = checked
                                        .get(child)
                                        .ok_or_else(|| binding("charset source node absent"))?;
                                    arguments.push(child);
                                    child = source.subtree_end;
                                }
                                NativeHostMetadata::Charset(hosts::NativeCharsetMetadata {
                                    operation,
                                    arguments: arguments.into_boxed_slice(),
                                    source_type,
                                    explicit_collation: crate::collation_derive::coercibility_of(
                                        &sql.args[0],
                                    )
                                        == crate::expr_collation::Coercibility::EXPLICIT,
                                    result_type: snapshot_field_type(item.declared_sql()),
                                    output_node: ordinal,
                                    output_role: item.view.value_role(),
                                })
                            }
                            SqlBatchHostOperation::ExtremumNative => {
                                let mut arguments = Vec::new();
                                reserve(&mut arguments, *arity)?;
                                let mut decimals = Vec::new();
                                reserve(&mut decimals, *arity)?;
                                let mut child = ordinal
                                    .checked_add(1)
                                    .ok_or_else(|| resource("extremum child ordinal overflow"))?;
                                for _ in 0..*arity {
                                    let source = checked
                                        .get(child)
                                        .ok_or_else(|| binding("extremum source absent"))?;
                                    arguments.push(child);
                                    decimals.push(
                                        source.sql.map_or(tidb_datatype::UNSPECIFIED_LENGTH, |field| {
                                            if field.eval_type() == NativeEvalType::Int { 0 } else { field.decimal() }
                                        }),
                                    );
                                    child = source.subtree_end;
                                }
                                NativeHostMetadata::Extremum(hosts::NativeExtremumMetadata {
                                    arguments: arguments.into_boxed_slice(),
                                    signature: crate::rewriter::result_type::gl_signature(
                                        &sql.args,
                                    ),
                                    decimals: decimals.into_boxed_slice(),
                                    all_constant: sql.args.iter().all(|arg| {
                                        arg.const_level() == crate::expression::ConstLevel::STRICT
                                    }),
                                    collation: sql.derived_collation(),
                                    greatest: sql.func_name.lowercase() == "greatest",
                                    result_type: item.sql.map(snapshot_field_type),
                                    output_node: ordinal,
                                    output_role: item
                                        .view
                                        .value_role()
                                        .ok_or_else(|| binding("extremum result role missing"))?,
                                })
                            }
                            SqlBatchHostOperation::IntervalNative => {
                                let mut arguments = Vec::new();
                                reserve(&mut arguments, *arity)?;
                                let mut child = ordinal
                                    .checked_add(1)
                                    .ok_or_else(|| resource("INTERVAL child ordinal overflow"))?;
                                for _ in 0..*arity {
                                    let source = checked
                                        .get(child)
                                        .ok_or_else(|| binding("INTERVAL source node absent"))?;
                                    arguments.push(hosts::NativeIntervalArgument {
                                        node: child,
                                        field: source.sql.map(snapshot_field_type),
                                    });
                                    child = source.subtree_end;
                                }
                                let search = tidb_query_expr::NativeIntervalSearch::new(
                                    arguments.iter().map(|argument| {
                                        argument.field.as_ref().map(crate::tikv::interval_field_metadata)
                                    }),
                                )
                                .map_err(crate::tikv::interval_value_error)?;
                                NativeHostMetadata::Interval(hosts::NativeIntervalMetadata {
                                    arguments: arguments.into_boxed_slice(),
                                    search,
                                    result_type: item.sql.map(snapshot_field_type),
                                    output_node: ordinal,
                                    output_role: item
                                        .view
                                        .value_role()
                                        .ok_or_else(|| binding("INTERVAL result role missing"))?,
                                })
                            }
                            SqlBatchHostOperation::InNative => {
                                let mut arguments = Vec::new();
                                reserve(&mut arguments, *arity)?;
                                let mut child = ordinal
                                    .checked_add(1)
                                    .ok_or_else(|| resource("IN child ordinal overflow"))?;
                                for _ in 0..*arity {
                                    let source = checked
                                        .get(child)
                                        .ok_or_else(|| binding("IN source missing"))?;
                                    arguments.push(hosts::NativeInArgument {
                                        node: child,
                                        field: source.sql.map(snapshot_field_type),
                                        facts: crate::ops::OperandFacts {
                                            eval_type: source.sql.map(FieldType::eval_type),
                                            unsigned: source.sql.is_some_and(FieldType::is_unsigned),
                                            constant: source.folds_to_constant,
                                            duration_column: matches!(
                                                source.node.kind(),
                                                BorrowedNodeKind::Column(_)
                                            ) && source.sql.is_some_and(|field| field.code()==FieldTypeCode::Duration),
                                        },
                                    });
                                    child = source.subtree_end;
                                }
                                let (stored, dynamic, has_null) = sql.native_in_cache();
                                let prepared = if let Some(stored) = stored {
                                    let mut keys = std::collections::HashSet::new();
                                    keys.try_reserve(stored.len()).map_err(|error| {
                                        LocalError::ResourceLimit(error.to_string())
                                    })?;
                                    for key in stored {
                                        let mut owned = Vec::new();
                                        reserve(&mut owned, key.len())?;
                                        owned.extend_from_slice(key);
                                        keys.insert(owned);
                                    }
                                    Some(keys)
                                } else {
                                    None
                                };
                                NativeHostMetadata::In(hosts::NativeInMetadata {
                                    arguments: arguments.into_boxed_slice(),
                                    prepared,
                                    dynamic: {
                                        let mut copied = Vec::new();
                                        reserve(&mut copied, dynamic.len())?;
                                        copied.extend_from_slice(dynamic);
                                        copied.into_boxed_slice()
                                    },
                                    has_null,
                                    collation: sql.derived_collation(),
                                    result_type: item.sql.map(snapshot_field_type),
                                    output_node: ordinal,
                                    output_role: item
                                        .view
                                        .value_role()
                                        .ok_or_else(|| binding("IN result role missing"))?,
                                })
                            }
                            SqlBatchHostOperation::CoalesceNative
                            | SqlBatchHostOperation::IfNullNative
                            | SqlBatchHostOperation::IfNative
                            | SqlBatchHostOperation::CaseNative
                            | SqlBatchHostOperation::AndNative
                            | SqlBatchHostOperation::OrNative
                            | SqlBatchHostOperation::IsTrueWithNullNative
                            | SqlBatchHostOperation::UnaryNotNative
                            | SqlBatchHostOperation::IsNullNative
                            | SqlBatchHostOperation::IsTrueNative
                            | SqlBatchHostOperation::IsFalseNative
                            | SqlBatchHostOperation::IsNotTrueNative
                            | SqlBatchHostOperation::IsNotFalseNative
                            | SqlBatchHostOperation::IsUnknownNative
                            | SqlBatchHostOperation::IsNotUnknownNative => {
                                let mut argument_nodes = Vec::new();
                                reserve(&mut argument_nodes, *arity)?;
                                let mut child = ordinal
                                    .checked_add(1)
                                    .ok_or_else(|| resource("nullable child ordinal overflow"))?;
                                for _ in 0..*arity {
                                    argument_nodes.push(child);
                                    child = checked
                                        .get(child)
                                        .ok_or_else(|| binding("nullable child source missing"))?
                                        .subtree_end;
                                }
                                NativeHostMetadata::Selection {
                                    name: sql.func_name.lowercase().into(),
                                    result_type: item.sql.map(snapshot_field_type),
                                    argument_nodes: argument_nodes.into_boxed_slice(),
                                    output_node: ordinal,
                                    output_role: item.view.value_role().ok_or_else(|| {
                                        binding("nullable control lacks native result role")
                                    })?,
                                }
                            }
                            operation @ (SqlBatchHostOperation::AddNative
                            | SqlBatchHostOperation::SubtractNative
                            | SqlBatchHostOperation::MultiplyNative
                            | SqlBatchHostOperation::ModuloNative
                            | SqlBatchHostOperation::DivideNative
                            | SqlBatchHostOperation::IntegerDivideNative
                            | SqlBatchHostOperation::AddWindowNative
                            | SqlBatchHostOperation::SubtractWindowNative
                            | SqlBatchHostOperation::MultiplyWindowNative
                            | SqlBatchHostOperation::ModuloWindowNative
                            | SqlBatchHostOperation::DivideWindowNative
                            | SqlBatchHostOperation::IntegerDivideWindowNative) => {
                                arithmetic_host_metadata(operation, sql, item, ordinal, &checked)?
                            }
                            operation @ (SqlBatchHostOperation::AbsIntWindowNative
                            | SqlBatchHostOperation::CastDecimalWindowNative
                            | SqlBatchHostOperation::CastJsonWindowNative) => {
                                let argument = ordinal
                                    .checked_add(1)
                                    .ok_or_else(|| resource("unary child ordinal overflow"))?;
                                let child = &checked[argument];
                                let source = match &sql.args[0] {
                                    _ if row_typed_context_target(&sql.args[0], child.view).is_some() => WindowArithmeticSource::TypedReady,
                                    Expression::Constant(_) => WindowArithmeticSource::Literal,
                                    Expression::Column(_) => WindowArithmeticSource::Column,
                                    Expression::ScalarFunction(function)
                                        if child.view.bulk_target()
                                            == Some(NativeEvalType::Int)
                                            && window_value_operation(function, child.view)
                                                .is_some() =>
                                    {
                                        WindowArithmeticSource::IntegerReady
                                    }
                                    _ => WindowArithmeticSource::Computed,
                                };
                                let overflow = if operation
                                    == SqlBatchHostOperation::AbsIntWindowNative
                                    && item.math_render_bound.is_some()
                                {
                                    crate::scalar_function::static_math_overflow_expression(sql)
                                } else {
                                    None
                                };
                                if overflow.as_ref().is_some_and(|text| {
                                    item.math_render_bound
                                        .is_none_or(|bound| text.len() > bound)
                                }) {
                                    return Err(binding(
                                        "unary diagnostic exceeds its admitted bound",
                                    )
                                    .into());
                                }
                                NativeHostMetadata::UnaryWindow {
                                    operation,
                                    protobuf_owner: pb_value_owner(sql),
                                    result_type: snapshot_field_type(item.declared_sql()),
                                    source_type: snapshot_field_type(child.declared_sql()),
                                    source,
                                    argument_node: argument,
                                    output_node: ordinal,
                                    output_role: item
                                        .view
                                        .value_role()
                                        .ok_or_else(|| binding("unary result role missing"))?,
                                    overflow_expression: overflow.map(String::into_boxed_str),
                                }
                            }
                            operation @ (SqlBatchHostOperation::BitwiseNative(_)
                            | SqlBatchHostOperation::XorNative
                            | SqlBatchHostOperation::UnarySignNative(_)) => {
                                let left = ordinal
                                    .checked_add(1)
                                    .ok_or_else(|| resource("bitwise source ordinal overflow"))?;
                                let right = if operation.accepts_arity(2) {
                                    Some(
                                        checked
                                            .get(left)
                                            .ok_or_else(|| binding("bitwise left source missing"))?
                                            .subtree_end,
                                    )
                                } else {
                                    None
                                };
                                let operation = match operation {
                                    SqlBatchHostOperation::BitwiseNative(operation) => {
                                        NativeRawValueOperation::Bitwise {
                                            operation,
                                            unsigned: [
                                                checked[left].sql.is_some_and(FieldType::is_unsigned),
                                                right.is_some_and(|right| {
                                                    checked[right].sql.is_some_and(FieldType::is_unsigned)
                                                }),
                                            ],
                                        }
                                    }
                                    SqlBatchHostOperation::UnarySignNative(sign) => {
                                        NativeRawValueOperation::UnarySign {
                                            sign,
                                            unsigned: checked[left].sql.is_some_and(FieldType::is_unsigned),
                                            constant: checked[left].folds_to_constant,
                                        }
                                    }
                                    SqlBatchHostOperation::XorNative => {
                                        NativeRawValueOperation::LogicalXor
                                    }
                                    _ => unreachable!("closed raw value operations"),
                                };
                                NativeHostMetadata::RawValues {
                                    operation,
                                    // Deferred typed wrappers already normalize their actual
                                    // child. Only this direct original batch root needs it here.
                                    integer_batch: matches!(item.view,NativeView::BulkRoot(NativeEvalType::Int))
                                        .then_some(item.sql.is_some_and(FieldType::is_unsigned)),
                                    name: sql.func_name.lowercase().into(),
                                    result_type: item.sql.map(snapshot_field_type),
                                    left_node: left,
                                    right_node: right,
                                    output_node: ordinal,
                                    output_role: item
                                        .view
                                        .value_role()
                                        .ok_or_else(|| binding("bitwise result role missing"))?,
                                }
                            }
                            SqlBatchHostOperation::AbsNative => {
                                let expression = if item.math_render_bound.is_some() {
                                    crate::scalar_function::static_math_overflow_expression(sql)
                                } else {
                                    None
                                };
                                if expression.as_ref().is_some_and(|text| {
                                    item.math_render_bound
                                        .is_none_or(|bound| text.len() > bound)
                                }) {
                                    return Err(binding(
                                        "math diagnostic exceeds its admitted bound",
                                    )
                                    .into());
                                }
                                NativeHostMetadata::Math {
                                    name: sql.func_name.lowercase().into(),
                                    result_type: snapshot_field_type(item.declared_sql()),
                                    argument_node: ordinal
                                        .checked_add(1)
                                        .ok_or_else(|| resource("math child ordinal overflow"))?,
                                    output_node: ordinal,
                                    output_role: item
                                        .view
                                        .value_role()
                                        .ok_or_else(|| binding("math result role missing"))?,
                                    overflow_expression: expression.map(String::into_boxed_str),
                                }
                            }
                            operation @ (SqlBatchHostOperation::CompareNative(_)
                            | SqlBatchHostOperation::NullSafeEqNative
                            | SqlBatchHostOperation::NullIfNative) => {
                                let left = ordinal
                                    .checked_add(1)
                                    .ok_or_else(|| resource("comparison child ordinal overflow"))?;
                                let right = checked
                                    .get(left)
                                    .ok_or_else(|| binding("comparison left source missing"))?
                                    .subtree_end;
                                let facts =
                                    |index: usize| -> LocalResult<crate::ops::OperandFacts> {
                                        let child = checked
                                            .get(index)
                                            .ok_or_else(|| binding("comparison source missing"))?;
                                        Ok(crate::ops::OperandFacts {
                                            eval_type: child.sql.map(FieldType::eval_type),
                                            unsigned: child.sql.is_some_and(FieldType::is_unsigned),
                                            constant: child.folds_to_constant,
                                            duration_column: matches!(
                                                child.node.kind(),
                                                BorrowedNodeKind::Column(_)
                                            ) && child.sql.is_some_and(|field| field.code() == FieldTypeCode::Duration),
                                        })
                                    };
                                NativeHostMetadata::Comparison {
                                    operation,
                                    name: sql.func_name.lowercase().into(),
                                    result_type: item.sql.map(snapshot_field_type),
                                    argument_nodes: [left, right],
                                    operands: [facts(left)?, facts(right)?],
                                    collation: sql.derived_collation(),
                                    output_node: ordinal,
                                    output_role: item
                                        .view
                                        .value_role()
                                        .ok_or_else(|| binding("comparison result role missing"))?,
                                }
                            }
                            SqlBatchHostOperation::CastNative => NativeHostMetadata::ValueCast {
                                cast_type: crate::scalar_function::cast_type_of(
                                    native_cast_name(sql).expect("checked native cast"),
                                    item.declared_sql(),
                                )?,
                                name: sql.func_name.lowercase().into(),
                                source_type: snapshot_field_type(
                                    sql.args[0].static_type().expect("checked source"),
                                ),
                                input: if row_typed_context_target(&sql.args[0], checked[ordinal + 1].view).is_some() {
                                    NativeValuePreparation::TypedReady
                                } else if matches!(sql.args[0], Expression::Constant(_)) {
                                    NativeValuePreparation::Literal
                                } else { NativeValuePreparation::Computed },
                                result_type: snapshot_field_type(item.declared_sql()),
                                argument_node: ordinal.checked_add(1).ok_or_else(|| {
                                    resource("native cast argument ordinal overflow")
                                })?,
                                output_node: ordinal,
                                output_role: item
                                    .view
                                    .value_role()
                                    .ok_or_else(|| binding("native cast has no output role"))?,
                            },
                            SqlBatchHostOperation::RegexpLike { .. } => {
                                NativeHostMetadata::Regexp(sql.batch_regexp_like_invocation())
                            }
                            SqlBatchHostOperation::CastCharNative { .. } => {
                                let field =
                                    sql.get_static_type().expect("checked native cast result");
                                NativeHostMetadata::CharNative {
                                    charset: if field.is_binary_string() {
                                        "BINARY"
                                    } else {
                                        field.charset_name()
                                    }
                                    .into(),
                                    source_type: snapshot_field_type(
                                        sql.args[0]
                                            .static_type()
                                            .expect("checked native argument type"),
                                    ),
                                    argument_node: ordinal.checked_add(1).ok_or_else(|| {
                                        resource("native argument source ordinal overflow")
                                    })?,
                                }
                            }
                            SqlBatchHostOperation::CastCharInt { .. } => {
                                let field = sql
                                    .get_static_type()
                                    .expect("checked character cast result");
                                NativeHostMetadata::CharCast {
                                    unsigned: native_int_is_unsigned(&sql.args[0], limits.tree)?,
                                    charset: if field.is_binary_string() {
                                        "BINARY"
                                    } else {
                                        field.charset_name()
                                    }
                                    .into(),
                                }
                            }
                            _ => NativeHostMetadata::Plain,
                        });
                        host_entries.push((
                            if numeric.is_some() {
                                if operation.is_windowed() {
                                    SqlBatchHostOperation::EagerValuesWindowNative
                                } else {
                                    SqlBatchHostOperation::EagerValuesNative
                                }
                            } else if diagnostic.is_some()
                                && operation == SqlBatchHostOperation::AbsNative
                            {
                                SqlBatchHostOperation::EagerValuesNative
                            } else if diagnostic.is_some()
                                && operation == SqlBatchHostOperation::AbsIntWindowNative
                            {
                                SqlBatchHostOperation::EagerValuesWindowNative
                            } else {
                                operation
                            },
                            HostSignature {
                                arg_types: arguments.into_boxed_slice(),
                                return_type: field.clone(),
                            },
                        ));
                        BuildKind::Host {
                            slot,
                            arity: arity
                                .checked_add(mirror_count)
                                .ok_or_else(|| resource("diagnostic graph arity overflow"))?,
                        }
                    }
                }
            }
        };
        build.push(BuildNode {
            field,
            kind: construction,
        });
        nodes.push(NodeRecord {
            sql: item.sql.map(snapshot_field_type),
            kind,
            view: item.view,
        });
    }
    let hosts = if host_entries.is_empty() {
        None
    } else {
        Some(HostCatalog::for_sql_batch(host_entries)?)
    };
    let mut stack = Vec::new();
    reserve(&mut stack, nodes.len())?;
    for node in build.into_iter().rev() {
        let expr = match node.kind {
            BuildKind::Literal(value) => LocalExpr::Constant {
                value,
                field_type: node.field,
                literal_kind: LiteralKind::Typed,
            },
            BuildKind::Input(slot) => LocalExpr::InputSlot {
                slot,
                field_type: node.field,
            },
            BuildKind::Host { slot, arity } => {
                let mut args = Vec::new();
                reserve(&mut args, arity)?;
                for _ in 0..arity {
                    args.push(
                        stack
                            .pop()
                            .ok_or_else(|| binding("missing SQL host argument"))?,
                    );
                }
                let catalog = hosts
                    .as_ref()
                    .ok_or_else(|| binding("SQL host call has no catalog"))?;
                LocalExpr::HostCall {
                    slot: catalog.slot(slot)?,
                    args: args.into_boxed_slice(),
                    return_type: node.field,
                }
            }
            BuildKind::Call {
                function,
                arity,
                policy,
            } => {
                let mut args = Vec::new();
                reserve(
                    &mut args,
                    arity.saturating_add(usize::from(policy.is_some())),
                )?;
                for _ in 0..arity {
                    args.push(
                        stack
                            .pop()
                            .ok_or_else(|| BatchFailure::admission("missing batch call child"))?,
                    );
                }
                if let Some(policy) = policy {
                    args.push(LocalExpr::Constant {
                        value: ScalarValue::Int(Some(policy)),
                        field_type: projected(&native_policy_type(), new_collation)?,
                        literal_kind: LiteralKind::Typed,
                    });
                }
                LocalExpr::Call {
                    function,
                    args: args.into_boxed_slice(),
                    return_type: node.field,
                    metadata: CallMetadata::None,
                }
            }
        };
        stack.push(expr);
    }
    if stack.len() != 1 {
        return Err(BatchFailure::admission("batch source is not one tree"));
    }
    let expr = stack
        .pop()
        .ok_or_else(|| BatchFailure::admission("missing batch root"))?;
    let sites = lowered_call_sites(&expr, sites)?;
    let facts = match &hosts {
        Some(hosts) => {
            BatchFacts::sql_native_batch_with_hosts(&expr, &schema, sites, limits.tree, hosts)?
        }
        None => BatchFacts::sql_native_batch(&expr, &schema, sites, limits.tree)?,
    };
    NativeBatchHosts::validate_typed_cache_slots(&host_metadata, typed_cache_slots)?;
    if let Some(catalog) = &hosts {
        NativeBatchHosts::validate_numeric_parents(catalog, &host_metadata, &numeric_parents)?;
        NativeBatchHosts::validate_diagnostic_parents(
            catalog,
            &host_metadata,
            &diagnostic_parents,
        )?;
        NativeBatchHosts::validate_diagnostic_slots(
            catalog,
            &host_metadata,
            &numeric_parents,
            &diagnostic_parents,
            &diagnostic_slots,
        )?;
    }
    let needs_native_values = nodes.iter().any(|node| node.view.value_role().is_some());
    Ok(SourcePlan {
        unit,
        logical_metadata_bytes: checked.logical_metadata_bytes,
        nodes: nodes.into_boxed_slice(),
        bindings: bindings.into_boxed_slice(),
        expr,
        schema: schema.into_boxed_slice(),
        facts,
        hosts,
        host_metadata: host_metadata.into_boxed_slice(),
        host_probes: host_probes.into_boxed_slice(),
        needs_native_values,
        constant_root_shortcut,
        context_row_only,
        context_policy: if root_type_probe.is_some()
            || !diagnostic_slots.is_empty()
            || !diagnostic_parents.is_empty()
            || !numeric_parents.is_empty()
            || typed_cache_slots != 0
        {
            Some(Box::new(ContextSourcePolicy {
                root_type_probe,
                diagnostic_slots: diagnostic_slots.into_boxed_slice(),
                diagnostic_parents: diagnostic_parents.into_boxed_slice(),
                numeric_parents: numeric_parents.into_boxed_slice(),
                typed_cache_slots,
            }))
        } else {
            None
        },
        limits,
    })
}

/// Independent compiled batch owner. No raw evaluation method accepts
/// arbitrary rows; every invocation goes through the real suite dispatch seal.
struct RowProfile {
    source: Arc<BatchSource>,
    program: LocalBatchProgram,
}
#[derive(Clone, Copy)]
enum DeferredRootProbe {
    Parameter(i64),
    Fixed(NativeEvalType),
}
struct DeferredRootProfiles {
    probe: DeferredRootProbe,
    typed: Vec<(NativeEvalType, RowProfile)>,
    selected: Option<usize>,
}
impl DeferredRootProfiles {
    fn possible(
        root: &Expression,
        limits: BatchSourceLimits,
    ) -> Result<Option<(DeferredRootProbe, Vec<NativeEvalType>)>> {
        let Expression::Constant(c) = root else {
            return Ok(None);
        };

        let mut inner = root;
        while let Expression::Constant(c) = inner {
            let Some(child) = c.deferred_expr.as_deref() else {
                return Ok(None);
            };
            inner = child;
        }
        if !matches!(inner, Expression::ScalarFunction(_)) {
            return Ok(None);
        }
        let all = [
            NativeEvalType::Int,
            NativeEvalType::Real,
            NativeEvalType::Decimal,
            NativeEvalType::String,
            NativeEvalType::Datetime,
            NativeEvalType::Timestamp,
            NativeEvalType::Duration,
            NativeEvalType::Json,
            NativeEvalType::VectorFloat32,
        ];
        let probe = match c.param_marker {
            Some(marker) => DeferredRootProbe::Parameter(marker.order),
            None => match &c.ret_type {
                Some(field) => DeferredRootProbe::Fixed(field.eval_type()),
                None => return Ok(None),
            },
        };
        let supported = |target| {
            !matches!(probe, DeferredRootProbe::Fixed(fixed) if fixed != target)
                && crate::scalar_function::batch_supported(root, target)
        };
        if !all.iter().copied().any(supported) {
            return Ok(None);
        }
        let mut budget = Budget { used: 0 };
        budget.array::<DeferredRootProfiles>(1)?;
        budget.array::<NativeEvalType>(all.len())?;
        let mut targets = Vec::new();
        reserve(&mut targets, all.len())?;
        for target in all {
            if supported(target) {
                targets.push(target);
            }
        }
        Ok((!targets.is_empty()).then_some((probe, targets)))
    }
}
// Native eligible batches publish only after all rows have succeeded, even
// when each control subtree itself has row demand. This is output staging,
// not a second expression executor.
#[derive(Default)]
struct PendingNativeOutput {
    values: Vec<Datum>,
}

// A private, unaliased one-column owner. Only the existing typed materializer
// writes it; no expression work occurs here.
struct PendingTypedOutput {
    chunk: Chunk,
}

pub(crate) struct PreparedBatch {
    deferred_profiles: Option<Box<DeferredRootProfiles>>,
    defer_batch_publication: bool,
    fast_profile: Option<decimal_fast_source::PreparedFast>,
    row_profile: Option<RowProfile>,
    source: Arc<BatchSource>,
    program: LocalBatchProgram,
    limits: ExecutionLimits,
    #[cfg(test)]
    selection: Vec<usize>,
    pub(super) window_size: usize,
    vector_allocations: u64,
    windows: u64,
}
impl PreparedBatch {
    #[cfg(test)]
    pub(crate) fn compile(
        suite: &EvaluatorSuite,
        row_schema: &[FieldType],
        new_collation: bool,
        unit: u64,
        source_limits: BatchSourceLimits,
        execution: ExecutionLimits,
        max_materialization_retained_bytes: usize,
    ) -> Result<Self> {
        suite_shape(suite)?;
        Self::compile_calculation(
            suite,
            0,
            row_schema,
            new_collation,
            unit,
            source_limits,
            execution,
            max_materialization_retained_bytes,
        )
    }

    #[cfg(test)]
    fn compile_calculation(
        suite: &EvaluatorSuite,
        calculated_slot: usize,
        row_schema: &[FieldType],
        new_collation: bool,
        unit: u64,
        source_limits: BatchSourceLimits,
        execution: ExecutionLimits,
        max_materialization_retained_bytes: usize,
    ) -> Result<Self> {
        Self::compile_calculation_profile(
            suite,
            calculated_slot,
            row_schema,
            new_collation,
            unit,
            execution,
            false,
        )
    }
    fn compile_calculation_profile(
        suite: &EvaluatorSuite,
        calculated_slot: usize,
        row_schema: &[FieldType],
        new_collation: bool,
        unit: u64,
        execution: ExecutionLimits,
        promote: bool,
    ) -> Result<Self> {
        let source_limits = BatchSourceLimits {
            tree: CompileLimits::default(),
            max_metadata_bytes: usize::MAX,
        };

        let root = suite
            .program
            .calculated
            .get(calculated_slot)
            .ok_or_else(|| BatchFailure::admission("batch calculation slot missing"))?;
        let ordered = !suite.program.vectorizable;
        let deferred = if ordered {
            None
        } else {
            DeferredRootProfiles::possible(root, source_limits)?
        };
        let native_context_view = if ordered || deferred.is_some() {
            None
        } else {
            additional_context_native_view(root, source_limits)
        };
        let view = if ordered || deferred.is_some() {
            Some(NativeView::RootValue)
        } else if native_context_view.is_some() {
            native_context_view
        } else if matches!(root,Expression::ScalarFunction(function) if pb_value_owner(function))
            && { pb_value_arithmetic_batch(root).is_some() }
        {
            Some(NativeView::BulkRoot(
                pb_value_arithmetic_batch(root).expect("actual arithmetic batch candidate"),
            ))
        } else if matches!(root,Expression::ScalarFunction(function) if pb_value_owner(function))
            && { pb_value_unary_batch(root).is_some() }
        {
            Some(NativeView::BulkRoot(
                pb_value_unary_batch(root).expect("actual unary batch candidate"),
            ))
        } else if matches!(root,Expression::ScalarFunction(function) if pb_value_owner(function))
            && { pb_value_integer_batch(root) }
        {
            Some(NativeView::BulkRoot(NativeEvalType::Int))
        } else if promote {
            promoted_native_root_view(root, source_limits)
        } else {
            None
        };
        let pair = !ordered
            && (native_context_view.is_some_and(|view| view.bulk_target().is_some())
                || matches!(root, Expression::ScalarFunction(function) if view.is_some_and(|view| window_value_operation(function, view).is_some()))
                || (view == Some(NativeView::BulkRoot(NativeEvalType::Int))
                    && pb_value_integer_batch(root)));
        let fast = !ordered && promote && decimal_fast_source::possible(root);
        let limits = source_limits;
        let source = lower_view_policy(
            suite,
            calculated_slot,
            row_schema,
            new_collation,
            unit,
            limits,
            view,
            deferred.is_some(),
        )?;
        let program = compile_batch(
            &source.expr,
            &source.schema,
            LocalCompileContext {
                limits: source.limits.tree,
            },
            &source.facts,
        )?;
        let row_profile = if pair {
            let row_source = lower_view(
                suite,
                calculated_slot,
                row_schema,
                new_collation,
                unit,
                limits,
                Some(NativeView::RootValue),
            )?;
            let row_program = compile_batch(
                &row_source.expr,
                &row_source.schema,
                LocalCompileContext {
                    limits: limits.tree,
                },
                &row_source.facts,
            )?;
            Some(RowProfile {
                source: row_source,
                program: row_program,
            })
        } else {
            None
        };
        let fast_profile = if fast {
            Some(decimal_fast_source::compile(
                suite,
                calculated_slot,
                row_schema,
                new_collation,
                unit,
                limits,
            )?)
        } else {
            None
        };
        let deferred_profiles = if let Some((probe, targets)) = deferred {
            let mut typed = Vec::new();
            reserve(&mut typed, targets.len())?;
            for target in targets {
                let source = lower_view_policy(
                    suite,
                    calculated_slot,
                    row_schema,
                    new_collation,
                    unit,
                    limits,
                    Some(NativeView::BulkRoot(target)),
                    true,
                )?;
                let program = compile_batch(
                    &source.expr,
                    &source.schema,
                    LocalCompileContext {
                        limits: limits.tree,
                    },
                    &source.facts,
                )?;
                typed.push((target, RowProfile { source, program }));
            }
            Some(Box::new(DeferredRootProfiles {
                probe,
                typed,
                selected: None,
            }))
        } else {
            None
        };
        let window_size = program.max_batch_size();
        Ok(Self {
            deferred_profiles,
            // Eligibility selects the native publication boundary, while the
            // compiled program independently selects operand demand. Both
            // typed carriers and native-value capabilities need the same native
            // publication boundary when their root is not already whole.
            defer_batch_publication: !ordered
                && promote
                && !program.requires_whole_selection()
                && crate::scalar_function::static_batch_target(root).is_some(),
            fast_profile,
            row_profile,
            source,
            program,
            limits: execution,
            #[cfg(test)]
            selection: Vec::new(),
            window_size,
            vector_allocations: 0,
            windows: 0,
        })
    }

    fn compiled_node_count(&self) -> LocalResult<usize> {
        let deferred = self
            .deferred_profiles
            .as_ref()
            .map_or(Ok(0usize), |profiles| {
                profiles.typed.iter().try_fold(0usize, |count, (_, row)| {
                    count
                        .checked_add(row.source.facts.node_count())
                        .ok_or_else(|| resource("deferred profile graph count overflow"))
                })
            })?;
        self.source
            .facts
            .node_count()
            .checked_add(
                self.row_profile
                    .as_ref()
                    .map_or(0, |row| row.source.facts.node_count()),
            )
            .and_then(|count| {
                count.checked_add(
                    self.fast_profile
                        .as_ref()
                        .map_or(0, |fast| fast.source.facts.node_count()),
                )
            })
            .and_then(|count| count.checked_add(deferred))
            .ok_or_else(|| resource("profile graph count overflow"))
    }
    pub(crate) fn scratch_snapshot(&self) -> BatchScratchSnapshot {
        BatchScratchSnapshot {
            selection_capacity: {
                #[cfg(test)]
                {
                    self.selection.capacity()
                }
                #[cfg(not(test))]
                {
                    0
                }
            },
            vector_allocations: self.vector_allocations,
            windows: self.windows,
        }
    }
    #[cfg(test)]
    pub(crate) fn declared_type_snapshot(&self) -> FieldType {
        snapshot_field_type(self.source.nodes[0].declared_sql())
    }

    #[cfg(test)]
    fn validate_invocation(
        &mut self,
        suite: &EvaluatorSuite,
        input: &Chunk,
        output: &Chunk,
        row_schema: &[FieldType],
    ) -> Result<()> {
        if self.deferred_profiles.as_ref().is_some_and(|profiles| {
            profiles.typed.iter().any(|(_, row)| {
                !Arc::ptr_eq(&row.source.owner, &self.source.owner)
                    || row.source.root_identity != self.source.root_identity
                    || row.source.calculated_slot != self.source.calculated_slot
                    || row.source.output_index != self.source.output_index
                    || row.source.row_schema != self.source.row_schema
            })
        }) {
            return Err(binding("deferred profiles do not share their sealed source").into());
        }
        if self.row_profile.as_ref().is_some_and(|row| {
            !Arc::ptr_eq(&row.source.owner, &self.source.owner)
                || row.source.root_identity != self.source.root_identity
                || row.source.calculated_slot != self.source.calculated_slot
        }) {
            return Err(binding("row/bulk profiles do not share their source owner").into());
        }
        if self.fast_profile.as_ref().is_some_and(|fast| {
            !Arc::ptr_eq(&fast.source.owner, &self.source.owner)
                || fast.source.root_identity != self.source.root_identity
                || fast.source.calculated_slot != self.source.calculated_slot
                || fast.source.output_index != self.source.output_index
        }) {
            return Err(binding("fast/ordinary profiles do not share their source owner").into());
        }
        if !Arc::ptr_eq(&suite.program, &self.source.owner) {
            return Err(BatchFailure::admission(
                "batch worker belongs to another native program",
            ));
        }
        let root = suite
            .program
            .calculated
            .get(self.source.calculated_slot)
            .ok_or_else(|| BatchFailure::admission("batch calculation slot is missing"))?;
        if root as *const Expression as usize != self.source.root_identity {
            return Err(BatchFailure::admission(
                "batch worker root belongs to another native program",
            ));
        }
        // `EvaluatorProgram::new` deep-detaches every FieldType backing before
        // publishing its private immutable Arc. Owner plus root identity now
        // authenticate the source tree validated by `lower`; only the
        // caller-owned row schema still needs a live comparison.
        if row_schema.len() != self.source.row_schema.len()
            || row_schema
                .iter()
                .zip(self.source.row_schema.iter())
                .any(|(a, b)| a.collation() != b.collation() || a != b)
            || input.num_cols() != row_schema.len()
        {
            return Err(LocalError::InvalidBatch(
                "batch incoming schema differs from preparation".into(),
            )
            .into());
        }
        let physical = input.physical_rows();
        for (index, field) in row_schema.iter().enumerate() {
            let column = input.column(index);
            if column.rows() != physical || column.type_size() != get_fixed_len(field) {
                return Err(LocalError::InvalidBatch(
                    "batch native column layout/length differs".into(),
                )
                .into());
            }
        }
        if output.num_cols() != projection_output_count(&suite.program)?
            || self.source.nodes[0].sql.as_ref().is_some_and(|field| {
                output.column(self.source.output_index).type_size() != get_fixed_len(field)
            })
            || output.sel().is_some()
            || output.is_incomplete_chunk()
        {
            return Err(LocalError::InvalidBatch(
                "batch destination layout differs from its declared output".into(),
            )
            .into());
        }
        let count = input.num_rows();
        if input
            .sel()
            .is_some_and(|selected| selected.iter().any(|row| *row >= physical))
        {
            return Err(
                LocalError::InvalidBatch("batch selection exceeds physical rows".into()).into(),
            );
        }
        // Refusal must not retain an oversized allocation that prevents later
        // small invocations from recovering under the same materialization cap.

        if count > self.selection.capacity() {
            let mut next = Vec::new();
            reserve(&mut next, count)?;

            self.selection = next;
        } else {
            self.selection.clear();
        }
        if let Some(selected) = input.sel() {
            self.selection.extend_from_slice(selected);
        } else {
            self.selection.extend(0..physical);
        }

        Ok(())
    }

    fn write_native_output(
        &self,
        values: VectorValue,
        selection: &[usize],
        output: &mut Chunk,
        store: &RefCell<NativeValueStore>,
        mut pending: Option<&mut PendingNativeOutput>,
    ) -> Result<()> {
        let VectorValue::Bytes(values) = values else {
            return Err(binding("native result is not a value handle vector").into());
        };
        if values.len() != selection.len() {
            return Err(binding("native result selection length differs").into());
        }

        let store = store.borrow();
        let origin = |occurrence: usize, input_row: usize| NativeValueOrigin {
            node: 0,
            row: InputRow {
                occurrence,
                input_row,
            },
        };
        // Check the entire window before publishing any cell. A wire NULL is
        // not a native NULL: all native values, including NULL, have handles.
        for (occurrence, &input_row) in selection.iter().enumerate() {
            let token = values
                .get(occurrence)
                .ok_or_else(|| binding("native output handle is wire NULL"))?;
            store.get(
                token,
                NativeValueRole::Output,
                origin(occurrence, input_row),
            )?;
        }
        for (occurrence, &input_row) in selection.iter().enumerate() {
            let value = store.get(
                values.get(occurrence).expect("validated handle"),
                NativeValueRole::Output,
                origin(occurrence, input_row),
            )?;
            if let Some(pending) = pending.as_deref_mut() {
                pending
                    .values
                    .try_reserve(1)
                    .map_err(|_| resource("pending native result reservation failed"))?;

                pending.values.push(value.clone());
            } else {
                output.append_datum(self.source.output_index, value);
            }
        }
        Ok(())
    }

    fn capture_typed_output(
        &self,
        values: VectorValue,
        selection: &[usize],
        pending: &mut PendingTypedOutput,
    ) -> Result<()> {
        // Preserve shape/error priority before any copy or resource-based refusal.
        let expected = batch_eval_type(self.source.nodes[0].declared_sql().eval_type());
        if values.eval_type() != expected || values.len() != selection.len() {
            return Err(binding("batch result shape differs from computed boundary").into());
        }

        self.write_output(values, selection, &mut pending.chunk, 0)?;

        Ok(())
    }

    fn write_output(
        &self,
        values: VectorValue,
        selection: &[usize],
        output: &mut Chunk,
        output_index: usize,
    ) -> Result<()> {
        let expected = batch_eval_type(self.source.nodes[0].declared_sql().eval_type());
        if values.eval_type() != expected || values.len() != selection.len() {
            return Err(binding("batch result shape differs from computed boundary").into());
        }

        // Check the complete result shape before publishing any cell.
        // The destination Chunk owns its ordinary allocation policy just as it
        // does for every other evaluator route.
        // Invocation rejects output selections. Resolve the destination owner
        // once for the whole window, preserving ColumnWrite alias/COW handling.
        let mut output = output.column_mut(output_index);
        match &values {
            VectorValue::Int(ints) => {
                // The original REGEXP_LIKE integer batch publishes scalar
                // cells, including the Column scratch state used on reuse.
                if native_regexp_row_batch(
                    &self.source.owner.calculated[self.source.calculated_slot],
                ) {
                    for index in 0..values.len() {
                        match ChunkRef::get_option_ref(ints, index) {
                            Some(value) => output.append_int64(*value),
                            None => output.append_null(),
                        }
                    }
                } else {
                    output.append_fixed_cells((0..values.len()).map(|index| {
                        ChunkRef::get_option_ref(ints, index).map(|value| value.to_ne_bytes())
                    }));
                }
            }
            VectorValue::Real(reals) => {
                output.append_fixed_cells((0..values.len()).map(|index| {
                    ChunkRef::get_option_ref(reals, index)
                        .map(|value| value.into_inner().to_ne_bytes())
                }));
            }
            VectorValue::Decimal(decimals) => {
                let convert = |value: &TikvDecimal| {
                    tidb_datatype::MyDecimal::try_from_shared_math(value, usize::MAX)
                        .map_err(|_| binding("batch Decimal output is not representable"))
                };

                // Validate every cell once before publishing any output. The
                // previous adapter performed the same conversion twice.
                let converted = (0..values.len())
                    .map(|index| {
                        ChunkRef::get_option_ref(decimals, index)
                            .map(&convert)
                            .transpose()
                    })
                    .collect::<LocalResult<Vec<_>>>()?;

                output.append_fixed_cells(
                    converted
                        .iter()
                        .map(|value| value.as_ref().map(|value| value.to_raw_bytes())),
                );
            }
            VectorValue::Bytes(bytes) => {
                output.append_raw_cells((0..values.len()).map(|index| {
                    bytes
                        .get(index)
                        .map_or((false, &[][..]), |value| (true, value))
                }));
            }
            VectorValue::DateTime(values) => {
                output.append_fixed_cells((0..values.len()).map(|index| {
                    ChunkRef::get_option_ref(values, index).map(|value| {
                        Time::from_go_raw_like_go(value.go_raw())
                            .go_raw()
                            .to_ne_bytes()
                    })
                }));
            }
            VectorValue::Duration(values) => {
                output.append_fixed_cells((0..values.len()).map(|index| {
                    ChunkRef::get_option_ref(values, index)
                        .map(|value| value.to_nanos().to_ne_bytes())
                }));
            }
            VectorValue::Json(values) => {
                let converted = (0..values.len())
                    .map(|index| {
                        values
                            .get(index)
                            .map(|value| {
                                BinaryJSON::from_raw(value.get_type() as u8, value.value().to_vec())
                                    .map_err(|_| binding("batch JSON output is not representable"))
                            })
                            .transpose()
                    })
                    .collect::<LocalResult<Vec<_>>>()?;
                for value in converted {
                    match value {
                        Some(value) => output.append_json(&value),
                        None => output.append_null(),
                    }
                }
            }
            VectorValue::VectorFloat32(values) => {
                for index in 0..values.len() {
                    match values.get(index) {
                        Some(value) => output
                            .append_vector_float32(&native_vector_from_tikv(value.to_owned())?),
                        None => output.append_null(),
                    }
                }
            }
            _ => unreachable!("checked batch output carrier"),
        }
        Ok(())
    }
}

// Private native leaf seam; only tests can substitute an adversarial provider,
// and they still enter through the suite/token and genuine Chunk preflight.
trait NativeDatumSource {
    fn schema(&self) -> &[tipb::FieldType];
    fn read(
        &mut self,
        ctx: &mut EvalContext,
        slot: usize,
        row: InputRow,
        expected: &tipb::FieldType,
    ) -> LocalResult<Datum>;

    fn read_handle(
        &mut self,
        _ctx: &mut EvalContext,
        _slot: usize,
        _row: InputRow,
        _expected: &tipb::FieldType,
    ) -> LocalResult<Option<native_values::NativeValueHandle>> {
        Ok(None)
    }

    fn read_batch(
        &mut self,
        _ctx: &mut EvalContext,
        _slot: usize,
        _physical_rows: &[usize],
        _expected: &tipb::FieldType,
    ) -> LocalResult<Option<VectorValue>> {
        Ok(None)
    }
}
// One leaf primitive for real Chunk and real scalar-row consumers. It never
// calls an expression evaluator or derives a SQL type from physical storage.
// Import an actual native value directly into its arena; its identity is inline.
fn read_source_handle(
    source: &SourcePlan,
    native_values: Option<&RefCell<NativeValueStore>>,
    native_row: tidb_chunk::row::Row<'_>,
    ordinal: usize,
    row: InputRow,
) -> LocalResult<native_values::NativeValueHandle> {
    let node = &source.nodes[ordinal];
    if let SourceKind::DecimalFastInput(value) = &node.kind {
        let store = native_values.ok_or_else(|| binding("fast source store missing"))?;
        let value = value.read(
            native_row
                .chunk()
                .ok_or_else(|| binding("fast source has no physical owner"))?,
            row.input_row,
        );
        let token = store
            .borrow_mut()
            .insert_decimal_fast(value, NativeValueOrigin { node: ordinal, row })?;
        return Ok(token);
    }
    if node.view.value_role().is_some() {
        let store = native_values.ok_or_else(|| binding("native source has no value store"))?;
        let token = match &node.kind {
            SourceKind::NativeConstant { value } => {
                // Exactly the public Expression.Eval result view. Borrow
                // unchanged values until insert_clone's pre-copy gate.
                let integer = (!(ordinal == 0 && source.constant_root_shortcut)
                    && node.view.bulk_target().is_none()
                    && !matches!(
                        node.view,
                        NativeView::RowTyped(_)
                            | NativeView::DiagnosticTyped(_)
                            | NativeView::EmptyRowTyped(_)
                    ))
                .then(|| crate::expression::enum_set_integer_view(value, node.sql.as_ref()))
                .flatten()
                .map(Datum::UInt);
                store.borrow_mut().insert_clone(
                    integer.as_ref().unwrap_or(value),
                    node.view.value_role().expect("checked native view"),
                    NativeValueOrigin { node: ordinal, row },
                )?
            }
            SourceKind::Column { index } => {
                native_row
                    .chunk()
                    .ok_or_else(|| binding("column source has no physical owner"))?;
                store.borrow().check_live()?;
                let value = native_row.get_datum(*index, node.declared_sql());
                let value =
                    crate::expression::apply_enum_set_integer_view(value, node.sql.as_ref());
                store.borrow_mut().insert_owned(
                    value,
                    node.view.value_role().expect("checked native view"),
                    NativeValueOrigin { node: ordinal, row },
                )?
            }
            _ => return Err(binding("native value binding has no native source")),
        };
        return Ok(token);
    }
    Err(binding("source has no native handle carrier"))
}

fn read_source_row(
    source: &SourcePlan,
    native_values: Option<&RefCell<NativeValueStore>>,
    native_row: tidb_chunk::row::Row<'_>,
    ordinal: usize,
    row: InputRow,
) -> LocalResult<Datum> {
    let node = &source.nodes[ordinal];
    if matches!(&node.kind, SourceKind::DecimalFastInput(_)) || node.view.value_role().is_some() {
        // This Datum-returning compatibility seam owns Bytes. Real native
        // bindings use read_handle/read_batch and avoid this temporary owner.
        return read_source_handle(source, native_values, native_row, ordinal, row)
            .map(|handle| Datum::Bytes(handle.to_vec()));
    }
    let SourceKind::Column { index, .. } = &node.kind else {
        return Err(binding("batch slot has no native column"));
    };
    // Physical already: get_row would apply the actual Chunk Sel twice.
    let value = native_row.get_datum(*index, node.declared_sql());
    if node.view == NativeView::JsonBytes {
        return match value {
            Datum::Null => Ok(Datum::Null),
            Datum::Json(value) => Ok(Datum::Bytes(value.encoded())),
            _ => Err(binding("native JSON view read a non-JSON source")),
        };
    }
    Ok(value)
}

struct NativeChunk<'a> {
    source: &'a SourcePlan,
    native_values: Option<&'a RefCell<NativeValueStore>>,
    input: &'a Chunk,
    selection: &'a [usize],
}
impl NativeDatumSource for NativeChunk<'_> {
    fn schema(&self) -> &[tipb::FieldType] {
        &self.source.schema
    }
    fn read(
        &mut self,
        _ctx: &mut EvalContext,
        slot: usize,
        row: InputRow,
        expected: &tipb::FieldType,
    ) -> LocalResult<Datum> {
        let ordinal = *self
            .source
            .bindings
            .get(slot)
            .ok_or_else(|| binding("unknown batch slot"))?;
        if self.source.schema.get(slot) != Some(expected)
            || row.input_row >= self.input.physical_rows()
            || self.selection.get(row.occurrence) != Some(&row.input_row)
        {
            return Err(binding("batch demanded occurrence/type differs"));
        }
        read_source_row(
            self.source,
            self.native_values,
            self.input.physical_row(row.input_row),
            ordinal,
            row,
        )
    }

    fn read_handle(
        &mut self,
        _ctx: &mut EvalContext,
        slot: usize,
        row: InputRow,
        expected: &tipb::FieldType,
    ) -> LocalResult<Option<native_values::NativeValueHandle>> {
        let ordinal = *self
            .source
            .bindings
            .get(slot)
            .ok_or_else(|| binding("unknown batch slot"))?;
        let node = &self.source.nodes[ordinal];
        if node.view.value_role().is_none()
            && !matches!(&node.kind, SourceKind::DecimalFastInput(_))
        {
            return Ok(None);
        }
        if self.source.schema.get(slot) != Some(expected)
            || row.input_row >= self.input.physical_rows()
            || self.selection.get(row.occurrence) != Some(&row.input_row)
        {
            return Err(binding("batch demanded occurrence/type differs"));
        }
        read_source_handle(
            self.source,
            self.native_values,
            self.input.physical_row(row.input_row),
            ordinal,
            row,
        )
        .map(Some)
    }

    fn read_batch(
        &mut self,
        _ctx: &mut EvalContext,
        slot: usize,
        physical_rows: &[usize],
        expected: &tipb::FieldType,
    ) -> LocalResult<Option<VectorValue>> {
        let ordinal = *self
            .source
            .bindings
            .get(slot)
            .ok_or_else(|| binding("unknown batch slot"))?;
        if self.source.schema.get(slot) != Some(expected)
            || physical_rows != self.selection
            || physical_rows
                .iter()
                .any(|row| *row >= self.input.physical_rows())
        {
            return Err(binding("batch occurrence/type differs"));
        }
        let node = &self.source.nodes[ordinal];
        if let SourceKind::DecimalFastInput(value) = &node.kind {
            return decimal_fast_source::read_batch(
                value,
                self.input,
                self.native_values,
                ordinal,
                physical_rows,
            )
            .map(Some);
        }
        if node.view.value_role().is_some() {
            let payload = physical_rows
                .len()
                .checked_mul(native_values::HANDLE_BYTES)
                .ok_or_else(|| resource("native handle batch size overflow"))?;
            let mut output = ChunkedVecBytes::try_with_capacities(physical_rows.len(), payload)
                .map_err(|error| LocalError::ResourceLimit(error.to_string()))?;
            for (occurrence, &input_row) in physical_rows.iter().enumerate() {
                let token = read_source_handle(
                    self.source,
                    self.native_values,
                    self.input.physical_row(input_row),
                    ordinal,
                    InputRow {
                        occurrence,
                        input_row,
                    },
                )?;
                output.push_ref(Some(&token));
            }
            return Ok(Some(VectorValue::Bytes(output)));
        }
        let SourceKind::Column { index, .. } = &node.kind else {
            return Err(binding("batch slot has no native column"));
        };
        let column = self.input.column(*index);
        if node.view == NativeView::JsonBytes {
            let cells = column.raw_cells();
            let payload = physical_rows
                .iter()
                .try_fold(0usize, |total, &row| {
                    total.checked_add(if cells.is_null(row) {
                        0
                    } else {
                        cells.get(row).len()
                    })
                })
                .ok_or_else(|| resource("native JSON byte view size overflow"))?;
            let mut output = ChunkedVecBytes::try_with_capacities(physical_rows.len(), payload)
                .map_err(|error| LocalError::ResourceLimit(error.to_string()))?;
            for &row in physical_rows {
                output.push_ref((!cells.is_null(row)).then(|| cells.get(row)));
            }
            return Ok(Some(VectorValue::Bytes(output)));
        }
        match node.declared_sql().eval_type() {
            NativeEvalType::Int => {
                let cells = column.raw_cells();
                let mut output = ChunkedVecSized::<Int>::with_capacity(physical_rows.len());
                for &row in physical_rows {
                    output.push((!cells.is_null(row)).then(|| {
                        i64::from_ne_bytes(
                            cells.get(row).try_into().expect("Int cell is eight bytes"),
                        )
                    }));
                }
                Ok(Some(VectorValue::Int(output)))
            }
            NativeEvalType::Real => {
                let cells = column.raw_cells();
                let mut output = ChunkedVecSized::<Real>::with_capacity(physical_rows.len());
                for &row in physical_rows {
                    let value = if column.is_null(row) {
                        None
                    } else {
                        Some(
                            Real::new(f64::from_ne_bytes(
                                cells.get(row).try_into().expect("Real cell is eight bytes"),
                            ))
                            .map_err(|_| binding("batch Real column contains a NaN value"))?,
                        )
                    };
                    output.push(value);
                }
                Ok(Some(VectorValue::Real(output)))
            }
            NativeEvalType::Decimal => {
                let cells = column.raw_cells();
                let mut output = ChunkedVecSized::<TikvDecimal>::with_capacity(physical_rows.len());
                for &row in physical_rows {
                    let value = if cells.is_null(row) {
                        None
                    } else {
                        Some(
                            TikvDecimal::try_from_native_cell_bytes(
                                cells
                                    .get(row)
                                    .try_into()
                                    .expect("Decimal cell is forty bytes"),
                                1024 * 1024,
                            )
                            .map_err(|_| binding("batch Decimal column is not representable"))?,
                        )
                    };
                    output.push(value);
                }
                Ok(Some(VectorValue::Decimal(output)))
            }
            NativeEvalType::String => {
                let cells = column.raw_cells();
                let payload = physical_rows.iter().try_fold(0usize, |total, &row| {
                    total.checked_add(if column.is_null(row) {
                        0
                    } else {
                        cells.get(row).len()
                    })
                });
                let payload = payload.ok_or_else(|| resource("batch Bytes payload overflow"))?;
                let mut output = ChunkedVecBytes::try_with_capacities(physical_rows.len(), payload)
                    .map_err(|error| LocalError::ResourceLimit(error.to_string()))?;
                for &row in physical_rows {
                    if column.is_null(row) {
                        output.push_ref(None);
                    } else {
                        output.push_ref(Some(cells.get(row)));
                    }
                }
                Ok(Some(VectorValue::Bytes(output)))
            }
            NativeEvalType::Datetime | NativeEvalType::Timestamp => {
                let mut output =
                    ChunkedVecSized::<TikvDateTime>::with_capacity(physical_rows.len());
                for &row in physical_rows {
                    output.push(
                        (!column.is_null(row)).then(|| {
                            TikvDateTime::from_go_raw_like_go(column.get_time(row).go_raw())
                        }),
                    );
                }
                Ok(Some(VectorValue::DateTime(output)))
            }
            NativeEvalType::Duration => {
                let fsp = node.declared_sql().decimal();
                let mut output =
                    ChunkedVecSized::<TikvDuration>::with_capacity(physical_rows.len());
                for &row in physical_rows {
                    let value = if column.is_null(row) {
                        None
                    } else {
                        let value = column.get_duration(row, fsp);
                        Some(
                            TikvDuration::from_native_raw_parts(
                                value.nanoseconds(),
                                i8::try_from(value.fsp())
                                    .map_err(|_| binding("batch Duration FSP is out of range"))?,
                            )
                            .map_err(|_| binding("batch Duration column is not representable"))?,
                        )
                    };
                    output.push(value);
                }
                Ok(Some(VectorValue::Duration(output)))
            }
            NativeEvalType::Json => {
                let cells = column.raw_cells();
                let mut output = ChunkedVecJson::with_capacity(physical_rows.len());
                for &row in physical_rows {
                    if column.is_null(row) {
                        output.push(None);
                    } else {
                        let (kind, payload) = cells
                            .get(row)
                            .split_first()
                            .expect("a JSON cell always carries its type code");
                        let kind = JsonType::try_from(*kind)
                            .map_err(|_| binding("batch JSON column has an unknown type code"))?;
                        output.push(Some(TikvJson::new(kind, payload.to_vec())));
                    }
                }
                Ok(Some(VectorValue::Json(output)))
            }
            NativeEvalType::VectorFloat32 => {
                let mut output = ChunkedVecVectorFloat32::with_capacity(physical_rows.len());
                for &row in physical_rows {
                    output.push(
                        (!column.is_null(row))
                            .then(|| column.get_vector_float32(row).into_wire_raw()),
                    );
                }
                Ok(Some(VectorValue::VectorFloat32(output)))
            }
            _ => Err(binding("batch native column has an unsupported carrier")),
        }
    }
}
struct CheckedInputs<'a> {
    source: &'a mut dyn NativeDatumSource,
    hosts: Option<&'a mut dyn LocalHostServices>,
}
impl LocalRuntimeServices for CheckedInputs<'_> {
    fn host_services(&mut self) -> Option<&mut dyn LocalHostServices> {
        match &mut self.hosts {
            Some(hosts) => Some(&mut **hosts),
            None => None,
        }
    }
    fn binding_schema(&self) -> &[tipb::FieldType] {
        self.source.schema()
    }
    fn read_input(
        &mut self,
        ctx: &mut EvalContext,
        slot: usize,
        row: InputRow,
        expected: &tipb::FieldType,
    ) -> LocalResult<VectorValue> {
        if let Some(handle) = self.source.read_handle(ctx, slot, row, expected)? {
            let mut output = ChunkedVecBytes::try_with_capacities(1, native_values::HANDLE_BYTES)
                .map_err(|error| LocalError::ResourceLimit(error.to_string()))?;
            output.push_ref(Some(&handle));
            return Ok(VectorValue::Bytes(output));
        }

        let value = self.source.read(ctx, slot, row, expected)?;
        let eval_type = match value {
            Datum::Int(_) | Datum::Null
                if matches!(expected.get_tp(), value if value == tidb_query_datatype::FieldTypeTp::Tiny as i32
                    || value == tidb_query_datatype::FieldTypeTp::Short as i32
                    || value == tidb_query_datatype::FieldTypeTp::Int24 as i32
                    || value == tidb_query_datatype::FieldTypeTp::Long as i32
                    || value == tidb_query_datatype::FieldTypeTp::LongLong as i32) =>
            {
                TikvEvalType::Int
            }
            Datum::UInt(_)
                if matches!(expected.get_tp(), value if value == tidb_query_datatype::FieldTypeTp::Tiny as i32
                    || value == tidb_query_datatype::FieldTypeTp::Short as i32
                    || value == tidb_query_datatype::FieldTypeTp::Int24 as i32
                    || value == tidb_query_datatype::FieldTypeTp::Long as i32
                    || value == tidb_query_datatype::FieldTypeTp::LongLong as i32)
                    && expected.get_flag() & (1 << 5) != 0 =>
            {
                TikvEvalType::Int
            }
            Datum::Real(_) | Datum::Null
                if expected.get_tp() == tidb_query_datatype::FieldTypeTp::Double as i32 =>
            {
                TikvEvalType::Real
            }
            Datum::Decimal(_) | Datum::Null
                if expected.get_tp() == tidb_query_datatype::FieldTypeTp::NewDecimal as i32 =>
            {
                TikvEvalType::Decimal
            }
            Datum::String(_) | Datum::Bytes(_) | Datum::Raw(_) | Datum::Null
                if matches!(
                    expected.get_tp(),
                    value if value == tidb_query_datatype::FieldTypeTp::VarChar as i32
                        || value == tidb_query_datatype::FieldTypeTp::VarString as i32
                        || value == tidb_query_datatype::FieldTypeTp::String as i32
                        || value == tidb_query_datatype::FieldTypeTp::Blob as i32
                        || value == tidb_query_datatype::FieldTypeTp::TinyBlob as i32
                        || value == tidb_query_datatype::FieldTypeTp::MediumBlob as i32
                        || value == tidb_query_datatype::FieldTypeTp::LongBlob as i32
                ) =>
            {
                TikvEvalType::Bytes
            }
            Datum::Time(_) | Datum::Null
                if matches!(
                    expected.get_tp(),
                    value if value == tidb_query_datatype::FieldTypeTp::Date as i32
                        || value == tidb_query_datatype::FieldTypeTp::DateTime as i32
                        || value == tidb_query_datatype::FieldTypeTp::Timestamp as i32
                ) =>
            {
                TikvEvalType::DateTime
            }
            Datum::Duration(_) | Datum::Null
                if expected.get_tp() == tidb_query_datatype::FieldTypeTp::Duration as i32 =>
            {
                TikvEvalType::Duration
            }
            Datum::Json(_) | Datum::Null
                if expected.get_tp() == tidb_query_datatype::FieldTypeTp::Json as i32 =>
            {
                TikvEvalType::Json
            }
            Datum::VectorFloat32(_) | Datum::Null
                if expected.get_tp()
                    == tidb_query_datatype::FieldTypeTp::TiDbVectorFloat32 as i32 =>
            {
                TikvEvalType::VectorFloat32
            }
            _ => return Err(binding("batch demanded Datum differs from its carrier")),
        };
        let value = match eval_type {
            TikvEvalType::Decimal => match value {
                Datum::Null => ScalarValue::Decimal(None),
                Datum::Decimal(value) => ScalarValue::Decimal(Some(
                    value
                        .try_to_shared_math(1024 * 1024)
                        .map_err(|_| binding("batch demanded Decimal is not representable"))?,
                )),
                _ => return Err(binding("batch demanded Datum differs from its carrier")),
            },
            TikvEvalType::DateTime => match value {
                Datum::Null => ScalarValue::DateTime(None),
                Datum::Time(value) => {
                    ScalarValue::DateTime(Some(TikvDateTime::from_go_raw_like_go(value.go_raw())))
                }
                _ => return Err(binding("batch demanded Datum differs from its carrier")),
            },
            TikvEvalType::Duration => match value {
                Datum::Null => ScalarValue::Duration(None),
                Datum::Duration(value) => ScalarValue::Duration(Some(
                    TikvDuration::from_native_raw_parts(
                        value.nanoseconds(),
                        i8::try_from(value.fsp())
                            .map_err(|_| binding("batch Duration FSP is out of range"))?,
                    )
                    .map_err(|_| binding("batch demanded Duration is not representable"))?,
                )),
                _ => return Err(binding("batch demanded Datum differs from its carrier")),
            },
            TikvEvalType::Json => match value {
                Datum::Null => ScalarValue::Json(None),
                Datum::Json(value) => ScalarValue::Json(Some(TikvJson::new(
                    JsonType::try_from(value.type_code())
                        .map_err(|_| binding("batch demanded JSON has an unknown type code"))?,
                    value.value().to_vec(),
                ))),
                _ => return Err(binding("batch demanded Datum differs from its carrier")),
            },
            TikvEvalType::VectorFloat32 => match value {
                Datum::Null => ScalarValue::VectorFloat32(None),
                Datum::VectorFloat32(value) => {
                    ScalarValue::VectorFloat32(Some(value.into_wire_raw()))
                }
                _ => return Err(binding("batch demanded Datum differs from its carrier")),
            },
            _ => {
                to_scalar(&value, eval_type)
                    .map_err(|error| LocalError::BindingContract(error.to_string()))?
                    .0
            }
        };
        Ok(VectorValue::from_scalar(&value, 1))
    }

    fn read_input_batch(
        &mut self,
        ctx: &mut EvalContext,
        slot: usize,
        physical_rows: &[usize],
        expected: &tipb::FieldType,
    ) -> LocalResult<Option<VectorValue>> {
        self.source.read_batch(ctx, slot, physical_rows, expected)
    }
}

#[cfg(test)]
struct Consumer<'a> {
    worker: &'a mut PreparedBatch,
    ctx: &'a mut EvalContext,
    row_schema: &'a [FieldType],
    #[cfg(test)]
    override_source: Option<&'a mut dyn NativeDatumSource>,
}
#[cfg(test)]
impl BatchConsumer for Consumer<'_> {
    type Error = BatchFailure;
    fn before_run(&mut self, suite: &EvaluatorSuite, input: &Chunk, output: &Chunk) -> Result<()> {
        self.worker
            .validate_invocation(suite, input, output, self.row_schema)
    }
    fn consume(
        &mut self,
        invocation: BatchInvocation<'_>,
        native_ctx: &dyn Columns,
        output: &mut Chunk,
    ) -> Result<()> {
        let source = Arc::clone(&self.worker.source);
        if !Arc::ptr_eq(invocation.program, &source.owner)
            || invocation.calculated_slot != source.calculated_slot
            || invocation.output_index != source.output_index
            || !std::ptr::eq(
                invocation.candidate.expression(),
                &source.owner.calculated[source.calculated_slot],
            )
            || invocation.candidate.target() != source.nodes[0].declared_sql().eval_type()
            || invocation.input.num_rows() != self.worker.selection.len()
        {
            return Err(BatchFailure::admission(
                "batch invocation does not join its own source/program",
            ));
        }
        // Consume the native eligibility evidence without calling its value
        // worker. The actual suite/global decision minted it for THIS call.
        drop(invocation.candidate);
        self.worker.execute_chunk(
            native_ctx,
            self.ctx,
            invocation.input,
            output,
            #[cfg(test)]
            self.override_source.take(),
        )
    }
    fn requires_batch_candidate_before_native_specialization(&self) -> bool {
        true
    }
    fn row_route(&mut self) -> Result<()> {
        Err(BatchFailure::admission(
            "mandatory batch consumer did not reach the native batch entry",
        ))
    }
}

#[cfg(test)]
impl EvaluatorSuite {
    pub(crate) fn run_batch<C: Columns>(
        &self,
        native_ctx: &C,
        ctx: &mut EvalContext,
        worker: &mut PreparedBatch,
        input: &mut Chunk,
        output: &mut Chunk,
        row_schema: &[FieldType],
    ) -> Result<()> {
        self.run_with_consumer(
            native_ctx,
            input,
            output,
            &mut Consumer {
                worker,
                ctx,
                row_schema,
                #[cfg(test)]
                override_source: None,
            },
        )
    }
}

#[path = "batch_scalar_source.rs"]
mod scalar_source;
#[path = "batch_source_fragment.rs"]
pub(crate) mod source_fragment;

#[path = "batch_native_source.rs"]
mod native_source;

pub(crate) fn eval_eligible_native_batch(
    candidate: crate::scalar_function::EligibleBatch<'_>,
    context: &dyn Columns,
    input: &Chunk,
) -> std::result::Result<Vec<Datum>, EvalError> {
    native_source::eval(candidate, context, input)
}

pub(crate) fn eval_native_decimal_priority(
    function: &ScalarFunction,
    context: &dyn Columns,
    input: &Chunk,
    output: &mut Chunk,
    output_index: usize,
) -> std::result::Result<bool, EvalError> {
    decimal_fast_source::eval_function(function, context, input, output, output_index)
}

#[cfg(test)]
#[path = "batch_tests.rs"]
mod tests;

impl PreparedBatch {
    #[cfg(test)]
    fn execute_chunk(
        &mut self,
        native_ctx: &dyn Columns,
        ctx: &mut EvalContext,
        input: &Chunk,
        output: &mut Chunk,
        #[cfg(test)] override_source: Option<&mut dyn NativeDatumSource>,
    ) -> Result<()> {
        self.execute_profile(
            native_ctx,
            ctx,
            input,
            output,
            false,
            #[cfg(test)]
            override_source,
        )
    }
    fn select_deferred_profile(&mut self, ctx: &dyn Columns, enabled: bool) -> Result<bool> {
        let Some(profiles) = &mut self.deferred_profiles else {
            return Ok(enabled);
        };
        // Clear before the actual callback so unwind/error cannot reuse a stale choice.
        profiles.selected = None;
        if !enabled {
            return Ok(false);
        }
        let target = match profiles.probe {
            DeferredRootProbe::Fixed(target) => target,
            DeferredRootProbe::Parameter(order) => {
                (|| crate::context_value::read_parameter_type(order, ctx))()?.eval_type()
            }
        };
        profiles.selected = profiles
            .typed
            .iter()
            .position(|(candidate, _)| *candidate == target);
        Ok(profiles.selected.is_some())
    }
    #[cfg(test)]
    fn execute_profile(
        &mut self,
        native_ctx: &dyn Columns,
        ctx: &mut EvalContext,
        input: &Chunk,
        output: &mut Chunk,
        row_domain: bool,
        override_source: Option<&mut dyn NativeDatumSource>,
    ) -> Result<()> {
        let selection = self.selection.clone();
        self.execute_selected_profile(
            native_ctx,
            ctx,
            input,
            output,
            row_domain,
            &selection,
            override_source,
        )
    }
    fn execute_selected_profile(
        &mut self,
        native_ctx: &dyn Columns,
        ctx: &mut EvalContext,
        input: &Chunk,
        output: &mut Chunk,
        row_domain: bool,
        selection: &[usize],
        #[cfg(test)] mut override_source: Option<&mut dyn NativeDatumSource>,
    ) -> Result<()> {
        let deferred_index = self
            .deferred_profiles
            .as_ref()
            .and_then(|profiles| profiles.selected);
        let use_row = row_domain && self.row_profile.is_some();
        let source = Arc::clone(if let Some(index) = deferred_index {
            &self.deferred_profiles.as_ref().unwrap().typed[index]
                .1
                .source
        } else if use_row {
            &self
                .row_profile
                .as_ref()
                .expect("selected row profile")
                .source
        } else {
            &self.source
        });
        let row_domain = row_domain || source.context_row_only;
        let whole = if let Some(index) = deferred_index {
            self.deferred_profiles.as_ref().unwrap().typed[index]
                .1
                .program
                .requires_whole_selection()
        } else if use_row {
            self.row_profile
                .as_ref()
                .unwrap()
                .program
                .requires_whole_selection()
        } else {
            self.program.requires_whole_selection()
        };
        let whole = whole && !source.context_row_only;
        let window_size = if row_domain { 1 } else { self.window_size };
        let defer =
            !row_domain && (self.defer_batch_publication || (deferred_index.is_some() && !whole));
        let native_output = source.nodes[0].view.value_role() == Some(NativeValueRole::Output);
        let mut pending = (defer && native_output).then(PendingNativeOutput::default);
        let mut typed_pending = if defer && !native_output && !selection.is_empty() {
            let pending = PendingTypedOutput {
                chunk: Chunk::new_empty(std::slice::from_ref(source.nodes[0].declared_sql())),
            };

            Some(pending)
        } else {
            None
        };
        let native_values = if source.needs_native_values {
            Some(RefCell::new(NativeValueStore::new()?))
        } else {
            None
        };
        let mut hosts = source
            .hosts
            .clone()
            .map(|catalog| {
                Ok::<_, LocalError>(
                    NativeBatchHosts::new(catalog, &source.host_metadata, native_ctx)
                        .with_native_values(native_values.as_ref())
                        .with_input(input)
                        .with_typed_cache_slots(
                            source
                                .context_policy
                                .as_ref()
                                .map_or(0, |p| p.typed_cache_slots),
                        )?
                        .with_constant_probes(&source.host_probes)
                        .with_constant_root_shortcut(source.constant_root_shortcut)
                        .with_diagnostic_parents(
                            source
                                .context_policy
                                .as_ref()
                                .map_or(&[], |p| p.diagnostic_parents.as_ref()),
                        )
                        .with_numeric_parents(
                            source
                                .context_policy
                                .as_ref()
                                .map_or(&[], |p| p.numeric_parents.as_ref()),
                        )
                        .with_diagnostic_slots(
                            source
                                .context_policy
                                .as_ref()
                                .map_or(&[], |p| p.diagnostic_slots.as_ref()),
                        ),
                )
            })
            .transpose()?;
        // Explicit whole programs receive the complete selection. Other
        // programs use bounded driver windows in the same canonical route;
        // eligible batch results are staged across those windows. No error
        // selects a different evaluator or retries an expression.
        let mut start = 0;
        while start < selection.len() {
            let end = if whole {
                selection.len()
            } else {
                selection.len().min(start.saturating_add(window_size))
            };
            let selection = &selection[start..end];
            if let Some(store) = &native_values {
                store.borrow_mut().begin_window()?;
            }
            let mut native = NativeChunk {
                source: &source,
                native_values: native_values.as_ref(),
                input,
                selection,
            };
            #[cfg(test)]
            let provider: &mut dyn NativeDatumSource = match override_source.as_mut() {
                Some(source) => &mut **source,
                None => &mut native,
            };
            #[cfg(not(test))]
            let provider: &mut dyn NativeDatumSource = &mut native;
            let mut checked = CheckedInputs {
                source: provider,
                hosts: hosts
                    .as_mut()
                    .map(|hosts| hosts as &mut dyn LocalHostServices),
            };
            let program = if let Some(index) = deferred_index {
                &mut self.deferred_profiles.as_mut().unwrap().typed[index]
                    .1
                    .program
            } else if use_row {
                &mut self.row_profile.as_mut().unwrap().program
            } else {
                &mut self.program
            };
            let result = program.eval_with_bindings(
                self.limits,
                ctx,
                input.physical_rows(),
                selection,
                &mut checked,
            );
            drop(checked);
            if let Some(error) = hosts.as_mut().and_then(NativeBatchHosts::take_native_error) {
                return Err(error.into());
            }
            let values = result.map_err(BatchFailure::from)?;
            self.windows = self.windows.saturating_add(1);
            let vectors = source
                .bindings
                .len()
                .saturating_add(source.facts.call_sites().len());
            self.vector_allocations = self
                .vector_allocations
                .saturating_add(u64::try_from(vectors).unwrap_or(u64::MAX));
            if source.nodes[0].view.value_role() == Some(NativeValueRole::Output) {
                self.write_native_output(
                    values,
                    selection,
                    output,
                    native_values
                        .as_ref()
                        .ok_or_else(|| binding("native output has no store"))?,
                    pending.as_mut(),
                )?;
            } else if let Some(pending) = typed_pending.as_mut() {
                self.capture_typed_output(values, selection, pending)?;
            } else {
                self.write_output(values, selection, output, self.source.output_index)?;
            }
            start = end;
        }
        if let Some(pending) = typed_pending {
            let rows = pending.chunk.num_rows();
            if rows != selection.len()
                || pending.chunk.column(0).type_size()
                    != output.column(self.source.output_index).type_size()
            {
                return Err(binding("pending typed output layout/length changed").into());
            }
            if native_regexp_row_batch(&self.source.owner.calculated[self.source.calculated_slot]) {
                for row in 0..rows {
                    if pending.chunk.column(0).is_null(row) {
                        output.append_null(self.source.output_index);
                    } else {
                        output.append_int64(
                            self.source.output_index,
                            pending.chunk.column(0).get_int64(row),
                        );
                    }
                }
            } else {
                output.append_column_range_from(
                    self.source.output_index,
                    &pending.chunk,
                    0,
                    0,
                    rows,
                );
            }
        }
        if let Some(pending) = pending {
            for value in &pending.values {
                output.append_datum(self.source.output_index, value);
            }
        }
        Ok(())
    }
}

fn projection_output_count(program: &EvaluatorProgram) -> Result<usize> {
    program
        .column_mapping
        .values()
        .try_fold(program.calculated.len(), |count, outputs| {
            count
                .checked_add(outputs.len())
                .ok_or_else(|| resource("projection output count overflow").into())
        })
}

/// A complete projection: one shared program per calculated output and
/// direct-column ownership transfer only after every calculation succeeds.
/// There is no nested EvaluatorSuite or native expression value callback.
pub(crate) struct PreparedBatchSuite {
    owner: Arc<EvaluatorProgram>,
    workers: Vec<PreparedBatch>,
    schema: Box<[FieldType]>,
    selection: Vec<usize>,
    output_count: usize,
    bind_ready_cache: bool,
}

impl PreparedBatchSuite {
    pub(crate) fn compile(
        suite: &EvaluatorSuite,
        row_schema: &[FieldType],
        new_collation: bool,
        unit: u64,
        _source_limits: BatchSourceLimits,
        execution: ExecutionLimits,
        _max_materialization_retained_bytes: usize,
    ) -> Result<Self> {
        let count = suite.program.calculated.len();
        let limits = BatchSourceLimits {
            tree: CompileLimits::default(),
            max_metadata_bytes: usize::MAX,
        };
        for &index in suite.program.column_mapping.keys() {
            // Direct outputs transfer native Chunk storage. They never enter
            // the closed computation wire carriers, so that carrier admission
            // policy does not apply. Bounds, metadata and invocation layout
            // validation still precede every ownership transfer.
            row_schema
                .get(index)
                .ok_or_else(|| binding("projection column outside row schema"))?;
        }
        let mut workers = Vec::new();
        reserve(&mut workers, count)?;
        for slot in 0..count {
            let worker = PreparedBatch::compile_calculation_profile(
                suite,
                slot,
                row_schema,
                new_collation,
                unit,
                execution,
                true,
            )?;

            workers.push(worker);
        }
        let mut schema = Vec::new();
        reserve(&mut schema, row_schema.len())?;
        schema.extend(row_schema.iter().map(snapshot_field_type));
        let bind_context_native = suite
            .program
            .calculated
            .iter()
            .any(|root| additional_context_native_view(root, limits).is_some());
        Ok(Self {
            owner: Arc::clone(&suite.program),
            bind_ready_cache: bind_context_native || workers.iter().any(|worker| {
                std::iter::once(worker.source.as_ref())
                    .chain(worker.row_profile.iter().map(|row| row.source.as_ref()))
                    .any(|source| {
                        source.host_metadata.iter().any(|meta| {
                            matches!(
                                meta,
                                NativeHostMetadata::WeightString(_)
                                    | NativeHostMetadata::Regexp(_)
                                    | NativeHostMetadata::RegexpValues(_)
                                    | NativeHostMetadata::PbEagerValues(_)
                                    | NativeHostMetadata::PbBinary(_)
                                    | NativeHostMetadata::PbBoolean(_)
                                    | NativeHostMetadata::PbLazy(_)
                            ) || matches!(
                        meta,
                        NativeHostMetadata::HeaderOutcome {
                            kind: crate::scalar_function::header_value::HeaderKind::PbRegexpArity | crate::scalar_function::header_value::HeaderKind::PbBinaryArity | crate::scalar_function::header_value::HeaderKind::PbCastArity,
                            ..
                        }
                    )
                        })
                    })
            }) || suite.program.calculated.iter().any(|root| {
                // The contextual row profiles retain their original initial
                // binding. Pure value roots keep the existing no-Ready path.
                (native_string_row_batch(root) || native_boolean_row_batch(root))
                    && contains_context_value(root, limits.tree.max_depth, &mut {
                        limits.tree.max_nodes
                    })
            }),
            workers,
            schema: schema.into_boxed_slice(),
            selection: Vec::new(),
            output_count: projection_output_count(&suite.program)?,
        })
    }

    pub(crate) fn set_window_size(&mut self, size: usize) {
        for worker in &mut self.workers {
            worker.window_size = size.min(worker.program.max_batch_size());
        }
    }

    pub(crate) fn scratch_snapshot(&self) -> BatchScratchSnapshot {
        self.workers.iter().fold(
            BatchScratchSnapshot {
                selection_capacity: self.selection.capacity(),
                ..Default::default()
            },
            |mut total, worker| {
                let next = worker.scratch_snapshot();
                total.selection_capacity = total
                    .selection_capacity
                    .saturating_add(next.selection_capacity);
                total.vector_allocations = total
                    .vector_allocations
                    .saturating_add(next.vector_allocations);
                total.windows = total.windows.saturating_add(next.windows);
                total
            },
        )
    }

    pub(crate) fn run<C: Columns>(
        &mut self,
        suite: &EvaluatorSuite,
        native_ctx: &C,
        ctx: &mut EvalContext,
        input: &mut Chunk,
        output: &mut Chunk,
        row_schema: &[FieldType],
    ) -> Result<()> {
        if !Arc::ptr_eq(&suite.program, &self.owner) {
            return Err(binding("projection worker belongs to another program").into());
        }
        if self.bind_ready_cache {
            // These original packet value owners retain the suite's once-only
            // binding before flags/children, including its unwind poison guard.
            suite.ready_value_cache.with_columns(native_ctx, |bound| {
                self.run_bound(suite, bound, ctx, input, output, row_schema)
            })
        } else {
            self.run_bound(suite, native_ctx, ctx, input, output, row_schema)
        }
    }
    fn run_bound<C: Columns>(
        &mut self,
        suite: &EvaluatorSuite,
        native_ctx: &C,
        ctx: &mut EvalContext,
        input: &mut Chunk,
        output: &mut Chunk,
        row_schema: &[FieldType],
    ) -> Result<()> {
        if row_schema.len() != self.schema.len()
            || row_schema
                .iter()
                .zip(self.schema.iter())
                .any(|(a, b)| a.collation() != b.collation() || a != b)
            || input.num_cols() != row_schema.len()
            || output.num_cols() != self.output_count
            || output.sel().is_some()
            || output.is_incomplete_chunk()
        {
            return Err(binding("projection input/output schema differs from preparation").into());
        }
        for (index, field) in row_schema.iter().enumerate() {
            if input.column(index).rows() != input.physical_rows()
                || input.column(index).type_size() != get_fixed_len(field)
            {
                return Err(binding("projection input column layout differs").into());
            }
        }
        if input
            .sel()
            .is_some_and(|rows| rows.iter().any(|&row| row >= input.physical_rows()))
        {
            return Err(binding("projection selection exceeds physical rows").into());
        }
        for (&index, outputs) in &self.owner.column_mapping {
            if outputs
                .iter()
                .any(|&out| output.column(out).type_size() != get_fixed_len(&row_schema[index]))
            {
                return Err(binding("projection direct output layout differs").into());
            }
        }
        for worker in &self.workers {
            if worker.source.nodes[0].sql.as_ref().is_some_and(|field| {
                output.column(worker.source.output_index).type_size() != get_fixed_len(field)
            }) {
                return Err(binding("projection calculated output layout differs").into());
            }
        }
        // Caller-owned row selection is borrowed once for all calculated outputs.
        let selection = if let Some(selection) = input.sel() {
            selection
        } else {
            self.selection.clear();
            reserve(&mut self.selection, input.physical_rows())?;
            self.selection.extend(0..input.physical_rows());
            self.selection.as_slice()
        };
        if !self.owner.vectorizable {
            // Host bindings read physical rows; the selection slice supplies the
            // current logical occurrence without constructing a Chunk view.
            for &physical in selection {
                for worker in &mut self.workers {
                    worker.execute_selected_profile(
                        native_ctx,
                        ctx,
                        input,
                        output,
                        true,
                        std::slice::from_ref(&physical),
                        #[cfg(test)]
                        None,
                    )?;
                }
            }
        } else {
            for worker in &mut self.workers {
                if worker
                    .execute_selected_decimal_priority(native_ctx, ctx, input, output, selection)?
                {
                    continue;
                }
                // The original getter belongs to this output and occurs only
                // after its Decimal priority stage declined, even for empty input.
                let vectorized = native_ctx.enable_vectorized_expression();
                let selected_vectorized = worker.select_deferred_profile(native_ctx, vectorized)?;
                if vectorized {
                    if let Some(order) = worker
                        .source
                        .context_policy
                        .as_ref()
                        .and_then(|p| p.root_type_probe)
                    {
                        (|| order.run(native_ctx))()?;
                    }
                }
                worker.execute_selected_profile(
                    native_ctx,
                    ctx,
                    input,
                    output,
                    !selected_vectorized,
                    selection,
                    #[cfg(test)]
                    None,
                )?;
            }
        }
        if let Some(helper) = &suite.column_swap_helper {
            if input.sel().is_some() {
                helper
                    .copy_selected_columns(input, output)
                    .map_err(EvaluatorError::Chunk)?;
            } else {
                helper
                    .swap_columns(input, output)
                    .map_err(EvaluatorError::Chunk)?;
            }
        }
        if self.output_count == 0 {
            output.set_num_virtual_rows(input.num_rows());
        }
        Ok(())
    }
}

// A standalone caller supplies an immutable root and Row without a Suite.
// Legacy allowance arguments remain inert, as for prepared projections.
fn public_scalar_limits() -> (BatchSourceLimits, ExecutionLimits) {
    let execution = ExecutionLimits::default();
    let maximum = execution.max_retained_bytes;
    (
        BatchSourceLimits {
            tree: CompileLimits::default(),
            max_metadata_bytes: maximum,
        },
        execution,
    )
}
impl BatchFailure {
    fn into_public_error(self, phase: crate::ExpressionRuntimeFailurePhase) -> EvalError {
        match self.into_error_at(phase) {
            EvaluatorError::Eval(error) => error,
            EvaluatorError::Chunk(message) => EvalError::ExpressionRuntimeFailure(
                crate::ExpressionRuntimeFailure::from_local_eval(binding(message), Some(phase)),
            ),
        }
    }
}
/// One immutable real expression, prepared without retaining a context or row.
/// Callers preserve their demand/truth order outside the scalar frame and
/// supply the genuine physical coordinate on every invocation.
pub(super) struct PreparedScalarExpression<'a> {
    plan: scalar_source::BorrowedExpression<'a>,
}
impl<'a> PreparedScalarExpression<'a> {
    pub(super) fn compile(
        root: &'a Expression,
        width: usize,
    ) -> std::result::Result<Self, EvalError> {
        let (source, execution) = public_scalar_limits();
        scalar_source::BorrowedExpression::compile(root, width, true, source, execution)
            .map(|plan| Self { plan })
            .map_err(|error| error.into_public_error(crate::ExpressionRuntimeFailurePhase::Prepare))
    }
    pub(super) fn eval(
        &mut self,
        context: &dyn Columns,
        row: tidb_chunk::row::Row<'_>,
    ) -> std::result::Result<Datum, EvalError> {
        self.plan
            .eval(context, row)
            .map_err(|error| error.into_public_error(crate::ExpressionRuntimeFailurePhase::Invoke))
    }
    fn eval_with_error_value(
        &mut self,
        context: &dyn Columns,
        row: tidb_chunk::row::Row<'_>,
    ) -> std::result::Result<Datum, (Datum, EvalError)> {
        self.plan
            .eval_with_error_value(context, row)
            .map_err(|(value, error)| {
                (
                    value,
                    error.into_public_error(crate::ExpressionRuntimeFailurePhase::Invoke),
                )
            })
    }
}
/// Affine compiled state for one genuine immutable Arc-owned tree.
/// The scalar plan owns the tree through its complete metadata lifetime.
pub(crate) struct PreparedOwnedExpression {
    plan: scalar_source::OwnedExpression,
}
impl PreparedOwnedExpression {
    pub(crate) fn compile(
        root: std::sync::Arc<Expression>,
        width: usize,
    ) -> std::result::Result<Self, EvalError> {
        let (source, execution) = public_scalar_limits();
        scalar_source::OwnedExpression::compile(root, width, true, source, execution)
            .map(|plan| Self { plan })
            .map_err(|error| error.into_public_error(crate::ExpressionRuntimeFailurePhase::Prepare))
    }
    pub(crate) fn matches(&self, root: &std::sync::Arc<Expression>, width: usize) -> bool {
        self.plan.matches(root, width)
    }
    pub(crate) fn eval(
        &mut self,
        context: &dyn Columns,
        row: tidb_chunk::row::Row<'_>,
    ) -> std::result::Result<Datum, EvalError> {
        self.plan
            .eval(context, row)
            .map_err(|error| error.into_public_error(crate::ExpressionRuntimeFailurePhase::Invoke))
    }
}
pub(crate) fn eval_public_expression(
    root: &Expression,
    context: &dyn Columns,
    row: tidb_chunk::row::Row<'_>,
) -> std::result::Result<Datum, EvalError> {
    #[cfg(test)]
    crate::test_evaluation::note_shared();
    let mut plan = PreparedScalarExpression::compile(root, row.len())?;
    plan.eval(context, row)
}
pub(crate) fn eval_public_expression_with_error_value(
    root: &Expression,
    context: &dyn Columns,
    row: tidb_chunk::row::Row<'_>,
) -> std::result::Result<Datum, (Datum, EvalError)> {
    #[cfg(test)]
    crate::test_evaluation::note_shared();
    let mut plan =
        PreparedScalarExpression::compile(root, row.len()).map_err(|error| (Datum::Null, error))?;
    plan.eval_with_error_value(context, row)
}
pub(crate) fn eval_public_function(
    root: &ScalarFunction,
    context: &dyn Columns,
    row: tidb_chunk::row::Row<'_>,
) -> std::result::Result<Datum, EvalError> {
    #[cfg(test)]
    crate::test_evaluation::note_shared();
    let (source, execution) = public_scalar_limits();
    let mut plan = scalar_source::BorrowedExpression::compile_function(
        root,
        row.len(),
        true,
        source,
        execution,
    )
    .map_err(|error| error.into_public_error(crate::ExpressionRuntimeFailurePhase::Prepare))?;
    plan.eval(context, row)
        .map_err(|error| error.into_public_error(crate::ExpressionRuntimeFailurePhase::Invoke))
}

#[path = "batch_ast_source.rs"]
pub(crate) mod ast_source;
