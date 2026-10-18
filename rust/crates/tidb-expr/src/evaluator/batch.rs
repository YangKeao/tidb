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
use tidb_query_expr::types::BATCH_MAX_SIZE;

#[path = "batch_decimal_fast_source.rs"]
mod decimal_fast_source;
#[path = "batch_hosts.rs"]
mod hosts;
#[path = "batch_values.rs"]
mod native_values;
use hosts::{
    NativeBatchHosts, NativeHostMetadata, NativeRawValueOperation, WindowArithmeticMetadata,
    WindowArithmeticSource,
};
use native_values::{native_payload_bytes, NativeValueOrigin, NativeValueRole, NativeValueStore};
use std::cell::RefCell;
use tidb_query_expr::local::LocalHostServices;

use crate::expr_collation::CollationInfo;
use crate::pushdown_catalog::CATALOG;
use crate::scalar_function::ScalarFunction;

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
                    return EvalError::Unsupported("invalid UTF-8 string datum").into()
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
        .try_reserve_exact(count)
        .map_err(|_| resource("batch caller reservation failed"))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NativeView {
    Typed,
    JsonBytes,
    ValueToken,
    RootValue,
    BulkValue(NativeEvalType),
    BulkRoot(NativeEvalType),
    DecimalFast,
}
impl NativeView {
    fn value_role(self) -> Option<NativeValueRole> {
        match self {
            Self::ValueToken | Self::BulkValue(_) => Some(NativeValueRole::CharInput),
            Self::RootValue | Self::BulkRoot(_) => Some(NativeValueRole::Output),
            Self::DecimalFast => Some(NativeValueRole::DecimalFast),
            Self::Typed | Self::JsonBytes => None,
        }
    }
    fn bulk_target(self) -> Option<NativeEvalType> {
        match self {
            Self::BulkValue(target) | Self::BulkRoot(target) => Some(target),
            _ => None,
        }
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
fn profile_shape(root: &Expression, limits: BatchSourceLimits) -> LocalResult<()> {
    let mut pending = Vec::new();
    let mut budget = Budget {
        used: 0,
        max: limits.max_metadata_bytes,
    };
    budget.array::<(&Expression, usize)>(1)?;
    reserve(&mut pending, 1)?;
    pending.push((root, 1usize));
    let mut scheduled = 1usize;
    while let Some((node, depth)) = pending.pop() {
        if depth > limits.tree.max_depth || scheduled > limits.tree.max_nodes {
            return Err(resource("native profile source node/depth policy exceeded"));
        }
        let children: &[Expression] = match node {
            Expression::ScalarFunction(function) => &function.args,
            Expression::Constant(constant) => match constant.deferred_expr.as_deref() {
                Some(child) => std::slice::from_ref(child),
                None => &[],
            },
            _ => &[],
        };
        scheduled = scheduled
            .checked_add(children.len())
            .filter(|count| *count <= limits.tree.max_nodes)
            .ok_or_else(|| resource("native profile source node policy exceeded"))?;
        budget.array::<(&Expression, usize)>(children.len())?;
        reserve(&mut pending, children.len())?;
        let next = depth
            .checked_add(1)
            .ok_or_else(|| resource("native profile source depth overflow"))?;
        pending.extend(children.iter().rev().map(|child| (child, next)));
    }
    Ok(())
}
fn promoted_native_root_view(
    root: &Expression,
    limits: BatchSourceLimits,
) -> Result<Option<NativeView>> {
    let Expression::ScalarFunction(function) = root else {
        return Ok(None);
    };
    let row_control = matches!(
        function.func_name.lowercase(),
        "json_sum_crc32" | "str_to_date" | "timestampdiff" | "sig_timestampdiff"
    ) || native_charset_operation(function.func_name.lowercase()).is_some()
        || native_unary_sign_operation(function.func_name.lowercase()).is_some()
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
        return Ok(None);
    }
    let Some(field) = root.static_type() else {
        return Ok(None);
    };
    profile_shape(root, limits)?;
    if row_control {
        // Native controls/comparisons invoke their selected operands per row,
        // even when the outer batch flag is enabled. Eager typed descendants
        // can change demand and error identity without any arithmetic node.
        // A closed native-value subtree therefore keeps its genuine row domain
        // independently of the spelling or carrier types of its descendants.
        return Ok(
            native_value_tree_complete(root, limits.tree.max_depth, &mut { limits.tree.max_nodes })
                .then_some(NativeView::RootValue),
        );
    }
    let target = field.eval_type();
    Ok(if crate::scalar_function::batch_supported(root, target) {
        let view = NativeView::BulkRoot(target);
        bulk_value_tree_complete(root, view, limits.tree.max_depth, &mut {
            limits.tree.max_nodes
        })
        .then_some(view)
    } else {
        native_value_tree_complete(root, limits.tree.max_depth, &mut { limits.tree.max_nodes })
            .then_some(NativeView::RootValue)
    })
}

// Native value roots use the checked output ABI. Ordinary representable
// literals keep their existing broadcast fast path. This inspects borrowed
// kinds/declarations, never converts a payload before the complete byte gate.
fn root_source_view(root: &Expression, limits: CompileLimits) -> NativeView {
    match root {
        Expression::Column(_) => NativeView::RootValue,
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
                return NativeView::Typed;
            };
            let Some(sql) = root.static_type() else {
                return NativeView::Typed;
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
        Expression::Column(column) => {
            column.virtual_expr.is_none() && column.correlated_col_unique_id == 0
        }
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
        Expression::Constant(_) | Expression::Column(_) => true,
        Expression::ScalarFunction(function) => {
            if !native_value_call(function) {
                return false;
            }
            for child in &function.args {
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
        Expression::CorrelatedColumn(_) => false,
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
    NativeConstant { value: Datum },
    Constant { value: ScalarValue },
    Column { index: usize },
    Call { target: CallTarget, arity: usize },
}
impl SourceKind {
    fn capture(node: &Expression, sql: &FieldType, view: NativeView) -> Result<Self> {
        Ok(match node {
            Expression::Constant(value) if view.value_role().is_some() => Self::NativeConstant {
                value: value
                    .literal_value()
                    .ok_or_else(|| BatchFailure::admission("nonliteral native source"))?
                    .clone(),
            },
            Expression::Constant(value) => Self::Constant {
                value: source_literal(
                    value
                        .literal_value()
                        .ok_or_else(|| BatchFailure::admission("nonliteral batch source"))?,
                    sql,
                    view,
                )?,
            },
            Expression::Column(column) => Self::Column {
                index: column.index as usize,
            },
            Expression::ScalarFunction(function) => {
                let (target, arity) = resolve_call_in_view(function, view)?;
                Self::Call { target, arity }
            }
            Expression::CorrelatedColumn(_) => {
                return Err(BatchFailure::admission("correlated source"))
            }
        })
    }
}

#[derive(Debug)]
struct NodeRecord {
    sql: FieldType,
    kind: SourceKind,
    view: NativeView,
}

struct BatchSource {
    // The actual native program owner, not another lowered row/D4 plan. Native
    // evaluation is never called through it by the mandatory consumer.
    owner: Arc<EvaluatorProgram>,
    // Address of the root owned by `owner`. The Arc pins the program and its
    // calculated Vec for this source's lifetime; the address is compared only
    // as an identity and is never dereferenced.
    root_identity: usize,
    calculated_slot: usize,
    output_index: usize,
    unit: u64,
    nodes: Box<[NodeRecord]>,
    bindings: Box<[usize]>, // slot -> all-node source ordinal
    row_schema: Box<[FieldType]>,
    expr: LocalExpr,
    schema: Box<[tipb::FieldType]>,
    facts: BatchFacts,
    hosts: Option<HostCatalog>,
    host_metadata: Box<[NativeHostMetadata]>,
    needs_native_values: bool,
    limits: BatchSourceLimits,
}
impl std::fmt::Debug for BatchSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchSource")
            .field("unit", &self.unit)
            .field("nodes", &self.nodes.len())
            .finish()
    }
}

struct CheckedNode<'a> {
    node: &'a Expression,
    sql: &'a FieldType,
    view: NativeView,
    subtree_end: usize,
    folds_to_constant: bool,
    math_render_bound: Option<usize>,
    numeric_render_bound: Option<usize>,
    numeric_format_failure: Option<[u8; 8]>,
}
struct Budget {
    used: usize,
    max: usize,
}
impl Budget {
    fn add(&mut self, bytes: usize) -> LocalResult<()> {
        self.used = self
            .used
            .checked_add(bytes)
            .filter(|sum| *sum <= self.max)
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
            matches!(argument, Expression::Constant(value) if matches!(value.literal_value(), Some(Datum::BinaryLiteral(_)))),
            domain,
        ));
    }
    if matches!(
        function.func_name.lowercase(),
        "json_sum_crc32" | "str_to_date" | "timestampdiff" | "sig_timestampdiff"
    ) || native_charset_operation(function.func_name.lowercase()).is_some()
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
    let (field, collation, pb) = match node {
        Expression::Constant(value) => (
            value.ret_type.as_ref(),
            &value.collation,
            value.pb_origin().is_some(),
        ),
        Expression::Column(value) => (
            value.ret_type.as_ref(),
            &value.collation,
            value.pb_origin().is_some(),
        ),
        Expression::ScalarFunction(value) => (
            value.ret_type.as_ref(),
            &value.collation,
            value.pb_origin().is_some(),
        ),
        Expression::CorrelatedColumn(_) => {
            return Err(BatchFailure::admission("correlated batch source"))
        }
    };
    if pb {
        if !allow_pb {
            return Err(BatchFailure::admission("PB state inside batch source"));
        }
    }
    Ok((
        field.ok_or_else(|| BatchFailure::admission("missing batch declaration"))?,
        collation,
    ))
}
fn check_batch_call(function: &ScalarFunction, view: NativeView) -> Result<()> {
    let expected_arity = resolve_call_in_view(function, view)?.1;
    if function.args.len() != expected_arity
        || (function.pb_signature().is_some() && !pb_timestamp_diff(function))
        || (function.pb_origin().is_some() && !pb_timestamp_diff(function))
        || function.has_values_offset()
        || function.has_grouping_metadata()
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
    matches!(
        target,
        "signed"
            | "unsigned"
            | "unsigned_in_union"
            | "char"
            | "binary"
            | "decimal"
            | "date"
            | "datetime"
            | "time"
            | "year"
            | "double"
            | "float"
            | "vector"
            | "json"
    )
    .then_some(target)
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
fn native_charset_operation(name: &str) -> Option<tidb_query_expr::NativeCharsetOperation> {
    use tidb_query_expr::NativeCharsetOperation as Op;
    Some(match name {
        "to_binary" => Op::ToBinary,
        "from_binary" => Op::FromBinary,
        "convert_using" => Op::ConvertUsing,
        _ => return None,
    })
}
fn native_value_call(function: &ScalarFunction) -> bool {
    matches!(
        function.func_name.lowercase(),
        "json_sum_crc32" | "str_to_date" | "timestampdiff" | "sig_timestampdiff"
    ) || native_charset_operation(function.func_name.lowercase()).is_some()
        || matches!(
            function.func_name.lowercase(),
            "in" | "interval" | "greatest" | "least"
        )
        || native_unary_sign_operation(function.func_name.lowercase()).is_some()
        || native_arithmetic_operation(function.func_name.lowercase()).is_some()
        || native_character_value_argument(function)
}
fn resolve_call_in_view(
    function: &ScalarFunction,
    view: NativeView,
) -> Result<(CallTarget, usize)> {
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
                    ))
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
                || function.get_static_type().is_none()
                || function.args.iter().any(|arg| arg.static_type().is_none())
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
                }
            }
            Expression::Column(column) => {
                return column
                    .get_static_type()
                    .map(FieldType::is_unsigned)
                    .ok_or_else(|| BatchFailure::admission("integer source has no declaration"))
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
                        ))
                    }
                },
                CallTarget::Host(operation) if operation.output_type() == TikvEvalType::Int => {
                    return Ok(false)
                }
                _ => {
                    return Err(BatchFailure::admission(
                        "integer result kind has no native policy",
                    ))
                }
            },
            Expression::CorrelatedColumn(_) => {
                return Err(BatchFailure::admission("correlated integer kind"))
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
                ))
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
                ))
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

// Complete source and incoming schema byte gates precede EVERY equality and
// detached/projected/C snapshot. Borrowed temporary nodes have no executable
// children and are not a second interpreter. Stable owner interval is required.
#[cfg(test)]
fn preflight<'a>(
    root: &'a Expression,
    row_schema: &[FieldType],
    limits: BatchSourceLimits,
) -> Result<Vec<CheckedNode<'a>>> {
    preflight_view(root, row_schema, limits, None)
}
fn preflight_view<'a>(
    root: &'a Expression,
    row_schema: &[FieldType],
    limits: BatchSourceLimits,
    view: Option<NativeView>,
) -> Result<Vec<CheckedNode<'a>>> {
    if limits.tree.max_nodes == 0 || limits.tree.max_depth == 0 {
        return Err(resource("batch source node/depth policy is zero").into());
    }
    let mut budget = Budget {
        used: 0,
        max: limits.max_metadata_bytes,
    };
    for field in row_schema {
        budget.field(field, 1)?;
    }
    let mut pending = Vec::new();
    let mut checked = Vec::new();
    budget.array::<(&Expression, usize, NativeView, bool)>(1)?;
    reserve(&mut pending, 1)?;
    pending.push((
        root,
        1usize,
        view.unwrap_or_else(|| root_source_view(root, limits.tree)),
        matches!(root, Expression::ScalarFunction(function) if pb_timestamp_diff(function)),
    ));
    let mut scheduled = 1usize;
    while let Some((node, depth, view, allow_pb)) = pending.pop() {
        let allow_pb = allow_pb
            || matches!(node, Expression::ScalarFunction(function) if pb_timestamp_diff(function));
        let (sql, collation) = parts(node, allow_pb)?;
        if view.value_role().is_none() {
            check_type(sql)?;
        } else if !matches!(node, Expression::Constant(_) | Expression::Column(_))
            && !matches!(node, Expression::ScalarFunction(function) if native_value_call(function))
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
            let origin = match node {
                Expression::Constant(v) => v.pb_origin(),
                Expression::Column(v) => v.pb_origin(),
                Expression::ScalarFunction(v) => v.pb_origin(),
                _ => None,
            };
            if let Some(origin) = origin {
                if let Some(original) = origin.effective_type_ref() {
                    budget.field(original, 1)?;
                }
                if !origin.matches_effective_type(sql) {
                    return Err(BatchFailure::admission(
                        "PB effective declaration differs from its original snapshot",
                    ));
                }
            }
        }
        if view != NativeView::Typed {
            budget.field(&native_bytes_type(), 3)?;
        }
        budget.array::<CheckedNode<'_>>(1)?;
        budget.array::<NodeRecord>(1)?;
        budget.array::<BuildNode>(1)?;
        budget.array::<LocalExpr>(1)?;
        let (charset, collate) = collation.charset_and_collation();
        budget.add(charset.len())?;
        budget.add(collate.len())?;
        match node {
            Expression::Constant(value) => {
                let literal = value
                    .literal_value()
                    .ok_or_else(|| BatchFailure::admission("parameter/deferred batch source"))?;
                if view.value_role().is_some() {
                    budget.add(
                        native_payload_bytes(literal)?
                            .checked_mul(3)
                            .ok_or_else(|| resource("native literal source copy size overflow"))?,
                    )?;
                    budget.array::<usize>(1)?;
                    budget.field(&native_bytes_type(), 2)?;
                } else {
                    budget.add(literal_copy_budget(literal, sql)?)?;
                }
            }
            Expression::Column(column) => {
                if column.virtual_expr.is_some() || column.correlated_col_unique_id != 0 {
                    return Err(BatchFailure::admission("virtual/correlated batch column"));
                }
                let index = usize::try_from(column.index)
                    .map_err(|_| BatchFailure::admission("negative batch column index"))?;
                if index >= row_schema.len() {
                    return Err(BatchFailure::admission("batch column outside row schema"));
                }
                budget.add(column.orig_name.len())?;
                budget.array::<usize>(1)?;
                budget.field(sql, 1)?; // projected binding declaration copy
            }
            Expression::ScalarFunction(function) => {
                let arity = function.args.len();
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
                budget.array::<(&Expression, usize, NativeView, bool)>(arity)?;
                check_batch_call(function, view)?;
                budget.add(function.func_name.original().len())?;
                budget.add(function.func_name.lowercase().len())?;
                budget.array::<OrdinaryCallSite>(2)?; // initial and lowered source ordinals
                if matches!(resolve_call_in_view(function, view)?.0, CallTarget::Host(_)) {
                    budget.array::<(SqlBatchHostOperation, HostSignature)>(1)?;
                    budget.array::<NativeHostMetadata>(1)?;
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
                    if native_arithmetic_operation(function.func_name.lowercase()).is_some() {
                        budget.field(sql, 1)?;
                        for arg in &function.args {
                            budget
                                .field(arg.static_type().expect("checked arithmetic source"), 1)?;
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
                                keys.capacity()
                                    .checked_mul(4)
                                    .ok_or_else(|| resource("IN key table allowance overflow"))?,
                            )?;
                            for key in keys {
                                budget.add(
                                    key.len().checked_mul(2).ok_or_else(|| {
                                        resource("IN key copy allowance overflow")
                                    })?,
                                )?;
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
                            budget.field(arg.static_type().expect("checked INTERVAL source"), 1)?;
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
                    budget.array::<tipb::FieldType>(function.args.len())?;
                    budget.field(sql, 1)?;
                    for argument in &function.args {
                        budget.field(
                            argument.static_type().ok_or_else(|| {
                                BatchFailure::admission("host argument has no static declaration")
                            })?,
                            1,
                        )?;
                    }
                }
                let child_depth = depth
                    .checked_add(1)
                    .filter(|depth| *depth <= limits.tree.max_depth)
                    .ok_or_else(|| resource("batch source depth policy exceeded"))?;
                reserve(&mut pending, arity)?;
                for (index, child) in function.args.iter().enumerate().rev() {
                    pending.push((
                        child,
                        child_depth,
                        argument_view(function, view, index),
                        allow_pb,
                    ));
                }
            }
            Expression::CorrelatedColumn(_) => unreachable!(),
        }
        reserve(&mut checked, 1)?;
        checked.push(CheckedNode {
            node,
            sql,
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
        let mut folds_to_constant = matches!(checked[index].node, Expression::Constant(_));
        if let Expression::ScalarFunction(function) = checked[index].node {
            folds_to_constant = !crate::constant_fold::is_unfoldable_call(
                function.func_name.lowercase(),
                function.args.len(),
            );
            for _ in &function.args {
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
        if end <= index || end > checked.len() {
            return Err(binding("source subtree join differs").into());
        }
        checked[index].subtree_end = end;
        checked[index].folds_to_constant = folds_to_constant;
        checked[index].math_render_bound =
            crate::scalar_function::math_diagnostic_node_bound(checked[index].node, math_children)
                .map_err(resource)?;
        checked[index].numeric_format_failure =
            crate::scalar_function::numeric_diagnostic_format_failure(
                checked[index].node,
                numeric_child_failure,
            );
        checked[index].numeric_render_bound =
            crate::scalar_function::numeric_diagnostic_node_bound(
                checked[index].node,
                numeric_children,
            )
            .map_err(resource)?;
    }
    // Charge every owned diagnostic copy group before any temporary literal
    // conversion, source capture, formatting or immutable metadata snapshot.
    for item in &checked {
        if item.view.value_role().is_some()
            && matches!(item.node, Expression::ScalarFunction(function) if function.func_name.lowercase()=="abs")
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
    for (index, item) in checked.iter().enumerate() {
        let Expression::ScalarFunction(function) = item.node else {
            continue;
        };
        if item.view.value_role().is_none()
            || native_arithmetic_operation(function.func_name.lowercase()).is_none()
        {
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
    // Even a temporary representability conversion must wait for the complete
    // source/schema/payload byte gate, including literals later in the graph.
    for item in &checked {
        if let Expression::Constant(value) = item.node {
            if item.view.value_role().is_some() {
                continue;
            }
            source_literal(
                value.literal_value().expect("checked literal"),
                item.sql,
                item.view,
            )?;
        }
        if let Expression::Column(column) = item.node {
            let field = &row_schema[column.index as usize];
            if field.collation() != item.sql.collation() || field != item.sql {
                return Err(BatchFailure::admission(
                    "batch binding differs from complete row declaration",
                ));
            }
        }
    }
    Ok(checked)
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
    limits: CompileLimits,
) -> Result<Vec<OrdinaryCallSite>> {
    let mut sites = Vec::new();
    reserve(&mut sites, original.len())?;
    let mut original = original.into_iter();
    let mut pending = Vec::new();
    reserve(&mut pending, 1)?;
    pending.push(root);
    let mut ordinal = 0usize;
    while let Some(node) = pending.pop() {
        if ordinal >= limits.max_nodes {
            return Err(resource("lowered batch source node policy exceeded").into());
        }
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

fn arithmetic_host_metadata(
    operation: SqlBatchHostOperation,
    sql: &ScalarFunction,
    item: &CheckedNode<'_>,
    ordinal: usize,
    checked: &[CheckedNode<'_>],
) -> Result<NativeHostMetadata> {
    let left = ordinal
        .checked_add(1)
        .ok_or_else(|| resource("arithmetic source ordinal overflow"))?;
    let right = checked
        .get(left)
        .ok_or_else(|| binding("arithmetic left source missing"))?
        .subtree_end;
    let nodes = [left, right];
    let op = crate::scalar_function::binary_op_for_name(sql.func_name.lowercase())
        .ok_or_else(|| binding("arithmetic source operation missing"))?;
    let capture = |float| -> Result<Option<Box<str>>> {
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
        if arithmetic_subject_required(sql, &sql.args[index]) {
            subject_format_failures[index] = child.numeric_format_failure;
            if let Some(bound) = child.numeric_render_bound {
                let text = crate::scalar_function::static_numeric_expression_text(child.node, true);
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
        Some(WindowArithmeticMetadata { domain,
            integer: target == NativeEvalType::Int && domain == NativeEvalType::Int
                && [0, 1].into_iter().all(|index| crate::scalar_function::native_numeric_batch_source(checked[nodes[index]].sql,
                    matches!(&sql.args[index],Expression::Constant(value) if matches!(value.literal_value(),Some(Datum::BinaryLiteral(_)))), domain) == NativeEvalType::Int),
            output_integer: target == NativeEvalType::Int, sources })
    } else {
        None
    };
    let intdiv = if sql.func_name.lowercase() == "intdiv" {
        let mut arguments = [None, None];
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
        Some(Box::new(
            crate::scalar_function::NativeIntDivDiagnostics::new(
                sql,
                arguments,
                nodes.map(|node| checked[node].numeric_format_failure),
            ),
        ))
    } else {
        None
    };
    Ok(NativeHostMetadata::Arithmetic {
                                    batch, operation,name:sql.func_name.lowercase().into(),result_type:snapshot_field_type(item.sql),
                                    argument_nodes:nodes,source_types:[snapshot_field_type(checked[left].sql),snapshot_field_type(checked[right].sql)],
                                    literal:[matches!(sql.args[0],Expression::Constant(_)),matches!(sql.args[1],Expression::Constant(_))],
                                    binary_literal:[0,1].map(|index|matches!(&sql.args[index],Expression::Constant(value) if matches!(value.value,Datum::BinaryLiteral(_)))),
                                    mode:sql.native_arithmetic_mode(),generic_mode:sql.native_arithmetic_generic_mode(),subjects,
                                    subject_format_failures,overflow_format_failure:item.numeric_format_failure,
                                    overflow_expressions:[capture(false)?,capture(true)?],intdiv,output_node:ordinal,
                                    output_role:item.view.value_role().ok_or_else(||binding("arithmetic result role missing"))?,
                                })
}

#[cfg(test)]
fn lower(
    suite: &EvaluatorSuite,
    calculated_slot: usize,
    row_schema: &[FieldType],
    new_collation: bool,
    unit: u64,
    limits: BatchSourceLimits,
) -> Result<Arc<BatchSource>> {
    lower_view(
        suite,
        calculated_slot,
        row_schema,
        new_collation,
        unit,
        limits,
        None,
    )
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
    let root = suite
        .program
        .calculated
        .get(calculated_slot)
        .ok_or_else(|| BatchFailure::admission("batch calculation slot is missing"))?;
    let checked = preflight_view(root, row_schema, limits, view)?;
    let mut host_entries = Vec::new();
    let mut host_metadata = Vec::new();
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
            projected(item.sql, new_collation)?
        };
        let kind = SourceKind::capture(item.node, item.sql, item.view)?;
        let construction = match &kind {
            SourceKind::DecimalFastInput(_) => {
                return Err(binding("fast source entered the ordinary source factory").into())
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
            SourceKind::Call { target, arity } => {
                reserve(&mut sites, 1)?;
                let node =
                    u64::try_from(ordinal).map_err(|_| resource("batch source ID overflow"))?;
                sites.push(OrdinaryCallSite::sql_native_batch(
                    ordinal,
                    OrdinarySourceId::new(unit, node),
                ));
                match *target {
                    CallTarget::Kernel(function) => BuildKind::Call {
                        function,
                        arity: *arity,
                        policy: match item.node {
                            Expression::ScalarFunction(sql) => native_batch_policy(sql),
                            _ => None,
                        },
                    },
                    CallTarget::Host(operation) => {
                        let Expression::ScalarFunction(sql) = item.node else {
                            return Err(binding("host call lost its SQL source").into());
                        };
                        let mut arguments = Vec::new();
                        reserve(&mut arguments, *arity)?;
                        for argument in &sql.args {
                            arguments.push(
                                if matches!(
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
                                ) {
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
                        let slot = host_entries.len();
                        reserve(&mut host_entries, 1)?;
                        reserve(&mut host_metadata, 1)?;
                        host_metadata.push(match operation {
                            SqlBatchHostOperation::TimestampDiffNative { stop_on_null, .. } => {
                                let mut arguments = [0; 3];
                                let mut child = ordinal
                                    .checked_add(1)
                                    .ok_or_else(|| resource("timestamp diff ordinal overflow"))?;
                                for argument in &mut arguments {
                                    *argument = child;
                                    child = checked
                                        .get(child)
                                        .ok_or_else(|| binding("timestamp diff child absent"))?
                                        .subtree_end;
                                }
                                NativeHostMetadata::TimestampDiff {
                                    arguments,
                                    argument_types: std::array::from_fn(|i| {
                                        snapshot_field_type(
                                            sql.args[i]
                                                .static_type()
                                                .expect("checked timestamp argument"),
                                        )
                                    }),
                                    stop_on_null,
                                    result_type: snapshot_field_type(item.sql),
                                    output_node: ordinal,
                                    output_role: item.view.value_role(),
                                }
                            }
                            SqlBatchHostOperation::StrToDateNative { output } => {
                                let first = ordinal
                                    .checked_add(1)
                                    .ok_or_else(|| resource("date source ordinal overflow"))?;
                                let second = checked
                                    .get(first)
                                    .ok_or_else(|| binding("date first source absent"))?
                                    .subtree_end;
                                NativeHostMetadata::StrToDate {
                                    arguments: [first, second],
                                    result_type: snapshot_field_type(item.sql),
                                    output_node: ordinal,
                                    output_role: item.view.value_role(),
                                    output,
                                }
                            }
                            SqlBatchHostOperation::JsonSumCrc32Native { .. } => {
                                NativeHostMetadata::JsonSumCrc32 {
                                    argument_node: ordinal.checked_add(1).ok_or_else(|| {
                                        resource("JSON checksum argument ordinal overflow")
                                    })?,
                                    result_type: snapshot_field_type(item.sql),
                                    output_node: ordinal,
                                    output_role: item.view.value_role(),
                                }
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
                                let source_type = snapshot_field_type(source.sql);
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
                                    result_type: snapshot_field_type(item.sql),
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
                                        if source.sql.eval_type() == NativeEvalType::Int {
                                            0
                                        } else {
                                            source.sql.decimal()
                                        },
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
                                    result_type: snapshot_field_type(item.sql),
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
                                        field: snapshot_field_type(source.sql),
                                    });
                                    child = source.subtree_end;
                                }
                                let search = tidb_query_expr::NativeIntervalSearch::new(
                                    arguments.iter().map(|argument| {
                                        Some(crate::tikv::interval_field_metadata(&argument.field))
                                    }),
                                )
                                .map_err(crate::tikv::interval_value_error)?;
                                NativeHostMetadata::Interval(hosts::NativeIntervalMetadata {
                                    arguments: arguments.into_boxed_slice(),
                                    search,
                                    result_type: snapshot_field_type(item.sql),
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
                                        field: snapshot_field_type(source.sql),
                                        facts: crate::ops::OperandFacts {
                                            eval_type: Some(source.sql.eval_type()),
                                            unsigned: source.sql.is_unsigned(),
                                            constant: source.folds_to_constant,
                                            duration_column: matches!(
                                                source.node,
                                                Expression::Column(_)
                                            ) && source.sql.code()
                                                == FieldTypeCode::Duration,
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
                                    result_type: snapshot_field_type(item.sql),
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
                                    result_type: snapshot_field_type(item.sql),
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
                                    result_type: snapshot_field_type(item.sql),
                                    source_type: snapshot_field_type(child.sql),
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
                                                checked[left].sql.is_unsigned(),
                                                right.is_some_and(|right| {
                                                    checked[right].sql.is_unsigned()
                                                }),
                                            ],
                                        }
                                    }
                                    SqlBatchHostOperation::UnarySignNative(sign) => {
                                        NativeRawValueOperation::UnarySign {
                                            sign,
                                            unsigned: checked[left].sql.is_unsigned(),
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
                                    name: sql.func_name.lowercase().into(),
                                    result_type: snapshot_field_type(item.sql),
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
                                    result_type: snapshot_field_type(item.sql),
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
                                            eval_type: Some(child.sql.eval_type()),
                                            unsigned: child.sql.is_unsigned(),
                                            constant: child.folds_to_constant,
                                            duration_column: matches!(
                                                child.node,
                                                Expression::Column(_)
                                            ) && child.sql.code()
                                                == FieldTypeCode::Duration,
                                        })
                                    };
                                NativeHostMetadata::Comparison {
                                    operation,
                                    name: sql.func_name.lowercase().into(),
                                    result_type: snapshot_field_type(item.sql),
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
                                    item.sql,
                                )?,
                                name: sql.func_name.lowercase().into(),
                                source_type: snapshot_field_type(
                                    sql.args[0].static_type().expect("checked source"),
                                ),
                                input_is_literal: matches!(sql.args[0], Expression::Constant(_)),
                                result_type: snapshot_field_type(item.sql),
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
                            operation,
                            HostSignature {
                                arg_types: arguments.into_boxed_slice(),
                                return_type: field.clone(),
                            },
                        ));
                        BuildKind::Host {
                            slot,
                            arity: *arity,
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
            sql: snapshot_field_type(item.sql),
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
    let sites = lowered_call_sites(&expr, sites, limits.tree)?;
    let facts = match &hosts {
        Some(hosts) => {
            BatchFacts::sql_native_batch_with_hosts(&expr, &schema, sites, limits.tree, hosts)?
        }
        None => BatchFacts::sql_native_batch(&expr, &schema, sites, limits.tree)?,
    };
    let mut detached = Vec::new();
    reserve(&mut detached, row_schema.len())?;
    detached.extend(row_schema.iter().map(snapshot_field_type));
    let needs_native_values = nodes.iter().any(|node| node.view.value_role().is_some());
    Ok(Arc::new(BatchSource {
        owner: Arc::clone(&suite.program),
        root_identity: root as *const Expression as usize,
        calculated_slot,
        output_index: suite.program.calculated_output_indexes[calculated_slot],
        unit,
        nodes: nodes.into_boxed_slice(),
        bindings: bindings.into_boxed_slice(),
        row_schema: detached.into_boxed_slice(),
        expr,
        schema: schema.into_boxed_slice(),
        facts,
        hosts,
        host_metadata: host_metadata.into_boxed_slice(),
        needs_native_values,
        limits,
    }))
}

/// Independent compiled batch owner. No raw evaluation method accepts
/// arbitrary rows; every invocation goes through the real suite dispatch seal.
struct RowProfile {
    source: Arc<BatchSource>,
    program: LocalBatchProgram,
}
// Native eligible batches publish only after all rows have succeeded, even
// when each control subtree itself has row demand. This is output staging,
// not a second expression executor.
#[derive(Default)]
struct PendingNativeOutput {
    values: Vec<Datum>,
    payload_bytes: usize,
}

// A private, unaliased one-column owner. Only the existing typed materializer
// writes it; no expression work occurs here. Keep actual buffer capacities and
// an opaque column-slot/header allowance, not Go's public struct-size constant.
struct PendingTypedOutput {
    chunk: Chunk,
}
impl PendingTypedOutput {
    fn header_bytes() -> usize {
        size_of::<Chunk>() + 2 * size_of::<tidb_chunk::ColumnHandle>()
    }
    fn initial_bytes(field: &FieldType) -> LocalResult<usize> {
        Self::header_bytes()
            .checked_add(usize::try_from(get_fixed_len(field)).unwrap_or(size_of::<i64>()))
            .ok_or_else(|| resource("pending typed header size overflow"))
    }
    fn retained_bytes(&self) -> LocalResult<usize> {
        let column = self.chunk.column(0);
        Self::header_bytes()
            .checked_add(column.data_capacity())
            .and_then(|bytes| bytes.checked_add(column.null_bitmap_capacity()))
            .and_then(|bytes| {
                column
                    .offset_capacity()
                    .checked_mul(size_of::<i64>())
                    .and_then(|offsets| bytes.checked_add(offsets))
            })
            .and_then(|bytes| bytes.checked_add(usize::try_from(column.type_size()).unwrap_or(0)))
            .ok_or_else(|| resource("pending typed retained size overflow"))
    }
}

pub(crate) struct PreparedBatch {
    defer_batch_publication: bool,
    fast_profile: Option<decimal_fast_source::PreparedFast>,
    row_profile: Option<RowProfile>,
    source: Arc<BatchSource>,
    program: LocalBatchProgram,
    limits: ExecutionLimits,
    max_materialization_retained_bytes: usize,
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
            source_limits,
            execution,
            max_materialization_retained_bytes,
            false,
        )
    }
    fn compile_calculation_profile(
        suite: &EvaluatorSuite,
        calculated_slot: usize,
        row_schema: &[FieldType],
        new_collation: bool,
        unit: u64,
        source_limits: BatchSourceLimits,
        execution: ExecutionLimits,
        max_materialization_retained_bytes: usize,
        promote: bool,
    ) -> Result<Self> {
        let root = suite
            .program
            .calculated
            .get(calculated_slot)
            .ok_or_else(|| BatchFailure::admission("batch calculation slot missing"))?;
        let view = if promote {
            promoted_native_root_view(root, source_limits)?
        } else {
            None
        };
        let pair = matches!(root, Expression::ScalarFunction(function) if view.is_some_and(|view| window_value_operation(function, view).is_some()));
        let fast = promote && decimal_fast_source::possible(root);
        let groups = 1 + usize::from(pair) + usize::from(fast);
        let limits = if groups > 1 {
            BatchSourceLimits {
                max_metadata_bytes: source_limits.max_metadata_bytes / groups,
                ..source_limits
            }
        } else {
            source_limits
        };
        let source = lower_view(
            suite,
            calculated_slot,
            row_schema,
            new_collation,
            unit,
            limits,
            view,
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
        let window_size = program.max_batch_size();
        Ok(Self {
            // Eligibility selects the native publication boundary, while the
            // compiled program independently selects operand demand. Both
            // typed carriers and native-value capabilities need the same native
            // publication boundary when their root is not already whole.
            defer_batch_publication: promote
                && !program.requires_whole_selection()
                && crate::scalar_function::static_batch_target(root).is_some(),
            fast_profile,
            row_profile,
            source,
            program,
            limits: execution,
            max_materialization_retained_bytes,
            selection: Vec::new(),
            window_size,
            vector_allocations: 0,
            windows: 0,
        })
    }

    fn compiled_node_count(&self) -> LocalResult<usize> {
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
            .ok_or_else(|| resource("profile graph count overflow"))
    }
    pub(crate) fn scratch_snapshot(&self) -> BatchScratchSnapshot {
        BatchScratchSnapshot {
            selection_capacity: self.selection.capacity(),
            vector_allocations: self.vector_allocations,
            windows: self.windows,
        }
    }
    #[cfg(test)]
    pub(crate) fn declared_type_snapshot(&self) -> FieldType {
        snapshot_field_type(&self.source.nodes[0].sql)
    }

    fn validate_invocation(
        &mut self,
        suite: &EvaluatorSuite,
        input: &Chunk,
        output: &Chunk,
        row_schema: &[FieldType],
    ) -> Result<()> {
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
        // caller-owned row schema still needs a bounded live comparison.
        let mut schema_budget = Budget {
            used: 0,
            max: self.source.limits.max_metadata_bytes,
        };
        for field in row_schema {
            schema_budget.field(field, 1)?;
        }
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
            || output.column(self.source.output_index).type_size()
                != get_fixed_len(&self.source.nodes[0].sql)
            || output.sel().is_some()
            || output.is_incomplete_chunk()
        {
            return Err(LocalError::InvalidBatch(
                "batch destination layout differs from its declared output".into(),
            )
            .into());
        }
        let count = input.num_rows();
        self.selection.clear();
        reserve(&mut self.selection, count)?;
        if let Some(selected) = input.sel() {
            if selected.iter().any(|row| *row >= physical) {
                return Err(LocalError::InvalidBatch(
                    "batch selection exceeds physical rows".into(),
                )
                .into());
            }
            self.selection.extend_from_slice(selected);
        } else {
            self.selection.extend(0..physical);
        }
        self.accept_retained(array_bytes::<usize>(self.selection.capacity())?)?;
        Ok(())
    }
    fn accept_retained(&self, bytes: usize) -> LocalResult<()> {
        if bytes > self.max_materialization_retained_bytes {
            Err(resource(
                "batch caller retained materialization policy exceeded",
            ))
        } else {
            Ok(())
        }
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
        let bytes = values
            .retained_heap_bytes()
            .ok_or_else(|| resource("native result retained size overflow"))?
            .checked_add(array_bytes::<usize>(self.selection.capacity())?)
            .ok_or_else(|| resource("native result/selection size overflow"))?;
        self.accept_retained(bytes)?;
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
                let payload_bytes = pending
                    .payload_bytes
                    .checked_add(
                        native_values::native_payload_bytes(value)?
                            .checked_mul(2)
                            .ok_or_else(|| resource("pending native payload allowance overflow"))?,
                    )
                    .ok_or_else(|| resource("pending native payload total overflow"))?;
                let count = pending
                    .values
                    .len()
                    .checked_add(1)
                    .ok_or_else(|| resource("pending native result count overflow"))?;
                self.accept_retained(
                    bytes
                        .checked_add(array_bytes::<Datum>(count)?)
                        .and_then(|bytes| bytes.checked_add(payload_bytes))
                        .ok_or_else(|| resource("pending native result size overflow"))?,
                )?;
                pending
                    .values
                    .try_reserve(1)
                    .map_err(|_| resource("pending native result reservation failed"))?;
                self.accept_retained(
                    bytes
                        .checked_add(array_bytes::<Datum>(pending.values.capacity())?)
                        .and_then(|bytes| bytes.checked_add(payload_bytes))
                        .ok_or_else(|| resource("pending native result capacity overflow"))?,
                )?;
                pending.values.push(value.clone());
                pending.payload_bytes = payload_bytes;
            } else {
                output.append_datum(self.source.output_index, value);
            }
        }
        Ok(())
    }

    fn typed_carrier_bytes(values: &VectorValue) -> LocalResult<usize> {
        let carrier_bytes = match values {
            VectorValue::Int(ints) => array_bytes::<i64>(ints.capacity())?.checked_add(
                ints.get_bit_vec()
                    .retained_heap_bytes()
                    .ok_or_else(|| resource("batch bitmap size overflow"))?,
            ),
            VectorValue::Real(reals) => array_bytes::<Real>(reals.capacity())?.checked_add(
                reals
                    .get_bit_vec()
                    .retained_heap_bytes()
                    .ok_or_else(|| resource("batch bitmap size overflow"))?,
            ),
            VectorValue::Decimal(decimals) => array_bytes::<TikvDecimal>(decimals.capacity())?
                .checked_add(
                    decimals
                        .get_bit_vec()
                        .retained_heap_bytes()
                        .ok_or_else(|| resource("batch bitmap size overflow"))?,
                )
                .and_then(|bytes| {
                    decimals
                        .checked_decimal_spill_bytes()
                        .and_then(|spill| bytes.checked_add(spill))
                }),
            VectorValue::Bytes(bytes) => bytes.retained_heap_bytes(),
            VectorValue::DateTime(values) => array_bytes::<TikvDateTime>(values.capacity())?
                .checked_add(
                    values
                        .get_bit_vec()
                        .retained_heap_bytes()
                        .ok_or_else(|| resource("batch bitmap size overflow"))?,
                ),
            VectorValue::Duration(values) => array_bytes::<TikvDuration>(values.capacity())?
                .checked_add(
                    values
                        .get_bit_vec()
                        .retained_heap_bytes()
                        .ok_or_else(|| resource("batch bitmap size overflow"))?,
                ),
            VectorValue::Json(values) => values.retained_heap_bytes(),
            VectorValue::VectorFloat32(values) => values.retained_heap_bytes(),
            VectorValue::Enum(_) | VectorValue::Set(_) => None,
        }
        .ok_or_else(|| resource("batch carrier retained size overflow"))?;
        Ok(carrier_bytes)
    }

    fn capture_typed_output(
        &self,
        values: VectorValue,
        selection: &[usize],
        pending: &mut PendingTypedOutput,
    ) -> Result<()> {
        // Preserve shape/error priority before any copy or resource-based refusal.
        let expected = batch_eval_type(self.source.nodes[0].sql.eval_type());
        if values.eval_type() != expected || values.len() != selection.len() {
            return Err(binding("batch result shape differs from computed boundary").into());
        }
        let carrier = Self::typed_carrier_bytes(&values)?;
        let count = values.len();
        let width = usize::try_from(get_fixed_len(&self.source.nodes[0].sql)).unwrap_or(0);
        // Two encoded layouts plus two carrier payloads cover known conversion
        // scratch and the pending destination. Post-copy capacities are checked
        // separately; this is not a hard allocator-peak guarantee.
        let layouts = width
            .checked_add(size_of::<i64>() + 1)
            .and_then(|per_row| per_row.checked_mul(count))
            .and_then(|bytes| bytes.checked_mul(2))
            .ok_or_else(|| resource("pending typed copy layout overflow"))?;
        let before = pending
            .retained_bytes()?
            .checked_add(array_bytes::<usize>(self.selection.capacity())?)
            .and_then(|bytes| bytes.checked_add(carrier))
            .and_then(|bytes| {
                carrier
                    .checked_mul(2)
                    .and_then(|copy| bytes.checked_add(copy))
            })
            .and_then(|bytes| bytes.checked_add(layouts))
            .ok_or_else(|| resource("pending typed copy allowance overflow"))?;
        self.accept_retained(before)?;
        self.write_output(values, selection, &mut pending.chunk, 0)?;
        self.accept_retained(
            pending
                .retained_bytes()?
                .checked_add(array_bytes::<usize>(self.selection.capacity())?)
                .ok_or_else(|| resource("pending typed/selection size overflow"))?,
        )?;
        Ok(())
    }

    fn write_output(
        &self,
        values: VectorValue,
        selection: &[usize],
        output: &mut Chunk,
        output_index: usize,
    ) -> Result<()> {
        let expected = batch_eval_type(self.source.nodes[0].sql.eval_type());
        if values.eval_type() != expected || values.len() != selection.len() {
            return Err(binding("batch result shape differs from computed boundary").into());
        }
        let carrier_bytes = Self::typed_carrier_bytes(&values)?;
        let source_bytes = carrier_bytes
            .checked_add(array_bytes::<usize>(self.selection.capacity())?)
            .ok_or_else(|| resource("batch source/selection retained size overflow"))?;
        self.accept_retained(source_bytes)?;
        // Shape and retained-memory checks complete before the first write, so
        // a binding/resource error cannot leave a partially appended column.
        // The destination Chunk owns its ordinary allocation policy just as it
        // does for every other evaluator route.
        // Invocation rejects output selections. Resolve the destination owner
        // once for the whole window, preserving ColumnWrite alias/COW handling.
        let mut output = output.column_mut(output_index);
        match &values {
            VectorValue::Int(ints) => {
                output.append_fixed_cells((0..values.len()).map(|index| {
                    ChunkRef::get_option_ref(ints, index).map(|value| value.to_ne_bytes())
                }));
            }
            VectorValue::Real(reals) => {
                output.append_fixed_cells((0..values.len()).map(|index| {
                    ChunkRef::get_option_ref(reals, index)
                        .map(|value| value.into_inner().to_ne_bytes())
                }));
            }
            VectorValue::Decimal(decimals) => {
                let convert = |value: &TikvDecimal| {
                    tidb_datatype::MyDecimal::try_from_shared_math(
                        value,
                        self.max_materialization_retained_bytes,
                    )
                    .map_err(|_| binding("batch Decimal output is not representable"))
                };
                let converted_bytes =
                    array_bytes::<Option<tidb_datatype::MyDecimal>>(values.len())?;
                self.accept_retained(
                    source_bytes
                        .checked_add(converted_bytes)
                        .ok_or_else(|| resource("batch Decimal conversion size overflow"))?,
                )?;
                // Validate every cell once before publishing any output. The
                // previous adapter performed the same conversion twice.
                let converted = (0..values.len())
                    .map(|index| {
                        ChunkRef::get_option_ref(decimals, index)
                            .map(&convert)
                            .transpose()
                    })
                    .collect::<LocalResult<Vec<_>>>()?;
                self.accept_retained(
                    source_bytes
                        .checked_add(array_bytes::<Option<tidb_datatype::MyDecimal>>(
                            converted.capacity(),
                        )?)
                        .ok_or_else(|| resource("batch Decimal conversion size overflow"))?,
                )?;
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
struct NativeChunk<'a> {
    source: &'a BatchSource,
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
        let node = &self.source.nodes[ordinal];
        if let SourceKind::DecimalFastInput(value) = &node.kind {
            let store = self
                .native_values
                .ok_or_else(|| binding("fast source store missing"))?;
            let value = value.read(self.input, row.input_row)?;
            let token = store
                .borrow_mut()
                .insert_decimal_fast(value, NativeValueOrigin { node: ordinal, row })?;
            return Ok(Datum::Bytes(token));
        }
        if node.view.value_role().is_some() {
            let store = self
                .native_values
                .ok_or_else(|| binding("native source has no value store"))?;
            let token = match &node.kind {
                SourceKind::NativeConstant { value } => {
                    // Exactly the public Expression.Eval result view. Borrow
                    // unchanged values until insert_clone's pre-copy gate.
                    let integer = crate::expression::enum_set_integer_view(value, Some(&node.sql))
                        .map(Datum::UInt);
                    store.borrow_mut().insert_clone(
                        integer.as_ref().unwrap_or(value),
                        node.view.value_role().expect("checked native view"),
                        NativeValueOrigin { node: ordinal, row },
                    )?
                }
                SourceKind::Column { index } => {
                    let column = self.input.column(*index);
                    let cells = column.raw_cells();
                    store
                        .borrow()
                        .precheck_import(if cells.is_null(row.input_row) {
                            0
                        } else {
                            cells.get(row.input_row).len()
                        })?;
                    let value = self
                        .input
                        .physical_row(row.input_row)
                        .get_datum(*index, &node.sql);
                    let value =
                        crate::expression::apply_enum_set_integer_view(value, Some(&node.sql));
                    store.borrow_mut().insert_clone(
                        &value,
                        node.view.value_role().expect("checked native view"),
                        NativeValueOrigin { node: ordinal, row },
                    )?
                }
                _ => return Err(binding("native value binding has no native source")),
            };
            return Ok(Datum::Bytes(token));
        }
        let SourceKind::Column { index, .. } = &node.kind else {
            return Err(binding("batch slot has no native column"));
        };
        // Physical already: get_row would apply the actual Chunk Sel twice.
        let value = self
            .input
            .physical_row(row.input_row)
            .get_datum(*index, &node.sql);
        if node.view == NativeView::JsonBytes {
            return match value {
                Datum::Null => Ok(Datum::Null),
                Datum::Json(value) => Ok(Datum::Bytes(value.encoded())),
                _ => Err(binding("native JSON view read a non-JSON source")),
            };
        }
        Ok(value)
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
            let store = self
                .native_values
                .ok_or_else(|| binding("fast source store missing"))?;
            let count = physical_rows.len();
            let payload = count
                .checked_mul(native_values::HANDLE_BYTES)
                .ok_or_else(|| resource("fast input vector bytes overflow"))?;
            store.borrow().precheck_import(
                payload
                    .checked_add(array_bytes::<usize>(
                        count
                            .checked_add(1)
                            .ok_or_else(|| resource("fast input offsets overflow"))?,
                    )?)
                    .ok_or_else(|| resource("fast input vector allowance overflow"))?,
            )?;
            let mut output = ChunkedVecBytes::try_with_capacities(count, payload)
                .map_err(|error| LocalError::ResourceLimit(error.to_string()))?;
            let mut unsupported = false;
            for (occurrence, &input_row) in physical_rows.iter().enumerate() {
                let observed = if unsupported {
                    tidb_query_expr::NativeDecimalFastOutcome::Unsupported
                } else {
                    value.read(self.input, input_row)?
                };
                unsupported |= matches!(
                    observed,
                    tidb_query_expr::NativeDecimalFastOutcome::Unsupported
                );
                let token = store.borrow_mut().insert_decimal_fast(
                    observed,
                    NativeValueOrigin {
                        node: ordinal,
                        row: InputRow {
                            occurrence,
                            input_row,
                        },
                    },
                )?;
                output.push_ref(Some(&token));
            }
            return Ok(Some(VectorValue::Bytes(output)));
        }
        if node.view.value_role().is_some() {
            return Ok(None);
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
        match node.sql.eval_type() {
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
                let fsp = node.sql.decimal();
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
            || invocation.candidate.target() != source.nodes[0].sql.eval_type()
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
    fn execute_profile(
        &mut self,
        native_ctx: &dyn Columns,
        ctx: &mut EvalContext,
        input: &Chunk,
        output: &mut Chunk,
        row_domain: bool,
        #[cfg(test)] mut override_source: Option<&mut dyn NativeDatumSource>,
    ) -> Result<()> {
        let use_row = row_domain && self.row_profile.is_some();
        let source = Arc::clone(if use_row {
            &self
                .row_profile
                .as_ref()
                .expect("selected row profile")
                .source
        } else {
            &self.source
        });
        let whole = if use_row {
            self.row_profile
                .as_ref()
                .unwrap()
                .program
                .requires_whole_selection()
        } else {
            self.program.requires_whole_selection()
        };
        let window_size = if row_domain { 1 } else { self.window_size };
        let defer = !row_domain && self.defer_batch_publication;
        let native_output = source.nodes[0].view.value_role() == Some(NativeValueRole::Output);
        let mut pending = (defer && native_output).then(PendingNativeOutput::default);
        let mut typed_pending = if defer && !native_output && !self.selection.is_empty() {
            self.accept_retained(
                PendingTypedOutput::initial_bytes(&source.nodes[0].sql)?
                    .checked_add(array_bytes::<usize>(self.selection.capacity())?)
                    .ok_or_else(|| resource("pending typed initialization size overflow"))?,
            )?;
            let pending = PendingTypedOutput {
                chunk: Chunk::new_empty(std::slice::from_ref(&source.nodes[0].sql)),
            };
            self.accept_retained(
                pending
                    .retained_bytes()?
                    .checked_add(array_bytes::<usize>(self.selection.capacity())?)
                    .ok_or_else(|| resource("pending typed initial owner size overflow"))?,
            )?;
            Some(pending)
        } else {
            None
        };
        let native_values = if source.needs_native_values {
            Some(RefCell::new(NativeValueStore::new(
                self.max_materialization_retained_bytes,
            )?))
        } else {
            None
        };
        let mut hosts = source.hosts.clone().map(|catalog| {
            NativeBatchHosts::new(
                catalog,
                &source.host_metadata,
                native_ctx,
                self.max_materialization_retained_bytes,
            )
            .with_native_values(native_values.as_ref())
        });
        // Explicit whole programs receive the complete selection. Other
        // programs use bounded driver windows in the same canonical route;
        // eligible batch results are staged across those windows. No error
        // selects a different evaluator or retries an expression.
        let mut start = 0;
        while start < self.selection.len() {
            let end = if whole {
                self.selection.len()
            } else {
                self.selection.len().min(start.saturating_add(window_size))
            };
            let selection = &self.selection[start..end];
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
            let program = if use_row {
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
            if rows != self.selection.len()
                || pending.chunk.column(0).type_size()
                    != output.column(self.source.output_index).type_size()
            {
                return Err(binding("pending typed output layout/length changed").into());
            }
            output.append_column_range_from(self.source.output_index, &pending.chunk, 0, 0, rows);
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

/// A complete pure projection: one shared program per calculated output and
/// direct-column ownership transfer only after every calculation succeeds.
/// There is no nested EvaluatorSuite or native expression value callback.
pub(crate) struct PreparedBatchSuite {
    owner: Arc<EvaluatorProgram>,
    workers: Vec<PreparedBatch>,
    schema: Box<[FieldType]>,
    source_limits: BatchSourceLimits,
    output_count: usize,
}

impl PreparedBatchSuite {
    pub(crate) fn compile(
        suite: &EvaluatorSuite,
        row_schema: &[FieldType],
        new_collation: bool,
        unit: u64,
        source_limits: BatchSourceLimits,
        execution: ExecutionLimits,
        max_materialization_retained_bytes: usize,
    ) -> Result<Self> {
        if !suite.program.vectorizable {
            return Err(BatchFailure::admission(
                "projection requires ordered effectful evaluation",
            ));
        }
        let count = suite.program.calculated.len();
        let groups = count
            .checked_add(1)
            .ok_or_else(|| resource("projection source group overflow"))?;
        let worker_bytes = array_bytes::<PreparedBatch>(count)?;
        let metadata_bytes = source_limits
            .max_metadata_bytes
            .checked_sub(worker_bytes)
            .ok_or_else(|| resource("projection worker metadata policy exceeded"))?;
        let limits = BatchSourceLimits {
            max_metadata_bytes: metadata_bytes / groups,
            ..source_limits
        };
        let mut budget = Budget {
            used: 0,
            max: limits.max_metadata_bytes,
        };
        for field in row_schema {
            budget.field(field, 1)?;
        }
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
        let mut remaining_nodes = limits.tree.max_nodes;
        for slot in 0..count {
            let mut calculation_limits = limits;
            calculation_limits.tree.max_nodes = remaining_nodes;
            let worker = PreparedBatch::compile_calculation_profile(
                suite,
                slot,
                row_schema,
                new_collation,
                unit,
                calculation_limits,
                execution,
                max_materialization_retained_bytes / count.max(1),
                true,
            )?;
            remaining_nodes = remaining_nodes
                .checked_sub(worker.compiled_node_count()?)
                .ok_or_else(|| resource("projection source node policy exceeded"))?;
            workers.push(worker);
        }
        let mut schema = Vec::new();
        reserve(&mut schema, row_schema.len())?;
        schema.extend(row_schema.iter().map(snapshot_field_type));
        Ok(Self {
            owner: Arc::clone(&suite.program),
            workers,
            schema: schema.into_boxed_slice(),
            source_limits: limits,
            output_count: projection_output_count(&suite.program)?,
        })
    }

    pub(crate) fn set_window_size(&mut self, size: usize) {
        for worker in &mut self.workers {
            worker.window_size = size.min(worker.program.max_batch_size());
        }
    }

    pub(crate) fn scratch_snapshot(&self) -> BatchScratchSnapshot {
        self.workers
            .iter()
            .fold(BatchScratchSnapshot::default(), |mut total, worker| {
                let next = worker.scratch_snapshot();
                total.selection_capacity = total
                    .selection_capacity
                    .saturating_add(next.selection_capacity);
                total.vector_allocations = total
                    .vector_allocations
                    .saturating_add(next.vector_allocations);
                total.windows = total.windows.saturating_add(next.windows);
                total
            })
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
        // Bound metadata before comparing it, even for a projection containing
        // only direct columns or an empty selected-row set.
        let mut budget = Budget {
            used: 0,
            max: self.source_limits.max_metadata_bytes,
        };
        for field in row_schema {
            budget.field(field, 1)?;
        }
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
        // Validate every output before any kernel or ownership move.
        for worker in &mut self.workers {
            worker.validate_invocation(suite, input, output, row_schema)?;
        }
        for worker in &mut self.workers {
            if worker.execute_decimal_priority(native_ctx, ctx, input, output)? {
                continue;
            }
            // The original getter belongs to this output and occurs only
            // after its Decimal priority stage declined, even for empty input.
            let vectorized = native_ctx.enable_vectorized_expression();
            worker.execute_profile(
                native_ctx,
                ctx,
                input,
                output,
                !vectorized,
                #[cfg(test)]
                None,
            )?;
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
