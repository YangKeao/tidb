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

#[path = "batch_hosts.rs"]
mod hosts;
#[path = "batch_values.rs"]
mod native_values;
use hosts::{NativeBatchHosts, NativeHostMetadata};
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
}

#[derive(Clone, Copy, Debug)]
enum CallTarget {
    Kernel(FunctionRef),
    Host(SqlBatchHostOperation),
}

#[derive(Debug)]
enum SourceKind {
    NativeConstant { value: Datum },
    Constant { value: ScalarValue },
    Column { index: usize },
    Call { target: CallTarget, arity: usize },
}
impl SourceKind {
    fn capture(node: &Expression, sql: &FieldType, view: NativeView) -> Result<Self> {
        Ok(match node {
            Expression::Constant(value) if view == NativeView::ValueToken => Self::NativeConstant {
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
                let (target, arity) = resolve_call(function)?;
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
    if view == NativeView::ValueToken {
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
fn argument_view(function: &ScalarFunction) -> NativeView {
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
        (NativeEvalType::Duration, Datum::Duration(value)) => TikvDuration::from_nanos(
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
fn parts(node: &Expression) -> Result<(&FieldType, &CollationInfo)> {
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
        return Err(BatchFailure::admission("PB state inside batch source"));
    }
    Ok((
        field.ok_or_else(|| BatchFailure::admission("missing batch declaration"))?,
        collation,
    ))
}
fn check_batch_call(function: &ScalarFunction) -> Result<()> {
    let expected_arity = resolve_call(function)?.1;
    if function.args.len() != expected_arity
        || function.pb_signature().is_some()
        || function.pb_origin().is_some()
        || function.has_values_offset()
        || function.has_grouping_metadata()
    {
        return Err(BatchFailure::admission("outside native batch call closure"));
    }
    Ok(())
}
fn resolve_call(function: &ScalarFunction) -> Result<(CallTarget, usize)> {
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
                    if matches!(operation, SqlBatchHostOperation::CastCharNative { .. }) {
                        matches!(arg, Expression::Column(_)) || matches!(arg, Expression::Constant(value) if value.literal_value().is_some())
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
    let (target, arity) = resolve_call(function).ok()?;
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
fn preflight<'a>(
    root: &'a Expression,
    row_schema: &[FieldType],
    limits: BatchSourceLimits,
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
    budget.array::<(&Expression, usize, NativeView)>(1)?;
    reserve(&mut pending, 1)?;
    pending.push((root, 1usize, NativeView::Typed));
    let mut scheduled = 1usize;
    while let Some((node, depth, view)) = pending.pop() {
        let (sql, collation) = parts(node)?;
        if view != NativeView::ValueToken {
            check_type(sql)?;
        } else if !matches!(node, Expression::Constant(_) | Expression::Column(_)) {
            return Err(binding("native value view requires a leaf source").into());
        }
        budget.field(sql, 3)?; // native source, projected declaration, C snapshot
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
                if view == NativeView::ValueToken {
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
                check_batch_call(function)?;
                budget.add(function.func_name.original().len())?;
                budget.add(function.func_name.lowercase().len())?;
                budget.array::<OrdinaryCallSite>(2)?; // initial and lowered source ordinals
                if matches!(resolve_call(function)?.0, CallTarget::Host(_)) {
                    budget.array::<(SqlBatchHostOperation, HostSignature)>(1)?;
                    budget.array::<NativeHostMetadata>(1)?;
                    if function.func_name.lowercase() == "cast_char" {
                        budget.add(sql.charset_name().len())?;
                        if matches!(
                            resolve_call(function)?.0,
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
                let child_depth = depth
                    .checked_add(1)
                    .filter(|depth| *depth <= limits.tree.max_depth)
                    .ok_or_else(|| resource("batch source depth policy exceeded"))?;
                budget.array::<(&Expression, usize, NativeView)>(arity)?;
                reserve(&mut pending, arity)?;
                for child in function.args.iter().rev() {
                    pending.push((child, child_depth, argument_view(function)));
                }
            }
            Expression::CorrelatedColumn(_) => unreachable!(),
        }
        reserve(&mut checked, 1)?;
        checked.push(CheckedNode { node, sql, view });
    }
    // Even a temporary representability conversion must wait for the complete
    // source/schema/payload byte gate, including literals later in the graph.
    for item in &checked {
        if let Expression::Constant(value) = item.node {
            if item.view == NativeView::ValueToken {
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

fn lower(
    suite: &EvaluatorSuite,
    calculated_slot: usize,
    row_schema: &[FieldType],
    new_collation: bool,
    unit: u64,
    limits: BatchSourceLimits,
) -> Result<Arc<BatchSource>> {
    let root = suite
        .program
        .calculated
        .get(calculated_slot)
        .ok_or_else(|| BatchFailure::admission("batch calculation slot is missing"))?;
    let checked = preflight(root, row_schema, limits)?;
    let mut host_entries = Vec::new();
    let mut host_metadata = Vec::new();
    let mut nodes = Vec::new();
    let mut bindings = Vec::new();
    let mut schema = Vec::new();
    let mut sites = Vec::new();
    let mut build = Vec::new();
    reserve(&mut nodes, checked.len())?;
    reserve(&mut build, checked.len())?;
    for (ordinal, item) in checked.into_iter().enumerate() {
        let field = if item.view != NativeView::Typed {
            projected(&native_bytes_type(), new_collation)?
        } else {
            projected(item.sql, new_collation)?
        };
        let kind = SourceKind::capture(item.node, item.sql, item.view)?;
        let construction = match &kind {
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
                                if matches!(operation, SqlBatchHostOperation::CastCharNative { .. })
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
                        let slot = host_entries.len();
                        reserve(&mut host_entries, 1)?;
                        reserve(&mut host_metadata, 1)?;
                        host_metadata.push(match operation {
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
    let needs_native_values = nodes.iter().any(|node| node.view == NativeView::ValueToken);
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
pub(crate) struct PreparedBatch {
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
        let source = lower(
            suite,
            calculated_slot,
            row_schema,
            new_collation,
            unit,
            source_limits,
        )?;
        let program = compile_batch(
            &source.expr,
            &source.schema,
            LocalCompileContext {
                limits: source.limits.tree,
            },
            &source.facts,
        )?;
        let window_size = program.max_batch_size();
        Ok(Self {
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
        let carrier_bytes = match &values {
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
        if node.view == NativeView::ValueToken {
            let store = self
                .native_values
                .ok_or_else(|| binding("native source has no value store"))?;
            let token = match &node.kind {
                SourceKind::NativeConstant { value } => store.borrow_mut().insert_clone(
                    value,
                    NativeValueRole::CharInput,
                    NativeValueOrigin { node: ordinal, row },
                )?,
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
                    store.borrow_mut().insert_clone(
                        &value,
                        NativeValueRole::CharInput,
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
        if node.view == NativeView::ValueToken {
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
                            TikvDuration::from_nanos(
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
                    TikvDuration::from_nanos(
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
    fn execute_chunk(
        &mut self,
        native_ctx: &dyn Columns,
        ctx: &mut EvalContext,
        input: &Chunk,
        output: &mut Chunk,
        #[cfg(test)] mut override_source: Option<&mut dyn NativeDatumSource>,
    ) -> Result<()> {
        let source = Arc::clone(&self.source);
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
        // TiKV deliberately bounds one driver invocation. A TiDB Chunk may be
        // wider than that boundary, so keep one canonical prepared route and
        // stream it through consecutive whole-selection windows. Every window
        // uses the same sealed program and preserves occurrence order, repeats,
        // NULLs and failure propagation without entering another evaluator.
        let mut start = 0;
        while start < self.selection.len() {
            let end = self
                .selection
                .len()
                .min(start.saturating_add(self.window_size));
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
            let result = self.program.eval_with_bindings(
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
            self.write_output(values, selection, output, self.source.output_index)?;
            start = end;
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
            let field = row_schema
                .get(index)
                .ok_or_else(|| binding("projection column outside row schema"))?;
            check_type(field)?;
        }
        let mut workers = Vec::new();
        reserve(&mut workers, count)?;
        let mut remaining_nodes = limits.tree.max_nodes;
        for slot in 0..count {
            let mut calculation_limits = limits;
            calculation_limits.tree.max_nodes = remaining_nodes;
            let worker = PreparedBatch::compile_calculation(
                suite,
                slot,
                row_schema,
                new_collation,
                unit,
                calculation_limits,
                execution,
                max_materialization_retained_bytes / count.max(1),
            )?;
            remaining_nodes = remaining_nodes
                .checked_sub(worker.source.facts.node_count())
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
        let vectorized = native_ctx.enable_vectorized_expression();
        for worker in &mut self.workers {
            if !vectorized {
                worker.window_size = 1;
            }
            worker.execute_chunk(
                native_ctx,
                ctx,
                input,
                output,
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
