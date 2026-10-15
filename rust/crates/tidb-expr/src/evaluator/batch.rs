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

//! Private, mandatory projection consumer for the checked batch entry.
//! Public EvaluatorSuite::run still uses the native consumer. No filter, grouping,
//! row fallback, D4 diagnostic conversion or control/result-lineage composition.
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
    compile_batch, BatchFacts, CallMetadata, CompileLimits, ExecutionLimits, FunctionRef, InputRow,
    LiteralKind, LocalBatchProgram, LocalCompileContext, LocalError, LocalExpr, LocalResult,
    LocalRuntimeServices, OrdinaryCallSite, OrdinarySourceId,
};
use tidb_query_expr::types::BATCH_MAX_SIZE;

use crate::expr_collation::CollationInfo;
use crate::pushdown_catalog::CATALOG;
use crate::scalar_function::ScalarFunction;

use super::{
    BatchConsumer, BatchInvocation, BatchRefusal, BatchScratchSnapshot, Chunk, Columns, EvalError,
    EvaluatorError, EvaluatorProgram, EvaluatorSuite, Expression,
};

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
        let local = match self.kind {
            FailureKind::Native(error) => return error,
            FailureKind::Local(error) => error,
            FailureKind::Admission(message) => LocalError::InvalidSpec(message.into()),
            FailureKind::Bridge(error) => LocalError::BindingContract(error.to_string()),
        };
        EvalError::ExpressionRuntimeFailure(crate::ExpressionRuntimeFailure::from_local_eval(
            local,
            Some(crate::ExpressionRuntimeFailurePhase::Invoke),
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

#[derive(Debug)]
enum SourceKind {
    Constant { value: ScalarValue },
    Column { index: usize },
    Call { function: FunctionRef, arity: usize },
}
impl SourceKind {
    fn capture(node: &Expression, sql: &FieldType) -> Result<Self> {
        Ok(match node {
            Expression::Constant(value) => Self::Constant {
                value: strict_literal(
                    value
                        .literal_value()
                        .ok_or_else(|| BatchFailure::admission("nonliteral batch source"))?,
                    sql,
                )?,
            },
            Expression::Column(column) => Self::Column {
                index: column.index as usize,
            },
            Expression::ScalarFunction(function) => {
                let (function, arity) = batch_fact(function)?;
                Self::Call { function, arity }
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
    let expected_arity = batch_fact(function)?.1;
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
    if tidb_query_expr::local::batch_signature(FunctionRef::TiPb(signature)).is_none() {
        return Err(BatchFailure::admission(
            "catalog signature is outside the eager batch closure",
        ));
    }
    Ok((FunctionRef::TiPb(signature), function.args.len()))
}

/// Resolves one SQL call through the same exact catalog fact and TiKV
/// capability registry used by preparation. The native eligibility pass uses
/// this view so adding a signature cannot leave its mandatory consumer behind.
pub(crate) fn batch_call_signature(
    function: &ScalarFunction,
) -> Option<(&'static [TikvEvalType], TikvEvalType)> {
    let (function, arity) = batch_fact(function).ok()?;
    let (arguments, result) = tidb_query_expr::local::batch_signature(function)?;
    (arguments.len() == arity).then_some((arguments, result))
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
    if !matches!(root, Expression::ScalarFunction(_)) {
        return Err(BatchFailure::admission(
            "native batch entry does not admit a leaf root",
        ));
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
    budget.array::<(&Expression, usize)>(1)?;
    reserve(&mut pending, 1)?;
    pending.push((root, 1usize));
    let mut scheduled = 1usize;
    while let Some((node, depth)) = pending.pop() {
        let (sql, collation) = parts(node)?;
        check_type(sql)?;
        budget.field(sql, 3)?; // native source, projected declaration, C snapshot
        budget.array::<CheckedNode<'_>>(1)?;
        budget.array::<NodeRecord>(1)?;
        budget.array::<BuildNode>(1)?;
        budget.array::<LocalExpr>(1)?;
        let (charset, collate) = collation.charset_and_collation();
        budget.add(charset.len())?;
        budget.add(collate.len())?;
        match node {
            Expression::Constant(value) => {
                strict_literal(
                    value.literal_value().ok_or_else(|| {
                        BatchFailure::admission("parameter/deferred batch source")
                    })?,
                    sql,
                )?;
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
                budget.array::<OrdinaryCallSite>(1)?;
                let arity = function.args.len();
                scheduled = scheduled
                    .checked_add(arity)
                    .filter(|nodes| *nodes <= limits.tree.max_nodes)
                    .ok_or_else(|| resource("batch source node policy exceeded"))?;
                let child_depth = depth
                    .checked_add(1)
                    .filter(|depth| *depth <= limits.tree.max_depth)
                    .ok_or_else(|| resource("batch source depth policy exceeded"))?;
                budget.array::<(&Expression, usize)>(arity)?;
                reserve(&mut pending, arity)?;
                for child in function.args.iter().rev() {
                    pending.push((child, child_depth));
                }
            }
            Expression::CorrelatedColumn(_) => unreachable!(),
        }
        reserve(&mut checked, 1)?;
        checked.push(CheckedNode { node, sql });
    }
    for item in &checked {
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
    Ok(&program.calculated[0])
}
enum BuildKind {
    Literal(ScalarValue),
    Input(usize),
    Call { function: FunctionRef, arity: usize },
}
struct BuildNode {
    field: tipb::FieldType,
    kind: BuildKind,
}

fn lower(
    suite: &EvaluatorSuite,
    row_schema: &[FieldType],
    new_collation: bool,
    unit: u64,
    limits: BatchSourceLimits,
) -> Result<Arc<BatchSource>> {
    let root = suite_shape(suite)?;
    let checked = preflight(root, row_schema, limits)?;
    let mut nodes = Vec::new();
    let mut bindings = Vec::new();
    let mut schema = Vec::new();
    let mut sites = Vec::new();
    let mut build = Vec::new();
    reserve(&mut nodes, checked.len())?;
    reserve(&mut build, checked.len())?;
    for (ordinal, item) in checked.into_iter().enumerate() {
        let field = projected(item.sql, new_collation)?;
        let kind = SourceKind::capture(item.node, item.sql)?;
        let construction = match &kind {
            SourceKind::Constant { value, .. } => BuildKind::Literal(value.clone()),
            SourceKind::Column { .. } => {
                let slot = bindings.len();
                reserve(&mut bindings, 1)?;
                reserve(&mut schema, 1)?;
                bindings.push(ordinal);
                schema.push(field.clone());
                BuildKind::Input(slot)
            }
            SourceKind::Call { function, arity } => {
                reserve(&mut sites, 1)?;
                let node =
                    u64::try_from(ordinal).map_err(|_| resource("batch source ID overflow"))?;
                sites.push(OrdinaryCallSite::sql_native_batch(
                    ordinal,
                    OrdinarySourceId::new(unit, node),
                ));
                BuildKind::Call {
                    function: *function,
                    arity: *arity,
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
        });
    }
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
            BuildKind::Call { function, arity } => {
                let mut args = Vec::new();
                reserve(&mut args, arity)?;
                for _ in 0..arity {
                    args.push(
                        stack
                            .pop()
                            .ok_or_else(|| BatchFailure::admission("missing batch call child"))?,
                    );
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
    let facts = BatchFacts::sql_native_batch(&expr, &schema, sites, limits.tree)?;
    let mut detached = Vec::new();
    reserve(&mut detached, row_schema.len())?;
    detached.extend(row_schema.iter().map(snapshot_field_type));
    Ok(Arc::new(BatchSource {
        owner: Arc::clone(&suite.program),
        root_identity: root as *const Expression as usize,
        calculated_slot: 0,
        output_index: 0,
        unit,
        nodes: nodes.into_boxed_slice(),
        bindings: bindings.into_boxed_slice(),
        row_schema: detached.into_boxed_slice(),
        expr,
        schema: schema.into_boxed_slice(),
        facts,
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
    pub(crate) fn compile(
        suite: &EvaluatorSuite,
        row_schema: &[FieldType],
        new_collation: bool,
        unit: u64,
        source_limits: BatchSourceLimits,
        execution: ExecutionLimits,
        max_materialization_retained_bytes: usize,
    ) -> Result<Self> {
        let source = lower(suite, row_schema, new_collation, unit, source_limits)?;
        let program = compile_batch(
            &source.expr,
            &source.schema,
            LocalCompileContext {
                limits: source.limits.tree,
            },
            &source.facts,
        )?;
        Ok(Self {
            source,
            program,
            limits: execution,
            max_materialization_retained_bytes,
            selection: Vec::new(),
            window_size: BATCH_MAX_SIZE,
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
        let root = suite_shape(suite)?;
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
        if output.num_cols() != 1
            || output.column(0).type_size() != get_fixed_len(&self.source.nodes[0].sql)
            || output.sel().is_some()
            || output.is_incomplete_chunk()
        {
            return Err(LocalError::InvalidBatch(
                "batch destination layout is not one supported column".into(),
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
        let expected = match self.source.nodes[0].sql.eval_type() {
            NativeEvalType::Int => TikvEvalType::Int,
            NativeEvalType::Real => TikvEvalType::Real,
            NativeEvalType::Decimal => TikvEvalType::Decimal,
            NativeEvalType::String => TikvEvalType::Bytes,
            NativeEvalType::Datetime | NativeEvalType::Timestamp => TikvEvalType::DateTime,
            NativeEvalType::Duration => TikvEvalType::Duration,
            NativeEvalType::Json => TikvEvalType::Json,
            NativeEvalType::VectorFloat32 => TikvEvalType::VectorFloat32,
        };
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
        let SourceKind::Column { index, .. } = &node.kind else {
            return Err(binding("batch slot has no native column"));
        };
        // Physical already: get_row would apply the actual Chunk Sel twice.
        Ok(self
            .input
            .physical_row(row.input_row)
            .get_datum(*index, &node.sql))
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
        let SourceKind::Column { index, .. } = &node.kind else {
            return Err(binding("batch slot has no native column"));
        };
        let column = self.input.column(*index);
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
}
impl LocalRuntimeServices for CheckedInputs<'_> {
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

struct Consumer<'a> {
    worker: &'a mut PreparedBatch,
    ctx: &'a mut EvalContext,
    row_schema: &'a [FieldType],
    #[cfg(test)]
    override_source: Option<&'a mut dyn NativeDatumSource>,
}
impl BatchConsumer for Consumer<'_> {
    type Error = BatchFailure;
    fn before_run(&mut self, suite: &EvaluatorSuite, input: &Chunk, output: &Chunk) -> Result<()> {
        self.worker
            .validate_invocation(suite, input, output, self.row_schema)
    }
    fn consume(
        &mut self,
        invocation: BatchInvocation<'_>,
        _native_ctx: &dyn Columns,
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
        // TiKV deliberately bounds one driver invocation. A TiDB Chunk may be
        // wider than that boundary, so keep one canonical prepared route and
        // stream it through consecutive whole-selection windows. This is not a
        // Every window uses the same sealed batch program and typed carrier.
        // and preserves occurrence order, duplicates, NULLs and first error.
        let mut start = 0;
        while start < self.worker.selection.len() {
            let end = self
                .worker
                .selection
                .len()
                .min(start.saturating_add(self.worker.window_size));
            let selection = &self.worker.selection[start..end];
            let mut native = NativeChunk {
                source: &source,
                input: invocation.input,
                selection,
            };
            #[cfg(test)]
            let provider: &mut dyn NativeDatumSource = match self.override_source.as_mut() {
                Some(source) => &mut **source,
                None => &mut native,
            };
            #[cfg(not(test))]
            let provider: &mut dyn NativeDatumSource = &mut native;
            let mut checked = CheckedInputs { source: provider };
            let values = self
                .worker
                .program
                .eval_with_bindings(
                    self.worker.limits,
                    self.ctx,
                    invocation.input.physical_rows(),
                    selection,
                    &mut checked,
                )
                .map_err(BatchFailure::from)?;
            self.worker.windows = self.worker.windows.saturating_add(1);
            let vectors = source
                .bindings
                .len()
                .saturating_add(source.facts.call_sites().len());
            self.worker.vector_allocations = self
                .worker
                .vector_allocations
                .saturating_add(u64::try_from(vectors).unwrap_or(u64::MAX));
            self.worker
                .write_output(values, selection, output, invocation.output_index)?;
            start = end;
        }
        Ok(())
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
