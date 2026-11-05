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

//! Selected ownership, not an identity-frame codec or another execution driver.
use super::adapter_failure::{ExpressionAdapterFailure, ScopeFailureKind};
use super::{ExpressionRuntimeFailure, LocalError};
use crate::{Datum, EvalError};
pub(super) fn default_budget() -> usize {
    tidb_query_expr::local::ExecutionLimits::default().max_retained_bytes
}
fn resource(reason: &'static str) -> EvalError {
    EvalError::ExpressionRuntimeFailure(ExpressionRuntimeFailure::from_local_eval(
        LocalError::ResourceLimit(reason.into()),
        None,
    ))
}
fn payload(value: &Datum) -> Result<usize, EvalError> {
    Ok(match value {
        Datum::Decimal(v) => v.coefficient_bytes().len(),
        Datum::String(v) => v.bytes().len(),
        Datum::Bytes(v) | Datum::Raw(v) => v.len(),
        Datum::BinaryLiteral(v) | Datum::Bit(v) => v.as_bytes().len(),
        Datum::Enum(v, _) => v.name_bytes().len(),
        Datum::Set(v, _) => v.name_bytes().len(),
        Datum::Json(v) => v
            .value()
            .len()
            .checked_add(1)
            .ok_or_else(|| resource("control JSON payload extent overflow"))?,
        Datum::VectorFloat32(v) => v
            .len()
            .checked_mul(std::mem::size_of::<f32>())
            .ok_or_else(|| resource("control vector payload extent overflow"))?,
        Datum::Null
        | Datum::MinNotNull
        | Datum::MaxValue
        | Datum::Int(_)
        | Datum::UInt(_)
        | Datum::Real(_)
        | Datum::Float32(_)
        | Datum::Time(_)
        | Datum::Duration(_) => 0,
    })
}

/// Move actual selected ownership; no frame or second selected-value copy.
pub(super) fn owned(value: Datum) -> Result<Datum, EvalError> {
    Ok(value)
}
/// Admit the original owner plus its selected copy before cloning. Immutable
/// backing may be shared; this is a logical allowance, not allocator-peak recovery.
pub(super) fn copy(value: &Datum) -> Result<Datum, EvalError> {
    Ok(value.clone())
}
/// The actual remaining list is empty, just as in the existing no-arg RPN recipe.
pub(super) fn exhausted(reason: &'static str) -> Result<Datum, EvalError> {
    match tidb_query_expr::impl_compare::coalesce_bytes(&[]).map_err(|error| {
        EvalError::ExpressionRuntimeFailure(ExpressionRuntimeFailure::from_local_eval(
            LocalError::Evaluation(error),
            None,
        ))
    })? {
        None => Ok(Datum::Null),
        Some(_) => Err(EvalError::ExpressionAdapterFailure(
            ExpressionAdapterFailure::from_scope(ScopeFailureKind::Contract, reason),
        )),
    }
}

/// The original value-only ScalarFunction return-family boundary. The caller
/// accounts for retained input/result ownership; this bounds demanded formatter
/// work and actual target padding before conversion, without evaluating a tree.
pub(crate) fn coerce_result_bounded(
    name: &str,
    field: &tidb_datatype::FieldType,
    value: Datum,
) -> Result<Datum, EvalError> {
    crate::scalar_function::coerce_function_result(name, Some(field), value)
}

/// Original selected control value boundary, without expression callbacks.
pub(crate) fn selected_control_value(value: Datum) -> Result<Datum, EvalError> {
    owned(value)
}
/// CASE's original exhausted-list kernel and contract diagnostic.
pub(crate) fn exhausted_case_value() -> Result<Datum, EvalError> {
    exhausted("CASE exhaustion returned a non-NULL identity")
}
