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
fn check(value: &Datum, maximum: usize, copies: usize) -> Result<(), EvalError> {
    if maximum == usize::MAX {
        return Err(resource("control ownership requires a finite allowance"));
    }
    let bytes = payload(value)?
        .checked_mul(copies)
        .ok_or_else(|| resource("control ownership allowance overflow"))?;
    if bytes > maximum {
        return Err(resource("control selected ownership exceeds allowance"));
    }
    Ok(())
}
/// Move actual selected ownership; no frame or second selected-value copy.
pub(super) fn owned(value: Datum, maximum: usize) -> Result<Datum, EvalError> {
    check(&value, maximum, 1)?;
    Ok(value)
}
/// Admit the original owner plus its selected copy before cloning. Immutable
/// backing may be shared; this is a logical allowance, not allocator-peak recovery.
pub(super) fn copy(value: &Datum, maximum: usize) -> Result<Datum, EvalError> {
    check(value, maximum, 2)?;
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
    maximum: usize,
) -> Result<Datum, EvalError> {
    if maximum == usize::MAX {
        return Err(resource("return coercion requires a finite allowance"));
    }
    let bit_integer = field.code() == tidb_datatype::FieldTypeCode::Bit
        && matches!(value, Datum::Int(_) | Datum::UInt(_));
    if !value.is_null() && (!crate::scalar_function::same_eval_family(&value, field) || bit_integer)
    {
        let input = match tidb_query_expr::native_string_coercion_work(value.as_shared_json_input())
        {
            Ok(n) => n,
            Err(tidb_query_expr::NativeStringCoercionError::ResourceLimit) => {
                return Err(resource("return formatter extent overflow"))
            }
            // ConvertTo owns unsupported/UTF-8 error disposition, including its
            // existing fallback. A preflight must not replace it with an error.
            Err(tidb_query_expr::NativeStringCoercionError::Unsupported(_)) => {
                payload(&value)?.max(128)
            }
        };
        use tidb_query_datatype::codec::native_eval_type::{
            native_decimal_target_shape, NativeDecimalTargetShape,
        };
        let padding = if bit_integer {
            255
        } else {
            match field.code() {
                tidb_datatype::FieldTypeCode::NewDecimal => match native_decimal_target_shape(
                    field.flen(),
                    field.decimal(),
                    tidb_datatype::UNSPECIFIED_LENGTH,
                ) {
                    // Integer precision can only clamp an already larger input.
                    // Fraction scale alone can add new coefficient padding.
                    NativeDecimalTargetShape::Bounded { scale, .. } => scale as usize,
                    _ => 0,
                },
                tidb_datatype::FieldTypeCode::String if field.is_binary_string() => {
                    usize::try_from(field.flen().max(0))
                        .map_err(|_| resource("binary return padding extent overflow"))?
                }
                // VARCHAR/ordinary CHAR merely truncate; a huge declared width
                // does not request that many bytes. Time/numeric fields stay fixed.
                _ => 0,
            }
        };
        let allowance = input
            .checked_add(padding)
            .and_then(|n| n.checked_mul(64))
            .and_then(|n| n.checked_add(4096))
            .ok_or_else(|| resource("return coercion work extent overflow"))?;
        if allowance > maximum {
            return Err(resource("return coercion work allowance exceeded"));
        }
    }
    crate::scalar_function::coerce_function_result(name, Some(field), value)
}
