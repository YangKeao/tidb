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

use super::adapter_failure::{ExpressionAdapterFailure, ScopeFailureKind};
use crate::{Columns, Datum, EvalError};
use tidb_datatype::FieldType;
fn invalid_report() -> EvalError {
    EvalError::ExpressionAdapterFailure(ExpressionAdapterFailure::from_scope(
        ScopeFailureKind::Contract,
        "invalid native EXTRACT report",
    ))
}
pub(crate) fn eval_extract_in(
    ctx: &dyn Columns,
    unit: &str,
    value: &Datum,
    source: Option<&FieldType>,
) -> Result<Datum, EvalError> {
    eval_extract_values_bounded(
        ctx,
        unit,
        value,
        source,
        super::identity_actual::default_budget(),
    )
}
pub(crate) fn eval_extract_composite_in(
    ctx: &dyn Columns,
    unit: &str,
    values: &[Datum],
) -> Result<Datum, EvalError> {
    eval_extract_composite_values_bounded(
        ctx,
        unit,
        values,
        super::identity_actual::default_budget(),
    )
}
fn actual_resource() -> EvalError {
    EvalError::ExpressionRuntimeFailure(super::ExpressionRuntimeFailure::from_local_eval(
        super::LocalError::ResourceLimit("native EXTRACT actual work allowance exceeded".into()),
        None,
    ))
}
fn actual_work(n: usize, m: usize, extra: usize, maximum: usize) -> Result<(), EvalError> {
    let work = n
        .checked_mul(m)
        .and_then(|n| n.checked_add(extra))
        .ok_or_else(actual_resource)?;
    if work > maximum {
        return Err(actual_resource());
    }
    Ok(())
}
fn actual_string_error(e: tidb_query_expr::NativeStringCoercionError) -> EvalError {
    match e {
        tidb_query_expr::NativeStringCoercionError::Unsupported(s) => EvalError::Unsupported(s),
        tidb_query_expr::NativeStringCoercionError::ResourceLimit => actual_resource(),
    }
}
fn actual_value(
    ctx: &dyn Columns,
    value: tidb_query_expr::NativeExtractValue,
) -> Result<Datum, EvalError> {
    use tidb_query_expr::NativeExtractValue as V;
    match value {
        V::Null => Ok(Datum::Null),
        V::Value(n) => Ok(Datum::Int(n)),
        V::InvalidUnit(message) => Err(EvalError::Conversion(
            tidb_error::terror::TerrorError::compatible(
                tidb_error::terror::TerrorCode::new(1105),
                message,
            ),
        )),
        V::InvalidTime(message) => Err(EvalError::Conversion(
            tidb_datatype::ERR_TRUNCATED_WRONG_VALUE.generate(message),
        )),
        V::Warning(message) => {
            ctx.append_warning(1292, message);
            Ok(Datum::Null)
        }
    }
}
/// Actual scalar EXTRACT over an already normalized original unit and value.
/// The original cast functions remain the sole owners of statement callbacks.
pub(crate) fn eval_extract_values_bounded(
    ctx: &dyn Columns,
    unit: &str,
    value: &Datum,
    source: Option<&FieldType>,
    maximum: usize,
) -> Result<Datum, EvalError> {
    use tidb_query_expr::{NativeExtractMixedStage as M, NativeExtractSelection as S};
    let selection = tidb_query_expr::native_extract_select_values(
        unit,
        source.map(|f| f.code().as_shared_type_name_code()),
        value.as_shared_json_input(),
        maximum,
    )
    .map_err(|_| actual_resource())?;
    let result = match selection {
        S::Datetime => {
            let work = tidb_query_expr::native_datetime_argument_cast_work(
                value.as_shared_json_input(),
                value.as_shared_numeric_input(),
                source.map(|f| f.code().as_shared_type_name_code()),
            )
            .map_err(actual_string_error)?;
            actual_work(work, 1, 0, maximum)?;
            match crate::cast::cast_arg_as_datetime(value, source, ctx)? {
                Datum::Null => return Ok(Datum::Null),
                Datum::Time(v) => tidb_query_expr::native_extract_datetime_values(
                    unit,
                    v.core_time().raw(),
                    maximum,
                ),
                _ => return Err(invalid_report()),
            }
        }
        S::Duration => {
            let work =
                tidb_query_expr::native_duration_argument_cast_work(value.as_shared_json_input())
                    .map_err(actual_string_error)?;
            actual_work(work, 1, 0, maximum)?;
            match crate::cast::cast_arg_as_duration(value, source, ctx)? {
                Datum::Null => return Ok(Datum::Null),
                Datum::Duration(v) => {
                    tidb_query_expr::native_extract_duration_values(unit, v.nanoseconds(), maximum)
                }
                _ => return Err(invalid_report()),
            }
        }
        S::Mixed => {
            let work = tidb_query_expr::native_byte_coercion_work(value.as_shared_json_input())
                .map_err(actual_string_error)?;
            actual_work(work, 4, 256, maximum)?;
            let cast = crate::cast::cast_arg_as_string(value, source, ctx)?;
            if matches!(cast, Datum::Null) {
                return Ok(Datum::Null);
            }
            // Inspect the actual cast result for its original UTF-8 error kind:
            // Bit became Bytes and Raw became String. Do not reject the source
            // with a different kind-specific string preflight before this cast.
            let held = cast
                .go_bytes()
                .len()
                .checked_mul(2)
                .and_then(|n| n.checked_add(256))
                .ok_or_else(actual_resource)?;
            let string_max = maximum.checked_sub(held).ok_or_else(actual_resource)?;
            let text = tidb_query_expr::native_coerce_string_bounded(
                cast.as_shared_json_input(),
                string_max,
            )
            .map_err(actual_string_error)?;
            drop(cast);
            let remaining = maximum
                .checked_sub(text.capacity())
                .ok_or_else(actual_resource)?;
            tidb_query_expr::native_extract_mixed_work(unit, &text, remaining)
                .map_err(|_| actual_resource())?;
            let first = ctx.date_modes().allow_invalid_dates;
            match tidb_query_expr::native_extract_mixed_duration_values(
                unit, &text, first, remaining,
            )
            .map_err(|_| actual_resource())?
            {
                M::Ready(value) => return actual_value(ctx, value),
                M::NeedDatetime(state) => {
                    state.preflight(remaining).map_err(|_| actual_resource())?;
                    let second = ctx.date_modes().allow_invalid_dates;
                    state.finish(second, remaining)
                }
            }
        }
    }
    .map_err(|_| actual_resource())?;
    actual_value(ctx, result)
}
/// Original broad one-argument composite policy over actual coerced text.
pub(crate) fn eval_extract_composite_values_bounded(
    ctx: &dyn Columns,
    unit: &str,
    values: &[Datum],
    maximum: usize,
) -> Result<Datum, EvalError> {
    let [value] = values else {
        return Err(EvalError::Unsupported("bad function arity"));
    };
    actual_work(0, 0, 0, maximum)?;
    if matches!(value, Datum::Null) {
        return actual_value(ctx, tidb_query_expr::NativeExtractValue::Null);
    }
    let text = tidb_query_expr::native_coerce_string_bounded(value.as_shared_json_input(), maximum)
        .map_err(actual_string_error)?;
    let remaining = maximum
        .checked_sub(text.capacity())
        .ok_or_else(actual_resource)?;
    let value = tidb_query_expr::native_extract_composite_values(unit, Some(&text), remaining)
        .map_err(|_| actual_resource())?;
    actual_value(ctx, value)
}
/// Original eager scalar wrapper's byte-preserving, lossy UTF-8 uppercase unit
/// normalization. Both source expressions must have run before this helper.
pub(crate) fn eval_extract_scalar_values_bounded(
    ctx: &dyn Columns,
    unit: &Datum,
    value: &Datum,
    source: Option<&FieldType>,
    maximum: usize,
) -> Result<Datum, EvalError> {
    let bytes = tidb_query_expr::native_coerce_bytes_bounded(unit.as_shared_json_input(), maximum)
        .map_err(actual_string_error)?;
    let Some(bytes) = bytes else {
        return actual_value(ctx, tidb_query_expr::NativeExtractValue::Null);
    };
    let available = maximum
        .checked_sub(bytes.capacity())
        .ok_or_else(actual_resource)?;
    actual_work(bytes.len(), 6, 128, available)?;
    let unit = String::from_utf8_lossy(&bytes).to_ascii_uppercase();
    drop(bytes);
    let remaining = maximum
        .checked_sub(unit.capacity())
        .ok_or_else(actual_resource)?;
    eval_extract_values_bounded(ctx, &unit, value, source, remaining)
}

pub(crate) fn eval_extract_scalar_values_in(
    ctx: &dyn Columns,
    unit: &Datum,
    value: &Datum,
    source: Option<&FieldType>,
) -> Result<Datum, EvalError> {
    eval_extract_scalar_values_bounded(
        ctx,
        unit,
        value,
        source,
        super::identity_actual::default_budget(),
    )
}
