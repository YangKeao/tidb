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
use super::{ExpressionRuntimeFailure, LocalError};
use crate::{Columns, Datum, ErrorLevel, EvalError};
use tidb_datatype::FieldType;
use tidb_query_expr::NativeDateArithmeticRequest as Request;
fn invalid_report() -> EvalError {
    EvalError::ExpressionAdapterFailure(ExpressionAdapterFailure::from_scope(
        ScopeFailureKind::Contract,
        "invalid native date arithmetic report",
    ))
}

fn unsupported(reason: &str) -> EvalError {
    let reason = match reason {
        "composite INTERVAL amount" => "composite INTERVAL amount",
        "INTERVAL unit" => "INTERVAL unit",
        "range sentinel INTERVAL amount" => "range sentinel INTERVAL amount",
        "DATE_ADD duration operand" => "DATE_ADD duration operand",
        _ => return invalid_report(),
    };
    EvalError::Unsupported(reason)
}

pub(crate) fn eval_date_add_in(
    ctx: &dyn Columns,
    unit: &str,
    date: &Datum,
    amount: &Datum,
    sign: i64,
    result_fsp: Option<u32>,
) -> Result<Datum, EvalError> {
    date_arithmetic_values_bounded(
        ctx,
        unit,
        date,
        amount,
        tidb_query_expr::NativeDateArithmeticValueConfig::calendar(sign, result_fsp),
        super::identity_actual::default_budget(),
    )
}
pub(crate) fn eval_date_add_default_in(
    _authority: &dyn Columns,
    unit: &str,
    date: &Datum,
    amount: &Datum,
    sign: i64,
) -> Result<Datum, EvalError> {
    // AST DATE_ADD's original whole-body default policy is deliberate.
    date_arithmetic_values_bounded(
        &crate::NoColumns,
        unit,
        date,
        amount,
        tidb_query_expr::NativeDateArithmeticValueConfig::calendar(sign, None),
        super::identity_actual::default_budget(),
    )
}
fn value_error(error: tidb_query_expr::NativeDateArithmeticValueError) -> EvalError {
    match error {
        tidb_query_expr::NativeDateArithmeticValueError::InvalidResponse => invalid_report(),
        tidb_query_expr::NativeDateArithmeticValueError::ResourceLimit => {
            EvalError::ExpressionRuntimeFailure(ExpressionRuntimeFailure::from_local_eval(
                LocalError::ResourceLimit(
                    "native date arithmetic actual work allowance exceeded".into(),
                ),
                None,
            ))
        }
    }
}
fn value_resource() -> EvalError {
    value_error(tidb_query_expr::NativeDateArithmeticValueError::ResourceLimit)
}
fn value_work(
    state: &tidb_query_expr::NativeDateArithmeticValues<'_>,
    n: usize,
    m: usize,
    extra: usize,
) -> Result<(), EvalError> {
    let work = n
        .checked_mul(m)
        .and_then(|n| n.checked_add(extra))
        .ok_or_else(value_resource)?;
    if work > state.remaining_bytes().map_err(value_error)? {
        return Err(value_resource());
    }
    Ok(())
}
fn value_integer_work(
    state: &tidb_query_expr::NativeDateArithmeticValues<'_>,
    value: &Datum,
) -> Result<(), EvalError> {
    use tidb_query_datatype::codec::native_numeric::NativeNumericInput as I;
    match value.as_shared_numeric_input() {
        I::Json { value, .. } => value_work(state, value.len(), 128, 1024),
        I::Time(_) | I::Duration(_) => value_work(state, 0, 0, 1024),
        _ => Ok(()),
    }
}
/// Drive only demanded datatype conversions and statement callbacks over the
/// shared actual continuation, without a report program or expression callback.
pub(crate) fn date_arithmetic_values_bounded(
    semantic: &dyn Columns,
    unit: &str,
    date: &Datum,
    amount: &Datum,
    config: tidb_query_expr::NativeDateArithmeticValueConfig,
    maximum: usize,
) -> Result<Datum, EvalError> {
    use tidb_query_expr::{
        NativeDateArithmeticOverflowMode as Mode, NativeDateArithmeticValue as V,
        NativeDateArithmeticValueAction as A, NativeDateArithmeticValues as State,
    };
    let values = [date, amount];
    let mut state = State::new(
        [date.as_shared_json_input(), amount.as_shared_json_input()],
        unit,
        config,
        maximum,
    )
    .map_err(value_error)?;
    let mut step = state.begin().map_err(value_error)?;
    loop {
        if let Some(warning) = step.warning {
            semantic.append_warning(warning.code, &warning.message)
        }
        step = match step.action {
            A::Ready(V::Null) => return Ok(Datum::Null),
            A::Ready(V::Text(text)) => return Ok(Datum::new_string(text)),
            A::Ready(V::Duration { nanos, fsp }) => {
                return Ok(Datum::new_duration(
                    tidb_datatype::MySqlDuration::from_raw_parts(nanos, fsp),
                ))
            }
            A::Ready(V::Unsupported(reason)) => return Err(unsupported(reason)),
            A::Ready(V::Error(message)) => {
                return Err(EvalError::Conversion(
                    tidb_datatype::ERR_DATETIME_FUNCTION_OVERFLOW.generate(message),
                ))
            }
            A::Request {
                kind: Request::CoerceString,
                index,
            } => {
                let value = values.get(index).ok_or_else(invalid_report)?;
                let text = tidb_query_expr::native_coerce_string_bounded(
                    value.as_shared_json_input(),
                    state.remaining_bytes().map_err(value_error)?,
                )
                .map_err(|e| match e {
                    tidb_query_expr::NativeStringCoercionError::Unsupported(reason) => {
                        EvalError::Unsupported(reason)
                    }
                    tidb_query_expr::NativeStringCoercionError::ResourceLimit => value_resource(),
                })?;
                state.resume_string(Some(text)).map_err(value_error)?
            }
            A::Request {
                kind: Request::ToI64,
                index,
            } => {
                let value = values.get(index).ok_or_else(invalid_report)?;
                value_integer_work(&state, value)?;
                let number = value
                    .to_i64()
                    .map_err(|_| EvalError::Unsupported("INTERVAL amount conversion"))?
                    .value;
                state.resume_integer(number).map_err(value_error)?
            }
            A::Request {
                kind: Request::ParseDecimal,
                index: 1,
            } => {
                let text = state.decimal_text().map_err(value_error)?;
                value_work(&state, text.len(), 32, 4096)?;
                // Same nine-word native parser called by Decimal::parse_mysql.
                let (decimal, _) =
                    tidb_query_datatype::codec::mysql::native_decimal_parse_mysql(text, 9);
                state
                    .resume_decimal(decimal.as_ref())
                    .map_err(value_error)?
            }
            A::Request { .. } => return Err(invalid_report()),
            A::Overflow => state
                .resume_overflow(match semantic.truncate_level() {
                    ErrorLevel::Ignore => Mode::Ignore,
                    ErrorLevel::Warn => Mode::Warn,
                    ErrorLevel::Error => Mode::Error,
                })
                .map_err(value_error)?,
        };
    }
}

/// Preserve metadata inference before either source is evaluated. Only temporal
/// declarations enter the original unit formatter; other declarations return
/// None without formatting an unused unit.
pub(crate) fn date_add_result_fsp_checked(
    unit: &str,
    date: Option<&FieldType>,
    amount: Option<&FieldType>,
) -> Result<Option<u32>, EvalError> {
    if date.is_some_and(|t| {
        matches!(
            t.eval_type(),
            tidb_datatype::EvalType::Datetime
                | tidb_datatype::EvalType::Timestamp
                | tidb_datatype::EvalType::Duration
        )
    }) {
        let work = unit.len().checked_mul(2).ok_or_else(value_resource)?;
        if work > super::identity_actual::default_budget() {
            return Err(value_resource());
        }
    }
    Ok(crate::time_fn::calendar::date_add_result_fsp(
        unit, date, amount,
    ))
}
fn datetime_cast_work(value: &Datum, field: Option<&FieldType>) -> Result<usize, EvalError> {
    tidb_query_expr::native_datetime_argument_cast_work(
        value.as_shared_json_input(),
        value.as_shared_numeric_input(),
        field.map(|f| f.code().as_shared_type_name_code()),
    )
    .map_err(|e| match e {
        tidb_query_expr::NativeStringCoercionError::Unsupported(reason) => {
            EvalError::Unsupported(reason)
        }
        tidb_query_expr::NativeStringCoercionError::ResourceLimit => value_resource(),
    })
}
/// The original scalar signature's pre/post temporal policy, shared once by
/// regular row calls and the canonical source host after both eager operands.
pub(crate) fn date_add_scalar_values_bounded(
    ctx: &dyn Columns,
    unit: &str,
    values: &mut [Datum; 2],
    fields: [Option<&FieldType>; 2],
    result: Option<&FieldType>,
    sign: i64,
    result_fsp: Option<u32>,
    maximum: usize,
) -> Result<Datum, EvalError> {
    if result.is_some_and(|f| f.code() == tidb_datatype::FieldTypeCode::Duration) {
        return date_arithmetic_values_bounded(
            ctx,
            unit,
            &values[0],
            &values[1],
            tidb_query_expr::NativeDateArithmeticValueConfig::duration(
                sign,
                i64::from(result_fsp.unwrap_or(0)),
                fields[1].map(FieldType::decimal),
            ),
            maximum,
        );
    }
    if result.is_some_and(|f| f.code() == tidb_datatype::FieldTypeCode::Datetime)
        && fields[0].is_some_and(|f| f.code() == tidb_datatype::FieldTypeCode::Duration)
    {
        let work = datetime_cast_work(&values[0], fields[0])?;
        if work > maximum {
            return Err(value_resource());
        }
        values[0] = crate::cast::cast_arg_as_datetime(&values[0], fields[0], ctx)?;
    }
    let value = date_arithmetic_values_bounded(
        ctx,
        unit,
        &values[0],
        &values[1],
        tidb_query_expr::NativeDateArithmeticValueConfig::calendar(sign, result_fsp),
        maximum,
    )?;
    let (kind, fsp) = match result.map(FieldType::code) {
        Some(tidb_datatype::FieldTypeCode::Date) => (tidb_datatype::TimeType::Date, Some(0)),
        Some(tidb_datatype::FieldTypeCode::Datetime) => (
            tidb_datatype::TimeType::DateTime,
            result.map(FieldType::decimal),
        ),
        _ => return Ok(value),
    };
    let work = datetime_cast_work(&value, None)?;
    let total = value
        .go_bytes()
        .len()
        .checked_mul(2)
        .and_then(|n| n.checked_add(work))
        .ok_or_else(value_resource)?;
    if total > maximum {
        return Err(value_resource());
    }
    crate::cast::parse_computed_time(&value, ctx, kind, fsp)
}
