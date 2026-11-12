// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Actual timestamp values and original statement callback ownership.
use crate::{Columns, Datum, Decimal, EvalError};
use tidb_datatype::{CoreTime, SessionTimeZone, Time, TimeType};
use tidb_query_expr::{
    NativeEpochValueError as E, NativeFromUnixTimeValue as F, NativeUnixTimestampValue as U,
};
fn error(e: E) -> EvalError {
    match e {
        E::Resource => {
            EvalError::ExpressionRuntimeFailure(super::ExpressionRuntimeFailure::from_local_eval(
                super::LocalError::ResourceLimit(
                    "native epoch actual work allowance exceeded".into(),
                ),
                None,
            ))
        }
        E::InvalidKind => super::native_time_result_contract_error(),
    }
}
fn string_error(e: tidb_query_expr::NativeStringCoercionError) -> EvalError {
    match e {
        tidb_query_expr::NativeStringCoercionError::Unsupported(e) => EvalError::Unsupported(e),
        _ => error(E::Resource),
    }
}
fn work(max: usize, n: usize) -> Result<(), EvalError> {
    if n > max {
        Err(error(E::Resource))
    } else {
        Ok(())
    }
}
fn unix_result(value: U, ctx: &dyn Columns, max: usize) -> Result<Datum, EvalError> {
    match value {
        U::Null => Ok(Datum::Null),
        U::Int(n) => Ok(Datum::Int(n)),
        U::Decimal(d) => Ok(Datum::Decimal(Decimal::from_shared_parse(d))),
        U::Warning(s) => {
            ctx.append_warning(1292, &s);
            Ok(Datum::Null)
        }
        U::NeedZone(state) => {
            work(max, 128)?;
            let zone = ctx.time_zone();
            let value = state.finish(&zone, max).map_err(error)?;
            match value {
                U::NeedZone(_) => Err(super::native_time_result_contract_error()),
                v => unix_result(v, ctx, max),
            }
        }
    }
}
pub(crate) fn unix_values_bounded(
    ctx: &dyn Columns,
    values: &[Datum],
    maximum: usize,
) -> Result<Datum, EvalError> {
    if values.is_empty() {
        work(maximum, 128)?;
        let Some((secs, nanos, _)) = ctx.now() else {
            return Err(EvalError::Unsupported("session clock"));
        };
        return unix_result(
            tidb_query_expr::native_unix_timestamp_now_values(secs, nanos, maximum)
                .map_err(error)?,
            ctx,
            maximum,
        );
    }
    let [value] = values else {
        return Err(EvalError::Unsupported("bad function arity"));
    };
    let text = tidb_query_expr::native_coerce_string_bounded(value.as_shared_json_input(), maximum)
        .map_err(string_error)?;
    if value.is_null() {
        return Ok(Datum::Null);
    }
    let remaining = maximum
        .checked_sub(text.capacity())
        .ok_or_else(|| error(E::Resource))?;
    tidb_query_expr::native_epoch_text_work(&text, remaining).map_err(error)?;
    let is_float = matches!(
        value,
        Datum::Int(_) | Datum::UInt(_) | Datum::Decimal(_) | Datum::Real(_) | Datum::Float32(_)
    );
    let zone = ctx.time_zone();
    let result =
        tidb_query_expr::native_unix_timestamp_parse_values(&text, is_float, &zone, remaining)
            .map_err(error)?;
    // Parsing diagnostics may borrow no source text; release its allocation
    // before delivering the warning or independently requesting the second zone.
    drop(text);
    unix_result(result, ctx, remaining)
}
/// Actual local value before the optional format's conversion. Its source
/// expression has already run; the caller owns its later authenticated value.
pub(crate) fn from_head_values_bounded(
    ctx: &dyn Columns,
    source: &Datum,
    maximum: usize,
) -> Result<Datum, EvalError> {
    work(maximum, 0)?;
    let result = match source {
        Datum::Null => return Ok(Datum::Null),
        value @ (Datum::Int(_)
        | Datum::UInt(_)
        | Datum::Decimal(_)
        | Datum::Real(_)
        | Datum::Float32(_)) => tidb_query_expr::native_from_unixtime_numeric_values(
            value.as_shared_json_input(),
            maximum,
        )
        .map_err(error)?,
        value => {
            let text = tidb_query_expr::native_coerce_string_bounded(
                value.as_shared_json_input(),
                maximum,
            )
            .map_err(string_error)?;
            let remaining = maximum
                .checked_sub(text.capacity())
                .ok_or_else(|| error(E::Resource))?;
            tidb_query_expr::native_from_unixtime_text_values(&text, remaining).map_err(error)?
        }
    };
    let state = match result {
        F::Null => return Ok(Datum::Null),
        F::Ready(s) => s,
        F::Truncate { value, message } => {
            work(maximum, message.capacity())?;
            ctx.handle_truncate(&message)?;
            value
        }
    };
    state.preflight(maximum).map_err(error)?;
    let zone = ctx.time_zone();
    let Some(local) = state.local(&zone, maximum).map_err(error)? else {
        return Ok(Datum::Null);
    };
    Ok(Datum::new_string(local))
}
pub(crate) fn from_values_bounded(
    ctx: &dyn Columns,
    values: &[Datum],
    maximum: usize,
) -> Result<Datum, EvalError> {
    if !(1..=2).contains(&values.len()) {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    let value = from_head_values_bounded(ctx, &values[0], maximum)?;
    if value.is_null() || values.len() == 1 {
        return Ok(value);
    }
    let held = match &value {
        Datum::String(s) => s.bytes().len(),
        _ => unreachable!(),
    };
    date_format_values_bounded(
        ctx,
        &value,
        &values[1],
        maximum
            .checked_sub(held)
            .ok_or_else(|| error(E::Resource))?,
    )
}
pub(crate) fn date_format_values_bounded(
    _ctx: &dyn Columns,
    date: &Datum,
    format: &Datum,
    maximum: usize,
) -> Result<Datum, EvalError> {
    let date_text =
        tidb_query_expr::native_coerce_string_bounded(date.as_shared_json_input(), maximum)
            .map_err(string_error)?;
    let remaining = maximum
        .checked_sub(date_text.capacity())
        .ok_or_else(|| error(E::Resource))?;
    let format_text =
        tidb_query_expr::native_coerce_string_bounded(format.as_shared_json_input(), remaining)
            .map_err(string_error)?;
    let remaining = remaining
        .checked_sub(format_text.capacity())
        .ok_or_else(|| error(E::Resource))?;
    let result = tidb_query_expr::native_date_format_text_values(
        (!date.is_null()).then_some(date_text.as_str()),
        (!format.is_null()).then_some(format_text.as_str()),
        remaining,
    )
    .map_err(error)?;
    Ok(result.map_or(Datum::Null, Datum::new_string))
}
fn shared_time(t: Time) -> tidb_query_datatype::codec::mysql::time::NativeTemporalValue {
    tidb_query_datatype::codec::mysql::time::NativeTemporalValue {
        raw: t.core_time().raw(),
        kind: match t.kind() {
            TimeType::Date => tidb_query_datatype::codec::mysql::TimeType::Date,
            TimeType::DateTime => tidb_query_datatype::codec::mysql::TimeType::DateTime,
            TimeType::Timestamp => tidb_query_datatype::codec::mysql::TimeType::Timestamp,
        },
        fsp: t.fsp(),
    }
}
pub(crate) fn unix_legacy_values_bounded(
    value: Option<Time>,
    zone: &SessionTimeZone,
    decimal: bool,
    maximum: usize,
) -> Result<Datum, EvalError> {
    let result = tidb_query_expr::native_unix_timestamp_legacy_values(
        value.map(shared_time),
        zone,
        decimal,
        maximum,
    )
    .map_err(error)?;
    match result {
        U::Null => Ok(Datum::Null),
        U::Int(v) => Ok(Datum::Int(v)),
        U::Decimal(v) => Ok(Datum::Decimal(Decimal::from_shared_parse(v))),
        _ => Err(super::native_time_result_contract_error()),
    }
}
pub(crate) fn from_legacy_values_bounded<T>(
    value: Option<Decimal>,
    zone: &SessionTimeZone,
    ctx: &dyn Columns,
    maximum: usize,
    pack: impl FnOnce(Option<Time>, &dyn Columns) -> T,
) -> Result<T, EvalError> {
    let value = tidb_query_expr::native_from_unixtime_legacy_values(
        value.as_ref().map(Decimal::as_shared_parse),
        zone,
        maximum,
    )
    .map_err(error)?;
    Ok(pack(
        value.map(|t| Time::from_raw_parts(CoreTime::from_raw(t.raw), TimeType::DateTime, t.fsp)),
        ctx,
    ))
}
pub(crate) fn date_format_legacy_values_bounded(
    value: Option<(CoreTime, Option<&str>)>,
    maximum: usize,
) -> Result<Option<Vec<u8>>, EvalError> {
    Ok(
        tidb_query_expr::native_date_format_core_values(value.map(|(t, s)| (t.raw(), s)), maximum)
            .map_err(error)?
            .map(String::into_bytes),
    )
}

pub(crate) fn unix_in(ctx: &dyn Columns, values: &[Datum]) -> Result<Datum, EvalError> {
    unix_values_bounded(ctx, values, super::identity_actual::default_budget())
}
pub(crate) fn from_in(ctx: &dyn Columns, values: &[Datum]) -> Result<Datum, EvalError> {
    from_values_bounded(ctx, values, super::identity_actual::default_budget())
}
pub(crate) fn date_format_in(
    ctx: &dyn Columns,
    date: &Datum,
    format: &Datum,
) -> Result<Datum, EvalError> {
    date_format_values_bounded(ctx, date, format, super::identity_actual::default_budget())
}
pub(crate) fn legacy_format_in(
    value: Option<(CoreTime, Option<&str>)>,
) -> Result<Option<Vec<u8>>, EvalError> {
    date_format_legacy_values_bounded(value, super::identity_actual::default_budget())
}

/// The original scalar temporal result policy, with parser/diagnostic
/// work checked before its original temporal context callbacks.
pub(crate) fn finish_temporal_values_bounded(
    ctx: &dyn Columns,
    result: Datum,
    field: &tidb_datatype::FieldType,
    maximum: usize,
) -> Result<Datum, EvalError> {
    if !result.is_null()
        && matches!(
            field.code(),
            tidb_datatype::FieldTypeCode::Datetime
                | tidb_datatype::FieldTypeCode::Date
                | tidb_datatype::FieldTypeCode::Duration
        )
    {
        let n = tidb_query_expr::native_string_coercion_work(result.as_shared_json_input())
            .map_err(string_error)?;
        let allowance = n
            .checked_mul(64)
            .and_then(|n| n.checked_add(4096))
            .ok_or_else(|| error(E::Resource))?;
        work(maximum, allowance)?;
    }
    crate::scalar_function::finish_native_temporal_result(Some(field), result, ctx)
}

/// The original ScalarFunction::Eval return-family boundary. It is distinct
/// from FROM_UNIXTIME's earlier temporal policy and runs once for both families.
pub(crate) fn finish_epoch_result_bounded(
    value: Datum,
    field: &tidb_datatype::FieldType,
    from: bool,
) -> Result<Datum, EvalError> {
    crate::scalar_function::coerce_function_result(
        if from {
            "from_unixtime"
        } else {
            "unix_timestamp"
        },
        Some(field),
        value,
    )
}
