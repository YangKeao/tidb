// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Actual calendar inputs, preserving typed-core and text entry contracts.
use crate::{Columns, Datum, EvalError};
use tidb_datatype::{CoreTime, FieldType, Time, TimeType};
use tidb_query_expr::{
    NativeCalendarError as E, NativeCalendarOperation as Op, NativeCalendarValue as V,
    NativeCalendarWeek,
};
fn resource() -> EvalError {
    EvalError::ExpressionRuntimeFailure(super::ExpressionRuntimeFailure::from_local_eval(
        super::LocalError::ResourceLimit("native calendar actual work allowance exceeded".into()),
        None,
    ))
}
fn work(maximum: usize, n: usize) -> Result<(), EvalError> {
    if maximum == usize::MAX || n > maximum {
        Err(resource())
    } else {
        Ok(())
    }
}
fn error(e: E) -> EvalError {
    match e {
        E::Resource => resource(),
        E::Contract => super::native_time_result_contract_error(),
    }
}
fn string_error(e: tidb_query_expr::NativeStringCoercionError) -> EvalError {
    match e {
        tidb_query_expr::NativeStringCoercionError::Unsupported(s) => EvalError::Unsupported(s),
        _ => resource(),
    }
}
fn string(v: &Datum, maximum: usize) -> Result<Option<String>, EvalError> {
    if v.is_null() {
        return Ok(None);
    }
    tidb_query_expr::native_coerce_string_bounded(v.as_shared_json_input(), maximum)
        .map(Some)
        .map_err(string_error)
}
fn datum(v: V) -> Datum {
    match v {
        V::Null => Datum::Null,
        V::Int(v) => Datum::Int(v),
        V::Text(v) => Datum::new_string(v),
    }
}
pub(crate) enum Head {
    Value(Datum),
    Week(NativeCalendarWeek),
}
/// No default getter here: direct WEEK's caller owns its original eager read.
pub(crate) fn head(
    op: Op,
    value: &Datum,
    ctx: &dyn Columns,
    maximum: usize,
) -> Result<Head, EvalError> {
    work(maximum, 0)?;
    if matches!(op, Op::Year | Op::Month | Op::Day | Op::Quarter) {
        let core = crate::time_fn::calendar::component_time_core(std::slice::from_ref(value))?
            .map(CoreTime::raw);
        return tidb_query_expr::native_calendar_core(op, core, maximum)
            .map(|v| Head::Value(v.map_or(Datum::Null, Datum::Int)))
            .map_err(error);
    }
    if op == Op::Date {
        let t = match value {
            Datum::Null => return Ok(Head::Value(Datum::Null)),
            Datum::Time(t) => t,
            _ => {
                return Err(EvalError::Unsupported(
                    "DATE argument reached the signature without its ETDatetime cast",
                ))
            }
        };
        let raw = t.core_time().raw();
        let modes = ctx.date_modes();
        if tidb_query_datatype::codec::mysql::Time::native_date_rejects_zero(
            raw,
            modes.no_zero_date,
            modes.no_zero_in_date,
        ) {
            let n = tidb_query_expr::native_string_coercion_work(value.as_shared_json_input())
                .map_err(string_error)?;
            work(maximum, n.checked_add(1024).ok_or_else(resource)?)?;
            ctx.handle_truncate(&format!("Incorrect datetime value: '{t}'"))?;
        }
        let v = tidb_query_expr::native_calendar_date(
            raw,
            modes.no_zero_date,
            modes.no_zero_in_date,
            maximum,
        )
        .map_err(error)?;
        return Ok(Head::Value(v.map_or(Datum::Null, |v| {
            Datum::Time(Time::from_raw_parts(
                CoreTime::from_raw(v),
                TimeType::Date,
                0,
            ))
        })));
    }
    let text = string(value, maximum)?;
    if matches!(op, Op::Week | Op::YearWeek) {
        return tidb_query_expr::native_calendar_week(text.as_deref(), op == Op::YearWeek, maximum)
            .map(|v| v.map_or(Head::Value(Datum::Null), Head::Week))
            .map_err(error);
    }
    tidb_query_expr::native_calendar_text(op, text.as_deref(), maximum)
        .map(|v| Head::Value(datum(v)))
        .map_err(error)
}
/// Numeric storage, not unused display precision, bounds late mode conversion.
pub(crate) fn mode(value: &Datum, maximum: usize) -> Result<Option<i64>, EvalError> {
    let n = match value {
        Datum::String(v) => v.bytes().len(),
        Datum::Bytes(v) | Datum::Raw(v) => v.len(),
        Datum::Decimal(v) => v.coefficient_bytes().len(),
        Datum::Json(v) => v.value().len(),
        Datum::BinaryLiteral(v) | Datum::Bit(v) => v.as_bytes().len(),
        _ => 0,
    };
    work(
        maximum,
        n.checked_mul(16)
            .and_then(|n| n.checked_add(1024))
            .ok_or_else(resource)?,
    )?;
    crate::time_fn::int_arg(value)
}
pub(crate) fn finish_week(
    v: NativeCalendarWeek,
    mode: Option<i64>,
    maximum: usize,
) -> Result<Datum, EvalError> {
    v.finish(mode, maximum).map(Datum::Int).map_err(error)
}
pub(crate) fn eval_bounded(
    op: Op,
    values: &[Datum],
    default: i64,
    ctx: &dyn Columns,
    maximum: usize,
) -> Result<Datum, EvalError> {
    if !op.accepts_arity(values.len()) {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    match head(op, &values[0], ctx, maximum)? {
        Head::Value(v) => Ok(v),
        Head::Week(v) => finish_week(
            v,
            match values.get(1) {
                Some(v) => mode(v, maximum)?,
                None => Some(default),
            },
            maximum,
        ),
    }
}
pub(crate) fn eval(
    op: Op,
    values: &[Datum],
    default: i64,
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    eval_bounded(
        op,
        values,
        default,
        ctx,
        super::control_value::default_budget(),
    )
}
pub(crate) fn core(op: Op, value: Option<CoreTime>) -> Result<Option<i64>, EvalError> {
    tidb_query_expr::native_calendar_core(
        op,
        value.map(CoreTime::raw),
        super::control_value::default_budget(),
    )
    .map_err(error)
}
pub(crate) fn nanos(op: Op, value: Option<i64>) -> Result<Option<i64>, EvalError> {
    tidb_query_expr::native_calendar_nanos(op, value, super::control_value::default_budget())
        .map_err(error)
}
/// Original static source declaration matters, particularly YEAR source values.
pub(crate) fn cast_source(
    op: Op,
    value: &Datum,
    source: &FieldType,
    ctx: &dyn Columns,
    maximum: usize,
) -> Result<Datum, EvalError> {
    if !op.datetime_argument() {
        return Ok(value.clone());
    }
    let n = tidb_query_expr::native_datetime_argument_cast_work(
        value.as_shared_json_input(),
        value.as_shared_numeric_input(),
        Some(source.code().as_shared_type_name_code()),
    )
    .map_err(string_error)?;
    work(maximum, n)?;
    crate::cast::cast_arg_as_datetime(value, Some(source), ctx)
}
pub(crate) fn finish_source(
    op: Op,
    value: Datum,
    field: &FieldType,
    ctx: &dyn Columns,
    maximum: usize,
) -> Result<Datum, EvalError> {
    let value = if op == Op::LastDay {
        super::finish_temporal_values_bounded(ctx, value, field, maximum)?
    } else {
        value
    };
    super::coerce_result_bounded(op.name(), field, value, maximum)
}
