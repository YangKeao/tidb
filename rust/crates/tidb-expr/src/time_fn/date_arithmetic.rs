// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

//! Go `baseDateArithmetical.vecGetInterval*` / `vecGetDate*` and ADD/SUB kernels.

use crate::{Columns, EvalError};
use tidb_datatype::{Datum, EvalType, FieldType, TimeType};

/// Go `intervalReformatString`, `intervalDecimalToString`, and numeric formatting.
pub(crate) fn interval(unit: &str, value: Datum, decimal: i64, ctx: &dyn Columns) -> Result<Datum, EvalError> {
    let text = match value {
        Datum::Null => return Ok(Datum::Null),
        Datum::Int(value) => value.to_string(),
        Datum::UInt(value) => value.to_string(),
        Datum::Real(value) => tidb_goruntime::go_strconv::format_float(
            value,
            b'f',
            i32::try_from(decimal).unwrap_or(-1),
            64,
        ),
        Datum::Decimal(value) if crate::time_fn::calendar::is_composite_unit(unit) => {
            crate::time_fn::calendar::format_decimal_composite_interval(unit, &value)
        }
        Datum::Decimal(value) if unit.eq_ignore_ascii_case("SECOND") => value.to_string(),
        Datum::Decimal(value) => {
            let rounded = value.round_to_i64();
            if rounded.is_none() {
                crate::expression_errors::handle_truncate(
                    ctx,
                    &format!("Truncated incorrect DECIMAL value: '{value}'"),
                )?;
            }
            rounded
                .unwrap_or_else(|| value.round_to_i64_saturating())
                .to_string()
        }
        value => {
            let text = tidb_goruntime::GoString::from_bytes(
                crate::arg_eval_type::eval_string(&value)?.expect("non-null interval"),
            )
            .to_utf8_lossy_go();
            if crate::time_fn::calendar::is_composite_unit(unit) {
                text
            } else if unit.eq_ignore_ascii_case("SECOND") {
                let (decimal, error) = tidb_datatype::MyDecimal::from_string(text.as_bytes());
                if error.is_some() {
                    crate::expression_errors::handle_truncate(
                        ctx,
                        &format!("Truncated incorrect DECIMAL value: '{text}'"),
                    )?;
                }
                String::from_utf8_lossy(&decimal.to_string_bytes()).into_owned()
            } else {
                let text = text.trim();
                let bytes = text.as_bytes();
                let start = usize::from(bytes.first().is_some_and(|b| matches!(b, b'+' | b'-')));
                let digits = bytes[start..]
                    .iter()
                    .take_while(|b| b.is_ascii_digit())
                    .count();
                let integer = if digits == 0 {
                    "0"
                } else {
                    &text[..start + digits]
                };
                if integer != text {
                    crate::expression_errors::handle_truncate(
                        ctx,
                        &format!("Truncated incorrect DECIMAL value: '{text}'"),
                    )?;
                }
                integer.to_owned()
            }
        }
    };
    Ok(Datum::new_string_datum(text))
}

/// Go `vecGetDateFrom{Int,Real,Decimal,String,Datetime}` before `timeOp`.
pub(crate) fn date(unit: &str, value: Datum, ctx: &dyn Columns) -> Result<Datum, EvalError> {
    if value.is_null() || matches!(value, Datum::Duration(_)) {
        return Ok(value);
    }
    let zone = ctx.time_zone();
    let zero = ctx.type_flags().ignore_zero_in_date_err();
    let invalid = ctx.type_flags().ignore_invalid_date_err();
    let clock = tidb_datatype::is_clock_unit(unit);
    let result = match &value {
        Datum::Time(time) => Ok(*time),
        Datum::Int(number) => tidb_datatype::parse_time_from_int64(*number, zero, invalid, &zone),
        Datum::UInt(number) => {
            tidb_datatype::parse_time_from_int64(*number as i64, zero, invalid, &zone)
        }
        Datum::Real(number) => {
            tidb_datatype::parse_time_from_float64(*number, zero, invalid, &zone)
        }
        Datum::Decimal(number) => {
            tidb_datatype::parse_time_from_decimal(number, zero, invalid, &zone)
        }
        value => {
            let bytes = crate::arg_eval_type::eval_string(value)?.expect("date string");
            let text = tidb_goruntime::GoString::from_bytes(bytes).to_utf8_lossy_go();
            let kind = if !clock && tidb_datatype::is_date_format(&text) {
                TimeType::Date
            } else {
                TimeType::DateTime
            };
            tidb_datatype::parse_time(&text, kind, 6, false, zero, invalid, &zone)
                .map(|parsed| parsed.time)
        }
    };
    let mut time = match result {
        Ok(time) => time,
        Err(tidb_datatype::TimeError::Syntax(message)) => return Err(EvalError::Raw(message)),
        Err(_) => {
            let numeric = matches!(
                value,
                Datum::Int(_) | Datum::UInt(_) | Datum::Real(_) | Datum::Decimal(_)
            );
            let text = match &value {
                Datum::UInt(number) => (*number as i64).to_string(),
                Datum::Real(number) => (*number as i64).to_string(),
                Datum::Decimal(number) => number
                    .to_string()
                    .split('.')
                    .next()
                    .unwrap_or("")
                    .to_owned(),
                _ => value.sql_string().unwrap_or_default(),
            };
            crate::expression_errors::handle_truncate(
                ctx,
                &format!(
                    "Incorrect {} value: '{text}'",
                    if numeric { "time" } else { "datetime" }
                ),
            )?;
            return Ok(Datum::Null);
        }
    };
    if matches!(value, Datum::String(_) | Datum::Bytes(_))
        && ctx.date_modes().no_zero_date
        && time.invalid_zero()
    {
        crate::expression_errors::handle_truncate(
            ctx,
            &format!(
                "Incorrect datetime value: '{}'",
                value.sql_string().unwrap_or_default()
            ),
        )?;
        return Ok(Datum::Null);
    }
    if clock || time.kind() == TimeType::Timestamp {
        time.set_kind(TimeType::DateTime);
    }
    Ok(Datum::Time(time))
}

/// Go `baseDateArithmetical.add`, `sub`, `addDate`, and `opDuration`.
pub(crate) fn operation(
    field: &FieldType,
    date_field: &FieldType,
    unit: &str,
    subtract: bool,
    mut date: Datum,
    amount: Datum,
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    if date.is_null() || amount.is_null() {
        return Ok(Datum::Null);
    }
    let interval = amount
        .sql_string()
        .map_err(|_| EvalError::Unsupported("interval string"))?;
    if field.eval_type() == EvalType::Duration {
        let Datum::Duration(duration) = date else {
            unreachable!("duration signature");
        };
        let interval_value = match tidb_datatype::extract_duration_value(unit, &interval) {
            Ok(value) => value,
            Err(_) => {
                crate::expression_errors::handle_truncate(
                    ctx,
                    &format!("Incorrect datetime value: '{interval}'"),
                )?;
                return Ok(Datum::Null);
            }
        };
        let value = if subtract {
            duration.checked_sub(interval_value)
        } else {
            duration.checked_add(interval_value)
        }
        .map_err(|_| EvalError::IntOverflow)?;
        return Ok(Datum::Duration(
            tidb_datatype::MySqlDuration::from_raw_parts(value.nanoseconds(), field.decimal()),
        ));
    }
    if matches!(date, Datum::Duration(_)) {
        date = crate::cast::cast_arg_as_datetime(&date, Some(date_field), ctx)?;
    }
    let Datum::Time(mut time) = date else {
        return Ok(Datum::Null);
    };
    let parts = match tidb_datatype::parse_duration_value(unit, &interval) {
        Ok(parts) => parts,
        Err(_) => {
            crate::expression_errors::handle_truncate(
                ctx,
                &format!("Incorrect datetime value: '{interval}'"),
            )?;
            return Ok(Datum::Null);
        }
    };
    if parts.truncated {
        let error = tidb_datatype::types_errors::ERR_TRUNCATED_WRONG_VAL
            .gen_with_stack_by_args(&[tidb_mysql::errors::FormatArg::from(interval.as_str())]);
        crate::expression_errors::handle_truncate(ctx, error.message())?;
    }
    let base = match time.go_time(&chrono::Utc) {
        Ok(base) => base,
        Err(_) => {
            crate::expression_errors::handle_truncate(
                ctx,
                &format!("Incorrect time value: '{}'", time),
            )?;
            return Ok(Datum::Null);
        }
    };
    let sign: i64 = if subtract { -1 } else { 1 };
    let nanos = parts.nanoseconds.wrapping_mul(sign);
    let Some(shifted) = base.checked_add_signed(chrono::Duration::nanoseconds(nanos)) else {
        return overflow(ctx);
    };
    let core = tidb_datatype::core_time_from_datetime(shifted);
    let core = match core.add_date(
        parts.years.wrapping_mul(sign),
        parts.months.wrapping_mul(sign),
        parts.days.wrapping_mul(sign),
    ) {
        Ok(core) => core,
        Err(_) => return overflow(ctx),
    };
    let core = if core.year() == 0 {
        tidb_datatype::CoreTime::from_date(
            0,
            0,
            0,
            core.hour(),
            core.minute(),
            core.second(),
            core.microsecond(),
        )
    } else {
        core
    };
    time.set_core_time(core);
    time.set_fsp(field.decimal())
        .map_err(|error| EvalError::Raw(error.to_string()))?;
    if field.eval_type() == EvalType::String {
        time.set_fsp(if time.core_time().microsecond() == 0 {
            0
        } else {
            6
        })
        .expect("valid fsp");
        Ok(Datum::new_string_datum(time.to_string()))
    } else {
        Ok(Datum::Time(time))
    }
}

/// Go `handleInvalidTimeError(ErrDatetimeFunctionOverflow("datetime"))`.
fn overflow(ctx: &dyn Columns) -> Result<Datum, EvalError> {
    crate::time_fn::calendar::date_arithmetic_overflow(ctx)
}
