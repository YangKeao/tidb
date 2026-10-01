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
// See the License for the specific language governing permissions and
// limitations under the License.

//! Time builtin family, translated from `pkg/expression/builtin_time.go`.
//!
//! This module is the single Rust ownership boundary for the Go source
//! family. It owns both pure value functions and statement-clock functions;
//! callers enter through one narrow [`dispatch`] seam instead of growing the
//! generic builtin dispatcher or splitting helpers across unrelated modules.
//!
//! `ADDTIME`, `SUBTIME`, `TIMESTAMP`, `TIMESTAMPADD` and `SYSDATE` -- the
//! five that Go types from the argument `FieldType`s rather than from their
//! values -- live in [`add_sub`], with the microsecond value domain they
//! need in [`duration_parse`].

pub(crate) mod add_sub;
pub(crate) mod calendar;
mod convert_tz;
pub(crate) mod duration_parse;
pub(crate) mod extract;
pub(crate) mod session_tz;

#[cfg(test)]
use self::calendar::week_of_year;
use self::calendar::{civil_from_days, days_from_civil, parse_date_ymd};
use crate::coerce::coerce_str;
use crate::{Columns, Datum, EvalError};
use tidb_query_datatype::codec::mysql::Time as TikvTime;

/// Dispatches this family's builtins; `None` if `name` isn't one of them.
pub(crate) fn dispatch(
    name: &str,
    vals: &[Datum],
    cols: &dyn Columns,
) -> Option<Result<Datum, EvalError>> {
    Some(match name {
        // `pkg/expression/builtin.go:722-725` binds all four names to the SAME
        // `nowFunctionClass`, so LOCALTIME and LOCALTIMESTAMP are NOW down to
        // the optional fsp argument and the statement-timestamp clock. Go's
        // capture: `select localtime(), localtimestamp(), localtime,
        // localtimestamp, now()` prints one value five times, and
        // `localtime() = now()` is 1.
        "NOW" | "CURRENT_TIMESTAMP" | "LOCALTIME" | "LOCALTIMESTAMP" => now(vals, cols),
        "UTC_TIMESTAMP" => utc_timestamp(vals, cols),
        "CURDATE" | "CURRENT_DATE" => current_date(vals, cols),
        "UTC_DATE" => utc_date(vals, cols),
        "CURTIME" => current_time(vals, "curtime", cols),
        "CURRENT_TIME" => current_time(vals, "current_time", cols),
        "UTC_TIME" => utc_time(vals, cols),
        "DATE" => date(vals, cols),
        "MICROSECOND" => microsecond(vals),
        "TIME" => time(vals, cols),
        "MONTH" => month_in(vals, cols),
        "DAY" | "DAYOFMONTH" => day_of_month_in(vals, cols),
        "DAYOFWEEK" => day_of_week_in(vals, cols),
        "DAYOFYEAR" => day_of_year_in(vals, cols),
        "WEEKDAY" => weekday_in(vals, cols),
        "QUARTER" => quarter_in(vals, cols),
        "WEEK" => week_in(vals, cols.default_week_format(), cols),
        "WEEKOFYEAR" => week_of_year_builtin_in(vals, cols),
        "TIDB_PARSE_TSO_LOGICAL" => tidb_parse_tso_logical_in(vals, cols),
        "TIDB_BOUNDED_STALENESS" => tidb_bounded_staleness(vals, cols),
        "TIDB_CURRENT_TSO" => current_tso(vals, cols),
        "GET_FORMAT" => get_format_value_in(vals, cols),
        "YEARWEEK" => yearweek_in(vals, cols),
        "MONTHNAME" => monthname_in(vals, cols),
        "DAYNAME" => dayname_in(vals, cols),
        "LAST_DAY" => last_day(vals),
        "TIME_TO_SEC" => time_to_sec_in(vals, cols),
        "SEC_TO_TIME" => sec_to_time(vals, cols),
        "MAKEDATE" => makedate(vals),
        "MAKETIME" => maketime(vals, cols),
        "PERIOD_ADD" => period_add_in(vals, cols),
        "PERIOD_DIFF" => period_diff_in(vals, cols),
        "TIME_FORMAT" => time_format(vals),
        "STR_TO_DATE" => calendar::str_to_date(vals, cols),
        "FROM_DAYS" => calendar::from_days(vals),
        "TIMEDIFF" => time_diff(vals),
        "CONVERT_TZ" => convert_tz::convert_tz(vals),
        "FROM_UNIXTIME" => session_tz::from_unixtime(vals, cols),
        "UNIX_TIMESTAMP" => session_tz::unix_timestamp(vals, cols),
        "TIDB_PARSE_TSO" => session_tz::tidb_parse_tso(vals, cols),
        "TIMESTAMPDIFF" => calendar::timestamp_diff(vals),
        // `ADDTIME`/`SUBTIME` reach here with no static argument types, so
        // every argument takes Go's `default` branch -- which is the branch
        // Go itself selects for a string constant. The chunk tier, which
        // does have the types, enters through [`add_sub::add_sub_time`]
        // directly; see that module's doc for the row/vec split this
        // `row_path = true` selects.
        "ADDTIME" | "SUBTIME" => add_sub::add_sub_untyped(name, vals, cols),
        "TIMESTAMP" => add_sub::timestamp(vals, cols),
        "TIMESTAMPADD" => add_sub::timestamp_add(vals, cols),
        "SYSDATE" => add_sub::sysdate(vals, cols),
        "TO_DAYS" => calendar::to_days_in(vals, cols),
        "TO_SECONDS" => calendar::to_seconds_in(vals, cols),
        // `EXTRACT(<composite unit> FROM value)`, e.g. `HOUR_MINUTE`,
        // `DAY_SECOND`, `YEAR_MONTH` — see `calendar::extract_composite`'s
        // own doc.
        "YEAR_MONTH" | "DAY_HOUR" | "DAY_MINUTE" | "DAY_SECOND" | "DAY_MICROSECOND"
        | "HOUR_MINUTE" | "HOUR_SECOND" | "HOUR_MICROSECOND" | "MINUTE_SECOND"
        | "MINUTE_MICROSECOND" | "SECOND_MICROSECOND" => {
            calendar::extract_composite(name, vals, cols)
        }
        _ => return None,
    })
}

/// `TIDB_CURRENT_TSO()`: the active transaction's start timestamp, or zero
/// when the session is not inside a transaction.
fn current_tso(vals: &[Datum], cols: &dyn Columns) -> Result<Datum, EvalError> {
    if !vals.is_empty() {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    Ok(Datum::Int(cols.current_tso()))
}

/// Go `builtinTiDBBoundedStalenessSig.evalTime`: choose a read timestamp from
/// the requested inclusive window and the statement's SafeTS. The storage
/// layer publishes the already timezone-adjusted SafeTS through
/// `Columns::bounded_staleness_safe_time`; a context without storage has no
/// value and therefore uses the lower bound (the same outcome as a zero
/// SafeTS for normal post-epoch datetimes). The result is always a DATETIME
/// with millisecond precision, matching `setDecimalAndFlenForDatetime(3)`.
fn tidb_bounded_staleness(vals: &[Datum], cols: &dyn Columns) -> Result<Datum, EvalError> {
    let [left, right] = vals else {
        return Err(EvalError::WrongParameterCount("tidb_bounded_staleness"));
    };
    let (Datum::Time(left), Datum::Time(right)) = (left, right) else {
        return if vals.iter().any(Datum::is_null) {
            Ok(Datum::Null)
        } else {
            Err(EvalError::Unsupported(
                "TIDB_BOUNDED_STALENESS arguments reached the signature without ETDatetime casts",
            ))
        };
    };
    // `builtinTiDBBoundedStalenessSig` runs `InvalidZero` through
    // `handleInvalidTimeError` before converting either endpoint to Go time.
    // Keep that check here, after the signature cast has produced typed
    // values, so zero/zero-in-date inputs cannot accidentally become a valid
    // lower-bound read timestamp.
    for value in [left, right] {
        if value.invalid_zero() {
            cols.handle_truncate(&format!("Incorrect datetime value: '{value}'"))?;
            return Ok(Datum::Null);
        }
    }
    if left.compare(*right).is_gt() {
        return Ok(Datum::Null);
    }
    let mut result = match cols.bounded_staleness_safe_time() {
        Some(safe) if safe.compare(*left).is_lt() => *left,
        Some(safe) if safe.compare(*right).is_gt() => *right,
        Some(safe) => safe,
        None => *left,
    };
    result.set_kind(tidb_datatype::TimeType::DateTime);
    result
        .set_fsp(3)
        .map_err(|_| EvalError::Unsupported("invalid bounded-staleness result precision"))?;
    Ok(Datum::Time(result))
}

/// `DATE(expr)`, after Go's declared `ETDatetime` argument cast has produced
/// a typed temporal value. The function applies its own zero-date SQL-mode
/// checks, clears the clock, and changes the result domain to `DATE`.
pub(crate) fn date(vals: &[Datum], cols: &dyn Columns) -> Result<Datum, EvalError> {
    let [value] = vals else {
        return Err(EvalError::Unsupported("bad function arity"));
    };
    let Datum::Time(mut value) = value else {
        return if matches!(value, Datum::Null) {
            Ok(Datum::Null)
        } else {
            Err(EvalError::Unsupported(
                "DATE argument reached the signature without its ETDatetime cast",
            ))
        };
    };

    let modes = cols.date_modes();
    if (value.is_zero() && modes.no_zero_date)
        || (!value.is_zero() && value.invalid_zero() && modes.no_zero_in_date)
    {
        cols.handle_truncate(&format!("Incorrect datetime value: '{value}'"))?;
        return Ok(Datum::Null);
    }

    let core = value.core_time();
    value.set_core_time(tidb_datatype::CoreTime::from_date(
        u16::try_from(core.year()).expect("a typed temporal value has a nonnegative year"),
        core.month(),
        core.day(),
        0,
        0,
        0,
        0,
    ));
    value.set_kind(tidb_datatype::TimeType::Date);
    Ok(Datum::Time(value))
}

/// `builtinNowWithArgSig` / `builtinNowWithoutArgSig`: local
/// (`time_zone`-adjusted) statement time, always truncating fractional
/// seconds. `CURRENT_TIMESTAMP` is the same function class.
fn now(vals: &[Datum], cols: &dyn Columns) -> Result<Datum, EvalError> {
    let fsp = parse_fsp_with_null_as_zero(vals, "now")?.unwrap_or(0);
    let (utc_secs, nanos, tz_offset) = cols.now().ok_or(no_clock_err())?;
    Ok(Datum::new_string(format_datetime(
        utc_secs + i64::from(tz_offset),
        nanos,
        fsp,
        false,
    )))
}

/// `builtinUTCTimestampWithArgSig` / `builtinUTCTimestampWithoutArgSig`:
/// raw UTC statement time, always rounding fractional seconds half-up.
fn utc_timestamp(vals: &[Datum], cols: &dyn Columns) -> Result<Datum, EvalError> {
    let fsp = parse_fsp_with_null_as_zero(vals, "utc_timestamp")?.unwrap_or(0);
    let (utc_secs, nanos, _) = cols.now().ok_or(no_clock_err())?;
    Ok(Datum::new_string(format_datetime(
        utc_secs, nanos, fsp, true,
    )))
}

/// `builtinCurrentDateSig`: local statement date. `CURDATE` and
/// `CURRENT_DATE` share this signature and accept no argument.
fn current_date(vals: &[Datum], cols: &dyn Columns) -> Result<Datum, EvalError> {
    if !vals.is_empty() {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    let (utc_secs, _, tz_offset) = cols.now().ok_or(no_clock_err())?;
    Ok(Datum::new_string(format_date(
        utc_secs + i64::from(tz_offset),
    )))
}

/// `builtinUTCDateSig`: raw UTC statement date with no arguments.
fn utc_date(vals: &[Datum], cols: &dyn Columns) -> Result<Datum, EvalError> {
    if !vals.is_empty() {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    let (utc_secs, _, _) = cols.now().ok_or(no_clock_err())?;
    Ok(Datum::new_string(format_date(utc_secs)))
}

/// `builtinCurrentTime0ArgSig` / `builtinCurrentTime1ArgSig`: local
/// statement time. The zero-argument signature truncates; an explicit FSP,
/// including zero, rounds half-up. `CURTIME` and `CURRENT_TIME` are aliases.
fn current_time(
    vals: &[Datum],
    function: &'static str,
    cols: &dyn Columns,
) -> Result<Datum, EvalError> {
    let fsp = parse_fsp_with_null_as_zero(vals, function)?;
    let (utc_secs, nanos, tz_offset) = cols.now().ok_or(no_clock_err())?;
    // builtinCurrentTime1ArgSig first renders TimeFSPFormat (six digits,
    // truncating sub-microsecond nanoseconds) and only then ParseDuration
    // rounds to the requested FSP. Preserve that two-stage source algorithm;
    // it is observably different from UTC_TIMESTAMP's direct half-up path.
    let nanos = fsp.map_or(nanos, |_| nanos / 1_000 * 1_000);
    Ok(Datum::new_string(format_time_only(
        utc_secs + i64::from(tz_offset),
        nanos,
        fsp.unwrap_or(0),
        fsp.is_some(),
    )))
}

/// `builtinUTCTimeWithoutArgSig` / `builtinUTCTimeWithArgSig`: raw UTC
/// statement time with the same zero-argument-truncate / explicit-FSP-round
/// split as [`current_time`].
fn utc_time(vals: &[Datum], cols: &dyn Columns) -> Result<Datum, EvalError> {
    if matches!(vals, [Datum::Null]) {
        return Ok(Datum::Null);
    }
    let fsp = parse_fsp_for(vals, "utc_time")?;
    let (utc_secs, nanos, _) = cols.now().ok_or(no_clock_err())?;
    // builtinUTCTimeWithArgSig has the identical TimeFSPFormat-then-parse
    // conversion as CURRENT_TIME's explicit signature.
    let nanos = fsp.map_or(nanos, |_| nanos / 1_000 * 1_000);
    Ok(Datum::new_string(format_time_only(
        utc_secs,
        nanos,
        fsp.unwrap_or(0),
        fsp.is_some(),
    )))
}

/// `builtinMicroSecondSig.evalInt`: read the fractional component of the
/// ETDuration argument. Go deliberately suppresses a duration-cast error and
/// returns NULL, unlike `TIME()` which reports the same truncation through the
/// statement context.
pub(crate) fn microsecond(vals: &[Datum]) -> Result<Datum, EvalError> {
    if vals.len() != 1 {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    let Some(value) = coerce_str(&vals[0])? else {
        return Ok(Datum::Null);
    };
    let fsp = duration_parse::get_fsp(&value);
    Ok(match duration_parse::parse_duration(&value, fsp) {
        Ok(duration) => Datum::Int(duration.micro_second()),
        Err(_) => Datum::Null,
    })
}

/// `builtinTimeSig.evalDuration`: parse the string as a TiDB duration while
/// preserving its written FSP. `ErrTruncatedWrongVal` is a statement warning
/// for a SELECT and leaves Go's zero-value duration as the result.
fn time(vals: &[Datum], cols: &dyn Columns) -> Result<Datum, EvalError> {
    if vals.len() != 1 {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    let Some(value) = coerce_str(&vals[0])? else {
        return Ok(Datum::Null);
    };
    let fsp = duration_parse::get_fsp(&value);
    match duration_parse::parse_duration(&value, fsp) {
        Ok(duration) => Ok(Datum::new_string(duration.format())),
        Err(_) => {
            cols.handle_truncate(&format!(
                "Truncated incorrect time value: '{}'",
                tidb_datatype::warning_subject_byte_cap(&value)
            ))?;
            Ok(Datum::new_string("00:00:00".to_owned()))
        }
    }
}

fn no_clock_err() -> EvalError {
    EvalError::Unsupported("no session clock (SET timestamp)")
}

/// Parses the source family's optional 0-6 fractional-seconds precision.
fn parse_fsp_for(vals: &[Datum], function: &'static str) -> Result<Option<u32>, EvalError> {
    match vals {
        [] => Ok(None),
        [Datum::Int(i)] if (0..=6).contains(i) => Ok(Some(*i as u32)),
        [Datum::UInt(i)] if *i <= 6 => Ok(Some(*i as u32)),
        // Go `types.ErrTooBigPrecision` (1426), raised at evaluation time by
        // the clock signatures themselves
        // (`pkg/expression/builtin_time.go:2730` and siblings).
        [Datum::Int(i)] if *i > 6 => Err(EvalError::TooBigFsp { fsp: *i, function }),
        [Datum::UInt(i)] => Err(EvalError::TooBigFsp {
            fsp: *i as i64,
            function,
        }),
        _ => Err(EvalError::Unsupported(
            "bad fractional-seconds-precision argument",
        )),
    }
}

fn parse_fsp_with_null_as_zero(
    vals: &[Datum],
    function: &'static str,
) -> Result<Option<u32>, EvalError> {
    if matches!(vals, [Datum::Null]) {
        Ok(Some(0))
    } else {
        parse_fsp_for(vals, function)
    }
}

/// Renders an epoch second as a Gregorian `YYYY-MM-DD` date.
fn format_date(secs: i64) -> String {
    let (y, m, d) = civil_from_days(secs.div_euclid(86_400));
    format!("{y:04}-{m:02}-{d:02}")
}

fn format_hms(secs: i64) -> String {
    let secs_of_day = secs.rem_euclid(86_400);
    let (hour, minute, second) = (
        secs_of_day / 3_600,
        (secs_of_day % 3_600) / 60,
        secs_of_day % 60,
    );
    format!("{hour:02}:{minute:02}:{second:02}")
}

fn frac_suffix(nanos: u32, fsp: u32) -> String {
    if fsp == 0 {
        return String::new();
    }
    let fraction = nanos / 10u32.pow(9 - fsp);
    format!(".{fraction:0width$}", width = fsp as usize)
}

/// TiDB's `types.ModeHalfUp` rounding at the requested FSP.
fn round_nanos(nanos: u32, fsp: u32) -> (i64, u32) {
    let scale = 10u32.pow(9 - fsp);
    let half_up = nanos + scale / 2;
    if half_up >= 1_000_000_000 {
        (1, 0)
    } else {
        (0, (half_up / scale) * scale)
    }
}

fn format_datetime(secs: i64, nanos: u32, fsp: u32, round: bool) -> String {
    let (carry, nanos) = if round {
        round_nanos(nanos, fsp)
    } else {
        (0, nanos)
    };
    let secs = secs + carry;
    format!(
        "{} {}{}",
        format_date(secs),
        format_hms(secs),
        frac_suffix(nanos, fsp)
    )
}

fn format_time_only(secs: i64, nanos: u32, fsp: u32, round: bool) -> String {
    let (carry, nanos) = if round {
        round_nanos(nanos, fsp)
    } else {
        (0, nanos)
    };
    let secs = secs + carry;
    format!("{}{}", format_hms(secs), frac_suffix(nanos, fsp))
}

/// Parses a date/datetime argument at the same value boundary as Go's
/// `EvalTime`.  [`parse_date_ymd`] intentionally ignores a trailing time
/// suffix because date-part functions only need the calendar fields; the
/// `LAST_DAY` signature still rejects a malformed suffix (for example
/// `23:59:61`) before it computes the month end.
fn single_datetime(vals: &[Datum]) -> Result<Option<(i64, u32, u32)>, EvalError> {
    if vals.len() != 1 {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    let Some(value) = coerce_str(&vals[0])? else {
        return Ok(None);
    };
    let value = value.trim();
    let (date, time) = value
        .split_once(char::is_whitespace)
        .map_or((value, None), |(date, time)| (date, Some(time.trim())));
    let Some(ymd) = parse_date_ymd(date) else {
        return Ok(None);
    };
    if let Some(time) = time {
        if calendar::parse_time_with_fraction(time).is_none() {
            return Ok(None);
        }
    }
    Ok(Some(ymd))
}

/// `builtinMonthSig.evalInt` in `pkg/expression/builtin_time.go`.
///
/// The source returns the stored month field directly with no zero
/// rejection, because `monthFunctionClass` declares its argument
/// `types.ETDatetime` (`builtin_time.go:1116`) and so receives a value
/// `EvalTime` already produced non-NULL — see [`calendar::component_time_core`].
pub(crate) fn month_in(vals: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    calendar::component_field_in(vals, crate::tikv::EvaluatedBytesOp::MonthCoreNative, ctx)
}

#[cfg(test)]
pub(crate) fn month(vals: &[Datum]) -> Result<Datum, EvalError> {
    month_in(vals, &crate::NoColumns)
}

/// The legacy MONTH seam accepts an already-evaluated core without rebuilding
/// or validating a Time. Even NULL reaches the same shared field kernel.
pub(crate) fn month_core_in(
    value: Option<tidb_datatype::CoreTime>,
    ctx: &dyn Columns,
) -> Result<Option<i64>, EvalError> {
    crate::tikv::evaluate_args_in(
        crate::tikv::EvaluatedBytesOp::MonthCoreNative,
        ctx,
        || {
            Ok(crate::tikv::EvaluatedArgs::TimeCoreBits(
                value.map(tidb_datatype::CoreTime::raw),
            ))
        },
        |computed| match computed.into_int_datum()? {
            Datum::Null => Ok(None),
            Datum::Int(value) => Ok(Some(value)),
            _ => Err(EvalError::Unsupported("MONTH result kind mismatch")),
        },
    )
}

/// `builtinDayOfMonthSig.evalInt` in `pkg/expression/builtin_time.go`.
///
/// The source returns the stored day field directly with no zero rejection,
/// because `dayOfMonthFunctionClass` declares its argument
/// `types.ETDatetime` (`builtin_time.go:1284`) and so receives a value
/// `EvalTime` already produced non-NULL — see [`calendar::component_time_core`].
fn day_of_month_in(vals: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    calendar::component_field_in(
        vals,
        crate::tikv::EvaluatedBytesOp::DayOfMonthCoreNative,
        ctx,
    )
}

#[cfg(test)]
fn day_of_month(vals: &[Datum]) -> Result<Datum, EvalError> {
    day_of_month_in(vals, &crate::NoColumns)
}

/// `builtinDayOfWeekSig.evalInt`: Sunday is 1 through Saturday 7.
/// Original date parsing and projection run in the worker, including NULL.
pub(crate) fn day_of_week_in(vals: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_bytes_in(
        crate::tikv::EvaluatedBytesOp::DayOfWeekTextNative,
        ctx,
        || single_temporal_text(vals),
        crate::tikv::EvaluatedBytesResult::into_int_datum,
    )
}

#[cfg(test)]
fn day_of_week(vals: &[Datum]) -> Result<Datum, EvalError> {
    day_of_week_in(vals, &crate::NoColumns)
}

/// `builtinDayOfYearSig.evalInt`: one-based day within the calendar year.
/// Keep the existing cast and text preparation, not a host-computed day count.
pub(crate) fn day_of_year_in(vals: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_bytes_in(
        crate::tikv::EvaluatedBytesOp::DayOfYearTextNative,
        ctx,
        || single_temporal_text(vals),
        crate::tikv::EvaluatedBytesResult::into_int_datum,
    )
}

#[cfg(test)]
fn day_of_year(vals: &[Datum]) -> Result<Datum, EvalError> {
    day_of_year_in(vals, &crate::NoColumns)
}

/// `builtinWeekDaySig.evalInt`: Monday is 0 through Sunday 6.
/// The worker retains native date validity, including valid year-zero dates.
pub(crate) fn weekday_in(vals: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_bytes_in(
        crate::tikv::EvaluatedBytesOp::WeekdayTextNative,
        ctx,
        || single_temporal_text(vals),
        crate::tikv::EvaluatedBytesResult::into_int_datum,
    )
}

#[cfg(test)]
fn weekday(vals: &[Datum]) -> Result<Datum, EvalError> {
    weekday_in(vals, &crate::NoColumns)
}

/// `builtinQuarterSig.evalInt` = `(date.Month() + 2) / 3`, returning 1-4 for
/// a real date and `0` for a month-zero one — with no zero rejection, because
/// `quarterFunctionClass` declares its argument `types.ETDatetime`
/// (`builtin_time.go:5833`) and so receives a value `EvalTime` already
/// produced non-NULL. Real TiDB's recorded `QUARTER(v1)` over a
/// zero-datetime column is `0`; `gorun` confirms `QUARTER(20240000)` is `0`
/// too, by the same stored month.
///
/// The month-zero string form no longer needs a parser of its own here: the
/// ETDatetime cast ([`crate::arg_eval_type`]) is what decides a string, and
/// it is `types.ParseTime` under the READ path's `IgnoreZeroInDate`, which
/// keeps a zero month exactly as Go does.
fn quarter_in(vals: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    calendar::component_field_in(vals, crate::tikv::EvaluatedBytesOp::QuarterCoreNative, ctx)
}

#[cfg(test)]
fn quarter(vals: &[Datum]) -> Result<Datum, EvalError> {
    quarter_in(vals, &crate::NoColumns)
}

/// `WEEKOFYEAR(date)` uses mode 3 in its single shared worker invocation.
/// Date parsing and NULL results follow admission; original text preparation
/// remains inside the frontend guard.
fn week_of_year_builtin_in(vals: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_bytes_in(
        crate::tikv::EvaluatedBytesOp::WeekOfYearTextNative,
        ctx,
        || single_temporal_text(vals),
        crate::tikv::EvaluatedBytesResult::into_int_datum,
    )
}

#[cfg(test)]
fn week_of_year_builtin(vals: &[Datum]) -> Result<Datum, EvalError> {
    week_of_year_builtin_in(vals, &crate::NoColumns)
}

/// `TIDB_PARSE_TSO_LOGICAL(tso)`. Port of `builtinTidbParseTsoLogicalSig` =
/// `oracle.ExtractLogical`: the low 18 bits of the timestamp oracle value. A
/// non-positive or NULL argument yields NULL. Session-independent (the physical
/// half, `TIDB_PARSE_TSO`, is not, because it renders a datetime in the session
/// time zone). For a positive `tso`, masking the low 18 bits as `i64` equals
/// Go's `uint64(tso) & logicalBits`.
pub(crate) fn tidb_parse_tso_logical_in(
    vals: &[Datum],
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_args_in(
        crate::tikv::EvaluatedBytesOp::TsoLogicalNative,
        ctx,
        || {
            if vals.len() != 1 {
                return Err(EvalError::Unsupported("bad function arity"));
            }
            Ok(crate::tikv::EvaluatedArgs::Int(int_arg(&vals[0])?))
        },
        crate::tikv::EvaluatedBytesResult::into_int_datum,
    )
}

#[cfg(test)]
fn tidb_parse_tso_logical(vals: &[Datum]) -> Result<Datum, EvalError> {
    tidb_parse_tso_logical_in(vals, &crate::NoColumns)
}

/// Direct table-vector compatibility, not a production evaluator fallback.
#[cfg(test)]
pub(crate) fn get_format(format_type: &str, location: &str) -> String {
    TikvTime::get_format_native(format_type.as_bytes(), location.as_bytes()).to_owned()
}

/// Preserve the values signature's byte domain and first-NULL demand boundary.
pub(crate) fn get_format_value_in(vals: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    // Only Datum::Null makes eval_string return None. This structural choice
    // does not coerce either argument or compute a lookup-table answer.
    let operation = if matches!(vals.first(), Some(Datum::Null)) {
        crate::tikv::EvaluatedBytesOp::GetFormatNullNative
    } else {
        crate::tikv::EvaluatedBytesOp::GetFormatNative
    };
    crate::tikv::evaluate_args_in(
        operation,
        ctx,
        || {
            let [format_type, location] = vals else {
                return Err(EvalError::Unsupported("bad function arity"));
            };
            let format_type = crate::arg_eval_type::eval_string(format_type)?;
            if format_type.is_none() {
                // This is the actually observed NULL, not a fabricated NULL
                // for the unobserved location. The witness rejects Some.
                return Ok(crate::tikv::EvaluatedArgs::Bytes(format_type));
            }
            let location = crate::arg_eval_type::eval_string(location)?;
            Ok(crate::tikv::EvaluatedArgs::Bytes2(format_type, location))
        },
        |computed| {
            Ok(computed
                .into_bytes()?
                .map_or(Datum::Null, Datum::new_string))
        },
    )
}

/// The independent AST path retains strict text coercion of its observed child.
/// The grammar selector is real input; only the worker selects the format.
pub(crate) fn get_format_ast_in(
    format_type: &str,
    location: &Datum,
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_args_in(
        crate::tikv::EvaluatedBytesOp::GetFormatNative,
        ctx,
        || {
            let location = coerce_str(location)?.map(String::into_bytes);
            Ok(crate::tikv::EvaluatedArgs::Bytes2(
                Some(format_type.as_bytes().to_vec()),
                location,
            ))
        },
        |computed| {
            Ok(computed
                .into_bytes()?
                .map_or(Datum::Null, Datum::new_string))
        },
    )
}

/// Probe the date in a worker before demanding the optional mode. The probe
/// returns the original owned text, never parsed fields or a host-side answer.
/// Its driver call finishes before the second call starts: no frontend callback
/// reenters C4. Admission may therefore fail before mode coercion is demanded.
fn week_text_in(
    vals: &[Datum],
    default_mode: i64,
    operation: crate::tikv::EvaluatedBytesOp,
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    let date = crate::tikv::evaluate_args_in(
        crate::tikv::EvaluatedBytesOp::WeekDateTextNative,
        ctx,
        || {
            if !(1..=2).contains(&vals.len()) {
                return Err(EvalError::Unsupported("bad function arity"));
            }
            Ok(crate::tikv::EvaluatedArgs::Bytes(
                coerce_str(&vals[0])?.map(String::into_bytes),
            ))
        },
        crate::tikv::EvaluatedBytesResult::into_bytes,
    )?;
    let Some(date) = date else {
        return Ok(Datum::Null);
    };
    crate::tikv::evaluate_args_in(
        operation,
        ctx,
        || {
            // An actual NULL mode stays NULL here; the worker applies mode zero.
            let mode = if vals.len() == 2 {
                int_arg(&vals[1])?
            } else {
                Some(default_mode)
            };
            Ok(crate::tikv::EvaluatedArgs::BytesInt(Some(date), mode))
        },
        crate::tikv::EvaluatedBytesResult::into_int_datum,
    )
}

/// Only the no-mode branch consumes the supplied default. Callers preserve the
/// original eager default_week_format getter, even for explicit modes or errors.
pub(crate) fn week_in(
    vals: &[Datum],
    default_week_format: i64,
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    week_text_in(
        vals,
        default_week_format,
        crate::tikv::EvaluatedBytesOp::WeekTextNative,
        ctx,
    )
}

#[cfg(test)]
pub(crate) fn week(vals: &[Datum], default_week_format: i64) -> Result<Datum, EvalError> {
    week_in(vals, default_week_format, &crate::NoColumns)
}

/// YEARWEEK's omitted mode is always zero, not the session default. Its year
/// combination and negative sentinel are calculated only by the final worker.
fn yearweek_in(vals: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    let operation = crate::tikv::EvaluatedBytesOp::YearWeekTextNative;
    week_text_in(vals, 0, operation, ctx)
}

#[cfg(test)]
fn yearweek(vals: &[Datum]) -> Result<Datum, EvalError> {
    yearweek_in(vals, &crate::NoColumns)
}

/// Legacy WEEK is a nonvalidating raw-core mode-zero projection, distinct from
/// SQL text parsing. Preserve actual NULL and all original core bits.
pub(crate) fn week_core_in(
    value: Option<tidb_datatype::CoreTime>,
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_args_in(
        crate::tikv::EvaluatedBytesOp::WeekCoreNative,
        ctx,
        || {
            Ok(crate::tikv::EvaluatedArgs::TimeCoreBits(
                value.map(tidb_datatype::CoreTime::raw),
            ))
        },
        crate::tikv::EvaluatedBytesResult::into_int_datum,
    )
}

/// Original single-argument text preparation, used only inside the guards.
/// Parsing stays in the worker: resource refusal precedes bad-text results,
/// while the original arity, UTF-8 and coercion errors precede admission.
fn single_temporal_text(vals: &[Datum]) -> Result<Option<Vec<u8>>, EvalError> {
    let [value] = vals else {
        return Err(EvalError::Unsupported("bad function arity"));
    };
    Ok(coerce_str(value)?.map(String::into_bytes))
}

/// `builtinMonthNameSig` retains its existing upstream ETDatetime cast.
/// The worker parses the actual text and selects the shared full month name.
pub(crate) fn monthname_in(vals: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_bytes_in(
        crate::tikv::EvaluatedBytesOp::MonthNameTextNative,
        ctx,
        || single_temporal_text(vals),
        |computed| {
            Ok(computed
                .into_bytes()?
                .map_or(Datum::Null, Datum::new_string))
        },
    )
}

#[cfg(test)]
fn monthname(vals: &[Datum]) -> Result<Datum, EvalError> {
    monthname_in(vals, &crate::NoColumns)
}

/// `builtinDayNameSig` parses the actual text and selects the shared full
/// weekday name in the worker; no native date result or table lookup is kept.
pub(crate) fn dayname_in(vals: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_bytes_in(
        crate::tikv::EvaluatedBytesOp::DayNameTextNative,
        ctx,
        || single_temporal_text(vals),
        |computed| {
            Ok(computed
                .into_bytes()?
                .map_or(Datum::Null, Datum::new_string))
        },
    )
}

#[cfg(test)]
fn dayname(vals: &[Datum]) -> Result<Datum, EvalError> {
    dayname_in(vals, &crate::NoColumns)
}

/// `builtinLastDaySig` in `pkg/expression/builtin_time.go`.
fn last_day(vals: &[Datum]) -> Result<Datum, EvalError> {
    Ok(single_datetime(vals)?.map_or(Datum::Null, |(y, m, _)| {
        let next_month = if m == 12 { (y + 1, 1) } else { (y, m + 1) };
        let (last_y, last_m, last_d) =
            civil_from_days(days_from_civil(next_month.0, next_month.1, 1) - 1);
        Datum::new_string(format!("{last_y:04}-{last_m:02}-{last_d:02}"))
    }))
}

/// Go `RoundFloat` + the int cast (`pkg/types/helper.go:30`,
/// `convert.go:109-122`): round half-to-even, then truncate the (already
/// integral) value.
fn round_float_to_i64(value: f64) -> i64 {
    let rounded = value.round_ties_even();
    if rounded >= i64::MAX as f64 {
        i64::MAX
    } else if rounded <= i64::MIN as f64 {
        i64::MIN
    } else {
        rounded as i64
    }
}

fn int_arg(value: &Datum) -> Result<Option<i64>, EvalError> {
    match value {
        Datum::Null => Ok(None),
        Datum::Int(v) => Ok(Some(*v)),
        Datum::UInt(v) => Ok(Some(*v as i64)),
        Datum::Decimal(v) => Ok(Some(v.round_to_i64().ok_or(EvalError::IntOverflow)?)),
        // Go converts a float argument to the signatures' ETInt through
        // `types.ConvertFloatToInt`, which rounds HALF-TO-EVEN
        // (`pkg/types/helper.go:30`: `RoundFloat = math.RoundToEven`) --
        // `makedate(71.1, 1.89)` is day 2, not day 1. Truncating here
        // answered 1971-01-01 where Go answers 1971-01-02.
        Datum::Real(v) => Ok(Some(round_float_to_i64(*v))),
        // Go's string-to-ETInt coercion for a STRING CONSTANT reads the
        // valid numeric PREFIX and truncates at the dot
        // (`getValidIntPrefix` -> `floatStrToIntStr`'s integer half; the
        // source table itself is the proof: MAKETIME(0, "58.5", 0) answers
        // 00:58:00 and MAKETIME(0, "59.5", 1) answers 00:59:01 -- both
        // TRUNCATED -- while the REAL 59.5 rounds to 60 and refuses).
        Datum::String(v) => Ok(Some(
            v.as_utf8()
                .map_err(|_| EvalError::Unsupported("invalid UTF-8 string datum"))?
                .trim()
                .parse::<f64>()
                .map(|value| value.trunc() as i64)
                .unwrap_or(0),
        )),
        Datum::Bytes(v) => Ok(Some(
            std::str::from_utf8(v)
                .map_err(|_| EvalError::Unsupported("invalid UTF-8 byte datum"))?
                .trim()
                .parse::<f64>()
                .map(|value| value.trunc() as i64)
                .unwrap_or(0),
        )),
        Datum::MinNotNull | Datum::MaxValue => {
            Err(EvalError::Unsupported("range sentinel time argument"))
        }
        other => other
            .to_i64()
            .map(|converted| Some(converted.value))
            .map_err(|_| EvalError::Unsupported("time argument conversion")),
    }
}

fn number_arg(value: &Datum, cols: &dyn Columns) -> Result<Option<f64>, EvalError> {
    Ok(match value {
        Datum::Null => None,
        Datum::Int(v) => Some(*v as f64),
        Datum::UInt(v) => Some(*v as f64),
        Datum::Decimal(v) => Some(v.to_f64()),
        Datum::Real(v) => Some(*v),
        // go's ETReal argument cast (WrapWithCastAsReal) warns the DOUBLE
        // truncation for a non-numeric string (oracle: MAKETIME's second
        // hand warns 1292 with 'x' -- g-fsp).
        Datum::String(v) => match v.as_utf8().ok().map(|text| text.trim().parse::<f64>()) {
            Some(Ok(parsed)) => Some(parsed),
            Some(Err(_)) => {
                let text = v.as_utf8().unwrap_or_default();
                let text = text.trim();
                cols.append_warning(1292, &format!("Truncated incorrect DOUBLE value: '{text}'"));
                Some(0.0)
            }
            None => Some(0.0),
        },
        Datum::Bytes(v) => {
            match std::str::from_utf8(v)
                .ok()
                .map(|text| text.trim().parse::<f64>())
            {
                Some(Ok(parsed)) => Some(parsed),
                Some(Err(_)) => {
                    let text = std::str::from_utf8(v).unwrap_or_default();
                    let text = text.trim();
                    cols.append_warning(
                        1292,
                        &format!("Truncated incorrect DOUBLE value: '{text}'"),
                    );
                    Some(0.0)
                }
                None => Some(0.0),
            }
        }
        Datum::MinNotNull | Datum::MaxValue => {
            return Err(EvalError::Unsupported("range sentinel numeric argument"));
        }
        other => Some(
            other
                .to_f64()
                .map_err(|_| EvalError::Unsupported("numeric argument conversion"))?
                .value,
        ),
    })
}

/// Parses TiDB's duration inputs used by `TIME_TO_SEC` and `TIME_FORMAT`.
/// This covers the accepted `H:M[:S[.fraction]]` and right-aligned numeric
/// forms exercised by `builtin_time_test.go`; the return is signed seconds
/// plus the fraction text preserved for formatting.
fn duration(value: &Datum) -> Result<Option<(i64, String)>, EvalError> {
    Ok(coerce_str(value)?.and_then(|text| TikvTime::parse_native_duration_text(&text)))
}

enum TimeDiffValue {
    DateTime { micros: i64, fsp: usize },
    Duration { micros: i64, fsp: usize },
}

/// `TIMEDIFF(expr1, expr2)`, covering the string-valued signatures exercised
/// by `builtin_time_test.go`.  Go selects among typed Time/Duration
/// signatures before evaluation; this value-only port keeps that distinction
/// by rejecting a mixed date-time/duration pair, while returning the canonical
/// duration string for matching pairs.  Zero month/day components are
/// accepted for the same `IgnoreZeroInDate` source rows and are interpreted
/// by the source-compatible `calcDaynr` arithmetic.
fn time_diff(vals: &[Datum]) -> Result<Datum, EvalError> {
    if vals.len() != 2 {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    let Some(left) = parse_time_diff_value(&vals[0])? else {
        return Ok(Datum::Null);
    };
    let Some(right) = parse_time_diff_value(&vals[1])? else {
        return Ok(Datum::Null);
    };
    let (left_micros, right_micros, fsp) = match (left, right) {
        (
            TimeDiffValue::DateTime {
                micros: left,
                fsp: left_fsp,
            },
            TimeDiffValue::DateTime {
                micros: right,
                fsp: right_fsp,
            },
        )
        | (
            TimeDiffValue::Duration {
                micros: left,
                fsp: left_fsp,
            },
            TimeDiffValue::Duration {
                micros: right,
                fsp: right_fsp,
            },
        ) => (left, right, left_fsp.max(right_fsp)),
        _ => return Ok(Datum::Null),
    };
    Ok(Datum::new_string(format_time_diff(
        truncate_time_diff(left_micros.saturating_sub(right_micros)),
        fsp,
    )))
}

fn parse_time_diff_value(value: &Datum) -> Result<Option<TimeDiffValue>, EvalError> {
    let Some(text) = coerce_str(value)? else {
        return Ok(None);
    };
    let text = text.trim();
    if text.is_empty() {
        return Ok(None);
    }
    if let Some((date, time)) = text.split_once(char::is_whitespace) {
        return Ok(parse_datetime_diff_value(date, time.trim()));
    }
    if text.contains(':') {
        return Ok(parse_duration_diff_value(text));
    }
    // A date-only value is a datetime at midnight.  Do not mistake a
    // colon-separated duration for a date (`10:9:0` was handled above).
    Ok(parse_datetime_diff_value(text, "00:00:00"))
}

fn parse_datetime_diff_value(date: &str, time: &str) -> Option<TimeDiffValue> {
    let parts = calendar::split_numeric_components_for_time_diff(date)?;
    let year = calendar::expand_year_for_time_diff(parts[0].0, parts[0].1);
    let month = parts[1].0;
    let day = parts[2].0;
    if month > 12 || day > 31 {
        return None;
    }
    if month != 0 && day > calendar::days_in_month_for_time_diff(year, month) {
        return None;
    }
    let (hour, minute, second, fraction) = calendar::parse_time_with_fraction(time)?;
    let fsp = fraction.len();
    let microsecond = fraction.parse::<u32>().ok().unwrap_or(0) * 10u32.pow(6 - fsp as u32);
    let micros = calendar::time_diff_daynr(year, month, day)
        .checked_mul(86_400_000_000)?
        .checked_add(i64::from(hour) * 3_600_000_000)?
        .checked_add(i64::from(minute) * 60_000_000)?
        .checked_add(i64::from(second) * 1_000_000)?
        .checked_add(i64::from(microsecond))?;
    Some(TimeDiffValue::DateTime { micros, fsp })
}

const MAX_TIME_DIFF_MICROS: i64 = (838 * 3_600 + 59 * 60 + 59) * 1_000_000;

fn truncate_time_diff(micros: i64) -> i64 {
    micros.clamp(-MAX_TIME_DIFF_MICROS, MAX_TIME_DIFF_MICROS)
}

fn parse_duration_diff_value(text: &str) -> Option<TimeDiffValue> {
    let (negative, text) = text
        .strip_prefix('-')
        .map_or((false, text), |text| (true, text));
    let mut fields = text.splitn(3, ':');
    let hour = fields.next()?.parse::<i64>().ok()?;
    let minute = fields.next()?.parse::<u32>().ok()?;
    let second_part = fields.next()?;
    let (second_part, fraction) = second_part.split_once('.').unwrap_or((second_part, ""));
    let second = second_part.parse::<u32>().ok()?;
    if minute > 59 || second > 59 || fraction.len() > 6 || !fraction.is_ascii() {
        return None;
    }
    let microsecond = if fraction.is_empty() {
        0
    } else {
        fraction.parse::<u32>().ok()? * 10u32.pow(6 - fraction.len() as u32)
    };
    let micros = hour
        .checked_mul(3_600_000_000)?
        .checked_add(i64::from(minute) * 60_000_000)?
        .checked_add(i64::from(second) * 1_000_000)?
        .checked_add(i64::from(microsecond))?;
    Some(TimeDiffValue::Duration {
        micros: if negative { -micros } else { micros },
        fsp: fraction.len(),
    })
}

fn format_time_diff(micros: i64, fsp: usize) -> String {
    let sign = if micros < 0 { "-" } else { "" };
    let absolute = micros.unsigned_abs();
    let hours = absolute / 3_600_000_000;
    let minutes = absolute / 60_000_000 % 60;
    let seconds = absolute / 1_000_000 % 60;
    if fsp == 0 {
        return format!("{sign}{hours:02}:{minutes:02}:{seconds:02}");
    }
    let divisor = 10u64.pow(6 - fsp as u32);
    let fraction = absolute / divisor % 10u64.pow(fsp as u32);
    format!(
        "{sign}{hours:02}:{minutes:02}:{seconds:02}.{fraction:0width$}",
        width = fsp
    )
}

/// `builtinTimeToSecSig` sends the original text, not precomputed seconds.
/// Its complete duration parser remains distinct from the HMS clamp policy.
pub(crate) fn time_to_sec_in(vals: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_bytes_in(
        crate::tikv::EvaluatedBytesOp::TimeToSecTextNative,
        ctx,
        || single_temporal_text(vals),
        crate::tikv::EvaluatedBytesResult::into_int_datum,
    )
}

#[cfg(test)]
fn time_to_sec(vals: &[Datum]) -> Result<Datum, EvalError> {
    time_to_sec_in(vals, &crate::NoColumns)
}

/// `builtinSecToTimeSig` in `pkg/expression/builtin_time.go`.
fn sec_to_time(vals: &[Datum], cols: &dyn Columns) -> Result<Datum, EvalError> {
    if vals.len() != 1 {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    let Some(seconds) = number_arg(&vals[0], cols)? else {
        return Ok(Datum::Null);
    };
    Ok(Datum::new_string(format_duration(
        seconds,
        duration_precision(&vals[0])?,
    )))
}

/// The result FSP comes from TiDB's argument type. String coercion uses the
/// duration parser's default FSP six; numeric literals preserve their own
/// fractional scale; integers have no fractional component.
fn duration_precision(value: &Datum) -> Result<usize, EvalError> {
    Ok(match value {
        Datum::Int(_) | Datum::UInt(_) => 0,
        Datum::String(_) | Datum::Bytes(_) => 6,
        Datum::Decimal(v) => v
            .to_string()
            .split_once('.')
            .map_or(0, |(_, f)| f.len().min(6)),
        // Go's makeTimeFunctionClass.getFunction
        // (`builtin_time.go:5587-5597`): an ETReal/ETDecimal argument takes
        // its field type's decimal, with >6 and Unspecified both clamping to
        // 6. A datum-level port cannot see the column's declared scale, and
        // the CONSTANT every test passes carries UnspecifiedLength -- so a
        // real argument answers 6 here, which is exactly what the source
        // table shows (MAKETIME(12,15,30.3000001) -> ...300000).
        Datum::Real(_) | Datum::Float32(_) => 6,
        Datum::Duration(value) => {
            usize::try_from(value.fsp()).expect("duration FSP is nonnegative")
        }
        Datum::Time(value) => usize::from(value.fsp()),
        Datum::Null => 0,
        Datum::MinNotNull | Datum::MaxValue => {
            return Err(EvalError::Unsupported("range sentinel duration argument"));
        }
        other => other
            .sql_string()
            .ok()
            .and_then(|text| text.split_once('.').map(|(_, f)| f.len().min(6)))
            .unwrap_or(0),
    })
}

fn format_duration(seconds: f64, fsp: usize) -> String {
    let sign = if seconds < 0.0 { "-" } else { "" };
    let max = 838.0 * 3600.0 + 59.0 * 60.0 + 59.0;
    let mut seconds = seconds.abs();
    if seconds > max {
        seconds = max;
    }
    let whole = seconds.trunc() as i64;
    let hour = whole / 3600;
    let minute = whole / 60 % 60;
    let mut second = whole % 60;
    if fsp == 0 {
        return format!("{sign}{hour:02}:{minute:02}:{second:02}");
    }
    // Go reaches this text through `fmt.Sprintf("%v", second)` followed by
    // `ParseDuration`, whose fraction rounding works on the DECIMAL DIGITS
    // and carries half-up at the requested precision
    // (`Duration.RoundFrac`: Go's time.Round rounds nearest values and sends
    // exact ties toward positive infinity).
    // Doing the arithmetic in f64 first re-derives 30.0000005 as
    // ...4999996µs and loses the digit -- so round off the SHORTEST decimal
    // rendering instead.
    // Go formats the REAL second value with %v (shortest repr): 30.1 stays
    // "30.1", 30.0000005 stays "30.0000005".
    let digits = format!("{seconds}");
    let mut digits_fraction = match digits.split_once('.') {
        Some((_, fraction)) => fraction.to_owned(),
        None => String::new(),
    };
    // Round half-up at the requested precision off the FIRST DISCARDED
    // digit, carrying into the whole part when the fraction overflows.
    let round_up = digits_fraction.len() > fsp && digits_fraction.as_bytes()[fsp] >= b'5';
    digits_fraction.truncate(fsp);
    while digits_fraction.len() < fsp {
        digits_fraction.push('0');
    }
    let mut fraction: i64 = digits_fraction.parse().unwrap_or(0);
    if round_up {
        fraction += 1;
        if fraction >= 10_i64.pow(fsp as u32) {
            fraction = 0;
            second += 1;
        }
    }
    format!("{sign}{hour:02}:{minute:02}:{second:02}.{fraction:0fsp$}")
}

/// `builtinMakeDateSig` in `pkg/expression/builtin_time.go`.
fn makedate(vals: &[Datum]) -> Result<Datum, EvalError> {
    if vals.len() != 2 {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    let (Some(mut year), Some(day)) = (int_arg(&vals[0])?, int_arg(&vals[1])?) else {
        return Ok(Datum::Null);
    };
    if day <= 0 || !(0..=9999).contains(&year) {
        return Ok(Datum::Null);
    }
    if year < 70 {
        year += 2000;
    } else if year < 100 {
        year += 1900;
    }
    let (result_y, result_m, result_d) = civil_from_days(days_from_civil(year, 1, 1) + day - 1);
    if !(1..=9999).contains(&result_y) {
        return Ok(Datum::Null);
    }
    Ok(Datum::new_string(format!(
        "{result_y:04}-{result_m:02}-{result_d:02}"
    )))
}

/// `builtinMakeTimeSig` in `pkg/expression/builtin_time.go`.
fn maketime(vals: &[Datum], cols: &dyn Columns) -> Result<Datum, EvalError> {
    if vals.len() != 3 {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    let (Some(mut hour), Some(minute), Some(second)) = (
        int_arg(&vals[0])?,
        int_arg(&vals[1])?,
        number_arg(&vals[2], cols)?,
    ) else {
        return Ok(Datum::Null);
    };
    if !(0..60).contains(&minute) || !(0.0..60.0).contains(&second) {
        return Ok(Datum::Null);
    }
    // Go's `makeTime` checks the argument FieldType's UnsignedFlag before it
    // interprets the signed value.  A UInt datum carrying a wrapped negative
    // hour (for example `CAST(-1 AS UNSIGNED)`) therefore clamps to the
    // positive TIME limit instead of producing a negative duration.  The
    // value-level evaluator has no separate FieldType parameter, so Datum::UInt
    // is the equivalent type signal here.
    let hour_unsigned = matches!(vals[0], Datum::UInt(_));
    let mut overflow = false;
    if hour < 0 && hour_unsigned {
        hour = 838;
        overflow = true;
    }
    let negative = hour < 0;
    let hour_abs = hour.unsigned_abs();
    if hour_abs > 838 || (hour_abs == 838 && minute == 59 && second > 59.0) {
        overflow = true;
    }
    let total = if overflow {
        838.0 * 3600.0 + 59.0 * 60.0 + 59.0
    } else {
        hour_abs as f64 * 3600.0 + minute as f64 * 60.0 + second
    };
    Ok(Datum::new_string(format_duration(
        if negative { -total } else { total },
        duration_precision(&vals[2])?,
    )))
}

/// Both original conversions run left-to-right even when the first is NULL.
/// Validation and wrapping arithmetic belong to the worker, so admission now
/// precedes invalid-period 1210 diagnostics; original coercion errors stay first.
fn period_in(
    vals: &[Datum],
    operation: crate::tikv::EvaluatedBytesOp,
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_args_in(
        operation,
        ctx,
        || {
            if vals.len() != 2 {
                return Err(EvalError::Unsupported("bad function arity"));
            }
            Ok(crate::tikv::EvaluatedArgs::Int2(
                int_arg(&vals[0])?,
                int_arg(&vals[1])?,
            ))
        },
        crate::tikv::EvaluatedBytesResult::into_int_datum,
    )
}

pub(crate) fn period_add_in(vals: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    period_in(vals, crate::tikv::EvaluatedBytesOp::PeriodAddNative, ctx)
}

pub(crate) fn period_diff_in(vals: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    period_in(vals, crate::tikv::EvaluatedBytesOp::PeriodDiffNative, ctx)
}

#[cfg(test)]
fn period_add(vals: &[Datum]) -> Result<Datum, EvalError> {
    period_add_in(vals, &crate::NoColumns)
}

#[cfg(test)]
fn period_diff(vals: &[Datum]) -> Result<Datum, EvalError> {
    period_diff_in(vals, &crate::NoColumns)
}

/// `builtinTimeFormatSig` in `pkg/expression/builtin_time.go`; shares the
/// `types.Duration.DurationFormat` specifier family with DATE_FORMAT.
fn time_format(vals: &[Datum]) -> Result<Datum, EvalError> {
    if vals.len() != 2 {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    let Some((seconds, fraction)) = duration(&vals[0])? else {
        return Ok(Datum::Null);
    };
    let Some(mask) = coerce_str(&vals[1])? else {
        return Ok(Datum::Null);
    };
    if mask.is_empty() {
        return Ok(Datum::Null);
    }
    let sign = if seconds < 0 { "-" } else { "" };
    let total = seconds.unsigned_abs();
    let hour = total / 3600;
    let minute = total / 60 % 60;
    let second = total % 60;
    let hour12 = (hour + 11) % 12 + 1;
    let mut out = String::new();
    let mut chars = mask.chars();
    while let Some(c) = chars.next() {
        if c != '%' {
            out.push(c);
            continue;
        }
        match chars.next() {
            None => out.push('%'),
            Some('H') => out.push_str(&format!("{sign}{hour:02}")),
            Some('k') => out.push_str(&format!("{sign}{hour}")),
            Some('h' | 'I') => out.push_str(&format!("{hour12:02}")),
            Some('l') => out.push_str(&hour12.to_string()),
            Some('i') => out.push_str(&format!("{minute:02}")),
            Some('S' | 's') => out.push_str(&format!("{second:02}")),
            Some('f') => out.push_str(&format!("{fraction:0<6}")),
            Some('p') => out.push_str(if hour < 12 { "AM" } else { "PM" }),
            Some('T') => out.push_str(&format!("{sign}{hour:02}:{minute:02}:{second:02}")),
            Some('r') => out.push_str(&format!(
                "{hour12:02}:{minute:02}:{second:02} {}",
                if hour < 12 { "AM" } else { "PM" }
            )),
            Some('%') => out.push('%'),
            Some(other) => out.push(other),
        }
    }
    Ok(Datum::new_string(out))
}

#[cfg(test)]
mod clock_source_tests {
    use std::cell::RefCell;

    use super::*;

    #[derive(Default)]
    struct WarningContext {
        warnings: RefCell<Vec<(u16, String)>>,
    }

    impl Columns for WarningContext {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }

        fn append_warning(&self, code: u16, message: &str) {
            self.warnings.borrow_mut().push((code, message.to_owned()));
        }
    }

    fn source_eval(name: &str, args: &[Datum], ctx: &WarningContext) -> Result<Datum, EvalError> {
        crate::func::eval_func_values_in(name, args, ctx)
            .or_else(|| dispatch(name, args, ctx))
            .expect("TestClock builtin must be dispatched")
    }

    fn string(value: &str) -> Datum {
        Datum::new_string(value.to_owned())
    }

    /// Go raises `types.ErrTooBigPrecision` (1426) at EVALUATION time for an
    /// fsp above `MaxFsp` (`builtin_time.go:2730` and siblings) -- a coded
    /// diagnostic, not the generic fallback, and NULL args still mean fsp 0.
    #[test]
    fn clock_fsp_above_max_reports_coded_1426() {
        let ctx = WarningContext::default();
        let err = source_eval("NOW", &[Datum::Int(7)], &ctx).unwrap_err();
        assert!(
            matches!(
                &err,
                EvalError::TooBigFsp {
                    fsp: 7,
                    function: "now"
                }
            ),
            "{err:?}"
        );
        let err = source_eval("CURTIME", &[Datum::Int(8)], &ctx).unwrap_err();
        assert!(
            matches!(
                &err,
                EvalError::TooBigFsp {
                    fsp: 8,
                    function: "curtime"
                }
            ),
            "{err:?}"
        );
    }

    /// Exact Go `TestClock`: HOUR, MINUTE, SECOND, MICROSECOND and TIME over
    /// its three source values, every NULL arm, and the malformed TIME warning.
    #[test]
    fn test_clock() {
        let ctx = WarningContext::default();
        for (input, hour, minute, second, micros, time) in [
            ("10:10:10.123456", 10, 10, 10, 123_456, "10:10:10.123456"),
            ("11:11:11.11", 11, 11, 11, 110_000, "11:11:11.11"),
            ("2010-10-10 11:11:11.11", 11, 11, 11, 110_000, "11:11:11.11"),
        ] {
            let args = [string(input)];
            assert_eq!(source_eval("HOUR", &args, &ctx).unwrap(), Datum::Int(hour));
            assert_eq!(
                source_eval("MINUTE", &args, &ctx).unwrap(),
                Datum::Int(minute)
            );
            assert_eq!(
                source_eval("SECOND", &args, &ctx).unwrap(),
                Datum::Int(second)
            );
            assert_eq!(
                source_eval("MICROSECOND", &args, &ctx).unwrap(),
                Datum::Int(micros)
            );
            assert_eq!(source_eval("TIME", &args, &ctx).unwrap(), string(time));
        }

        for name in ["HOUR", "MINUTE", "SECOND", "MICROSECOND", "TIME"] {
            assert_eq!(
                source_eval(name, &[Datum::Null], &ctx).unwrap(),
                Datum::Null
            );
        }

        let malformed = [string("2011-11-11 10:10:10.11.12")];
        for name in ["HOUR", "MINUTE", "SECOND", "MICROSECOND"] {
            assert_eq!(source_eval(name, &malformed, &ctx).unwrap(), Datum::Null);
        }
        let warning_count = ctx.warnings.borrow().len();
        assert_eq!(
            source_eval("TIME", &malformed, &ctx).unwrap(),
            string("00:00:00")
        );
        assert_eq!(ctx.warnings.borrow().len(), warning_count + 1);
        assert_eq!(
            ctx.warnings.borrow().last(),
            Some(&(
                1292,
                "Truncated incorrect time value: '2011-11-11 10:10:10.11.12'".to_owned()
            ))
        );
    }
}

#[cfg(test)]
mod tests;
