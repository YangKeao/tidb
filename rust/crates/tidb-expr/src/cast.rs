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

//! `CAST(expr AS type)` / `CONVERT(expr, type)` evaluation
//! ([`eval_cast`], dispatched from `crate::eval_in`'s `Expr::Cast` arm) and
//! `CONVERT(expr USING charset)` (a plain stringification passthrough,
//! handled directly in `crate::eval_in`'s own `Expr::ConvertUsing` arm —
//! this crate has no charset domain at all).
//!
//! JSON and DATE/DATETIME targets retain their native datum domains. Every
//! rule here (string-to-number prefix parsing width, rounding tie-breaking per
//! source type, `UNSIGNED`'s negative-float-clamps-to-zero rule, `DECIMAL`'s
//! precision clamp, `BINARY`'s NUL-padding) was confirmed via `goeval`, not
//! assumed — see each function's own doc for the specific probe.

use crate::coerce::coerce_str;
use crate::Decimal;
use crate::{Datum, EvalError};
use tidb_ast::CastType;
use tidb_datatype::{
    find_encoding, number_to_duration, ConversionFlags, DatumValueError, EvalType, FieldType,
    FieldTypeCode, ScalarConversionEvent, TransformOp, JSON_TYPE_CODE_DATE,
    JSON_TYPE_CODE_DATETIME, JSON_TYPE_CODE_DURATION, JSON_TYPE_CODE_STRING,
    JSON_TYPE_CODE_TIMESTAMP,
};

fn parse_date_ymd(input: &str) -> Option<(i64, u32, u32)> {
    let input = input.trim();
    let date = input
        .split_once(char::is_whitespace)
        .map_or(input, |(date, _)| date);
    let bare = matches!(date.len(), 6 | 8) && date.bytes().all(|byte| byte.is_ascii_digit());
    let (year, month, day) = if bare {
        let year_digits = date.len() - 4;
        let (year, rest) = date.split_at(year_digits);
        let (month, day) = rest.split_at(2);
        (
            expand_date_year(year.parse().ok()?, year_digits),
            month.parse().ok()?,
            day.parse().ok()?,
        )
    } else {
        let parts = split_date_components(date)?;
        let [(year, year_digits), (month, _), (day, _)] = parts.as_slice() else {
            return None;
        };
        (expand_date_year(*year, *year_digits), *month, *day)
    };
    if !(1..=12).contains(&month) || day == 0 || day > days_in_date_month(year, month) {
        return None;
    }
    Some((year, month, day))
}

fn split_date_components(input: &str) -> Option<Vec<(u32, usize)>> {
    let mut parts = Vec::new();
    let mut current = String::new();
    for character in input.chars() {
        if character.is_ascii_digit() {
            current.push(character);
        } else {
            if current.is_empty() {
                return None;
            }
            parts.push((current.parse().ok()?, current.len()));
            current.clear();
        }
    }
    if current.is_empty() {
        return None;
    }
    parts.push((current.parse().ok()?, current.len()));
    (parts.len() == 3).then_some(parts)
}

fn expand_date_year(value: u32, digits: usize) -> i64 {
    if digits > 2 {
        i64::from(value)
    } else if value <= 69 {
        2000 + i64::from(value)
    } else {
        1900 + i64::from(value)
    }
}

fn days_in_date_month(year: i64, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if (year % 4 == 0 && year % 100 != 0) || year % 400 == 0 => 29,
        2 => 28,
        _ => 0,
    }
}

/// `CAST(expr AS TIME[(fsp)])` returns TiDB's elapsed-time datum, not a
/// calendar string. The source type selects the Go signature: numeric inputs
/// turn a truncation/overflow into `NULL`, while string inputs retain the
/// parser's best-effort duration after reporting the truncation event.
fn cast_to_duration(
    v: &Datum,
    source: Option<&tidb_datatype::FieldType>,
    ctx: &dyn crate::Columns,
    fsp: i64,
) -> Result<Datum, EvalError> {
    let input = v.sql_string().unwrap_or_else(|_| "<binary>".to_owned());
    let target = FieldType::new(FieldTypeCode::Duration).with_decimal(fsp);
    let source_eval_type = source.map(FieldType::eval_type);

    if let Datum::Json(value) = v {
        if !matches!(
            value.type_code(),
            JSON_TYPE_CODE_DATE
                | JSON_TYPE_CODE_DATETIME
                | JSON_TYPE_CODE_TIMESTAMP
                | JSON_TYPE_CODE_DURATION
                | JSON_TYPE_CODE_STRING
        ) {
            ctx.handle_truncate(&format!(
                "Truncated incorrect time value: '{}'",
                tidb_datatype::warning_subject_byte_cap(&input)
            ))?;
            return Ok(Datum::Null);
        }
    }

    let numeric = matches!(
        source_eval_type,
        Some(EvalType::Int | EvalType::Real | EvalType::Decimal)
    ) || (source_eval_type.is_none()
        && matches!(
            v,
            Datum::Int(_) | Datum::UInt(_) | Datum::Real(_) | Datum::Float32(_) | Datum::Decimal(_)
        ));
    let converted = if source_eval_type == Some(EvalType::Int)
        || (source_eval_type.is_none() && matches!(v, Datum::Int(_) | Datum::UInt(_)))
    {
        let number = match v {
            Datum::Int(value) => *value,
            // Go's ETInt ABI is `int64`: an unsigned source reaches this
            // signature through the same low-64-bit representation.
            Datum::UInt(value) => *value as i64,
            _ => return Err(EvalError::Unsupported("CAST AS TIME integer datum")),
        };
        number_to_duration(number, fsp)
            .map(|converted| (Datum::new_duration(converted.value), converted.event))
            .map_err(|error| DatumValueError::Comparison(error.to_string()))
    } else {
        v.convert_to_in(&target, ConversionFlags::default(), &ctx.time_zone())
            .map(|converted| (converted.value, converted.event))
    };

    match converted {
        Ok((value, None | Some(ScalarConversionEvent::RoundedToScale))) => Ok(value),
        Ok((value, Some(_))) => {
            ctx.handle_truncate(&format!(
                "Truncated incorrect time value: '{}'",
                tidb_datatype::warning_subject_byte_cap(&input)
            ))?;
            Ok(if numeric { Datum::Null } else { value })
        }
        Err(DatumValueError::Unsupported(_, _)) => {
            Err(EvalError::Unsupported("CAST AS TIME source datum"))
        }
        Err(_) => {
            ctx.handle_truncate(&format!(
                "Truncated incorrect time value: '{}'",
                tidb_datatype::warning_subject_byte_cap(&input)
            ))?;
            Ok(Datum::Null)
        }
    }
}

/// Go `WrapWithCastAsDuration` applied to one builtin argument value.
///
/// A duration expression is already in the requested domain. Calendar values
/// preserve their declared fractional precision; every other source receives
/// Go's `MaxFsp` target before the cast signature parses it.
pub(crate) fn cast_arg_as_duration(
    value: &Datum,
    source: Option<&tidb_datatype::FieldType>,
    ctx: &dyn crate::Columns,
) -> Result<Datum, EvalError> {
    if matches!(value, Datum::Duration(_) | Datum::Null) {
        return Ok(value.clone());
    }
    let fsp = source
        .filter(|field_type| {
            matches!(
                field_type.code(),
                FieldTypeCode::Date | FieldTypeCode::Datetime | FieldTypeCode::Timestamp
            )
        })
        .map_or(6, FieldType::decimal);
    cast_to_duration(value, source, ctx, fsp)
}

/// Go `types.ProduceStrWithSpecifiedTp` (`pkg/types/datum.go:1289-1304`),
/// warning half: a value the target width cannot hold raises
/// `ErrDataTooLong` (1406) "Data Too Long, field len %d, data len %d".
///
/// `data_len` is what Go's `characterLen` counts, which is the SOURCE's own
/// length in the target's unit -- runes for a character target, bytes for a
/// binary one -- NOT the truncated result's. Captured:
/// `CAST('中文abc' AS CHAR(2))` warns `field len 2, data len 5` while
/// `CAST('中文abc' AS BINARY(4))` warns `field len 4, data len 9`.
///
/// Go's one exception, the whitespace-only overflow that downgrades to a
/// 1265 `Data truncated`, needs `tp.GetType() == TypeVarchar`; a CAST target
/// is `TypeVarString`, so it cannot apply here. Captured:
/// `CAST('ab   ' AS CHAR(2))` warns 1406, not 1265.
/// `SIGNED`'s own coercion: `Int` is unchanged; `Decimal`/`Float` round to
/// the nearest integer (ties away from zero for `Decimal`, ties to EVEN
/// for `Float` — a real asymmetry, matching the `~` bitwise operator's own
/// established rule, confirmed via `goeval`: `CAST(2.5e0 AS SIGNED)` is
/// `2`, `CAST(1.5 AS SIGNED)` — a `DECIMAL` literal — is also `2`, but
/// `CAST(0.5e0 AS SIGNED)` is `0`, the even neighbor); either CLAMPS
/// (never errors) on overflow past `i64`, confirmed via `goeval`:
/// `CAST(1e300 AS SIGNED)` is `9223372036854775807`. `Str` parses a
/// leading `[+-]?digits` prefix ONLY (no `.`, no exponent — confirmed via
/// `goeval`: `CAST('3.5abc' AS SIGNED)` sees just `3`, `CAST('.5' AS
/// SIGNED)` sees no digits at all), defaulting to `0` if no digit is
/// found. A nonnegative integer prefix is parsed through the full `u64`
/// domain and then converted to `i64`, preserving Go's negative-complement
/// result above `i64::MAX`; true `u64` overflow returns `-1`. Negative
/// overflow clamps to `i64::MIN`.
pub(crate) fn to_i64_signed(v: &Datum) -> i64 {
    to_i64_signed_in(v, &tidb_datatype::SessionTimeZone::utc())
}

/// [`to_i64_signed`] with the session's `time_zone`, which Go's
/// `toSignedInteger` hands to `Time.RoundFrac` -- load-bearing only when a
/// DATETIME's fractional carry lands on a DST transition instant.
pub(crate) fn to_i64_signed_in(v: &Datum, zone: &tidb_datatype::SessionTimeZone) -> i64 {
    match v {
        Datum::Int(i) => *i,
        Datum::UInt(i) => *i as i64,
        Datum::Decimal(d) => d.round_to_i64_saturating(),
        Datum::Real(f) => f.round_ties_even() as i64,
        Datum::String(s) => s.as_utf8().map(str_int_prefix).unwrap_or(0),
        Datum::Bytes(s) => std::str::from_utf8(s).map(str_int_prefix).unwrap_or(0),
        Datum::Null | Datum::MinNotNull | Datum::MaxValue => unreachable!("guarded by caller"),
        other => other.to_i64_in(zone).map_or(0, |converted| converted.value),
    }
}

/// Scans a MySQL-style INTEGER numeric prefix: optional leading
/// whitespace, optional sign, then a run of ASCII digits — stopping at
/// the first non-digit (no `.`, no exponent; see [`to_i64_signed`]'s own
/// doc for the confirming probe). `0` if no digit is found. Saturates to
/// `i64::MIN`/`MAX` on overflow rather than replicating real TiDB's own
/// exotic bit-reinterpretation for a string whose digit run exceeds even
/// `u64` range (confirmed via `goeval`: `CAST('99999999999999999999' AS
/// SIGNED)` — twenty `9`s — is `-1` in real TiDB, a `u64::MAX` value
/// bit-reinterpreted as `i64`; this project deliberately does not
/// replicate that, saturating to `i64::MAX` instead — a principled,
/// documented divergence for a value nobody writes intentionally, not an
/// oversight).
/// Go `types.getValidIntPrefix`'s `isFuncCast` arm, reporting ONLY whether
/// the scan consumed the whole string. Go scans BYTES and advances the valid
/// length only on a digit, so a lone sign leaves length zero:
/// `[+-]?` at offset 0 is skipped without counting, every following ASCII
/// digit sets the length to `i + 1`, and the first other byte stops the scan.
///
/// Returned separately from [`str_int_prefix`] because the two answers have
/// different lifetimes in Go too: the prefix VALUE is returned to the caller
/// unconditionally, while the truncation event goes through
/// `Context.HandleTruncate` and may be discarded, warned, or raised.
fn int_prefix_consumed_all(s: &str) -> bool {
    // Go `StrToInt`/`StrToUint` trim BOTH ends before scanning, so trailing
    // space is not a truncation; `CAST('  12  ' AS SIGNED)` is exact.
    let trimmed = s.trim();
    let mut valid_len = 0;
    for (i, byte) in trimmed.bytes().enumerate() {
        if (byte == b'+' || byte == b'-') && i == 0 {
            continue;
        }
        if byte.is_ascii_digit() {
            valid_len = i + 1;
            continue;
        }
        break;
    }
    valid_len != 0 && valid_len == trimmed.len()
}

/// Only a string-valued operand reaches Go's `builtinCastStringAsIntSig`;
/// the numeric signatures have their own, overflow-shaped diagnostic, in
/// [`report_signed_overflow`].
pub(crate) fn report_int_truncation(v: &Datum, ctx: &dyn crate::Columns) -> Result<(), EvalError> {
    let text = match v {
        Datum::String(value) => value.as_utf8().ok(),
        Datum::Bytes(value) => std::str::from_utf8(value).ok(),
        _ => None,
    };
    match text {
        Some(text)
            if !int_prefix_consumed_all(text) || signed_string_integer_parse_overflows(text) =>
        {
            ctx.handle_truncate(&format!(
                "Truncated incorrect INTEGER value: '{}'",
                tidb_datatype::warning_subject_byte_cap(text.trim())
            ))
        }
        _ => Ok(()),
    }
}

fn signed_string_integer_parse_overflows(text: &str) -> bool {
    if !int_prefix_consumed_all(text) {
        return false;
    }
    let trimmed = text.trim();
    if trimmed.starts_with('-') {
        trimmed.parse::<i64>().is_err()
    } else {
        trimmed
            .strip_prefix('+')
            .unwrap_or(trimmed)
            .parse::<u64>()
            .is_err()
    }
}

fn str_int_prefix(s: &str) -> i64 {
    let s = s.trim_start();
    let (negative, rest) = match s.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
    if digits.is_empty() {
        return 0;
    }
    if negative {
        format!("-{digits}").parse::<i64>().unwrap_or(i64::MIN)
    } else {
        digits.parse::<u64>().map_or(-1, |value| value as i64)
    }
}

/// `CHAR(N)`'s own truncation is handled inline in [`eval_cast`] (keeps
/// the first `N` characters, never pads); this is `BINARY(N)`'s own
/// FIXED-WIDTH behavior — truncates the same way if longer, but PADS
/// with `\0` bytes if shorter, confirmed via `goeval`:
/// `CAST('hi' AS BINARY(5))` is `"hi\0\0\0"`, 5 bytes exactly. MySQL
/// `BINARY` counts BYTES, not characters; the byte-preserving `Datum::Bytes`
/// result keeps the same behavior for non-UTF-8 truncation boundaries too.
fn truncate_clock_for_date(
    mut time: tidb_datatype::Time,
    kind: tidb_datatype::TimeType,
) -> tidb_datatype::Time {
    if kind != tidb_datatype::TimeType::Date {
        return time;
    }
    let core = time.core_time();
    time.set_core_time(tidb_datatype::CoreTime::from_date(
        core.year() as u16,
        core.month(),
        core.day(),
        0,
        0,
        0,
        0,
    ));
    time
}

/// The `types.Time` Go's chosen `builtinCast*AsTimeSig` produces. `None` is
/// Go's NULL (any warning already raised). Both explicit CAST and the
/// argument-cast seam retain this native temporal value.
fn cast_to_time_value(
    v: &Datum,
    source: Option<&tidb_datatype::FieldType>,
    ctx: &dyn crate::Columns,
    kind: tidb_datatype::TimeType,
    fsp: Option<i64>,
) -> Result<Option<tidb_datatype::Time>, EvalError> {
    // A `YEAR` source is the one case the datum kind cannot speak for. Go
    // `builtinCastIntAsTimeSig.evalTime` (`builtin_cast.go:1127-1131`) asks the
    // ARGUMENT'S TYPE, not the integer's digits:
    //
    //   if b.args[0].GetType(ctx).GetType() == mysql.TypeYear {
    //       res, err = types.ParseTimeFromYear(val)
    //   } else {
    //       res, err = types.ParseTimeFromNum(typeCtx(ctx), val, ...)
    //   }
    //
    // and `types.ParseTimeFromYear` (`time.go:2072-2081`) INJECTS the value as
    // the year FIELD -- `FromDate(int(year), 0, 0, 0, 0, 0, 0)`, so `2018` is
    // `2018-00-00 00:00:00` -- with `0` mapping to the zero date typed
    // `mysql.TypeDate`. Routing that same `2018` through `ParseTimeFromNum`,
    // which reads an int as a packed `YYYYMMDD`, FAILS and yields NULL. Every
    // other INT source keeps `ParseTimeFromNum` below.
    if let Some(year) = year_source_value(v, source) {
        let time = tidb_datatype::parse_time_from_year(year)
            .map_err(|_| EvalError::Unsupported("a YEAR value outside the year range"))?;
        return Ok(Some(time));
    }
    // A DURATION source is the second kind whose text cannot speak for it. Go
    // `builtinCastDurationAsTimeSig.evalTime` (`builtin_cast.go:2275-2291`)
    // never parses `20:00:01` as a wall clock; it calls
    // `val.ConvertToTimeWithTimestamp(tc, b.tp.GetType(), ts)`, which takes
    // the CALENDAR DATE of the statement's own timestamp and mixes the
    // elapsed time into it (`types/time.go:1500-1507`). Routing the text
    // through `ParseTime` instead reads the `20` as a YEAR.
    //
    // Neither half of this is visible in the recorded corpus: every recorded
    // statement that reaches it has the OTHER argument winning, so any wrong
    // conversion still prints the recorded answer. Both are pinned by
    // `a_duration_beside_a_temporal_literal_lands_on_the_statement_date` in
    // `tidb-session`, which puts the duration on the winning side and then
    // moves the session zone across the date line.
    //
    // The two date-mode flags SURVIVED their own mutation (hardcoding both to
    // `false` moves nothing): `mixDateAndDuration` always starts from a real
    // calendar date, so no zero or invalid component can arise for them to
    // rule on. They are passed because Go passes its `ctx`, not because a
    // value distinguishes them.
    if let Datum::Duration(duration) = v {
        let modes = ctx.date_modes();
        let (utc_secs, nanos, tz_offset) = ctx
            .now()
            .ok_or(EvalError::Unsupported("no statement clock for a TIME cast"))?;
        // Go reads the calendar date of `ts.In(ctx.Location())`; `now`'s third
        // field is that location's offset AT that instant, so a fixed offset
        // names the same civil day without re-resolving the zone.
        let zone = chrono::FixedOffset::east_opt(tz_offset).ok_or(EvalError::Unsupported(
            "session time-zone offset out of range",
        ))?;
        let Some(stamp) = chrono::DateTime::from_timestamp(utc_secs, nanos) else {
            return Ok(None);
        };
        return Ok(duration
            .convert_to_time(
                stamp.with_timezone(&zone),
                kind,
                !modes.no_zero_in_date,
                modes.allow_invalid_dates,
            )
            .and_then(|time| match fsp {
                Some(fsp) => time.round_frac(fsp, &ctx.time_zone()),
                None => Ok(time),
            })
            .ok());
    }
    let Some(s) = coerce_str(v)? else {
        return Ok(None);
    };
    let modes = ctx.date_modes();
    // Go routes each source TYPE to its own parser, not its text: only the
    // STRING/BYTES signatures (`builtinCastStringAsTimeSig`) parse the wall-
    // clock text through `ParseTime`. An INT source takes `ParseTimeFromNum`,
    // and a REAL/DECIMAL source takes `ParseTimeFromFloatString`, both of which
    // read the value as TiDB's packed `YYYYMMDD[HHMMSS]` NUMBER -- not as a
    // free-form date string. Funnelling a decimal through the string parser is
    // what made `cast(121212.1111 as datetime)` absorb `.1111` as a clock
    // (`2012-12-12 11:11:00`) and `cast(111.1 as datetime)` fail outright,
    // where TiDB answers `2012-12-12 00:00:00` and `2000-01-11 00:00:00`
    // (`expression/cast`). The parser choice mirrors `Datum::convert_to_time`,
    // the faithful write-path port.
    let parsed = parse_time_by_source(
        v,
        &s,
        kind,
        fsp,
        modes.allow_invalid_dates,
        &ctx.time_zone(),
    );
    let Ok((time, truncated, dst_adjusted)) = parsed else {
        invalid_time_warning(ctx, &s);
        return Ok(None);
    };
    if truncated {
        ctx.append_warning(
            1292,
            &format!(
                "Truncated incorrect datetime value: '{}'",
                tidb_datatype::warning_subject_byte_cap(&s)
            ),
        );
    }
    if dst_adjusted {
        ctx.append_warning(
            8179,
            &format!(
                "Timestamp is not valid, since it is in Daylight Saving Time transition '{}' for time zone '{:?}'",
                s,
                ctx.time_zone(),
            ),
        );
    }
    // Go's SECOND check is the STRING signature's ALONE
    // (`builtinCastStringAsTimeSig`: `res.IsZero() && HasNoZeroDateMode()`).
    // The INT/REAL/DECIMAL signatures have no such rejection -- a numeric zero
    // is the zero time, not NULL (Go `#11203`), so `cast(0 as datetime)` and a
    // `0`-valued double/decimal column read `0000-00-00 00:00:00`, matching
    // `expression/cast`. Gating this on the text sources keeps the numeric
    // sources on Go's own no-rejection path.
    if matches!(v, Datum::String(_) | Datum::Bytes(_)) && time.is_zero() && modes.no_zero_date {
        invalid_time_warning(ctx, &s);
        return Ok(None);
    }
    Ok(Some(truncate_clock_for_date(time, kind)))
}

pub(crate) fn parse_computed_time(
    value: &Datum,
    ctx: &dyn crate::Columns,
    kind: tidb_datatype::TimeType,
    fsp: Option<i64>,
) -> Result<Datum, EvalError> {
    Ok(cast_to_time_value(value, None, ctx, kind, fsp)?.map_or(Datum::Null, Datum::Time))
}

fn year_source_value(v: &Datum, source: Option<&tidb_datatype::FieldType>) -> Option<i64> {
    if source?.code() != tidb_datatype::FieldTypeCode::Year {
        return None;
    }
    match v {
        Datum::Int(value) => Some(*value),
        Datum::UInt(value) => i64::try_from(*value).ok(),
        _ => None,
    }
}

/// Parses one cast operand into a `Time`, choosing the parser by SOURCE TYPE
/// the way Go's per-signature `builtinCast*AsTimeSig` split does (see
/// [`cast_to_time`]'s doc for why the text is not enough). The read-path flags
/// are the string signature's own: `allow_zero_in_date` is UNCONDITIONALLY
/// `true` (a SELECT reads a zero-in-date back intact), and `allow_invalid_date`
/// follows `ALLOW_INVALID_DATES`. `Err(())` is Go's parse failure, which the
/// caller turns into a 1292 warning plus NULL.
///
/// The parser routing mirrors `Datum::convert_to_time`, the faithful write-path
/// port: INT/UINT -> `parse_time_from_num`, DECIMAL -> `parse_time_from_decimal`,
/// REAL/FLOAT -> `parse_time_from_float64`. A UINT beyond `i64::MAX` cannot be a
/// packed datetime and is a parse failure. The float/decimal parsers classify
/// DATE-vs-DATETIME by digit count, so the target `kind` is re-imposed with
/// `set_kind` -- exactly what `convert_to_time` does after those two parsers.
fn parse_time_by_source(
    v: &Datum,
    text: &str,
    kind: tidb_datatype::TimeType,
    fsp: Option<i64>,
    allow_invalid: bool,
    zone: &tidb_datatype::SessionTimeZone,
) -> Result<(tidb_datatype::Time, bool, bool), ()> {
    match v {
        Datum::Int(value) => tidb_datatype::parse_time_from_num(
            *value,
            kind,
            fsp.unwrap_or(0),
            true,
            allow_invalid,
            true,
            zone,
        )
        .map(|parsed| (parsed.time, false, parsed.dst_adjusted))
        .map_err(|_| ()),
        Datum::UInt(value) => {
            let signed = i64::try_from(*value).map_err(|_| ())?;
            tidb_datatype::parse_time_from_num(
                signed,
                kind,
                fsp.unwrap_or(0),
                true,
                allow_invalid,
                true,
                zone,
            )
            .map(|parsed| (parsed.time, false, parsed.dst_adjusted))
            .map_err(|_| ())
        }
        Datum::Decimal(value) => {
            let mut time = tidb_datatype::parse_time_from_decimal(value, true, allow_invalid, zone)
                .map_err(|_| ())?;
            time.set_kind(kind);
            match fsp {
                Some(fsp) => time
                    .round_frac(fsp, zone)
                    .map(|time| (time, false, false))
                    .map_err(|_| ()),
                None => Ok((time, false, false)),
            }
        }
        Datum::Real(value) => real_to_time(*value, kind, fsp.unwrap_or(0), allow_invalid, zone)
            .map(|time| (time, false, false)),
        Datum::Float32(value) => real_to_time(*value, kind, fsp.unwrap_or(0), allow_invalid, zone)
            .map(|time| (time, false, false)),
        Datum::Time(value) => {
            let mut time = *value;
            time.set_kind(kind);
            match fsp {
                Some(fsp) => time
                    .round_frac(fsp, zone)
                    .map(|time| (time, false, false))
                    .map_err(|_| ()),
                None => Ok((time, false, false)),
            }
        }
        // STRING/BYTES and every other coercible source keep Go's
        // `builtinCastStringAsTimeSig` path: parse the wall-clock TEXT.
        //
        // The zone is the SESSION's, as Go's `builtinCastStringAsTimeSig` passes
        // `ctx.TypeCtx()`. It does more than TIMESTAMP's range check: a literal
        // whose fraction is wider than `fsp` ROUNDS, and Go applies that carry to
        // the INSTANT in `ctx.Location()`, so a carry landing on a DST transition
        // moves the wall clock by the offset change too. CAPTURED from real TiDB:
        // `cast('2011-03-13 01:59:59.9999999' as datetime)` is
        // `2011-03-13 02:00:00` under `time_zone='UTC'` and `03:00:00` under
        // `'America/Los_Angeles'` (02:00 does not exist there), and
        // `cast('2011-11-06 01:59:59.9999999' as datetime)` is `02:00:00` under
        // UTC and `01:00:00` there (the repeated hour). Hardcoding UTC returned
        // the UTC answer for every session.
        _ => tidb_datatype::parse_time(
            text,
            kind,
            fsp.unwrap_or_else(|| i64::from(tidb_datatype::get_fsp(text))),
            false,
            true,
            allow_invalid,
            zone,
        )
        .map(|parsed| (parsed.time, parsed.truncated, parsed.dst_adjusted))
        .map_err(|_| ()),
    }
}

/// REAL/FLOAT source shared by `Real` and `Float32`: Go
/// `builtinCastRealAsTimeSig` reads the float's packed-number form. `0.0` is
/// the zero time rather than a failure (Go's `#11203` guard); the float parser
/// already returns the zero time for a `0` integer part, so no special case is
/// needed here.
fn real_to_time(
    value: f64,
    kind: tidb_datatype::TimeType,
    fsp: i64,
    allow_invalid: bool,
    zone: &tidb_datatype::SessionTimeZone,
) -> Result<tidb_datatype::Time, ()> {
    let mut time =
        tidb_datatype::parse_time_from_float64(value, true, allow_invalid, zone).map_err(|_| ())?;
    time.set_kind(kind);
    time.round_frac(fsp, zone).map_err(|_| ())
}

/// Go `handleInvalidTimeError` on the read path: `ErrWrongValue` (1292)
/// becomes a warning and the cast yields NULL.
fn invalid_time_warning(ctx: &dyn crate::Columns, input: &str) {
    ctx.append_warning(1292, &format!("Incorrect datetime value: '{input}'"));
}
