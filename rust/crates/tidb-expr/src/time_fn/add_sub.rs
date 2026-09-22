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

//! `ADDTIME`, `SUBTIME` and `TIMESTAMP`, from
//! `pkg/expression/builtin_time.go`.
//!
//! # What makes these three one module
//!
//! Go picks their SIGNATURE from the argument `FieldType`s at build time.
//! `addTimeFunctionClass.getFunction` is a twelve-way switch over the
//! `(tp1, tp2)` cross product, and the arms differ in more than bookkeeping:
//! a DATETIME second argument makes the whole call NULL whatever the values
//! are, and the result fsp comes from a different operand in each arm.
//! [`TemporalKind`] is that switch, and [`add_sub_time`] is the twelve arms.
//!
//! # The two tiers, and Go's own row/vec split
//!
//! Go carries TWO bodies per signature: `evalString`/`evalTime` (the row
//! path, which is also what CONSTANT FOLDING runs) and `vecEvalString`
//! (the vectorized path a real column takes). They are not the same
//! function, and the difference is observable. Captured:
//!
//! ```text
//! -- both operands constant, so Go folds and takes the ROW path
//! select addtime('2020-01-01 10:00:00','2020-01-01 10:00:00')  NULL
//! -- the same values in a VARCHAR column, so Go takes the VEC path
//! select addtime(a,b) from u  -- a=b='2020-01-01 10:00:00'     2020-01-01 20:00:00
//! ```
//!
//! `builtinAddStringAndStringSig.evalString` ends with a `parser.Number` /
//! `parser.Char('-')` guard that nulls a second argument shaped
//! `<digits>-<more>`; `builtinAddStringAndStringSig.vecEvalString`
//! (`builtin_time_vec_generated.go:370`) simply does not have it. SUBTIME's
//! row body does not have it either, which is why the same pair of constants
//! answers a real value under `SUBTIME`. [`add_sub_time`]'s `row_path` flag
//! is that guard, and nothing else.

use tidb_datatype::{Datum, FieldType, FieldTypeCode};

use super::duration_parse::{
    fsp_for_time_add_sub, get_fsp, is_duration, parse_datetime, parse_duration, GoDateTime,
    GoDuration, Truncated, MAX_FSP, MIN_FSP,
};
use crate::coerce::coerce_str;
use crate::{Columns, EvalError};

/// The three temporal branches `getBf4TimeAddSub` reads off an argument's
/// `FieldType`, plus the `default` arm that covers everything else.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TemporalKind {
    /// `mysql.TypeDatetime` / `mysql.TypeTimestamp`.
    Datetime,
    /// `mysql.TypeDate`.
    Date,
    /// `mysql.TypeDuration`.
    Duration,
    /// Go's `default`: a string, a number, anything else.
    Other,
}

/// The argument branch, taken from the static `FieldType` where the chunk
/// tier has one and from the DATUM otherwise. The AST tier has no field
/// types at all, so a plain string literal lands on `Other` -- which is the
/// arm Go itself selects for a string constant.
pub(crate) fn kind_of(field_type: Option<&FieldType>, value: &Datum) -> TemporalKind {
    if let Some(ft) = field_type {
        return match ft.code() {
            FieldTypeCode::Datetime | FieldTypeCode::Timestamp => TemporalKind::Datetime,
            FieldTypeCode::Date | FieldTypeCode::NewDate => TemporalKind::Date,
            FieldTypeCode::Duration => TemporalKind::Duration,
            _ => TemporalKind::Other,
        };
    }
    match value {
        Datum::Time(_) => TemporalKind::Datetime,
        Datum::Duration(_) => TemporalKind::Duration,
        _ => TemporalKind::Other,
    }
}

fn truncated_time_warning(cols: &dyn Columns, value: &str) -> Datum {
    cols.append_warning(
        1292,
        &format!(
            "Truncated incorrect time value: '{}'",
            tidb_datatype::warning_subject_byte_cap(value)
        ),
    );
    Datum::Null
}

/// `ADDTIME`/`SUBTIME` where no static argument type is available: the AST
/// tier, and the chunk tier's fallback. Both arguments take Go's `default`
/// branch unless the DATUM itself is temporal, and the ROW body applies --
/// which is the body Go's constant folding runs for a literal call.
pub(crate) fn add_sub_untyped(
    name: &str,
    vals: &[Datum],
    cols: &dyn Columns,
) -> Result<Datum, EvalError> {
    if vals.len() != 2 {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    let kinds = [kind_of(None, &vals[0]), kind_of(None, &vals[1])];
    let sign = if name.eq_ignore_ascii_case("SUBTIME") {
        -1
    } else {
        1
    };
    add_sub_time(vals, kinds, sign, true, cols)
}

pub(crate) fn date_add_duration(
    unit: &str,
    date: &Datum,
    amount: &Datum,
    amount_type: Option<&FieldType>,
    sign: i64,
    result_fsp: i64,
) -> Result<Datum, EvalError> {
    let Datum::Duration(date) = date else {
        return if matches!(date, Datum::Null) {
            Ok(Datum::Null)
        } else {
            Err(EvalError::Unsupported("DATE_ADD duration operand"))
        };
    };
    let upper = unit.to_ascii_uppercase();
    let interval = if let Some((index, count)) = super::calendar::composite_spec(&upper) {
        let text = match amount {
            Datum::Null => return Ok(Datum::Null),
            Datum::Decimal(value) => {
                super::calendar::format_decimal_composite_interval(&upper, value)
            }
            Datum::Real(value) => amount_type
                .map(FieldType::decimal)
                .filter(|decimal| *decimal >= 0)
                .map_or_else(
                    || value.to_string(),
                    |decimal| format!("{value:.decimal$}", decimal = decimal as usize),
                ),
            Datum::Float32(value) => amount_type
                .map(FieldType::decimal)
                .filter(|decimal| *decimal >= 0)
                .map_or_else(
                    || value.to_string(),
                    |decimal| format!("{value:.decimal$}", decimal = decimal as usize),
                ),
            _ => match coerce_str(amount)? {
                Some(value) => value,
                None => return Ok(Datum::Null),
            },
        };
        let (years, months, _, _) = super::calendar::parse_composite_value(index, count, &text);
        if years != 0 || months != 0 {
            return Ok(Datum::Null);
        }
        match tidb_datatype::extract_duration_value(&upper, &text) {
            Ok(value) => value,
            Err(_) => return Ok(Datum::Null),
        }
    } else {
        let nanos = match upper.as_str() {
            "MICROSECOND" => super::calendar::whole_interval_amount(&upper, amount)?
                .and_then(|value| value.checked_mul(1_000)),
            "SECOND" => super::calendar::second_interval_micros(amount)?
                .and_then(|value| value.checked_mul(1_000)),
            "MINUTE" => super::calendar::whole_interval_amount(&upper, amount)?
                .and_then(|value| value.checked_mul(60_000_000_000)),
            "HOUR" => super::calendar::whole_interval_amount(&upper, amount)?
                .and_then(|value| value.checked_mul(3_600_000_000_000)),
            _ => return Ok(Datum::Null),
        };
        let Some(nanos) =
            nanos.filter(|value| value.unsigned_abs() <= tidb_datatype::MAX_TIME_NANOS as u64)
        else {
            return Ok(Datum::Null);
        };
        tidb_datatype::MySqlDuration::from_raw_parts(nanos, result_fsp)
    };
    let value = if sign < 0 {
        date.checked_sub(interval)
    } else {
        date.checked_add(interval)
    };
    Ok(match value {
        Ok(value) => Datum::new_duration(tidb_datatype::MySqlDuration::from_raw_parts(
            value.nanoseconds(),
            result_fsp,
        )),
        Err(_) => Datum::Null,
    })
}

/// Go's `getBf4TimeAddSub` + `addTimeFunctionClass.getFunction` /
/// `subTimeFunctionClass.getFunction`, evaluated.
///
/// `sign` is `1` for `ADDTIME` and `-1` for `SUBTIME`; `row_path` selects
/// Go's `evalString` body over its `vecEvalString` one (see the module doc).
pub(crate) fn add_sub_time(
    vals: &[Datum],
    kinds: [TemporalKind; 2],
    sign: i64,
    row_path: bool,
    cols: &dyn Columns,
) -> Result<Datum, EvalError> {
    if vals.len() != 2 {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    // Every `...Null` signature: a DATETIME/TIMESTAMP second argument makes
    // the result NULL whatever the first argument is.
    if kinds[1] == TemporalKind::Datetime {
        return Ok(Datum::Null);
    }
    let (Some(left), Some(right)) = (coerce_str(&vals[0])?, coerce_str(&vals[1])?) else {
        return Ok(Datum::Null);
    };
    match kinds[0] {
        // `...DatetimeAnd*`: Go's row body passes the parsed duration fsp to
        // `Time.Add`. The vectorized DATETIME+TIME arm instead constructs
        // `Duration{Fsp: -1}` and therefore keeps the first argument's fsp;
        // the DATETIME+STRING vector arm keeps the parsed string fsp. Constant
        // folding takes the row body, so preserve the right-side fractional
        // digits there and retain that one vectorized distinction.
        TemporalKind::Datetime => {
            let Some(delta) = second_as_duration(&right, kinds[1], cols, false)? else {
                return Ok(Datum::Null);
            };
            let delta = if !row_path && kinds[1] == TemporalKind::Duration {
                GoDuration { fsp: -1, ..delta }
            } else {
                delta
            };
            datetime_result(&left, delta, sign)
        }
        // `...DateAnd*`: `arg0.SetType(TypeDatetime)` first, so a DATE reads
        // as midnight; the result is a STRING and the DATE's own fsp is 0,
        // which leaves the duration's fsp deciding. The DATE+STRING row and
        // vector bodies use `getFsp4TimeAddSub` (non-zero fraction => 6),
        // unlike the DATETIME+STRING bodies' `GetFsp`.
        TemporalKind::Date => {
            let Some(delta) = second_as_duration(&right, kinds[1], cols, true)? else {
                return Ok(Datum::Null);
            };
            datetime_result(&left, delta, sign)
        }
        // `...DurationAnd*`: both operands are durations and so is the
        // result, at the larger of the two fsps.
        // The first operand is a TIME column here, so its fsp is its own
        // (Go's `EvalDuration` reads the column type's decimal), NOT MaxFsp
        // the way the string arm's `strDurationAddDuration` parses it.
        TemporalKind::Duration => {
            let Ok(first) = parse_duration(&left, get_fsp(&left)) else {
                return Ok(truncated_time_warning(cols, &left));
            };
            let Some(delta) = second_as_duration(&right, kinds[1], cols, false)? else {
                return Ok(Datum::Null);
            };
            Ok(Datum::new_string(first.combine(delta, sign).format()))
        }
        // `...StringAnd*`: the ONE arm that decides between the duration and
        // the datetime reading at RUNTIME, from the first argument's text.
        TemporalKind::Other => {
            let delta = match kinds[1] {
                TemporalKind::Duration => match parse_duration(&right, MAX_FSP) {
                    Ok(delta) => delta,
                    Err(Truncated) => return Ok(truncated_time_warning(cols, &right)),
                },
                _ => {
                    // `builtinAddStringAndStringSig`: the second argument's
                    // fsp comes from `getFsp4TimeAddSub`, not `GetFsp`.
                    match parse_duration(&right, fsp_for_time_add_sub(&right)) {
                        Ok(delta) => delta,
                        Err(Truncated) => return Ok(truncated_time_warning(cols, &right)),
                    }
                }
            };
            // ADDTIME only (`sign > 0`): `builtinSubStringAndStringSig` has
            // no such guard, which is why the same constant pair answers
            // NULL under ADDTIME and a real value under SUBTIME.
            if row_path
                && sign > 0
                && kinds[1] != TemporalKind::Duration
                && trailing_dash_group(&right)
            {
                return Ok(Datum::Null);
            }
            if is_duration(&left) {
                let Ok(first) = parse_duration(&left, MAX_FSP) else {
                    return Ok(truncated_time_warning(cols, &left));
                };
                let sum = first.combine(delta, sign);
                let fsp = if sum.micro_second() == 0 {
                    MIN_FSP
                } else {
                    MAX_FSP
                };
                return Ok(Datum::new_string(GoDuration { fsp, ..sum }.format()));
            }
            // `strDatetimeAddDuration`/`strDatetimeSubDuration`: the datetime
            // is parsed at MaxFsp and the RESULT's fsp is MaxFsp only when
            // the sum carries a microsecond.
            str_datetime_add_duration(&left, delta, sign, cols)
        }
    }
}

/// The second argument as a duration, for every arm whose second operand is
/// evaluated as one. `None` means the whole call is NULL.
fn second_as_duration(
    text: &str,
    kind: TemporalKind,
    cols: &dyn Columns,
    date_string_fsp: bool,
) -> Result<Option<GoDuration>, EvalError> {
    if kind != TemporalKind::Duration && !is_duration(text) {
        // `builtin...AndStringSig`: a second argument that is not
        // duration-shaped is NULL without a warning.
        return Ok(None);
    }
    let fsp = if date_string_fsp && kind != TemporalKind::Duration {
        fsp_for_time_add_sub(text)
    } else {
        get_fsp(text)
    };
    match parse_duration(text, fsp) {
        Ok(duration) => Ok(Some(duration)),
        Err(Truncated) => {
            truncated_time_warning(cols, text);
            Ok(None)
        }
    }
}

/// `builtinAdd{Datetime,Date}And{Duration,String}Sig`: the first argument is
/// evaluated as a DATETIME, and a zero one makes the result NULL.
fn datetime_result(text: &str, delta: GoDuration, sign: i64) -> Result<Datum, EvalError> {
    let Some(first) = parse_datetime(text) else {
        return Ok(Datum::Null);
    };
    if first.is_zero() {
        return Ok(Datum::Null);
    }
    let signed = GoDuration {
        micros: delta.micros * sign,
        ..delta
    };
    match first.add(signed) {
        Some(result) if result.in_range() => Ok(Datum::new_string(result.format())),
        _ => Ok(Datum::Null),
    }
}

/// Go `strDatetimeAddDuration`/`strDatetimeSubDuration`.
fn str_datetime_add_duration(
    text: &str,
    delta: GoDuration,
    sign: i64,
    cols: &dyn Columns,
) -> Result<Datum, EvalError> {
    let Some(first) = parse_datetime(text) else {
        // Go appends the parse error as a warning "regardless of the
        // sql_mode, this is compatible with MySQL" and answers NULL.
        cols.append_warning(1292, &format!("Incorrect datetime value: '{text}'"));
        return Ok(Datum::Null);
    };
    let first = GoDateTime {
        fsp: MAX_FSP,
        ..first
    };
    let signed = GoDuration {
        micros: delta.micros * sign,
        ..delta
    };
    let Some(result) = first.add(signed) else {
        return Ok(Datum::Null);
    };
    if !result.in_range() {
        return Ok(Datum::Null);
    }
    let fsp = if result.micros == 0 { MIN_FSP } else { MAX_FSP };
    Ok(Datum::new_string(GoDateTime { fsp, ..result }.format()))
}

/// The tail of `builtinAddStringAndStringSig.evalString`: a second argument
/// that reads as `<digits>-<something>` makes the result NULL. Only ADDTIME,
/// only the row path (see the module doc).
fn trailing_dash_group(text: &str) -> bool {
    let trimmed = text.trim_start_matches(|c: char| c.is_ascii_whitespace());
    let digits = trimmed
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(trimmed.len());
    if digits == 0 {
        return false;
    }
    matches!(trimmed[digits..].strip_prefix('-'), Some(rest) if !rest.is_empty())
}

/// `timestampFunctionClass`: `builtinTimestamp1ArgSig` /
/// `builtinTimestamp2ArgsSig`. The result is a DATETIME whose fsp is the
/// argument's own; the second argument is a DURATION added to it, and it is
/// rejected outright when it carries a date part.
pub(crate) fn timestamp(vals: &[Datum], cols: &dyn Columns) -> Result<Datum, EvalError> {
    if vals.is_empty() || vals.len() > 2 {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    let Some(text) = coerce_str(&vals[0])? else {
        return Ok(Datum::Null);
    };
    // Go selects `ParseTimeFromFloatString` for numeric and DECIMAL
    // signatures, even though all signatures first call EvalString.  That
    // parser treats a suffix after a packed date as a fractional second and
    // preserves zero-date DECIMAL values (for example `0.123`).  String and
    // temporal signatures use `ParseTime`; retaining the source-kind bit here
    // keeps the date-only compact suffix (`20240315.5`) as an hour for STRING
    // but as a fractional second for numeric values.
    let is_float = matches!(
        vals[0],
        Datum::Int(_) | Datum::UInt(_) | Datum::Decimal(_) | Datum::Real(_) | Datum::Float32(_)
    );
    let parsed = tidb_datatype::parse_time(
        &text,
        tidb_datatype::TimeType::DateTime,
        i64::from(get_fsp(&text)),
        is_float,
        true,
        false,
        &cols.time_zone(),
    );
    let Ok(parsed) = parsed else {
        cols.append_warning(1292, &format!("Incorrect datetime value: '{text}'"));
        return Ok(Datum::Null);
    };
    let core = parsed.time.core_time();
    let base = GoDateTime {
        year: i64::from(core.year()),
        month: u32::from(core.month()),
        day: u32::from(core.day()),
        hour: u32::from(core.hour()),
        minute: u32::from(core.minute()),
        second: u32::from(core.second()),
        micros: core.microsecond(),
        fsp: parsed.time.fsp().into(),
    };
    if vals.len() == 1 {
        return Ok(Datum::new_string(base.format()));
    }
    let Some(second) = coerce_str(&vals[1])? else {
        return Ok(Datum::Null);
    };
    // `builtinTimestamp2ArgsSig`: a second argument that is not
    // duration-shaped is NULL before any parse is attempted, and so is a
    // first argument with a zero year ("MySQL won't evaluate add for date
    // with zero year").
    if base.year == 0 || !is_duration(&second) {
        return Ok(Datum::Null);
    }
    let Ok(delta) = parse_duration(&second, get_fsp(&second)) else {
        return Ok(Datum::Null);
    };
    match base.add(delta) {
        Some(result) if result.in_range() => Ok(Datum::new_string(
            GoDateTime {
                fsp: base.fsp.max(delta.fsp),
                ..result
            }
            .format(),
        )),
        _ => Ok(Datum::Null),
    }
}

#[cfg(test)]
mod sysdate_source_tests {
    use crate::{Datum, EvalError, NoColumns};

    const REMOVED: EvalError = EvalError::Unsupported(
        "native temporal clock evaluation was removed; function unsupported",
    );

    #[test]
    fn test_sys_date() {
        for (args, former) in [
            (vec![], "host clock at fsp 0"),
            (vec![Datum::Int(6)], "host clock at fsp 6"),
            (vec![Datum::Int(-2)], "bad fractional-seconds precision"),
        ] {
            assert_eq!(
                crate::func::eval_func_values_in("SYSDATE", &args, &NoColumns),
                Some(Err(REMOVED.clone())),
                "former oracle: {former}"
            );
        }
    }

    #[test]
    fn sysdate_is_now_uses_the_statement_clock_and_now_rounding() {
        for (args, former) in [
            (vec![], "2023-11-15 06:13:20"),
            (vec![Datum::Int(3)], "2023-11-15 06:13:20.654"),
            (vec![Datum::Int(6)], "2023-11-15 06:13:20.654999"),
        ] {
            assert_eq!(
                crate::func::eval_func_values_in("SYSDATE", &args, &NoColumns),
                Some(Err(REMOVED.clone())),
                "former oracle: {former}"
            );
        }
    }
}
