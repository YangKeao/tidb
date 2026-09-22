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
//! This module is the remaining native ownership boundary for retained
//! calendar, duration and timezone functions. Statement-clock and excluded
//! temporal kernels have been physically removed; callers enter through one
//! narrow [`dispatch`] seam.
//!
//! `ADDTIME`, `SUBTIME` and `TIMESTAMP` -- the three retained functions that
//! Go types from argument `FieldType`s rather than values -- live in
//! [`add_sub`], with their microsecond value domain in [`duration_parse`].

pub(crate) mod add_sub;
pub(crate) mod calendar;
pub(crate) mod duration_parse;
pub(crate) mod extract;
mod session_tz;

use self::calendar::{parse_date_ymd, week_of_year};
use crate::coerce::coerce_str;
use crate::{Columns, Datum, EvalError};

/// Dispatches this family's builtins; `None` if `name` isn't one of them.
pub(crate) fn dispatch(
    name: &str,
    vals: &[Datum],
    cols: &dyn Columns,
) -> Option<Result<Datum, EvalError>> {
    Some(match name {
        "WEEK" => week(vals, cols.default_week_format()),
        "STR_TO_DATE" => calendar::str_to_date(vals, cols),
        "FROM_UNIXTIME" => session_tz::from_unixtime(vals, cols),
        "UNIX_TIMESTAMP" => session_tz::unix_timestamp(vals, cols),
        "TIMESTAMPDIFF" => calendar::timestamp_diff(vals),
        // `ADDTIME`/`SUBTIME` reach here with no static argument types, so
        // every argument takes Go's `default` branch -- which is the branch
        // Go itself selects for a string constant. The chunk tier, which
        // does have the types, enters through [`add_sub::add_sub_time`]
        // directly; see that module's doc for the row/vec split this
        // `row_path = true` selects.
        "ADDTIME" | "SUBTIME" => add_sub::add_sub_untyped(name, vals, cols),
        "TIMESTAMP" => add_sub::timestamp(vals, cols),
        "TO_DAYS" => calendar::to_days(vals),
        "TO_SECONDS" => calendar::to_seconds(vals),
        // `EXTRACT(<composite unit> FROM value)`, e.g. `HOUR_MINUTE`,
        // `DAY_SECOND`, `YEAR_MONTH` — see `calendar::extract_composite`'s
        // own doc.
        "YEAR_MONTH" | "DAY_HOUR" | "DAY_MINUTE" | "DAY_SECOND" | "DAY_MICROSECOND"
        | "HOUR_MINUTE" | "HOUR_SECOND" | "HOUR_MICROSECOND" | "MINUTE_SECOND"
        | "MINUTE_MICROSECOND" | "SECOND_MICROSECOND" => calendar::extract_composite(name, vals),
        _ => return None,
    })
}

pub(crate) fn week(vals: &[Datum], default_week_format: i64) -> Result<Datum, EvalError> {
    if !(1..=2).contains(&vals.len()) {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    let Some(date) = coerce_str(&vals[0])?.and_then(|s| parse_date_ymd(&s)) else {
        return Ok(Datum::Null);
    };
    let mode = if vals.len() == 2 {
        int_arg(&vals[1])?.unwrap_or(0)
    } else {
        default_week_format
    };
    Ok(Datum::Int(
        week_of_year(date.0, date.1, date.2, mode, false).1,
    ))
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

/// Canonical duration rendering shared by the datatype bridge and retained
/// temporal functions; this does not dispatch a SQL builtin.
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
        for (name, fsp, former) in [
            ("NOW", 7, "TooBigFsp { fsp: 7, function: now }"),
            ("CURTIME", 8, "TooBigFsp { fsp: 8, function: curtime }"),
        ] {
            assert_eq!(
                source_eval(name, &[Datum::Int(fsp)], &ctx),
                Err(EvalError::Unsupported(
                    "native temporal clock evaluation was removed; function unsupported"
                )),
                "former oracle: {former}"
            );
        }
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
            for (name, former) in [("MICROSECOND", Datum::Int(micros)), ("TIME", string(time))] {
                assert_eq!(
                    source_eval(name, &args, &ctx),
                    Err(EvalError::Unsupported(
                        "native temporal value evaluation was removed; TiKV engine required"
                    )),
                    "former oracle: {former:?}"
                );
            }
        }

        for name in ["HOUR", "MINUTE", "SECOND"] {
            assert_eq!(
                source_eval(name, &[Datum::Null], &ctx).unwrap(),
                Datum::Null
            );
        }
        for name in ["MICROSECOND", "TIME"] {
            assert_eq!(
                source_eval(name, &[Datum::Null], &ctx),
                Err(EvalError::Unsupported(
                    "native temporal value evaluation was removed; TiKV engine required"
                )),
                "former oracle: NULL"
            );
        }

        let malformed = [string("2011-11-11 10:10:10.11.12")];
        for name in ["HOUR", "MINUTE", "SECOND"] {
            assert_eq!(source_eval(name, &malformed, &ctx).unwrap(), Datum::Null);
        }
        let warning_count = ctx.warnings.borrow().len();
        for (name, former) in [("MICROSECOND", Datum::Null), ("TIME", string("00:00:00"))] {
            assert_eq!(
                source_eval(name, &malformed, &ctx),
                Err(EvalError::Unsupported(
                    "native temporal value evaluation was removed; TiKV engine required"
                )),
                "former oracle: {former:?}"
            );
        }
        assert_eq!(ctx.warnings.borrow().len(), warning_count);
    }
}

#[cfg(test)]
mod tests;
