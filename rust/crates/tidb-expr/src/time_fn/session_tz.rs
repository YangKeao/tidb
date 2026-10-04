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

//! The session-timezone time builtins: `FROM_UNIXTIME`, `UNIX_TIMESTAMP`,
//! and `TIDB_PARSE_TSO`, transcreated from `evalFromUnixTime`,
//! `builtinUnixTimestamp*Sig`/`goTimeToMysqlUnixTimestamp`, and
//! `builtinTidbParseTsoSig` in `pkg/expression/builtin_time.go`.
//!
//! These render or interpret wall-clock time in the session `time_zone`
//! ([`Columns::time_zone`]); the trait's default is the goeval oracle's
//! pinned `UTC+11`, keeping the golden corpus deterministic.
//!
//! Contracts pinned against goeval, not assumed:
//! - `FROM_UNIXTIME`'s fsp comes from the argument's type: integers 0,
//!   decimals their (capped) scale, strings/floats 6; the value is ROUNDED
//!   half-up at fsp (`.1234567` → `.123457`), the range is
//!   `[0, 32536771199]`, and out-of-range is NULL.
//! - `UNIX_TIMESTAMP`'s fsp comes from the argument's fractional digits;
//!   the value is TRUNCATED after Go's six-digit input rounding; datetimes
//!   outside `['1970-01-01 00:00:01', '3001-01-18 23:59:59.999999']` UTC are
//!   0. An all-zero date or syntactically invalid value is NULL, while a
//!   partially zero date (for example `2017-00-02`) returns numeric 0; fsp 0
//!   yields an integer, otherwise a decimal.
//! - `TIDB_PARSE_TSO` renders `tso >> 18` milliseconds since epoch at full
//!   (6-digit) precision; a non-positive tso is NULL.
//! - The zero-argument `UNIX_TIMESTAMP()` needs the statement clock and
//!   declines when [`Columns::now`] is absent.

#[cfg(test)]
use chrono::NaiveDate;
use chrono::{Datelike, NaiveDateTime, Timelike, Utc};

use super::calendar::date_format_in;
use crate::coerce::coerce_str;
use crate::context::SessionTimeZone;
use crate::{Columns, Datum, Decimal, EvalError};

/// MySQL 8.0.28's maximum unix timestamp: '3001-01-18 23:59:59' UTC.
const MAX_UNIX_SECS: i64 = 32_536_771_199;

/// Renders the instant `secs`+`micros` (unix epoch) as a local wall clock in
/// the session zone.
fn instant_to_local(secs: i64, micros: u32, tz: &SessionTimeZone) -> Option<NaiveDateTime> {
    let utc = chrono::DateTime::<Utc>::from_timestamp(secs, micros * 1000)?;
    Some(match tz {
        SessionTimeZone::Local => utc.with_timezone(&chrono::Local).naive_local(),
        SessionTimeZone::Fixed { offset_secs, .. } => {
            (utc + chrono::Duration::seconds(i64::from(*offset_secs))).naive_utc()
        }
        SessionTimeZone::Named(tz) => utc.with_timezone(tz).naive_local(),
    })
}

fn format_local(local: NaiveDateTime, fsp: usize) -> String {
    let mut out = format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
        local.year(),
        local.month(),
        local.day(),
        local.hour(),
        local.minute(),
        local.second()
    );
    if fsp > 0 {
        let micros = local.and_utc().timestamp_subsec_micros();
        let shown = micros / 10_u32.pow(6 - fsp as u32);
        out.push('.');
        out.push_str(&format!("{shown:0fsp$}"));
    }
    out
}

/// The unix-seconds argument as `(total_nanoseconds, fsp)`; `None` is NULL.
/// Go derives fsp from the argument TYPE: int 0, decimal its capped scale,
/// real/string `MaxFsp`; real values first pass through
/// `MyDecimal.FromFloat64`'s shortest `%g` spelling. The nanoseconds keep the
/// full written fraction so rounding at fsp happens on the complete value, as
/// in `evalFromUnixTime`.
fn unix_arg_nanos(value: &Datum, cols: &dyn Columns) -> Result<Option<(i128, usize)>, EvalError> {
    let (text, fsp) = match value {
        Datum::Null => return Ok(None),
        Datum::Int(v) => (v.to_string(), 0),
        Datum::UInt(v) => (v.to_string(), 0),
        Datum::Decimal(d) => {
            let text = d.to_string();
            let scale = text.split_once('.').map_or(0, |(_, f)| f.len()).min(6);
            (text, scale)
        }
        Datum::Real(v) | Datum::Float32(v) => {
            let Some(decimal) = Decimal::from_f64(*v) else {
                return Ok(None);
            };
            (decimal.to_string(), 6)
        }
        other => {
            let Some(text) = coerce_str(other)? else {
                return Ok(None);
            };
            // go's `builtinFromUnixTimeSig` takes an ETDecimal argument, so a
            // textual source was already cast: a wholly non-numeric string
            // (`'a'`) casts to 0 and answers the epoch, while a numeric
            // spelling keeps its own scale.
            let trimmed = text.trim();
            let (int_part, _) = trimmed.split_once('.').unwrap_or((trimmed, ""));
            if int_part.parse::<i64>().is_err() {
                // go's ETDecimal argument cast runs `StrToDecimal` through
                // `HandleTruncate`: a wholly non-numeric string warns
                // "Truncated incorrect DECIMAL value: 'a'" on its way to the
                // epoch answer (captured on the oracle).
                cols.handle_truncate(&format!(
                    "Truncated incorrect DECIMAL value: '{}'",
                    tidb_datatype::warning_subject_byte_cap(trimmed)
                ))?;
                return Ok(Some((0_i128, 0)));
            }
            (text, 6)
        }
    };

    let text = text.trim();
    let (int_part, frac_part) = text.split_once('.').unwrap_or((text, ""));
    let Ok(int_part): Result<i64, _> = int_part.parse() else {
        return Ok(None);
    };
    if int_part < 0 || frac_part.starts_with('-') {
        return Ok(None);
    }
    let frac_digits: String = frac_part.chars().take(9).collect();
    if !frac_digits.bytes().all(|b| b.is_ascii_digit()) {
        return Ok(None);
    }
    let frac_nanos: i128 = if frac_digits.is_empty() {
        0
    } else {
        format!("{frac_digits:0<9}").parse().unwrap()
    };
    Ok(Some((
        i128::from(int_part) * 1_000_000_000 + frac_nanos,
        fsp,
    )))
}

/// `FROM_UNIXTIME(unix[, format])`.
pub(crate) fn from_unixtime(vals: &[Datum], cols: &dyn Columns) -> Result<Datum, EvalError> {
    if !(1..=2).contains(&vals.len()) {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    let Some((total_nanos, fsp)) = unix_arg_nanos(&vals[0], cols)? else {
        return Ok(Datum::Null);
    };
    let integral = total_nanos / 1_000_000_000;
    if integral > i128::from(MAX_UNIX_SECS) {
        return Ok(Datum::Null);
    }

    // Round half-up at fsp over the complete value (convertTimeToMysqlTime
    // with ModeHalfUp), carrying into the seconds when the fraction rolls.
    let factor = 10_i128.pow(9 - fsp as u32);
    let rounded = (total_nanos + factor / 2) / factor * factor;
    let secs = (rounded / 1_000_000_000) as i64;
    let micros = ((rounded % 1_000_000_000) / 1000) as u32;

    let Some(local) = instant_to_local(secs, micros, &cols.time_zone()) else {
        return Ok(Datum::Null);
    };
    let formatted = format_local(local, fsp);
    if vals.len() == 2 {
        return date_format_in(&Datum::new_string(formatted), &vals[1], cols);
    }
    Ok(Datum::new_string(formatted))
}

/// `UNIX_TIMESTAMP([datetime])`.
pub(crate) fn unix_timestamp(vals: &[Datum], cols: &dyn Columns) -> Result<Datum, EvalError> {
    use crate::tikv::{EvaluatedArgs, EvaluatedBytesOp as Op, EvaluatedBytesResult};
    use tidb_query_expr::NativeUnixTimestampResult;
    crate::tikv::evaluate_prepared_args_scoped_in(
        cols,
        || {
            match vals.len() {
                0 => {
                    let Some((seconds, nanos, _)) = cols.now() else {
                        return Err(EvalError::Unsupported("session clock"));
                    };
                    return Ok((
                        Op::UnixTimestampNowNative,
                        EvaluatedArgs::Int2(Some(seconds), Some(i64::from(nanos))),
                    ));
                }
                1 => {}
                _ => return Err(EvalError::Unsupported("bad function arity")),
            }
            let Some(text) = coerce_str(&vals[0])? else {
                return Ok((Op::UnixTimestampNullNative, EvaluatedArgs::Bytes(None)));
            };
            let is_float = matches!(
                vals[0],
                Datum::Int(_)
                    | Datum::UInt(_)
                    | Datum::Decimal(_)
                    | Datum::Real(_)
                    | Datum::Float32(_)
            );
            Ok((
                Op::UnixTimestampParseNative,
                EvaluatedArgs::TemporalParseText {
                    value: text.into_bytes(),
                    is_float,
                    zone: cols.time_zone(),
                },
            ))
        },
        |computed, scoped_cols| {
            let Some(bytes) = computed.into_bytes()? else {
                return Ok(Datum::Null);
            };
            let result = tidb_query_expr::decode_native_unix_timestamp_result(&bytes)
                .ok_or_else(crate::tikv::native_time_result_contract_error)?;
            match result {
                NativeUnixTimestampResult::Value(_) => {
                    EvaluatedBytesResult::Bytes(Some(bytes)).into_identity_datum()
                }
                NativeUnixTimestampResult::Warning { code, message } => {
                    scoped_cols.append_warning(code, message);
                    Ok(Datum::Null)
                }
                NativeUnixTimestampResult::Continue(_) if vals.len() == 1 && !vals[0].is_null() => {
                    // Only the worker's actual continuation demands the second
                    // zone. Do not cache the first getter or re-pack its frame.
                    crate::tikv::evaluate_prepared_args_in(
                        scoped_cols,
                        || {
                            Ok((
                                Op::UnixTimestampValueNative,
                                EvaluatedArgs::TemporalValue {
                                    value: bytes,
                                    zone: scoped_cols.time_zone(),
                                },
                            ))
                        },
                        EvaluatedBytesResult::into_identity_datum,
                    )
                }
                _ => Err(crate::tikv::native_time_result_contract_error()),
            }
        },
    )
}

/// `TIDB_PARSE_TSO(tso)`: the physical half as a full-precision native
/// DATETIME in the session zone.
pub(super) fn tidb_parse_tso(vals: &[Datum], cols: &dyn Columns) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_prepared_args_in(
        cols,
        || {
            use chrono::{Offset, TimeZone};
            if vals.len() != 1 {
                return Err(EvalError::Unsupported("bad function arity"));
            }
            let tso = super::int_arg(&vals[0])?;
            let offset = match tso {
                Some(value) if value > 0 => {
                    // Preserve the getter before UTC preparation. Fixed zones
                    // retain their raw SDK offset, not TimeZone's clamped view.
                    let zone = cols.time_zone();
                    Some(match &zone {
                        SessionTimeZone::Fixed { offset_secs, .. } => i64::from(*offset_secs),
                        SessionTimeZone::Named(_) | SessionTimeZone::Local => {
                            let instant = tidb_query_expr::native_tso_utc(value)
                                .expect("a positive signed TSO is within Chrono's timestamp range");
                            i64::from(
                                zone.offset_from_utc_datetime(&instant.naive_utc())
                                    .fix()
                                    .local_minus_utc(),
                            )
                        }
                    })
                }
                _ => None,
            };
            Ok((
                crate::tikv::EvaluatedBytesOp::TidbParseTsoNative,
                crate::tikv::EvaluatedArgs::Int2(tso, offset),
            ))
        },
        crate::tikv::EvaluatedBytesResult::into_identity_datum,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NoColumns;

    fn call(f: fn(&[Datum], &dyn Columns) -> Result<Datum, EvalError>, vals: &[Datum]) -> Datum {
        f(vals, &NoColumns).unwrap()
    }

    fn s(v: &str) -> Datum {
        Datum::new_string(v.to_string())
    }

    fn dec(v: &str) -> Datum {
        Datum::Decimal(Decimal::from_literal(v))
    }

    #[test]
    fn unix_timestamp_workers_preserve_two_zone_demands_and_direct_warnings() {
        use std::cell::{Cell, RefCell};
        struct Demand {
            zones: Cell<usize>,
            second_offset: Cell<i32>,
            warnings: RefCell<Vec<(u16, String)>>,
        }
        impl Columns for Demand {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            fn time_zone(&self) -> SessionTimeZone {
                let read = self.zones.get();
                self.zones.set(read + 1);
                if read == 0 {
                    SessionTimeZone::utc()
                } else {
                    SessionTimeZone::Fixed {
                        name: "second actual zone".to_owned(),
                        offset_secs: self.second_offset.get(),
                    }
                }
            }
            fn now(&self) -> Option<(i64, u32, i32)> {
                panic!("one-argument UNIX_TIMESTAMP must not read now")
            }
            fn date_modes(&self) -> tidb_datatype::DateModes {
                panic!("UNIX_TIMESTAMP has fixed parse modes")
            }
            fn truncate_level(&self) -> crate::context::ErrorLevel {
                panic!("UNIX_TIMESTAMP warnings are direct")
            }
            fn append_warning(&self, code: u16, message: &str) {
                self.warnings.borrow_mut().push((code, message.to_owned()));
            }
        }
        for slots in [1, 0] {
            let owner = crate::AsciiPoolOwner::new(
                crate::AsciiPoolPolicy::checked(
                    slots,
                    slots,
                    16 * 1024 * 1024,
                    4 * 1024 * 1024,
                    4 * 1024 * 1024,
                    64,
                    8,
                    4 * 1024 * 1024,
                )
                .unwrap(),
            )
            .unwrap();
            let execution = owner.begin_execution().unwrap();
            let ctx = Demand {
                zones: Cell::new(0),
                second_offset: Cell::new(3600),
                warnings: RefCell::new(Vec::new()),
            };
            for (value, expected, zones, warning) in [
                (Datum::Null, Datum::Null, 0_usize, false),
                (s("bad"), Datum::Null, 1, true),
                (s("0000-00-00 01:00:00"), Datum::Null, 1, false),
                (s("2017-00-02 00:00:00.123"), dec("0.000"), 1, false),
                (s("0000-01-01"), Datum::Int(0), 2, false),
                (s("1960-01-01"), Datum::Int(0), 2, false),
                (s("1970-01-01 01:00:01"), Datum::Int(1), 2, false),
                (s("1970-01-01 01:00:01.123"), dec("1.123"), 2, false),
                (s("1970-01-01 03:00:01+02:00"), Datum::Int(1), 2, false),
            ] {
                ctx.zones.set(0);
                ctx.warnings.borrow_mut().clear();
                let result = execution
                    .scope()
                    .with_columns(&ctx, |columns| unix_timestamp(&[value], columns));
                if slots == 0 {
                    let error = result.unwrap_err();
                    let EvalError::ExpressionAdapterFailure(failure) = error else {
                        panic!("{error:?}")
                    };
                    assert_eq!(
                        failure.class(),
                        crate::ExpressionAdapterFailureClass::PoolResource
                    );
                    assert_eq!(
                        failure.origin(),
                        crate::ExpressionAdapterFailureOrigin::Pool
                    );
                } else {
                    let actual = result.unwrap();
                    assert_eq!(actual, expected);
                    if let (Datum::Decimal(actual), Datum::Decimal(expected)) = (&actual, &expected)
                    {
                        assert_eq!(actual.to_string(), expected.to_string());
                    }
                }
                assert_eq!(
                    ctx.zones.get(),
                    if slots == 0 { zones.min(1) } else { zones }
                );
                let expected_warnings = if slots == 1 && warning {
                    vec![(1292, "Incorrect datetime value: 'bad'".to_owned())]
                } else {
                    Vec::new()
                };
                assert_eq!(*ctx.warnings.borrow(), expected_warnings);
            }
            ctx.zones.set(0);
            ctx.second_offset.set(90000);
            let result = execution.scope().with_columns(&ctx, |columns| {
                unix_timestamp(&[s("1970-01-02 01:00:01")], columns)
            });
            if slots == 1 {
                assert_eq!(result.unwrap(), Datum::Int(1));
            } else {
                assert!(matches!(
                    result,
                    Err(EvalError::ExpressionAdapterFailure(_))
                ));
            }
            assert_eq!(ctx.zones.get(), if slots == 1 { 2 } else { 1 });
            for (values, message) in [
                (
                    vec![Datum::new_bytes(vec![0xff])],
                    "invalid UTF-8 byte datum",
                ),
                (vec![Datum::Null, Datum::Null], "bad function arity"),
            ] {
                ctx.zones.set(0);
                assert!(
                    matches!(execution.scope().with_columns(&ctx, |columns| unix_timestamp(&values, columns)), Err(EvalError::Unsupported(actual)) if actual == message)
                );
                assert_eq!(ctx.zones.get(), 0);
            }
        }
    }

    #[test]
    fn unix_timestamp_workers_preserve_raw_clock_and_decimal_scale() {
        use std::cell::Cell;
        struct Clock {
            clock: Cell<Option<(i64, u32, i32)>>,
            reads: Cell<usize>,
            zones: Cell<usize>,
        }
        impl Columns for Clock {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            fn now(&self) -> Option<(i64, u32, i32)> {
                self.reads.set(self.reads.get() + 1);
                self.clock.get()
            }
            fn time_zone(&self) -> SessionTimeZone {
                self.zones.set(self.zones.get() + 1);
                SessionTimeZone::utc()
            }
            fn date_modes(&self) -> tidb_datatype::DateModes {
                panic!("UNIX_TIMESTAMP has fixed parse modes")
            }
            fn truncate_level(&self) -> crate::context::ErrorLevel {
                panic!("UNIX_TIMESTAMP has no truncate-policy demand")
            }
            fn append_warning(&self, _: u16, _: &str) {
                panic!("valid UNIX_TIMESTAMP inputs must not warn")
            }
        }
        for slots in [1, 0] {
            let owner = crate::AsciiPoolOwner::new(
                crate::AsciiPoolPolicy::checked(
                    slots,
                    slots,
                    16 * 1024 * 1024,
                    4 * 1024 * 1024,
                    4 * 1024 * 1024,
                    64,
                    8,
                    4 * 1024 * 1024,
                )
                .unwrap(),
            )
            .unwrap();
            let execution = owner.begin_execution().unwrap();
            let ctx = Clock {
                clock: Cell::new(None),
                reads: Cell::new(0),
                zones: Cell::new(0),
            };
            let check = |result: Result<Datum, EvalError>, expected: Datum| {
                if slots == 0 {
                    let error = result.unwrap_err();
                    let EvalError::ExpressionAdapterFailure(failure) = error else {
                        panic!("{error:?}")
                    };
                    assert_eq!(
                        failure.class(),
                        crate::ExpressionAdapterFailureClass::PoolResource
                    );
                    assert_eq!(
                        failure.origin(),
                        crate::ExpressionAdapterFailureOrigin::Pool
                    );
                } else {
                    let actual = result.unwrap();
                    assert_eq!(actual, expected);
                    if let (Datum::Decimal(actual), Datum::Decimal(expected)) = (&actual, &expected)
                    {
                        assert_eq!(actual.to_string(), expected.to_string());
                    }
                }
            };
            for (clock, expected) in [
                ((0, 0, 12345), 0),
                ((0, u32::MAX, i32::MIN), 4),
                ((1, 999999999, i32::MAX), 1),
                ((MAX_UNIX_SECS, 999999999, -3600), MAX_UNIX_SECS),
                ((MAX_UNIX_SECS + 1, 0, 3600), 0),
                ((-1, 0, 0), 0),
            ] {
                ctx.clock.set(Some(clock));
                ctx.reads.set(0);
                ctx.zones.set(0);
                check(
                    execution
                        .scope()
                        .with_columns(&ctx, |columns| unix_timestamp(&[], columns)),
                    Datum::Int(expected),
                );
                assert_eq!(ctx.reads.get(), 1);
                assert_eq!(ctx.zones.get(), 0);
            }
            ctx.clock.set(None);
            ctx.reads.set(0);
            assert!(matches!(
                execution
                    .scope()
                    .with_columns(&ctx, |columns| unix_timestamp(&[], columns)),
                Err(EvalError::Unsupported("session clock"))
            ));
            assert_eq!(ctx.reads.get(), 1);
            assert_eq!(ctx.zones.get(), 0);
            for (value, expected) in [
                (Datum::Int(19700101000001), Datum::Int(1)),
                (Datum::UInt(19700101000001), Datum::Int(1)),
                (dec("19700101.5"), dec("0.0")),
                (Datum::Real(19700101.5), dec("0.0")),
                (Datum::Float32(700101.5), dec("0.0")),
                (s("19700101.5"), dec("18000.0")),
                (s("1970-01-01 00:00:01.1234567"), dec("1.123457")),
                (s("1970-01-01 00:00:00.9999999"), dec("1.000000")),
                (s("1969-12-31 23:59:59.123"), dec("0.000")),
                (s("3001-01-18 23:59:59.999999"), dec("32536771199.999999")),
                (s("3001-01-18 23:59:59.9999999"), dec("0.000000")),
            ] {
                ctx.reads.set(0);
                ctx.zones.set(0);
                check(
                    execution
                        .scope()
                        .with_columns(&ctx, |columns| unix_timestamp(&[value], columns)),
                    expected,
                );
                assert_eq!(ctx.reads.get(), 0);
                assert_eq!(ctx.zones.get(), if slots == 1 { 2 } else { 1 });
            }
        }
    }

    /// Every vector is goeval output under its pinned UTC+11 session zone.
    #[test]
    fn from_unixtime_goeval_vectors() {
        let cases: &[(Datum, &str)] = &[
            (Datum::Int(0), "1970-01-01 11:00:00"),
            (Datum::Int(1), "1970-01-01 11:00:01"),
            (Datum::Int(1_447_430_881), "2015-11-14 03:08:01"),
            (dec("1447430881.123456"), "2015-11-14 03:08:01.123456"),
            (dec("1447430881.999999"), "2015-11-14 03:08:01.999999"),
            // Literal scale 7 rounds half-up into fsp 6.
            (dec("1447430881.1234567"), "2015-11-14 03:08:01.123457"),
            (dec("1447430881.12"), "2015-11-14 03:08:01.12"),
            (Datum::Int(MAX_UNIX_SECS), "3001-01-19 10:59:59"),
            // A string argument carries MaxFsp.
            (s("1447430881.5"), "2015-11-14 03:08:01.500000"),
        ];
        for (arg, want) in cases {
            assert_eq!(
                call(from_unixtime, std::slice::from_ref(arg)),
                s(want),
                "FROM_UNIXTIME({arg:?})"
            );
        }
        assert_eq!(call(from_unixtime, &[Datum::Int(-1)]), Datum::Null);
        assert_eq!(
            call(from_unixtime, &[Datum::Int(MAX_UNIX_SECS + 1)]),
            Datum::Null
        );
        assert_eq!(call(from_unixtime, &[Datum::Null]), Datum::Null);

        // Two-argument form composes with DATE_FORMAT.
        assert_eq!(
            call(from_unixtime, &[Datum::Int(1_447_430_881), s("%H")]),
            s("03")
        );
    }

    #[test]
    fn unix_timestamp_goeval_vectors() {
        let cases: &[(&str, Datum)] = &[
            ("2015-11-13 10:20:19", Datum::Int(1_447_370_419)),
            ("2015-11-13 10:20:19.012", dec("1447370419.012")),
            ("1970-01-01 00:00:00", Datum::Int(0)),
            ("1969-12-31 23:59:59", Datum::Int(0)),
            ("3001-01-18 23:59:59", Datum::Int(32_536_731_599)),
            ("2038-01-19 03:14:07", Datum::Int(2_147_444_047)),
        ];
        for (arg, want) in cases {
            assert_eq!(
                call(unix_timestamp, &[s(arg)]),
                *want,
                "UNIX_TIMESTAMP({arg})"
            );
        }
        assert_eq!(
            call(unix_timestamp, &[s("0000-00-00 00:00:00")]),
            Datum::Null
        );
        assert_eq!(call(unix_timestamp, &[s("not-a-date")]), Datum::Null);
        assert_eq!(call(unix_timestamp, &[Datum::Null]), Datum::Null);
        // The zero-argument form needs the statement clock.
        assert!(unix_timestamp(&[], &NoColumns).is_err());
    }

    /// A session in a NAMED zone. `NoColumns` reports a fixed offset, where a
    /// daylight-saving transition cannot occur at all, so the transition
    /// behaviour is unreachable without one of these.
    struct ParisSession;

    impl Columns for ParisSession {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }

        fn time_zone(&self) -> SessionTimeZone {
            SessionTimeZone::Named(chrono_tz::Europe::Paris)
        }
    }

    /// Captured from a real TiDB session (`gorun`, `set @@time_zone =
    /// 'Europe/Paris'`). Paris skips 02:00 -> 03:00 on 2025-03-30, so every
    /// wall clock in the gap names no instant at all; TiDB answers the
    /// transition rather than failing, which is `types.CoreTime.AdjustedGoTime`.
    ///
    /// The autumn case is the CONTROL: 02:30 occurs TWICE that day, which is
    /// the ambiguous arm and not the gap arm, and it must keep answering the
    /// earlier of the two instants however the gap arm changes.
    #[test]
    fn unix_timestamp_in_a_daylight_saving_gap_answers_the_transition() {
        for (arg, want) in [
            ("2025-03-30 01:59:59", 1_743_296_399),
            ("2025-03-30 02:00:00", 1_743_296_400),
            ("2025-03-30 02:30:00", 1_743_296_400),
            ("2025-03-30 02:59:59", 1_743_296_400),
            ("2025-03-30 03:00:00", 1_743_296_400),
            ("2025-10-26 02:30:00", 1_761_442_200),
        ] {
            assert_eq!(
                unix_timestamp(&[s(arg)], &ParisSession).unwrap(),
                Datum::Int(want),
                "UNIX_TIMESTAMP({arg}) in Europe/Paris"
            );
        }
    }

    #[test]
    fn tidb_parse_tso_worker_preserves_zone_demand_and_raw_offsets() {
        use std::cell::Cell;
        use tidb_datatype::{Time, TimeType};
        struct Zone {
            zone: SessionTimeZone,
            reads: Cell<usize>,
        }
        impl Columns for Zone {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            fn time_zone(&self) -> SessionTimeZone {
                self.reads.set(self.reads.get() + 1);
                self.zone.clone()
            }
            fn now(&self) -> Option<(i64, u32, i32)> {
                panic!("TIDB_PARSE_TSO must not read the statement clock")
            }
        }
        let fixed = |offset_secs| SessionTimeZone::Fixed {
            name: "raw SDK offset".to_owned(),
            offset_secs,
        };
        let timestamp = |hour, minute, second| {
            NaiveDate::from_ymd_opt(2020, 3, 29)
                .unwrap()
                .and_hms_opt(hour, minute, second)
                .unwrap()
                .and_utc()
                .timestamp_millis()
                << 18
        };
        let time = |[year, month, day, hour, minute, second, micros]: [i32; 7]| {
            Datum::Time(
                Time::from_date_checked(
                    year,
                    month,
                    day,
                    hour,
                    minute,
                    second,
                    micros,
                    TimeType::DateTime,
                    6,
                )
                .unwrap(),
            )
        };
        let local = instant_to_local(0, 0, &SessionTimeZone::Local).unwrap();
        let cases = [
            (Datum::Int(1), fixed(0), [1970, 1, 1, 0, 0, 0, 0]),
            (Datum::Int(1), fixed(i32::MAX), [2038, 1, 19, 3, 14, 7, 0]),
            (
                Datum::Int(1),
                fixed(i32::MIN),
                [1901, 12, 13, 20, 45, 52, 0],
            ),
            (
                s("404411537129996288"),
                fixed(8 * 3600),
                [2018, 11, 20, 17, 53, 4, 877_000],
            ),
            (
                Datum::Int(timestamp(0, 59, 59)),
                SessionTimeZone::Named(chrono_tz::Europe::Paris),
                [2020, 3, 29, 1, 59, 59, 0],
            ),
            (
                Datum::Int(timestamp(1, 0, 0)),
                SessionTimeZone::Named(chrono_tz::Europe::Paris),
                [2020, 3, 29, 3, 0, 0, 0],
            ),
            (
                Datum::Int(1),
                SessionTimeZone::Local,
                [
                    local.year(),
                    local.month() as i32,
                    local.day() as i32,
                    local.hour() as i32,
                    local.minute() as i32,
                    local.second() as i32,
                    0,
                ],
            ),
        ];
        for slots in [1, 0] {
            let owner = crate::AsciiPoolOwner::new(
                crate::AsciiPoolPolicy::checked(
                    slots,
                    slots,
                    16 * 1024 * 1024,
                    4 * 1024 * 1024,
                    4 * 1024 * 1024,
                    64,
                    8,
                    4 * 1024 * 1024,
                )
                .unwrap(),
            )
            .unwrap();
            let execution = owner.begin_execution().unwrap();
            let check = |result: Result<Datum, EvalError>, expected: Datum| {
                if slots == 1 {
                    assert_eq!(result.unwrap(), expected);
                } else {
                    let error = result.expect_err("every TSO root must retain the zero-slot scope");
                    let EvalError::ExpressionAdapterFailure(failure) = error else {
                        panic!("{error:?}")
                    };
                    assert_eq!(
                        failure.class(),
                        crate::ExpressionAdapterFailureClass::PoolResource
                    );
                    assert_eq!(
                        failure.origin(),
                        crate::ExpressionAdapterFailureOrigin::Pool
                    );
                }
            };
            for (value, zone, fields) in &cases {
                let ctx = Zone {
                    zone: zone.clone(),
                    reads: Cell::new(0),
                };
                let result = execution.scope().with_columns(&ctx, |columns| {
                    tidb_parse_tso(std::slice::from_ref(value), columns)
                });
                check(result, time(*fields));
                assert_eq!(ctx.reads.get(), 1);
            }
            let ctx = Zone {
                zone: fixed(i32::MAX),
                reads: Cell::new(0),
            };
            for value in [
                Datum::Null,
                Datum::Int(0),
                Datum::Int(-1),
                Datum::UInt(u64::MAX),
                s("bad"),
            ] {
                check(
                    execution
                        .scope()
                        .with_columns(&ctx, |columns| tidb_parse_tso(&[value], columns)),
                    Datum::Null,
                );
                assert_eq!(
                    ctx.reads.get(),
                    0,
                    "NULL and nonpositive inputs do not observe the zone"
                );
            }
            for args in [vec![], vec![Datum::Int(1), Datum::Int(2)]] {
                assert!(matches!(
                    execution
                        .scope()
                        .with_columns(&ctx, |columns| tidb_parse_tso(&args, columns)),
                    Err(EvalError::Unsupported("bad function arity"))
                ));
            }
            for (value, message) in [
                (Datum::MinNotNull, "range sentinel time argument"),
                (Datum::new_bytes(vec![0xff]), "invalid UTF-8 byte datum"),
            ] {
                assert!(matches!(
                    execution.scope().with_columns(&ctx, |columns| tidb_parse_tso(&[value], columns)),
                    Err(EvalError::Unsupported(actual)) if actual == message
                ));
            }
            assert!(matches!(
                execution
                    .scope()
                    .with_columns(&ctx, |columns| tidb_parse_tso(
                        &[dec("99999999999999999999999999999999999999")],
                        columns
                    )),
                Err(EvalError::IntOverflow)
            ));
            assert_eq!(
                ctx.reads.get(),
                0,
                "arity and coercion errors precede zone demand and the worker"
            );
        }
    }

    #[test]
    fn tidb_parse_tso_goeval_vectors() {
        assert_eq!(
            call(tidb_parse_tso, &[Datum::Int(424_930_234_047_906_595)])
                .sql_string()
                .unwrap(),
            "2021-05-14 19:16:41.903000"
        );
        assert_eq!(call(tidb_parse_tso, &[Datum::Int(0)]), Datum::Null);
        assert_eq!(call(tidb_parse_tso, &[Datum::Null]), Datum::Null);
    }
}
