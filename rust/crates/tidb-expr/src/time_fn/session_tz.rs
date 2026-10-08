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
use chrono::{Datelike, NaiveDate, Timelike};

use super::calendar::date_format_in;
use crate::coerce::coerce_str;
use crate::context::SessionTimeZone;
#[cfg(test)]
use crate::Decimal;
use crate::{Columns, Datum, EvalError};

/// MySQL 8.0.28's maximum unix timestamp: '3001-01-18 23:59:59' UTC.
#[cfg(test)]
const MAX_UNIX_SECS: i64 = 32_536_771_199;

#[cfg(test)]
use tidb_query_expr::native_from_unixtime_instant_to_local as instant_to_local;

/// `FROM_UNIXTIME(unix[, format])`.
pub(crate) fn from_unixtime(vals: &[Datum], cols: &dyn Columns) -> Result<Datum, EvalError> {
    use crate::tikv::{EvaluatedArgs, EvaluatedBytesOp as Op};
    use tidb_query_expr::NativeFromUnixTimeResult;
    crate::tikv::evaluate_prepared_args_scoped_in(
        cols,
        || {
            if !(1..=2).contains(&vals.len()) {
                return Err(EvalError::Unsupported("bad function arity"));
            }
            match &vals[0] {
                Datum::Null => Ok((Op::FromUnixTimeNullNative, EvaluatedArgs::Bytes(None))),
                value @ (Datum::Int(_)
                | Datum::UInt(_)
                | Datum::Decimal(_)
                | Datum::Real(_)
                | Datum::Float32(_)) => Ok((
                    Op::FromUnixTimeNumericNative,
                    crate::tikv::prepare_datum_identity_args(value)?,
                )),
                value => Ok((
                    Op::FromUnixTimeTextNative,
                    EvaluatedArgs::Bytes(coerce_str(value)?.map(String::into_bytes)),
                )),
            }
        },
        |computed, scoped_cols| {
            let Some(bytes) = computed.into_bytes()? else {
                return Ok(Datum::Null);
            };
            match tidb_query_expr::decode_native_from_unixtime_result(&bytes)
                .ok_or_else(crate::tikv::native_time_result_contract_error)?
            {
                NativeFromUnixTimeResult::Continue(_) => {}
                NativeFromUnixTimeResult::Truncate { message, .. } => {
                    scoped_cols.handle_truncate(message)?;
                }
            }
            // Keep the complete actual report, including a truncate report.
            // Its policy replay must finish before the original zone demand.
            crate::tikv::evaluate_prepared_args_scoped_in(
                scoped_cols,
                || {
                    Ok((
                        Op::FromUnixTimeLocalNative,
                        EvaluatedArgs::TemporalValue {
                            value: bytes,
                            zone: scoped_cols.time_zone(),
                        },
                    ))
                },
                |computed, local_cols| {
                    let Some(bytes) = computed.into_bytes()? else {
                        return Ok(Datum::Null);
                    };
                    let value = Datum::new_string(bytes);
                    if vals.len() == 1 {
                        Ok(value)
                    } else {
                        // The existing formatter owns both conversions and its
                        // worker, reached only after a successful local value.
                        date_format_in(&value, &vals[1], local_cols)
                    }
                },
            )
        },
    )
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
    fn from_unixtime_workers_preserve_truncate_zone_and_late_format_order() {
        use crate::context::ErrorLevel;
        use std::cell::RefCell;
        struct Policy {
            level: ErrorLevel,
            events: RefCell<Vec<&'static str>>,
            warnings: RefCell<Vec<(u16, String)>>,
        }
        impl Columns for Policy {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            fn now(&self) -> Option<(i64, u32, i32)> {
                panic!("FROM_UNIXTIME has no clock demand")
            }
            fn date_modes(&self) -> tidb_datatype::DateModes {
                panic!("FROM_UNIXTIME leaf has no mode demand")
            }
            fn time_zone(&self) -> SessionTimeZone {
                self.events.borrow_mut().push("zone");
                SessionTimeZone::utc()
            }
            fn truncate_level(&self) -> ErrorLevel {
                self.events.borrow_mut().push("policy");
                self.level
            }
            fn append_warning(&self, code: u16, message: &str) {
                self.events.borrow_mut().push("warning");
                self.warnings.borrow_mut().push((code, message.to_owned()));
            }
        }
        for slots in [1, 0] {
            let owner = crate::ReadyValuePoolOwner::new(
                crate::ReadyValuePoolPolicy::checked(
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
            let resource = |error: EvalError| {
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
            };
            for level in [ErrorLevel::Ignore, ErrorLevel::Warn, ErrorLevel::Error] {
                let ctx = Policy {
                    level,
                    events: RefCell::new(Vec::new()),
                    warnings: RefCell::new(Vec::new()),
                };
                for text in [" a ", ".1", "1e2", "9223372036854775808"] {
                    for bad_format in [false, true] {
                        ctx.events.borrow_mut().clear();
                        ctx.warnings.borrow_mut().clear();
                        let format = if bad_format {
                            Datum::new_bytes(vec![0xff])
                        } else {
                            s("%Y")
                        };
                        let result = execution.scope().with_columns(&ctx, |columns| {
                            from_unixtime(&[s(text), format], columns)
                        });
                        let message =
                            format!("Truncated incorrect DECIMAL value: '{}'", text.trim());
                        if slots == 0 {
                            resource(result.unwrap_err());
                        } else if level == ErrorLevel::Error {
                            assert!(
                                matches!(result, Err(EvalError::TruncatedWrongValue(actual)) if actual == message)
                            );
                        } else if bad_format {
                            assert!(matches!(
                                result,
                                Err(EvalError::Unsupported("invalid UTF-8 byte datum"))
                            ));
                        } else {
                            assert_eq!(result.unwrap(), s("1970"));
                        }
                        let events = if slots == 0 {
                            vec![]
                        } else if level == ErrorLevel::Error {
                            vec!["policy"]
                        } else if level == ErrorLevel::Warn {
                            vec!["policy", "warning", "zone"]
                        } else {
                            vec!["policy", "zone"]
                        };
                        assert_eq!(*ctx.events.borrow(), events);
                        let warnings = if slots == 1 && level == ErrorLevel::Warn {
                            vec![(1292, message)]
                        } else {
                            Vec::new()
                        };
                        assert_eq!(*ctx.warnings.borrow(), warnings);
                    }
                }
                for value in [
                    Datum::Null,
                    Datum::Int(-1),
                    Datum::Int(MAX_UNIX_SECS + 1),
                    Datum::UInt(u64::MAX),
                    Datum::Real(f64::NAN),
                    s("1.12345678X"),
                ] {
                    ctx.events.borrow_mut().clear();
                    ctx.warnings.borrow_mut().clear();
                    let result = execution.scope().with_columns(&ctx, |columns| {
                        from_unixtime(&[value, Datum::new_bytes(vec![0xff])], columns)
                    });
                    if slots == 0 {
                        resource(result.unwrap_err());
                    } else {
                        assert_eq!(result.unwrap(), Datum::Null);
                    }
                    assert!(ctx.events.borrow().is_empty());
                    assert!(ctx.warnings.borrow().is_empty());
                }
                ctx.events.borrow_mut().clear();
                let result = execution.scope().with_columns(&ctx, |columns| {
                    from_unixtime(&[Datum::Int(1), Datum::Null], columns)
                });
                if slots == 0 {
                    resource(result.unwrap_err());
                } else {
                    assert_eq!(result.unwrap(), Datum::Null);
                }
                assert_eq!(
                    *ctx.events.borrow(),
                    if slots == 0 { vec![] } else { vec!["zone"] }
                );
                for (values, message) in [
                    (vec![], "bad function arity"),
                    (
                        vec![Datum::Null, Datum::Null, Datum::Null],
                        "bad function arity",
                    ),
                    (
                        vec![Datum::new_bytes(vec![0xff])],
                        "invalid UTF-8 byte datum",
                    ),
                ] {
                    ctx.events.borrow_mut().clear();
                    assert!(
                        matches!(execution.scope().with_columns(&ctx, |columns| from_unixtime(&values, columns)), Err(EvalError::Unsupported(actual)) if actual == message)
                    );
                    assert!(ctx.events.borrow().is_empty());
                }
            }
        }
    }

    #[test]
    fn from_unixtime_workers_keep_kind_fraction_range_and_scoped_formatting() {
        use std::cell::Cell;
        struct Zone {
            reads: Cell<usize>,
            offset: Cell<i32>,
        }
        impl Columns for Zone {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            fn time_zone(&self) -> SessionTimeZone {
                self.reads.set(self.reads.get() + 1);
                SessionTimeZone::Fixed {
                    name: "raw offset".to_owned(),
                    offset_secs: self.offset.get(),
                }
            }
            fn now(&self) -> Option<(i64, u32, i32)> {
                panic!("FROM_UNIXTIME has no clock demand")
            }
            fn date_modes(&self) -> tidb_datatype::DateModes {
                panic!("FROM_UNIXTIME leaf has no mode demand")
            }
            fn truncate_level(&self) -> crate::context::ErrorLevel {
                panic!("valid numeric inputs must not consult truncate policy")
            }
            fn append_warning(&self, _: u16, _: &str) {
                panic!("valid numeric inputs must not warn")
            }
        }
        let owner = crate::ReadyValuePoolOwner::new(
            crate::ReadyValuePoolPolicy::checked(
                1,
                1,
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
        let ctx = Zone {
            reads: Cell::new(0),
            offset: Cell::new(0),
        };
        for (value, expected) in [
            (Datum::Int(1), "1970-01-01 00:00:01"),
            (Datum::UInt(1), "1970-01-01 00:00:01"),
            (dec("1.20"), "1970-01-01 00:00:01.20"),
            (Datum::Real(1.2), "1970-01-01 00:00:01.200000"),
            (Datum::Float32(1.00000049), "1970-01-01 00:00:01.000000"),
            (s("1"), "1970-01-01 00:00:01.000000"),
            (s("1.123456789X"), "1970-01-01 00:00:01.123457"),
            (dec("-0.1"), "1970-01-01 00:00:00.1"),
            (s("-0.1"), "1970-01-01 00:00:00.100000"),
            (Datum::Real(-0.1), "1970-01-01 00:00:00.100000"),
            (
                Datum::Decimal(Decimal::from_raw_parts(false, b"10049".to_vec(), 2, 4)),
                "1970-01-01 00:00:01.00",
            ),
            (dec("32536771199.9999999"), "3001-01-19 00:00:00.000000"),
        ] {
            ctx.reads.set(0);
            assert_eq!(
                execution
                    .scope()
                    .with_columns(&ctx, |columns| from_unixtime(&[value], columns))
                    .unwrap(),
                s(expected)
            );
            assert_eq!(ctx.reads.get(), 1);
        }
        ctx.reads.set(0);
        assert_eq!(
            execution
                .scope()
                .with_columns(&ctx, |columns| from_unixtime(
                    &[s("1.123456789X"), s("%Y-%m-%d %H:%i:%s.%f")],
                    columns
                ))
                .unwrap(),
            s("1970-01-01 00:00:01.123457")
        );
        assert_eq!(ctx.reads.get(), 1);
        ctx.offset.set(90000);
        ctx.reads.set(0);
        assert_eq!(
            execution
                .scope()
                .with_columns(&ctx, |columns| from_unixtime(&[Datum::Int(0)], columns))
                .unwrap(),
            s("1970-01-02 01:00:00")
        );
        assert_eq!(ctx.reads.get(), 1);
        assert_eq!(
            from_unixtime(&[Datum::Int(0), s("%Y")], &NoColumns).unwrap(),
            s("1970")
        );
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
            let owner = crate::ReadyValuePoolOwner::new(
                crate::ReadyValuePoolPolicy::checked(
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
            let owner = crate::ReadyValuePoolOwner::new(
                crate::ReadyValuePoolPolicy::checked(
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
            let owner = crate::ReadyValuePoolOwner::new(
                crate::ReadyValuePoolPolicy::checked(
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
