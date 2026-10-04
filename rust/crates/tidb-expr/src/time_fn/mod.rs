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
use crate::coerce::coerce_str;
use crate::{Columns, Datum, EvalError};
#[cfg(test)]
use tidb_query_datatype::codec::mysql::Time as TikvTime;
#[cfg(test)]
use tidb_query_expr::native_format_clock_datetime as format_datetime;

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
        "MICROSECOND" => microsecond_in(vals, cols),
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
        "LAST_DAY" => last_day_in(vals, cols),
        "TIME_TO_SEC" => time_to_sec_in(vals, cols),
        "SEC_TO_TIME" => sec_to_time_in(vals, cols),
        "MAKEDATE" => makedate_in(vals, cols),
        "MAKETIME" => maketime_in(vals, cols),
        "PERIOD_ADD" => period_add_in(vals, cols),
        "PERIOD_DIFF" => period_diff_in(vals, cols),
        "TIME_FORMAT" => time_format_in(vals, cols),
        "STR_TO_DATE" => calendar::str_to_date(vals, cols),
        "FROM_DAYS" => calendar::from_days_in(vals, cols),
        "TIMEDIFF" => time_diff_in(vals, cols),
        "CONVERT_TZ" => convert_tz::convert_tz_in(vals, cols),
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
    crate::tikv::eval_bounded_staleness_in(cols, vals)
}

/// `DATE(expr)`, after Go's declared `ETDatetime` argument cast has produced
/// a typed temporal value. The function applies its own zero-date SQL-mode
/// checks, clears the clock, and changes the result domain to `DATE`.
pub(crate) fn date(vals: &[Datum], cols: &dyn Columns) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_prepared_args_in(
        cols,
        || {
            let [value] = vals else {
                return Err(EvalError::Unsupported("bad function arity"));
            };
            let Datum::Time(value) = value else {
                return if matches!(value, Datum::Null) {
                    Ok((
                        crate::tikv::EvaluatedBytesOp::DateDiffNullNative,
                        crate::tikv::EvaluatedArgs::NullWitness(None),
                    ))
                } else {
                    Err(EvalError::Unsupported(
                        "DATE argument reached the signature without its ETDatetime cast",
                    ))
                };
            };
            let core = value.core_time().raw();
            let modes = cols.date_modes();
            if tidb_query_datatype::codec::mysql::Time::native_date_rejects_zero(
                core,
                modes.no_zero_date,
                modes.no_zero_in_date,
            ) {
                cols.handle_truncate(&format!("Incorrect datetime value: '{value}'"))?;
            }
            // Even a soft-rejected non-NULL retains its actual core and modes.
            // The worker owns the nullable decision and unconditional midnight
            // projection, including hidden clock bits in a Date-kind input.
            Ok((
                crate::tikv::EvaluatedBytesOp::DateCoreNative,
                crate::tikv::prepare_date_args(core, modes)?,
            ))
        },
        crate::tikv::EvaluatedBytesResult::into_date_core_datum,
    )
}

/// `builtinNowWithArgSig` / `builtinNowWithoutArgSig`: local
/// (`time_zone`-adjusted) statement time, always truncating fractional
/// seconds. `CURRENT_TIMESTAMP` is the same function class.
fn now(vals: &[Datum], cols: &dyn Columns) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_prepared_args_in(
        cols,
        || prepare_now_args(vals, cols),
        |computed| {
            Ok(computed
                .into_bytes()?
                .map_or(Datum::Null, Datum::new_string))
        },
    )
}

/// Nonexecuting preparation shared with SYSDATE's statement-clock alias.
/// The caller owns the single guard; precision errors precede clock demand.
pub(super) fn prepare_now_args(
    vals: &[Datum],
    cols: &dyn Columns,
) -> Result<(crate::tikv::EvaluatedBytesOp, crate::tikv::EvaluatedArgs), EvalError> {
    let fsp = parse_fsp_with_null_as_zero(vals, "now")?.unwrap_or(0);
    let clock = cols.now().ok_or(no_clock_err())?;
    Ok((
        crate::tikv::EvaluatedBytesOp::NowNative,
        crate::tikv::prepare_clock_args(clock, Some(fsp))?,
    ))
}

/// `builtinUTCTimestampWithArgSig` / `builtinUTCTimestampWithoutArgSig`:
/// raw UTC statement time, always rounding fractional seconds half-up.
fn utc_timestamp(vals: &[Datum], cols: &dyn Columns) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_prepared_args_in(
        cols,
        || {
            let fsp = parse_fsp_with_null_as_zero(vals, "utc_timestamp")?.unwrap_or(0);
            let clock = cols.now().ok_or(no_clock_err())?;
            Ok((
                crate::tikv::EvaluatedBytesOp::UtcTimestampNative,
                crate::tikv::prepare_clock_args(clock, Some(fsp))?,
            ))
        },
        |computed| {
            Ok(computed
                .into_bytes()?
                .map_or(Datum::Null, Datum::new_string))
        },
    )
}

/// `builtinCurrentDateSig`: local statement date. `CURDATE` and
/// `CURRENT_DATE` share this signature and accept no argument.
fn current_date(vals: &[Datum], cols: &dyn Columns) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_prepared_args_in(
        cols,
        || {
            if !vals.is_empty() {
                return Err(EvalError::Unsupported("bad function arity"));
            }
            let clock = cols.now().ok_or(no_clock_err())?;
            Ok((
                crate::tikv::EvaluatedBytesOp::CurrentDateNative,
                crate::tikv::prepare_clock_args(clock, None)?,
            ))
        },
        |computed| {
            Ok(computed
                .into_bytes()?
                .map_or(Datum::Null, Datum::new_string))
        },
    )
}

/// `builtinUTCDateSig`: raw UTC statement date with no arguments.
fn utc_date(vals: &[Datum], cols: &dyn Columns) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_prepared_args_in(
        cols,
        || {
            if !vals.is_empty() {
                return Err(EvalError::Unsupported("bad function arity"));
            }
            let clock = cols.now().ok_or(no_clock_err())?;
            Ok((
                crate::tikv::EvaluatedBytesOp::UtcDateNative,
                crate::tikv::prepare_clock_args(clock, None)?,
            ))
        },
        |computed| {
            Ok(computed
                .into_bytes()?
                .map_or(Datum::Null, Datum::new_string))
        },
    )
}

/// `builtinCurrentTime0ArgSig` / `builtinCurrentTime1ArgSig`: local
/// statement time. The zero-argument signature truncates; an explicit FSP,
/// including zero, rounds half-up. `CURTIME` and `CURRENT_TIME` are aliases.
fn current_time(
    vals: &[Datum],
    function: &'static str,
    cols: &dyn Columns,
) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_prepared_args_in(
        cols,
        || {
            let fsp = parse_fsp_with_null_as_zero(vals, function)?;
            let clock = cols.now().ok_or(no_clock_err())?;
            let operation = if fsp.is_some() {
                crate::tikv::EvaluatedBytesOp::CurrentTimeWithFspNative
            } else {
                crate::tikv::EvaluatedBytesOp::CurrentTimeWithoutFspNative
            };
            // Preserve the original instant and offset. The worker owns local
            // adjustment and the explicit-FSP microsecond-then-round policy.
            Ok((operation, crate::tikv::prepare_clock_args(clock, fsp)?))
        },
        |computed| {
            Ok(computed
                .into_bytes()?
                .map_or(Datum::Null, Datum::new_string))
        },
    )
}

/// `builtinUTCTimeWithoutArgSig` / `builtinUTCTimeWithArgSig`: raw UTC
/// statement time with the same zero-argument-truncate / explicit-FSP-round
/// split as [`current_time`].
fn utc_time(vals: &[Datum], cols: &dyn Columns) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_prepared_args_in(
        cols,
        || {
            if matches!(vals, [Datum::Null]) {
                return Ok((
                    crate::tikv::EvaluatedBytesOp::UtcTimeNullNative,
                    crate::tikv::EvaluatedArgs::NullWitness(None),
                ));
            }
            let fsp = parse_fsp_for(vals, "utc_time")?;
            let clock = cols.now().ok_or(no_clock_err())?;
            let operation = if fsp.is_some() {
                crate::tikv::EvaluatedBytesOp::UtcTimeWithFspNative
            } else {
                crate::tikv::EvaluatedBytesOp::UtcTimeWithoutFspNative
            };
            Ok((operation, crate::tikv::prepare_clock_args(clock, fsp)?))
        },
        |computed| {
            Ok(computed
                .into_bytes()?
                .map_or(Datum::Null, Datum::new_string))
        },
    )
}

/// `builtinMicroSecondSig.evalInt`: read the fractional component of the
/// ETDuration argument. Go deliberately suppresses a duration-cast error and
/// returns NULL, unlike `TIME()` which reports the same truncation through the
/// statement context.
pub(crate) fn microsecond_in(vals: &[Datum], cols: &dyn Columns) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_prepared_args_in(
        cols,
        || {
            if vals.len() != 1 {
                return Err(EvalError::Unsupported("bad function arity"));
            }
            Ok((
                crate::tikv::EvaluatedBytesOp::MicrosecondNative,
                crate::tikv::EvaluatedArgs::Bytes(coerce_str(&vals[0])?.map(String::into_bytes)),
            ))
        },
        crate::tikv::EvaluatedBytesResult::into_int_datum,
    )
}

/// `builtinTimeSig.evalDuration`: parse the string as a TiDB duration while
/// preserving its written FSP. `ErrTruncatedWrongVal` is a statement warning
/// for a SELECT and leaves Go's zero-value duration as the result.
fn time(vals: &[Datum], cols: &dyn Columns) -> Result<Datum, EvalError> {
    let source = std::cell::RefCell::new(None);
    crate::tikv::evaluate_prepared_args_in(
        cols,
        || {
            if vals.len() != 1 {
                return Err(EvalError::Unsupported("bad function arity"));
            }
            let text = coerce_str(&vals[0])?;
            let bytes = text.as_ref().map(|value| value.as_bytes().to_vec());
            // Keep the original text solely for the source diagnostic. Parsing
            // and every result byte, including the error fallback, belong to C4.
            *source.borrow_mut() = text;
            Ok((
                crate::tikv::EvaluatedBytesOp::TimeNative,
                crate::tikv::EvaluatedArgs::Bytes(bytes),
            ))
        },
        |computed| {
            let Some(bytes) = computed.into_bytes()? else {
                return Ok(Datum::Null);
            };
            let report = tidb_query_expr::decode_native_time_result(&bytes)
                .ok_or_else(crate::tikv::native_time_result_contract_error)?;
            if report.truncated {
                let source = source.borrow();
                let text = source
                    .as_deref()
                    .ok_or_else(crate::tikv::native_time_result_contract_error)?;
                cols.handle_truncate(&format!(
                    "Truncated incorrect time value: '{}'",
                    tidb_datatype::warning_subject_byte_cap(text)
                ))?;
            }
            Ok(Datum::new_string(report.value))
        },
    )
}

#[cfg(test)]
#[test]
fn time_microsecond_workers_preserve_parse_warning_and_scope_boundaries() {
    use std::cell::RefCell;
    struct Policy {
        strict: bool,
        warnings: RefCell<Vec<(u16, String)>>,
    }
    impl Columns for Policy {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn now(&self) -> Option<(i64, u32, i32)> {
            panic!("duration text must not read the clock")
        }
        fn time_zone(&self) -> crate::context::SessionTimeZone {
            panic!("duration text must not read a session zone")
        }
        fn truncate_level(&self) -> crate::context::ErrorLevel {
            if self.strict {
                crate::context::ErrorLevel::Error
            } else {
                crate::context::ErrorLevel::Warn
            }
        }
        fn append_warning(&self, code: u16, message: &str) {
            self.warnings.borrow_mut().push((code, message.to_owned()));
        }
    }
    let resource = |result: Result<Datum, EvalError>| {
        let error = result.expect_err("duration roots must retain the zero-slot scope");
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
        for strict in [false, true] {
            let ctx = Policy {
                strict,
                warnings: RefCell::new(Vec::new()),
            };
            for (input, text, micros) in [
                ("-00:00:00.123456", "-00:00:00.123456", 123456),
                ("11:11:11.1 ", "11:11:11.10", 100000),
                ("00:00:00.9999999", "00:00:01.000000", 0),
                ("20101010111111.123456", "11:11:11.123456", 123456),
            ] {
                for (name, expected) in [
                    ("TIME", Datum::new_string(text)),
                    ("MICROSECOND", Datum::Int(micros)),
                ] {
                    let result = execution.scope().with_columns(&ctx, |columns| {
                        dispatch(name, &[Datum::new_string(input)], columns).unwrap()
                    });
                    if slots == 1 {
                        assert_eq!(result.unwrap(), expected);
                    } else {
                        resource(result);
                    }
                    assert!(ctx.warnings.borrow().is_empty());
                }
            }
            for input in ["bad", "12:00:00..1", "838:59:59.000001", "101010111111.1"] {
                let value = Datum::new_string(input);
                let result = execution.scope().with_columns(&ctx, |columns| {
                    microsecond_in(std::slice::from_ref(&value), columns)
                });
                if slots == 1 {
                    assert_eq!(result.unwrap(), Datum::Null);
                } else {
                    resource(result);
                }
                assert!(ctx.warnings.borrow().is_empty());
                let result = execution
                    .scope()
                    .with_columns(&ctx, |columns| time(std::slice::from_ref(&value), columns));
                let message = format!("Truncated incorrect time value: '{input}'");
                if slots == 0 {
                    resource(result);
                } else if strict {
                    assert_eq!(result, Err(EvalError::TruncatedWrongValue(message.clone())));
                } else {
                    assert_eq!(result.unwrap(), Datum::new_string("00:00:00"));
                }
                if slots == 1 && !strict {
                    assert_eq!(ctx.warnings.take(), [(1292, message)]);
                } else {
                    assert!(ctx.warnings.borrow().is_empty());
                }
            }
            for name in ["TIME", "MICROSECOND"] {
                let result = execution.scope().with_columns(&ctx, |columns| {
                    dispatch(name, &[Datum::Null], columns).unwrap()
                });
                if slots == 1 {
                    assert_eq!(result.unwrap(), Datum::Null);
                } else {
                    resource(result);
                }
                for args in [vec![], vec![Datum::Null, Datum::Null]] {
                    assert!(matches!(
                        execution
                            .scope()
                            .with_columns(&ctx, |columns| dispatch(name, &args, columns).unwrap()),
                        Err(EvalError::Unsupported("bad function arity"))
                    ));
                }
                for (value, error) in [
                    (Datum::new_bytes(vec![0xff]), "invalid UTF-8 byte datum"),
                    (Datum::MinNotNull, "range sentinel string coercion"),
                ] {
                    assert!(
                        matches!(execution.scope().with_columns(&ctx, |columns| dispatch(name, &[value], columns).unwrap()), Err(EvalError::Unsupported(actual)) if actual == error)
                    );
                }
                assert!(ctx.warnings.borrow().is_empty());
            }
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

/// `builtinLastDaySig` retains the original guarded text coercion. The worker
/// owns whitespace splitting, strict clock-suffix validation and month-end
/// arithmetic; the existing outer typed DATE conversion is unchanged.
fn last_day_in(vals: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_bytes_in(
        crate::tikv::EvaluatedBytesOp::LastDayTextNative,
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
fn last_day(vals: &[Datum]) -> Result<Datum, EvalError> {
    last_day_in(vals, &crate::NoColumns)
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

/// Retains the original private entry used by the immutable source vectors.
#[cfg(test)]
fn time_diff(vals: &[Datum]) -> Result<Datum, EvalError> {
    time_diff_in(vals, &crate::NoColumns)
}

/// Prepare only demanded text coercions. The shared parser determines whether
/// the original left operand permits right coercion; the worker independently
/// parses the actual texts and owns subtraction, clamping, and formatting.
fn time_diff_in(vals: &[Datum], cols: &dyn Columns) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_prepared_args_in(
        cols,
        || {
            if vals.len() != 2 {
                return Err(EvalError::Unsupported("bad function arity"));
            }
            let left = coerce_str(&vals[0])?;
            let right = if tidb_query_expr::native_time_diff_needs_right(left.as_deref()) {
                coerce_str(&vals[1])?
            } else {
                // An undemanded suffix, not a replacement for the real left
                // operand: the worker still receives and parses invalid text.
                None
            };
            Ok((
                crate::tikv::EvaluatedBytesOp::TimeDiffTextNative,
                crate::tikv::EvaluatedArgs::Bytes2(
                    left.map(String::into_bytes),
                    right.map(String::into_bytes),
                ),
            ))
        },
        |computed| {
            Ok(match computed.into_bytes()? {
                Some(bytes) => Datum::new_string(bytes),
                None => Datum::Null,
            })
        },
    )
}

// GoDuration::format still shares this exact formatter without adopting the
// TIMEDIFF parser or its clamp policy.
fn format_time_diff(micros: i64, fsp: usize) -> String {
    tidb_query_expr::native_format_time_diff(micros, fsp)
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

/// `builtinSecToTimeSig` keeps original numeric/FSP preparation in the guard.
/// The worker owns clamping and formatting, including non-SQL raw FSP values.
fn sec_to_time_in(vals: &[Datum], cols: &dyn Columns) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_args_in(
        crate::tikv::EvaluatedBytesOp::SecToTimeNative,
        cols,
        || {
            if vals.len() != 1 {
                return Err(EvalError::Unsupported("bad function arity"));
            }
            let seconds = number_arg(&vals[0], cols)?;
            let precision = match seconds {
                // Successful FSP comes from nonnegative i64 metadata, u8, or
                // at most six digits, so this transport is lossless, not a clamp.
                Some(_) => Some(duration_precision(&vals[0])? as i64),
                // NULL seconds never demand FSP; this is not a second SQL NULL.
                None => None,
            };
            Ok(crate::tikv::EvaluatedArgs::Ieee754BitsInt {
                value: seconds.map(f64::to_bits),
                scale: precision,
            })
        },
        |computed| {
            Ok(computed
                .into_bytes()?
                .map_or(Datum::Null, Datum::new_string))
        },
    )
}

#[cfg(test)]
fn sec_to_time(vals: &[Datum], cols: &dyn Columns) -> Result<Datum, EvalError> {
    sec_to_time_in(vals, cols)
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

/// `builtinMakeDateSig` retains both conversions, including after a left NULL.
/// Year/day validation, arithmetic and date formatting belong to the worker.
fn makedate_in(vals: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_args_in(
        crate::tikv::EvaluatedBytesOp::MakeDateNative,
        ctx,
        || {
            if vals.len() != 2 {
                return Err(EvalError::Unsupported("bad function arity"));
            }
            let (year, day) = (int_arg(&vals[0])?, int_arg(&vals[1])?);
            Ok(crate::tikv::EvaluatedArgs::Int2(year, day))
        },
        |computed| {
            Ok(computed
                .into_bytes()?
                .map_or(Datum::Null, Datum::new_string))
        },
    )
}

#[cfg(test)]
fn makedate(vals: &[Datum]) -> Result<Datum, EvalError> {
    makedate_in(vals, &crate::NoColumns)
}

/// `builtinMakeTimeSig` first computes actual seconds in a worker. Only a
/// successful result demands the original second datum's FSP in a second,
/// sequential invocation; no callback reenters C4 and no host range test runs.
/// This deliberately places the first admission before FSP preparation.
fn maketime_in(vals: &[Datum], cols: &dyn Columns) -> Result<Datum, EvalError> {
    let seconds = crate::tikv::evaluate_args_in(
        crate::tikv::EvaluatedBytesOp::MakeTimePartsNative,
        cols,
        || {
            if vals.len() != 3 {
                return Err(EvalError::Unsupported("bad function arity"));
            }
            let (hour, minute, second) = (
                int_arg(&vals[0])?,
                int_arg(&vals[1])?,
                number_arg(&vals[2], cols)?,
            );
            let hour_unsigned = matches!(vals[0], Datum::UInt(_));
            Ok(crate::tikv::EvaluatedArgs::MakeTimeParts {
                hour: hour.map(|hour| (hour, hour_unsigned)),
                minute,
                second: second.map(f64::to_bits),
            })
        },
        crate::tikv::EvaluatedBytesResult::into_ieee754_bits,
    )?;
    let Some(seconds) = seconds else {
        return Ok(Datum::Null);
    };
    crate::tikv::evaluate_args_in(
        crate::tikv::EvaluatedBytesOp::SecToTimeNative,
        cols,
        || {
            Ok(crate::tikv::EvaluatedArgs::Ieee754BitsInt {
                value: Some(seconds),
                // Same lossless FSP transport as SEC_TO_TIME, without a 0..=6 cap.
                scale: Some(duration_precision(&vals[2])? as i64),
            })
        },
        |computed| {
            Ok(computed
                .into_bytes()?
                .map_or(Datum::Null, Datum::new_string))
        },
    )
}

#[cfg(test)]
fn maketime(vals: &[Datum], cols: &dyn Columns) -> Result<Datum, EvalError> {
    maketime_in(vals, cols)
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

/// `builtinTimeFormatSig` demands its mask only after duration parsing succeeds.
/// Two complete sequential calls preserve that boundary without host parsing or
/// callback reentry. First admission can precede mask coercion; successful calls
/// pay for two parses, owned-text transport and two leases (two one-shot workers
/// without a context capability). Formatting retains the native text policy.
fn time_format_in(vals: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    let text = crate::tikv::evaluate_args_in(
        crate::tikv::EvaluatedBytesOp::DurationTextProbeNative,
        ctx,
        || {
            if vals.len() != 2 {
                return Err(EvalError::Unsupported("bad function arity"));
            }
            Ok(crate::tikv::EvaluatedArgs::Bytes(
                coerce_str(&vals[0])?.map(String::into_bytes),
            ))
        },
        crate::tikv::EvaluatedBytesResult::into_bytes,
    )?;
    let Some(text) = text else {
        return Ok(Datum::Null);
    };
    crate::tikv::evaluate_args_in(
        crate::tikv::EvaluatedBytesOp::TimeFormatTextNative,
        ctx,
        || {
            let mask = coerce_str(&vals[1])?.map(String::into_bytes);
            Ok(crate::tikv::EvaluatedArgs::Bytes2(Some(text), mask))
        },
        |computed| {
            Ok(computed
                .into_bytes()?
                .map_or(Datum::Null, Datum::new_string))
        },
    )
}

#[cfg(test)]
fn time_format(vals: &[Datum]) -> Result<Datum, EvalError> {
    time_format_in(vals, &crate::NoColumns)
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
#[test]
fn time_diff_worker_preserves_demand_and_raw_formatter_policy() {
    struct Quiet;
    impl Columns for Quiet {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
            panic!("untyped TIMEDIFF must not read the session time zone")
        }
        fn append_warning(&self, _: u16, _: &str) {
            panic!("TIMEDIFF parsing and clamping do not emit warnings")
        }
    }
    let string = |text: &str| Datum::new_string(text);
    let cases = [
        (Datum::Null, Datum::new_bytes(vec![255]), Datum::Null),
        (string("bad"), Datum::new_bytes(vec![255]), Datum::Null),
        (string("00:00:00.1234567"), Datum::MaxValue, Datum::Null),
        // The original checked hour multiplication rejects this before the
        // positive minute/second fields could bring a wider sum into range.
        (string("--2562047789:59:59"), Datum::MaxValue, Datum::Null),
        (string("10:10:10"), string("10:9:0"), string("00:01:10")),
        (string("--1:00:00"), string("00:00:00"), string("01:00:00")),
        (
            string("00:00:00.+1"),
            string("00:00:00"),
            string("00:00:00.01"),
        ),
        (
            string("2000-01-01 00:00:00.1234567"),
            string("2000-01-01 00:00:00"),
            string("00:00:00.123456"),
        ),
        (
            string("900:00:00.1"),
            string("00:00:00"),
            string("838:59:59.0"),
        ),
        (string("10:10:10"), Datum::Null, Datum::Null),
        (string("2000-01-01"), string("00:00:00"), Datum::Null),
    ];
    for slots in [0, 1] {
        let owner = crate::AsciiPoolOwner::new(
            crate::AsciiPoolPolicy::checked(
                slots,
                slots,
                16 << 20,
                1 << 20,
                2 << 20,
                64,
                8,
                1 << 16,
            )
            .unwrap(),
        )
        .unwrap();
        let execution = owner.begin_execution().unwrap();
        let scope = execution.scope();
        scope.with_columns(&Quiet, |ctx| {
            for (left, right, expected) in &cases {
                let result =
                    crate::time_fn::dispatch("TIMEDIFF", &[left.clone(), right.clone()], ctx)
                        .unwrap();
                if slots == 0 {
                    assert!(matches!(
                        result,
                        Err(EvalError::ExpressionAdapterFailure(ref failure))
                            if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource
                    ));
                } else {
                    assert_eq!(result.as_ref().unwrap(), expected);
                    if expected != &Datum::Null {
                        assert!(matches!(result, Ok(Datum::String(_))));
                    }
                }
            }
            assert_eq!(
                time_diff_in(&[], ctx),
                Err(EvalError::Unsupported("bad function arity")),
            );
            for values in [
                [Datum::new_bytes(vec![255]), Datum::Null],
                [string("00:00:00"), Datum::new_bytes(vec![255])],
            ] {
                assert_eq!(
                    time_diff_in(&values, ctx),
                    Err(EvalError::Unsupported("invalid UTF-8 byte datum")),
                );
            }
            assert_eq!(
                time_diff_in(&[string("00:00:00"), Datum::MaxValue], ctx),
                Err(EvalError::Unsupported("range sentinel string coercion")),
            );
        });
        drop(scope);
        execution.close();
    }
    // The forwarding formatter is shared with GoDuration, so it must not
    // acquire TIMEDIFF's result clamp or normalize a subsecond negative sign.
    assert_eq!(format_time_diff(3_240_000_000_000, 0), "900:00:00");
    assert_eq!(format_time_diff(-1, 0), "-00:00:00");
    assert_eq!(format_time_diff(-1, 6), "-00:00:00.000001");
}

#[cfg(test)]
#[test]
fn bounded_staleness_keeps_entry_preparation_and_source_demand() {
    use crate::constant::{Constant, ParamMarker};
    use crate::expression::Expression;
    use crate::scalar_function::ScalarFunction;
    use std::cell::RefCell;
    use tidb_datatype::{FieldType, FieldTypeCode, Time, TimeType};
    struct Demand {
        values: Vec<Datum>,
        fail: Option<usize>,
        safe: Time,
        events: RefCell<Vec<&'static str>>,
        warnings: RefCell<Vec<(u16, String)>>,
    }
    impl Columns for Demand {
        fn get(&self, path: &[String]) -> Option<Datum> {
            self.param_value(path[0].parse().unwrap()).ok()
        }
        fn param_value(&self, index: usize) -> Result<Datum, EvalError> {
            self.events
                .borrow_mut()
                .push(["left", "right", "extra"][index]);
            if self.fail == Some(index) {
                return Err(EvalError::Unsupported("bounded-staleness child"));
            }
            Ok(self.values[index].clone())
        }
        fn bounded_staleness_safe_time(&self) -> Option<Time> {
            self.events.borrow_mut().push("safe");
            Some(self.safe)
        }
        fn date_modes(&self) -> tidb_datatype::DateModes {
            self.events.borrow_mut().push("modes");
            tidb_datatype::DateModes::default()
        }
        fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
            self.events.borrow_mut().push("zone");
            tidb_datatype::SessionTimeZone::utc()
        }
        fn truncate_level(&self) -> crate::ErrorLevel {
            self.events.borrow_mut().push("policy");
            crate::ErrorLevel::Warn
        }
        fn append_warning(&self, code: u16, text: &str) {
            self.events.borrow_mut().push("warning");
            self.warnings.borrow_mut().push((code, text.to_owned()));
        }
        fn now(&self) -> Option<(i64, u32, i32)> {
            panic!("these endpoints need no statement clock")
        }
    }
    let left = Time::from_date_checked(2020, 1, 1, 0, 0, 0, 0, TimeType::DateTime, 0).unwrap();
    let right = Time::from_date_checked(2020, 1, 3, 0, 0, 0, 0, TimeType::DateTime, 0).unwrap();
    let safe =
        Time::from_date_checked(2020, 1, 2, 0, 0, 0, 123456, TimeType::Timestamp, 6).unwrap();
    let context = |values, fail| Demand {
        values,
        fail,
        safe,
        events: RefCell::new(Vec::new()),
        warnings: RefCell::new(Vec::new()),
    };
    let empty = tidb_chunk::mutrow::MutRow::from_datums(&[]);
    let evaluate = |mode, values: &[Datum], columns: &dyn Columns| {
        if mode == 0 {
            dispatch("TIDB_BOUNDED_STALENESS", values, columns).unwrap()
        } else if mode == 1 {
            let args = (0..values.len())
                .map(|index| tidb_ast::Expr::Column(vec![index.to_string()]))
                .collect::<Vec<_>>();
            crate::func::eval_func("TIDB_BOUNDED_STALENESS", &args, columns, None)
        } else {
            let args = values
                .iter()
                .enumerate()
                .map(|(index, value)| {
                    let field =
                        FieldType::new(if matches!(value, Datum::String(_) | Datum::Bytes(_)) {
                            FieldTypeCode::VarString
                        } else {
                            FieldTypeCode::Datetime
                        });
                    let mut constant = Constant::new(Datum::Null, field);
                    constant.param_marker = Some(ParamMarker {
                        order: index as i64,
                    });
                    Expression::Constant(constant)
                })
                .collect::<Vec<_>>();
            let inferred =
                crate::rewriter::result_type::builtin_return_type("tidb_bounded_staleness", &args);
            if args.len() == 2 {
                let field = inferred.as_ref().unwrap();
                assert_eq!(field.code(), FieldTypeCode::Datetime);
                assert_eq!(field.decimal(), 3);
            }
            ScalarFunction::new(
                tidb_ast::CiString::new("tidb_bounded_staleness"),
                inferred.unwrap_or_else(|| FieldType::new(FieldTypeCode::Datetime)),
                args,
            )
            .eval(columns, empty.to_row())
        }
    };
    let pool = crate::AsciiPoolOwner::new(
        crate::AsciiPoolPolicy::checked(
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
    let execution = pool.begin_execution().unwrap();
    for mode in 0..3 {
        let ctx = context(vec![Datum::Time(left), Datum::Time(right)], None);
        let value = execution
            .scope()
            .with_columns(&ctx, |columns| evaluate(mode, &ctx.values, columns))
            .unwrap();
        let Datum::Time(value) = value else {
            panic!("typed temporal result")
        };
        assert_eq!(value.core_time().raw(), safe.core_time().raw());
        assert_eq!((value.kind(), value.fsp()), (TimeType::DateTime, 3));
        assert_eq!(
            *ctx.events.borrow(),
            if mode == 0 {
                vec!["safe"]
            } else {
                vec!["left", "right", "safe"]
            }
        );
        assert!(ctx.warnings.borrow().is_empty());
        for count in [0, 1, 3] {
            let ctx = context(vec![Datum::Time(left); count], None);
            assert_eq!(
                execution.scope().with_columns(&ctx, |columns| evaluate(
                    mode,
                    &ctx.values,
                    columns
                )),
                Err(EvalError::WrongParameterCount("tidb_bounded_staleness"))
            );
            assert_eq!(
                *ctx.events.borrow(),
                if mode == 0 {
                    vec![]
                } else {
                    ["left", "right", "extra"][..count].to_vec()
                }
            );
        }
    }
    for mode in [1, 2] {
        let ctx = context(
            vec![
                Datum::new_string("2020-01-01 00:00:00"),
                Datum::new_string("2020-01-03 00:00:00"),
            ],
            None,
        );
        assert!(matches!(
            execution
                .scope()
                .with_columns(&ctx, |columns| evaluate(mode, &ctx.values, columns)),
            Ok(Datum::Time(_))
        ));
        assert_eq!(
            *ctx.events.borrow(),
            vec!["left", "right", "modes", "zone", "modes", "zone", "safe"]
        );
        // NULL left does not skip right's datetime cast. A wrong raw value
        // would instead be absorbed by the direct signature's NULL guard.
        let ctx = context(vec![Datum::Null, Datum::new_bytes(vec![255])], None);
        assert!(execution
            .scope()
            .with_columns(&ctx, |columns| evaluate(mode, &ctx.values, columns))
            .is_err());
        assert_eq!(*ctx.events.borrow(), vec!["left", "right"]);
        assert!(ctx.warnings.borrow().is_empty());
        for failed in [0, 1, 2] {
            let ctx = context(
                vec![Datum::Null, Datum::Time(right), Datum::Time(right)],
                Some(failed),
            );
            let result = execution
                .scope()
                .with_columns(&ctx, |columns| evaluate(mode, &ctx.values, columns));
            assert!(result.is_err());
            if mode == 2 {
                assert_eq!(
                    result,
                    Err(EvalError::Unsupported("bounded-staleness child"))
                );
            }
            assert_eq!(*ctx.events.borrow(), ["left", "right", "extra"][..=failed]);
        }
    }
    for values in [
        vec![Datum::Null, Datum::MaxValue],
        vec![Datum::MaxValue, Datum::Null],
    ] {
        let ctx = context(values, None);
        assert_eq!(
            execution
                .scope()
                .with_columns(&ctx, |columns| evaluate(0, &ctx.values, columns)),
            Ok(Datum::Null)
        );
        assert!(ctx.events.borrow().is_empty());
    }
    let ctx = context(vec![Datum::Time(left), Datum::Int(1)], None);
    assert_eq!(
        execution
            .scope()
            .with_columns(&ctx, |columns| evaluate(0, &ctx.values, columns)),
        Err(EvalError::Unsupported(
            "TIDB_BOUNDED_STALENESS arguments reached the signature without ETDatetime casts"
        ))
    );
    assert!(ctx.events.borrow().is_empty());
    let invalid = Time::from_date_checked(2020, 0, 1, 0, 0, 0, 0, TimeType::DateTime, 0).unwrap();
    let ctx = context(vec![Datum::Time(invalid), Datum::Time(right)], None);
    assert_eq!(
        execution
            .scope()
            .with_columns(&ctx, |columns| evaluate(0, &ctx.values, columns)),
        Ok(Datum::Null)
    );
    assert_eq!(*ctx.events.borrow(), vec!["policy", "warning"]);
    assert_eq!(
        *ctx.warnings.borrow(),
        vec![(1292, format!("Incorrect datetime value: '{invalid}'"))]
    );
    let ctx = context(vec![Datum::Time(right), Datum::Time(left)], None);
    assert_eq!(
        execution
            .scope()
            .with_columns(&ctx, |columns| evaluate(0, &ctx.values, columns)),
        Ok(Datum::Null)
    );
    assert!(ctx.events.borrow().is_empty());
}

#[cfg(test)]
mod tests;
