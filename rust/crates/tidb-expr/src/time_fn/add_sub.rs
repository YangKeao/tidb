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

//! `ADDTIME`, `SUBTIME`, `TIMESTAMP`, `TIMESTAMPADD` and `SYSDATE`, from
//! `pkg/expression/builtin_time.go`.
//!
//! # What makes these five one module
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
//!
//! # SYSDATE clock selection
//!
//! `builtinSysDateWithoutFspSig` calls `time.Now()` per evaluation, where
//! `NOW` returns the one statement timestamp. `tidb_sysdate_is_now` changes
//! `SYSDATE` into the latter before evaluation.

use tidb_datatype::{Datum, FieldType, FieldTypeCode};

use super::duration_parse::{
    self, get_fsp, is_duration, parse_datetime, parse_duration, GoDateTime, GoDuration, MAX_FSP,
    MIN_FSP,
};
use crate::coerce::coerce_str;
use crate::{Columns, EvalError};

/// The three temporal branches `getBf4TimeAddSub` reads off an argument's
/// `FieldType`, plus the `default` arm that covers everything else.
pub(crate) use tidb_query_expr::NativeTimeAddKind as TemporalKind;

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
    cols: &dyn Columns,
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
            "MICROSECOND" => super::calendar::whole_interval_amount(&upper, amount, cols)?
                .and_then(|value| value.checked_mul(1_000)),
            "SECOND" => super::calendar::second_interval_micros(amount)?
                .and_then(|value| value.checked_mul(1_000)),
            "MINUTE" => super::calendar::whole_interval_amount(&upper, amount, cols)?
                .and_then(|value| value.checked_mul(60_000_000_000)),
            "HOUR" => super::calendar::whole_interval_amount(&upper, amount, cols)?
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
    use tidb_query_expr::{NativeTimeAddMetadata, NativeTimeAddResult, NativeTimeAddWarning};

    let sources = std::cell::RefCell::new((None, None));
    crate::tikv::evaluate_prepared_args_in(
        cols,
        || {
            if vals.len() != 2 {
                return Err(EvalError::Unsupported("bad function arity"));
            }
            let metadata = NativeTimeAddMetadata {
                left: kinds[0],
                right: kinds[1],
                row_path,
                right_binary: matches!(vals[1], Datum::BinaryLiteral(_) | Datum::Bit(_)),
            }
            .encode();
            if kinds[1] == TemporalKind::Datetime {
                // This signature never coerces either value. Its real static
                // metadata enters the worker, not a manufactured NULL operand.
                return Ok((
                    crate::tikv::EvaluatedBytesOp::TimeAddRightDatetimeNative,
                    crate::tikv::EvaluatedArgs::Int(Some(metadata)),
                ));
            }
            // Preserve the eager tuple: a NULL left still coerces the right,
            // while an error on the left prevents right coercion.
            let (left, right) = (coerce_str(&vals[0])?, coerce_str(&vals[1])?);
            let args = crate::tikv::EvaluatedArgs::BytesBytesInt(
                left.as_ref().map(|value| value.as_bytes().to_vec()),
                right.as_ref().map(|value| value.as_bytes().to_vec()),
                Some(metadata),
            );
            *sources.borrow_mut() = (left, right);
            let operation = if sign < 0 {
                crate::tikv::EvaluatedBytesOp::SubTimeNative
            } else {
                crate::tikv::EvaluatedBytesOp::AddTimeNative
            };
            Ok((operation, args))
        },
        |computed| {
            let Some(bytes) = computed.into_bytes()? else {
                return Ok(Datum::Null);
            };
            let report = tidb_query_expr::decode_native_time_add_result(&bytes)
                .ok_or_else(crate::tikv::native_time_result_contract_error)?;
            match report {
                NativeTimeAddResult::Value(value) => Ok(Datum::new_string(value)),
                NativeTimeAddResult::Warning(warning) => {
                    let sources = sources.borrow();
                    let source = match warning {
                        NativeTimeAddWarning::TruncatedRight => sources.1.as_deref(),
                        _ => sources.0.as_deref(),
                    }
                    .ok_or_else(crate::tikv::native_time_result_contract_error)?;
                    let message = match warning {
                        NativeTimeAddWarning::TruncatedLeft
                        | NativeTimeAddWarning::TruncatedRight => format!(
                            "Truncated incorrect time value: '{}'",
                            tidb_datatype::warning_subject_byte_cap(source)
                        ),
                        NativeTimeAddWarning::IncorrectTimeLeft => {
                            format!("Incorrect time value: '{source}'")
                        }
                        NativeTimeAddWarning::IncorrectDateTimeLeft => {
                            format!("Incorrect datetime value: '{source}'")
                        }
                    };
                    // ADDTIME/SUBTIME append even in strict mode; unlike TIME,
                    // these warnings never consult the truncation policy.
                    cols.append_warning(1292, &message);
                    Ok(Datum::Null)
                }
            }
        },
    )
}

#[cfg(test)]
#[test]
fn add_sub_workers_preserve_signatures_demand_and_direct_warnings() {
    use std::cell::{Cell, RefCell};
    use TemporalKind::{Date, Datetime, Duration, Other};
    struct Policy {
        warnings: RefCell<Vec<(u16, String)>>,
        policy_reads: Cell<usize>,
    }
    impl Columns for Policy {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn now(&self) -> Option<(i64, u32, i32)> {
            panic!("ADDTIME must not read a clock")
        }
        fn time_zone(&self) -> crate::context::SessionTimeZone {
            panic!("ADDTIME leaf must not read a zone")
        }
        fn truncate_level(&self) -> crate::context::ErrorLevel {
            self.policy_reads.set(self.policy_reads.get() + 1);
            crate::context::ErrorLevel::Error
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
        let ctx = Policy {
            warnings: RefCell::new(Vec::new()),
            policy_reads: Cell::new(0),
        };
        let check = |values: &[Datum], kinds, sign, row, expected: Datum, warning: Option<&str>| {
            let result = execution.scope().with_columns(&ctx, |columns| {
                add_sub_time(values, kinds, sign, row, columns)
            });
            if slots == 1 {
                assert_eq!(result.unwrap(), expected, "{kinds:?} sign={sign} row={row}");
            } else {
                let error = result.expect_err("all add/sub outcomes must use the supplied scope");
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
            let expected_warnings = if slots == 1 {
                warning
                    .map(|message| vec![(1292, message.to_owned())])
                    .unwrap_or_default()
            } else {
                Vec::new()
            };
            assert_eq!(ctx.warnings.take(), expected_warnings);
            assert_eq!(
                ctx.policy_reads.get(),
                0,
                "these warnings ignore even strict truncation policy"
            );
        };
        // Four left kinds crossed with the three effective right signatures.
        for (left_kind, left, added, subtracted) in [
            (
                Datetime,
                "2020-01-01 01:00:00",
                "2020-01-01 01:00:01",
                "2020-01-01 00:59:59",
            ),
            (
                Date,
                "2020-01-01",
                "2020-01-01 00:00:01",
                "2019-12-31 23:59:59",
            ),
            (Duration, "01:00:00", "01:00:01", "00:59:59"),
            (Other, "01:00:00", "01:00:01", "00:59:59"),
        ] {
            for right_kind in [Datetime, Duration, Other] {
                for (sign, text) in [(1, added), (-1, subtracted)] {
                    let (right, expected) = if right_kind == Datetime {
                        (Datum::new_bytes(vec![0xff]), Datum::Null)
                    } else {
                        (Datum::new_string("00:00:01"), Datum::new_string(text))
                    };
                    check(
                        &[Datum::new_string(left), right],
                        [left_kind, right_kind],
                        sign,
                        true,
                        expected,
                        None,
                    );
                }
            }
        }
        check(
            &[Datum::new_bytes(vec![0xff]), Datum::MinNotNull],
            [Other, Datetime],
            1,
            false,
            Datum::Null,
            None,
        );
        for (row, sign, expected) in [
            (true, 1, Datum::Null),
            (false, 1, Datum::new_string("2020-01-01 20:00:00")),
            (true, -1, Datum::new_string("2020-01-01 00:00:00")),
        ] {
            check(
                &[
                    Datum::new_string("2020-01-01 10:00:00"),
                    Datum::new_string("2020-01-01 10:00:00"),
                ],
                [Other, Other],
                sign,
                row,
                expected,
                None,
            );
        }
        for (row, sign, fraction) in [
            (true, 1, "100001"),
            (false, 1, "1"),
            (true, -1, "099999"),
            (false, -1, "0"),
        ] {
            check(
                &[
                    Datum::new_string("2020-01-01 00:00:00.1"),
                    Datum::new_string("00:00:00.000001"),
                ],
                [Datetime, Duration],
                sign,
                row,
                Datum::new_string(format!("2020-01-01 00:00:00.{fraction}")),
                None,
            );
        }
        for (left, right, kinds, warning) in [
            (
                "bad-left",
                "bad-right",
                [Duration, Other],
                Some("Truncated incorrect time value: 'bad-left'"),
            ),
            (
                "bad-left",
                "bad-right",
                [Other, Other],
                Some("Truncated incorrect time value: 'bad-right'"),
            ),
            ("bad-left", "bad-right", [Datetime, Other], None),
            ("bad-left", "00:00:01", [Datetime, Other], None),
            (
                "bad-left",
                "839:00:00",
                [Date, Other],
                Some("Truncated incorrect time value: '839:00:00'"),
            ),
            (
                "bad-left",
                "00:00:01",
                [Other, Other],
                Some("Incorrect datetime value: 'bad-left'"),
            ),
            (
                "18446744073709551616",
                "00:00:01",
                [Other, Other],
                Some("Incorrect time value: '18446744073709551616'"),
            ),
        ] {
            check(
                &[Datum::new_string(left), Datum::new_string(right)],
                kinds,
                1,
                true,
                Datum::Null,
                warning,
            );
        }
        let binary = tidb_datatype::BinaryLiteral::from_uint(u64::from(b'A'), None);
        for value in [Datum::BinaryLiteral(binary.clone()), Datum::Bit(binary)] {
            check(
                &[Datum::new_string("bad-left"), value.clone()],
                [Other, Other],
                1,
                true,
                Datum::Null,
                None,
            );
            check(
                &[Datum::new_string("bad-left"), value],
                [Other, Duration],
                1,
                true,
                Datum::Null,
                Some("Truncated incorrect time value: 'A'"),
            );
        }
        for values in [
            [Datum::Null, Datum::new_string("bad-right")],
            [Datum::new_string("bad-left"), Datum::Null],
        ] {
            check(&values, [Other, Other], 1, true, Datum::Null, None);
        }
        for values in [vec![], vec![Datum::Null], vec![Datum::Null; 3]] {
            assert!(matches!(
                execution.scope().with_columns(&ctx, |columns| add_sub_time(
                    &values,
                    [Other, Datetime],
                    1,
                    true,
                    columns
                )),
                Err(EvalError::Unsupported("bad function arity"))
            ));
        }
        for (values, expected) in [
            (
                [Datum::Null, Datum::new_bytes(vec![0xff])],
                "invalid UTF-8 byte datum",
            ),
            (
                [Datum::new_bytes(vec![0xff]), Datum::MinNotNull],
                "invalid UTF-8 byte datum",
            ),
            (
                [Datum::Null, Datum::MinNotNull],
                "range sentinel string coercion",
            ),
        ] {
            assert!(
                matches!(execution.scope().with_columns(&ctx, |columns| add_sub_time(&values, [Other, Other], 1, true, columns)), Err(EvalError::Unsupported(message)) if message == expected)
            );
        }
        assert!(ctx.warnings.borrow().is_empty());
        assert_eq!(ctx.policy_reads.get(), 0);
    }
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

/// `builtinTimestampAddSig.evalString` + `addUnitToTime`.
pub(crate) fn timestamp_add(vals: &[Datum], cols: &dyn Columns) -> Result<Datum, EvalError> {
    if vals.len() != 3 {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    let (Some(unit), Some(amount)) = (coerce_str(&vals[0])?, number_of(&vals[1])?) else {
        return Ok(Datum::Null);
    };
    let Some(text) = coerce_str(&vals[2])? else {
        return Ok(Datum::Null);
    };
    let Some(base) = parse_datetime(&text) else {
        cols.append_warning(1292, &format!("Incorrect datetime value: '{text}'"));
        return Ok(Datum::Null);
    };
    // Go converts the third argument through `Time.GoTime` before calling
    // `addUnitToTime`. Zero dates and month/day-zero values therefore fail
    // before arithmetic (for example `TIMESTAMPADD(DAY, 28768, 0)` is NULL),
    // even though the signed day-number helper could otherwise produce a
    // seemingly valid year-78 result.
    if !base.in_range() {
        cols.append_warning(1292, &format!("Incorrect datetime value: '{text}'"));
        return Ok(Datum::Null);
    }
    let unit = unit.to_ascii_uppercase();
    let Some(result) = add_unit_to_time(&unit, base, amount) else {
        return Err(EvalError::Unsupported("TIMESTAMPADD unit"));
    };
    let Some(result) = result else {
        return Ok(Datum::Null);
    };
    if !result.in_range() {
        cols.append_warning(
            1292,
            &format!(
                "Incorrect time value: '{{{} {} {} {} {} {} {}}}'",
                result.year,
                result.month,
                result.day,
                result.hour,
                result.minute,
                result.second,
                result.micros
            ),
        );
        return Ok(Datum::Null);
    }
    // Go: `fsp := types.DefaultFsp`, raised to `MaxFsp` when the result
    // carries a microsecond.
    let fsp = if result.micros == 0 { MIN_FSP } else { MAX_FSP };
    Ok(Datum::new_string(GoDateTime { fsp, ..result }.format()))
}

/// Go `addUnitToTime`. The outer `None` is an unknown unit (Go's
/// `ErrWrongValue`); the inner `None` is its `overflow` return.
fn add_unit_to_time(unit: &str, base: GoDateTime, amount: f64) -> Option<Option<GoDateTime>> {
    // Go computes BOTH: `s` is the truncated microsecond count, used only by
    // SECOND, and `v` is the rounded whole count every other unit uses.
    let truncated_micros = (amount * 1_000_000.0).trunc();
    let rounded = amount.round();
    let micros = match unit {
        "MICROSECOND" => rounded,
        "SECOND" => truncated_micros,
        "MINUTE" => rounded * 60_000_000.0,
        "HOUR" => rounded * 3_600_000_000.0,
        "DAY" => rounded * 86_400_000_000.0,
        "WEEK" => rounded * 7.0 * 86_400_000_000.0,
        "MONTH" => return Some(add_months(base, rounded, true)),
        "QUARTER" => return Some(add_months(base, rounded * 3.0, false)),
        "YEAR" => return Some(add_months(base, rounded * 12.0, false)),
        _ => return None,
    };
    if !micros.is_finite() || micros.abs() > 9e18 {
        return Some(None);
    }
    Some(base.add(GoDuration {
        micros: micros as i64,
        fsp: base.fsp,
    }))
}

/// The MONTH/QUARTER/YEAR arms of `addUnitToTime`. Go's MONTH arm goes
/// through `types.AddDate`, which CLAMPS to the target month's last day
/// (`2020-01-31 + 1 MONTH` is `2020-02-29`), while its QUARTER and YEAR arms
/// go through Go's own `time.Time.AddDate`, which OVERFLOWS
/// (`2020-02-29 + 1 YEAR` is `2021-03-01`). Both were captured.
fn add_months(base: GoDateTime, months: f64, clamp: bool) -> Option<GoDateTime> {
    if !months.is_finite() || months.abs() > 1e6 {
        return None;
    }
    let total = base.year * 12 + i64::from(base.month) - 1 + months as i64;
    if total < 0 {
        return None;
    }
    let year = total / 12;
    let month = (total % 12 + 1) as u32;
    let day = if clamp {
        base.day.min(last_day_of_month(year, month))
    } else {
        base.day
    };
    // A day past the target month's end rolls into the next month here,
    // which is exactly what Go's `time.Time.AddDate` normalization does.
    let (year, month, day) =
        duration_parse::date_from_daynr(duration_parse::daynr(year, month, 1) + i64::from(day) - 1);
    Some(GoDateTime {
        year,
        month,
        day,
        ..base
    })
}

fn last_day_of_month(year: i64, month: u32) -> u32 {
    let next = if month == 12 {
        duration_parse::daynr(year + 1, 1, 1)
    } else {
        duration_parse::daynr(year, month + 1, 1)
    };
    (next - duration_parse::daynr(year, month, 1)) as u32
}

fn number_of(value: &Datum) -> Result<Option<f64>, EvalError> {
    Ok(match value {
        Datum::Null => None,
        Datum::Int(v) => Some(*v as f64),
        Datum::UInt(v) => Some(*v as f64),
        Datum::Real(v) => Some(*v),
        Datum::Float32(v) => Some(*v),
        Datum::Decimal(d) => Some(d.to_f64()),
        _ => coerce_str(value)?.map(|text| text.trim().parse::<f64>().unwrap_or(0.0)),
    })
}

/// `builtinSysDateWithFspSig`/`builtinSysDateWithoutFspSig`: `time.Now()` in
/// the session zone, ROUNDED half-up to `fsp` digits -- not the statement
/// clock `NOW` reads, which is why two `SYSDATE()` calls in one statement can
/// differ and `SYSDATE() = NOW()` is `0` on a session whose statement clock
/// was taken earlier. With `tidb_sysdate_is_now=ON`, Go builds `NOW` instead,
/// including its truncating FSP behavior.
pub(crate) fn sysdate(vals: &[Datum], cols: &dyn Columns) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_prepared_args_in(
        cols,
        || {
            if cols.sysdate_is_now() {
                return super::prepare_now_args(vals, cols);
            }
            if vals.len() > 1 {
                return Err(EvalError::Unsupported("bad function arity"));
            }
            let fsp = match vals.first() {
                None | Some(Datum::Null) => 0,
                Some(Datum::Int(value)) if (0..=i64::from(MAX_FSP)).contains(value) => {
                    *value as u32
                }
                Some(Datum::UInt(value)) if *value <= MAX_FSP as u64 => *value as u32,
                Some(value) => {
                    let converted = value.to_i64().map_err(|_| {
                        EvalError::Unsupported("bad fractional-seconds-precision argument")
                    })?;
                    if !(0..=i64::from(MAX_FSP)).contains(&converted.value) {
                        return Err(EvalError::Unsupported(
                            "bad fractional-seconds-precision argument",
                        ));
                    }
                    converted.value as u32
                }
            };
            // Retain the statement's frozen offset, but capture the actual
            // live instant. The worker owns offset addition and half-up rounding.
            let (_, _, tz_offset) = cols.now().ok_or(EvalError::Unsupported(
                "SYSDATE needs the session clock, which is not wired here",
            ))?;
            let elapsed = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|_| EvalError::Unsupported("the host clock is before the Unix epoch"))?;
            Ok((
                crate::tikv::EvaluatedBytesOp::SysdateNative,
                crate::tikv::prepare_clock_args(
                    (elapsed.as_secs() as i64, elapsed.subsec_nanos(), tz_offset),
                    Some(fsp),
                )?,
            ))
        },
        |computed| {
            Ok(computed
                .into_bytes()?
                .map_or(Datum::Null, Datum::new_string))
        },
    )
}

#[cfg(test)]
mod sysdate_source_tests {
    use super::sysdate;
    use crate::{Columns, Datum, EvalError};

    struct StatementClock(i64);

    impl Columns for StatementClock {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }

        fn now(&self) -> Option<(i64, u32, i32)> {
            Some((self.0, 0, 0))
        }
    }

    struct AliasedStatementClock;

    impl Columns for AliasedStatementClock {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }

        fn now(&self) -> Option<(i64, u32, i32)> {
            Some((1_700_000_000, 654_999_999, 8 * 60 * 60))
        }

        fn sysdate_is_now(&self) -> bool {
            true
        }
    }

    fn host_now(fsp: u32) -> String {
        let elapsed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap();
        super::super::format_datetime(elapsed.as_secs() as i64, elapsed.subsec_nanos(), fsp, true)
    }

    /// Go `TestSysDate`: the function reads the host clock rather than the
    /// statement `timestamp`, accepts FSP 0 through 6, and rejects a negative
    /// constant. Rust has one evaluator, so the source's row/vector loops
    /// converge on this same boundary.
    #[test]
    fn test_sys_date() {
        for statement_timestamp in [1_234, 0] {
            let before = host_now(0);
            let result = sysdate(&[], &StatementClock(statement_timestamp)).unwrap();
            let after = host_now(0);
            let Datum::String(result) = result else {
                panic!("SYSDATE must return its datetime string");
            };
            let result = result.as_utf8().unwrap();
            assert!(before.as_str() <= result && result <= after.as_str());
        }

        for fsp in 0..=6 {
            let before = host_now(fsp);
            let result = sysdate(&[Datum::Int(i64::from(fsp))], &StatementClock(0)).unwrap();
            let after = host_now(fsp);
            let Datum::String(result) = result else {
                panic!("SYSDATE({fsp}) must return its datetime string");
            };
            let result = result.as_utf8().unwrap();
            assert!(
                before.as_str() <= result && result <= after.as_str(),
                "fsp={fsp}"
            );
        }

        assert_eq!(
            sysdate(&[Datum::Int(-2)], &StatementClock(0)),
            Err(EvalError::Unsupported(
                "bad fractional-seconds-precision argument"
            ))
        );
    }

    #[test]
    fn sysdate_is_now_uses_the_statement_clock_and_now_rounding() {
        assert_eq!(
            sysdate(&[], &AliasedStatementClock).unwrap(),
            Datum::new_string("2023-11-15 06:13:20")
        );
        assert_eq!(
            sysdate(&[Datum::Int(3)], &AliasedStatementClock).unwrap(),
            Datum::new_string("2023-11-15 06:13:20.654")
        );
        assert_eq!(
            sysdate(&[Datum::Int(6)], &AliasedStatementClock).unwrap(),
            Datum::new_string("2023-11-15 06:13:20.654999")
        );
    }
}
