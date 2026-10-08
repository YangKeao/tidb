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

use super::duration_parse::MAX_FSP;
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
    crate::tikv::eval_date_add_duration_in(cols, unit, date, amount, amount_type, sign, result_fsp)
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

/// `timestampFunctionClass`: `builtinTimestamp1ArgSig` /
/// `builtinTimestamp2ArgsSig`. The result is a DATETIME whose fsp is the
/// argument's own; the second argument is a DURATION added to it, and it is
/// rejected outright when it carries a date part.
pub(crate) fn timestamp(vals: &[Datum], cols: &dyn Columns) -> Result<Datum, EvalError> {
    use crate::tikv::{EvaluatedArgs, EvaluatedBytesOp as Op};
    use tidb_query_expr::NativeTimestampResult;
    crate::tikv::evaluate_prepared_args_scoped_in(
        cols,
        || {
            if vals.is_empty() || vals.len() > 2 {
                return Err(EvalError::Unsupported("bad function arity"));
            }
            let Some(text) = coerce_str(&vals[0])? else {
                return Ok((Op::TimestampNullNative, EvaluatedArgs::Bytes(None)));
            };
            // This is actual source metadata, not a parsed value. The worker
            // owns the float-string versus ordinary temporal parser decision.
            let is_float = matches!(
                vals[0],
                Datum::Int(_)
                    | Datum::UInt(_)
                    | Datum::Decimal(_)
                    | Datum::Real(_)
                    | Datum::Float32(_)
            );
            Ok((
                if vals.len() == 1 {
                    Op::Timestamp1Native
                } else {
                    Op::Timestamp2BaseNative
                },
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
            let result = tidb_query_expr::decode_native_timestamp_result(&bytes)
                .ok_or_else(crate::tikv::native_time_result_contract_error)?;
            match result {
                NativeTimestampResult::Value(value) if vals.len() == 1 => {
                    Ok(Datum::new_string(value))
                }
                NativeTimestampResult::Base(_) if vals.len() == 2 => {
                    // Preserve both stage ordering and the same owner/scope,
                    // including a one-shot invocation originating at NoColumns.
                    // A zero year still demands RHS coercion before its gate.
                    crate::tikv::evaluate_prepared_args_in(
                        scoped_cols,
                        || {
                            let second = coerce_str(&vals[1])?;
                            Ok((
                                Op::Timestamp2AddNative,
                                EvaluatedArgs::Bytes2(Some(bytes), second.map(String::into_bytes)),
                            ))
                        },
                        |computed| {
                            Ok(computed
                                .into_bytes()?
                                .map_or(Datum::Null, Datum::new_string))
                        },
                    )
                }
                NativeTimestampResult::Warning { code, message } => {
                    scoped_cols.append_warning(code, message);
                    Ok(Datum::Null)
                }
                _ => Err(crate::tikv::native_time_result_contract_error()),
            }
        },
    )
}

/// `builtinTimestampAddSig.evalString` + `addUnitToTime`.
pub(crate) fn timestamp_add(vals: &[Datum], cols: &dyn Columns) -> Result<Datum, EvalError> {
    use tidb_query_expr::NativeTimestampAddResult;

    let source = std::cell::RefCell::new(None);
    crate::tikv::evaluate_prepared_args_in(
        cols,
        || {
            if vals.len() != 3 {
                return Err(EvalError::Unsupported("bad function arity"));
            }
            // A NULL unit still demands the original numeric coercion. Neither
            // prefix NULL demands the third value's text at this leaf.
            let (unit, amount) = (coerce_str(&vals[0])?, number_of(&vals[1])?);
            let amount = amount.map(|value| value.to_bits() as i64);
            if unit.is_none() || amount.is_none() {
                return Ok((
                    crate::tikv::EvaluatedBytesOp::TimestampAddPrefixNullNative,
                    crate::tikv::EvaluatedArgs::BytesInt(unit.map(String::into_bytes), amount),
                ));
            }
            let text = coerce_str(&vals[2])?;
            let date = text.as_ref().map(|value| value.as_bytes().to_vec());
            *source.borrow_mut() = text;
            Ok((
                crate::tikv::EvaluatedBytesOp::TimestampAddNative,
                crate::tikv::EvaluatedArgs::BytesBytesInt(
                    unit.map(String::into_bytes),
                    date,
                    amount,
                ),
            ))
        },
        |computed| {
            let Some(bytes) = computed.into_bytes()? else {
                return Ok(Datum::Null);
            };
            let report = tidb_query_expr::decode_native_timestamp_add_result(&bytes)
                .ok_or_else(crate::tikv::native_time_result_contract_error)?;
            match report {
                NativeTimestampAddResult::Value(value) => Ok(Datum::new_string(value)),
                NativeTimestampAddResult::UnknownUnit => {
                    Err(EvalError::Unsupported("TIMESTAMPADD unit"))
                }
                NativeTimestampAddResult::IncorrectDateTimeInput => {
                    let source = source.borrow();
                    let text = source
                        .as_deref()
                        .ok_or_else(crate::tikv::native_time_result_contract_error)?;
                    cols.append_warning(1292, &format!("Incorrect datetime value: '{text}'"));
                    Ok(Datum::Null)
                }
                NativeTimestampAddResult::IncorrectTimeResult(message) => {
                    cols.append_warning(1292, message);
                    Ok(Datum::Null)
                }
            }
        },
    )
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
