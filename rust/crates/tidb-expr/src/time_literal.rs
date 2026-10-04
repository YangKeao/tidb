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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The typed temporal LITERALS `DATE 'lit'` and `TIMESTAMP 'lit'` (and their
//! ODBC spellings `{d 'lit'}` / `{ts 'lit'}`), which are NOT a
//! `CAST(lit AS DATE/DATETIME)`.
//!
//! Go builds a dedicated function class for each
//! (`pkg/expression/builtin_time.go`'s `dateLiteralFunctionClass` and
//! `timestampLiteralFunctionClass`), and the three ways they differ from the
//! cast are all observable:
//!
//! 1. A REGEX GATE runs before any parsing. `timestampPattern` requires a
//!    date AND an hour, so `TIMESTAMP '2024-01-01'` -- a perfectly good
//!    `CAST` input -- is `ErrWrongValue2` (1525). `datePattern` refuses
//!    anything carrying a time, so `{d '2024-01-01 01:12:31'}` is
//!    `ErrWrongValue` (1292).
//! 2. A PARSE FAILURE IS A HARD ERROR, where the cast reports a warning and
//!    answers `NULL` (`crate::cast::invalid_time_warning`). `TIMESTAMP
//!    '2024-01-01 14:00:00+14:01'` fails the statement.
//! 3. THE LITERAL'S OWN FRACTIONAL PRECISION SURVIVES.
//!    `types.GetFsp(str)` picks the fsp and `setDecimalAndFlenForDatetime`
//!    puts it in the result type, so `TIMESTAMP '2024-01-01 14:00:00.010'`
//!    prints `14:00:00.010`. `CAST(... AS DATETIME)` has decimal 0 and
//!    prints no fraction at all -- which is why the fraction cannot be
//!    restored by changing the cast.
//! 4. THE LITERAL IS TEMPORALLY TYPED. Go's two function classes declare
//!    `types.ETDatetime` and then `setDecimalAndFlenForDate` /
//!    `setDecimalAndFlenForDatetime(tm.Fsp())`, so `DATE 'lit'` reports
//!    `mysql.TypeDate` and `TIMESTAMP 'lit'` `mysql.TypeDatetime` -- exactly
//!    the types a `date`/`datetime` COLUMN reports. Every consumer that
//!    branches on "is this argument temporal?" therefore treats the literal
//!    and the column alike. This module returns that `FieldType` beside the
//!    value for that reason; folding to a `VarString` (what it used to do)
//!    made a literal invisible to `resolveType4Extremum`, to comparison
//!    refinement, and to anything else that reads
//!    `types.IsTypeTemporal(arg.GetType())`.
//!
//! Go does all three while BUILDING the expression, and so does this: the
//! literal folds to its formatted value here, and no per-row work remains.
//!
//! The parse runs in the SESSION's time zone, as Go's does: the rewriter
//! hands it down through [`crate::rewriter::ColumnResolver::time_zone`], the
//! fold-time sibling of the [`crate::Columns::time_zone`] the cast path
//! consults at eval time. A literal carrying an explicit offset
//! (`'... 14:00:00+02:00'`) normalizes into that zone, and a fractional
//! carry rounds the INSTANT in it (see [`timestamp_literal_in`]).
//!
//! The SQL mode follows the statement too. `ALLOW_INVALID_DATES` controls
//! calendar validation while `DATE` applies `NO_ZERO_DATE` and
//! `NO_ZERO_IN_DATE` after parsing, in the same order as Go's literal
//! function.

use crate::{Columns, EvalError};
use tidb_datatype::{CoreTime, FieldType, FieldTypeCode, Time, TimeType};

/// Go `mysql.MaxDateWidth`: `'YYYY-MM-DD'`.
const MAX_DATE_WIDTH: i64 = 10;
/// Go `mysql.MaxDatetimeWidthNoFsp`: `'YYYY-MM-DD HH:MM:SS'`.
const MAX_DATETIME_WIDTH_NO_FSP: i64 = 19;

/// Go `builtinDateLiteralSig`: the value of `DATE 'lit'`, or the error that
/// rejects the whole statement.
///
/// Test-only convenience retaining the original one-shot context. Production
/// rewriting uses [`date_literal_in`] with its resolver's optional execution
/// context and the same explicitly captured timezone and modes.
#[cfg(test)]
pub(crate) fn date_literal(
    text: &str,
    zone: &tidb_datatype::SessionTimeZone,
    modes: tidb_datatype::DateModes,
) -> Result<(Time, FieldType), EvalError> {
    date_literal_in(text, zone, modes, &crate::NoColumns)
}

/// Execute the complete literal policy under the supplied scope. The explicit
/// zone and modes remain authoritative; `ctx` supplies no temporal settings.
pub(crate) fn date_literal_in(
    text: &str,
    zone: &tidb_datatype::SessionTimeZone,
    modes: tidb_datatype::DateModes,
    ctx: &dyn Columns,
) -> Result<(Time, FieldType), EvalError> {
    crate::tikv::evaluate_prepared_args_in(
        ctx,
        || {
            Ok((
                crate::tikv::EvaluatedBytesOp::DateLiteralNative,
                literal_args(text, zone, modes),
            ))
        },
        |computed| {
            let time = literal_time(computed, TimeType::Date)?;
            let mut ft = FieldType::new(FieldTypeCode::Date);
            ft.set_decimal(0);
            ft.set_flen(MAX_DATE_WIDTH);
            Ok((time, ft))
        },
    )
}

/// Go `builtinTimestampLiteralSig`: the value of `TIMESTAMP 'lit'`, or the
/// error that rejects the whole statement.
///
/// The two codes are Go's own and are NOT interchangeable: the regex gate is
/// `ErrWrongValue2` (1525) and the parse failure is `ErrWrongValue` (1292),
/// which is why the recorded topic carries both against this one syntax.
#[cfg(test)]
pub(crate) fn timestamp_literal(
    text: &str,
    zone: &tidb_datatype::SessionTimeZone,
    modes: tidb_datatype::DateModes,
) -> Result<(Time, FieldType), EvalError> {
    timestamp_literal_in(text, zone, modes, &crate::NoColumns)
}

/// Builds a timestamp literal without fetching context timezone or modes a
/// second time after the rewriter already captured them.
pub(crate) fn timestamp_literal_in(
    text: &str,
    zone: &tidb_datatype::SessionTimeZone,
    modes: tidb_datatype::DateModes,
    ctx: &dyn Columns,
) -> Result<(Time, FieldType), EvalError> {
    crate::tikv::evaluate_prepared_args_in(
        ctx,
        || {
            Ok((
                crate::tikv::EvaluatedBytesOp::TimestampLiteralNative,
                literal_args(text, zone, modes),
            ))
        },
        |computed| {
            let time = literal_time(computed, TimeType::DateTime)?;
            let fsp = i64::from(time.fsp());
            let mut ft = FieldType::new(FieldTypeCode::Datetime);
            ft.set_decimal_under_limit(fsp);
            ft.set_flen_under_limit(MAX_DATETIME_WIDTH_NO_FSP + fsp + i64::from(fsp > 0));
            Ok((time, ft))
        },
    )
}

/// The parse runs in the SESSION's zone, as Go's does.
///
/// Go resolves `DATE 'lit'`/`TIMESTAMP 'lit'` in `getFunction`, whose `ctx`
/// carries the session location, and a literal whose fraction is wider than
/// `fsp` ROUNDS -- with the carry applied to the INSTANT in that zone. So
/// when the carry lands on a DST transition the two answers differ.
/// CAPTURED from real TiDB:
///
/// ```text
/// select timestamp '2011-03-13 01:59:59.9999999'
///   time_zone='UTC'                 2011-03-13 02:00:00.000000
///   time_zone='America/Los_Angeles' 2011-03-13 03:00:00.000000
/// select timestamp '2011-11-06 01:59:59.9999999'
///   time_zone='UTC'                 2011-11-06 02:00:00.000000
///   time_zone='America/Los_Angeles' 2011-11-06 01:00:00.000000
/// ```
///
/// This used to hardcode `chrono::Utc` -- the dropped-Context seam -- and
/// answered the UTC row for every session. The zone now arrives from
/// [`crate::rewriter::ColumnResolver::time_zone`], the fold-time sibling of
/// the [`crate::Columns::time_zone`] the eval-time cast already consults, so
/// the same statement rounds identically whichever of the two paths builds
/// it. An explicit `+HH:MM` offset in the literal likewise normalizes into
/// this zone rather than into UTC.
fn literal_args(
    text: &str,
    zone: &tidb_datatype::SessionTimeZone,
    modes: tidb_datatype::DateModes,
) -> crate::tikv::EvaluatedArgs {
    crate::tikv::EvaluatedArgs::TemporalText {
        value: text.as_bytes().to_vec(),
        modes: i64::from(modes.allow_invalid_dates)
            | (i64::from(modes.no_zero_date) << 1)
            | (i64::from(modes.no_zero_in_date) << 2),
        zone: zone.clone(),
    }
}

/// Decode only the shared result contract, then project its exact raw temporal
/// value into the native representation. There is no host reparse or validation.
fn literal_time(
    computed: crate::tikv::EvaluatedBytesResult,
    expected_kind: TimeType,
) -> Result<Time, EvalError> {
    use tidb_query_expr::NativeTemporalLiteralResult;
    let bytes = computed
        .into_bytes()?
        .ok_or_else(crate::tikv::native_time_result_contract_error)?;
    let result = tidb_query_expr::decode_native_temporal_literal_result(&bytes)
        .ok_or_else(crate::tikv::native_time_result_contract_error)?;
    match result {
        NativeTemporalLiteralResult::Value(value) => {
            if value.kind != expected_kind {
                return Err(crate::tikv::native_time_result_contract_error());
            }
            Ok(Time::from_raw_parts(
                CoreTime::from_raw(value.raw),
                value.kind,
                value.fsp,
            ))
        }
        NativeTemporalLiteralResult::WrongValue { code, message } => {
            Err(EvalError::WrongTemporalLiteral {
                code,
                message: message.to_owned(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn literal_workers_preserve_explicit_scope_and_captured_settings() {
        struct ScopeOnly;
        impl Columns for ScopeOnly {
            fn get(&self, _: &[String]) -> Option<crate::Datum> {
                panic!("literal has no column demand")
            }
            fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
                panic!("literal zone is already captured")
            }
            fn date_modes(&self) -> tidb_datatype::DateModes {
                panic!("literal modes are already captured")
            }
            fn append_warning(&self, _: u16, _: &str) {
                panic!("literal failures are hard errors")
            }
        }
        let zone = tidb_datatype::SessionTimeZone::Named(chrono_tz::America::Los_Angeles);
        let modes = tidb_datatype::DateModes::default();
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
            let ctx = ScopeOnly;
            for (timestamp, text, expected) in [
                (false, "2024-01-01", Ok(("2024-01-01", 10, 0))),
                (
                    true,
                    "2024-01-01 14:00:00.010",
                    Ok(("2024-01-01 14:00:00.010", 23, 3)),
                ),
                (
                    true,
                    "2024-01-01 14:00:00+02:00",
                    Ok(("2024-01-01 04:00:00", 19, 0)),
                ),
                (
                    true,
                    "2011-03-13 01:59:59.9999999",
                    Ok(("2011-03-13 03:00:00.000000", 26, 6)),
                ),
                (
                    false,
                    "2024-01-01 01:12:31",
                    Err((1292, "Incorrect date value: '2024-01-01 01:12:31'")),
                ),
                (
                    false,
                    "2020-02-30",
                    Err((1292, "Incorrect datetime value: '2020-2-30'")),
                ),
                (
                    true,
                    "2024-01-01",
                    Err((1525, "Incorrect datetime value: '2024-01-01'")),
                ),
                (
                    true,
                    "2024-01-01 14:00:00+14:01",
                    Err((
                        1292,
                        "Incorrect datetime value: '2024-01-01 14:00:00+14:01'",
                    )),
                ),
            ] {
                let result = execution.scope().with_columns(&ctx, |columns| {
                    if timestamp {
                        timestamp_literal_in(text, &zone, modes, columns)
                    } else {
                        date_literal_in(text, &zone, modes, columns)
                    }
                });
                if slots == 0 {
                    let error = result
                        .expect_err("guard must precede even invalid-pattern and parse policies");
                    let EvalError::ExpressionAdapterFailure(failure) = error else {
                        panic!("{text}: {error:?}")
                    };
                    assert_eq!(
                        failure.class(),
                        crate::ExpressionAdapterFailureClass::PoolResource
                    );
                    assert_eq!(
                        failure.origin(),
                        crate::ExpressionAdapterFailureOrigin::Pool
                    );
                    continue;
                }
                match expected {
                    Ok((shown, flen, fsp)) => {
                        let (time, field_type) = result.unwrap();
                        assert_eq!(time.to_string(), shown);
                        assert_eq!(
                            time.kind(),
                            if timestamp {
                                TimeType::DateTime
                            } else {
                                TimeType::Date
                            }
                        );
                        assert_eq!(
                            field_type.code(),
                            if timestamp {
                                FieldTypeCode::Datetime
                            } else {
                                FieldTypeCode::Date
                            }
                        );
                        assert_eq!((field_type.flen(), field_type.decimal()), (flen, fsp));
                        assert_eq!(i64::from(time.fsp()), fsp);
                    }
                    Err((expected_code, expected_message)) => {
                        let EvalError::WrongTemporalLiteral { code, message } = result.unwrap_err()
                        else {
                            panic!("{text}: expected original hard literal error")
                        };
                        assert_eq!(code, expected_code);
                        assert_eq!(message, expected_message);
                    }
                }
            }
            if slots == 1 {
                execution.scope().with_columns(&ctx, |columns| {
                    let permissive = tidb_datatype::DateModes {
                        allow_invalid_dates: true,
                        ..modes
                    };
                    assert_eq!(
                        date_literal_in("2017-2-31", &zone, permissive, columns)
                            .unwrap()
                            .0
                            .to_string(),
                        "2017-02-31"
                    );
                    for (text, strict) in [
                        (
                            "0000-00-00",
                            tidb_datatype::DateModes {
                                no_zero_date: true,
                                ..modes
                            },
                        ),
                        (
                            "2007-10-00",
                            tidb_datatype::DateModes {
                                no_zero_in_date: true,
                                ..modes
                            },
                        ),
                    ] {
                        assert!(matches!(
                            date_literal_in(text, &zone, strict, columns),
                            Err(EvalError::WrongTemporalLiteral { code: 1292, .. })
                        ));
                    }
                });
            }
        }
    }

    /// The printed value, which is all the pre-typing form of this module
    /// returned. The TYPE half is asserted separately in
    /// [`the_literal_reports_gos_own_temporal_type`].
    fn shown(result: Result<(Time, FieldType), EvalError>) -> String {
        result.unwrap().0.to_string()
    }

    /// The three properties this module exists for, one case each, taken from
    /// `tests/integrationtest/r/types/time.result`.
    #[test]
    fn literal_gates_and_fraction_follow_the_recording() {
        let utc = tidb_datatype::SessionTimeZone::utc();
        let modes = tidb_datatype::DateModes::default();
        // The regex gate: a date with no time is a 1525 for TIMESTAMP even
        // though CAST accepts it.
        assert!(matches!(
            timestamp_literal("2024-01-01", &utc, modes),
            Err(EvalError::WrongTemporalLiteral { code: 1525, .. })
        ));
        // A parse failure is a hard error, not a warning plus NULL.
        assert!(matches!(
            timestamp_literal("2024-01-01 14:00:00+14:01", &utc, modes),
            Err(EvalError::WrongTemporalLiteral { code: 1292, .. })
        ));
        // The literal's own fsp survives into the printed value.
        assert_eq!(
            shown(timestamp_literal("2024-01-01 14:00:00.010", &utc, modes)),
            "2024-01-01 14:00:00.010"
        );
        assert_eq!(
            shown(timestamp_literal("2024-01-01 14:00:00", &utc, modes)),
            "2024-01-01 14:00:00"
        );
        // DATE refuses a literal carrying a time part.
        assert!(matches!(
            date_literal("2024-01-01 01:12:31", &utc, modes),
            Err(EvalError::WrongTemporalLiteral { code: 1292, .. })
        ));
        assert_eq!(shown(date_literal("2024-01-01", &utc, modes)), "2024-01-01");
    }

    #[test]
    fn date_literal_uses_the_statement_sql_mode() {
        let utc = tidb_datatype::SessionTimeZone::utc();
        let permissive = tidb_datatype::DateModes::default();
        assert_eq!(
            shown(date_literal("0000-00-00", &utc, permissive)),
            "0000-00-00"
        );
        assert_eq!(
            shown(date_literal("2007-10-00", &utc, permissive)),
            "2007-10-00"
        );

        let no_zero_date = tidb_datatype::DateModes {
            no_zero_date: true,
            ..permissive
        };
        assert!(matches!(
            date_literal("0000-00-00", &utc, no_zero_date),
            Err(EvalError::WrongTemporalLiteral { code: 1292, ref message })
                if message == "Incorrect date value: '0000-00-00'"
        ));
        assert_eq!(
            shown(date_literal("2007-10-00", &utc, no_zero_date)),
            "2007-10-00"
        );

        let no_zero_in_date = tidb_datatype::DateModes {
            no_zero_in_date: true,
            ..permissive
        };
        assert_eq!(
            shown(date_literal("0000-00-00", &utc, no_zero_in_date)),
            "0000-00-00"
        );
        assert!(matches!(
            date_literal("2007-10-00", &utc, no_zero_in_date),
            Err(EvalError::WrongTemporalLiteral { code: 1292, ref message })
                if message == "Incorrect date value: '2007-10-00'"
        ));

        assert!(matches!(
            date_literal("2017-2-31", &utc, permissive),
            Err(EvalError::WrongTemporalLiteral { code: 1292, ref message })
                if message == "Incorrect datetime value: '2017-2-31'"
        ));
        assert_eq!(
            shown(date_literal(
                "2017-2-31",
                &utc,
                tidb_datatype::DateModes {
                    allow_invalid_dates: true,
                    ..permissive
                }
            )),
            "2017-02-31"
        );
    }

    /// The fold rounds in the SESSION zone, not in UTC: the capture in
    /// [`literal_args`]'s doc, replayed against both zones. The instants are DST
    /// TRANSITIONS on purpose -- a probe over ordinary instants shows no
    /// difference in ANY zone and is a false negative (the same trap the
    /// sibling test in `crate::cast` documents).
    #[test]
    fn the_fractional_carry_rounds_in_the_session_zone() {
        let utc = tidb_datatype::SessionTimeZone::utc();
        let la = tidb_datatype::SessionTimeZone::Named(chrono_tz::America::Los_Angeles);
        let modes = tidb_datatype::DateModes::default();
        for (input, in_utc, in_la) in [
            (
                "2011-03-13 01:59:59.9999999",
                "2011-03-13 02:00:00.000000",
                "2011-03-13 03:00:00.000000",
            ),
            (
                "2011-11-06 01:59:59.9999999",
                "2011-11-06 02:00:00.000000",
                "2011-11-06 01:00:00.000000",
            ),
        ] {
            assert_eq!(
                shown(timestamp_literal(input, &utc, modes)),
                in_utc,
                "{input}"
            );
            assert_eq!(
                shown(timestamp_literal(input, &la, modes)),
                in_la,
                "{input}"
            );
        }
        // An explicit offset in the literal normalizes into the session zone
        // (the divergence the module doc used to pin as UTC-only): 14:00 at
        // +02:00 is 12:00 UTC and 04:00 in Los Angeles (PST, -08:00).
        assert_eq!(
            shown(timestamp_literal("2024-01-01 14:00:00+02:00", &utc, modes)),
            "2024-01-01 12:00:00"
        );
        assert_eq!(
            shown(timestamp_literal("2024-01-01 14:00:00+02:00", &la, modes)),
            "2024-01-01 04:00:00"
        );
    }

    /// Go's `setDecimalAndFlenForDate` / `setDecimalAndFlenForDatetime`, the
    /// half a printed VALUE cannot show. `DATE 'lit'` is `mysql.TypeDate`
    /// with `MaxDateWidth` and scale 0; `TIMESTAMP 'lit'` is
    /// `mysql.TypeDatetime` whose scale is the LITERAL TEXT's own fsp and
    /// whose width grows by that fsp plus the `.` separator.
    ///
    /// The scale is read off the parsed `Time`, not off the text, so a
    /// literal whose fraction is wider than `MaxFsp` reports the CLAMPED 6 --
    /// the value rounds to six digits and the declared scale must agree with
    /// it, or the chunk cell and the header disagree.
    #[test]
    fn the_literal_reports_gos_own_temporal_type() {
        let utc = tidb_datatype::SessionTimeZone::utc();
        let modes = tidb_datatype::DateModes::default();
        let (_, date) = date_literal("2024-01-01", &utc, modes).unwrap();
        assert_eq!(date.code(), FieldTypeCode::Date);
        assert_eq!((date.flen(), date.decimal()), (10, 0));

        for (text, decimal, flen) in [
            ("2024-01-01 14:00:00", 0, 19),
            ("2024-01-01 14:00:00.010", 3, 23),
            ("2024-01-01 14:00:00.123456", 6, 26),
            // Wider than `MaxFsp`: the parse rounds to six digits, so the
            // reported scale is six and not the nine the text carries.
            ("2024-01-01 14:00:00.123456789", 6, 26),
        ] {
            let (time, ft) = timestamp_literal(text, &utc, modes).unwrap();
            assert_eq!(ft.code(), FieldTypeCode::Datetime, "{text}");
            assert_eq!((ft.flen(), ft.decimal()), (flen, decimal), "{text}");
            assert_eq!(i64::from(time.fsp()), decimal, "{text}");
        }
    }
}
