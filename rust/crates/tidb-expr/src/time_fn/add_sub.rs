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
        return kind_from_field_type(ft);
    }
    match value {
        Datum::Time(_) => TemporalKind::Datetime,
        Datum::Duration(_) => TemporalKind::Duration,
        _ => TemporalKind::Other,
    }
}

/// The original static signature classifier, shared with canonical source lowering.
pub(crate) fn kind_from_field_type(ft: &FieldType) -> TemporalKind {
    match ft.code() {
        FieldTypeCode::Datetime | FieldTypeCode::Timestamp => TemporalKind::Datetime,
        FieldTypeCode::Date | FieldTypeCode::NewDate => TemporalKind::Date,
        FieldTypeCode::Duration => TemporalKind::Duration,
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
    use tidb_query_expr::NativeDurationConstructionOperation as O;
    crate::tikv::duration_construction_value::eval(
        if sign < 0 {
            O::SubTime { kinds, row_path }
        } else {
            O::AddTime { kinds, row_path }
        },
        vals,
        cols,
    )
}

/// `timestampFunctionClass`: `builtinTimestamp1ArgSig` /
/// `builtinTimestamp2ArgsSig`. The result is a DATETIME whose fsp is the
/// argument's own; the second argument is a DURATION added to it, and it is
/// rejected outright when it carries a date part.
pub(crate) fn timestamp(vals: &[Datum], cols: &dyn Columns) -> Result<Datum, EvalError> {
    use tidb_query_expr::NativeDurationConstructionOperation as O;
    crate::tikv::duration_construction_value::eval(
        if vals.len() == 1 {
            O::Timestamp1
        } else {
            O::Timestamp2
        },
        vals,
        cols,
    )
}

/// `builtinTimestampAddSig.evalString` + `addUnitToTime`.
pub(crate) fn timestamp_add(vals: &[Datum], cols: &dyn Columns) -> Result<Datum, EvalError> {
    crate::tikv::duration_construction_value::eval(
        tidb_query_expr::NativeDurationConstructionOperation::TimestampAdd,
        vals,
        cols,
    )
}

pub(crate) fn number_of(value: &Datum) -> Result<Option<f64>, EvalError> {
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
    crate::tikv::clock_value::eval(tidb_query_expr::NativeClockOperation::Sysdate, vals, cols)
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
