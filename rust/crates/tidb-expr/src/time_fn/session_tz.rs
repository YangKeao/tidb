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

#[cfg(test)]
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
    crate::tikv::eval_from_unixtime_values_in(cols, vals)
}

/// `UNIX_TIMESTAMP([datetime])`.
pub(crate) fn unix_timestamp(vals: &[Datum], cols: &dyn Columns) -> Result<Datum, EvalError> {
    crate::tikv::eval_unix_timestamp_values_in(cols, vals)
}

/// `TIDB_PARSE_TSO(tso)`: the physical half as a full-precision native
/// DATETIME in the session zone.
pub(super) fn tidb_parse_tso(vals: &[Datum], cols: &dyn Columns) -> Result<Datum, EvalError> {
    crate::tikv::tso_value::eval(tidb_query_expr::NativeTsoOperation::Physical, vals, cols)
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
