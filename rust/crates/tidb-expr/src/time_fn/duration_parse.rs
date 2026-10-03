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

//! `types.ParseDuration` and the microsecond-precision datetime arithmetic the
//! `ADDTIME`/`SUBTIME`/`TIMESTAMP` family needs, translated from
//! `pkg/types/time.go` (`matchDuration`, `canFallbackToDateTime`,
//! `ParseDuration`, `Time.Add`) and `pkg/types/core_time.go` (`calcDaynr`,
//! `getDateFromDaynr`, `calcTimeDurationDiff`).
//!
//! This is a separate module from [`super`] because it is a VALUE domain, not
//! a builtin: the same duration and the same day-number arithmetic serve four
//! different function classes. The rest of `time_fn` represents a temporal
//! value as its formatted string and computes with seconds; these two types
//! carry microseconds, which is the whole point — `ADDTIME`'s result fsp is
//! decided by whether the sum's microsecond field is zero.

pub(crate) use tidb_query_expr::{
    NativeDurationDateTime as GoDateTime, NativeGoDuration as GoDuration,
};

/// Go `types.MaxFsp`.
pub(crate) const MAX_FSP: i32 = 6;
/// Go `types.MinFsp`.
pub(crate) const MIN_FSP: i32 = 0;

/// Go `calcDaynr`: days since 0000-00-00.
pub(crate) fn daynr(year: i64, month: u32, day: u32) -> i64 {
    GoDateTime::daynr(year, month, day)
}

/// Go `getDateFromDaynr`, with the original out-of-range zero answer.
pub(crate) fn date_from_daynr(daynr: i64) -> (i64, u32, u32) {
    GoDateTime::date_from_daynr(daynr)
}

/// The shared duration-shape predicate selects the ADDTIME argument domain.
pub(crate) fn is_duration(value: &str) -> bool {
    GoDuration::is_duration(value)
}

/// Original ADDTIME/SUBTIME written-fraction precision policy.
pub(crate) fn fsp_for_time_add_sub(value: &str) -> i32 {
    GoDuration::fsp_for_time_add_sub(value)
}

/// Go `types.GetFsp`: the number of fractional digits, capped at `MaxFsp`.
pub(crate) fn get_fsp(value: &str) -> i32 {
    tidb_query_expr::native_duration_fsp(value)
}

/// The one failure `ParseDuration` reports: `ErrTruncatedWrongVal`, which
/// every caller in this family turns into a warning plus a NULL result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Truncated;

/// Go `types.ParseDuration`, delegated with its original truncation result.
pub(crate) fn parse_duration(value: &str, fsp: i32) -> Result<GoDuration, Truncated> {
    tidb_query_expr::parse_native_duration(value, fsp).map_err(|_| Truncated)
}

/// Shared native datetime parser, including the original compact UTC path.
pub(crate) fn parse_datetime(value: &str) -> Option<GoDateTime> {
    tidb_query_expr::parse_native_duration_datetime(value)
}

#[cfg(test)]
mod shared_parser_tests {
    use super::{get_fsp, parse_datetime, parse_duration, GoDuration, Truncated};

    #[test]
    fn shared_duration_facades_keep_raw_fsp_and_distinct_datetime_fractions() {
        assert_eq!(get_fsp("1.é"), 2);
        assert_eq!(
            parse_duration("1", -1),
            Ok(GoDuration {
                micros: 1_000_000,
                fsp: -1
            })
        );
        let negative = parse_duration("-00:00:00.1234567", 6).unwrap();
        assert_eq!((negative.micros, negative.fsp), (-123_457, 6));
        assert_eq!(negative.micro_second(), 123_457);
        assert_eq!(negative.format(), "-00:00:00.123457");
        for (text, expected_micros) in [
            ("20170118123050.1234567", 123_457),
            ("2017-01-18 12:30:50.1234567", 123_456),
        ] {
            let datetime = parse_datetime(text).unwrap();
            assert_eq!((datetime.year, datetime.month, datetime.day), (2017, 1, 18));
            assert_eq!(
                (datetime.hour, datetime.minute, datetime.second),
                (12, 30, 50)
            );
            assert_eq!((datetime.micros, datetime.fsp), (expected_micros, 6));
            let duration = parse_duration(text, 6).unwrap();
            assert_eq!(duration.micros, 45_050_000_000 + i64::from(expected_micros));
        }
        assert_eq!(parse_duration("838:59:59.000001", 6), Err(Truncated));
        assert_eq!(parse_duration("1x", 0), Err(Truncated));
        assert_eq!(
            parse_duration("2011-11-11 10:10:10.11.12", 6),
            Err(Truncated)
        );
    }
}

#[cfg(test)]
mod source_tests {
    use super::is_duration;

    /// Exact Go `TestIsDuration` table. This predicate chooses ADDTIME's
    /// duration-vs-datetime signature before either parser is invoked.
    #[test]
    fn test_is_duration() {
        for (input, expected) in [
            ("110:00:00", true),
            ("aa:bb:cc", false),
            ("1 01:00:00", true),
            ("01:00:00.999999", true),
            ("071231235959.999999", false),
            ("20171231235959.999999", false),
            ("2017-01-01 01:01:01.11", false),
            ("07-12-31 23:59:59.999999", false),
            ("2007-12-31 23:59:59.999999", false),
        ] {
            assert_eq!(is_duration(input), expected, "{input}");
        }
    }
}
