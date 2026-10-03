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

use std::sync::OnceLock;

/// Go `types.MaxFsp`.
pub(crate) const MAX_FSP: i32 = 6;
/// Go `types.MinFsp`.
pub(crate) const MIN_FSP: i32 = 0;

/// Go `types.Duration`: a signed microsecond span plus the fsp it prints at.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct GoDuration {
    pub(crate) micros: i64,
    pub(crate) fsp: i32,
}

impl GoDuration {
    /// Go `Duration.MicroSecond`: the fractional part alone, always positive.
    pub(crate) fn micro_second(self) -> i64 {
        tidb_query_expr::NativeGoDuration {
            micros: self.micros,
            fsp: self.fsp,
        }
        .micro_second()
    }

    /// Go `Duration.Add`/`Duration.Sub`: the sum keeps the LARGER fsp of the
    /// two operands. A `Duration{}` zero operand returns the receiver
    /// untouched, fsp included, which is why the `is_zero_value` guard is
    /// here and not at the call sites.
    pub(crate) fn combine(self, other: GoDuration, sign: i64) -> GoDuration {
        let scaled = GoDuration {
            micros: other.micros * sign,
            fsp: other.fsp,
        };
        if other.micros == 0 && other.fsp == 0 {
            return self;
        }
        GoDuration {
            micros: self.micros.saturating_add(scaled.micros),
            fsp: self.fsp.max(other.fsp),
        }
    }

    /// Go `Duration.String`.
    pub(crate) fn format(self) -> String {
        super::format_time_diff(self.micros, self.fsp.max(0) as usize)
    }
}

/// Go `types.Time`, reduced to the fields this family reads. `micros` is the
/// microsecond field, not a whole-value count.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct GoDateTime {
    pub(crate) year: i64,
    pub(crate) month: u32,
    pub(crate) day: u32,
    pub(crate) hour: u32,
    pub(crate) minute: u32,
    pub(crate) second: u32,
    pub(crate) micros: u32,
    pub(crate) fsp: i32,
}

impl GoDateTime {
    /// Go `Time.IsZero`.
    pub(crate) fn is_zero(self) -> bool {
        self.year == 0
            && self.month == 0
            && self.day == 0
            && self.hour == 0
            && self.minute == 0
            && self.second == 0
            && self.micros == 0
    }

    /// Go `Time.String`, truncating the microsecond field to `fsp` digits —
    /// TRUNCATING, not rounding, which is what makes `datetime(3) + time(6)`
    /// print `.579` for the exact value `.579789`.
    pub(crate) fn format(self) -> String {
        let stem = format!(
            "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
            self.year, self.month, self.day, self.hour, self.minute, self.second
        );
        let fsp = self.fsp.clamp(0, MAX_FSP);
        if fsp == 0 {
            return stem;
        }
        let divisor = 10u32.pow(6 - fsp as u32);
        format!(
            "{stem}.{:0width$}",
            self.micros / divisor,
            width = fsp as usize
        )
    }

    /// Go `Time.Add`: the datetime and the duration are reduced to one
    /// signed microsecond count (`calcTimeDurationDiff`), whose ABSOLUTE
    /// value is then split back into a day number and a time of day. Go
    /// discards the sign the same way (`_` on `neg`), so a sum that would
    /// fall before year 0 wraps rather than erroring; this port keeps that.
    ///
    /// The result fsp is Go's `max(d.Fsp, t.Fsp())`. The VECTORIZED
    /// signatures pass `types.Duration{Duration: arg1, Fsp: -1}`, so a
    /// caller reproducing the vectorized arm passes `fsp: -1` and the
    /// datetime's own fsp wins.
    pub(crate) fn add(self, delta: GoDuration) -> Option<GoDateTime> {
        let total = daynr(self.year, self.month, self.day)
            .checked_mul(86_400_000_000)?
            .checked_add(
                i64::from(self.hour) * 3_600_000_000
                    + i64::from(self.minute) * 60_000_000
                    + i64::from(self.second) * 1_000_000
                    + i64::from(self.micros),
            )?
            .checked_add(delta.micros)?;
        let total = total.abs();
        let seconds = total / 1_000_000;
        let micros = (total % 1_000_000) as u32;
        let (year, month, day) = date_from_daynr(seconds / 86_400);
        let rest = seconds % 86_400;
        Some(GoDateTime {
            year,
            month,
            day,
            hour: (rest / 3600) as u32,
            minute: (rest / 60 % 60) as u32,
            second: (rest % 60) as u32,
            micros,
            fsp: self.fsp.max(delta.fsp),
        })
    }

    /// Go `Time.Check` reduced to the range this family can produce: a day
    /// number outside `0001-01-01 .. 9999-12-31` is `getDateFromDaynr`'s own
    /// zero answer or a year past 9999, both of which Go reports as an
    /// invalid time rather than returning.
    pub(crate) fn in_range(self) -> bool {
        (1..=9999).contains(&self.year) && self.month >= 1 && self.day >= 1
    }
}

/// Go `calcDaynr`: days since 0000-00-00.
pub(crate) fn daynr(year: i64, month: u32, day: u32) -> i64 {
    if year == 0 && month == 0 {
        return 0;
    }
    let month = i64::from(month);
    let mut year = year;
    let mut delsum = 365 * year + 31 * (month - 1) + i64::from(day);
    if month <= 2 {
        year -= 1;
    } else {
        delsum -= (month * 4 + 23) / 10;
    }
    let temp = ((year / 100 + 1) * 3) / 4;
    delsum + year / 4 - temp
}

/// Go `calcDaysInYear`.
fn days_in_year(year: i64) -> i64 {
    if (year & 3) == 0 && (year % 100 != 0 || (year % 400 == 0 && year != 0)) {
        366
    } else {
        365
    }
}

/// Go `getDateFromDaynr`, the inverse of [`daynr`]. Out-of-range day numbers
/// answer `(0, 0, 0)`, exactly as Go's early return does.
pub(crate) fn date_from_daynr(daynr: i64) -> (i64, u32, u32) {
    if daynr <= 365 || daynr >= 3_652_500 {
        return (0, 0, 0);
    }
    let mut year = daynr * 100 / 36525;
    let temp = (((year - 1) / 100 + 1) * 3) / 4;
    let mut day_of_year = daynr - year * 365 - (year - 1) / 4 + temp;
    let mut in_year = days_in_year(year);
    while day_of_year > in_year {
        day_of_year -= in_year;
        year += 1;
        in_year = days_in_year(year);
    }
    let mut leap_day = 0;
    if in_year == 366 && day_of_year > 31 + 28 {
        day_of_year -= 1;
        if day_of_year == 31 + 28 {
            leap_day = 1;
        }
    }
    let mut month = 1;
    for length in [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31] {
        if day_of_year <= length {
            break;
        }
        day_of_year -= length;
        month += 1;
    }
    (year, month, (day_of_year + leap_day) as u32)
}

/// Go `expression.isDuration`, whose `durationPattern` decides whether
/// `ADDTIME`'s first STRING argument is read as a duration or as a datetime.
pub(crate) fn is_duration(value: &str) -> bool {
    static PATTERN: OnceLock<regex::Regex> = OnceLock::new();
    PATTERN
        .get_or_init(|| {
            regex::Regex::new(
                r"^\s*[-]?(((\d{1,2}\s+)?0*\d{0,3}(:0*\d{1,2}){0,2})|(\d{1,7}))?(\.\d*)?\s*$",
            )
            .expect("the source durationPattern is a valid regex")
        })
        .is_match(value)
}

/// Go `expression.getFsp4TimeAddSub`: `MaxFsp` when the string carries a
/// non-zero fractional part, `MinFsp` otherwise.
pub(crate) fn fsp_for_time_add_sub(value: &str) -> i32 {
    match value.find('.') {
        None => MIN_FSP,
        Some(dot) => {
            if value[dot + 1..].chars().any(|c| c != '0') {
                MAX_FSP
            } else {
                MIN_FSP
            }
        }
    }
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
    tidb_query_expr::parse_native_duration(value, fsp)
        .map(|duration| GoDuration {
            micros: duration.micros,
            fsp: duration.fsp,
        })
        .map_err(|_| Truncated)
}

/// Shared native datetime parser, including the original compact UTC path.
pub(crate) fn parse_datetime(value: &str) -> Option<GoDateTime> {
    tidb_query_expr::parse_native_duration_datetime(value).map(|datetime| GoDateTime {
        year: datetime.year,
        month: datetime.month,
        day: datetime.day,
        hour: datetime.hour,
        minute: datetime.minute,
        second: datetime.second,
        micros: datetime.micros,
        fsp: datetime.fsp,
    })
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
