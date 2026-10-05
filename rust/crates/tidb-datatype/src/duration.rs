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

//! MySQL `TIME` duration range policy from `pkg/types/time.go`.

use std::{cmp::Ordering, fmt};

use chrono::{DateTime, Datelike, Duration as ChronoDuration, TimeZone};
pub use tidb_query_datatype::codec::mysql::duration::{
    NativeDurationDateTimeFallbackKind as DurationDateTimeFallbackKind,
    NativeDurationOverflow as DurationOverflow, NativeDurationParseError as DurationParseError,
    NativeDurationParseEvent as DurationParseEvent, NativeDurationValueError as DurationValueError,
};
use tidb_query_datatype::codec::mysql::{duration as shared_duration, Duration as SharedDuration};

use crate::time_parse::adjust_year_with_event;
use crate::{
    check_fsp, core_time_from_datetime, Converted, Decimal, FspError, Time, TimeError, TimeType,
};

/// The maximum SQL `TIME` hour component accepted by TiDB.
pub const TIME_MAX_HOUR: i64 = 838;
/// The maximum SQL `TIME` minute component accepted by TiDB.
pub const TIME_MAX_MINUTE: i64 = 59;
/// The maximum SQL `TIME` second component accepted by TiDB.
pub const TIME_MAX_SECOND: i64 = 59;
/// The largest representable MySQL duration in nanoseconds.
pub const MAX_TIME_NANOS: i64 =
    (TIME_MAX_HOUR * 60 * 60 + TIME_MAX_MINUTE * 60 + TIME_MAX_SECOND) * 1_000_000_000;
/// The smallest representable MySQL duration in nanoseconds.
pub const MIN_TIME_NANOS: i64 = -MAX_TIME_NANOS;

/// MySQL `TIME` value with signed nanoseconds and fractional precision.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct MySqlDuration {
    nanoseconds: i64,
    fsp: i64,
}

impl MySqlDuration {
    /// Returns the maximum representable MySQL TIME value at `fsp`.
    pub fn maximum(fsp: i64) -> Result<Self, FspError> {
        Self::new(TIME_MAX_HOUR, TIME_MAX_MINUTE, TIME_MAX_SECOND, 0, fsp)
    }

    /// Constructs a duration from source clock fields.
    pub fn new(
        hour: i64,
        minute: i64,
        second: i64,
        microsecond: i64,
        fsp: i64,
    ) -> Result<Self, FspError> {
        let fsp = check_fsp(fsp)?;
        Ok(Self {
            nanoseconds: (hour * 3_600 + minute * 60 + second) * 1_000_000_000
                + microsecond * 1_000,
            fsp,
        })
    }

    /// Constructs from an existing signed nanosecond count.
    pub fn from_nanoseconds(nanoseconds: i64, fsp: i64) -> Result<Self, FspError> {
        Ok(Self::from_raw_parts(nanoseconds, check_fsp(fsp)?))
    }

    /// Constructs the source `types.Duration` value without normalizing its
    /// metadata. Chunk cells store only nanoseconds; Go's `GetDuration`
    /// stamps the caller-provided `fillFsp` integer verbatim, including
    /// `UnspecifiedFsp` and values outside SQL's validated 0..=6 domain.
    #[must_use]
    pub const fn from_raw_parts(nanoseconds: i64, fsp: i64) -> Self {
        Self { nanoseconds, fsp }
    }

    /// Returns the signed nanosecond count.
    pub const fn nanoseconds(self) -> i64 {
        self.nanoseconds
    }

    /// Returns fractional-seconds precision.
    pub const fn fsp(self) -> i64 {
        self.fsp
    }

    /// Returns the negated duration.
    pub const fn negated(self) -> Self {
        Self {
            nanoseconds: -self.nanoseconds,
            fsp: self.fsp,
        }
    }

    /// Returns the absolute hour component, including values beyond 24.
    pub const fn hour(self) -> i64 {
        SharedDuration::hours_from_nanos(self.nanoseconds) as i64
    }

    /// Returns the absolute minute component.
    pub const fn minute(self) -> i64 {
        SharedDuration::minutes_from_nanos(self.nanoseconds) as i64
    }

    /// Returns the absolute second component.
    pub const fn second(self) -> i64 {
        SharedDuration::secs_from_nanos(self.nanoseconds) as i64
    }

    /// Returns the absolute microsecond component.
    pub const fn microsecond(self) -> i64 {
        SharedDuration::micro_secs_from_nanos(self.nanoseconds) as i64
    }

    /// Adds two durations while preserving the larger FSP.
    pub fn checked_add(self, other: Self) -> Result<Self, DurationRoundError> {
        let nanoseconds = self
            .nanoseconds
            .checked_add(other.nanoseconds)
            .ok_or(DurationRoundError::Overflow)?;
        Ok(Self {
            nanoseconds,
            fsp: self.fsp.max(other.fsp),
        })
    }

    /// Subtracts two durations while preserving the larger FSP.
    pub fn checked_sub(self, other: Self) -> Result<Self, DurationRoundError> {
        let nanoseconds = self
            .nanoseconds
            .checked_sub(other.nanoseconds)
            .ok_or(DurationRoundError::Overflow)?;
        Ok(Self {
            nanoseconds,
            fsp: self.fsp.max(other.fsp),
        })
    }

    /// Formats this duration with MySQL's `TIME_FORMAT` conversion rules.
    pub fn duration_format(self, layout: &str) -> String {
        tidb_query_datatype::codec::mysql::Time::native_raw_duration_format(
            self.nanoseconds,
            layout,
        )
    }

    /// Returns TiDB's numeric TIME representation.
    pub fn to_number(self) -> Decimal {
        let literal = if self.fsp == 0 {
            format!("{:02}{:02}{:02}", self.hour(), self.minute(), self.second())
        } else {
            let fraction = format!("{:06}", self.microsecond());
            format!(
                "{:02}{:02}{:02}.{}",
                self.hour(),
                self.minute(),
                self.second(),
                &fraction[..usize::try_from(self.fsp).expect("nonnegative duration FSP")]
            )
        };
        let value = Decimal::from_literal(&literal);
        if self.nanoseconds < 0 {
            value.negate()
        } else {
            value
        }
    }

    /// Rounds fractional seconds with Go's nearest-value `Time.Round` rule.
    pub fn round_frac(self, fsp: i64) -> Result<Self, DurationRoundError> {
        let rounded = round_duration_fsp(self.nanoseconds, self.fsp, fsp)?;
        Ok(Self {
            nanoseconds: rounded.nanoseconds(),
            fsp: rounded.fsp(),
        })
    }

    /// Compares signed duration values.
    pub const fn compare(self, other: Self) -> Ordering {
        if self.nanoseconds < other.nanoseconds {
            Ordering::Less
        } else if self.nanoseconds > other.nanoseconds {
            Ordering::Greater
        } else {
            Ordering::Equal
        }
    }

    /// Parses and compares a duration string at maximum FSP.
    pub fn compare_string(self, input: &str) -> Result<Ordering, DurationParseError> {
        let parsed = parse_duration(input.as_bytes(), 6)?;
        Ok(self.nanoseconds.cmp(&parsed.nanoseconds()))
    }

    /// Adds this duration to the calendar date containing `timestamp`.
    pub fn convert_to_time<TZ: TimeZone>(
        self,
        timestamp: DateTime<TZ>,
        kind: TimeType,
        allow_zero_in_date: bool,
        allow_invalid_date: bool,
    ) -> Result<Time, TimeError> {
        let timezone = timestamp.timezone();
        let midnight = timezone
            .with_ymd_and_hms(
                timestamp.year(),
                timestamp.month(),
                timestamp.day(),
                0,
                0,
                0,
            )
            .single()
            .ok_or(TimeError::InvalidDate)?;
        let value = midnight
            .checked_add_signed(ChronoDuration::nanoseconds(self.nanoseconds))
            .ok_or(TimeError::OutOfRange("time"))?;
        let datetime = Time::new(core_time_from_datetime(value), TimeType::DateTime, self.fsp)?;
        datetime
            .convert_kind(kind, allow_zero_in_date, allow_invalid_date, &timezone)
            .map(|result| result.0)
    }

    /// Converts a TIME value to YEAR using TiDB's two source modes.
    pub fn convert_to_year<TZ: TimeZone>(
        self,
        now: DateTime<TZ>,
        through_concat: bool,
    ) -> Result<i64, TimeError> {
        let converted = self.convert_to_year_with_event(now, through_concat)?;
        if converted.event.is_some() {
            return Err(TimeError::OutOfRange("year"));
        }
        Ok(converted.value)
    }

    pub(crate) fn convert_to_year_with_event<TZ: TimeZone>(
        self,
        now: DateTime<TZ>,
        through_concat: bool,
    ) -> Result<Converted<i64>, TimeError> {
        if through_concat {
            let rounded = self
                .round_frac(0)
                .map_err(|_| TimeError::OutOfRange("year"))?;
            let numeric = rounded.hour() * 10_000 + rounded.minute() * 100 + rounded.second();
            let numeric = if rounded.nanoseconds < 0 {
                -numeric
            } else {
                numeric
            };
            return Ok(adjust_year_with_event(numeric, false));
        }
        let value = self.convert_to_time(now, TimeType::DateTime, false, false)?;
        Ok(adjust_year_with_event(
            i64::from(value.core_time().year()),
            false,
        ))
    }
}

impl fmt::Display for MySqlDuration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        SharedDuration::write_native_display(self.nanoseconds, self.fsp, formatter)
    }
}

/// A duration value after source `RoundFrac` normalization.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RoundedDuration {
    nanoseconds: i64,
    fsp: i64,
}

/// A successfully parsed MySQL duration literal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ParsedDuration {
    nanoseconds: i64,
    fsp: i64,
    overflow: Option<DurationOverflow>,
    truncated: bool,
}

impl ParsedDuration {
    fn from_shared(value: shared_duration::NativeParsedDuration) -> Self {
        Self {
            nanoseconds: value.nanos,
            fsp: value.fsp,
            overflow: value.overflow,
            truncated: value.truncated,
        }
    }

    /// Returns the signed nanosecond value after FSP rounding and range clamp.
    pub const fn nanoseconds(self) -> i64 {
        self.nanoseconds
    }

    /// Returns the normalized fractional-seconds precision.
    pub const fn fsp(self) -> i64 {
        self.fsp
    }

    /// Returns the range overflow direction, if the source would warn/error.
    pub const fn overflow(self) -> Option<DurationOverflow> {
        self.overflow
    }

    /// Returns whether the source reported `ErrTruncatedWrongVal`.
    pub const fn truncated(self) -> bool {
        self.truncated
    }

    /// Returns the pure source-side event for this parsed duration.
    pub const fn event(self) -> Option<DurationParseEvent> {
        shared_duration::NativeParsedDuration {
            nanos: self.nanoseconds,
            fsp: self.fsp,
            overflow: self.overflow,
            truncated: self.truncated,
        }
        .event()
    }
}

impl RoundedDuration {
    /// Returns the rounded signed nanosecond count.
    pub const fn nanoseconds(self) -> i64 {
        self.nanoseconds
    }

    /// Returns the normalized fractional-seconds precision.
    pub const fn fsp(self) -> i64 {
        self.fsp
    }
}

/// An error from source-compatible duration FSP rounding.
pub use tidb_query_datatype::codec::native_duration_convert::NativeDurationRoundError as DurationRoundError;

/// Result of Go `TruncateOverflowMySQLTime`'s clamp operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DurationRangeResult {
    value: i64,
    overflow: Option<DurationOverflow>,
}

impl DurationRangeResult {
    /// Returns the clamped or unchanged duration in nanoseconds.
    pub const fn value(self) -> i64 {
        self.value
    }

    /// Returns the overflow direction, if the source would return an error.
    pub const fn overflow(self) -> Option<DurationOverflow> {
        self.overflow
    }

    /// Returns the pure source-side event for this range result.
    pub const fn event(self) -> Option<DurationParseEvent> {
        match self.overflow {
            Some(direction) => Some(DurationParseEvent::Overflow(direction)),
            None => None,
        }
    }
}

/// Clamps a duration to TiDB's MySQL `TIME` range.
///
/// This is the value/error part of Go `TruncateOverflowMySQLTime`: callers
/// receive the endpoint value together with an explicit overflow direction.
/// Turning that direction into a warning or statement error remains outside
/// this dependency-leaf API.
pub const fn truncate_overflow_mysql_time(value: i64) -> DurationRangeResult {
    let (value, overflow) = shared_duration::native_truncate_overflow_mysql_time(value);
    DurationRangeResult { value, overflow }
}

/// Parses the source's dependency-closed `[-]HH:MM[:SS][.fraction]` and
/// compact `[-]HHMMSS[.fraction]` grammars.
///
/// A day prefix (`D HH[:MM[:SS]]`) is also accepted because Go's
/// `matchDayHHMMSS` feeds it through the same duration path. Date/datetime
/// calendar conversion, statement warnings, and session context are
/// deliberately not included. Date-shaped input returns a typed
/// [`DurationParseError::DateTimeFallback`] signal. Values beyond MySQL's
/// `TIME` range are clamped and exposed via [`ParsedDuration::overflow`],
/// matching the source value/error split; callers can consume
/// [`ParsedDuration::event`] without importing SQL warning policy.
pub fn parse_duration(input: &[u8], target_fsp: i64) -> Result<ParsedDuration, DurationParseError> {
    shared_duration::native_parse_duration(input, target_fsp).map(ParsedDuration::from_shared)
}

/// Parses the complete Go `ParseDuration` surface, including datetime fallback.
pub fn parse_mysql_duration<TZ: TimeZone>(
    input: &str,
    target_fsp: i64,
    timezone: &TZ,
    allow_zero_in_date: bool,
    allow_invalid_date: bool,
) -> Result<ParsedDuration, DurationValueError> {
    shared_duration::native_parse_mysql_duration(
        input,
        target_fsp,
        timezone,
        allow_zero_in_date,
        allow_invalid_date,
    )
    .map(ParsedDuration::from_shared)
}

/// Classifies the exact shape accepted by Go `canFallbackToDateTime`.
///
/// The input must already have the outer whitespace removed, as it is at the
/// call site in Go `ParseDuration`. The source parser treats each byte as a
/// Unicode code point; for the 0..255 range that means ASCII digits and the
/// Latin-1 punctuation code points in the shared source-version table.
pub fn classify_duration_datetime_fallback(input: &[u8]) -> Option<DurationDateTimeFallbackKind> {
    shared_duration::native_classify_duration_datetime_fallback(input)
}

/// Boolean form of [`classify_duration_datetime_fallback`] for callers that
/// only need the source `canFallbackToDateTime` predicate.
pub fn can_fallback_to_datetime(input: &[u8]) -> bool {
    shared_duration::native_can_fallback_to_datetime(input)
}

/// Rounds duration nanoseconds using Go `Duration.RoundFrac`'s nearest-value
/// rule and returns normalized FSP metadata. Non-halfway values round away
/// from zero when their magnitude is past the midpoint; an exact negative
/// halfway value rounds toward zero because Go delegates to `Time.Round`,
/// whose tie rule is toward positive infinity.
///
/// The target FSP is normalized first, then compared with `current_fsp`,
/// matching Go's early return. A target FSP above six is clamped by
/// [`check_fsp`]; invalid negative values are returned as a typed error. The
/// function does not apply MySQL range clamping or statement warning policy.
pub fn round_duration_fsp(
    nanoseconds: i64,
    current_fsp: i64,
    target_fsp: i64,
) -> Result<RoundedDuration, DurationRoundError> {
    tidb_query_datatype::codec::native_duration_convert::native_round_duration_fsp(
        nanoseconds,
        current_fsp,
        target_fsp,
    )
    .map(|value| RoundedDuration {
        nanoseconds: value.nanoseconds,
        fsp: value.fsp,
    })
}

#[cfg(test)]
#[test]
fn shared_duration_extract_adapters_preserve_status_timezone_and_raw_metadata() {
    use crate::{extract_datetime_num, extract_duration_num, CoreTime};
    #[derive(Clone)]
    struct NoTimezoneReads;
    impl TimeZone for NoTimezoneReads {
        type Offset = chrono::FixedOffset;
        fn from_offset(_: &Self::Offset) -> Self {
            panic!("unexpected timezone reconstruction")
        }
        fn offset_from_local_date(
            &self,
            _: &chrono::NaiveDate,
        ) -> chrono::LocalResult<Self::Offset> {
            panic!("unexpected local date conversion")
        }
        fn offset_from_local_datetime(
            &self,
            _: &chrono::NaiveDateTime,
        ) -> chrono::LocalResult<Self::Offset> {
            panic!("unexpected local clock conversion")
        }
        fn offset_from_utc_date(&self, _: &chrono::NaiveDate) -> Self::Offset {
            panic!("unexpected UTC date conversion")
        }
        fn offset_from_utc_datetime(&self, _: &chrono::NaiveDateTime) -> Self::Offset {
            panic!("unexpected UTC clock conversion")
        }
    }
    for (input, nanos, overflow) in [
        ("900:00:00x", MAX_TIME_NANOS, DurationOverflow::Positive),
        ("-900:00:00x", MIN_TIME_NANOS, DurationOverflow::Negative),
    ] {
        let parsed = parse_mysql_duration(input, 6, &NoTimezoneReads, false, false).unwrap();
        assert_eq!(
            (
                parsed.nanoseconds(),
                parsed.fsp(),
                parsed.overflow(),
                parsed.truncated()
            ),
            (nanos, 6, Some(overflow), true)
        );
        assert_eq!(parsed.event(), Some(DurationParseEvent::Overflow(overflow)));
        assert!(format!("{parsed:?}").starts_with("ParsedDuration { nanoseconds:"));
    }
    let invalid_fsp = parse_duration(b"not a duration", -2).unwrap_err();
    assert!(matches!(invalid_fsp, DurationParseError::InvalidFsp(_)));
    assert_eq!(invalid_fsp.event(), None);
    let malformed = parse_mysql_duration("x", 0, &NoTimezoneReads, false, false).unwrap_err();
    assert_eq!(format!("{malformed:?}"), "Duration(InvalidFormat)");
    assert_eq!(malformed.to_string(), "invalid duration format");
    assert_eq!(
        classify_duration_datetime_fallback(b"2020-01-01 01:02:03"),
        Some(DurationDateTimeFallbackKind::Separated)
    );
    assert!(!can_fallback_to_datetime(b" 2020-01-01 01:02:03"));
    for (input, fsp) in [("0000-00-00 00:00:00", 0), ("2020-01-01 00:00:00", 6)] {
        let parsed = parse_mysql_duration(input, 6, &NoTimezoneReads, true, false).unwrap();
        assert_eq!(
            (parsed.nanoseconds(), parsed.fsp(), parsed.event()),
            (0, fsp, None)
        );
    }
    for allow_zero in [false, true] {
        for allow_invalid in [false, true] {
            let partial = parse_mysql_duration(
                "2021-02-00 01:02:03",
                0,
                &NoTimezoneReads,
                allow_zero,
                allow_invalid,
            );
            assert_eq!(
                partial.map(|v| v.nanoseconds()),
                if allow_zero {
                    Ok(3_723_000_000_000)
                } else {
                    Err(DurationValueError::Time(TimeError::ZeroInDate))
                }
            );
            let invalid = parse_mysql_duration(
                "2021-02-29 01:02:03",
                0,
                &NoTimezoneReads,
                allow_zero,
                allow_invalid,
            );
            assert_eq!(
                invalid.map(|v| v.nanoseconds()),
                if allow_invalid {
                    Ok(3_723_000_000_000)
                } else {
                    Err(DurationValueError::Time(TimeError::InvalidDate))
                }
            );
        }
    }
    // A timezone-bearing fallback must use the supplied destination zone, not
    // a hardcoded UTC substitute; ordinary duration parsing above needs none.
    for (offset, nanos) in [(0, 82_923_000_000_000), (3_600, 123_000_000_000)] {
        let zone = chrono::FixedOffset::east_opt(offset).unwrap();
        let parsed =
            parse_mysql_duration("2020-01-01 01:02:03+02:00", 0, &zone, false, false).unwrap();
        assert_eq!(
            (parsed.nanoseconds(), parsed.fsp(), parsed.event()),
            (nanos, 0, None)
        );
    }
    for (raw, expected_fsp) in [(0, 0), (1, 6)] {
        let value = Time::from_raw_parts(CoreTime::from_raw(raw), TimeType::Date, 255)
            .to_duration()
            .unwrap();
        assert_eq!((value.nanoseconds(), value.fsp()), (0, expected_fsp));
    }
    let time = Time::from_raw_parts(
        CoreTime::from_date(2020, 1, 2, 3, 4, 5, 123456),
        TimeType::Date,
        0,
    );
    let duration = time.to_duration().unwrap();
    assert_eq!(
        (duration.nanoseconds(), duration.fsp()),
        (11_045_123_456_000, 0)
    );
    assert_eq!(
        extract_datetime_num(time, "day_microsecond"),
        Ok(2_030_405_123_456)
    );
    assert_eq!(
        extract_duration_num(
            MySqlDuration::from_raw_parts(-duration.nanoseconds(), -2),
            "DAY_MICROSECOND"
        ),
        Ok(-30_405_123_456)
    );
    let invalid = crate::time_parse::extract_datetime_num_with_error(time, " HOUR");
    assert_eq!(
        (invalid.value, invalid.error),
        (0, Some(TimeError::InvalidUnit(" HOUR".to_owned())))
    );
    let invalid = crate::time_parse::extract_duration_num_with_error(duration, "Year");
    assert_eq!(
        (invalid.value, invalid.error),
        (0, Some(TimeError::InvalidUnit("Year".to_owned())))
    );
    assert!(
        crate::is_clock_unit("day_microsecond")
            && crate::is_date_unit("day_microsecond")
            && crate::is_microsecond_unit("day_microsecond")
    );
    assert!(!crate::is_clock_unit(" HOUR"));
    const CLAMPED: DurationRangeResult = truncate_overflow_mysql_time(i64::MIN);
    assert_eq!(
        (CLAMPED.value(), CLAMPED.overflow(), CLAMPED.event()),
        (
            MIN_TIME_NANOS,
            Some(DurationOverflow::Negative),
            Some(DurationParseEvent::Overflow(DurationOverflow::Negative))
        )
    );
}
