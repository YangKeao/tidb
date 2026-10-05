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

use std::cmp::Ordering;
use std::fmt;

#[cfg(test)]
use chrono::Timelike;
use chrono::{DateTime, TimeZone};
pub use tidb_query_datatype::codec::mysql::time::{
    NativeTimeConversionError as TimeConversionError, NativeTimeDifference as TimeDifference,
    NativeTimestampInterval as TimestampInterval,
};
use tidb_query_datatype::codec::mysql::{time as shared_time, Time as SharedTime};

const HOUR_OFFSET: u64 = 36;
const MINUTE_OFFSET: u64 = 30;
const SECOND_OFFSET: u64 = 24;
const MICROSECOND_OFFSET: u64 = 4;
const DAYS_BY_MONTH: [u8; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

/// TiDB's compact internal calendar representation.
#[derive(Clone, Copy, Default, Eq, Hash, PartialEq)]
pub struct CoreTime(u64);

impl CoreTime {
    /// Constructs from TiDB's exact internal bit representation.
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    /// Returns TiDB's exact internal bit representation.
    pub const fn raw(self) -> u64 {
        self.0
    }

    /// Constructs the exact bit layout used by Go `FromDate`.
    pub const fn from_date(
        year: u16,
        month: u8,
        day: u8,
        hour: u8,
        minute: u8,
        second: u8,
        microsecond: u32,
    ) -> Self {
        Self(SharedTime::native_core_from_fields(
            year,
            month,
            day,
            hour,
            minute,
            second,
            microsecond,
        ))
    }

    /// Returns the year.
    pub const fn year(self) -> i32 {
        SharedTime::year_from_core_bits(self.0) as i32
    }

    /// Returns the month.
    pub const fn month(self) -> u8 {
        SharedTime::month_from_core_bits(self.0) as u8
    }

    /// Returns the day of month.
    pub const fn day(self) -> u8 {
        SharedTime::day_from_core_bits(self.0) as u8
    }

    /// Returns the hour.
    pub const fn hour(self) -> u8 {
        ((self.0 >> HOUR_OFFSET) & 0x1f) as u8
    }

    /// Returns the minute.
    pub const fn minute(self) -> u8 {
        ((self.0 >> MINUTE_OFFSET) & 0x3f) as u8
    }

    /// Returns the second.
    pub const fn second(self) -> u8 {
        ((self.0 >> SECOND_OFFSET) & 0x3f) as u8
    }

    /// Returns the microsecond.
    pub const fn microsecond(self) -> u32 {
        ((self.0 >> MICROSECOND_OFFSET) & 0x0f_ffff) as u32
    }

    /// Returns whether the represented year is a leap year.
    pub const fn is_leap_year(self) -> bool {
        is_leap_year(self.year())
    }

    /// Returns the day within the year, or zero for an incomplete date.
    pub const fn year_day(self) -> i32 {
        SharedTime::native_core_year_day(self.raw())
    }

    /// Returns the normalized Gregorian weekday.
    ///
    /// Like Go's `time.Date`, invalid month-day combinations are normalized;
    /// for example 2019-02-31 is the Sunday 2019-03-03.
    pub fn weekday(self) -> Weekday {
        Weekday::from_sunday_index(SharedTime::native_core_weekday_sunday_index(self.raw()) as i32)
    }

    /// Converts this value through an IANA timezone, rejecting invalid or
    /// nonexistent local wall-clock values.
    pub fn to_datetime<TZ: TimeZone>(
        self,
        timezone: &TZ,
    ) -> Result<DateTime<TZ>, TimeConversionError> {
        tidb_query_datatype::codec::mysql::time::native_core_to_datetime(
            self.raw(),
            timezone,
            false,
        )
    }

    /// Converts through an IANA timezone and moves a spring-forward gap to its
    /// closest valid upper boundary.
    pub fn adjusted_datetime<TZ: TimeZone>(
        self,
        timezone: &TZ,
    ) -> Result<DateTime<TZ>, TimeConversionError> {
        tidb_query_datatype::codec::mysql::time::native_core_to_datetime(self.raw(), timezone, true)
    }

    /// Returns the week under MySQL's mode rules.
    pub const fn week(self, mode: u8) -> i32 {
        SharedTime::native_core_week(self.raw(), mode)
    }

    /// Returns the week-numbering year and week with MySQL `YEARWEEK` rules.
    pub const fn year_week(self, mode: u8) -> (i32, i32) {
        calc_week(self, week_mode(mode) | WEEK_BEHAVIOUR_YEAR)
    }

    /// Returns the signed comparison used by Go `compareTime`.
    pub fn compare(self, other: Self) -> Ordering {
        SharedTime::native_core_compare(self.raw(), other.raw())
    }

    /// Returns the calendar day difference between two dates.
    pub const fn date_diff(self, other: Self) -> i32 {
        SharedTime::native_core_date_diff(self.raw(), other.raw())
    }

    /// Calculates the absolute temporal difference with Go's signed operand rule.
    pub fn time_diff(self, other: Self, sign: i32) -> TimeDifference {
        time_diff_internal(
            self,
            other.year(),
            other.month() as i32,
            other.day() as i32,
            other.hour() as i32,
            other.minute() as i32,
            other.second() as i32,
            other.microsecond() as i32,
            sign,
        )
    }

    /// Computes MySQL `TIMESTAMPDIFF`.
    pub fn timestamp_diff(self, other: Self, interval: TimestampInterval) -> i64 {
        timestamp_diff(interval, self, other)
    }

    /// Adds calendar years, months, and days with TiDB's month-end rule.
    pub fn add_date(self, years: i64, months: i64, days: i64) -> Result<Self, DateAddError> {
        shared_time::native_core_add_date(self.raw(), years, months, days)
            .map(Self::from_raw)
            .ok_or(DateAddError)
    }

    /// Returns the `YYYYMMDDHHMMSS` integer used by temporal comparison.
    pub const fn datetime_number(self) -> u64 {
        datetime_to_u64(self)
    }

    /// Adds a signed duration while preserving the existing clock fields.
    pub fn add_duration(self, nanoseconds: i64) -> Self {
        Self::from_raw(shared_time::native_core_add_duration(
            self.raw(),
            nanoseconds,
        ))
    }

    /// Adds a signed duration in nanoseconds using TiDB's date/time mixing.
    pub fn mix_duration(&mut self, nanoseconds: i64) {
        if nanoseconds >= 0 && nanoseconds / 3_600_000_000_000 < 24 {
            let micros = nanoseconds / 1_000;
            self.set_time_from_micros(micros);
            return;
        }
        *self = self.add_duration(nanoseconds);
    }

    fn set_time_from_micros(&mut self, micros: i64) {
        let seconds = micros / 1_000_000;
        let hour = (seconds / 3_600) as u8;
        let minute = (seconds % 3_600 / 60) as u8;
        let second = (seconds % 60) as u8;
        let microsecond = (micros % 1_000_000) as u32;
        *self = Self::from_date(
            self.year() as u16,
            self.month(),
            self.day(),
            hour,
            minute,
            second,
            microsecond,
        );
    }
}

/// Calendar overflow from [`CoreTime::add_date`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DateAddError;

impl fmt::Display for DateAddError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("datetime function overflow: datetime")
    }
}

impl std::error::Error for DateAddError {}

/// Gregorian weekday using Go's Sunday-zero order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Weekday {
    /// Sunday.
    Sunday,
    /// Monday.
    Monday,
    /// Tuesday.
    Tuesday,
    /// Wednesday.
    Wednesday,
    /// Thursday.
    Thursday,
    /// Friday.
    Friday,
    /// Saturday.
    Saturday,
}

impl Weekday {
    const fn from_sunday_index(index: i32) -> Self {
        match index {
            0 => Self::Sunday,
            1 => Self::Monday,
            2 => Self::Tuesday,
            3 => Self::Wednesday,
            4 => Self::Thursday,
            5 => Self::Friday,
            _ => Self::Saturday,
        }
    }

    /// Returns the Sunday-zero index used by Go and MySQL `%w`.
    pub const fn sunday_index(self) -> u8 {
        match self {
            Self::Sunday => 0,
            Self::Monday => 1,
            Self::Tuesday => 2,
            Self::Wednesday => 3,
            Self::Thursday => 4,
            Self::Friday => 5,
            Self::Saturday => 6,
        }
    }

    /// Returns MySQL's abbreviated English weekday name.
    pub const fn abbreviated_name(self) -> &'static str {
        match self {
            Self::Sunday => "Sun",
            Self::Monday => "Mon",
            Self::Tuesday => "Tue",
            Self::Wednesday => "Wed",
            Self::Thursday => "Thu",
            Self::Friday => "Fri",
            Self::Saturday => "Sat",
        }
    }
}

impl fmt::Display for Weekday {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(SharedTime::weekday_name_from_sunday_index(u32::from(
            self.sunday_index(),
        )))
    }
}

impl fmt::Debug for CoreTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl fmt::Display for CoreTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{{{} {} {} {} {} {} {}}}",
            self.year(),
            self.month(),
            self.day(),
            self.hour(),
            self.minute(),
            self.second(),
            self.microsecond()
        )
    }
}

/// Returns whether `year` is a Gregorian leap year.
pub const fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

/// Returns the last valid day of `month`, or zero for an invalid month.
pub const fn get_last_day(year: i32, month: u8) -> u8 {
    if month == 0 || month > 12 {
        return 0;
    }
    if month == 2 && is_leap_year(year) {
        29
    } else {
        DAYS_BY_MONTH[month as usize - 1]
    }
}

/// Calculates days since MySQL's `0000-00-00` epoch.
pub const fn calc_daynr(year: i32, month: i32, day: i32) -> i32 {
    SharedTime::native_calc_daynr_i32(year, month, day)
}

/// Converts a MySQL day number back to a calendar date.
pub const fn get_date_from_daynr(daynr: u32) -> (u32, u32, u32) {
    shared_time::native_get_date_from_daynr(daynr)
}

#[cfg(test)]
fn fix_days(years: i64, months: i64, days: i64, original: CoreTime) -> i64 {
    shared_time::native_core_fix_days(original.raw(), years, months, days)
}

const WEEK_BEHAVIOUR_MONDAY_FIRST: u8 = 1;
const WEEK_BEHAVIOUR_YEAR: u8 = 2;
const WEEK_BEHAVIOUR_FIRST_WEEKDAY: u8 = 4;

const fn week_mode(mode: u8) -> u8 {
    SharedTime::normalize_week_mode_bits((mode & 7) as u32) as u8
}

/// Calculates weekday from a MySQL day number.
pub const fn calc_weekday(daynr: i32, sunday_first: bool) -> i32 {
    SharedTime::native_calc_weekday_i32(daynr, sunday_first)
}

/// Returns 365 or 366 using TiDB's year-zero rule.
pub const fn calc_days_in_year(year: i32) -> i32 {
    SharedTime::native_calc_days_in_year_i32(year)
}

const fn calc_week(time: CoreTime, behaviour: u8) -> (i32, i32) {
    SharedTime::native_calc_week_i32(
        time.year(),
        time.month() as i32,
        time.day() as i32,
        behaviour & WEEK_BEHAVIOUR_MONDAY_FIRST != 0,
        behaviour & WEEK_BEHAVIOUR_YEAR != 0,
        behaviour & WEEK_BEHAVIOUR_FIRST_WEEKDAY != 0,
    )
}

const fn datetime_to_u64(time: CoreTime) -> u64 {
    time.year() as u64 * 10_000_000_000
        + time.month() as u64 * 100_000_000
        + time.day() as u64 * 1_000_000
        + time.hour() as u64 * 10_000
        + time.minute() as u64 * 100
        + time.second() as u64
}

#[allow(clippy::too_many_arguments)]
fn time_diff_internal(
    left: CoreTime,
    year: i32,
    month: i32,
    day: i32,
    hour: i32,
    minute: i32,
    second: i32,
    microsecond: i32,
    sign: i32,
) -> TimeDifference {
    SharedTime::native_core_time_diff(
        left.raw(),
        year,
        month,
        day,
        hour,
        minute,
        second,
        microsecond,
        sign,
    )
}

fn timestamp_diff(interval: TimestampInterval, start: CoreTime, end: CoreTime) -> i64 {
    SharedTime::native_core_timestamp_diff(start.raw(), end.raw(), interval)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Datelike, Offset};

    #[test]
    fn from_date_keeps_const_raw_field_masking() {
        const NORMAL: CoreTime = CoreTime::from_date(1, 2, 3, 4, 5, 6, 7);
        const MASKED: CoreTime =
            CoreTime::from_date(0x4001, 0x12, 0x23, 0x24, 0x45, 0x46, 0x10_0007);
        const MAXIMUM: CoreTime = CoreTime::from_date(
            u16::MAX,
            u8::MAX,
            u8::MAX,
            u8::MAX,
            u8::MAX,
            u8::MAX,
            u32::MAX,
        );
        assert_eq!(NORMAL.raw(), 0x0004_8641_4600_0070);
        assert_eq!(MASKED.raw(), 0x0004_8641_4600_0070);
        assert_eq!(MAXIMUM.raw(), 0xffff_ffff_ffff_fff0);
        assert_eq!(CoreTime::from_date(0, 0, 0, 0, 0, 0, 0).raw(), 0);
        assert_eq!((MASKED.year(), MASKED.month(), MASKED.day()), (1, 2, 3));
        assert_eq!(
            (
                MASKED.hour(),
                MASKED.minute(),
                MASKED.second(),
                MASKED.microsecond()
            ),
            (4, 5, 6, 7)
        );
    }

    #[test]
    fn test_week_behaviour() {
        assert_eq!(1, WEEK_BEHAVIOUR_MONDAY_FIRST);
        assert_eq!(2, WEEK_BEHAVIOUR_YEAR);
        assert_eq!(4, WEEK_BEHAVIOUR_FIRST_WEEKDAY);
        assert_ne!(1 & WEEK_BEHAVIOUR_MONDAY_FIRST, 0);
        assert_ne!(2 & WEEK_BEHAVIOUR_YEAR, 0);
        assert_ne!(4 & WEEK_BEHAVIOUR_FIRST_WEEKDAY, 0);
    }

    #[test]
    fn test_week() {
        let time = CoreTime::from_date(2008, 2, 20, 0, 0, 0, 0);
        assert_eq!(time.week(0), 7);
        assert_eq!(time.week(1), 8);
        assert_eq!(CoreTime::from_date(2008, 12, 31, 0, 0, 0, 0).week(1), 53);
    }

    #[test]
    fn test_calc_daynr() {
        assert_eq!(calc_daynr(0, 0, 0), 0);
        assert_eq!(calc_daynr(9999, 12, 31), 3_652_424);
        assert_eq!(calc_daynr(1970, 1, 1), 719_528);
        assert_eq!(calc_daynr(2006, 12, 16), 733_026);
        assert_eq!(calc_daynr(10, 1, 2), 3_654);
        assert_eq!(calc_daynr(2008, 2, 20), 733_457);
    }

    #[test]
    fn test_compare_time() {
        let rows = [
            (
                CoreTime::from_date(0, 0, 0, 0, 0, 0, 0),
                CoreTime::from_date(0, 0, 0, 0, 0, 0, 0),
                Ordering::Equal,
            ),
            (
                CoreTime::from_date(0, 0, 0, 0, 1, 0, 0),
                CoreTime::default(),
                Ordering::Greater,
            ),
            (
                CoreTime::from_date(2006, 1, 2, 3, 4, 5, 6),
                CoreTime::from_date(2016, 1, 2, 3, 4, 5, 0),
                Ordering::Less,
            ),
            (
                CoreTime::from_date(0, 0, 0, 11, 22, 33, 0),
                CoreTime::from_date(0, 0, 0, 12, 21, 33, 0),
                Ordering::Less,
            ),
            (
                CoreTime::from_date(9999, 12, 30, 23, 59, 59, 999_999),
                CoreTime::from_date(0, 1, 2, 3, 4, 5, 6),
                Ordering::Greater,
            ),
        ];
        for (left, right, expected) in rows {
            assert_eq!(left.compare(right), expected);
            assert_eq!(right.compare(left), expected.reverse());
        }
    }

    #[test]
    fn test_calc_time_time_diff() {
        let rows = [
            (
                CoreTime::from_date(2006, 0, 1, 12, 23, 21, 0),
                CoreTime::from_date(2006, 0, 3, 21, 23, 22, 0),
                1,
                57 * 3_600 + 1,
            ),
            (
                CoreTime::from_date(0, 0, 0, 21, 23, 24, 0),
                CoreTime::from_date(0, 0, 0, 11, 23, 22, 0),
                1,
                10 * 3_600 + 2,
            ),
            (
                CoreTime::from_date(0, 0, 0, 1, 2, 3, 0),
                CoreTime::from_date(0, 0, 0, 5, 2, 0, 0),
                -1,
                6 * 3_600 + 4 * 60 + 3,
            ),
        ];
        for (left, right, sign, expected_seconds) in rows {
            let difference = left.time_diff(right, sign);
            assert_eq!(difference.seconds, expected_seconds);
            assert_eq!(difference.microseconds, 0);
        }
    }

    #[test]
    fn test_timestamp_diff() {
        let start = CoreTime::from_date(2002, 5, 1, 0, 0, 0, 0);
        let end = CoreTime::from_date(2001, 1, 1, 0, 0, 0, 0);
        assert_eq!(start.timestamp_diff(end, TimestampInterval::Year), -1);
        assert_eq!(start.timestamp_diff(end, TimestampInterval::Quarter), -5);
        assert_eq!(start.timestamp_diff(end, TimestampInterval::Month), -16);
        assert_eq!(start.timestamp_diff(end, TimestampInterval::Day), -485);
        assert_eq!(
            start.timestamp_diff(end, TimestampInterval::Microsecond),
            -41_904_000_000_000
        );
    }

    #[test]
    fn test_get_date_from_daynr() {
        for (daynr, expected) in [
            (730_669, (2000, 7, 3)),
            (720_195, (1971, 10, 30)),
            (719_528, (1970, 1, 1)),
            (719_892, (1970, 12, 31)),
            (730_850, (2000, 12, 31)),
            (730_544, (2000, 2, 29)),
            (204_960, (561, 2, 28)),
            (0, (0, 0, 0)),
            (32, (0, 0, 0)),
            (366, (1, 1, 1)),
            (744_729, (2038, 12, 31)),
            (3_652_424, (9999, 12, 31)),
        ] {
            assert_eq!(get_date_from_daynr(daynr), expected);
        }
    }

    #[test]
    fn test_mix_date_and_time() {
        let rows = [
            (
                CoreTime::from_date(1896, 3, 4, 0, 0, 0, 0),
                44_604_000_005_000,
                CoreTime::from_date(1896, 3, 4, 12, 23, 24, 5),
            ),
            (
                CoreTime::from_date(1896, 3, 4, 0, 0, 0, 0),
                87_804_000_005_000,
                CoreTime::from_date(1896, 3, 5, 0, 23, 24, 5),
            ),
            (
                CoreTime::from_date(2016, 12, 31, 0, 0, 0, 0),
                86_400_000_000_000,
                CoreTime::from_date(2017, 1, 1, 0, 0, 0, 0),
            ),
            (
                CoreTime::from_date(2016, 12, 0, 0, 0, 0, 0),
                86_400_000_000_000,
                CoreTime::from_date(2016, 12, 1, 0, 0, 0, 0),
            ),
            (
                CoreTime::from_date(2017, 1, 12, 3, 23, 15, 0),
                -8_470_000_000_000,
                CoreTime::from_date(2017, 1, 12, 1, 2, 5, 0),
            ),
        ];
        for (mut date, duration, expected) in rows {
            date.mix_duration(duration);
            assert_eq!(date, expected);
        }
    }

    #[test]
    fn test_is_leap_year() {
        for (time, expected) in [
            (CoreTime::from_date(1960, 1, 1, 0, 0, 0, 0), true),
            (CoreTime::from_date(1963, 2, 21, 0, 0, 0, 0), false),
            (CoreTime::from_date(2008, 11, 25, 0, 0, 0, 0), true),
            (CoreTime::from_date(2017, 4, 24, 0, 0, 0, 0), false),
            (CoreTime::from_date(1988, 2, 29, 0, 0, 0, 0), true),
            (CoreTime::from_date(2000, 3, 15, 0, 0, 0, 0), true),
            (CoreTime::from_date(1992, 5, 3, 0, 0, 0, 0), true),
            (CoreTime::from_date(2024, 10, 1, 0, 0, 0, 0), true),
            (CoreTime::from_date(2016, 6, 29, 0, 0, 0, 0), true),
            (CoreTime::from_date(2015, 6, 29, 0, 0, 0, 0), false),
            (CoreTime::from_date(2014, 9, 31, 0, 0, 0, 0), false),
            (CoreTime::from_date(2001, 12, 7, 0, 0, 0, 0), false),
            (CoreTime::from_date(1989, 7, 6, 0, 0, 0, 0), false),
        ] {
            assert_eq!(time.is_leap_year(), expected, "{time}");
        }
    }

    #[test]
    fn test_get_last_day() {
        for (year, month, expected) in [
            (2000, 1, 31),
            (2000, 2, 29),
            (2000, 4, 30),
            (1900, 2, 28),
            (1996, 2, 29),
        ] {
            assert_eq!(get_last_day(year, month), expected);
        }
    }

    #[test]
    fn test_weekday() {
        for (time, expected) in [
            (
                CoreTime::from_date(2019, 1, 1, 0, 0, 0, 0),
                Weekday::Tuesday,
            ),
            (
                CoreTime::from_date(2019, 2, 31, 0, 0, 0, 0),
                Weekday::Sunday,
            ),
            (
                CoreTime::from_date(2019, 4, 31, 0, 0, 0, 0),
                Weekday::Wednesday,
            ),
        ] {
            assert_eq!(time.weekday(), expected);
            assert_eq!(time.weekday().to_string(), expected.to_string());
        }
    }

    #[test]
    fn test_add_date() {
        let january_first = CoreTime::from_date(2000, 1, 1, 0, 0, 0, 0);
        for (years, months, days, original, should_error) in [
            (1, 1, 0, january_first, false),
            (2, 1, 12, january_first, false),
            (3, 1, 12, january_first, false),
            (
                4,
                2,
                24,
                CoreTime::from_date(2000, 2, 10, 0, 0, 0, 0),
                false,
            ),
            (1, 4, 5, CoreTime::from_date(2019, 4, 1, 1, 2, 3, 0), false),
            (7_999, 1, 1, january_first, false),
            (-2_000, 1, 1, january_first, false),
            (8_000, 1, 1, january_first, true),
            (10_001 * 365, 1, 1, january_first, true),
            (1, 10_001 * 36, 1, january_first, true),
            (1, 1, 10_001 * 365, january_first, true),
            (-2_001, 1, 1, january_first, true),
            (-10_001 * 365, 1, 1, january_first, true),
            (1, -10_001 * 36, 1, january_first, true),
            (1, 1, -10_001 * 365, january_first, true),
        ] {
            let result = original.add_date(years, months, days);
            if should_error {
                assert_eq!(result, Err(DateAddError));
            } else {
                assert_eq!(result.unwrap().year(), original.year() + years as i32);
            }
        }
    }

    #[test]
    fn add_date_clamps_month_end() {
        let january_end = CoreTime::from_date(2018, 1, 31, 1, 2, 3, 4);
        assert_eq!(
            january_end.add_date(0, 1, 0).unwrap(),
            CoreTime::from_date(2018, 2, 28, 1, 2, 3, 4)
        );
        assert_eq!(
            january_end.add_date(0, 1, 12).unwrap(),
            CoreTime::from_date(2018, 3, 15, 1, 2, 3, 4)
        );
    }

    #[test]
    fn test_get_fix_days() {
        for (years, months, days, original, expected) in [
            (
                2_000,
                1,
                0,
                CoreTime::from_date(2000, 1, 31, 0, 0, 0, 0),
                -2,
            ),
            (
                2_000,
                1,
                12,
                CoreTime::from_date(2000, 1, 31, 0, 0, 0, 0),
                0,
            ),
            (
                2_000,
                1,
                12,
                CoreTime::from_date(1999, 12, 31, 0, 0, 0, 0),
                0,
            ),
            (
                2_000,
                2,
                24,
                CoreTime::from_date(2000, 2, 10, 0, 0, 0, 0),
                0,
            ),
            (2_019, 4, 5, CoreTime::from_date(2019, 4, 1, 1, 2, 3, 0), 0),
        ] {
            assert_eq!(fix_days(years, months, days, original), expected);
        }
    }

    #[test]
    fn test_adjusted_go_time() {
        for (zone, input, expected_date, expected_clock, expected_microsecond, expected_offset) in [
            (
                "Australia/Lord_Howe",
                CoreTime::from_date(2020, 10, 4, 1, 59, 59, 997),
                (2020, 10, 4),
                (1, 59, 59),
                997,
                10 * 3_600 + 30 * 60,
            ),
            (
                "Australia/Lord_Howe",
                CoreTime::from_date(2020, 10, 4, 2, 0, 0, 0),
                (2020, 10, 4),
                (2, 30, 0),
                0,
                11 * 3_600,
            ),
            (
                "Australia/Lord_Howe",
                CoreTime::from_date(2020, 10, 4, 2, 15, 0, 0),
                (2020, 10, 4),
                (2, 30, 0),
                0,
                11 * 3_600,
            ),
            (
                "Australia/Lord_Howe",
                CoreTime::from_date(2020, 10, 4, 2, 29, 59, 999_999),
                (2020, 10, 4),
                (2, 30, 0),
                0,
                11 * 3_600,
            ),
            (
                "Australia/Lord_Howe",
                CoreTime::from_date(2020, 10, 4, 2, 30, 0, 1),
                (2020, 10, 4),
                (2, 30, 0),
                1,
                11 * 3_600,
            ),
            (
                "Australia/Lord_Howe",
                CoreTime::from_date(2020, 6, 29, 3, 45, 0, 0),
                (2020, 6, 29),
                (3, 45, 0),
                0,
                10 * 3_600 + 30 * 60,
            ),
            (
                "Australia/Lord_Howe",
                CoreTime::from_date(2020, 4, 4, 1, 45, 0, 0),
                (2020, 4, 4),
                (1, 45, 0),
                0,
                11 * 3_600,
            ),
            (
                "Europe/Vilnius",
                CoreTime::from_date(2020, 3, 29, 3, 45, 0, 0),
                (2020, 3, 29),
                (4, 0, 0),
                0,
                3 * 3_600,
            ),
            (
                "Europe/Vilnius",
                CoreTime::from_date(2020, 3, 29, 3, 59, 59, 456_789),
                (2020, 3, 29),
                (4, 0, 0),
                0,
                3 * 3_600,
            ),
            (
                "Europe/Vilnius",
                CoreTime::from_date(2020, 3, 29, 4, 0, 1, 130_000),
                (2020, 3, 29),
                (4, 0, 1),
                130_000,
                3 * 3_600,
            ),
            (
                "Europe/Vilnius",
                CoreTime::from_date(2020, 10, 25, 3, 45, 0, 0),
                (2020, 10, 25),
                (3, 45, 0),
                0,
                2 * 3_600,
            ),
            (
                "Europe/Vilnius",
                CoreTime::from_date(2020, 6, 29, 3, 45, 0, 0),
                (2020, 6, 29),
                (3, 45, 0),
                0,
                3 * 3_600,
            ),
            (
                "Europe/Amsterdam",
                CoreTime::from_date(2020, 3, 29, 2, 45, 0, 0),
                (2020, 3, 29),
                (3, 0, 0),
                0,
                2 * 3_600,
            ),
            (
                "Europe/Amsterdam",
                CoreTime::from_date(2020, 10, 25, 2, 35, 0, 0),
                (2020, 10, 25),
                (2, 35, 0),
                0,
                3_600,
            ),
        ] {
            let timezone: chrono_tz::Tz = zone.parse().unwrap();
            let value = input.adjusted_datetime(&timezone).unwrap();
            assert_eq!(
                (value.year(), value.month(), value.day()),
                expected_date,
                "{zone} {input}"
            );
            assert_eq!(
                (value.hour(), value.minute(), value.second()),
                expected_clock,
                "{zone} {input}"
            );
            assert_eq!(value.nanosecond() / 1_000, expected_microsecond);
            assert_eq!(value.offset().fix().local_minus_utc(), expected_offset);
        }

        let timezone: chrono_tz::Tz = "Europe/Amsterdam".parse().unwrap();
        let gap = CoreTime::from_date(2020, 3, 29, 2, 45, 0, 0);
        assert_eq!(
            gap.to_datetime(&timezone),
            Err(TimeConversionError::NonexistentLocalTime)
        );
        assert_eq!(
            CoreTime::from_date(2020, 2, 31, 2, 35, 0, 0).adjusted_datetime(&chrono_tz::UTC),
            Err(TimeConversionError::InvalidCalendar)
        );
    }
}

#[cfg(test)]
#[test]
fn shared_core_differences_keep_raw_fields_and_signed_operand_policy() {
    let start = CoreTime::from_date(2020, 1, 15, 12, 0, 0, 5);
    let end = CoreTime::from_date(2021, 4, 15, 12, 0, 0, 4);
    for (interval, expected) in [
        (TimestampInterval::Year, 1),
        (TimestampInterval::Quarter, 4),
        (TimestampInterval::Month, 14),
    ] {
        let shared: tidb_query_datatype::codec::mysql::time::NativeTimestampInterval = interval;
        assert_eq!(start.timestamp_diff(end, shared), expected);
        assert_eq!(end.timestamp_diff(start, interval), -expected);
    }
    let zero = CoreTime::default();
    let clock = CoreTime::from_date(0, 0, 0, 1, 2, 3, 4);
    assert_eq!(zero.timestamp_diff(clock, TimestampInterval::Second), 3723);
    assert_eq!(
        zero.timestamp_diff(clock, TimestampInterval::Microsecond),
        3_723_000_004
    );
    assert_eq!(
        clock.timestamp_diff(zero, TimestampInterval::Microsecond),
        -3_723_000_004
    );
    for (sign, seconds, microseconds, negative) in [
        (1, 122, 999995, false),
        (-1, 7323, 13, false),
        (0, 3723, 4, false),
        (2, 3477, 14, true),
    ] {
        let value: tidb_query_datatype::codec::mysql::time::NativeTimeDifference =
            time_diff_internal(clock, 0, 0, 0, 1, 0, 0, 9, sign);
        assert_eq!(
            value,
            TimeDifference {
                seconds,
                microseconds,
                negative
            }
        );
    }
    // The right operand's fields are i32 inputs, not packed calendar fields;
    // narrowing hour=48 into CoreTime's five-bit field would change the answer.
    assert_eq!(
        time_diff_internal(zero, 0, 0, 0, 48, 0, 0, 1, 1),
        TimeDifference {
            seconds: 172800,
            microseconds: 1,
            negative: true
        }
    );
    let raw = CoreTime::from_raw(u64::MAX & !15);
    let tagged = CoreTime::from_raw(u64::MAX);
    assert_eq!(
        raw.time_diff(tagged, 1),
        TimeDifference {
            seconds: 0,
            microseconds: 0,
            negative: false
        }
    );
    assert_eq!(raw.timestamp_diff(tagged, TimestampInterval::Month), 0);
    let first =
        crate::Time::from_date_checked(2020, 1, 1, 0, 0, 0, 0, crate::TimeType::DateTime, 0)
            .unwrap();
    let second =
        crate::Time::from_date_checked(2020, 1, 2, 0, 0, 0, 0, crate::TimeType::DateTime, 0)
            .unwrap();
    assert_eq!(crate::timestamp_diff("day", first, second), Ok(1));
    assert_eq!(
        crate::timestamp_diff(" DAY", first, second),
        Err(crate::TimeError::InvalidUnit(" DAY".to_owned()))
    );
}
