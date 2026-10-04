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

use chrono::TimeZone;
use tidb_query_datatype::codec::mysql::time::native_parse_str_to_date;
pub use tidb_query_datatype::codec::mysql::time::{
    native_is_go_punctuation as is_go_punctuation,
    native_str_to_date_format_type as get_format_type,
};

use crate::{CoreTime, Time, TimeError};

impl Time {
    /// Parses a MySQL `STR_TO_DATE` value.
    ///
    /// The boolean reports trailing source characters, matching TiDB's warning
    /// result without coupling the datatype crate to a statement context.
    pub fn str_to_date<TZ: TimeZone>(
        date: &str,
        format: &str,
        allow_zero_in_date: bool,
        allow_invalid_date: bool,
        timezone: &TZ,
    ) -> Result<(Self, bool), TimeError> {
        let (value, warning) = native_parse_str_to_date(date, format)?;
        let result = Self::new(CoreTime::from_raw(value.raw), value.kind, value.fsp.into())?;
        result.validate(allow_zero_in_date, allow_invalid_date, timezone)?;
        Ok((result, warning))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(input: &str, format: &str, allow_invalid: bool) -> Result<CoreTime, TimeError> {
        Time::str_to_date(input, format, true, allow_invalid, &chrono_tz::UTC)
            .map(|(time, _)| time.core_time())
    }

    #[test]
    fn test_str_to_date() {
        let rows = [
            (
                "01,05,2013",
                "%d,%m,%Y",
                CoreTime::from_date(2013, 5, 1, 0, 0, 0, 0),
            ),
            (
                "5 12 2021",
                "%m%d%Y",
                CoreTime::from_date(2021, 5, 12, 0, 0, 0, 0),
            ),
            (
                "May 01, 2013",
                "%M %d,%Y",
                CoreTime::from_date(2013, 5, 1, 0, 0, 0, 0),
            ),
            (
                "a09:30:17",
                "a%h:%i:%s",
                CoreTime::from_date(0, 0, 0, 9, 30, 17, 0),
            ),
            (
                "09:30:17a",
                "%h:%i:%s",
                CoreTime::from_date(0, 0, 0, 9, 30, 17, 0),
            ),
            (
                "12:43:24",
                "%h:%i:%s",
                CoreTime::from_date(0, 0, 0, 0, 43, 24, 0),
            ),
            ("abc", "abc", CoreTime::default()),
            ("09", "%m", CoreTime::from_date(0, 9, 0, 0, 0, 0, 0)),
            ("09", "%s", CoreTime::from_date(0, 0, 0, 0, 0, 9, 0)),
            (
                "12:43:24 AM",
                "%r",
                CoreTime::from_date(0, 0, 0, 0, 43, 24, 0),
            ),
            (
                "12:43:24 PM",
                "%r",
                CoreTime::from_date(0, 0, 0, 12, 43, 24, 0),
            ),
            (
                "11:43:24 PM",
                "%r",
                CoreTime::from_date(0, 0, 0, 23, 43, 24, 0),
            ),
            ("00:12:13", "%T", CoreTime::from_date(0, 0, 0, 0, 12, 13, 0)),
            (
                "23:59:59",
                "%T",
                CoreTime::from_date(0, 0, 0, 23, 59, 59, 0),
            ),
            ("00/00/0000", "%m/%d/%Y", CoreTime::default()),
            (
                "04/30/2004",
                "%m/%d/%Y",
                CoreTime::from_date(2004, 4, 30, 0, 0, 0, 0),
            ),
            (
                "15:35:00",
                "%H:%i:%s",
                CoreTime::from_date(0, 0, 0, 15, 35, 0, 0),
            ),
            (
                "Jul 17 33",
                "%b %k %S",
                CoreTime::from_date(0, 7, 0, 17, 0, 33, 0),
            ),
            (
                "2016-January:7 432101",
                "%Y-%M:%l %f",
                CoreTime::from_date(2016, 1, 0, 7, 0, 0, 432_101),
            ),
            (
                "10:13 PM",
                "%l:%i %p",
                CoreTime::from_date(0, 0, 0, 22, 13, 0, 0),
            ),
            ("12:00:00 AM", "%h:%i:%s %p", CoreTime::default()),
            (
                "12:00:00 PM",
                "%h:%i:%s %p",
                CoreTime::from_date(0, 0, 0, 12, 0, 0, 0),
            ),
            (
                "12:00:00 PM",
                "%I:%i:%s %p",
                CoreTime::from_date(0, 0, 0, 12, 0, 0, 0),
            ),
            (
                "1:00:00 PM",
                "%h:%i:%s %p",
                CoreTime::from_date(0, 0, 0, 13, 0, 0, 0),
            ),
            (
                "18/10/22",
                "%y/%m/%d",
                CoreTime::from_date(2018, 10, 22, 0, 0, 0, 0),
            ),
            (
                "8/10/22",
                "%y/%m/%d",
                CoreTime::from_date(2008, 10, 22, 0, 0, 0, 0),
            ),
            (
                "69/10/22",
                "%y/%m/%d",
                CoreTime::from_date(2069, 10, 22, 0, 0, 0, 0),
            ),
            (
                "70/10/22",
                "%y/%m/%d",
                CoreTime::from_date(1970, 10, 22, 0, 0, 0, 0),
            ),
            (
                "18/10/22",
                "%Y/%m/%d",
                CoreTime::from_date(2018, 10, 22, 0, 0, 0, 0),
            ),
            (
                "2018/10/22",
                "%Y/%m/%d",
                CoreTime::from_date(2018, 10, 22, 0, 0, 0, 0),
            ),
            (
                "8/10/22",
                "%Y/%m/%d",
                CoreTime::from_date(2008, 10, 22, 0, 0, 0, 0),
            ),
            (
                "69/10/22",
                "%Y/%m/%d",
                CoreTime::from_date(2069, 10, 22, 0, 0, 0, 0),
            ),
            (
                "70/10/22",
                "%Y/%m/%d",
                CoreTime::from_date(1970, 10, 22, 0, 0, 0, 0),
            ),
            (
                "18/10/22",
                "%Y/%m/%d",
                CoreTime::from_date(2018, 10, 22, 0, 0, 0, 0),
            ),
            (
                "100/10/22",
                "%Y/%m/%d",
                CoreTime::from_date(100, 10, 22, 0, 0, 0, 0),
            ),
            (
                "09/10/1021",
                "%d/%m/%y",
                CoreTime::from_date(2010, 10, 9, 0, 0, 0, 0),
            ),
            (
                "09/10/1021",
                "%d/%m/%Y",
                CoreTime::from_date(1021, 10, 9, 0, 0, 0, 0),
            ),
            (
                "09/10/10",
                "%d/%m/%Y",
                CoreTime::from_date(2010, 10, 9, 0, 0, 0, 0),
            ),
            (
                "31/may/2016 12:34:56.1234",
                "%d/%b/%Y %H:%i:%S.%f",
                CoreTime::from_date(2016, 5, 31, 12, 34, 56, 123_400),
            ),
            (
                "30/april/2016 12:34:56.",
                "%d/%M/%Y %H:%i:%s.%f",
                CoreTime::from_date(2016, 4, 30, 12, 34, 56, 0),
            ),
            (
                "31/mAy/2016 12:34:56.1234",
                "%d/%b/%Y %H:%i:%S.%f",
                CoreTime::from_date(2016, 5, 31, 12, 34, 56, 123_400),
            ),
            (
                "30/apRil/2016 12:34:56.",
                "%d/%M/%Y %H:%i:%s.%f",
                CoreTime::from_date(2016, 4, 30, 12, 34, 56, 0),
            ),
            (
                " 04 :13:56 AM13/05/2019",
                "%r %d/%c/%Y",
                CoreTime::from_date(2019, 5, 13, 4, 13, 56, 0),
            ),
            (
                "12: 13:56 AM 13/05/2019",
                "%r%d/%c/%Y",
                CoreTime::from_date(2019, 5, 13, 0, 13, 56, 0),
            ),
            (
                "12:13 :56 pm 13/05/2019",
                "%r %d/%c/%Y",
                CoreTime::from_date(2019, 5, 13, 12, 13, 56, 0),
            ),
            (
                "12:3: 56pm  13/05/2019",
                "%r %d/%c/%Y",
                CoreTime::from_date(2019, 5, 13, 12, 3, 56, 0),
            ),
            (
                "11:13:56",
                "%r",
                CoreTime::from_date(0, 0, 0, 11, 13, 56, 0),
            ),
            ("11:13", "%r", CoreTime::from_date(0, 0, 0, 11, 13, 0, 0)),
            ("11:", "%r", CoreTime::from_date(0, 0, 0, 11, 0, 0, 0)),
            ("11", "%r", CoreTime::from_date(0, 0, 0, 11, 0, 0, 0)),
            ("12", "%r", CoreTime::default()),
            (
                " 4 :13:56 13/05/2019",
                "%T %d/%c/%Y",
                CoreTime::from_date(2019, 5, 13, 4, 13, 56, 0),
            ),
            (
                "23: 13:56  13/05/2019",
                "%T%d/%c/%Y",
                CoreTime::from_date(2019, 5, 13, 23, 13, 56, 0),
            ),
            (
                "12:13 :56 13/05/2019",
                "%T %d/%c/%Y",
                CoreTime::from_date(2019, 5, 13, 12, 13, 56, 0),
            ),
            (
                "19:3: 56  13/05/2019",
                "%T %d/%c/%Y",
                CoreTime::from_date(2019, 5, 13, 19, 3, 56, 0),
            ),
            ("21:13", "%T", CoreTime::from_date(0, 0, 0, 21, 13, 0, 0)),
            ("21:", "%T", CoreTime::from_date(0, 0, 0, 21, 0, 0, 0)),
            (
                " 2/Jun",
                "%d/%b/%Y",
                CoreTime::from_date(0, 6, 2, 0, 0, 0, 0),
            ),
            (" liter", "lit era l", CoreTime::default()),
            (
                "29/Feb/2020 12:34:56.",
                "%d/%b/%Y %H:%i:%s.%f",
                CoreTime::from_date(2020, 2, 29, 12, 34, 56, 0),
            ),
            (
                "31/April/2016 12:34:56.",
                "%d/%M/%Y %H:%i:%s.%f",
                CoreTime::from_date(2016, 4, 31, 12, 34, 56, 0),
            ),
            (
                "29/Feb/2021 12:34:56.",
                "%d/%b/%Y %H:%i:%s.%f",
                CoreTime::from_date(2021, 2, 29, 12, 34, 56, 0),
            ),
            (
                "30/Feb/2016 12:34:56.1234",
                "%d/%b/%Y %H:%i:%S.%f",
                CoreTime::from_date(2016, 2, 30, 12, 34, 56, 123_400),
            ),
        ];
        assert_eq!(rows.len(), 63, "one entry per Go success source row");
        for (input, format, expected) in rows {
            assert_eq!(
                parse(input, format, true).unwrap(),
                expected,
                "{input} {format}"
            );
        }
    }

    #[test]
    fn punctuation_token_matches_go_unicode_punctuation() {
        assert_eq!(
            parse("2013¿5", "%Y%.%c", true),
            Ok(CoreTime::from_date(2013, 5, 0, 0, 0, 0, 0))
        );
        assert!(parse("2013+5", "%Y%.%c", true).is_err());
        assert!(parse("2013\u{1b4e}5", "%Y%.%c", true).is_err());
    }

    #[test]
    fn exhausted_format_tokens_preserve_go_meridiem_fix_state() {
        // Go records ctx["%p"] = 0 after the clock is consumed. With `%H`,
        // mysqlTimeFix rejects the combination; with `%h`, the absent AM/PM
        // token is treated as AM and the parsed hour remains valid.
        assert!(parse("11:30:45", "%H:%i:%s %p", true).is_err());
        assert_eq!(
            parse("11:30:45", "%h:%i:%s %p", true),
            Ok(CoreTime::from_date(0, 0, 0, 11, 30, 45, 0))
        );
        assert!(parse("", "%p", true).is_err());
    }

    /// Go `Time.StrToDate` forwards `FlagIgnoreZeroInDate` to `Time.Check`.
    /// A partial format therefore keeps its zero month/day only when the
    /// caller explicitly allows zero-in-date values.
    #[test]
    fn str_to_date_zero_in_date_flag_is_not_hardcoded() {
        let refused = Time::str_to_date("2013-05", "%Y-%m", false, false, &chrono_tz::UTC);
        assert_eq!(refused, Err(TimeError::ZeroInDate));

        let accepted = Time::str_to_date("2013-05", "%Y-%m", true, false, &chrono_tz::UTC)
            .expect("IgnoreZeroInDate keeps the partial date");
        assert_eq!(accepted.0.to_string(), "2013-05-00 00:00:00");
    }

    #[test]
    fn test_str_to_date_errors() {
        let rows = [
            ("04/31/2004", "%m/%d/%Y", false),
            ("29/Feb/2021 12:34:56.", "%d/%b/%Y %H:%i:%s.%f", false),
            ("512 2021", "%m%d %Y", true),
            ("a09:30:17", "%h:%i:%s", true),
            ("12:43:24 a", "%r", true),
            ("23:60:12", "%T", true),
            ("18", "%l", true),
            ("00:21:22 AM", "%h:%i:%s %p", true),
            ("100/10/22", "%y/%m/%d", true),
            ("2010-11-12 11 am", "%Y-%m-%d %H %p", true),
            ("2010-11-12 13 am", "%Y-%m-%d %h %p", true),
            ("2010-11-12 0 am", "%Y-%m-%d %h %p", true),
            ("15 SEPTEMB 2001", "%d %M %Y", true),
            ("13:13:56 AM13/5/2019", "%r", true),
            ("00:13:56 AM13/05/2019", "%r", true),
            ("00:13:56 pM13/05/2019", "%r", true),
            ("11:13:56a", "%r", true),
        ];
        assert_eq!(rows.len(), 17, "one entry per Go error source row");
        for (input, format, allow_invalid) in rows {
            assert!(
                parse(input, format, allow_invalid).is_err(),
                "{input} {format}"
            );
        }
    }

    /// Complete translation of `pkg/types/time_test.go::TestGetFormatType`.
    #[test]
    fn test_get_format_type() {
        assert_eq!(get_format_type("TEST"), (false, false));
        assert_eq!(get_format_type("%y %m %d 2019 04 01"), (false, true));
        assert_eq!(get_format_type("%h 30"), (true, false));
    }

    #[test]
    fn get_format_type_supplemental_rows() {
        assert_eq!(get_format_type("%Y-%m-%d %H:%i:%s"), (true, true));
        assert_eq!(get_format_type("%"), (false, false));
    }
}

#[cfg(test)]
#[test]
fn shared_str_to_date_adapter_keeps_validation_flags_metadata_and_timezone_boundary() {
    #[derive(Clone)]
    struct NoTimezoneReads;
    impl chrono::TimeZone for NoTimezoneReads {
        type Offset = chrono::FixedOffset;
        fn from_offset(_: &Self::Offset) -> Self {
            panic!("DATETIME must not reconstruct a timezone")
        }
        fn offset_from_local_date(
            &self,
            _: &chrono::NaiveDate,
        ) -> chrono::LocalResult<Self::Offset> {
            panic!("DATETIME must not resolve local dates")
        }
        fn offset_from_local_datetime(
            &self,
            _: &chrono::NaiveDateTime,
        ) -> chrono::LocalResult<Self::Offset> {
            panic!("DATETIME must not resolve local clocks")
        }
        fn offset_from_utc_date(&self, _: &chrono::NaiveDate) -> Self::Offset {
            panic!("DATETIME must not resolve UTC dates")
        }
        fn offset_from_utc_datetime(&self, _: &chrono::NaiveDateTime) -> Self::Offset {
            panic!("DATETIME must not resolve UTC clocks")
        }
    }
    for allow_zero in [false, true] {
        for allow_invalid in [false, true] {
            let partial = Time::str_to_date(
                "2013-05",
                "%Y-%m",
                allow_zero,
                allow_invalid,
                &NoTimezoneReads,
            )
            .map(|(value, warning)| (value.core_time(), warning));
            assert_eq!(
                partial,
                if allow_zero {
                    Ok((CoreTime::from_date(2013, 5, 0, 0, 0, 0, 0), false))
                } else {
                    Err(TimeError::ZeroInDate)
                }
            );
            let invalid = Time::str_to_date(
                "2021-02-29",
                "%Y-%m-%d",
                allow_zero,
                allow_invalid,
                &NoTimezoneReads,
            )
            .map(|(value, warning)| (value.core_time(), warning));
            assert_eq!(
                invalid,
                if allow_invalid {
                    Ok((CoreTime::from_date(2021, 2, 29, 0, 0, 0, 0), false))
                } else {
                    Err(TimeError::InvalidDate)
                }
            );
            // Meridiem fixing must fail before the later zero-in-date check.
            assert_eq!(
                Time::str_to_date(
                    "2013-05 23 AM",
                    "%Y-%m %H %p",
                    allow_zero,
                    allow_invalid,
                    &NoTimezoneReads
                ),
                Err(TimeError::InvalidClock)
            );
        }
    }
    // Preserve hidden microseconds even though this public parser constructs
    // DATETIME(0), and carry its independent trailing-source warning bit.
    for (suffix, expected_warning) in [("", false), ("tail", true)] {
        let (value, warning) = Time::str_to_date(
            &format!("2020-03-08 02:30:00.123456{suffix}"),
            "%Y-%m-%d %H:%i:%s.%f",
            false,
            false,
            &NoTimezoneReads,
        )
        .unwrap();
        assert_eq!(
            value.core_time().raw(),
            CoreTime::from_date(2020, 3, 8, 2, 30, 0, 123456).raw()
        );
        assert_eq!(value.kind(), crate::TimeType::DateTime);
        assert_eq!(value.fsp(), 0);
        assert_eq!(value.to_string(), "2020-03-08 02:30:00");
        assert_eq!(warning, expected_warning);
    }
}
