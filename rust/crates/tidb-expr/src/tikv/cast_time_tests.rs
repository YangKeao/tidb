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

#[test]
fn time_cast_bridges_preserve_raw_views_early_returns_and_context_warning_order() {
    use super::{eval_cast_arg_as_datetime_in, eval_cast_time_value_in};
    use crate::{Columns, Datum, EvalError};
    use std::cell::{Cell, RefCell};
    use tidb_datatype::{
        Collation, CoreTime, DateModes, FieldType, FieldTypeCode, MySqlDuration, MysqlEnum,
        SessionTimeZone, Time, TimeType,
    };

    struct Session {
        now: Cell<Option<(i64, u32, i32)>>,
        los_angeles: Cell<bool>,
        events: RefCell<Vec<&'static str>>,
        warnings: RefCell<Vec<(u16, String)>>,
    }
    impl Columns for Session {
        fn get(&self, _: &[String]) -> Option<Datum> {
            panic!("direct temporal bridge must not evaluate children");
        }
        fn date_modes(&self) -> DateModes {
            self.events.borrow_mut().push("modes");
            DateModes {
                no_zero_date: true,
                no_zero_in_date: true,
                allow_invalid_dates: false,
            }
        }
        fn now(&self) -> Option<(i64, u32, i32)> {
            self.events.borrow_mut().push("now");
            self.now.get()
        }
        fn time_zone(&self) -> SessionTimeZone {
            self.events.borrow_mut().push("zone");
            if self.los_angeles.get() {
                SessionTimeZone::Named(chrono_tz::America::Los_Angeles)
            } else {
                SessionTimeZone::utc()
            }
        }
        fn append_warning(&self, code: u16, message: &str) {
            self.events.borrow_mut().push("warn");
            self.warnings.borrow_mut().push((code, message.to_owned()));
        }
        fn handle_truncate(&self, _: &str) -> Result<(), EvalError> {
            panic!("datetime read warnings must not use truncation policy");
        }
    }
    impl Session {
        fn reset(&self) {
            self.events.borrow_mut().clear();
            self.warnings.borrow_mut().clear();
        }
        fn check(&self, events: &[&str], warnings: &[(u16, &str)]) {
            assert_eq!(self.events.borrow().as_slice(), events);
            let expected: Vec<_> = warnings
                .iter()
                .map(|(code, text)| (*code, text.to_string()))
                .collect();
            assert_eq!(*self.warnings.borrow(), expected);
        }
    }
    let ctx = Session {
        now: Cell::new(Some((1_577_836_800, 0, -3600))),
        los_angeles: Cell::new(false),
        events: RefCell::new(Vec::new()),
        warnings: RefCell::new(Vec::new()),
    };
    let year = FieldType::new(FieldTypeCode::Year);
    let cast = |value: &Datum, source: Option<&FieldType>, kind, fsp| {
        eval_cast_time_value_in(&ctx, value, source, kind, fsp)
    };
    assert_eq!(
        cast(&Datum::Null, Some(&year), TimeType::DateTime, Some(6)),
        Ok(None)
    );
    assert_eq!(
        eval_cast_arg_as_datetime_in(&ctx, &Datum::Null, Some(&year)),
        Ok(Datum::Null)
    );
    let raw = Time::from_raw_parts(
        CoreTime::from_date(2024, 1, 2, 3, 4, 5, 123456),
        TimeType::Timestamp,
        6,
    );
    assert_eq!(
        eval_cast_arg_as_datetime_in(&ctx, &Datum::Time(raw), Some(&year)),
        Ok(Datum::Time(raw))
    );
    let raw_fsp = Time::from_raw_parts(CoreTime::default(), TimeType::Date, 255);
    assert_eq!(
        eval_cast_arg_as_datetime_in(&ctx, &Datum::Time(raw_fsp), None),
        Ok(Datum::Time(raw_fsp))
    );
    let from_year = cast(&Datum::Int(2018), Some(&year), TimeType::Date, Some(6))
        .unwrap()
        .unwrap();
    assert_eq!(from_year.kind(), TimeType::DateTime);
    assert_eq!(from_year.fsp(), 0);
    assert_eq!(from_year.to_string(), "2018-00-00 00:00:00");
    let zero_year = cast(&Datum::Int(0), Some(&year), TimeType::DateTime, Some(6))
        .unwrap()
        .unwrap();
    assert_eq!(zero_year.kind(), TimeType::Date);
    assert_eq!(zero_year.fsp(), 0);
    assert_eq!(
        cast(&Datum::Int(-1), Some(&year), TimeType::DateTime, None),
        Err(EvalError::Unsupported(
            "a YEAR value outside the year range"
        ))
    );
    assert_eq!(
        cast(&Datum::new_bytes([0xff]), None, TimeType::DateTime, None),
        Err(EvalError::Unsupported("invalid UTF-8 byte datum"))
    );
    ctx.check(&[], &[]);

    let date = cast(&Datum::Time(raw), None, TimeType::Date, Some(0))
        .unwrap()
        .unwrap();
    assert_eq!(date.kind(), TimeType::Date);
    assert_eq!(
        date.core_time(),
        CoreTime::from_date(2024, 1, 2, 0, 0, 0, 0)
    );
    ctx.check(&["modes", "zone"], &[]);
    ctx.reset();
    assert!(cast(&Datum::Int(0), None, TimeType::DateTime, Some(0))
        .unwrap()
        .unwrap()
        .is_zero());
    ctx.check(&["modes", "zone"], &[]);
    for (value, warning) in [
        (
            Datum::new_string("0000-00-00"),
            "Incorrect datetime value: '0000-00-00 00:00:00.000000'",
        ),
        (Datum::UInt(u64::MAX), "Incorrect time value: '-1'"),
        (
            Datum::Enum(MysqlEnum::new("bad", 17), Collation::DEFAULT),
            "Incorrect time value: '17'",
        ),
    ] {
        ctx.reset();
        assert_eq!(cast(&value, None, TimeType::DateTime, Some(0)), Ok(None));
        ctx.check(&["modes", "zone", "warn"], &[(1292, warning)]);
    }

    let duration = Datum::Duration(MySqlDuration::new(1, 0, 0, 0, 0).unwrap());
    for (fsp, events) in [
        (None, &["modes", "now"][..]),
        (Some(0), &["modes", "now", "zone"][..]),
    ] {
        ctx.reset();
        let time = cast(&duration, None, TimeType::DateTime, fsp)
            .unwrap()
            .unwrap();
        assert_eq!(time.to_string(), "2019-12-31 01:00:00");
        ctx.check(events, &[]);
    }
    ctx.now.set(None);
    ctx.reset();
    assert_eq!(
        cast(&duration, None, TimeType::DateTime, Some(0)),
        Err(EvalError::Unsupported("no statement clock for a TIME cast"))
    );
    ctx.check(&["modes", "now"], &[]);
    ctx.now.set(Some((i64::MAX, 0, i32::MAX)));
    ctx.reset();
    assert_eq!(
        cast(&duration, None, TimeType::DateTime, Some(0)),
        Err(EvalError::Unsupported(
            "session time-zone offset out of range"
        ))
    );
    ctx.check(&["modes", "now"], &[]);
    ctx.now.set(Some((i64::MAX, 0, 0)));
    ctx.reset();
    assert_eq!(cast(&duration, None, TimeType::DateTime, Some(0)), Ok(None));
    ctx.check(&["modes", "now"], &[]);

    ctx.los_angeles.set(true);
    ctx.reset();
    let time = cast(
        &Datum::new_string("2011-03-13 02:00:00"),
        None,
        TimeType::Timestamp,
        Some(0),
    )
    .unwrap()
    .unwrap();
    assert_eq!(time.to_string(), "2011-03-13 03:00:00");
    let warning = format!(
        "Timestamp is not valid, since it is in Daylight Saving Time transition '2011-03-13 02:00:00' for time zone '{:?}'",
        SessionTimeZone::Named(chrono_tz::America::Los_Angeles),
    );
    ctx.check(
        &["modes", "zone", "zone", "warn"],
        &[(8179, warning.as_str())],
    );
}
