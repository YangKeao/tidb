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
fn year_cast_preserves_clock_demand_hybrid_names_ordinals_and_signed_fallback() {
    use super::{eval_cast_signed_value_in, eval_cast_year_in};
    use crate::{Columns, Datum, EvalError};
    use std::cell::{Cell, RefCell};
    use tidb_datatype::{Collation, GoString, MySqlDuration, MysqlEnum, MysqlSet, SessionTimeZone};

    struct Session {
        now: Cell<Option<(i64, u32, i32)>>,
        concat: Cell<bool>,
        events: RefCell<Vec<&'static str>>,
    }
    impl Columns for Session {
        fn get(&self, _: &[String]) -> Option<Datum> {
            panic!("direct YEAR cast must not evaluate children");
        }
        fn now(&self) -> Option<(i64, u32, i32)> {
            self.events.borrow_mut().push("now");
            self.now.get()
        }
        fn time_zone(&self) -> SessionTimeZone {
            self.events.borrow_mut().push("zone");
            SessionTimeZone::utc()
        }
        fn cast_time_to_year_through_concat(&self) -> bool {
            self.events.borrow_mut().push("concat");
            self.concat.get()
        }
        fn append_warning(&self, _: u16, _: &str) {
            panic!("value-only YEAR cast must not append warnings");
        }
        fn handle_truncate(&self, _: &str) -> Result<(), EvalError> {
            panic!("value-only YEAR cast must not report truncation");
        }
    }
    let ctx = Session {
        // The offset carried by now is ignored; only the session zone is used.
        now: Cell::new(Some((1_577_836_800, 0, i32::MAX))),
        concat: Cell::new(false),
        events: RefCell::new(Vec::new()),
    };
    let duration = Datum::Duration(MySqlDuration::new(0, 20, 12, 0, 0).unwrap());
    for (concat, expected) in [(false, 2020), (true, 2012)] {
        ctx.concat.set(concat);
        ctx.events.borrow_mut().clear();
        assert_eq!(eval_cast_year_in(&ctx, &duration), Ok(Datum::Int(expected)));
        assert_eq!(*ctx.events.borrow(), ["now", "zone", "concat"]);
    }
    ctx.events.borrow_mut().clear();
    assert_eq!(
        eval_cast_year_in(
            &ctx,
            &Datum::Duration(MySqlDuration::new(12, 59, 59, 0, 0).unwrap()),
        ),
        Err(EvalError::Unsupported("duration to YEAR conversion")),
    );
    assert_eq!(*ctx.events.borrow(), ["now", "zone", "concat"]);
    for (now, error) in [
        (None, "no statement clock for a YEAR cast"),
        (Some((i64::MAX, 0, 0)), "statement clock is out of range"),
        (
            Some((1_577_836_800, 2_000_000_000, 0)),
            "statement clock is out of range",
        ),
    ] {
        ctx.now.set(now);
        ctx.events.borrow_mut().clear();
        assert_eq!(
            eval_cast_year_in(&ctx, &duration),
            Err(EvalError::Unsupported(error))
        );
        assert_eq!(*ctx.events.borrow(), ["now"]);
    }
    ctx.events.borrow_mut().clear();
    for (value, expected) in [
        (Datum::new_string("2024-01-02"), 2024),
        (Datum::new_string("not-a-year"), 0),
        (Datum::UInt(u64::MAX), -1),
        (Datum::UInt(1_u64 << 63), i64::MIN),
        (
            Datum::Enum(MysqlEnum::new("2024-01-02", 17), Collation::DEFAULT),
            2024,
        ),
        (
            Datum::Enum(MysqlEnum::new("ordinal", 17), Collation::DEFAULT),
            17,
        ),
        (
            Datum::Enum(MysqlEnum::new("ordinal", u64::MAX), Collation::DEFAULT),
            i64::MAX,
        ),
        (
            Datum::Set(MysqlSet::new("2024-01-02", 9), Collation::DEFAULT),
            2024,
        ),
        (Datum::Set(MysqlSet::new("mask", 9), Collation::DEFAULT), 9),
    ] {
        assert_eq!(
            eval_cast_year_in(&ctx, &value),
            Ok(Datum::Int(expected)),
            "{value:?}"
        );
        assert!(ctx.events.borrow().is_empty());
    }
    for (value, error) in [
        (Datum::new_bytes([0xff]), "invalid UTF-8 byte datum"),
        (
            Datum::Enum(
                MysqlEnum::new(GoString::from_bytes([0xff]), 17),
                Collation::DEFAULT,
            ),
            "invalid UTF-8 ENUM name",
        ),
        (
            Datum::Set(
                MysqlSet::new(GoString::from_bytes([0xff]), 9),
                Collation::DEFAULT,
            ),
            "invalid UTF-8 SET name",
        ),
        (Datum::MaxValue, "range sentinel string coercion"),
    ] {
        assert_eq!(
            eval_cast_year_in(&ctx, &value),
            Err(EvalError::Unsupported(error))
        );
        assert!(ctx.events.borrow().is_empty());
    }
    assert_eq!(
        eval_cast_signed_value_in(
            &Datum::Enum(MysqlEnum::new("2024-01-02", 17), Collation::DEFAULT),
            &SessionTimeZone::utc(),
        ),
        17,
    );
    assert_eq!(
        eval_cast_signed_value_in(&Datum::UInt(u64::MAX), &SessionTimeZone::utc()),
        -1,
    );
}
