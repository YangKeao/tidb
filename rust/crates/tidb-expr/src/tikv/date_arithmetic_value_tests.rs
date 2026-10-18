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

//! Original calendar/default/duration demand, warning and overflow witnesses.
use super::*;
use crate::{Columns, Datum, ErrorLevel, EvalError};
use std::cell::RefCell;
use tidb_datatype::{FieldType, FieldTypeCode, MySqlDuration};
struct Context {
    cache: Option<ReadyValueCache>,
    reject: bool,
    level: usize,
    events: RefCell<Vec<String>>,
}
impl Columns for Context {
    fn get(&self, _: &[String]) -> Option<Datum> {
        panic!("no invented date arithmetic source")
    }
    fn ready_value_cache(&self) -> Option<&ReadyValueCache> {
        assert!(
            !self.reject,
            "date arithmetic attempted retired Ready lookup"
        );
        self.cache.as_ref()
    }
    fn context_id(&self) -> u64 {
        8301
    }
    fn truncate_level(&self) -> ErrorLevel {
        self.events
            .borrow_mut()
            .push(format!("level:{}", self.level));
        match self.level {
            0 => ErrorLevel::Warn,
            1 => ErrorLevel::Error,
            _ => ErrorLevel::Ignore,
        }
    }
    fn append_warning(&self, code: u16, message: &str) {
        self.events
            .borrow_mut()
            .push(format!("warn:{code}:{message}"));
    }
    fn date_modes(&self) -> tidb_datatype::DateModes {
        panic!("date arithmetic does not read date modes")
    }
    fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
        panic!("date arithmetic does not read time zone")
    }
}
fn context(profile: usize, level: usize) -> Context {
    Context {
        cache: (profile != 0).then(ReadyValueCache::new),
        reject: false,
        level,
        events: RefCell::new(vec![]),
    }
}
fn dates() -> Vec<Datum> {
    let mut values = crate::ops::arithmetic::tests::fixtures();
    values.extend(
        [
            "2020-01-31",
            "2020-02-29",
            "2021-02-29",
            "0000-00-00",
            "0000-01-01",
            "9999-12-31",
            "2020-01-01 12:34:56.123456",
            "2020-01-01 23:59:59.999999",
            "20200101",
            "2020-00-01",
            " bad ",
            "24:00:00",
            "2020-01-01 trailing",
            " 一😂 ",
        ]
        .map(Datum::new_string),
    );
    values
}
fn amounts() -> Vec<Datum> {
    let mut values = crate::ops::arithmetic::tests::fixtures();
    values.extend(
        [
            "1.23456789",
            "1 02:03:04.005",
            "-1-02",
            "+12tail",
            "-9999999999999999999999",
            "1e100",
            "1:02:03.004005",
        ]
        .map(Datum::new_string),
    );
    values
}
fn duration_case(
    ctx: &dyn Columns,
    unit: &str,
    date: &Datum,
    amount: &Datum,
    decimal: Option<i64>,
    sign: i64,
    fsp: u32,
) -> Result<Datum, EvalError> {
    let amount_field = decimal.map(|v| FieldType::new(FieldTypeCode::Double).with_decimal(v));
    let result = FieldType::new(FieldTypeCode::Duration).with_decimal(i64::from(fsp));
    super::date_arithmetic::date_add_scalar_values_bounded(
        ctx,
        unit,
        &mut [date.clone(), amount.clone()],
        [None, amount_field.as_ref()],
        Some(&result),
        sign,
        Some(fsp),
        64 << 20,
    )
}
fn invoke(
    ctx: &dyn Columns,
    variant: usize,
    unit: &str,
    date: &Datum,
    amount: &Datum,
) -> Result<Datum, EvalError> {
    match variant {
        0 => eval_date_add_in(ctx, unit, date, amount, 1, None),
        1 => eval_date_add_in(ctx, unit, date, amount, -1, None),
        2 => eval_date_add_in(ctx, unit, date, amount, 1, Some(0)),
        3 => eval_date_add_in(ctx, unit, date, amount, -1, Some(6)),
        4 => eval_date_add_default_in(ctx, unit, date, amount, 1),
        5 => eval_date_add_default_in(ctx, unit, date, amount, -1),
        6 => duration_case(ctx, unit, date, amount, None, 1, 6),
        7 => duration_case(ctx, unit, date, amount, Some(2), -1, 0),
        8 => duration_case(ctx, unit, date, amount, Some(6), 1, 6),
        _ => unreachable!(),
    }
}
fn observe(value: &Result<Datum, EvalError>, ctx: &Context) -> String {
    let identity = value
        .as_ref()
        .ok()
        .map(|v| identity_value::encode(v).unwrap());
    format!("{value:?}|{identity:?}|{:?}\n", ctx.events.borrow())
}
#[test]
fn original_date_arithmetic_calendar_default_duration_demand_and_overflow() {
    let dates = dates();
    let amounts = amounts();
    let units = [
        "DAY",
        "WEEK",
        "MONTH",
        "QUARTER",
        "YEAR",
        "HOUR",
        "MINUTE",
        "SECOND",
        "MICROSECOND",
        "YEAR_MONTH",
        "DAY_SECOND",
        "DAY_MICROSECOND",
        "HOUR_MINUTE",
        "SECOND_MICROSECOND",
        "day",
        "bad一",
    ];
    let mut hash = 0xcbf29ce484222325u64;
    let mut count = 0;
    for profile in 0..3 {
        for level in 0..3 {
            let mut ctx = context(profile, level);
            for variant in 0..9 {
                for unit in units {
                    for date in &dates {
                        for amount in &amounts {
                            if profile == 2 {
                                ctx = context(profile, level)
                            }
                            ctx.events.borrow_mut().clear();
                            let value = invoke(&ctx, variant, unit, date, amount);
                            for b in observe(&value, &ctx).bytes() {
                                hash = (hash ^ u64::from(b)).wrapping_mul(0x100000001b3)
                            }
                            count += 1;
                        }
                    }
                }
            }
        }
    }
    eprintln!("ORIGINAL date arithmetic count={count} hash={hash:016x}");
    assert_eq!(count, 2325024);
    assert_eq!(hash, 0xfeea99cd98022f82);
}
#[test]
fn date_arithmetic_all_actual_entries_never_lookup_retired_ready_workers() {
    let mut ctx = context(0, 0);
    ctx.reject = true;
    let mut panicked = vec![];
    for variant in 0..9 {
        let date = if variant < 6 {
            Datum::new_string("2020-01-01")
        } else {
            Datum::new_duration(MySqlDuration::from_raw_parts(3600000000000, 6))
        };
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            invoke(&ctx, variant, "SECOND", &date, &Datum::Int(1))
        }))
        .is_err()
        {
            panicked.push(variant)
        }
    }
    assert!(
        panicked.is_empty(),
        "actual entries still require Ready programs: {panicked:?}"
    );
}
#[test]
fn date_arithmetic_finite_work_follows_actual_demand_and_precision() {
    use tidb_query_expr::NativeDateArithmeticValueConfig as Config;
    let run = |date: &Datum, amount: &Datum, unit, config, maximum| {
        let mut ctx = context(0, 0);
        ctx.reject = true;
        let result = super::date_arithmetic::date_arithmetic_values_bounded(
            &ctx, unit, date, amount, config, maximum,
        );
        (result, ctx.events.into_inner())
    };
    let huge = Datum::new_string("1".repeat(16384));
    let (value, events) = run(&Datum::Null, &huge, "DAY", Config::calendar(1, None), 6);
    assert!(matches!(value, Ok(Datum::Null)));
    assert!(events.is_empty());
    let (value, events) = run(
        &Datum::Null,
        &huge,
        "DAY_SECOND",
        Config::duration(1, 6, Some(i64::MAX)),
        0,
    );
    assert!(matches!(value, Ok(Datum::Null)));
    assert!(events.is_empty());
    let resource = |value: Result<Datum, EvalError>| {
        assert!(
            matches!(value, Err(EvalError::ExpressionRuntimeFailure(_))),
            "{value:?}"
        )
    };
    let (value, events) = run(
        &Datum::Null,
        &huge,
        "DAY_SECOND",
        Config::calendar(1, None),
        8192,
    );
    resource(value);
    assert!(events.is_empty());
    let duration = Datum::new_duration(MySqlDuration::from_raw_parts(0, 6));
    let (value, events) = run(
        &duration,
        &Datum::Real(1.25),
        "HOUR_MINUTE",
        Config::duration(1, 6, Some(i64::MAX)),
        8192,
    );
    resource(value);
    assert!(events.is_empty());
    // A valid precision does not expand nonfinite text; an invalid formatter
    // parameter still passes through the work gate before its original panic.
    for n in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        let (value, events) = run(
            &duration,
            &Datum::Real(n),
            "HOUR_MINUTE",
            Config::duration(1, 6, Some(65535)),
            8192,
        );
        // Original composite parsing of NaN/inf text contributes zero.
        assert_eq!(value.unwrap(), duration);
        assert!(events.is_empty());
        let (value, events) = run(
            &duration,
            &Datum::Real(n),
            "HOUR_MINUTE",
            Config::duration(1, 6, Some(i64::MAX)),
            8192,
        );
        resource(value);
        assert!(events.is_empty());
    }
    let (value, events) = run(
        &Datum::new_string("9999-12-31"),
        &Datum::Int(1),
        "DAY",
        Config::calendar(1, None),
        128,
    );
    resource(value);
    assert!(
        events.is_empty(),
        "overflow disposition must remain unread on resource refusal"
    );
    // A fresh valid owner remains usable after all the rejected operations.
    let (value, _) = run(
        &Datum::new_string("2020-01-01"),
        &Datum::Int(1),
        "DAY",
        Config::calendar(1, None),
        8192,
    );
    assert_eq!(value.unwrap().go_bytes(), b"2020-01-02");
}

#[test]
fn date_arithmetic_incoming_spare_capacity_is_live_during_warning_work() {
    use tidb_query_datatype::codec::native_sql_string::NativeSqlStringInput as I;
    use tidb_query_expr::{
        NativeDateArithmeticValueConfig as Config, NativeDateArithmeticValueError as E,
        NativeDateArithmeticValues as State,
    };
    let mut state = State::new(
        [I::String(b"bad"), I::Int(1)],
        "DAY",
        Config::calendar(1, None),
        6000,
    )
    .unwrap();
    state.begin().unwrap();
    let mut incoming = String::with_capacity(6000);
    incoming.push_str("bad");
    assert!(matches!(
        state.resume_string(Some(incoming)),
        Err(E::ResourceLimit)
    ));
    assert!(matches!(state.resume_string(None), Err(E::InvalidResponse)));
}
