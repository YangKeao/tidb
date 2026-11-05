// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Independent original calendar/core/week observations before retirement.
use super::*;
use crate::{Columns, Datum, EvalError};
use std::cell::{Cell, RefCell};
use tidb_datatype::{CoreTime, DateModes, SessionTimeZone, Time, TimeType};
pub(crate) const NAMES: [&str; 20] = [
    "DATE",
    "YEAR",
    "MONTH",
    "DAY",
    "DAYOFMONTH",
    "QUARTER",
    "HOUR",
    "MINUTE",
    "SECOND",
    "DAYOFWEEK",
    "DAYOFYEAR",
    "WEEKDAY",
    "MONTHNAME",
    "DAYNAME",
    "WEEKOFYEAR",
    "WEEK",
    "YEARWEEK",
    "LAST_DAY",
    "TO_DAYS",
    "TO_SECONDS",
];
pub(crate) struct Context {
    cache: Option<ReadyValueCache>,
    pub(crate) reject: bool,
    pub(crate) enabled: Cell<bool>,
    seed: usize,
    level: usize,
    modes: Cell<usize>,
    zones: Cell<usize>,
    defaults: Cell<usize>,
    pub(crate) events: RefCell<Vec<String>>,
}
pub(crate) fn context(profile: usize, seed: usize, level: usize) -> Context {
    Context {
        cache: (profile != 0).then(ReadyValueCache::default),
        reject: false,
        enabled: Cell::new(false),
        seed,
        level,
        modes: Cell::new(0),
        zones: Cell::new(0),
        defaults: Cell::new(0),
        events: RefCell::new(vec![]),
    }
}
impl Context {
    pub(crate) fn reset(&self) {
        self.modes.set(0);
        self.zones.set(0);
        self.defaults.set(0);
        self.events.borrow_mut().clear();
    }
}
impl Columns for Context {
    fn get(&self, _: &[String]) -> Option<Datum> {
        panic!("no invented calendar source")
    }
    fn context_id(&self) -> u64 {
        8901
    }
    fn ready_value_cache(&self) -> Option<&ReadyValueCache> {
        assert!(!self.reject, "calendar attempted retired Ready lookup");
        self.cache.as_ref()
    }
    fn enable_vectorized_expression(&self) -> bool {
        self.enabled.get()
    }
    fn date_modes(&self) -> DateModes {
        let n = self.modes.get();
        self.modes.set(n + 1);
        let b = (self.seed + n) % 8;
        self.events.borrow_mut().push(format!("mode:{n}:{b}"));
        DateModes {
            no_zero_date: b & 1 != 0,
            no_zero_in_date: b & 2 != 0,
            allow_invalid_dates: b & 4 != 0,
        }
    }
    fn time_zone(&self) -> SessionTimeZone {
        let n = self.zones.get();
        self.zones.set(n + 1);
        self.events.borrow_mut().push(format!("zone:{n}"));
        match (self.seed + n) % 3 {
            0 => SessionTimeZone::utc(),
            1 => SessionTimeZone::Named(chrono_tz::America::Los_Angeles),
            _ => SessionTimeZone::Fixed {
                name: "raw+24".into(),
                offset_secs: 86400,
            },
        }
    }
    fn now(&self) -> Option<(i64, u32, i32)> {
        self.events.borrow_mut().push("clock".into());
        if self.seed % 3 == 0 {
            None
        } else {
            Some((1700000000, 654321955, 28800))
        }
    }
    fn default_week_format(&self) -> i64 {
        let n = self.defaults.get();
        self.defaults.set(n + 1);
        let v = [0, 1, 2, 3, 7, -1, i64::MIN, i64::MAX][(self.seed + n) % 8];
        self.events
            .borrow_mut()
            .push(format!("week_default:{n}:{v}"));
        v
    }
    fn truncate_level(&self) -> crate::ErrorLevel {
        self.events
            .borrow_mut()
            .push(format!("level:{}", self.level));
        match self.level {
            0 => crate::ErrorLevel::Warn,
            1 => crate::ErrorLevel::Error,
            _ => crate::ErrorLevel::Ignore,
        }
    }
    fn append_warning(&self, code: u16, message: &str) {
        self.events
            .borrow_mut()
            .push(format!("warning:{code}:{message}"));
    }
}
pub(crate) fn original(
    name: &str,
    values: &[Datum],
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    match name {
        "YEAR" => crate::time_fn::calendar::year_in(values, ctx),
        "HOUR" => crate::time_fn::calendar::hour_in(values, ctx),
        "MINUTE" => crate::time_fn::calendar::minute_in(values, ctx),
        "SECOND" => crate::time_fn::calendar::second_in(values, ctx),
        _ => crate::time_fn::dispatch(name, values, ctx).unwrap(),
    }
}
fn values() -> Vec<Datum> {
    let mut v = crate::ops::arithmetic::tests::fixtures();
    v.extend(
        [
            "0000-00-00",
            "0000-01-01",
            "2020-00-00",
            "2020-02-31",
            "2000-02-29 12:34:56.123456",
            "2019-12-30",
            "2020-12-31",
            "2021-01-01",
            "20240315",
            "71/1/1",
            "2024-01-01 garbage",
            "2024-01-01 99:99:99",
            "-900:59:59.999999",
            "24:59:59",
            "bad",
            "",
        ]
        .into_iter()
        .map(Datum::new_string),
    );
    for raw in [
        0,
        u64::MAX,
        tidb_query_datatype::codec::mysql::Time::native_core_from_fields(
            0, 1, 1, 12, 34, 56, 123456,
        ),
        tidb_query_datatype::codec::mysql::Time::native_core_from_fields(
            2020, 0, 2, 12, 34, 56, 123456,
        ),
        tidb_query_datatype::codec::mysql::Time::native_core_from_fields(
            2024, 2, 29, 12, 34, 56, 654321,
        ),
    ] {
        for kind in [TimeType::Date, TimeType::DateTime, TimeType::Timestamp] {
            for fsp in [0, 3, 6] {
                v.push(Datum::Time(Time::from_raw_parts(
                    CoreTime::from_raw(raw),
                    kind,
                    fsp,
                )));
            }
        }
    }
    v
}
fn modes() -> Vec<Datum> {
    vec![
        Datum::Null,
        Datum::Int(0),
        Datum::Int(3),
        Datum::Int(-1),
        Datum::UInt(u64::MAX),
        Datum::new_string(" 3.5 "),
        Datum::Bytes(vec![0xff]),
        Datum::Decimal(crate::Decimal::from_literal("999999999999999999999999")),
    ]
}
fn observation(value: Result<Datum, EvalError>, ctx: &Context) -> String {
    let raw = match &value {
        Ok(Datum::Time(t)) => format!("/{:016x}/{:?}/{}", t.core_time().raw(), t.kind(), t.fsp()),
        _ => String::new(),
    };
    format!("{value:?}{raw}/{:?}", ctx.events.borrow())
}
fn legacy(which: usize, raw: Option<u64>, ctx: &dyn Columns) -> Result<Datum, EvalError> {
    match which {
        0 => crate::time_fn::month_core_in(raw.map(CoreTime::from_raw), ctx)
            .map(|v| v.map_or(Datum::Null, Datum::Int)),
        1 => crate::time_fn::week_core_in(raw.map(CoreTime::from_raw), ctx),
        i => crate::time_fn::calendar::hms_nanos_in(
            [
                crate::LegacyHmsField::Hour,
                crate::LegacyHmsField::Minute,
                crate::LegacyHmsField::Second,
            ][i - 2],
            raw.map(|v| v as i64),
            ctx,
        )
        .map(|v| v.map_or(Datum::Null, Datum::Int)),
    }
}
#[test]
fn calendar_original_observations() {
    let mut count = 0usize;
    let mut hash = 0xcbf29ce484222325u64;
    let mut record = |v, ctx: &Context| {
        for b in observation(v, ctx).bytes() {
            hash = (hash ^ u64::from(b)).wrapping_mul(0x100000001b3);
        }
        count += 1;
    };
    for profile in 0..3 {
        for seed in 0..8 {
            for level in 0..3 {
                let ctx = context(profile, seed, level);
                for name in NAMES {
                    for vals in [vec![], vec![Datum::Null, Datum::Null, Datum::Null]] {
                        ctx.reset();
                        record(original(name, &vals, &ctx), &ctx);
                    }
                    for value in values() {
                        ctx.reset();
                        record(original(name, std::slice::from_ref(&value), &ctx), &ctx);
                        if matches!(name, "WEEK" | "YEARWEEK") {
                            for mode in modes() {
                                ctx.reset();
                                record(original(name, &[value.clone(), mode], &ctx), &ctx);
                            }
                        }
                    }
                }
                for which in 0..5 {
                    for raw in [
                        None,
                        Some(0),
                        Some(u64::MAX),
                        Some(0x1ec8420000000000),
                        Some(i64::MIN as u64),
                        Some(9_223_372_036_854_775_807),
                    ] {
                        ctx.reset();
                        record(legacy(which, raw, &ctx), &ctx);
                    }
                }
            }
        }
    }
    eprintln!("CALENDAR OLD {count} {hash:016X}");
    assert_eq!((count, hash), (246096, 0xE72FB95D39CD0172));
}
#[test]
fn calendar_direct_never_requests_ready() {
    let mut failed = vec![];
    let value = Datum::Time(Time::from_raw_parts(
        CoreTime::from_raw(0x1ec8420000000000),
        TimeType::DateTime,
        0,
    ));
    for name in NAMES {
        let mut ctx = context(1, 0, 0);
        ctx.reject = true;
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            original(name, std::slice::from_ref(&value), &ctx)
        }))
        .is_err()
        {
            failed.push(name.to_owned());
        }
    }
    for which in 0..5 {
        let mut ctx = context(1, 0, 0);
        ctx.reject = true;
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            legacy(which, Some(0), &ctx)
        }))
        .is_err()
        {
            failed.push(format!("legacy{which}"));
        }
    }
    assert!(failed.is_empty(), "Ready routes: {failed:?}");
}
#[test]
fn calendar_values_finite_work_null_mode_storage_and_original_panic() {
    use tidb_query_expr::NativeCalendarOperation as O;
    let ctx = context(0, 0, 0);
    assert_eq!(
        super::calendar_value::eval_bounded(O::Year, &[Datum::Null], 0, &ctx, usize::MAX),
        Ok(Datum::Null)
    );
    assert_eq!(
        super::calendar_value::eval_bounded(O::Year, &[Datum::Null], 0, &ctx, 0),
        Ok(Datum::Null)
    );
    assert!(ctx.events.borrow().is_empty());
    let huge = Datum::Bytes(vec![b'1'; 100000]);
    assert_eq!(
        super::calendar_value::eval_bounded(O::Week, &[Datum::Null, huge.clone()], 3, &ctx, 8192),
        Ok(Datum::Null)
    );
    assert!(matches!(
        super::calendar_value::eval_bounded(O::Hour, &[huge], 0, &ctx, 8192),
        Err(EvalError::ExpressionRuntimeFailure(_))
    ));
    let decimal = Datum::Decimal(tidb_datatype::Decimal::from_raw_parts(
        false,
        vec![b'3'],
        1000000,
        0,
    ));
    assert_eq!(super::calendar_value::mode(&decimal, 8192), Ok(Some(3)));
    let t = Datum::Time(Time::from_raw_parts(
        CoreTime::from_raw(0),
        TimeType::DateTime,
        255,
    ));
    // Accepted zero dates never render unused FSP. Rejected dates preflight
    // diagnostics before formatting, while ample work preserves the old panic.
    assert!(matches!(
        super::calendar_value::eval_bounded(O::Date, std::slice::from_ref(&t), 0, &ctx, 8192),
        Ok(Datum::Time(_))
    ));
    let reject = context(0, 1, 0);
    assert!(matches!(
        super::calendar_value::eval_bounded(O::Date, std::slice::from_ref(&t), 0, &reject, 0),
        Err(EvalError::ExpressionRuntimeFailure(_))
    ));
    let reject = context(0, 1, 0);
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        super::calendar_value::eval_bounded(O::Date, std::slice::from_ref(&t), 0, &reject, 8192)
    }))
    .is_err());
}
