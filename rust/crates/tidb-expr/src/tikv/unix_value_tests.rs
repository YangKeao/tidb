// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Original native/legacy timestamp conversion and formatter observations.
use super::*;
use crate::{Columns, Datum, Decimal, ErrorLevel, EvalError};
use std::cell::{Cell, RefCell};
use tidb_datatype::{CoreTime, SessionTimeZone, Time, TimeType};
pub(crate) struct Context {
    cache: Option<ReadyValueCache>,
    pub(crate) reject: bool,
    pub(crate) enabled: Cell<bool>,
    seed: usize,
    level: usize,
    zones: Cell<usize>,
    modes: Cell<usize>,
    pub(crate) events: RefCell<Vec<String>>,
}
fn zone(n: usize) -> SessionTimeZone {
    match n % 8 {
        0 => SessionTimeZone::utc(),
        1 => SessionTimeZone::Named(chrono_tz::America::Los_Angeles),
        2 => SessionTimeZone::Named(chrono_tz::Europe::Paris),
        3 => SessionTimeZone::Fixed {
            name: "+08".into(),
            offset_secs: 28800,
        },
        4 => SessionTimeZone::Fixed {
            name: "raw+24".into(),
            offset_secs: 86400,
        },
        5 => SessionTimeZone::Fixed {
            name: "raw-24".into(),
            offset_secs: -86400,
        },
        6 => SessionTimeZone::Fixed {
            name: "rawMAX".into(),
            offset_secs: i32::MAX,
        },
        _ => SessionTimeZone::Fixed {
            name: "rawMIN".into(),
            offset_secs: i32::MIN,
        },
    }
}
impl Columns for Context {
    fn get(&self, _: &[String]) -> Option<Datum> {
        panic!("no invented timestamp source")
    }
    fn ready_value_cache(&self) -> Option<&ReadyValueCache> {
        assert!(!self.reject, "timestamp attempted retired Ready lookup");
        self.cache.as_ref()
    }
    fn enable_vectorized_expression(&self) -> bool {
        self.enabled.get()
    }
    fn context_id(&self) -> u64 {
        8601
    }
    fn now(&self) -> Option<(i64, u32, i32)> {
        self.events.borrow_mut().push("now".into());
        match self.seed % 6 {
            0 => None,
            1 => Some((0, u32::MAX, 28800)),
            2 => Some((1, 123456789, 0)),
            3 => Some((-1, 0, -3600)),
            4 => Some((32_536_771_199, 999999999, 0)),
            _ => Some((32_536_771_200, 0, 0)),
        }
    }
    fn time_zone(&self) -> SessionTimeZone {
        let n = self.zones.get();
        self.zones.set(n + 1);
        let z = zone(self.seed + n);
        self.events.borrow_mut().push(format!("zone:{n}:{z:?}"));
        z
    }
    fn date_modes(&self) -> tidb_datatype::DateModes {
        let n = self.modes.get();
        self.modes.set(n + 1);
        let b = (self.seed + n) % 8;
        self.events.borrow_mut().push(format!("modes:{n}:{b}"));
        tidb_datatype::DateModes {
            no_zero_date: b & 1 != 0,
            no_zero_in_date: b & 2 != 0,
            allow_invalid_dates: b & 4 != 0,
        }
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
    fn append_warning(&self, code: u16, s: &str) {
        self.events.borrow_mut().push(format!("warn:{code}:{s}"));
    }
}
pub(crate) fn context(profile: usize, seed: usize, level: usize) -> Context {
    Context {
        cache: (profile != 0).then(ReadyValueCache::new),
        reject: false,
        enabled: Cell::new(crate::NoColumns.enable_vectorized_expression()),
        seed,
        level,
        zones: Cell::new(0),
        modes: Cell::new(0),
        events: RefCell::new(vec![]),
    }
}
impl Context {
    fn reset(&self) {
        self.zones.set(0);
        self.modes.set(0);
        self.events.borrow_mut().clear();
    }
}
fn values() -> Vec<Datum> {
    let mut v = crate::ops::arithmetic::tests::fixtures();
    v.extend(
        [
            "0000-00-00 12:34:56.123",
            "2017-00-02 00:00:00.123",
            "2017-01-00",
            "2020-02-31",
            "2024.03.15 12:34:56.1",
            "2025-03-30 02:30:00",
            "2025-10-26 02:30:00",
            "2011-03-13 02:30:00",
            "2011-11-06 01:30:00",
            "1970-01-01 00:00:00.999999",
            "1970-01-01 00:00:01.000001",
            "3001-01-18 23:59:59.999999",
            "3001-01-19 00:00:00",
            "-0.5",
            "1.123456789junk",
            "32536771199.9999999",
            "1.0e2",
            " 1e2 ",
            "",
            " 一😂 ",
        ]
        .map(Datum::new_string),
    );
    for s in [
        "0",
        "-0.5",
        "0.000001",
        "1.123456789",
        "32536771199.999999",
        "32536771200",
        "151113102019",
    ] {
        v.push(Datum::Decimal(Decimal::from_literal(s)));
    }
    for (negative, digits, scale, storage) in [
        (false, b"1255".as_slice(), 2, 3),
        (true, b"000".as_slice(), 2, 3),
        (false, b"000001".as_slice(), 6, 6),
    ] {
        v.push(Datum::Decimal(
            Decimal::from_raw_parts(negative, digits.to_vec(), scale, storage)
                .with_declared_shape(30, 9),
        ));
    }
    v.push(Datum::Bytes(vec![0xff, 0]));
    v
}
fn times() -> Vec<Option<Time>> {
    let mut v = vec![None];
    for raw in [
        0,
        u64::MAX,
        tidb_query_datatype::codec::mysql::Time::native_core_from_fields(
            1970, 1, 1, 0, 0, 1, 123456,
        ),
        tidb_query_datatype::codec::mysql::Time::native_core_from_fields(2025, 3, 30, 2, 30, 0, 0),
        tidb_query_datatype::codec::mysql::Time::native_core_from_fields(2025, 10, 26, 2, 30, 0, 0),
        tidb_query_datatype::codec::mysql::Time::native_core_from_fields(2020, 0, 2, 0, 0, 0, 0),
    ] {
        for kind in [TimeType::Date, TimeType::DateTime, TimeType::Timestamp] {
            for fsp in [0, 3, 6, 255] {
                v.push(Some(Time::from_raw_parts(
                    CoreTime::from_raw(raw),
                    kind,
                    fsp,
                )));
            }
        }
    }
    v
}
fn formats() -> Vec<Datum> {
    vec![
        Datum::Null,
        Datum::new_string(""),
        Datum::new_string("%Y-%m-%d %H:%i:%s.%f %X %x %V %v"),
        Datum::new_string("%W %M %D %j %U %u %w %q%"),
        Datum::Bytes(vec![0xff]),
        Datum::Int(12),
        Datum::MinNotNull,
    ]
}
fn observe(result: Result<Datum, EvalError>, ctx: &Context) -> String {
    let raw = result
        .as_ref()
        .ok()
        .map(|v| identity_value::encode(v).unwrap());
    format!("{result:?}|{raw:?}|{:?}\n", ctx.events.borrow())
}

#[test]
fn original_timestamp_native_legacy_context_and_date_format_observations() {
    let values = values();
    let times = times();
    let formats = formats();
    let mut count = 0;
    let mut hash = 0xcbf29ce484222325u64;
    let mut record = |result, ctx: &Context| {
        for b in observe(result, ctx).bytes() {
            hash = (hash ^ u64::from(b)).wrapping_mul(0x100000001b3);
        }
        count += 1;
    };
    for profile in 0..3 {
        for seed in 0..12 {
            for level in 0..3 {
                let mut ctx = context(profile, seed, level);
                macro_rules! run {
                    ($e:expr) => {{
                        if profile == 2 {
                            ctx = context(profile, seed, level);
                        }
                        ctx.reset();
                        record($e, &ctx);
                    }};
                }
                run!(crate::time_fn::session_tz::unix_timestamp(&[], &ctx));
                run!(crate::time_fn::session_tz::unix_timestamp(
                    &[Datum::Null, Datum::Null],
                    &ctx
                ));
                run!(crate::time_fn::session_tz::from_unixtime(&[], &ctx));
                run!(crate::time_fn::session_tz::from_unixtime(
                    &[Datum::Null, Datum::Null, Datum::Null],
                    &ctx
                ));
                for value in &values {
                    run!(crate::time_fn::session_tz::unix_timestamp(
                        std::slice::from_ref(value),
                        &ctx
                    ));
                    run!(crate::time_fn::session_tz::from_unixtime(
                        std::slice::from_ref(value),
                        &ctx
                    ));
                    for format in &formats {
                        run!(crate::time_fn::session_tz::from_unixtime(
                            &[value.clone(), format.clone()],
                            &ctx
                        ));
                        run!(crate::time_fn::calendar::date_format_in(
                            value, format, &ctx
                        ));
                    }
                }
                for value in &times {
                    for n in 0..8 {
                        let z = zone(n);
                        run!(unix_timestamp_int_legacy_in(*value, &z, &ctx)
                            .map(|v| v.map_or(Datum::Null, |v| Datum::Int(v as i64))));
                        run!(unix_timestamp_dec_legacy_in(*value, &z, &ctx)
                            .map(|v| v.map_or(Datum::Null, Datum::Decimal)));
                        for format in [None, Some("%Y-%m-%d %H:%i:%s.%f %W %X %x")] {
                            run!(crate::eval_legacy_date_format_in(
                                value.map(|t| (t.core_time(), format)),
                                &ctx
                            )
                            .map(|v| v.map_or(Datum::Null, Datum::Bytes)));
                        }
                    }
                }
                for decimal in std::iter::once(None).chain(values.iter().filter_map(|v| {
                    if let Datum::Decimal(d) = v {
                        Some(Some(d.clone()))
                    } else {
                        None
                    }
                })) {
                    for n in 0..8 {
                        let z = zone(n);
                        run!(eval_from_unixtime_legacy_scoped_in(
                            decimal.clone(),
                            &z,
                            &ctx,
                            |v, selected| {
                                ctx.events
                                    .borrow_mut()
                                    .push(format!("pack:{}", selected.context_id()));
                                v.map_or(Datum::Null, Datum::Time)
                            }
                        ));
                        run!(eval_from_unixtime_legacy_scoped_in(
                            decimal.clone(),
                            &z,
                            &ctx,
                            |v, selected| {
                                ctx.events
                                    .borrow_mut()
                                    .push(format!("pack:{}", selected.context_id()));
                                crate::eval_legacy_date_format_in(
                                    v.map(|t| (t.core_time(), Some("%Y-%m-%d %f"))),
                                    selected,
                                )
                                .map(|v| v.map_or(Datum::Null, Datum::Bytes))
                            }
                        )
                        .and_then(|v| v));
                    }
                }
                run!(crate::eval_date_format_null_in(&ctx));
                run!(crate::eval_legacy_date_format_missing_in(&ctx)
                    .map(|v| v.map_or(Datum::Null, |v| Datum::Int(v as i64))));
            }
        }
    }
    assert_eq!(
        count, 384264,
        "old timestamp count={count} digest={hash:016X}"
    );
    assert_eq!(hash, 0x14B4FDA02CC55EED);
}
#[test]
fn timestamp_and_format_actual_entries_never_lookup_ready() {
    let mut ctx = context(0, 2, 0);
    ctx.reject = true;
    let mut panicked = vec![];
    for case in 0..12 {
        let run = || -> Result<Datum, EvalError> {
            match case {
                0 => crate::time_fn::session_tz::unix_timestamp(&[], &ctx),
                1 => crate::time_fn::session_tz::unix_timestamp(&[Datum::Null], &ctx),
                2 => crate::time_fn::session_tz::unix_timestamp(
                    &[Datum::new_string("2020-01-02")],
                    &ctx,
                ),
                3 => crate::time_fn::session_tz::from_unixtime(&[Datum::Null], &ctx),
                4 => crate::time_fn::session_tz::from_unixtime(&[Datum::Int(42)], &ctx),
                5 => crate::time_fn::session_tz::from_unixtime(&[Datum::new_string("x")], &ctx),
                6 => crate::time_fn::session_tz::from_unixtime(
                    &[Datum::Int(42), Datum::new_string("%Y")],
                    &ctx,
                ),
                7 => unix_timestamp_int_legacy_in(None, &zone(0), &ctx).map(|_| Datum::Null),
                8 => unix_timestamp_dec_legacy_in(times()[2], &zone(0), &ctx).map(|_| Datum::Null),
                9 => eval_from_unixtime_legacy_scoped_in(
                    Some(Decimal::from_literal("42")),
                    &zone(0),
                    &ctx,
                    |v, _| v.map_or(Datum::Null, Datum::Time),
                ),
                10 => crate::time_fn::calendar::date_format_in(
                    &Datum::new_string("2020-01-02"),
                    &Datum::new_string("%Y"),
                    &ctx,
                ),
                _ => crate::eval_legacy_date_format_in(
                    Some((CoreTime::from_raw(0), Some("%Y"))),
                    &ctx,
                )
                .map(|_| Datum::Null),
            }
        };
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(run)).is_err() {
            panicked.push(case);
        }
    }
    assert!(
        panicked.is_empty(),
        "retired timestamp/format Ready routes {panicked:?}"
    );
}

#[test]
fn timestamp_actual_return_width_without_padding_does_not_allocate_declared_width() {
    let field =
        tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::Varchar).with_flen(i64::MAX);
    let expected = crate::scalar_function::coerce_function_result(
        "unix_timestamp",
        Some(&field),
        Datum::Int(42),
    )
    .unwrap();
    assert_eq!(expected, Datum::new_string("42"));
    assert_eq!(
        super::epoch_value::finish_epoch_result_bounded(Datum::Int(42), &field, false).unwrap(),
        expected
    );
}
