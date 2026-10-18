// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0.
//! Independent original legacy source-channel, explicit-zone and presence record.
use super::*;
use crate::{Columns, Datum, EvalError};
use std::cell::RefCell;
use tidb_datatype::{Decimal, MySqlDuration, SessionTimeZone, Time};
use LegacyDateArithmeticDateKind as D;
use LegacyDateArithmeticIntervalKind as I;
use LegacyDateArithmeticValue as V;
struct Context {
    cache: Option<ReadyValueCache>,
    reject: bool,
    events: RefCell<Vec<String>>,
}
impl Columns for Context {
    fn get(&self, _: &[String]) -> Option<Datum> {
        panic!("no fabricated legacy sources")
    }
    fn ready_value_cache(&self) -> Option<&ReadyValueCache> {
        assert!(
            !self.reject,
            "legacy date arithmetic attempted retired Ready lookup"
        );
        self.cache.as_ref()
    }
    fn context_id(&self) -> u64 {
        8401
    }
    fn date_modes(&self) -> tidb_datatype::DateModes {
        panic!("legacy flags are fixed, not statement modes")
    }
    fn time_zone(&self) -> SessionTimeZone {
        panic!("legacy explicit zone is authoritative")
    }
    fn append_warning(&self, code: u16, text: &str) {
        self.events.borrow_mut().push(format!("warn:{code}:{text}"))
    }
}
fn context(profile: usize) -> Context {
    Context {
        cache: (profile != 0).then(ReadyValueCache::new),
        reject: false,
        events: RefCell::new(vec![]),
    }
}
#[derive(Debug)]
enum Error {
    Eval(EvalError),
    Child(usize),
}
impl From<EvalError> for Error {
    fn from(e: EvalError) -> Self {
        Self::Eval(e)
    }
}
fn clone_value(v: &V) -> V {
    match v {
        V::Bytes(v) => V::Bytes(v.clone()),
        V::Int(v) => V::Int(*v),
        V::Real(v) => V::Real(*v),
        V::Decimal(v) => V::Decimal(v.clone()),
        V::Time(v) => V::Time(*v),
        V::Duration(v) => V::Duration(*v),
    }
}
fn dates(kind: D) -> Vec<V> {
    match kind {
        D::String => std::iter::once(None)
            .chain(
                [
                    b"2020-01-31".as_slice(),
                    b"2020-01-31 12:34:56.123456",
                    b"0000-00-00",
                    b"2020-00-01",
                    b"2020-02-31",
                    b"bad",
                    b"2020-01-01\xff",
                ]
                .map(|v| Some(v.to_vec())),
            )
            .map(V::Bytes)
            .collect(),
        D::Int => [
            None,
            Some(20200131),
            Some(20200131123456),
            Some(0),
            Some(-1),
            Some(i128::MAX),
            Some(i128::from(i64::MAX)),
        ]
        .into_iter()
        .map(V::Int)
        .collect(),
        D::Real => [
            None,
            Some(20200131.123456),
            Some(20200131123456.0),
            Some(0.0),
            Some(-1.5),
            Some(f64::NAN),
            Some(f64::INFINITY),
        ]
        .into_iter()
        .map(V::Real)
        .collect(),
        D::Decimal => std::iter::once(V::Decimal(None))
            .chain(
                [
                    "20200131.123456",
                    "20200131123456.654321",
                    "0",
                    "-1",
                    "1e30",
                    "20200231",
                ]
                .map(|v| V::Decimal(Some(Decimal::parse_mysql(v).0))),
            )
            .collect(),
        D::Datetime => std::iter::once(V::Time(None))
            .chain(
                [
                    "2020-01-31",
                    "2020-01-31 12:34:56.123456",
                    "0000-00-00",
                    "2020-00-01",
                    "2020-02-31",
                    "9999-12-31",
                ]
                .map(|v| {
                    V::Time(Some(
                        tidb_datatype::parse_datetime(v, &chrono_tz::UTC, true, true)
                            .unwrap()
                            .time,
                    ))
                }),
            )
            .collect(),
        D::Duration => [
            None,
            Some(MySqlDuration::from_raw_parts(0, 0)),
            Some(MySqlDuration::from_raw_parts(3_600_000_000_000, 6)),
            Some(MySqlDuration::from_raw_parts(-120_000_000_000, 3)),
            Some(MySqlDuration::from_raw_parts(i64::MAX, 6)),
            Some(MySqlDuration::from_raw_parts(i64::MIN, 0)),
            Some(MySqlDuration::from_raw_parts(1, 77)),
        ]
        .into_iter()
        .map(V::Duration)
        .collect(),
    }
}
fn intervals(kind: I) -> Vec<V> {
    match kind {
        I::String => std::iter::once(None)
            .chain(
                [
                    b"1.5".as_slice(),
                    b"1e2",
                    b"1 02:03:04.005",
                    b"-1-02",
                    b"bad",
                    b"1:02",
                    b"\xff",
                ]
                .map(|v| Some(v.to_vec())),
            )
            .map(V::Bytes)
            .collect(),
        I::Int => [
            None,
            Some(0),
            Some(1),
            Some(-1),
            Some(100),
            Some(i128::MAX),
            Some(i128::MIN),
            Some(i128::from(i64::MAX)),
        ]
        .into_iter()
        .map(V::Int)
        .collect(),
        I::Real => [
            None,
            Some(0.0),
            Some(1.5),
            Some(-1.5),
            Some(1e30),
            Some(f64::NAN),
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
        ]
        .into_iter()
        .map(V::Real)
        .collect(),
        I::Decimal => std::iter::once(V::Decimal(None))
            .chain(
                [
                    "0",
                    "1.5",
                    "-1.5",
                    "1e2",
                    "1.23456789",
                    "1e30",
                    "9999999999999999999999",
                ]
                .map(|v| V::Decimal(Some(Decimal::parse_mysql(v).0))),
            )
            .collect(),
    }
}
fn invoke(
    ctx: &Context,
    meta: LegacyDateArithmeticMetadata,
    zone: &SessionTimeZone,
    date: &V,
    interval: &V,
    unit: Option<&[u8]>,
    fail: Option<usize>,
) -> Result<(Option<Datum>, i128), Error> {
    let read = |index, channel, selected: &dyn Columns| {
        ctx.events.borrow_mut().push(format!(
            "read:{index}:{channel:?}:{}",
            selected.context_id()
        ));
        selected.append_warning(9000 + index as u16, "source");
        if fail == Some(index) {
            return Err(Error::Child(index));
        }
        Ok(if index == 2 {
            V::Bytes(unit.map(<[u8]>::to_vec))
        } else if index == 0 {
            clone_value(date)
        } else {
            clone_value(interval)
        })
    };
    match meta.date {
        D::Datetime => eval_legacy_date_arithmetic_time_in(ctx, meta, zone, read)
            .map(|r| (r.value.map(Datum::Time), r.presence)),
        D::Duration => eval_legacy_date_arithmetic_duration_in(ctx, meta, zone, read)
            .map(|r| (r.value.map(Datum::new_duration), r.presence)),
        _ => eval_legacy_date_arithmetic_text_in(ctx, meta, zone, read)
            .map(|r| (r.value.map(Datum::Bytes), r.presence)),
    }
}
fn observe(value: &Result<(Option<Datum>, i128), Error>, ctx: &Context) -> String {
    // Read fields explicitly: an original callback E must not become EvalError.
    let outcome = match value {
        Ok((v, p)) => format!(
            "ok:{:?}:{p}",
            v.as_ref().map(|v| identity_value::encode(v).unwrap())
        ),
        Err(Error::Child(i)) => format!("child:{i}"),
        Err(Error::Eval(e)) => format!("eval:{e:?}"),
    };
    format!("{outcome}|{:?}\n", ctx.events.borrow())
}
fn original_legacy_date_arithmetic_observations(units: &[Option<&[u8]>]) -> (usize, u64) {
    let zones = [
        SessionTimeZone::utc(),
        SessionTimeZone::Named(chrono_tz::America::Los_Angeles),
        SessionTimeZone::Fixed {
            name: "+08".into(),
            offset_secs: 28800,
        },
    ];
    let mut count = 0;
    let mut hash = 0xcbf29ce484222325u64;
    for profile in 0..3 {
        let mut ctx = context(profile);
        for zone in &zones {
            for date_kind in [
                D::String,
                D::Int,
                D::Real,
                D::Decimal,
                D::Datetime,
                D::Duration,
            ] {
                for interval_kind in [I::String, I::Int, I::Real, I::Decimal] {
                    for subtract in [false, true] {
                        let meta = LegacyDateArithmeticMetadata {
                            date: date_kind,
                            interval: interval_kind,
                            subtract,
                        };
                        for date in dates(date_kind) {
                            for interval in intervals(interval_kind) {
                                for &unit in units {
                                    for fail in [None, Some(0), Some(1), Some(2)] {
                                        if profile == 2 {
                                            ctx = context(profile);
                                        }
                                        ctx.events.borrow_mut().clear();
                                        let result =
                                            invoke(&ctx, meta, zone, &date, &interval, unit, fail);
                                        for b in observe(&result, &ctx).bytes() {
                                            hash =
                                                (hash ^ u64::from(b)).wrapping_mul(0x100000001b3);
                                        }
                                        count += 1;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    (count, hash)
}
#[test]
fn original_legacy_date_arithmetic_channels_zones_errors_and_presence() {
    let units: [Option<&[u8]>; 12] = [
        None,
        Some(b"DAY"),
        Some(b"MONTH"),
        Some(b"SECOND"),
        Some(b"HOUR_MINUTE"),
        Some(b"YEAR_MONTH"),
        Some(b"DAY_MICROSECOND"),
        Some(b"MICROSECOND"),
        Some(b"WEEK"),
        Some(b"bad"),
        Some(b"day"),
        Some(b"\xff"),
    ];
    let (count, hash) = original_legacy_date_arithmetic_observations(&units);
    assert_eq!(count, 1188864);
    assert_eq!(hash, 0xf0099f79736b34c1);
}
#[test]
fn original_legacy_date_arithmetic_remaining_composite_and_clock_units() {
    let units: [Option<&[u8]>; 12] = [
        Some(b"MINUTE"),
        Some(b"HOUR"),
        Some(b"YEAR"),
        Some(b"QUARTER"),
        Some(b"DAY_HOUR"),
        Some(b"DAY_MINUTE"),
        Some(b"DAY_SECOND"),
        Some(b"HOUR_SECOND"),
        Some(b"MINUTE_SECOND"),
        Some(b"SECOND_MICROSECOND"),
        Some(b"HOUR_MICROSECOND"),
        Some(b"MINUTE_MICROSECOND"),
    ];
    let (count, hash) = original_legacy_date_arithmetic_observations(&units);
    eprintln!("ORIGINAL remaining legacy units count={count} hash={hash:016x}");
    assert_eq!(count, 1188864);
    assert_eq!(hash, 0x1f5bf8b3e75ad934);
}

#[test]
fn legacy_date_arithmetic_all_domains_never_lookup_ready_workers() {
    let mut ctx = context(0);
    ctx.reject = true;
    let mut panicked = vec![];
    for kind in [D::String, D::Datetime, D::Duration] {
        let meta = LegacyDateArithmeticMetadata {
            date: kind,
            interval: I::Int,
            subtract: false,
        };
        let date = dates(kind).remove(1);
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            invoke(
                &ctx,
                meta,
                &SessionTimeZone::utc(),
                &date,
                &V::Int(Some(1)),
                Some(b"DAY"),
                None,
            )
        }))
        .is_err()
        {
            panicked.push(kind);
        }
    }
    assert!(panicked.is_empty(), "retired Ready lookups in {panicked:?}");
}
