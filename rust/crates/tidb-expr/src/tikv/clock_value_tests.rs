// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Original clock callers, actual live samples and original getter ordering.
use super::*;
use crate::{Columns, Datum, EvalError};
use std::cell::{Cell, RefCell};
pub(crate) const NAMES: [&str; 12] = [
    "NOW",
    "CURRENT_TIMESTAMP",
    "LOCALTIME",
    "LOCALTIMESTAMP",
    "UTC_TIMESTAMP",
    "CURDATE",
    "CURRENT_DATE",
    "UTC_DATE",
    "CURTIME",
    "CURRENT_TIME",
    "UTC_TIME",
    "SYSDATE",
];
pub(crate) struct Context {
    cache: Option<ReadyValueCache>,
    pub(crate) reject: bool,
    pub(crate) enabled: Cell<bool>,
    seed: usize,
    pub(crate) alias: bool,
    level: usize,
    clock_reads: Cell<usize>,
    modes: Cell<usize>,
    zones: Cell<usize>,
    pub(crate) events: RefCell<Vec<String>>,
}
pub(crate) fn context(profile: usize, seed: usize, alias: bool, level: usize) -> Context {
    Context {
        cache: (profile != 0).then(ReadyValueCache::default),
        reject: false,
        enabled: Cell::new(false),
        seed,
        alias,
        level,
        clock_reads: Cell::new(0),
        modes: Cell::new(0),
        zones: Cell::new(0),
        events: RefCell::new(vec![]),
    }
}
fn clock(seed: usize) -> Option<(i64, u32, i32)> {
    match seed % 12 {
        0 => None,
        1 => Some((0, 0, 0)),
        2 => Some((0, u32::MAX, i32::MIN)),
        3 => Some((-1, 999999500, 1)),
        4 => Some((i64::MAX, 1, i32::MAX)),
        5 => Some((i64::MIN, 999999999, i32::MIN)),
        6 => Some((253402300800, 123456789, 86400)),
        7 => Some((-62167219200, 654321955, -86400)),
        8 => Some((1700000000, 654320955, 19800)),
        9 => Some((86399, 999999999, 0)),
        10 => Some((1, 1499999999, -28800)),
        _ => Some((1, u32::MAX, i32::MAX)),
    }
}
impl Context {
    pub(crate) fn reset(&self) {
        self.clock_reads.set(0);
        self.modes.set(0);
        self.zones.set(0);
        self.events.borrow_mut().clear();
    }
}
impl Columns for Context {
    fn get(&self, _: &[String]) -> Option<Datum> {
        panic!("no invented clock source")
    }
    fn ready_value_cache(&self) -> Option<&ReadyValueCache> {
        assert!(!self.reject, "clock attempted retired Ready lookup");
        self.cache.as_ref()
    }
    fn context_id(&self) -> u64 {
        8801
    }
    fn enable_vectorized_expression(&self) -> bool {
        self.enabled.get()
    }
    fn sysdate_is_now(&self) -> bool {
        self.events
            .borrow_mut()
            .push(format!("alias:{}", self.alias));
        self.alias
    }
    fn now(&self) -> Option<(i64, u32, i32)> {
        let n = self.clock_reads.get();
        self.clock_reads.set(n + 1);
        let v = if self.seed >= 12 {
            Some((1700000000 + n as i64, 654320955, 28800))
        } else {
            clock(self.seed + n)
        };
        self.events.borrow_mut().push(format!("now:{n}:{v:?}"));
        v
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
    fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
        let n = self.zones.get();
        self.zones.set(n + 1);
        self.events.borrow_mut().push(format!("zone:{n}"));
        match (self.seed + n) % 3 {
            0 => tidb_datatype::SessionTimeZone::utc(),
            1 => tidb_datatype::SessionTimeZone::Named(chrono_tz::America::Los_Angeles),
            _ => tidb_datatype::SessionTimeZone::Fixed {
                name: "raw+24".into(),
                offset_secs: 86400,
            },
        }
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
            .push(format!("warn:{code}:{message}"));
    }
}
pub(crate) fn original(
    name: &str,
    values: &[Datum],
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    if name == "SYSDATE" {
        crate::time_fn::add_sub::sysdate(values, ctx)
    } else {
        crate::time_fn::dispatch(name, values, ctx).unwrap()
    }
}
fn arguments() -> Vec<Vec<Datum>> {
    let mut vals = crate::ops::arithmetic::tests::fixtures();
    vals.extend((-2..=8).map(Datum::Int));
    vals.push(Datum::UInt(u64::MAX));
    vals.extend(
        ["0", "6", "6.5", "7", "bad", ""]
            .into_iter()
            .map(Datum::new_string),
    );
    let mut args = vec![vec![], vec![Datum::Null, Datum::Null]];
    args.extend(vals.into_iter().map(|v| vec![v]));
    args
}
fn observation(value: &Result<Datum, EvalError>, ctx: &Context) -> String {
    format!("{value:?}/{:?}", ctx.events.borrow())
}
#[test]
fn clock_original_observations() {
    let mut count = 0usize;
    let mut live = 0usize;
    let mut hash = 0xcbf29ce484222325u64;
    for profile in 0..3 {
        for seed in 0..12 {
            for alias in [false, true] {
                for level in 0..3 {
                    let ctx = context(profile, seed, alias, level);
                    for name in NAMES {
                        for vals in arguments() {
                            ctx.reset();
                            let value = original(name, &vals, &ctx);
                            if name == "SYSDATE" && !alias && value.is_ok() {
                                live += 1;
                                continue;
                            }
                            for b in observation(&value, &ctx).bytes() {
                                hash = (hash ^ u64::from(b)).wrapping_mul(0x100000001b3);
                            }
                            count += 1;
                        }
                    }
                }
            }
        }
    }
    eprintln!("CLOCK OLD {count}/{live} {hash:016X}");
    assert_eq!((count, live, hash), (132408, 2376, 0x0CD20F17C50A8D2B));
}
#[test]
fn clock_direct_never_requests_ready() {
    let mut failed = vec![];
    for name in NAMES {
        let mut ctx = context(1, 1, true, 0);
        ctx.reject = true;
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| original(name, &[], &ctx)))
            .is_err()
        {
            failed.push(name);
        }
    }
    assert!(failed.is_empty(), "Ready routes: {failed:?}");
}

pub(crate) fn operation(name: &str) -> tidb_query_expr::NativeClockOperation {
    use tidb_query_expr::NativeClockOperation as Op;
    match name {
        "NOW" | "CURRENT_TIMESTAMP" | "LOCALTIME" | "LOCALTIMESTAMP" => Op::Now,
        "UTC_TIMESTAMP" => Op::UtcTimestamp,
        "CURDATE" | "CURRENT_DATE" => Op::CurrentDate,
        "UTC_DATE" => Op::UtcDate,
        "CURTIME" => Op::Curtime,
        "CURRENT_TIME" => Op::CurrentTime,
        "UTC_TIME" => Op::UtcTime,
        "SYSDATE" => Op::Sysdate,
        _ => panic!(),
    }
}

#[test]
fn clock_precision_borrows_unsupported_payloads_and_limits_only_demanded_work() {
    use tidb_query_expr::NativeClockOperation as Op;
    let huge = Datum::Bytes(vec![b'1'; 1 << 20]);
    for op in [
        Op::Now,
        Op::UtcTimestamp,
        Op::Curtime,
        Op::CurrentTime,
        Op::UtcTime,
    ] {
        let ctx = context(0, 1, false, 0);
        assert_eq!(
            super::clock_value::eval_bounded(op, std::slice::from_ref(&huge), &ctx, 0),
            Err(EvalError::Unsupported(
                "bad fractional-seconds-precision argument"
            ))
        );
        assert!(ctx.events.borrow().is_empty());
    }
    let ctx = context(0, 1, true, 0);
    assert_eq!(
        super::clock_value::eval_bounded(Op::Sysdate, std::slice::from_ref(&huge), &ctx, 0),
        Err(EvalError::Unsupported(
            "bad fractional-seconds-precision argument"
        ))
    );
    assert_eq!(*ctx.events.borrow(), vec!["alias:true"]);
    let ctx = context(0, 1, false, 0);
    assert!(matches!(
        super::clock_value::eval_bounded(Op::Sysdate, std::slice::from_ref(&huge), &ctx, 16384),
        Err(EvalError::ExpressionRuntimeFailure(_))
    ));
    assert_eq!(*ctx.events.borrow(), vec!["alias:false"]);
    let ctx = context(0, 1, false, 0);
    let tiny = Datum::Decimal(crate::Decimal::from_raw_parts(
        false,
        vec![b'3'],
        1_000_000,
        0,
    ));
    assert!(super::clock_value::eval_bounded(Op::Sysdate, &[tiny], &ctx, 16384).is_ok());
    let ctx = context(0, 1, false, 0);
    assert_eq!(
        super::clock_value::eval_bounded(Op::UtcTime, &[Datum::Null], &ctx, 0),
        Ok(Datum::Null)
    );
    assert!(ctx.events.borrow().is_empty());
    assert!(matches!(
        super::clock_value::eval_bounded(Op::Now, &[], &ctx, 0),
        Err(EvalError::ExpressionRuntimeFailure(_))
    ));
    assert!(ctx.events.borrow().is_empty());
    assert_eq!(
        super::clock_value::eval_bounded(Op::UtcTime, &[Datum::Null], &ctx, usize::MAX),
        Ok(Datum::Null)
    );
}
