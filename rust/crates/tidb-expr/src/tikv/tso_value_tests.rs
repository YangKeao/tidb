// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Frozen pre-retirement TSO observations, including actual context demand.
use super::*;
use crate::{Columns, Datum, Decimal, EvalError};
use std::cell::{Cell, RefCell};
use tidb_datatype::SessionTimeZone;
pub(crate) struct Context {
    cache: Option<ReadyValueCache>,
    pub(crate) reject: bool,
    pub(crate) enabled: Cell<bool>,
    seed: usize,
    pub(crate) events: RefCell<Vec<String>>,
}
pub(crate) fn context(profile: usize, seed: usize) -> Context {
    Context {
        cache: (profile != 0).then(ReadyValueCache::default),
        reject: false,
        enabled: Cell::new(false),
        seed,
        events: RefCell::new(vec![]),
    }
}
impl Columns for Context {
    fn get(&self, _: &[String]) -> Option<Datum> {
        panic!("no invented source")
    }
    fn ready_value_cache(&self) -> Option<&ReadyValueCache> {
        assert!(!self.reject, "TSO attempted retired Ready lookup");
        self.cache.as_ref()
    }
    fn context_id(&self) -> u64 {
        8701
    }
    fn enable_vectorized_expression(&self) -> bool {
        self.enabled.get()
    }
    fn current_tso(&self) -> i64 {
        self.events.borrow_mut().push("current_tso".into());
        [0, -1, i64::MIN, i64::MAX, 424_930_234_047_906_595][self.seed % 5]
    }
    fn now(&self) -> Option<(i64, u32, i32)> {
        panic!("TSO must not use statement clock")
    }
    fn truncate_level(&self) -> crate::ErrorLevel {
        self.events.borrow_mut().push("truncate".into());
        match self.seed % 3 {
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
    fn time_zone(&self) -> SessionTimeZone {
        let n = self.events.borrow().len();
        let z = match (self.seed + n) % 9 {
            0 => SessionTimeZone::utc(),
            1 => SessionTimeZone::Named(chrono_tz::America::Los_Angeles),
            2 => SessionTimeZone::Named(chrono_tz::Europe::Paris),
            3 => SessionTimeZone::Local,
            i => SessionTimeZone::Fixed {
                name: format!("raw{i}"),
                offset_secs: [28800, 86400, -86400, i32::MAX, i32::MIN][i - 4],
            },
        };
        self.events.borrow_mut().push(format!("zone:{z:?}"));
        z
    }
}
pub(crate) fn original(name: &str, vals: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    crate::time_fn::dispatch(name, vals, ctx).unwrap()
}
fn observation(value: Result<Datum, EvalError>, ctx: &Context) -> String {
    let raw = match &value {
        Ok(Datum::Time(t)) => format!("/{:016x}/{:?}/{}", t.core_time().raw(), t.kind(), t.fsp()),
        _ => String::new(),
    };
    format!("{value:?}{raw}/{:?}", ctx.events.borrow())
}
fn values() -> Vec<Datum> {
    let mut v = crate::ops::arithmetic::tests::fixtures();
    v.extend(
        [
            i64::MIN,
            -1,
            0,
            1,
            (1 << 18) - 1,
            1 << 18,
            (1 << 18) + 1,
            424_930_234_047_906_595,
            i64::MAX,
        ]
        .into_iter()
        .map(Datum::Int),
    );
    v.extend(
        [
            "1.5",
            "-1.5",
            "424930234047906595",
            "999999999999999999999999999999999999999",
        ]
        .into_iter()
        .map(|s| Datum::Decimal(Decimal::from_literal(s))),
    );
    v.extend(
        [" 1e2 ", "bad", "9223372036854775808", "-0.5", ""]
            .into_iter()
            .map(Datum::new_string),
    );
    v.push(Datum::Bytes(vec![0xff]));
    v
}
#[test]
fn tso_original_observations() {
    // The immutable capture includes SessionTimeZone::Local under the original
    // home-gentoo Asia/Shanghai configuration. Pin it before process startup;
    // never mutate TZ inside the concurrently running parent test process.
    const CHILD: &str = "TIDB_TSO_FROZEN_SHANGHAI_SUBPROCESS";
    if std::env::var_os(CHILD).as_deref() != Some(std::ffi::OsStr::new("1")) {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "tikv::tso_value_tests::tso_original_observations",
                "--nocapture",
            ])
            .env("TZ", "Asia/Shanghai")
            .env(CHILD, "1")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    let mut count = 0usize;
    let mut hash = 0xcbf29ce484222325u64;
    for profile in 0..3 {
        for seed in 0..18 {
            let ctx = context(profile, seed);
            for name in [
                "TIDB_PARSE_TSO",
                "TIDB_PARSE_TSO_LOGICAL",
                "TIDB_CURRENT_TSO",
            ] {
                let mut args = vec![vec![], vec![Datum::Null, Datum::Null]];
                args.extend(values().into_iter().map(|v| vec![v]));
                for vals in args {
                    ctx.events.borrow_mut().clear();
                    let observed = observation(original(name, &vals, &ctx), &ctx);
                    for b in observed.bytes() {
                        hash = (hash ^ u64::from(b)).wrapping_mul(0x100000001b3);
                    }
                    count += 1;
                }
            }
        }
    }
    eprintln!("TSO OLD {count} {hash:016X}");
    assert_eq!((count, hash), (8586, 0x05FEB3EE0578F58D));
}
#[test]
fn tso_direct_never_requests_ready() {
    for name in ["TIDB_PARSE_TSO", "TIDB_PARSE_TSO_LOGICAL"] {
        let mut ctx = context(1, 0);
        ctx.reject = true;
        assert!(original(name, &[Datum::Int(424_930_234_047_906_595)], &ctx).is_ok());
    }
}

#[test]
fn tso_finite_work_and_arity_precede_context_demand() {
    use tidb_query_expr::NativeTsoOperation as Op;
    for op in [Op::Current, Op::Physical, Op::Logical] {
        let ctx = context(0, 0);
        let vals = vec![Datum::Int(1); op.arity()];
        for budget in [0, usize::MAX] {
            assert!(matches!(
                super::tso_value::eval_bounded(op, &vals, &ctx, budget),
                Err(EvalError::ExpressionRuntimeFailure(_))
            ));
        }
        let wrong = vec![Datum::Null; op.arity() + 1];
        assert_eq!(
            super::tso_value::eval_bounded(op, &wrong, &ctx, 0),
            Err(EvalError::Unsupported("bad function arity"))
        );
        assert!(ctx.events.borrow().is_empty());
    }
}

#[test]
fn tso_unused_decimal_display_precision_does_not_consume_work() {
    use tidb_query_expr::NativeTsoOperation as Op;
    let huge = Datum::Decimal(Decimal::from_raw_parts(false, vec![b'1'], 1_000_000, 0));
    // Original integer cast and direct signature both read storage scale; the
    // visible million-digit shape is not formatted for this signed input.
    let ctx = context(0, 0);
    assert_eq!(
        crate::cast::cast_arg_as_int(&huge, None, &ctx).unwrap(),
        Datum::Int(1)
    );
    ctx.events.borrow_mut().clear();
    assert_eq!(
        super::tso_value::eval_bounded(Op::Logical, std::slice::from_ref(&huge), &ctx, 16384),
        Ok(Datum::Int(1))
    );
    assert_eq!(
        super::tso_value::eval_source_bounded(Op::Logical, &[huge], None, &ctx, 16384),
        Ok(Datum::Int(1))
    );
}

#[test]
fn tso_only_demanded_decimal_diagnostics_charge_visible_precision() {
    use tidb_query_expr::NativeTsoOperation as Op;
    let unsigned =
        tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong).with_unsigned(true);
    for (negative, digits, source, reject, expected) in [
        (false, "1", Some(&unsigned), false, Datum::Int(1)),
        (true, "0", Some(&unsigned), false, Datum::Null),
        (true, "1", Some(&unsigned), true, Datum::Null),
        (
            false,
            "999999999999999999999999999",
            None,
            true,
            Datum::Null,
        ),
    ] {
        let value = Datum::Decimal(Decimal::from_raw_parts(
            negative,
            digits.as_bytes().to_vec(),
            1_000_000,
            0,
        ));
        let ctx = context(0, 0);
        let result = super::tso_value::eval_source_bounded(
            Op::Logical,
            std::slice::from_ref(&value),
            source,
            &ctx,
            16384,
        );
        if reject {
            assert!(matches!(
                result,
                Err(EvalError::ExpressionRuntimeFailure(_))
            ));
            assert!(ctx.events.borrow().is_empty());
        } else {
            assert_eq!(result, Ok(expected));
        }
    }
}
