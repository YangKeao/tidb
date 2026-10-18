// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Original remaining time-arithmetic observations before Ready retirement.
use crate::{Columns, Datum, EvalError};
use tidb_query_expr::NativeTimeAddKind as K;
#[derive(Clone, Copy, Debug)]
pub(crate) enum Entry {
    Add { kinds: [K; 2], sign: i64, row: bool },
    Timestamp(usize),
    TimestampAdd,
}
pub(crate) fn original(
    entry: Entry,
    values: &[Datum],
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    use crate::time_fn::add_sub as t;
    match entry {
        Entry::Add { kinds, sign, row } => t::add_sub_time(values, kinds, sign, row, ctx),
        Entry::Timestamp(_) => t::timestamp(values, ctx),
        Entry::TimestampAdd => t::timestamp_add(values, ctx),
    }
}
pub(crate) fn fixtures() -> Vec<(Entry, Vec<Vec<Datum>>)> {
    let s = Datum::new_string;
    let generic = super::duration_construction_value_tests::values();
    let texts = [
        "10:00:00",
        "2020-01-01 10:00:00",
        "0000-01-01",
        "2020-02-31",
        "20240315.5",
        "-838:59:59.9",
        "999:00:00",
        "bad",
        "",
        "00:00:00.0000005",
        "2020-01-01",
        "-12:34:56.25",
    ];
    let mut pairs = vec![vec![], vec![Datum::Null; 3], vec![Datum::Null; 2]];
    for a in texts {
        for b in texts {
            pairs.push(vec![s(a), s(b)]);
        }
    }
    for index in 0..2 {
        for value in &generic {
            let mut row = vec![s("2020-01-01 10:00:00.123456"), s("00:00:01.5")];
            row[index] = value.clone();
            pairs.push(row);
        }
    }
    let mut out = vec![];
    for left in [K::Datetime, K::Date, K::Duration, K::Other] {
        for right in [K::Datetime, K::Date, K::Duration, K::Other] {
            for row in [false, true] {
                for sign in [-1, 1] {
                    out.push((
                        Entry::Add {
                            kinds: [left, right],
                            sign,
                            row,
                        },
                        pairs.clone(),
                    ));
                }
            }
        }
    }
    for sign in [i64::MIN, -7, 0, 9, i64::MAX] {
        out.push((
            Entry::Add {
                kinds: [K::Other, K::Other],
                sign,
                row: true,
            },
            pairs.clone(),
        ));
    }
    let mut single = vec![vec![], vec![Datum::Null; 3]];
    single.extend(generic.iter().cloned().map(|v| vec![v]));
    out.push((Entry::Timestamp(1), single));
    out.push((
        Entry::Timestamp(2),
        generic
            .iter()
            .flat_map(|a| generic.iter().map(move |b| vec![a.clone(), b.clone()]))
            .chain([vec![], vec![Datum::Null; 3]])
            .collect(),
    ));
    let mut adds = vec![vec![], vec![Datum::Null; 4], vec![Datum::Null; 3]];
    for unit in [
        "MICROSECOND",
        "second",
        "MINUTE",
        "HOUR",
        "DAY",
        "WEEK",
        "MONTH",
        "QUARTER",
        "YEAR",
        "DAY_SECOND",
        "",
        "bad",
    ] {
        for amount in [
            Datum::Null,
            Datum::Int(-1),
            Datum::Int(0),
            Datum::UInt(u64::MAX),
            Datum::Real(-0.0),
            Datum::Real(0.0000099999),
            Datum::Real(1.5),
            Datum::Real(f64::NAN),
            Datum::Real(f64::INFINITY),
            Datum::Real(-9e18),
            s("1.5x"),
        ] {
            for date in [
                "2020-01-31",
                "2020-02-29 23:59:59.999999",
                "0000-00-00",
                "10000-01-01",
                "2020-02-31",
                "bad",
                "",
            ] {
                adds.push(vec![s(unit), amount.clone(), s(date)]);
            }
        }
    }
    for index in 0..3 {
        for value in generic.iter().cloned() {
            let mut row = vec![s("SECOND"), Datum::Real(1.5), s("2020-01-01")];
            row[index] = value;
            adds.push(row);
        }
    }
    out.push((Entry::TimestampAdd, adds));
    out
}
#[test]
fn time_arithmetic_original_observations() {
    // A child catches original domain panics without modifying the hook of a
    // concurrent suite, and pins process Local before any temporal use.
    const CHILD: &str = "TIDB_TIME_ARITHMETIC_ORIGINAL_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "tikv::time_arithmetic_value_tests::time_arithmetic_original_observations",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .env("TZ", "UTC")
            .status()
            .unwrap();
        assert!(status.success());
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    // Independent pre-retirement capture/pair archives own this checksum.
    // After retirement this is a fixed original witness, not a new pair run.
    let fixtures = fixtures();
    let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
    for profile in 0..3 {
        for seed in 0..4 {
            for level in 0..3 {
                for (entry, cases) in &fixtures {
                    for values in cases {
                        let ctx = super::calendar_value_tests::context(profile, seed, level);
                        let result = super::duration_construction_value_tests::observation(
                            || original(*entry, values, &ctx),
                            &ctx,
                        );
                        for b in result.bytes() {
                            hash = (hash ^ u64::from(b)).wrapping_mul(0x100000001b3);
                        }
                        count += 1;
                    }
                }
            }
        }
    }
    eprintln!("TIME ARITHMETIC ORIGINAL {count} {hash:016X}");
    assert_eq!((count, hash), (1345032, 0xD12EB9991F65CDD5));
}
#[test]
fn time_arithmetic_never_requests_ready() {
    let mut failed = vec![];
    for entry in [
        Entry::Add {
            kinds: [K::Other, K::Other],
            sign: 1,
            row: true,
        },
        Entry::Add {
            kinds: [K::Other, K::Other],
            sign: -1,
            row: false,
        },
        Entry::Add {
            kinds: [K::Other, K::Datetime],
            sign: 1,
            row: false,
        },
        Entry::Timestamp(1),
        Entry::Timestamp(2),
        Entry::TimestampAdd,
    ] {
        let mut ctx = super::calendar_value_tests::context(1, 0, 0);
        ctx.reject = true;
        let args = match entry {
            Entry::Timestamp(1) => vec![Datum::new_string("2020-01-01")],
            Entry::TimestampAdd => vec![
                Datum::new_string("SECOND"),
                Datum::Int(1),
                Datum::new_string("2020-01-01"),
            ],
            _ => vec![
                Datum::new_string("2020-01-01"),
                Datum::new_string("00:00:01"),
            ],
        };
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            original(entry, &args, &ctx)
        }))
        .is_err()
        {
            failed.push(entry);
        }
    }
    assert!(failed.is_empty(), "Ready entries: {failed:?}");
}

#[test]
fn time_arithmetic_actual_values_preserve_demand_strict_warnings_and_work() {
    use super::duration_construction_value::eval_bounded as eval;
    use tidb_query_expr::NativeDurationConstructionOperation as O;
    let mut ctx = super::calendar_value_tests::context(1, 0, 2);
    ctx.reject = true;
    let huge = Datum::Bytes(vec![255; 1 << 20]);
    let s = Datum::new_string;
    assert_eq!(
        eval(
            O::AddTime {
                kinds: [K::Other, K::Datetime],
                row_path: true
            },
            &[huge.clone(), huge.clone()],
            &ctx,
            0
        ),
        Ok(Datum::Null)
    );
    assert_eq!(
        eval(O::Timestamp2, &[Datum::Null, huge.clone()], &ctx, 0),
        Ok(Datum::Null)
    );
    assert!(ctx.events.borrow().is_empty());
    assert_eq!(
        eval(O::Timestamp2, &[s("bad"), huge.clone()], &ctx, 8192),
        Ok(Datum::Null)
    );
    assert!(ctx
        .events
        .borrow()
        .iter()
        .any(|e| e.starts_with("warning:1292:")));
    assert!(!ctx.events.borrow().iter().any(|e| e.starts_with("level:")));
    ctx.reset();
    let utf8 = crate::coerce::coerce_str(&Datum::Bytes(vec![255])).unwrap_err();
    assert_eq!(
        eval(
            O::Timestamp2,
            &[s("0000-01-01"), Datum::Bytes(vec![255])],
            &ctx,
            8192
        ),
        Err(utf8.clone())
    );
    assert_eq!(
        eval(
            O::TimestampAdd,
            &[Datum::Null, Datum::Int(1), huge],
            &ctx,
            8192
        ),
        Ok(Datum::Null)
    );
    assert_eq!(
        eval(
            O::TimestampAdd,
            &[Datum::Null, Datum::Bytes(vec![255]), s("bad")],
            &ctx,
            8192
        ),
        Err(utf8)
    );
    for (amount, expected) in [
        (s("1.5x"), "2020-01-01 00:00:00"),
        (s("1.5"), "2020-01-01 00:00:01.500000"),
    ] {
        assert_eq!(
            eval(
                O::TimestampAdd,
                &[s("SECOND"), amount, s("2020-01-01")],
                &ctx,
                8192
            ),
            Ok(s(expected))
        );
    }
    assert_eq!(
        eval(
            O::TimestampAdd,
            &[s("unknown"), Datum::Int(1), s("bad")],
            &ctx,
            8192
        ),
        Ok(Datum::Null)
    );
    assert_eq!(
        eval(
            O::TimestampAdd,
            &[s("unknown"), Datum::Int(1), s("2020-01-01")],
            &ctx,
            8192
        ),
        Err(EvalError::Unsupported("TIMESTAMPADD unit"))
    );
    let display = Datum::Decimal(tidb_datatype::Decimal::from_raw_parts(
        false,
        vec![b'3'],
        1000000,
        0,
    ));
    assert!(matches!(
        eval(
            O::TimestampAdd,
            &[Datum::Null, display, s("bad")],
            &ctx,
            8192
        ),
        Err(EvalError::ExpressionRuntimeFailure(_))
    ));
}
#[test]
fn time_arithmetic_zero_base_does_not_charge_unread_rhs() {
    let zone = tidb_datatype::SessionTimeZone::utc();
    let tidb_query_expr::NativeTimestampHeadValue::NeedRhs(base) =
        tidb_query_expr::native_timestamp_head_value("0000-01-01", false, &zone, true, 8192)
            .unwrap()
    else {
        panic!("year zero must demand RHS coercion")
    };
    // The caller has already coerced this real text. The original arithmetic
    // rejects year zero before scanning or parsing that RHS at all.
    assert_eq!(base.finish(Some(&"x".repeat(1 << 20)), 0), Ok(None));
}
