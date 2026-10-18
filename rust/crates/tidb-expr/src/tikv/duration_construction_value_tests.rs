// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Original duration/construction/format observations before retirement.
use crate::{Columns, Datum, EvalError};
use tidb_datatype::{CoreTime, MySqlDuration, Time, TimeType};
pub(crate) const NAMES: [&str; 13] = [
    "MICROSECOND",
    "TIME",
    "TIMEDIFF",
    "TIME_TO_SEC",
    "SEC_TO_TIME",
    "TIME_FORMAT",
    "MAKEDATE",
    "MAKETIME",
    "FROM_DAYS",
    "PERIOD_ADD",
    "PERIOD_DIFF",
    "GET_FORMAT",
    "DATEDIFF",
];
pub(crate) fn arity(name: &str) -> usize {
    match name {
        "MAKETIME" => 3,
        "TIMEDIFF" | "TIME_FORMAT" | "MAKEDATE" | "PERIOD_ADD" | "PERIOD_DIFF" | "GET_FORMAT"
        | "DATEDIFF" => 2,
        _ => 1,
    }
}
pub(crate) fn original(
    name: &str,
    values: &[Datum],
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    if name == "DATEDIFF" {
        crate::time_fn::calendar::date_diff_in(values, ctx)
    } else {
        crate::time_fn::dispatch(name, values, ctx).unwrap()
    }
}
pub(crate) fn values() -> Vec<Datum> {
    let mut v = crate::ops::arithmetic::tests::fixtures();
    v.extend(
        [
            "",
            "0",
            "1",
            "12:34:56.1234567",
            "-838:59:59.999999",
            "839:00:00",
            "24:00:00",
            "10:9:0",
            "00:00:00.+1",
            "2024-02-29 12:34:56.123456",
            "0000-00-00",
            "2020-00-00",
            "2020-02-31",
            "20240315",
            "20170118123050.1234567",
            " 3.5 ",
            "DATE",
            "TIME",
            "DATETIME",
            "USA",
            "EUR",
            "ISO",
            "JIS",
            "INTERNAL",
            "%H:%i:%s.%f",
            "%r %T %% %H %k",
            "junk",
        ]
        .into_iter()
        .map(Datum::new_string),
    );
    v.extend([
        Datum::Bytes(vec![0xff, 0]),
        Datum::Int(59),
        Datum::Int(60),
        Datum::Int(70),
        Datum::Int(100),
        Datum::Int(838),
        Datum::Int(3652424),
        Datum::Int(3652425),
        Datum::Int(3652499),
        Datum::Int(3652500),
        Datum::Decimal(tidb_datatype::Decimal::from_literal("30.0000005")),
    ]);
    for raw in [0, u64::MAX, 0x1fa0bb8b8b800000] {
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
    for raw in [i64::MIN, 0, i64::MAX] {
        for fsp in [0, 3, 6] {
            v.push(Datum::Duration(
                MySqlDuration::from_nanoseconds(raw, fsp).unwrap(),
            ));
        }
    }
    v
}
pub(crate) fn inputs(name: &str) -> Vec<Vec<Datum>> {
    let values = values();
    let n = arity(name);
    let mut cases = vec![vec![], vec![Datum::Null; n + 1]];
    if n == 1 {
        cases.extend(values.into_iter().map(|v| vec![v]));
    } else if n == 2 {
        for a in &values {
            for b in &values {
                cases.push(vec![a.clone(), b.clone()]);
            }
        }
    } else {
        for index in 0..3 {
            for value in &values {
                let mut args = vec![Datum::Int(12), Datum::Int(15), Datum::Real(30.0000005)];
                args[index] = value.clone();
                cases.push(args);
            }
        }
        let parts = [
            Datum::Null,
            Datum::Int(0),
            Datum::Int(-1),
            Datum::Int(59),
            Datum::Int(60),
            Datum::UInt(u64::MAX),
            Datum::Int(838),
            Datum::Real(59.9),
            Datum::Real(f64::NAN),
            Datum::new_string("bad"),
        ];
        for a in &parts {
            for b in &parts {
                for c in &parts {
                    cases.push(vec![a.clone(), b.clone(), c.clone()]);
                }
            }
        }
    }
    cases
}
pub(crate) fn observation(
    f: impl FnOnce() -> Result<Datum, EvalError>,
    ctx: &super::calendar_value_tests::Context,
) -> String {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
    let result = match result {
        Ok(result) => {
            let raw = match &result {
                Ok(Datum::Time(t)) => {
                    format!("/{:016x}/{:?}/{}", t.core_time().raw(), t.kind(), t.fsp())
                }
                _ => String::new(),
            };
            format!("{result:?}{raw}")
        }
        Err(p) => format!(
            "PANIC:{:?}",
            p.downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| p.downcast_ref::<&str>().copied())
        ),
    };
    format!("{result}/{:?}", ctx.events.borrow())
}
pub(crate) fn legacy(
    which: usize,
    left: Option<u64>,
    right: Option<u64>,
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    match which {
        0 => crate::time_fn::calendar::date_diff_core_in(
            left.map(CoreTime::from_raw),
            right.map(CoreTime::from_raw),
            ctx,
        ),
        1 => super::eval_legacy_microsecond_in(left.map(|v| v as i64), ctx),
        _ => super::eval_legacy_date_in(left, ctx),
    }
    .map(|v| v.map_or(Datum::Null, Datum::Int))
}
#[test]
fn duration_construction_isolated_original_sql_observations() {
    let mut count = 0usize;
    let mut hash = 0xcbf29ce484222325u64;
    let mut record = |s: String| {
        for b in s.bytes() {
            hash = (hash ^ u64::from(b)).wrapping_mul(0x100000001b3);
        }
        count += 1;
    };
    for profile in 0..3 {
        for seed in 0..8 {
            for level in 0..3 {
                let ctx = super::calendar_value_tests::context(profile, seed, level);
                for name in NAMES {
                    for args in inputs(name) {
                        ctx.reset();
                        record(observation(|| original(name, &args, &ctx), &ctx));
                    }
                }
                for which in 0..3 {
                    for left in [
                        None,
                        Some(0),
                        Some(u64::MAX),
                        Some(i64::MIN as u64),
                        Some(0x1fa0bb8b8b800000),
                    ] {
                        for right in [None, Some(0), Some(u64::MAX)] {
                            ctx.reset();
                            record(observation(|| legacy(which, left, right, &ctx), &ctx));
                        }
                    }
                }
                for kind in ["DATE", "TIME", "DATETIME", "timestamp", "bad"] {
                    for value in values() {
                        ctx.reset();
                        record(observation(
                            || crate::time_fn::get_format_ast_in(kind, &value, &ctx),
                            &ctx,
                        ));
                    }
                }
            }
        }
    }
    eprintln!("DURATION CONSTRUCTION ISOLATED SQL {count} {hash:016X}");
    assert_eq!((count, hash), (5839272, 0xE07682675B3A9EFD));
}
#[test]
fn duration_construction_never_requests_ready() {
    let mut failed = vec![];
    for name in NAMES {
        let mut ctx = super::calendar_value_tests::context(1, 0, 0);
        ctx.reject = true;
        let args = vec![Datum::new_string("12:34:56"); arity(name)];
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| original(name, &args, &ctx)))
            .is_err()
        {
            failed.push(name.to_owned());
        }
    }
    for which in 0..3 {
        let mut ctx = super::calendar_value_tests::context(1, 0, 0);
        ctx.reject = true;
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            legacy(which, Some(0), Some(0), &ctx)
        }))
        .is_err()
        {
            failed.push(format!("legacy{which}"));
        }
    }
    let mut ctx = super::calendar_value_tests::context(1, 0, 0);
    ctx.reject = true;
    if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        crate::time_fn::get_format_ast_in("DATE", &Datum::new_string("USA"), &ctx)
    }))
    .is_err()
    {
        failed.push("grammarGET_FORMAT".into());
    }
    assert!(failed.is_empty(), "Ready routes {failed:?}");
}
#[test]
fn duration_construction_finite_work_preserves_demand_display_panic_and_later_reuse() {
    use super::duration_construction_value as v;
    use tidb_query_expr::NativeDurationConstructionOperation as O;
    let mut ctx = super::calendar_value_tests::context(1, 0, 0);
    ctx.reject = true;
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| v::eval_bounded(
            O::FromDays,
            &[Datum::MinNotNull],
            &ctx,
            8192
        )))
        .is_err()
    );
    assert!(
        matches!(v::eval_bounded(O::FromDays,&[Datum::VectorFloat32(tidb_datatype::VectorFloat32::must_create(vec![1.0,-0.0]))],&ctx,8192),Ok(Datum::Time(t))if t.core_time().raw()==0)
    );
    let huge = Datum::Bytes(vec![0xff; 1 << 20]);
    for op in [O::TimeDiff, O::TimeFormat, O::GetFormat] {
        assert_eq!(
            v::eval_bounded(op, &[Datum::Null, huge.clone()], &ctx, 8192),
            Ok(Datum::Null)
        );
    }
    assert_eq!(
        v::get_format_source([&Datum::Null, &huge], 8192),
        Ok(Datum::Null)
    );
    assert_eq!(
        v::eval_bounded(O::GetFormat, &[Datum::Null, Datum::MinNotNull], &ctx, 8192),
        Ok(Datum::Null)
    );
    assert_eq!(
        v::get_format_source([&Datum::Null, &Datum::MinNotNull], 8192),
        Err(EvalError::Unsupported("range sentinel byte coercion"))
    );
    let overflow = Datum::Decimal(tidb_datatype::Decimal::from_literal(
        "9999999999999999999999999999999999999999999999999999999999999999",
    ));
    assert_eq!(
        v::eval_bounded(O::MakeTime, &[overflow, Datum::Null, huge], &ctx, 8192),
        Err(EvalError::IntOverflow)
    );
    let display = Datum::Decimal(tidb_datatype::Decimal::from_raw_parts(
        false,
        vec![b'3'],
        1000000,
        0,
    ));
    assert!(matches!(
        v::eval_bounded(
            O::MakeTime,
            &[Datum::Null, Datum::Int(60), display.clone()],
            &ctx,
            8192
        ),
        Err(EvalError::ExpressionRuntimeFailure(_))
    ));
    assert_eq!(
        v::eval_bounded(O::PeriodAdd, &[Datum::Int(202401), display], &ctx, 8192),
        Ok(Datum::Int(202404))
    );
    let raw = Datum::Time(Time::from_raw_parts(
        CoreTime::from_raw(0),
        TimeType::Date,
        255,
    ));
    assert_eq!(
        v::eval_bounded(
            O::MakeTime,
            &[Datum::Int(0), Datum::Int(60), raw.clone()],
            &ctx,
            4096
        ),
        Ok(Datum::Null)
    );
    assert!(matches!(
        v::eval_bounded(
            O::MakeTime,
            &[Datum::Int(0), Datum::Int(0), raw.clone()],
            &ctx,
            4096
        ),
        Err(EvalError::ExpressionRuntimeFailure(_))
    ));
    let value = v::eval_bounded(
        O::MakeTime,
        &[Datum::Int(0), Datum::Int(0), raw],
        &ctx,
        8192,
    )
    .unwrap();
    assert!(
        matches!(value,Datum::String(s)if s.bytes().len()==264 && s.bytes().starts_with(b"00:00:00."))
    );
    let err = v::eval_bounded(
        O::MakeTime,
        &[Datum::Null, Datum::Int(15), Datum::new_string("bad")],
        &ctx,
        8192,
    );
    assert_eq!(err, Ok(Datum::Null));
    assert!(ctx
        .events
        .borrow()
        .iter()
        .any(|v| v.contains("warning:1292:Truncated incorrect DOUBLE value")));
    assert!(matches!(
        v::eval_bounded(O::Time, &[Datum::Null], &ctx, usize::MAX),
        Err(EvalError::ExpressionRuntimeFailure(_))
    ));
}
#[test]
fn duration_construction_borrowed_format_lookup_keeps_large_identity_values_without_copy() {
    use super::duration_construction_value as v;
    let locale = Datum::Bytes(vec![0xff; 1 << 20]);
    let unknown = Datum::new_string("x".repeat(1 << 20));
    assert_eq!(
        v::get_format_source([&Datum::new_string("DATE"), &locale], 8192),
        Ok(Datum::new_string(""))
    );
    assert_eq!(
        v::get_format_source([&unknown, &locale], 8192),
        Ok(Datum::new_string(""))
    );
    let ctx = super::calendar_value_tests::context(0, 0, 0);
    assert_eq!(
        v::eval_bounded(
            tidb_query_expr::NativeDurationConstructionOperation::GetFormat,
            &[unknown.clone(), locale.clone()],
            &ctx,
            8192
        ),
        Ok(Datum::new_string(""))
    );
    assert_eq!(
        tidb_query_expr::native_get_format_value(Some(b"DATE"), Some(&vec![0xff; 1 << 20]), 8192),
        Ok(Some(String::new()))
    );
}
