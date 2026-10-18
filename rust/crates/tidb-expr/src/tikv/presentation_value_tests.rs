// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Original date/metric presentation observations before Ready retirement.
use crate::{Columns, Datum, EvalError};
pub(crate) const NAMES: [&str; 3] = ["DATE_FORMAT", "FORMAT_BYTES", "FORMAT_NANO_TIME"];
pub(crate) fn original(
    name: &str,
    values: &[Datum],
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    if name == "DATE_FORMAT" {
        crate::time_fn::calendar::date_format_in(&values[0], &values[1], ctx)
    } else {
        crate::builtin_ext::info::dispatch(name, values, ctx).expect("valid original metric arity")
    }
}
pub(crate) fn inputs(name: &str) -> Vec<Vec<Datum>> {
    let mut values = super::duration_construction_value_tests::values();
    if name == "DATE_FORMAT" {
        let layouts = [
            "%Y-%m-%d %H:%i:%s.%f",
            "%M %W %D %j",
            "%U %u %V %v %X %x",
            "%% %Q %",
            "",
            "%r %T",
            "%Y\0%f",
        ];
        let mut rows = vec![];
        for value in &values {
            for layout in layouts {
                rows.push(vec![value.clone(), Datum::new_string(layout)]);
            }
        }
        for first in [
            Datum::Null,
            Datum::new_string("2024-02-29 12:34:56.123456"),
            Datum::new_string("2020-00-00"),
            Datum::new_string("bad"),
        ] {
            for second in &values {
                rows.push(vec![first.clone(), second.clone()]);
            }
        }
        return rows;
    }
    for threshold in [
        0.0,
        1000.0,
        1024.0,
        1e6,
        (1u64 << 20) as f64,
        1e9,
        6e10,
        3.6e12,
        8.64e13,
        1e5,
        (1u64 << 60) as f64,
        1e300,
        f64::MAX,
    ] {
        for bits in [
            threshold.to_bits().wrapping_sub(1),
            threshold.to_bits(),
            threshold.to_bits().wrapping_add(1),
        ] {
            values.push(Datum::Real(f64::from_bits(bits)));
            values.push(Datum::Real(-f64::from_bits(bits)));
        }
    }
    for text in [
        "1e9999",
        "-1e9999",
        "12abc",
        " 12 ",
        "",
        "NaN",
        "Inf",
        " 1e-9999x",
    ] {
        values.push(Datum::new_string(text));
    }
    values.extend([
        Datum::Real(f64::INFINITY),
        Datum::Real(f64::NEG_INFINITY),
        Datum::Real(f64::from_bits(0x7ff8000000000001)),
    ]);
    values.into_iter().map(|v| vec![v]).collect()
}
#[test]
fn presentation_original_observations() {
    const CHILD: &str = "TIDB_PRESENTATION_ORIGINAL_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "tikv::presentation_value_tests::presentation_original_observations",
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
    let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
    for profile in 0..3 {
        for seed in 0..4 {
            for level in 0..3 {
                for name in NAMES {
                    for values in inputs(name) {
                        let ctx = super::calendar_value_tests::context(profile, seed, level);
                        let result = super::duration_construction_value_tests::observation(
                            || original(name, &values, &ctx),
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
    eprintln!("PRESENTATION ORIGINAL {count} {hash:016X}");
    assert_eq!((count, hash), (56016, 0x02F463312450CC05));
}
#[test]
fn presentation_never_requests_ready() {
    let mut failures = vec![];
    for name in NAMES {
        let mut ctx = super::calendar_value_tests::context(1, 0, 0);
        ctx.reject = true;
        let values = if name == "DATE_FORMAT" {
            vec![
                Datum::new_string("2024-02-29"),
                Datum::new_string("%Y-%m-%d"),
            ]
        } else {
            vec![Datum::Real(2048.0)]
        };
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            original(name, &values, &ctx)
        }))
        .is_err()
        {
            failures.push(name);
        }
    }
    assert!(failures.is_empty(), "Ready routes {failures:?}");
}

#[test]
fn presentation_metric_literal_budget_uses_only_demanded_prefix() {
    use super::duration_construction_value::eval_bounded;
    use tidb_query_expr::NativeDurationConstructionOperation as O;
    let mut ctx = super::calendar_value_tests::context(1, 0, 0);
    ctx.reject = true;
    for (op, expected) in [
        (O::FormatBytes, "16.00 EiB"),
        (O::FormatNanoTime, "2.14e+05 d"),
    ] {
        for value in [
            Datum::BinaryLiteral(tidb_datatype::BinaryLiteral::from(vec![1; 1 << 20])),
            Datum::Bit(tidb_datatype::BinaryLiteral::from(vec![1; 1 << 20])),
        ] {
            assert_eq!(
                eval_bounded(op, &[value], &ctx, 8192),
                Ok(Datum::new_string(expected))
            );
        }
    }
    let mut prefix = vec![0; 100];
    prefix.extend(vec![1; 1 << 20]);
    assert_eq!(
        eval_bounded(
            O::FormatBytes,
            &[Datum::BinaryLiteral(tidb_datatype::BinaryLiteral::from(
                prefix
            ))],
            &ctx,
            8192
        ),
        Ok(Datum::new_string("16.00 EiB"))
    );
    assert!(matches!(
        eval_bounded(
            O::FormatBytes,
            &[Datum::Bit(tidb_datatype::BinaryLiteral::from(vec![
                0;
                1 << 20
            ]))],
            &ctx,
            8192
        ),
        Err(EvalError::ExpressionRuntimeFailure(_))
    ));
    assert!(ctx.events.borrow().is_empty());
}
#[test]
fn presentation_values_keep_null_text_number_warnings_and_large_unsupported_errors() {
    use super::duration_construction_value::eval_bounded as eval;
    use tidb_query_expr::NativeDurationConstructionOperation as O;
    let mut ctx = super::calendar_value_tests::context(1, 0, 0);
    ctx.reject = true;
    for op in [O::FormatBytes, O::FormatNanoTime] {
        let value = Datum::VectorFloat32(tidb_datatype::VectorFloat32::must_create(vec![
            0.0;
            1 << 18
        ]));
        assert_eq!(
            eval(op, &[value], &ctx, 0),
            Err(EvalError::Unsupported("numeric argument conversion"))
        );
        assert_eq!(eval(op, &[Datum::Null], &ctx, 0), Ok(Datum::Null));
        let display = Datum::Decimal(tidb_datatype::Decimal::from_raw_parts(
            false,
            vec![b'3'],
            1000000,
            0,
        ));
        assert!(matches!(
            eval(op, &[display], &ctx, 8192),
            Err(EvalError::ExpressionRuntimeFailure(_))
        ));
    }
    let utf8 = crate::coerce::coerce_str(&Datum::Bytes(vec![255])).unwrap_err();
    assert_eq!(
        eval(
            O::DateFormat,
            &[Datum::Null, Datum::Bytes(vec![255])],
            &ctx,
            8192
        ),
        Err(utf8)
    );
    assert_eq!(
        eval(
            O::DateFormat,
            &[Datum::MinNotNull, Datum::Bytes(vec![b'x'; 1 << 20])],
            &ctx,
            8192
        ),
        Err(EvalError::Unsupported("range sentinel string coercion"))
    );
    assert_eq!(
        eval(O::FormatBytes, &[Datum::new_string("12abc")], &ctx, 8192),
        Ok(Datum::new_string("12 bytes"))
    );
    assert!(ctx
        .events
        .borrow()
        .iter()
        .any(|v| v.contains("warning:1292:")));
}
