// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Original fresh-cache CONVERT_TZ observations, isolated from process TZ.
use crate::{Columns, Datum, EvalError};
pub(crate) fn original(v: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    crate::time_fn::dispatch("CONVERT_TZ", v, ctx).unwrap()
}
pub(crate) fn inputs() -> Vec<Vec<Datum>> {
    let dates = [
        "2004-01-01 12:00:00.123456789",
        "20040101",
        "20000-01-01 00:00:00.010",
        "0000-00-00",
        "2020-02-31",
        "bad",
        "",
        "2021-01-01T00:00:00",
        "2007-11-04 01:30:00",
        "2025-10-26 02:30:00.5",
        "2025-03-30 02:30:00",
        "2007-03-11 02:30:00",
        "2021-10-03 02:30:00",
        "2011-12-30 12:00:00",
        "2021-11-07 01:30:00",
        "2021-03-14 02:30:00",
        "9999-12-31 23:59:59.9",
    ];
    let zones = [
        "+00:00",
        "-00:00",
        "+14:0",
        "-14:00",
        "+14:01",
        "+13:60",
        "+0:9",
        "UTC",
        "SYSTEM",
        "system",
        "US/Eastern",
        "Europe/Paris",
        "Australia/Sydney",
        "Pacific/Apia",
        "Asia/Shanghai",
        "",
        "bogus/zone",
        " +00:00",
        "+1:٢",
    ];
    let mut cases = vec![vec![], vec![Datum::Null; 4]];
    for date in dates {
        for from in zones {
            for to in zones {
                cases.push(vec![
                    Datum::new_string(date),
                    Datum::new_string(from),
                    Datum::new_string(to),
                ]);
            }
        }
    }
    for index in 0..3 {
        for value in super::duration_construction_value_tests::values() {
            let mut row = vec![
                Datum::new_string("2025-10-26 02:30:00.5"),
                Datum::new_string("Europe/Paris"),
                Datum::new_string("UTC"),
            ];
            row[index] = value;
            cases.push(row);
        }
    }
    cases
}
#[test]
fn convert_tz_original_observations() {
    const CHILD: &str = "TIDB_CONVERT_TZ_ORIGINAL_CHILD";
    let Ok(zone) = std::env::var(CHILD) else {
        for zone in ["UTC", "Asia/Shanghai", "America/New_York"] {
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "tikv::convert_tz_value_tests::convert_tz_original_observations",
                    "--nocapture",
                ])
                .env(CHILD, zone)
                .env("TZ", zone)
                .status()
                .unwrap();
            assert!(status.success(), "original child TZ={zone}");
        }
        return;
    };
    // Catch original offset/domain panics without logging thousands of hooks.
    std::panic::set_hook(Box::new(|_| {}));
    let mut count = 0usize;
    let mut hash = 0xcbf29ce484222325u64;
    for profile in 0..3 {
        for seed in 0..4 {
            for level in 0..3 {
                for values in inputs() {
                    // The independent original capture used a fresh cache per call.
                    // After retirement this checks its fixed original checksum;
                    // no obsolete cache-poison history is part of that witness.
                    let ctx = super::calendar_value_tests::context(profile, seed, level);
                    let result = super::duration_construction_value_tests::observation(
                        || original(&values, &ctx),
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
    eprintln!("CONVERT_TZ ORIGINAL TZ={zone} {count} {hash:016X}");
    let expected = match zone.as_str() {
        "UTC" => 0xBD5985171C035C25,
        "Asia/Shanghai" => 0x522632888DE0D4F5,
        "America/New_York" => 0xB1E585671A6BACCD,
        _ => panic!("unrecorded original child zone"),
    };
    assert_eq!((count, hash), (232452, expected));
}
#[test]
fn convert_tz_never_requests_ready() {
    let mut ctx = super::calendar_value_tests::context(1, 0, 0);
    ctx.reject = true;
    original(
        &[
            Datum::new_string("2004-01-01"),
            Datum::new_string("UTC"),
            Datum::new_string("UTC"),
        ],
        &ctx,
    )
    .unwrap();
}

#[test]
fn convert_tz_actual_values_keep_eager_text_order_and_finite_work() {
    use super::duration_construction_value::{eval_bounded, start};
    use tidb_query_expr::NativeDurationConstructionOperation as O;
    let mut ctx = super::calendar_value_tests::context(1, 0, 0);
    ctx.reject = true;
    let s = Datum::new_string;
    assert_eq!(
        eval_bounded(
            O::ConvertTz,
            &[Datum::Null, Datum::Null, Datum::Null],
            &ctx,
            8192
        ),
        Ok(Datum::Null)
    );
    let invalid = Datum::Bytes(vec![255]);
    let huge = Datum::Bytes(vec![b'x'; 1 << 20]);
    // NULL never suppresses a demanded text conversion; errors stop the tuple.
    let expected = crate::coerce::coerce_str(&invalid).unwrap_err();
    for row in [
        vec![Datum::Null, invalid.clone(), huge.clone()],
        vec![Datum::Null, s("UTC"), invalid.clone()],
    ] {
        assert_eq!(
            eval_bounded(O::ConvertTz, &row, &ctx, 8192),
            Err(expected.clone())
        );
    }
    let resource =
        eval_bounded(O::ConvertTz, &[Datum::Null, s("UTC"), huge], &ctx, 8192).unwrap_err();
    assert!(matches!(resource, EvalError::ExpressionRuntimeFailure(_)));
    // Original datetime parser runs before zone parsing. Both zone parsers run
    // before either Option is tested, even when the first zone is invalid.
    assert_eq!(
        eval_bounded(O::ConvertTz, &[s("bad"), s(""), s("+1:٢")], &ctx, 8192),
        Ok(Datum::Null)
    );
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| eval_bounded(
            O::ConvertTz,
            &[s("2004-01-01"), s(""), s("+1:٢")],
            &ctx,
            8192
        )))
        .is_err()
    );
    assert_eq!(
        eval_bounded(
            O::ConvertTz,
            &[s("2004-01-01"), s("UTC"), s("+0:9")],
            &ctx,
            8192
        ),
        Ok(s("2004-01-01 00:09:00"))
    );
    let first = start(O::ConvertTz, &Datum::Null, &ctx, 8192).unwrap();
    assert_eq!(first.argument(), Some((1, false)));
    let second = first.advance(&s("UTC"), &ctx, 8192).unwrap();
    assert_eq!(second.argument(), Some((2, false)));
    assert_eq!(
        second
            .advance(&Datum::Null, &ctx, 8192)
            .unwrap()
            .into_value(),
        Ok(Datum::Null)
    );
    assert!(ctx.events.borrow().is_empty());
}
