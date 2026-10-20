// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Independent original deterministic crypto values and warning/demand phases.
use crate::{constant::Constant, expression::Expression, scalar_function::ScalarFunction, Datum};
use tidb_datatype::{FieldType, FieldTypeCode};
pub(crate) const NAMES: [&str; 3] = ["COMPRESS", "UNCOMPRESS", "UNCOMPRESSED_LENGTH"];
pub(crate) fn values() -> Vec<Datum> {
    let mut values = super::byte_unary_value_tests::values();
    static EXTRA: std::sync::OnceLock<Vec<Datum>> = std::sync::OnceLock::new();
    values.extend(
        EXTRA
            .get_or_init(|| {
                serde_json::from_str::<Vec<Vec<u8>>>(include_str!(
                    "testdata/compression_original_n7z.json"
                ))
                .unwrap()
                .into_iter()
                .map(Datum::Bytes)
                .collect()
            })
            .iter()
            .cloned(),
    );
    values
}
pub(crate) fn argument_sets(_: &str, value: &Datum) -> Vec<Vec<Datum>> {
    vec![vec![value.clone()]]
}
fn fold(hash: &mut u64, bytes: &[u8]) {
    for &b in bytes {
        *hash = (*hash ^ u64::from(b)).wrapping_mul(0x100000001b3);
    }
}
fn isolated(which: &str) -> bool {
    const KEY: &str = "TIDB_COMPRESSION_ORIGINAL_UTC";
    if std::env::var(KEY).as_deref() == Ok(which) {
        return false;
    }
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            &format!("tikv::compression_value_tests::{which}"),
            "--nocapture",
        ])
        .env("TZ", "UTC")
        .env(KEY, which)
        .output()
        .unwrap();
    eprintln!("{}", String::from_utf8_lossy(&output.stderr));
    eprintln!("{}", String::from_utf8_lossy(&output.stdout));
    assert!(output.status.success());
    true
}
#[test]
fn compression_actual_original_direct_observations() {
    if isolated("compression_actual_original_direct_observations") {
        return;
    }
    let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
    for profile in 0..3 {
        for level in 0..3 {
            for name in NAMES {
                for value in values() {
                    for args in argument_sets(name, &value) {
                        let ctx = super::calendar_value_tests::context(profile, 2, level);
                        fold(
                            &mut hash,
                            &super::misc_identity_value_tests::observe(
                                || crate::builtin_ext::crypto::dispatch(name, &args, &ctx).unwrap(),
                                &ctx,
                            ),
                        );
                        count += 1;
                    }
                }
            }
        }
    }
    eprintln!("COMPRESSION ACTUAL DIRECT ORIGINAL {count} {hash:016X}");
    assert_eq!(count, 6318);
    assert_eq!(hash, 0x51EC3B8C9ED75F51);
}
#[test]
fn compression_actual_original_scalar_observations() {
    if isolated("compression_actual_original_scalar_observations") {
        return;
    }
    let now = chrono::DateTime::parse_from_rfc3339("2026-10-20T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    tidb_datatype::with_test_conversion_clock(now, scalar_observations);
}
fn scalar_observations() {
    let types = [
        FieldType::new(FieldTypeCode::LongLong),
        FieldType::new(FieldTypeCode::LongLong).with_unsigned(true),
        FieldType::new(FieldTypeCode::Float),
        FieldType::new(FieldTypeCode::Double),
        FieldType::new(FieldTypeCode::NewDecimal)
            .with_flen(65)
            .with_decimal(6),
        FieldType::new(FieldTypeCode::VarString),
        FieldType::new(FieldTypeCode::Date),
        FieldType::new(FieldTypeCode::Datetime).with_decimal(6),
        FieldType::new(FieldTypeCode::Duration).with_decimal(6),
        FieldType::new(FieldTypeCode::Json),
        FieldType::new(FieldTypeCode::VectorFloat32),
    ];
    let input = tidb_chunk::chunk::Chunk::new_with_capacity(&[], 1);
    let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
    for profile in 0..3 {
        for level in 0..3 {
            for name in NAMES {
                for ret in &types {
                    for value in values() {
                        for args in argument_sets(name, &value) {
                            let args = args
                                .into_iter()
                                .map(|value| {
                                    let field = super::misc_identity_value_tests::field(&value);
                                    Expression::Constant(Constant::new(value, field))
                                })
                                .collect();
                            let f = ScalarFunction::new(
                                tidb_ast::CiString::new(name),
                                ret.clone(),
                                args,
                            );
                            let ctx = super::calendar_value_tests::context(profile, 2, level);
                            fold(
                                &mut hash,
                                &super::misc_identity_value_tests::observe(
                                    || f.eval(&ctx, input.get_row(0)),
                                    &ctx,
                                ),
                            );
                            count += 1;
                        }
                    }
                }
            }
        }
    }
    eprintln!("COMPRESSION ACTUAL SCALAR ORIGINAL {count} {hash:016X}");
    assert_eq!(count, 69498);
    assert_eq!(hash, 0x4EBAC72F3EA6BB39);
}
#[test]
fn compression_actual_never_requests_ready() {
    let mut ctx = super::calendar_value_tests::context(1, 0, 0);
    ctx.reject = true;
    let mut failures = vec![];
    for name in NAMES {
        let args = vec![Datum::new_string("value")];
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            crate::builtin_ext::crypto::dispatch(name, &args, &ctx)
        }))
        .is_err()
        {
            failures.push(name);
        }
    }
    assert!(failures.is_empty(), "crypto actual Ready {failures:?}");
}
