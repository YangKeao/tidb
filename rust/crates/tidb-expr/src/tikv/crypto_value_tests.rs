// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Independent original deterministic crypto values and warning/demand phases.
use crate::{constant::Constant, expression::Expression, scalar_function::ScalarFunction, Datum};
use tidb_datatype::{FieldType, FieldTypeCode};
pub(crate) const NAMES: [&str; 4] = ["SM3", "PASSWORD", "ENCODE", "DECODE"];
pub(crate) fn values() -> Vec<Datum> {
    super::byte_unary_value_tests::values()
}
pub(crate) fn argument_sets(name: &str, value: &Datum) -> Vec<Vec<Datum>> {
    if !matches!(name, "ENCODE" | "DECODE") {
        return vec![vec![value.clone()]];
    }
    let passwords = vec![
        Datum::Null,
        Datum::new_string(""),
        Datum::new_string("pw"),
        Datum::Bytes(vec![255]),
        Datum::Int(0),
        Datum::Int(-1),
        Datum::UInt(u64::MAX),
        Datum::Real(f64::NAN),
        Datum::Decimal(tidb_datatype::Decimal::from_literal("1.5")),
        Datum::BinaryLiteral(tidb_datatype::BinaryLiteral::from(vec![255])),
        Datum::VectorFloat32(tidb_datatype::VectorFloat32::init(0)),
        super::misc_identity_value_tests::extra_values().remove(4),
    ];
    passwords
        .into_iter()
        .map(|password| vec![value.clone(), password])
        .collect()
}
fn fold(hash: &mut u64, bytes: &[u8]) {
    for &b in bytes {
        *hash = (*hash ^ u64::from(b)).wrapping_mul(0x100000001b3);
    }
}
fn isolated(which: &str) -> bool {
    const KEY: &str = "TIDB_CRYPTO_ORIGINAL_UTC";
    if std::env::var(KEY).as_deref() == Ok(which) {
        return false;
    }
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            &format!("tikv::crypto_value_tests::{which}"),
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
fn crypto_actual_original_direct_observations() {
    if isolated("crypto_actual_original_direct_observations") {
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
    eprintln!("CRYPTO ACTUAL DIRECT ORIGINAL {count} {hash:016X}");
    assert_eq!(count, 34866);
    assert_eq!(hash, 0x51014D84299C1F50);
}
#[test]
fn crypto_actual_original_scalar_observations() {
    if isolated("crypto_actual_original_scalar_observations") {
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
    eprintln!("CRYPTO ACTUAL SCALAR ORIGINAL {count} {hash:016X}");
    assert_eq!(count, 383526);
    assert_eq!(hash, 0x96AFB40E405F1DF7);
}
#[test]
fn crypto_actual_never_requests_ready() {
    let mut ctx = super::calendar_value_tests::context(1, 0, 0);
    ctx.reject = true;
    let mut failures = vec![];
    for name in NAMES {
        let mut args = vec![Datum::new_string("value")];
        if matches!(name, "ENCODE" | "DECODE") {
            args.push(Datum::new_string("password"));
        }
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
