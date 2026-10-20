// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Independent original deterministic crypto values and warning/demand phases.
use crate::{constant::Constant, expression::Expression, scalar_function::ScalarFunction, Datum};
use tidb_datatype::{FieldType, FieldTypeCode};
pub(crate) const NAMES: [&str; 4] = ["MD5", "SHA", "SHA1", "SHA2"];
pub(crate) fn values() -> Vec<Datum> {
    super::byte_unary_value_tests::values()
}
pub(crate) fn argument_sets(name: &str, value: &Datum) -> Vec<Vec<Datum>> {
    if name != "SHA2" {
        return vec![vec![value.clone()]];
    }
    let passwords = vec![
        Datum::Null,
        Datum::Int(0),
        Datum::Int(224),
        Datum::Int(256),
        Datum::Int(384),
        Datum::Int(512),
        Datum::Int(-1),
        Datum::Int(128),
        Datum::Int(i64::MIN),
        Datum::Int(i64::MAX),
        Datum::UInt(u64::MAX),
        Datum::Real(255.5),
        Datum::Real(256.5),
        Datum::Real(f64::NAN),
        Datum::Real(f64::INFINITY),
        Datum::Decimal(tidb_datatype::Decimal::from_literal("255.5")),
        Datum::Decimal(tidb_datatype::Decimal::from_literal("256.5")),
        Datum::Decimal(tidb_datatype::Decimal::from_raw_parts(
            false,
            vec![b'1'],
            u32::MAX,
            0,
        )),
        Datum::new_string("abc"),
        Datum::new_string("224suffix"),
        Datum::new_string(" 256suffix"),
        Datum::Bytes(vec![255]),
        Datum::Float32(256.5),
        Datum::BinaryLiteral(tidb_datatype::BinaryLiteral::from(vec![1])),
        Datum::Raw(vec![255]),
        Datum::VectorFloat32(tidb_datatype::VectorFloat32::init(0)),
        Datum::Enum(
            tidb_datatype::MysqlEnum::new("256", 256),
            tidb_datatype::Collation::Utf8Mb4Bin,
        ),
        Datum::Set(
            tidb_datatype::MysqlSet::new("256", 256),
            tidb_datatype::Collation::Utf8Mb4Bin,
        ),
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
    const KEY: &str = "TIDB_DIGEST_ORIGINAL_UTC";
    if std::env::var(KEY).as_deref() == Ok(which) {
        return false;
    }
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            &format!("tikv::digest_value_tests::{which}"),
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
fn digest_actual_original_direct_observations() {
    if isolated("digest_actual_original_direct_observations") {
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
    eprintln!("DIGEST ACTUAL DIRECT ORIGINAL {count} {hash:016X}");
    assert_eq!(count, 42912);
    assert_eq!(hash, 0xC0AA6D5E6AB8F921);
}
#[test]
fn digest_actual_original_scalar_observations() {
    if isolated("digest_actual_original_scalar_observations") {
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
    eprintln!("DIGEST ACTUAL SCALAR ORIGINAL {count} {hash:016X}");
    assert_eq!(count, 472032);
    assert_eq!(hash, 0x6B9F95BC40BDD875);
}
#[test]
fn digest_actual_never_requests_ready() {
    let mut ctx = super::calendar_value_tests::context(1, 0, 0);
    ctx.reject = true;
    let mut failures = vec![];
    for name in NAMES {
        let mut args = vec![Datum::new_string("value")];
        if name == "SHA2" {
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
