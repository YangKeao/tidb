// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Original misc value/coercion/context witnesses before Ready retirement.
use crate::{constant::Constant, expression::Expression, scalar_function::ScalarFunction, Datum};
use tidb_datatype::{FieldType, FieldTypeCode};
pub(crate) const NAMES: [&str; 7] = [
    "IS_UUID",
    "UUID_VERSION",
    "UUID_TIMESTAMP",
    "UUID_TO_BIN",
    "BIN_TO_UUID",
    "TIDB_SHARD",
    "VITESS_HASH",
];
pub(crate) fn values() -> Vec<Datum> {
    let mut out = super::duration_construction_value_tests::values();
    out.extend(super::misc_identity_value_tests::extra_values());
    for text in [
        "00000000-0000-0000-0000-000000000000",
        "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
        "1ec9414c-232a-6b00-b3c8-9f6bdec09a0c",
        "01890f47-7e00-7000-8000-000000000000",
        "ffffffff-ffff-7fff-ffff-ffffffffffff",
        " 6ba7b810-9dad-11d1-80b4-00c04fd430c8 ",
        "urn:UUID:6ba7b810-9dad-11d1-80b4-00c04fd430c8",
        "X6ba7b810-9dad-11d1-80b4-00c04fd430c8Y",
        "6ba7b8109dad11d180b400c04fd430c8",
        "bad",
    ] {
        out.push(Datum::new_string(text));
    }
    out.extend([
        Datum::Bytes((0..16).collect()),
        Datum::Bytes(vec![255; 16]),
        Datum::Bytes(vec![255; 15]),
        Datum::Bytes(vec![0; 17]),
    ]);
    out
}
pub(crate) fn argument_sets(name: &str, value: &Datum) -> Vec<Vec<Datum>> {
    let mut out = vec![vec![value.clone()]];
    if matches!(name, "UUID_TO_BIN" | "BIN_TO_UUID") {
        for flag in [
            Datum::Null,
            Datum::Int(0),
            Datum::Int(1),
            Datum::Int(-1),
            Datum::UInt(u64::MAX),
            Datum::new_string("1bad"),
            Datum::new_string("bad"),
            Datum::Bytes(vec![255]),
            Datum::Real(f64::NAN),
            Datum::Decimal(tidb_datatype::Decimal::from_literal("1.5")),
            Datum::VectorFloat32(tidb_datatype::VectorFloat32::init(0)),
        ] {
            out.push(vec![value.clone(), flag]);
        }
    }
    out
}
fn fold(hash: &mut u64, bytes: &[u8]) {
    for &b in bytes {
        *hash = (*hash ^ u64::from(b)).wrapping_mul(0x100000001b3);
    }
}
fn isolated(which: &str) -> bool {
    const KEY: &str = "TIDB_MISC_ORIGINAL_UTC";
    if std::env::var(KEY).as_deref() == Ok(which) {
        return false;
    }
    let out = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            &format!("tikv::misc_value_tests::{which}"),
            "--nocapture",
        ])
        .env("TZ", "UTC")
        .env(KEY, which)
        .output()
        .unwrap();
    eprintln!("{}", String::from_utf8_lossy(&out.stderr));
    eprintln!("{}", String::from_utf8_lossy(&out.stdout));
    assert!(out.status.success());
    true
}
#[test]
fn misc_native_original_direct_observations() {
    if isolated("misc_native_original_direct_observations") {
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
                                || {
                                    crate::builtin_ext::misc::dispatch_in(name, &args, &ctx)
                                        .unwrap()
                                },
                                &ctx,
                            ),
                        );
                        count += 1;
                    }
                }
            }
        }
    }
    eprintln!("MISC NATIVE DIRECT ORIGINAL {count} {hash:016X}");
    assert_eq!(count, 34713);
    assert_eq!(hash, 0x2E9C5CB462D67999);
}
#[test]
fn misc_native_original_scalar_observations() {
    if isolated("misc_native_original_scalar_observations") {
        return;
    }
    let declarations = [
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
                for ret in &declarations {
                    for value in values() {
                        for args in argument_sets(name, &value) {
                            let args = args
                                .into_iter()
                                .map(|v| {
                                    let ty = super::misc_identity_value_tests::field(&v);
                                    Expression::Constant(Constant::new(v, ty))
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
    eprintln!("MISC NATIVE SCALAR ORIGINAL {count} {hash:016X}");
    assert_eq!(count, 381843);
    assert_eq!(hash, 0x1EE864887CAE1C77);
}
#[test]
fn misc_native_never_requests_ready() {
    let mut ctx = super::calendar_value_tests::context(1, 0, 0);
    ctx.reject = true;
    let mut failures = vec![];
    for name in NAMES {
        let v = if name == "BIN_TO_UUID" {
            Datum::Bytes(vec![0; 16])
        } else if matches!(name, "TIDB_SHARD" | "VITESS_HASH") {
            Datum::Int(7)
        } else {
            Datum::new_string("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
        };
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            crate::builtin_ext::misc::dispatch_in(name, &[v], &ctx)
        }))
        .is_err()
        {
            failures.push(name);
        }
    }
    assert!(failures.is_empty(), "misc native Ready {failures:?}");
}
