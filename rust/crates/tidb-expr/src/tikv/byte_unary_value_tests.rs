// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Independent original ASCII/LTRIM/RTRIM observations before migration.
use crate::{constant::Constant, expression::Expression, scalar_function::ScalarFunction, Datum};
use tidb_datatype::{FieldType, FieldTypeCode};
pub(crate) const NAMES: [&str; 3] = ["ASCII", "LTRIM", "RTRIM"];
pub(crate) fn values() -> Vec<Datum> {
    let mut out = super::misc_value_tests::values();
    for bytes in [
        b"".as_slice(),
        b" ",
        b"  ",
        b"  abc  ",
        b"  \xff  ",
        b"\t abc\n",
        "  中  ".as_bytes(),
        "\u{a0} x\u{2003}".as_bytes(),
    ] {
        out.push(Datum::Bytes(bytes.to_vec()));
        out.push(Datum::new_string(bytes));
    }
    out
}
pub(crate) fn direct(
    name: &str,
    value: Datum,
    ctx: &dyn crate::Columns,
) -> Result<Datum, crate::EvalError> {
    if name == "ASCII" {
        crate::string_fn::ascii(&[value], ctx)
    } else {
        crate::builtin_ext::string2::dispatch(name, &[value], ctx).unwrap()
    }
}
fn fold(hash: &mut u64, bytes: &[u8]) {
    for &b in bytes {
        *hash = (*hash ^ u64::from(b)).wrapping_mul(0x100000001b3);
    }
}
fn isolated(which: &str) -> bool {
    const KEY: &str = "TIDB_BYTE_UNARY_ORIGINAL_UTC";
    if std::env::var(KEY).as_deref() == Ok(which) {
        return false;
    }
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            &format!("tikv::byte_unary_value_tests::{which}"),
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
fn byte_unary_original_direct_observations() {
    if isolated("byte_unary_original_direct_observations") {
        return;
    }
    let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
    for profile in 0..3 {
        for level in 0..3 {
            for name in NAMES {
                for v in values() {
                    let ctx = super::calendar_value_tests::context(profile, 2, level);
                    fold(
                        &mut hash,
                        &super::misc_identity_value_tests::observe(|| direct(name, v, &ctx), &ctx),
                    );
                    count += 1;
                }
            }
        }
    }
    eprintln!("BYTE UNARY DIRECT ORIGINAL {count} {hash:016X}");
    assert_eq!(count, 4023);
    assert_eq!(hash, 0xC717C464D28B4353);
}
#[test]
fn byte_unary_original_scalar_observations() {
    if isolated("byte_unary_original_scalar_observations") {
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
                for source in &types {
                    for ret in &types {
                        for value in values() {
                            let f = ScalarFunction::new(
                                tidb_ast::CiString::new(name),
                                ret.clone(),
                                vec![Expression::Constant(Constant::new(value, source.clone()))],
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
    eprintln!("BYTE UNARY SCALAR ORIGINAL {count} {hash:016X}");
    assert_eq!(count, 486783);
    assert_eq!(hash, 0xB3CF56F72DC700B1);
}
#[test]
fn byte_unary_never_requests_ready() {
    let mut ctx = super::calendar_value_tests::context(1, 0, 0);
    ctx.reject = true;
    let mut failures = vec![];
    for name in NAMES {
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            direct(name, Datum::Bytes(b" x ".to_vec()), &ctx)
        }))
        .is_err()
        {
            failures.push(name);
        }
    }
    assert!(failures.is_empty(), "byte unary Ready {failures:?}");
}
