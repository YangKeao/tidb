// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Original eager scalar value-body observations, before metadata extraction.
use crate::{constant::Constant, expression::Expression, scalar_function::ScalarFunction, Datum};
use tidb_datatype::{FieldType, FieldTypeCode};

pub(crate) const NAMES: &[&str] = &[
    "UNHEX",
    "HEX",
    "BIN",
    "OCT",
    "SPACE",
    "REVERSE",
    "UPPER",
    "LOWER",
    "QUOTE",
    "BIT_LENGTH",
    "TO_BASE64",
    "FROM_BASE64",
    "LEFT",
    "RIGHT",
    "REPEAT",
    "REPLACE",
    "SUBSTRING",
    "SUBSTR",
    "SUBSTRING_INDEX",
    "LPAD",
    "RPAD",
    "INSERT",
    "TRANSLATE",
    "JSON_ARRAY",
    "JSON_OBJECT",
    "JSON_TYPE",
    "JSON_DEPTH",
    "JSON_VALID",
    "JSON_PRETTY",
    "JSON_STORAGE_SIZE",
    "JSON_STORAGE_FREE",
    "JSON_QUOTE",
    "JSON_UNQUOTE",
    "JSON_KEYS",
    "JSON_LENGTH",
    "JSON_EXTRACT",
    "JSON_REMOVE",
    "JSON_SET",
    "JSON_INSERT",
    "JSON_REPLACE",
    "JSON_ARRAY_APPEND",
    "JSON_ARRAY_INSERT",
    "JSON_CONTAINS",
    "JSON_CONTAINS_PATH",
    "JSON_SEARCH",
    "JSON_MERGE",
    "JSON_MERGE_PRESERVE",
    "JSON_MERGE_PATCH",
    "FORMAT",
    "CONV",
    "ELT",
    "MAKE_SET",
    "EXPORT_SET",
    "SOUNDEX",
];
pub(crate) fn arguments(name: &str, value: &Datum) -> Vec<Datum> {
    let text = |s: &str| Datum::new_string(s);
    match name {
        "LEFT" | "RIGHT" | "REPEAT" | "SUBSTRING" | "SUBSTR" | "FORMAT" => {
            vec![value.clone(), Datum::Int(2)]
        }
        "REPLACE" => vec![value.clone(), text("a"), text("Z")],
        "SUBSTRING_INDEX" => vec![value.clone(), text(","), Datum::Int(2)],
        "LPAD" | "RPAD" => vec![value.clone(), Datum::Int(5), text("x")],
        "INSERT" => vec![value.clone(), Datum::Int(2), Datum::Int(2), text("x")],
        "TRANSLATE" => vec![value.clone(), text("a"), text("b")],
        "JSON_OBJECT" => vec![text("key"), value.clone()],
        "JSON_EXTRACT" | "JSON_REMOVE" => vec![value.clone(), text("$[0]")],
        "JSON_SET" | "JSON_INSERT" | "JSON_REPLACE" | "JSON_ARRAY_APPEND" | "JSON_ARRAY_INSERT" => {
            vec![value.clone(), text("$[0]"), Datum::Int(2)]
        }
        "JSON_CONTAINS" | "JSON_MERGE" | "JSON_MERGE_PRESERVE" | "JSON_MERGE_PATCH" => {
            vec![value.clone(), text("[1]")]
        }
        "JSON_CONTAINS_PATH" => vec![value.clone(), text("one"), text("$")],
        "JSON_SEARCH" => vec![value.clone(), text("one"), text("a")],
        "CONV" => vec![value.clone(), Datum::Int(10), Datum::Int(16)],
        "ELT" | "MAKE_SET" => vec![value.clone(), text("a"), text("b")],
        "EXPORT_SET" => vec![value.clone(), text("Y"), text("N")],
        _ => vec![value.clone()],
    }
}
pub(crate) fn function(name: &str, value: &Datum, ret: &FieldType) -> ScalarFunction {
    let args = arguments(name, value)
        .into_iter()
        .map(|v| {
            let f = super::misc_identity_value_tests::field(&v);
            Expression::Constant(Constant::new(v, f))
        })
        .collect();
    ScalarFunction::new(tidb_ast::CiString::new(name), ret.clone(), args)
}
pub(crate) fn types() -> Vec<FieldType> {
    vec![
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
    ]
}
fn isolated(which: &str) -> bool {
    const KEY: &str = "TIDB_EAGER_SCALAR_ORIGINAL_UTC";
    if std::env::var(KEY).as_deref() == Ok(which) {
        return false;
    }
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            &format!("tikv::eager_scalar_value_tests::{which}"),
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
fn fold(hash: &mut u64, value: &[u8]) {
    for &b in value {
        *hash = (*hash ^ u64::from(b)).wrapping_mul(0x100000001b3);
    }
}
#[test]
fn eager_scalar_original_observations() {
    if isolated("eager_scalar_original_observations") {
        return;
    }
    let now = chrono::DateTime::parse_from_rfc3339("2026-10-20T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    tidb_datatype::with_test_conversion_clock(now, || {
        let input = tidb_chunk::chunk::Chunk::new_with_capacity(&[], 1);
        let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
        for profile in 0..3 {
            for level in 0..3 {
                for &name in NAMES {
                    for ret in types() {
                        for value in super::byte_unary_value_tests::values() {
                            let f = function(name, &value, &ret);
                            let ctx = super::calendar_value_tests::context(profile, 2, level);
                            let before = crate::scalar_function::eager_value_phases_for_test();
                            let value = super::misc_identity_value_tests::observe(
                                || f.eval(&ctx, input.get_row(0)),
                                &ctx,
                            );
                            fold(&mut hash, &value);
                            fold(
                                &mut hash,
                                &((crate::scalar_function::eager_value_phases_for_test() - before)
                                    as u64)
                                    .to_le_bytes(),
                            );
                            count += 1;
                        }
                    }
                }
            }
        }
        assert_eq!((count, hash), (796554, 0xB0601ED2964A5721));
        eprintln!("EAGER SCALAR ORIGINAL {count} {hash:016X}");
    });
}
#[test]
fn eager_scalar_registered_shapes_really_enter_original_eager_phase() {
    let input = tidb_chunk::chunk::Chunk::new_with_capacity(&[], 1);
    for &name in NAMES {
        let f = function(
            name,
            &Datum::new_string("hello"),
            &FieldType::new(FieldTypeCode::VarString),
        );
        let ctx = super::calendar_value_tests::context(1, 2, 0);
        let before = crate::scalar_function::eager_value_phases_for_test();
        let _ = super::misc_identity_value_tests::observe(|| f.eval(&ctx, input.get_row(0)), &ctx);
        assert_eq!(
            crate::scalar_function::eager_value_phases_for_test() - before,
            1,
            "{name}"
        );
    }
}
