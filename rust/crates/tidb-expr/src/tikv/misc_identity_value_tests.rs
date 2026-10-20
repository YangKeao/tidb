// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Original actual-representation witnesses; hash frames instead of formatting
//! raw Decimal precision, malformed JSON or nonfinite vectors.
use crate::{constant::Constant, expression::Expression, scalar_function::ScalarFunction};
use crate::{Datum, EvalError};
use tidb_datatype::{Collation, FieldType, FieldTypeCode};

pub(crate) fn extra_values() -> Vec<Datum> {
    let mut vector = tidb_datatype::VectorFloat32::init(3);
    vector.elements_mut()[0] = f32::from_bits(0x7fc01234);
    vector.elements_mut()[1] = -0.0;
    vector.elements_mut()[2] = f32::INFINITY;
    vec![
        Datum::MinNotNull,
        Datum::MaxValue,
        Datum::Float32(f64::from_bits(0x7ff8000012345678)),
        Datum::Real(f64::from_bits(0xfff8000012345678)),
        Datum::Decimal(
            tidb_datatype::Decimal::from_raw_parts(true, vec![0, 255, b'0'], u32::MAX, 3)
                .with_declared_shape(-1, i64::MAX),
        ),
        Datum::Duration(tidb_datatype::MySqlDuration::from_raw_parts(
            i64::MAX,
            i64::MIN,
        )),
        Datum::Time(tidb_datatype::Time::from_raw_parts(
            tidb_datatype::CoreTime::from_raw(u64::MAX),
            tidb_datatype::TimeType::DateTime,
            u8::MAX,
        )),
        Datum::Json(tidb_datatype::BinaryJSON::from_encoded_parts(
            255,
            vec![255, 0, 1],
        )),
        Datum::VectorFloat32(vector),
        Datum::Raw(vec![255, 0, 195]),
        Datum::new_collation_string(vec![255, 0, 195], Collation::Utf8Mb4ZhPinyinTiDbAsCs),
        Datum::Enum(
            tidb_datatype::MysqlEnum::new(vec![255, 0], u64::MAX),
            Collation::Utf8Mb4ZhPinyinTiDbAsCs,
        ),
        Datum::Set(
            tidb_datatype::MysqlSet::new(vec![255, 0], u64::MAX),
            Collation::Gb18030Bin,
        ),
    ]
}
pub(crate) fn field(value: &Datum) -> FieldType {
    FieldType::new(match value {
        Datum::Float32(_) => FieldTypeCode::Float,
        Datum::Real(_) => FieldTypeCode::Double,
        Datum::Decimal(_) => FieldTypeCode::NewDecimal,
        Datum::String(_) | Datum::Bytes(_) | Datum::Raw(_) => FieldTypeCode::VarString,
        Datum::Enum(..) => FieldTypeCode::Enum,
        Datum::Set(..) => FieldTypeCode::Set,
        Datum::BinaryLiteral(_) | Datum::Bit(_) => FieldTypeCode::Bit,
        Datum::Duration(_) => FieldTypeCode::Duration,
        Datum::Time(_) => FieldTypeCode::Datetime,
        Datum::Json(_) => FieldTypeCode::Json,
        Datum::VectorFloat32(_) => FieldTypeCode::VectorFloat32,
        _ => FieldTypeCode::LongLong,
    })
    .with_unsigned(matches!(value, Datum::UInt(_)))
}
pub(crate) fn observe(
    f: impl FnOnce() -> Result<Datum, EvalError>,
    ctx: &super::calendar_value_tests::Context,
) -> Vec<u8> {
    let mut out = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(Ok(value)) => match super::identity_value::encode(&value) {
            Ok(Some(frame)) => {
                let mut out = vec![0];
                out.extend_from_slice(&(frame.len() as u64).to_le_bytes());
                out.extend(frame);
                out
            }
            Ok(None) => vec![1],
            Err(e) => format!("ENCODE:{e:?}").into_bytes(),
        },
        Ok(Err(e)) => format!("ERROR:{e:?}").into_bytes(),
        Err(e) => format!(
            "PANIC:{:?}",
            e.downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| e.downcast_ref::<&str>().copied())
        )
        .into_bytes(),
    };
    out.extend_from_slice(format!("|{:?}", ctx.events.borrow()).as_bytes());
    out
}
fn fold(hash: &mut u64, bytes: &[u8]) {
    for &b in bytes {
        *hash = (*hash ^ u64::from(b)).wrapping_mul(0x100000001b3);
    }
}
fn isolated(which: &str) -> bool {
    const MARKER: &str = "TIDB_MISC_IDENTITY_ORIGINAL_UTC";
    if std::env::var(MARKER).as_deref() == Ok(which) {
        return false;
    }
    let test = format!("tikv::misc_identity_value_tests::{which}");
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", &test, "--nocapture"])
        .env("TZ", "UTC")
        .env(MARKER, which)
        .output()
        .unwrap();
    eprint!("{}", String::from_utf8_lossy(&output.stderr));
    eprintln!("{}", String::from_utf8_lossy(&output.stdout));
    assert!(output.status.success(), "isolated identity witness failed");
    true
}
#[test]
fn misc_identity_original_direct_observations() {
    if isolated("misc_identity_original_direct_observations") {
        return;
    }
    let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
    let names = vec![
        Datum::Null,
        Datum::Int(17),
        Datum::MinNotNull,
        Datum::MaxValue,
        Datum::new_string("label"),
        Datum::Bytes(vec![255]),
        Datum::VectorFloat32(tidb_datatype::VectorFloat32::must_create(vec![1.0])),
        Datum::Json(tidb_datatype::BinaryJSON::parse("{}").unwrap()),
    ];
    let mut values = super::duration_construction_value_tests::values();
    values.extend(extra_values());
    for profile in 0..3 {
        for value in &values {
            let ctx = super::calendar_value_tests::context(profile, 0, 0);
            fold(
                &mut hash,
                &observe(
                    || {
                        crate::builtin_ext::misc::dispatch_in(
                            "ANY_VALUE",
                            std::slice::from_ref(value),
                            &ctx,
                        )
                        .unwrap()
                    },
                    &ctx,
                ),
            );
            count += 1;
            for name in &names {
                let ctx = super::calendar_value_tests::context(profile, 0, 0);
                fold(
                    &mut hash,
                    &observe(
                        || {
                            crate::builtin_ext::misc::dispatch_in(
                                "NAME_CONST",
                                &[name.clone(), value.clone()],
                                &ctx,
                            )
                            .unwrap()
                        },
                        &ctx,
                    ),
                );
                count += 1;
            }
        }
    }
    eprintln!("MISC IDENTITY DIRECT ORIGINAL {count} {hash:016X}");
    assert_eq!(count, 3213);
    assert_eq!(hash, 0x60F023D7530726AF);
}
#[test]
fn misc_identity_original_scalar_observations() {
    if isolated("misc_identity_original_scalar_observations") {
        return;
    }
    // The original capture ran on this UTC date. Duration -> Date/Datetime
    // reads the datatype's real clock, not Columns.now. Replay that original
    // clock input; retain every original case, algorithm, count and checksum.
    let captured = chrono::DateTime::parse_from_rfc3339("2026-10-19T17:18:44Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    tidb_datatype::with_test_conversion_clock(captured, misc_identity_scalar_at_original_capture);
}
fn misc_identity_scalar_at_original_capture() {
    let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
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
    for profile in 0..3 {
        for level in 0..3 {
            for name in ["any_value", "name_const"] {
                for source in &declarations {
                    for ret in &declarations {
                        for value in super::duration_construction_value_tests::values() {
                            let mut args =
                                vec![Expression::Constant(Constant::new(value, source.clone()))];
                            if name == "name_const" {
                                args.insert(
                                    0,
                                    Expression::Constant(Constant::new(
                                        Datum::new_string("label"),
                                        FieldType::new(FieldTypeCode::VarString),
                                    )),
                                );
                            }
                            let f = ScalarFunction::new(
                                tidb_ast::CiString::new(name),
                                ret.clone(),
                                args,
                            );
                            let ctx = super::calendar_value_tests::context(profile, 3, level);
                            fold(&mut hash, &observe(|| f.eval(&ctx, input.get_row(0)), &ctx));
                            count += 1;
                        }
                    }
                }
                for value in extra_values() {
                    let ret = field(&value);
                    let mut args = vec![Expression::Constant(Constant::new(value, ret.clone()))];
                    if name == "name_const" {
                        args.insert(
                            0,
                            Expression::Constant(Constant::new(
                                Datum::Null,
                                FieldType::new(FieldTypeCode::VarString),
                            )),
                        );
                    }
                    let f = ScalarFunction::new(tidb_ast::CiString::new(name), ret, args);
                    let ctx = super::calendar_value_tests::context(profile, 3, level);
                    fold(&mut hash, &observe(|| f.eval(&ctx, input.get_row(0)), &ctx));
                    count += 1;
                }
            }
        }
    }
    eprintln!("MISC IDENTITY SCALAR ORIGINAL {count} {hash:016X}");
    assert_eq!(count, 231102);
    assert_eq!(hash, 0xE5E2EFBE8F3E3F49);
}
#[test]
fn misc_identity_never_requests_ready() {
    let mut ctx = super::calendar_value_tests::context(1, 0, 0);
    ctx.reject = true;
    let mut failures = vec![];
    for (name, args) in [
        ("ANY_VALUE", vec![Datum::Int(7)]),
        ("NAME_CONST", vec![Datum::Null, Datum::Int(7)]),
    ] {
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            crate::builtin_ext::misc::dispatch_in(name, &args, &ctx)
        }))
        .is_err()
        {
            failures.push(name)
        }
    }
    assert!(failures.is_empty(), "misc identity Ready {failures:?}");
}

#[test]
fn misc_identity_actual_copy_preserves_opaque_payloads_and_bounded_probe_order() {
    for value in extra_values() {
        let extent = super::identity_actual::copy_extent(&value).unwrap();
        let copy = super::identity_actual::copy_bounded(&value, extent).unwrap();
        assert_eq!(
            super::identity_value::encode(&value).unwrap(),
            super::identity_value::encode(&copy).unwrap()
        );
        if extent > 0 {
            assert!(matches!(
                super::identity_actual::copy_bounded(&value, extent - 1),
                Err(EvalError::ExpressionRuntimeFailure(_))
            ));
        }
    }
    let huge_scale = Datum::Decimal(tidb_datatype::Decimal::from_raw_parts(
        false,
        vec![b'1'],
        u32::MAX,
        1,
    ));
    assert!(super::identity_actual::copy_bounded(&huge_scale, 1).is_ok());
    assert!(matches!(
        super::identity_actual::probe(Some(&huge_scale), 4096),
        Err(EvalError::ExpressionRuntimeFailure(_))
    ));
    let invalid = extra_values().remove(4);
    assert!(
        std::panic::catch_unwind(|| super::identity_actual::probe(Some(&invalid), 4096)).is_err()
    );
    for v in [
        Datum::new_string(vec![255; 1 << 20]),
        Datum::Enum(
            tidb_datatype::MysqlEnum::new(vec![255; 1 << 20], 7),
            Collation::Utf8Mb4Bin,
        ),
        Datum::Set(
            tidb_datatype::MysqlSet::new(vec![255; 1 << 20], 7),
            Collation::Utf8Mb4Bin,
        ),
    ] {
        assert!(super::identity_actual::copy_bounded(&v, 0).is_ok());
    }
    assert!(super::identity_actual::probe(None, 0).is_ok());
    assert!(super::identity_actual::copy_bounded(&Datum::Null, usize::MAX).is_err());
    assert_eq!(
        ScalarFunction::new(
            tidb_ast::CiString::new("any_value"),
            FieldType::new(FieldTypeCode::LongLong),
            vec![Expression::Constant(Constant::new(
                Datum::Int(7),
                FieldType::new(FieldTypeCode::LongLong)
            ))]
        )
        .const_level(),
        crate::expression::ConstLevel::NONE
    );
}

#[test]
fn misc_identity_duration_returns_read_the_supplied_calendar_day() {
    if isolated("misc_identity_duration_returns_read_the_supplied_calendar_day") {
        return;
    }
    let before = chrono::DateTime::parse_from_rfc3339("2026-10-19T17:18:44Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    let after = before + chrono::Duration::days(1);
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
    let values = super::duration_construction_value_tests::values()
        .into_iter()
        .filter(|v| matches!(v, Datum::Duration(_)))
        .collect::<Vec<_>>();
    assert_eq!(values.len(), 10);
    let input = tidb_chunk::chunk::Chunk::new_with_capacity(&[], 1);
    let mut count = 0;
    for profile in 0..3 {
        for level in 0..3 {
            for name in ["any_value", "name_const"] {
                for source in &declarations {
                    for ret in [
                        FieldType::new(FieldTypeCode::Date),
                        FieldType::new(FieldTypeCode::Datetime).with_decimal(6),
                    ] {
                        for value in &values {
                            let mut args = vec![Expression::Constant(Constant::new(
                                value.clone(),
                                source.clone(),
                            ))];
                            if name == "name_const" {
                                args.insert(
                                    0,
                                    Expression::Constant(Constant::new(
                                        Datum::new_string("label"),
                                        FieldType::new(FieldTypeCode::VarString),
                                    )),
                                );
                            }
                            let f = ScalarFunction::new(
                                tidb_ast::CiString::new(name),
                                ret.clone(),
                                args,
                            );
                            let a = super::calendar_value_tests::context(profile, 3, level);
                            let b = super::calendar_value_tests::context(profile, 3, level);
                            let left = tidb_datatype::with_test_conversion_clock(before, || {
                                observe(|| f.eval(&a, input.get_row(0)), &a)
                            });
                            let right = tidb_datatype::with_test_conversion_clock(after, || {
                                observe(|| f.eval(&b, input.get_row(0)), &b)
                            });
                            assert_ne!(
                                left, right,
                                "original Duration calendar input {name} {source:?} {ret:?}"
                            );
                            count += 1;
                        }
                    }
                }
            }
        }
    }
    eprintln!("MISC IDENTITY ORIGINAL CALENDAR DAY INPUTS {count}");
    assert_eq!(count, 3960);
}
