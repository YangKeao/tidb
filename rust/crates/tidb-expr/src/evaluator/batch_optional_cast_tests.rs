// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Genuine optional CAST metadata, original values and demand phases.
use super::*;
use crate::tikv::dynamic_leaf_tests as leaf;
use std::sync::{Arc, RwLock};

const TARGETS: [&str; 12] = [
    "char", "binary", "signed", "unsigned", "decimal", "double", "float", "date", "datetime",
    "time", "year", "vector",
];
fn result_field(target: &str) -> FieldType {
    match target {
        "char" => string_ty().with_flen(8),
        "binary" => string_ty().with_flen(8).with_collation(Collation::Binary),
        "decimal" => FieldType::new(FieldTypeCode::NewDecimal)
            .with_flen(12)
            .with_decimal(2),
        "double" => real_ty(),
        "float" => FieldType::new(FieldTypeCode::Float),
        "date" => FieldType::new(FieldTypeCode::Date),
        "datetime" => FieldType::new(FieldTypeCode::Datetime).with_decimal(3),
        "time" => FieldType::new(FieldTypeCode::Duration).with_decimal(3),
        "year" => FieldType::new(FieldTypeCode::Year),
        "vector" => FieldType::new(FieldTypeCode::VectorFloat32),
        _ => ty(),
    }
}
fn argument(kind: &str, value: &Datum, field: &FieldType, missing: bool) -> Expression {
    let mut arg = match kind {
        "column" => typed_column(0, field),
        "literal" => literal(value.clone(), field.clone()),
        other => leaf::expression(
            other,
            0,
            value,
            Some(field),
            (other == "correlated").then(|| Arc::new(RwLock::new(value.clone()))),
        ),
    };
    if missing {
        match &mut arg {
            Expression::Column(c) => c.ret_type = None,
            Expression::Constant(c) => c.ret_type = None,
            Expression::CorrelatedColumn(c) => c.column.ret_type = None,
            _ => unreachable!(),
        }
    }
    arg
}
fn root(target: &str, kind: &str, value: &Datum, source: &FieldType, mask: usize) -> Expression {
    let mut e = call(
        &format!("cast_{target}"),
        result_field(target),
        vec![argument(kind, value, source, mask & 1 != 0)],
    );
    if mask & 2 != 0 {
        let Expression::ScalarFunction(f) = &mut e else {
            unreachable!()
        };
        f.ret_type = None;
    }
    e
}
fn records(original: bool) -> Vec<String> {
    let mut records = vec![];
    for target in TARGETS {
        for kind in [
            "column",
            "literal",
            "param",
            "param_effect_deferred",
            "deferred_value",
            "deferred_row",
            "correlated",
        ] {
            for mask in 0..4 {
                for (vi, value) in [
                    Datum::Null,
                    Datum::Int(-7),
                    Datum::UInt(u64::MAX),
                    Datum::Real(1.25),
                    Datum::new_string("12.5"),
                    Datum::new_string("2026-01-02 03:04:05.125"),
                    Datum::Bytes(vec![0xff]),
                ]
                .into_iter()
                .enumerate()
                {
                    let source = crate::tikv::misc_identity_value_tests::field(&value);
                    let input = chunk(&[source.clone()], &[vec![value.clone()], vec![Datum::Null]]);
                    let expression = root(target, kind, &value, &source, mask);
                    for profile in [0, 1, 3] {
                        for (ri, row) in [
                            tidb_chunk::row::Row::empty(),
                            input.physical_row(0),
                            input.physical_row(1),
                        ]
                        .into_iter()
                        .enumerate()
                        {
                            for direct in [false, true] {
                                let context = leaf::Context::new(profile, value.clone());
                                let invoke = || {
                                    if direct {
                                        let Expression::ScalarFunction(function) = &expression
                                        else {
                                            unreachable!()
                                        };
                                        function.eval(&context, row)
                                    } else {
                                        expression.eval(&context, row)
                                    }
                                    .map_err(|error| (Datum::Null, error))
                                };
                                let observation = leaf::observe(
                                    || {
                                        if original {
                                            crate::test_original_eval::with_original(invoke)
                                        } else {
                                            invoke()
                                        }
                                    },
                                    &context,
                                );
                                records.push(format!("{target}/{kind}/{mask}/{vi}/{profile}/{ri}/{direct}|{observation}"));
                            }
                        }
                    }
                }
            }
        }
    }
    records
}
fn digest(records: &[String]) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for record in records {
        for byte in record.len().to_le_bytes().into_iter().chain(record.bytes()) {
            hash = (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3);
        }
    }
    hash
}
#[test]
fn ordinary_optional_cast_original_values_and_contexts() {
    let original = records(true);
    eprintln!("N58 ORIGINAL {} {:016X}", original.len(), digest(&original));
    assert_eq!(
        (original.len(), digest(&original)),
        (42336, 0x71C4B89C782FC261)
    );
}
#[test]
fn ordinary_optional_cast_matches_original_without_type_inference() {
    let original = records(true);
    let actual = records(false);
    let mismatches = original
        .iter()
        .zip(&actual)
        .enumerate()
        .filter(|(_, (old, new))| old != new)
        .collect::<Vec<_>>();
    for (index, (old, new)) in mismatches.iter().take(12) {
        eprintln!("N58 difference {index}\nold={old}\nnew={new}");
    }
    assert_eq!(mismatches.len(), 0, "original/current CAST differences");
}

fn suite_records(shared: bool, require_prepared: bool) -> Vec<String> {
    let mut records = vec![];
    let mut refused = vec![];
    for target in TARGETS {
        for kind in [
            "column",
            "literal",
            "param",
            "param_effect_deferred",
            "deferred_row",
            "correlated",
        ] {
            for mask in 0..4 {
                for enabled in [false, true] {
                    for profile in [0, 1, 3] {
                        for selection in 0..4 {
                            for (vi, value) in
                                [Datum::Int(7), Datum::new_string("12.5"), Datum::Null]
                                    .into_iter()
                                    .enumerate()
                            {
                                let field = result_field(target);
                                let owner = suite(root(target, kind, &value, &ty(), mask));
                                let mut context = leaf::Context::new(profile, value.clone());
                                context.enabled.set(enabled);
                                context.sequential = true;
                                let mut input = chunk(
                                    &[ty()],
                                    &[
                                        vec![Datum::Int(7)],
                                        vec![Datum::Null],
                                        vec![Datum::Int(-3)],
                                        vec![Datum::Int(22)],
                                    ],
                                );
                                input.set_sel(match selection {
                                    0 => None,
                                    1 => Some(vec![3, 0, 3, 2, 1]),
                                    2 => Some(vec![]),
                                    _ => Some(vec![1, 1, 0]),
                                });
                                let mut output = output_for(&field);
                                let outcome =
                                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                        if shared {
                                            owner.run_with_tikv_batch(
                                                &context,
                                                &mut input,
                                                &mut output,
                                                &[ty()],
                                            )
                                        } else {
                                            crate::test_original_eval::with_original(|| {
                                                owner.run(&context, &mut input, &mut output)
                                            })
                                        }
                                    }));
                                let result = match outcome {
                                    Ok(v) => format!("{v:?}"),
                                    Err(p) => format!(
                                        "PANIC:{}",
                                        p.downcast_ref::<String>()
                                            .map(String::as_str)
                                            .or_else(|| p.downcast_ref::<&str>().copied())
                                            .unwrap_or("opaque")
                                    ),
                                };
                                let values = (0..output.num_rows())
                                    .map(|i| leaf::frame(&output.get_row(i).get_datum(0, &field)))
                                    .collect::<Vec<_>>();
                                let label = format!(
                                    "{target}/{kind}/{mask}/{enabled}/{profile}/{selection}/{vi}"
                                );
                                records.push(format!(
                                    "{label}|{result}|{values:?}|{}",
                                    context.snapshot()
                                ));
                                if shared {
                                    let r = owner.batch_route_snapshot();
                                    if r.prepared != 1 || r.fallback != 0 || r.refusals != 0 {
                                        refused.push(format!("{label}:{r:?}"));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    eprintln!(
        "N58 SUITE ROUTES {} refused of{}",
        refused.len(),
        records.len()
    );
    if require_prepared {
        assert!(
            refused.is_empty(),
            "{} refused {:?}",
            refused.len(),
            refused.iter().take(12).collect::<Vec<_>>()
        );
    }
    records
}
#[test]
fn ordinary_optional_cast_original_projection_phases() {
    let original = suite_records(false, false);
    eprintln!(
        "N58 SUITE ORIGINAL {} {:016X}",
        original.len(),
        digest(&original)
    );
    assert_eq!(
        (original.len(), digest(&original)),
        (20736, 0xB680282F5C43F00A)
    );
}
#[test]
fn ordinary_optional_cast_projection_matches_original_phases() {
    let original = suite_records(false, false);
    let actual = suite_records(true, false);
    let mismatches = original
        .iter()
        .zip(&actual)
        .enumerate()
        .filter(|(_, (old, new))| old != new)
        .collect::<Vec<_>>();
    for (index, (old, new)) in mismatches.iter().take(12) {
        eprintln!("N58 SUITE difference{index}\nold={old}\nnew={new}");
    }
    assert_eq!(
        mismatches.len(),
        0,
        "projection CAST original/current differences"
    );
}
#[test]
fn ordinary_optional_cast_projection_uses_prepared_driver() {
    suite_records(true, true);
}

#[test]
fn ordinary_optional_cast_reuses_real_owner_after_null_and_sql_error() {
    let owner = Arc::new(root("char", "column", &Datum::Int(7), &ty(), 2));
    let input = chunk(
        &[ty()],
        &[
            vec![Datum::Null],
            vec![Datum::Int(7)],
            vec![Datum::Null],
            vec![Datum::Int(-3)],
            vec![Datum::Null],
        ],
    );
    let mut borrowed = PreparedScalarExpression::compile(owner.as_ref(), 1).unwrap();
    let cache = crate::ReadyValueCache::new();
    let before = crate::test_original_eval::scalar_preparations();
    for row in 0..input.num_rows() {
        let actual_row = input.get_row(row);
        let original =
            crate::test_original_eval::with_original(|| owner.eval(&crate::NoColumns, actual_row));
        let borrowed_result = borrowed.eval(&crate::NoColumns, actual_row);
        let owned_result = cache.eval_owned_expression(&owner, &crate::NoColumns, actual_row);
        assert_eq!(
            format!("{original:?}"),
            format!("{borrowed_result:?}"),
            "borrowed row{row}"
        );
        assert_eq!(
            format!("{original:?}"),
            format!("{owned_result:?}"),
            "owned row{row}"
        );
        if row % 2 == 0 {
            assert_eq!(owned_result.unwrap(), Datum::Null);
        } else {
            assert_eq!(
                owned_result.unwrap_err(),
                EvalError::Unsupported("a cast with no result type")
            );
        }
    }
    assert_eq!(crate::test_original_eval::scalar_preparations() - before, 1);
    assert_eq!(Arc::strong_count(&owner), 2);
    let denied = super::super::scalar_source::BorrowedExpression::compile(
        owner.as_ref(),
        1,
        true,
        BatchSourceLimits {
            max_metadata_bytes: 1,
            ..limits()
        },
        ExecutionLimits::default(),
        65536,
    );
    assert!(matches!(denied,Err(error) if error.refusal()==BatchRefusal::Resource));
}
