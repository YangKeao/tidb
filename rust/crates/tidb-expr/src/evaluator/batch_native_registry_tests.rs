// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Actual public native candidates across the complete function-name inventory.
use super::*;

struct FixedContext(leaf::Context);
impl Columns for FixedContext {
    fn get(&self, path: &[String]) -> Option<Datum> {
        self.0.get(path)
    }
    fn param_value(&self, order: usize) -> std::result::Result<Datum, EvalError> {
        self.0.param_value(order)
    }
    fn enable_vectorized_expression(&self) -> bool {
        self.0.enable_vectorized_expression()
    }
    fn time_zone(&self) -> SessionTimeZone {
        self.0.time_zone()
    }
    fn type_flags(&self) -> tidb_datatype::ConversionFlags {
        self.0.type_flags()
    }
    fn append_warning(&self, code: u16, message: &str) {
        self.0.append_warning(code, message)
    }
    fn get_uservar(&self, name: &str) -> Option<Datum> {
        self.0.get_uservar(name)
    }
    fn set_uservar(&self, name: &str, value: Datum) {
        self.0.set_uservar(name, value)
    }
    fn now(&self) -> Option<(i64, u32, i32)> {
        self.0.events.borrow_mut().push("now".into());
        Some((1_700_000_000, 123_000_000, 0))
    }
    fn sysdate_is_now(&self) -> bool {
        self.0.events.borrow_mut().push("sysdate_mode".into());
        true
    }
}
fn capture(
    expression: &Expression,
    context: &FixedContext,
    input: &Chunk,
    evaluate: &mut impl FnMut(
        &Expression,
        &FixedContext,
        &Chunk,
    ) -> std::result::Result<Option<Vec<Datum>>, EvalError>,
) -> String {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        evaluate(expression, context, input)
    }));
    let value = match result {
        Ok(Ok(None)) => "NONE".into(),
        Ok(Ok(Some(values))) => format!(
            "VALUES:{:?}",
            values.iter().map(leaf::frame).collect::<Vec<_>>()
        ),
        Ok(Err(error)) => format!("ERROR:{error:?}"),
        Err(payload) => format!(
            "PANIC:{}",
            payload
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| payload.downcast_ref::<&str>().map(|v| (*v).to_owned()))
                .unwrap_or_else(|| "opaque original panic".into())
        ),
    };
    format!("{value}|{}", context.0.snapshot())
}
fn registry_records(
    evaluate: impl FnMut(
        &Expression,
        &FixedContext,
        &Chunk,
    ) -> std::result::Result<Option<Vec<Datum>>, EvalError>,
) -> Vec<String> {
    registry_records_with_fields(
        &[None, Some(ty()), Some(real_ty()), Some(string_ty())],
        evaluate,
    )
}
fn remaining_fields() -> Vec<Option<FieldType>> {
    vec![
        Some(
            FieldType::new(FieldTypeCode::NewDecimal)
                .with_flen(20)
                .with_decimal(2),
        ),
        Some(FieldType::new(FieldTypeCode::Date).with_decimal(3)),
        Some(FieldType::new(FieldTypeCode::Datetime).with_decimal(3)),
        Some(FieldType::new(FieldTypeCode::Timestamp).with_decimal(3)),
        Some(FieldType::new(FieldTypeCode::Duration).with_decimal(3)),
        Some(FieldType::new(FieldTypeCode::Json)),
        Some(FieldType::new(FieldTypeCode::VectorFloat32)),
    ]
}
fn registry_records_with_fields(
    fields: &[Option<FieldType>],
    mut evaluate: impl FnMut(
        &Expression,
        &FixedContext,
        &Chunk,
    ) -> std::result::Result<Option<Vec<Datum>>, EvalError>,
) -> Vec<String> {
    let clock = chrono::DateTime::parse_from_rfc3339("2026-10-27T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    tidb_datatype::with_test_conversion_clock(clock, || {
        let mut records = Vec::new();
        for name in crate::scalar_function::eager_policy_tests::names() {
            // The actual native API declines all six entropy families. A
            // separate admission/no-effects test preserves that contract.
            if matches!(
                name.as_str(),
                "rand" | "random_bytes" | "uuid" | "uuid_v4" | "uuid_v7" | "uuid_short"
            ) {
                continue;
            }
            for arity in crate::scalar_function::eager_policy_tests::arities(&name) {
                for uppercase in [false, true] {
                    for (result, result_field) in fields.iter().enumerate() {
                        for parameter in [false, true] {
                            let mut function = ScalarFunction::new(
                                CiString::new(if uppercase {
                                    name.to_ascii_uppercase()
                                } else {
                                    name.clone()
                                }),
                                result_field
                                    .as_ref()
                                    .map(snapshot_field_type)
                                    .unwrap_or_else(ty),
                                (0..arity)
                                    .map(|index| {
                                        if index == 0 && parameter {
                                            leaf::expression(
                                                "param",
                                                0,
                                                &Datum::Int(0),
                                                Some(&ty()),
                                                None,
                                            )
                                        } else {
                                            int(0)
                                        }
                                    })
                                    .collect(),
                            );
                            if result_field.is_none() {
                                function.ret_type = None;
                            }
                            let expression = Expression::ScalarFunction(function);
                            for profile in [0, 1, 3] {
                                for (selection_case, selection) in
                                    [None, Some(vec![1, 0, 1]), Some(vec![])]
                                        .into_iter()
                                        .enumerate()
                                {
                                    let mut input =
                                        chunk(&[ty()], &[vec![Datum::Int(0)], vec![Datum::Null]]);
                                    input.set_sel(selection);
                                    let context = FixedContext(leaf::Context::new(
                                        profile,
                                        if profile == 3 {
                                            Datum::Null
                                        } else {
                                            Datum::Int(0)
                                        },
                                    ));
                                    let observation =
                                        capture(&expression, &context, &input, &mut evaluate);
                                    records.push(format!("{name}/{arity}/{uppercase}/{result}/{parameter}/{profile}/{selection_case}|{observation}"));
                                }
                            }
                        }
                    }
                }
            }
        }
        records
    })
}
#[test]
fn native_entry_registry_original_capture() {
    let before = crate::test_original_eval::entries();
    let first = crate::test_original_eval::with_original(|| {
        registry_records(|e, c, i| crate::scalar_function::try_eval_batch(e, c, i))
    });
    assert_eq!(
        crate::test_original_eval::entries().1,
        before.1,
        "original registry entered shared scalar evaluator"
    );
    let second = crate::test_original_eval::with_original(|| {
        registry_records(|e, c, i| crate::scalar_function::try_eval_batch(e, c, i))
    });
    eprintln!(
        "N60 registry original {} {:016X}",
        first.len(),
        digest(&first)
    );
    assert_eq!(
        (first.len(), digest(&first)),
        (326304, 0xA2B2_5084_573D_4F4F)
    );
    assert_eq!(
        first, second,
        "independent original repeated registry changed"
    );
}

#[test]
fn native_entry_registry_shared_graph_matches_original() {
    let old = crate::test_original_eval::with_original(|| {
        registry_records(|e, c, i| crate::scalar_function::try_eval_batch(e, c, i))
    });
    assert_eq!((old.len(), digest(&old)), (326304, 0xA2B2_5084_573D_4F4F));
    let actual = without_native_tree_dispatch(|| {
        registry_records(|e, c, i| crate::scalar_function::try_eval_batch(e, c, i))
    });
    let mut differences = 0usize;
    let mut categories = std::collections::BTreeMap::<String, usize>::new();
    let mut shown = std::collections::BTreeSet::new();
    for (index, (old, new)) in old.iter().zip(&actual).enumerate() {
        if old != new {
            let name = old.split('/').next().unwrap();
            let category = format!(
                "{name}: {} -> {}",
                old.split('|').nth(1).unwrap(),
                new.split('|').nth(1).unwrap()
            );
            *categories.entry(category.clone()).or_default() += 1;
            if shown.len() < 24 && shown.insert(category) {
                eprintln!("N60 registry difference{index}\nOLD {old}\nNEW {new}");
            }
            differences += 1;
        }
    }
    eprintln!(
        "N60 registry shared {} / {differences} differences; categories {categories:?}",
        actual.len()
    );
    assert_eq!(differences, 0);
}

#[test]
fn native_entry_return_families_original_capture() {
    let before = crate::test_original_eval::entries();
    let original = crate::test_original_eval::with_original(|| {
        registry_records_with_fields(&remaining_fields(), |e, c, i| {
            crate::scalar_function::try_eval_batch(e, c, i)
        })
    });
    assert_eq!(
        crate::test_original_eval::entries().1,
        before.1,
        "original return families entered shared scalar evaluator"
    );
    let repeated = crate::test_original_eval::with_original(|| {
        registry_records_with_fields(&remaining_fields(), |e, c, i| {
            crate::scalar_function::try_eval_batch(e, c, i)
        })
    });
    eprintln!(
        "N60 return families original {} {:016X}",
        original.len(),
        digest(&original)
    );
    assert_eq!(
        (original.len(), digest(&original)),
        (571032, 0xEE0D_1D76_A4E3_18D3)
    );
    assert_eq!(original, repeated);
}

#[test]
fn native_entry_return_families_shared_graph_matches_original() {
    let original = crate::test_original_eval::with_original(|| {
        registry_records_with_fields(&remaining_fields(), |e, c, i| {
            crate::scalar_function::try_eval_batch(e, c, i)
        })
    });
    assert_eq!(
        (original.len(), digest(&original)),
        (571032, 0xEE0D_1D76_A4E3_18D3)
    );
    let actual = without_native_tree_dispatch(|| {
        registry_records_with_fields(&remaining_fields(), |e, c, i| {
            crate::scalar_function::try_eval_batch(e, c, i)
        })
    });
    let mut differences = 0usize;
    let mut shown = std::collections::BTreeSet::new();
    for (index, (old, new)) in original.iter().zip(&actual).enumerate() {
        if old != new {
            let category = old.split('/').take(4).collect::<Vec<_>>().join("/");
            if shown.len() < 24 && shown.insert(category) {
                eprintln!("N60 return families difference{index}\nOLD {old}\nNEW {new}");
            }
            differences += 1;
        }
    }
    eprintln!(
        "N60 return families shared {} / {differences} differences",
        actual.len()
    );
    assert_eq!(differences, 0);
}
