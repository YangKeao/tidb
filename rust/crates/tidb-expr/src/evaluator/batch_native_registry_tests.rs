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
fn native_entry_registry_public_matches_frozen_results() {
    let actual = registry_records(|e, c, i| crate::scalar_function::try_eval_batch(e, c, i));
    assert_eq!(
        (actual.len(), digest(&actual)),
        (326304, 0xA2B2_5084_573D_4F4F)
    );
}

#[test]
fn native_entry_return_families_public_matches_frozen_results() {
    let actual = registry_records_with_fields(&remaining_fields(), |e, c, i| {
        crate::scalar_function::try_eval_batch(e, c, i)
    });
    assert_eq!(
        (actual.len(), digest(&actual)),
        (571032, 0xEE0D_1D76_A4E3_18D3)
    );
}
