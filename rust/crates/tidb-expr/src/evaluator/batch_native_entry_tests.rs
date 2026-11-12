// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Independent public native batch entry observations before its retirement.
use super::*;
use crate::tikv::dynamic_leaf_tests as leaf;
use std::sync::{Arc, RwLock};

fn without_native_tree_dispatch<T>(body: impl FnOnce() -> T) -> T {
    let before = crate::test_evaluation::shared_entries();
    let value = body();
    assert_eq!(
        crate::test_evaluation::shared_entries(),
        before,
        "batch source nested a public scalar driver"
    );
    value
}

fn fields() -> Vec<FieldType> {
    vec![
        ty(),
        ty().with_unsigned(true),
        real_ty(),
        FieldType::new(FieldTypeCode::NewDecimal)
            .with_flen(20)
            .with_decimal(2),
        string_ty(),
        FieldType::new(FieldTypeCode::Json),
    ]
}
fn physical_values(which: usize) -> Vec<Datum> {
    match which {
        0 => vec![Datum::Int(7), Datum::Null, Datum::Int(-3)],
        1 => vec![Datum::UInt(7), Datum::Null, Datum::UInt(u64::MAX)],
        2 => vec![Datum::Real(1.25), Datum::Null, Datum::Real(-0.0)],
        3 => vec![
            Datum::Decimal(Decimal::from_literal("2.50")),
            Datum::Null,
            Datum::Decimal(Decimal::from_literal("-1.25")),
        ],
        4 => vec![
            Datum::new_string("12x"),
            Datum::Null,
            Datum::Bytes(vec![0xff]),
        ],
        5 => vec![
            Datum::Json(BinaryJSON::parse("1").unwrap()),
            Datum::Null,
            Datum::Json(BinaryJSON::parse("null").unwrap()),
        ],
        _ => unreachable!(),
    }
}
fn argument(kind: usize, field: &FieldType, value: &Datum) -> Expression {
    match kind {
        0 => literal(value.clone(), field.clone()),
        1 => typed_column(0, field),
        2 => leaf::expression("param", 0, value, Some(field), None),
        3 => leaf::expression("deferred_row", 0, value, Some(field), None),
        4 => leaf::expression("deferred_value", 0, value, Some(field), None),
        5 => leaf::expression(
            "correlated",
            0,
            value,
            Some(field),
            Some(Arc::new(RwLock::new(value.clone()))),
        ),
        6 => typed_column(1, field),
        _ => unreachable!(),
    }
}
fn root(which: usize, value: Expression, field: &FieldType) -> Expression {
    match which {
        0 => value,
        1 => call("abs", field.clone(), vec![value]),
        2 => call("unaryminus", field.clone(), vec![value]),
        3 => call(
            "plus",
            field.clone(),
            vec![value, literal(Datum::Int(2), ty())],
        ),
        4 => call(
            "cast_json",
            FieldType::new(FieldTypeCode::Json),
            vec![value],
        ),
        5 => call(
            "cast_decimal",
            FieldType::new(FieldTypeCode::NewDecimal)
                .with_flen(20)
                .with_decimal(2),
            vec![value],
        ),
        6 => call("char_length", ty(), vec![value]),
        7 => call("upper", string_ty(), vec![value]),
        8 => call("substring", string_ty(), vec![value, int(1), int(2)]),
        9 => call("eq", ty(), vec![value, int(2)]),
        10 => call(
            "coalesce",
            field.clone(),
            vec![value, literal(Datum::Int(2), ty())],
        ),
        11 => call("__native_entry_unknown__", field.clone(), vec![value]),
        _ => unreachable!(),
    }
}
fn wrap(value: Expression, which: usize, field: &FieldType) -> Expression {
    if which == 0 {
        return value;
    }
    let mut outer = Constant::new(Datum::new_string("saved native wrapper"), field.clone());
    outer.deferred_expr = Some(Box::new(value));
    if which == 2 {
        outer.param_marker = Some(ParamMarker { order: 0 });
    }
    Expression::Constant(outer)
}
fn observation(
    expression: &Expression,
    context: &leaf::Context,
    input: &Chunk,
    evaluate: &mut impl FnMut(
        &Expression,
        &leaf::Context,
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
    format!("{value}|{}", context.snapshot())
}
fn records(
    mut evaluate: impl FnMut(
        &Expression,
        &leaf::Context,
        &Chunk,
    ) -> std::result::Result<Option<Vec<Datum>>, EvalError>,
) -> Vec<String> {
    let mut records = Vec::new();
    for (fi, field) in fields().iter().enumerate() {
        let values = physical_values(fi);
        let rows = values.iter().cloned().map(|v| vec![v]).collect::<Vec<_>>();
        for kind in 0..7 {
            for which in 0..12 {
                for wrapper in 0..3 {
                    let expression = wrap(
                        root(which, argument(kind, field, &values[0]), field),
                        wrapper,
                        field,
                    );
                    for phase in 0..4 {
                        for (selection_case, selection) in [
                            None,
                            Some(vec![2, 0, 2, 1]),
                            Some(vec![]),
                            Some(vec![1]),
                            Some(vec![usize::MAX]),
                            Some(vec![usize::MAX, 0, usize::MAX]),
                        ]
                        .into_iter()
                        .enumerate()
                        {
                            let mut input = chunk(std::slice::from_ref(field), &rows);
                            input.set_sel(selection);
                            let (profile, parameter) = match phase {
                                0 => (0, Datum::Int(7)),
                                1 => (0, Datum::new_string("9x")),
                                2 => (1, Datum::Int(11)),
                                3 => (3, Datum::Null),
                                _ => unreachable!(),
                            };
                            let mut context = leaf::Context::new(profile, parameter);
                            context.sequential = phase == 1;
                            let observation =
                                observation(&expression, &context, &input, &mut evaluate);
                            records.push(format!("{fi}/{kind}/{which}/{wrapper}/{phase}/{selection_case}|{observation}"));
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
fn native_entry_public_contract_matches_frozen_results() {
    let actual = records(|e, c, i| crate::scalar_function::try_eval_batch(e, c, i));
    assert_eq!(
        (actual.len(), digest(&actual)),
        (36288, 0xF371_2C96_0BDD_F2E8)
    );
}

#[path = "batch_native_registry_tests.rs"]
mod registry_tests;
fn integer_result_records(
    mut evaluate: impl FnMut(
        &Expression,
        &leaf::Context,
        &Chunk,
    ) -> std::result::Result<Option<Vec<Datum>>, EvalError>,
) -> Vec<String> {
    let values = vec![
        Datum::Null,
        Datum::Int(-1),
        Datum::UInt(u64::MAX),
        Datum::Real(f64::NAN),
        Datum::new_string("7x"),
        Datum::Bytes(vec![0xff]),
        Datum::Decimal(Decimal::from_literal("1.5")),
        Datum::VectorFloat32(VectorFloat32::must_create(vec![1.0, -2.0])),
    ];
    let mut records = vec![];
    for name in [
        "unaryplus",
        "coalesce",
        "ifnull",
        "if",
        "greatest",
        "least",
        "bitand",
        "rightshift",
    ] {
        for (vi, value) in values.iter().enumerate() {
            let field = crate::tikv::misc_identity_value_tests::field(value);
            for unsigned in [false, true] {
                for parameter in [false, true] {
                    for wrapped in [false, true] {
                        let producer = if parameter {
                            leaf::expression("param", 0, value, Some(&field), None)
                        } else {
                            literal(value.clone(), field.clone())
                        };
                        let unavailable =
                            || leaf::expression("param", 1, &Datum::Int(0), Some(&ty()), None);
                        let args = match name {
                            "unaryplus" => vec![producer],
                            "if" => vec![int(1), producer, unavailable()],
                            "ifnull" | "coalesce" => vec![producer, unavailable()],
                            _ => vec![producer, int(0)],
                        };
                        let result = ty().with_unsigned(unsigned);
                        let expression = call(name, result.clone(), args);
                        let expression = if wrapped {
                            wrap(expression, 1, &result)
                        } else {
                            expression
                        };
                        for profile in [0, 1, 3] {
                            for (si, selection) in [None, Some(vec![2, 0, 2]), Some(vec![])]
                                .into_iter()
                                .enumerate()
                            {
                                let mut input = chunk(&[], &[vec![], vec![], vec![]]);
                                input.set_sel(selection);
                                let context = leaf::Context::new(profile, value.clone());
                                let result =
                                    observation(&expression, &context, &input, &mut evaluate);
                                records.push(format!("{name}/{vi}/{unsigned}/{parameter}/{wrapped}/{profile}/{si}|{result}"));
                            }
                        }
                    }
                }
            }
        }
    }
    records
}
#[test]
fn native_entry_integer_result_matches_frozen_results() {
    let actual = integer_result_records(|e, c, i| crate::scalar_function::try_eval_batch(e, c, i));
    assert_eq!(
        (actual.len(), digest(&actual)),
        (4608, 0xD46B_5C12_B950_21E3)
    );
}
fn hybrid_result_records(
    mut evaluate: impl FnMut(
        &Expression,
        &leaf::Context,
        &Chunk,
    ) -> std::result::Result<Option<Vec<Datum>>, EvalError>,
) -> Vec<String> {
    let fields = vec![
        ty(),
        FieldType::new(FieldTypeCode::Bit).with_flen(64),
        FieldType::new(FieldTypeCode::Enum).with_flags(FieldTypeFlags::ENUM_SET_AS_INT),
        FieldType::new(FieldTypeCode::Set).with_flags(FieldTypeFlags::ENUM_SET_AS_INT),
    ];
    let values = [
        Datum::Int(0),
        Datum::Int(-1),
        Datum::UInt(u64::MAX),
        Datum::Null,
    ];
    let mut records = vec![];
    for name in [
        "unaryplus",
        "bitneg",
        "bitand",
        "bitor",
        "bitxor",
        "rightshift",
    ] {
        for (fi, field) in fields.iter().enumerate() {
            for unsigned in [false, true] {
                for (vi, value) in values.iter().enumerate() {
                    for wrapped in [false, true] {
                        for parameter in [false, true] {
                            let source = if parameter {
                                leaf::expression("param", 0, value, Some(&ty()), None)
                            } else {
                                literal(value.clone(), ty())
                            };
                            let args = if matches!(name, "unaryplus" | "bitneg") {
                                vec![source]
                            } else {
                                vec![source, int(0)]
                            };
                            let result = field.clone().with_unsigned(unsigned);
                            let expression = call(name, result.clone(), args);
                            let expression = if wrapped {
                                wrap(expression, 1, &result)
                            } else {
                                expression
                            };
                            for profile in [0, 1, 3] {
                                for (si, selection) in [None, Some(vec![2, 0, 2]), Some(vec![])]
                                    .into_iter()
                                    .enumerate()
                                {
                                    let mut input = chunk(&[], &[vec![], vec![], vec![]]);
                                    input.set_sel(selection);
                                    let context = leaf::Context::new(profile, value.clone());
                                    let result =
                                        observation(&expression, &context, &input, &mut evaluate);
                                    records.push(format!("{name}/{fi}/{unsigned}/{vi}/{wrapped}/{parameter}/{profile}/{si}|{result}"));
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    records
}
#[test]
fn native_entry_hybrid_result_matches_frozen_results() {
    let actual = hybrid_result_records(|e, c, i| crate::scalar_function::try_eval_batch(e, c, i));
    assert_eq!(
        (actual.len(), digest(&actual)),
        (6912, 0x8637_5CDF_1EAB_4989)
    );
}
fn run_native_surface(
    e: &Expression,
    c: &dyn Columns,
    i: &Chunk,
    _shared: bool,
) -> std::result::Result<Option<Vec<Datum>>, EvalError> {
    crate::scalar_function::try_eval_batch(e, c, i)
}
fn lifecycle_records(shared: bool) -> Vec<String> {
    let mut records = vec![];
    for which in 0..6 {
        let binding = Arc::new(RwLock::new(Datum::Null));
        let parameter = || leaf::expression("param", 0, &Datum::Int(0), Some(&ty()), None);
        let expression = match which {
            0 => plus(parameter(), parameter()),
            1 => call(
                "coalesce",
                ty(),
                vec![
                    leaf::expression(
                        "correlated",
                        0,
                        &Datum::Null,
                        Some(&ty()),
                        Some(Arc::clone(&binding)),
                    ),
                    parameter(),
                ],
            ),
            2 => call(
                "upper",
                string_ty(),
                vec![leaf::expression(
                    "param",
                    0,
                    &Datum::new_string("0"),
                    Some(&string_ty()),
                    None,
                )],
            ),
            3 => call(
                "cast_decimal",
                FieldType::new(FieldTypeCode::NewDecimal)
                    .with_flen(20)
                    .with_decimal(2),
                vec![plus(parameter(), parameter())],
            ),
            4 => {
                let field = FieldType::new(FieldTypeCode::Bit).with_flen(64);
                wrap(
                    call("bitand", field.clone(), vec![parameter(), int(7)]),
                    1,
                    &field,
                )
            }
            5 => plus(column(0), parameter()),
            _ => unreachable!(),
        };
        for scoped in [false, true] {
            let cache = crate::ReadyValueCache::new();
            for count in [0, 1, 1024, 1025, 2050] {
                let mut input = chunk(
                    &[ty()],
                    &[vec![Datum::Int(1)], vec![Datum::Null], vec![Datum::Int(3)]],
                );
                input.set_sel(Some((0..count).map(|n| [2, 0, 1][n % 3]).collect()));
                for phase in 0..6 {
                    *binding
                        .write()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) = Datum::Null;
                    let (profile, value, sequential) = match phase {
                        0 => (0, Datum::Int(7), false),
                        1 => (0, Datum::Int(0), true),
                        2 => (1, Datum::Int(7), false),
                        3 => (2, Datum::Int(7), false),
                        4 => (0, Datum::Int(-4), false),
                        5 => (3, Datum::Null, false),
                        _ => unreachable!(),
                    };
                    let mut context = leaf::Context::new(profile, value);
                    context.sequential = sequential;
                    context.binding = Some(Arc::clone(&binding));
                    let result = observation(&expression, &context, &input, &mut |e, c, i| {
                        if scoped {
                            cache.with_columns(c, |bound| run_native_surface(e, bound, i, shared))
                        } else {
                            run_native_surface(e, c, i, shared)
                        }
                    });
                    records.push(format!("{which}/{scoped}/{count}/{phase}|{result}"));
                }
            }
        }
    }
    records
}
#[test]
fn native_entry_long_live_context_panic_and_null_recovery() {
    let actual = lifecycle_records(true);
    assert_eq!(
        (actual.len(), digest(&actual)),
        (360, 0x2B52_45F3_8AEC_C3EC)
    );
}
#[test]
fn native_entry_resource_refusals_do_not_evaluate_or_retry_and_recover() {
    let parameter = || leaf::expression("param", 0, &Datum::Int(0), Some(&ty()), None);
    let expression = plus(parameter(), parameter());
    let input = chunk(&[], &[vec![], vec![], vec![]]);
    let original_context = leaf::Context::new(0, Datum::Int(7));
    let original =
        (|| crate::scalar_function::try_eval_batch(&expression, &original_context, &input))()
            .unwrap()
            .unwrap();
    let original_calls = original_context.calls.get();
    assert_eq!(
        original_calls, 4,
        "two real parameters each read original type then value"
    );
    without_native_tree_dispatch(|| {
        let (source, execution) = public_scalar_limits();
        for mode in 0..3 {
            let context = leaf::Context::new(0, Datum::Int(7));
            let mut source = source;
            let mut execution = execution;
            match mode {
                0 => source.tree.max_nodes = 0,
                1 => execution.max_steps = 0,
                2 => execution.max_retained_bytes = 1,
                _ => unreachable!(),
            }
            let candidate = crate::scalar_function::batch_candidate(&expression, &context)
                .unwrap()
                .unwrap();
            let actual =
                native_source::eval_with_policies(candidate, &context, &input, source, execution)
                    .unwrap();
            assert_eq!(actual, original);
            assert_eq!(context.calls.get(), original_calls);
            context.calls.set(0);
            let candidate = crate::scalar_function::batch_candidate(&expression, &context)
                .unwrap()
                .unwrap();
            assert_eq!(
                native_source::eval(candidate, &context, &input).unwrap(),
                original
            );
            assert_eq!(
                context.calls.get(),
                original_calls,
                "recovery replayed a value effect"
            );
        }
        let context = leaf::Context::new(0, Datum::Int(7));
        let empty = chunk(&[], &[]);
        let candidate = crate::scalar_function::batch_candidate(&expression, &context)
            .unwrap()
            .unwrap();
        assert!(native_source::eval_with_policies(
            candidate,
            &context,
            &empty,
            BatchSourceLimits {
                tree: CompileLimits {
                    max_nodes: 0,
                    ..source.tree
                },
                max_metadata_bytes: 0
            },
            ExecutionLimits {
                max_steps: 0,
                max_retained_bytes: 0,
                ..execution
            }
        )
        .unwrap()
        .is_empty());
        assert_eq!(
            context.calls.get(),
            0,
            "empty native entry evaluated or compiled values"
        );
        let mut huge = chunk(&[], &[]);
        huge.set_num_virtual_rows(usize::MAX);
        let candidate = crate::scalar_function::batch_candidate(&expression, &context)
            .unwrap()
            .unwrap();
        let error = native_source::eval(candidate, &context, &huge).unwrap_err();
        assert!(
            matches!(error,EvalError::ExpressionRuntimeFailure(ref error) if error.class()==crate::ExpressionRuntimeFailureClass::ResourceLimit)
        );
        assert_eq!(
            context.calls.get(),
            0,
            "oversized output allocated or evaluated before its extent check"
        );
    });
}
fn entropy_records(shared: bool) -> Vec<String> {
    let mut records = vec![];
    for name in [
        "rand",
        "random_bytes",
        "uuid",
        "uuid_v4",
        "uuid_v7",
        "uuid_short",
    ] {
        for uppercase in [false, true] {
            let field = if name == "rand" {
                real_ty()
            } else if name == "uuid_short" {
                ty().with_unsigned(true)
            } else {
                string_ty()
            };
            let args = if name == "rand" {
                vec![int(7)]
            } else if name == "random_bytes" {
                vec![int(4)]
            } else {
                vec![]
            };
            let expression = call(
                &if uppercase {
                    name.to_ascii_uppercase()
                } else {
                    name.into()
                },
                field,
                args,
            );
            for scoped in [false, true] {
                let cache = crate::ReadyValueCache::new();
                for repeat in 0..8 {
                    let context = leaf::Context::new(0, Datum::Int(0));
                    let input = chunk(&[], &[vec![], vec![], vec![]]);
                    let result = if scoped {
                        cache.with_columns(&context, |bound| {
                            run_native_surface(&expression, bound, &input, shared)
                        })
                    } else {
                        run_native_surface(&expression, &context, &input, shared)
                    };
                    let result = match result {
                        Ok(Some(_)) => {
                            panic!("native entropy admission changed; retain the original decline")
                        }
                        Ok(None) => "NONE".into(),
                        Err(error) => format!("ERROR:{error:?}"),
                    };
                    records.push(format!(
                        "{name}/{uppercase}/{scoped}/{repeat}|{result}|{}",
                        context.snapshot()
                    ));
                }
            }
        }
    }
    records
}
#[test]
fn native_entry_entropy_keeps_actual_admission_without_value_effects() {
    let original = (|| entropy_records(false))();
    let actual = without_native_tree_dispatch(|| entropy_records(true));
    assert_eq!(
        original, actual,
        "entropy admission, shape or effects changed"
    );
    assert!(actual.iter().all(|record| record.contains("|NONE|")));
    eprintln!(
        "N60 entropy admission {} entries / {} actual value batches / {} declined batches",
        actual.len(),
        actual.iter().filter(|r| r.contains("|VALUES:")).count(),
        actual.iter().filter(|r| r.contains("|NONE|")).count()
    );
}
