// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Public scalar callers have a real row, but no complete SQL row schema.
use super::*;
use crate::tikv::dynamic_leaf_tests as leaf;
use tidb_chunk::row::Row;

fn source(kind: usize, value: &Datum, optional: bool) -> Expression {
    let mut result = match kind {
        0 => literal(value.clone(), ty()),
        1 => column(0),
        2 => leaf::expression("param", 0, value, Some(&ty()), None),
        3 => leaf::expression("deferred_row", 0, value, Some(&ty()), None),
        4 => leaf::expression("correlated", 0, value, Some(&ty()), None),
        _ => unreachable!(),
    };
    if optional {
        match &mut result {
            Expression::Constant(c) => c.ret_type = None,
            Expression::Column(c) => c.ret_type = None,
            Expression::CorrelatedColumn(c) => c.column.ret_type = None,
            _ => unreachable!(),
        }
    }
    result
}
fn root(which: usize, argument: Expression) -> Expression {
    match which {
        0 => argument,
        1 => plus(argument, int(2)),
        2 => abs(argument),
        3 => call("if", ty(), vec![argument, int(11), int(22)]),
        4 => call("coalesce", ty(), vec![argument, int(33)]),
        5 => call(
            "concat",
            string_ty(),
            vec![argument, literal(Datum::new_string("!"), string_ty())],
        ),
        6 => call("eq", ty(), vec![argument, int(7)]),
        7 => call("in", ty(), vec![argument, int(7), int(8)]),
        _ => unreachable!(),
    }
}
fn records(
    mut evaluate: impl FnMut(
        &Expression,
        &leaf::Context,
        Row<'_>,
    ) -> std::result::Result<Datum, (Datum, EvalError)>,
    direct_function: bool,
) -> Vec<String> {
    let mut records = Vec::new();
    let input = chunk(
        &[ty()],
        &[
            vec![Datum::Int(10)],
            vec![Datum::Null],
            vec![Datum::Int(-3)],
        ],
    );
    for kind in 0..5 {
        for (vi, value) in [
            Datum::Null,
            Datum::Int(-1),
            Datum::Int(7),
            Datum::UInt(u64::MAX),
            Datum::Real(1.5),
            Datum::new_string("12"),
            Datum::Bytes(vec![0xff]),
        ]
        .into_iter()
        .enumerate()
        {
            for optional in [false, true] {
                for which in 0..8 {
                    let expression = root(which, source(kind, &value, optional));
                    if direct_function && !matches!(expression, Expression::ScalarFunction(_)) {
                        continue;
                    }
                    for profile in [0, 1, 3] {
                        for row in [
                            Row::empty(),
                            input.physical_row(0),
                            input.physical_row(1),
                            input.physical_row(2),
                        ] {
                            let context = leaf::Context::new(profile, value.clone());
                            let observation =
                                leaf::observe(|| evaluate(&expression, &context, row), &context);
                            records.push(format!(
                                "{kind}/{vi}/{optional}/{which}/{profile}/{}:{}|{observation}",
                                row.is_empty(),
                                row.idx()
                            ));
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
        // Include record boundaries, so concatenations cannot alias.
        for byte in record.len().to_le_bytes().into_iter().chain(record.bytes()) {
            hash = (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3);
        }
    }
    hash
}
#[test]
fn public_borrowed_original_expression_and_function_contracts() {
    let expressions = records(
        |expression, context, row| expression.eval_with_error_value(context, row),
        false,
    );
    let functions = records(
        |expression, context, row| {
            let Expression::ScalarFunction(function) = expression else {
                unreachable!()
            };
            function
                .eval(context, row)
                .map_err(|error| (Datum::Null, error))
        },
        true,
    );
    eprintln!(
        "N50 original expression {} {:016X}; function {} {:016X}",
        expressions.len(),
        digest(&expressions),
        functions.len(),
        digest(&functions)
    );
    assert_eq!(
        (expressions.len(), digest(&expressions)),
        (6720, 0x9DB38EEAA92DDE88)
    );
    assert_eq!(
        (functions.len(), digest(&functions)),
        (5880, 0x2550BA35CB389B63)
    );
    let ordinary = records(
        |expression, context, row| {
            expression
                .eval(context, row)
                .map_err(|error| (Datum::Null, error))
        },
        false,
    );
    eprintln!(
        "N50 original ordinary {} {:016X}",
        ordinary.len(),
        digest(&ordinary)
    );
    assert_eq!(
        (ordinary.len(), digest(&ordinary)),
        (6720, 0xE96DA50FA24BFFC5)
    );
}

#[test]
fn public_borrowed_expression_uses_shared_source_and_frame() {
    let original = records(
        |expression, context, row| {
            expression
                .eval(context, row)
                .map_err(|error| (Datum::Null, error))
        },
        false,
    );
    assert_eq!(
        (original.len(), digest(&original)),
        (6720, 0xE96DA50FA24BFFC5)
    );
    let actual = records(
        |expression, context, row| {
            super::super::scalar_source::BorrowedExpression::compile(
                expression,
                row.len(),
                true,
                limits(),
                ExecutionLimits::default(),
                1024 * 1024,
            )
            .and_then(|mut plan| plan.eval(context, row))
            .map_err(|error| match error.into_evaluator_error() {
                EvaluatorError::Eval(error) => (Datum::Null, error),
                other => panic!("unexpected scalar evaluator error {other:?}"),
            })
        },
        false,
    );
    for (index, (left, right)) in original.iter().zip(&actual).enumerate() {
        assert_eq!(left, right, "public source record {index}");
    }
    assert_eq!((actual.len(), digest(&actual)), (6720, 0xE96DA50FA24BFFC5));
}

#[test]
fn comparison_declined_integer_probe_demands_fresh_values_in_actual_suite() {
    for enabled in [false, true] {
        let expression = root(6, source(2, &Datum::Real(1.5), false));
        let reference = suite(expression.clone());
        let actual = suite(expression);
        let old = leaf::Context::new(0, Datum::Real(1.5));
        let new = leaf::Context::new(0, Datum::Real(1.5));
        old.enabled.set(enabled);
        new.enabled.set(enabled);
        let mut old_input = chunk(&[ty()], &[vec![Datum::Int(0)]]);
        let mut new_input = chunk(&[ty()], &[vec![Datum::Int(0)]]);
        let mut expected = output();
        let mut output = output();
        reference.run(&old, &mut old_input, &mut expected).unwrap();
        actual
            .run_with_tikv_batch(&new, &mut new_input, &mut output, &[ty()])
            .unwrap();
        assert_eq!(output.column(0), expected.column(0));
        assert_eq!(new.snapshot(), old.snapshot(), "vector flag {enabled}");
        let route = actual.batch_route_snapshot();
        assert_eq!((route.refusals, route.fallback), (0, 0));
    }
}

#[test]
fn comparison_fresh_parameter_value_changes_the_result() {
    use crate::tikv::typed_dynamic_tests::Context as Script;
    for enabled in [false, true] {
        for name in ["eq", "ne", "lt", "le", "gt", "ge"] {
            for left in [false, true] {
                let parameter = source(2, &Datum::Real(1.5), false);
                let expression = call(
                    name,
                    ty(),
                    if left {
                        vec![parameter, int(7)]
                    } else {
                        vec![int(7), parameter]
                    },
                );
                let reference = suite(expression.clone());
                let actual = suite(expression);
                let old = Script::new(0, vec![Datum::Real(1.5), Datum::Int(7)]);
                let new = Script::new(0, vec![Datum::Real(1.5), Datum::Int(7)]);
                old.inner.enabled.set(enabled);
                new.inner.enabled.set(enabled);
                let mut old_input = chunk(&[ty()], &[vec![Datum::Int(0)]]);
                let mut new_input = chunk(&[ty()], &[vec![Datum::Int(0)]]);
                let mut expected = output();
                let mut output = output();
                reference.run(&old, &mut old_input, &mut expected).unwrap();
                assert_eq!(old.inner.calls.get(), 2);
                if name == "eq" {
                    assert_eq!(values(&expected), vec![Datum::Int(1)]);
                }
                actual
                    .run_with_tikv_batch(&new, &mut new_input, &mut output, &[ty()])
                    .unwrap();
                assert_eq!(
                    output.column(0),
                    expected.column(0),
                    "{name}/{left}/{enabled}"
                );
                assert_eq!(new.snapshot(), old.snapshot(), "{name}/{left}/{enabled}");
                let route = actual.batch_route_snapshot();
                assert_eq!((route.refusals, route.fallback), (0, 0));
            }
        }
    }
}

#[test]
fn borrowed_public_plan_keeps_real_physical_row_optional_result_and_live_context() {
    use super::super::scalar_source::BorrowedExpression;
    let mut input = chunk(
        &[ty()],
        &[
            vec![Datum::Int(10)],
            vec![Datum::Null],
            vec![Datum::Int(-3)],
        ],
    );
    input.set_sel(Some(vec![2, 0, 2]));
    for which in [0, 1, 3, 4, 6, 7] {
        for missing_result in [false, true] {
            let mut expression = root(which, source(2, &Datum::Null, false));
            if missing_result {
                match &mut expression {
                    Expression::Constant(c) => c.ret_type = None,
                    Expression::ScalarFunction(f) => f.ret_type = None,
                    _ => unreachable!(),
                }
            }
            let mut plan = BorrowedExpression::compile(
                &expression,
                1,
                true,
                limits(),
                ExecutionLimits::default(),
                1 << 20,
            )
            .unwrap();
            for value in [Datum::Null, Datum::Int(7), Datum::Real(1.5)] {
                for profile in [0, 1, 3] {
                    for row in [input.get_row(0), input.physical_row(1), input.get_row(2)] {
                        let old = leaf::Context::new(profile, value.clone());
                        let actual = leaf::Context::new(profile, value.clone());
                        let want = leaf::observe(
                            || expression.eval(&old, row).map_err(|e| (Datum::Null, e)),
                            &old,
                        );
                        let got = leaf::observe(
                            || {
                                plan.eval(&actual, row).map_err(|e| {
                                    match e.into_evaluator_error() {
                                        EvaluatorError::Eval(e) => (Datum::Null, e),
                                        other => panic!("unexpected {other:?}"),
                                    }
                                })
                            },
                            &actual,
                        );
                        assert_eq!(got, want, "{which}/{missing_result}/{profile}");
                    }
                }
            }
        }
    }
    // Real storage has no public SQL-schema contract. Both declarations decode
    // the same eight-byte cell; the scalar source must use the node's own type.
    let mut column = Column::new(1, FieldType::new(FieldTypeCode::Year));
    column.index = 0;
    let expression = Expression::Column(column);
    let mut plan = BorrowedExpression::compile(
        &expression,
        1,
        true,
        limits(),
        ExecutionLimits::default(),
        1 << 20,
    )
    .unwrap();
    assert_eq!(
        plan.eval(&crate::NoColumns, input.get_row(0)).unwrap(),
        expression
            .eval(&crate::NoColumns, input.get_row(0))
            .unwrap()
    );
}

#[test]
fn borrowed_public_plan_resource_refusal_and_row_width_recover_without_retry() {
    use super::super::scalar_source::BorrowedExpression;
    let expression = root(6, source(2, &Datum::Null, false));
    let mut plan = BorrowedExpression::compile(
        &expression,
        0,
        true,
        limits(),
        ExecutionLimits::default(),
        64 << 10,
    )
    .unwrap();
    let huge = leaf::Context::new(0, Datum::Bytes(vec![b'x'; 2 << 20]));
    assert_eq!(
        plan.eval(&huge, Row::empty()).unwrap_err().refusal(),
        BatchRefusal::Resource
    );
    let small = leaf::Context::new(0, Datum::Int(7));
    assert_eq!(plan.eval(&small, Row::empty()).unwrap(), Datum::Int(1));
    assert_eq!(small.calls.get(), 1);
    let input = chunk(&[ty()], &[vec![Datum::Int(0)]]);
    let untouched = leaf::Context::new(0, Datum::Int(7));
    assert_eq!(
        plan.eval(&untouched, input.physical_row(0))
            .unwrap_err()
            .refusal(),
        BatchRefusal::Contract
    );
    assert_eq!(untouched.calls.get(), 0);
    assert_eq!(plan.eval(&untouched, Row::empty()).unwrap(), Datum::Int(1));
}

#[test]
fn actual_comparison_probe_keeps_selection_and_atomic_or_prefix_publication() {
    use crate::tikv::typed_dynamic_tests::Context as Script;
    for enabled in [false, true] {
        for size in [0, 3, 2051] {
            for selected in [false, true] {
                let expression = root(6, source(2, &Datum::Null, false));
                let reference = suite(expression.clone());
                let actual = suite(expression);
                let rows = (0..size).map(|_| vec![Datum::Int(0)]).collect::<Vec<_>>();
                let mut old_input = chunk(&[ty()], &rows);
                let mut new_input = chunk(&[ty()], &rows);
                let selection = selected.then(|| {
                    if size == 0 {
                        vec![]
                    } else {
                        (0..size + 6).map(|i| size - 1 - i % size).collect()
                    }
                });
                old_input.set_sel(selection.clone());
                new_input.set_sel(selection);
                let count = old_input.num_rows();
                let mut script = vec![Datum::Int(7); count.saturating_sub(1)];
                // Refuse the fast probe on the last row, then fail the genuine
                // fresh generic demand. Nothing is filtered at a window edge.
                script.extend([Datum::Real(1.5), Datum::MinNotNull]);
                let old = Script::new(0, script.clone());
                let new = Script::new(0, script);
                old.inner.enabled.set(enabled);
                new.inner.enabled.set(enabled);
                let mut expected = output();
                let mut output = output();
                expected.append_null(0);
                output.append_null(0);
                let want =
                    session_read_outcome(|| reference.run(&old, &mut old_input, &mut expected));
                let got = session_read_outcome(|| {
                    actual.run_with_tikv_batch(&new, &mut new_input, &mut output, &[ty()])
                });
                assert_eq!(got, want, "{enabled}/{size}/{selected}");
                assert_eq!(
                    output.column(0),
                    expected.column(0),
                    "{enabled}/{size}/{selected}"
                );
                assert_eq!(
                    new.snapshot(),
                    old.snapshot(),
                    "{enabled}/{size}/{selected}"
                );
                let route = actual.batch_route_snapshot();
                assert_eq!((route.refusals, route.fallback), (0, 0));
            }
        }
    }
}

#[test]
fn borrowed_scalar_does_not_validate_an_undemanded_empty_chunk_row() {
    use super::super::scalar_source::BorrowedExpression;
    // A cursor exists before cells do. The original scalar contract permits a
    // literal or an unselected column branch at that cursor.
    let input = Chunk::new_empty(&[ty()]);
    let row = input.get_row(0);
    for expression in [int(9), call("if", ty(), vec![int(0), column(0), int(9)])] {
        assert_eq!(
            expression.eval(&crate::NoColumns, row).unwrap(),
            Datum::Int(9)
        );
        let mut plan = BorrowedExpression::compile(
            &expression,
            row.len(),
            true,
            limits(),
            ExecutionLimits::default(),
            1 << 20,
        )
        .unwrap();
        assert_eq!(plan.eval(&crate::NoColumns, row).unwrap(), Datum::Int(9));
    }
}

#[path = "batch_public_error_tests.rs"]
mod error_value_tests;

#[path = "batch_public_function_tests.rs"]
mod function_root_tests;

#[path = "batch_public_row_tests.rs"]
mod physical_row_tests;
