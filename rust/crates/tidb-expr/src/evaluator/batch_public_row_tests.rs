// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Genuine physical-row metadata and eager unregistered-name contracts.
use super::*;

fn actual_column(index: i64, declared: bool, markers: usize) -> Expression {
    let mut value = Column::default();
    value.index = index;
    value.unique_id = 37;
    value.orig_name = "actual.row".into();
    value.ret_type = declared.then(ty);
    if markers & 1 != 0 {
        // Column.Eval ignores this planner metadata, including its side effects
        // and invalid parameter. A physical-row source must not walk it.
        value.virtual_expr = Some(Box::new(leaf::expression(
            "param",
            99,
            &Datum::Int(-99),
            Some(&ty()),
            None,
        )));
    }
    if markers & 2 != 0 {
        value.correlated_col_unique_id = 731;
    }
    Expression::Column(value)
}
fn row_records(
    mut evaluate: impl FnMut(
        &Expression,
        &leaf::Context,
        Row<'_>,
    ) -> std::result::Result<Datum, (Datum, EvalError)>,
) -> Vec<String> {
    let mut observations = Vec::new();
    let input = chunk(&[ty()], &[vec![Datum::Int(17)], vec![Datum::Null]]);
    for index in [-1, 0, 1] {
        for declared in [false, true] {
            for markers in 0..4 {
                for shape in 0..4 {
                    let value = actual_column(index, declared, markers);
                    let expression = match shape {
                        0 => value,
                        1 => call("if", ty(), vec![int(0), value, int(31)]),
                        2 => call("coalesce", ty(), vec![int(41), value]),
                        3 => {
                            let mut constant = Constant::default();
                            constant.value = Datum::new_string("actual outer cached Datum");
                            constant.ret_type = Some(ty());
                            constant.deferred_expr = Some(Box::new(value));
                            Expression::Constant(constant)
                        }
                        _ => unreachable!(),
                    };
                    for (row_case, row) in
                        [Row::empty(), input.physical_row(0), input.physical_row(1)]
                            .into_iter()
                            .enumerate()
                    {
                        let context = leaf::Context::new(0, Datum::Int(7));
                        let observation =
                            leaf::observe(|| evaluate(&expression, &context, row), &context);
                        observations.push(format!(
                            "{index}/{declared}/{markers}/{shape}/{row_case}|{observation}"
                        ));
                    }
                }
            }
        }
    }
    observations
}
fn cursor_records(
    mut evaluate: impl FnMut(
        &Expression,
        &leaf::Context,
        Row<'_>,
    ) -> std::result::Result<Datum, (Datum, EvalError)>,
) -> Vec<String> {
    let input = chunk(&[ty()], &[vec![Datum::Int(11)]]);
    let mut observations = Vec::new();
    for coordinate in [0, 1, 17, usize::MAX - 1, usize::MAX] {
        for shape in 0..4 {
            let expression = match shape {
                0 => int(7),
                1 => call("if", ty(), vec![int(0), column(0), int(31)]),
                2 => call("coalesce", ty(), vec![int(41), column(0)]),
                3 => leaf::expression("param", 0, &Datum::Int(23), Some(&ty()), None),
                _ => unreachable!(),
            };
            let context = leaf::Context::new(0, Datum::Int(23));
            let row = input.physical_row(coordinate);
            let observation = leaf::observe(|| evaluate(&expression, &context, row), &context);
            observations.push(format!("{coordinate}/{shape}|{observation}"));
        }
    }
    observations
}
fn unknown_records(
    mut evaluate: impl FnMut(
        &Expression,
        &leaf::Context,
        Row<'_>,
    ) -> std::result::Result<Datum, (Datum, EvalError)>,
) -> Vec<String> {
    let mut observations = Vec::new();
    for name in [
        "not_a_registered_function",
        "MiXeD_unregistered",
        "函数不存在",
        "sig_unregistered",
    ] {
        for optional in [false, true] {
            for arity in 0..4 {
                let mut function = ScalarFunction::default();
                function.func_name = CiString::new(name);
                function.ret_type = (!optional).then(ty);
                function.args = (0..arity)
                    .map(|order| {
                        let mut argument =
                            leaf::expression("param", order, &Datum::Int(-7), Some(&ty()), None);
                        if optional {
                            if let Expression::Constant(constant) = &mut argument {
                                constant.ret_type = None;
                            }
                        }
                        argument
                    })
                    .collect();
                let expression = Expression::ScalarFunction(function);
                for profile in [0, 1, 3] {
                    let context = leaf::Context::new(profile, Datum::Int(7));
                    let observation =
                        leaf::observe(|| evaluate(&expression, &context, Row::empty()), &context);
                    observations.push(format!("{name}/{optional}/{arity}/{profile}|{observation}"));
                }
            }
        }
    }
    observations
}
#[test]
fn original_physical_row_metadata_and_eager_name_contracts() {
    for (name, records) in [
        (
            "metadata",
            row_records(|e, c, r| e.eval_with_error_value(c, r)),
        ),
        (
            "cursor",
            cursor_records(|e, c, r| e.eval_with_error_value(c, r)),
        ),
        (
            "unknown",
            unknown_records(|e, c, r| e.eval_with_error_value(c, r)),
        ),
    ] {
        eprintln!(
            "N53 original {name} {} {:016X}",
            records.len(),
            digest(&records)
        );
        assert_eq!(
            (records.len(), digest(&records)),
            match name {
                "metadata" => (288, 0x433BC944B34D725D),
                "cursor" => (20, 0x8A3846E1FFF37587),
                "unknown" => (96, 0x636A12C8D9724869),
                _ => unreachable!(),
            }
        );
    }
}

fn frame_value(
    expression: &Expression,
    context: &leaf::Context,
    row: Row<'_>,
) -> std::result::Result<Datum, (Datum, EvalError)> {
    let error = |error: BatchFailure| match error.into_evaluator_error() {
        EvaluatorError::Eval(error) => error,
        other => panic!("unexpected public-source failure {other:?}"),
    };
    let mut plan = super::super::scalar_source::BorrowedExpression::compile(
        expression,
        row.len(),
        true,
        limits(),
        ExecutionLimits::default(),
        1 << 20,
    )
    .map_err(|e| (Datum::Null, error(e)))?;
    plan.eval_with_error_value(context, row)
        .map_err(|(datum, e)| (datum, error(e)))
}
#[test]
fn physical_row_metadata_uses_original_column_eval_contract() {
    let original = row_records(|e, c, r| e.eval_with_error_value(c, r));
    let actual = row_records(frame_value);
    assert_eq!(
        (original.len(), digest(&original)),
        (288, 0x433BC944B34D725D)
    );
    for (index, (left, right)) in original.iter().zip(&actual).enumerate() {
        assert_eq!(left, right, "physical row metadata record {index}");
    }
    assert_eq!((actual.len(), digest(&actual)), (288, 0x433BC944B34D725D));
}
#[test]
fn genuine_cursor_keeps_max_coordinate_and_original_live_context() {
    let original = cursor_records(|e, c, r| e.eval_with_error_value(c, r));
    let actual = cursor_records(frame_value);
    assert_eq!(
        (original.len(), digest(&original)),
        (20, 0x8A3846E1FFF37587)
    );
    for (index, (left, right)) in original.iter().zip(&actual).enumerate() {
        assert_eq!(left, right, "genuine cursor record {index}");
    }
    assert_eq!((actual.len(), digest(&actual)), (20, 0x8A3846E1FFF37587));
}
#[test]
fn unregistered_names_keep_original_eager_values_and_errors() {
    let original = unknown_records(|e, c, r| e.eval_with_error_value(c, r));
    let actual = unknown_records(frame_value);
    assert_eq!(
        (original.len(), digest(&original)),
        (96, 0x636A12C8D9724869)
    );
    for (index, (left, right)) in original.iter().zip(&actual).enumerate() {
        assert_eq!(left, right, "unregistered name record {index}");
    }
    assert_eq!((actual.len(), digest(&actual)), (96, 0x636A12C8D9724869));
}

#[test]
fn public_row_contracts_match_frozen_results() {
    let metadata = (|| row_records(|e, c, r| e.eval_with_error_value(c, r)))();
    assert_eq!(
        (metadata.len(), digest(&metadata)),
        (288, 0x433BC944B34D725D)
    );
    let cursor = (|| cursor_records(|e, c, r| e.eval_with_error_value(c, r)))();
    assert_eq!((cursor.len(), digest(&cursor)), (20, 0x8A3846E1FFF37587));
    let unknown = (|| unknown_records(|e, c, r| e.eval_with_error_value(c, r)))();
    assert_eq!((unknown.len(), digest(&unknown)), (96, 0x636A12C8D9724869));
}
