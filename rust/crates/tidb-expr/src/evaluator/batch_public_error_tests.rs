// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Original public Constant Datum/error ownership, before frame error capture.
use super::*;

fn expression(kind: usize, order: i64, target: Option<FieldType>, cached: &Datum) -> Expression {
    let column = |index| {
        let mut c = Column::new(1, ty());
        c.index = index;
        Expression::Column(c)
    };
    let inner = |child| {
        let mut c = Constant::new(Datum::new_string("inner cached value"), ty());
        c.deferred_expr = Some(Box::new(child));
        Expression::Constant(c)
    };
    let mut c = Constant::new(cached.clone(), ty());
    c.ret_type = target;
    c.param_marker = (kind < 2).then_some(ParamMarker { order });
    c.deferred_expr = match kind {
        0 => None,
        1 | 2 => Some(Box::new(column(0))),
        3 => Some(Box::new(column(99))),
        4 => Some(Box::new(inner(column(99)))),
        5 => Some(Box::new(inner(column(0)))),
        _ => unreachable!(),
    };
    Expression::Constant(c)
}
fn error_records(
    mut evaluate: impl FnMut(
        &Expression,
        &leaf::Context,
        Row<'_>,
    ) -> std::result::Result<Datum, (Datum, EvalError)>,
) -> Vec<String> {
    let input = chunk(&[ty()], &[vec![Datum::Int(10)], vec![Datum::Null]]);
    let mut out = Vec::new();
    for kind in 0..6 {
        for (target_id, target) in [None, Some(ty()), Some(string_ty())]
            .into_iter()
            .enumerate()
        {
            for (cached_id, cached) in [
                Datum::new_string("outer cached planning value"),
                Datum::UInt(u64::MAX),
            ]
            .into_iter()
            .enumerate()
            {
                for order in if kind < 2 { vec![-1, 0, 1] } else { vec![0] } {
                    let expression = expression(kind, order, target.clone(), &cached);
                    for profile in [0, 1, 3] {
                        for (vi, value) in [
                            Datum::Null,
                            Datum::Int(7),
                            Datum::UInt(u64::MAX),
                            Datum::Real(1.5),
                            Datum::new_string("bad"),
                            Datum::Bytes(vec![0xff]),
                        ]
                        .into_iter()
                        .enumerate()
                        {
                            for row in [Row::empty(), input.physical_row(0), input.physical_row(1)]
                            {
                                let ctx = leaf::Context::new(profile, value.clone());
                                let observation =
                                    leaf::observe(|| evaluate(&expression, &ctx, row), &ctx);
                                out.push(format!("{kind}/{target_id}/{cached_id}/{order}/{profile}/{vi}/{}:{}|{observation}",row.is_empty(),row.idx()));
                            }
                        }
                    }
                }
            }
        }
    }
    out
}
#[test]
fn public_constant_original_error_values() {
    let records = error_records(|expression, ctx, row| expression.eval_with_error_value(ctx, row));
    eprintln!(
        "N51 original error ownership {} {:016X}",
        records.len(),
        digest(&records)
    );
    assert_eq!(
        (records.len(), digest(&records)),
        (3240, 0xFB8415D6D51EBFBD)
    );
}

#[test]
fn public_constant_shared_frame_keeps_original_error_values() {
    use super::super::super::scalar_source::BorrowedExpression;
    let original =
        error_records(|expression, context, row| expression.eval_with_error_value(context, row));
    assert_eq!(
        (original.len(), digest(&original)),
        (3240, 0xFB8415D6D51EBFBD)
    );
    let actual = error_records(|expression, context, row| {
        BorrowedExpression::compile(
            expression,
            row.len(),
            true,
            limits(),
            ExecutionLimits::default(),
            1 << 20,
        )
        .map_err(|error| (Datum::Null, error))
        .and_then(|mut plan| plan.eval_with_error_value(context, row))
        .map_err(|(value, error)| match error.into_evaluator_error() {
            EvaluatorError::Eval(error) => (value, error),
            other => panic!("unexpected {other:?}"),
        })
    });
    for (index, (want, got)) in original.iter().zip(&actual).enumerate() {
        assert_eq!(got, want, "original error Datum record {index}");
    }
    assert_eq!((actual.len(), digest(&actual)), (3240, 0xFB8415D6D51EBFBD));
}

#[test]
fn public_shared_frame_keeps_the_prior_public_error_pair_pin() {
    use super::super::super::scalar_source::BorrowedExpression;
    let actual = records(
        |expression, context, row| {
            BorrowedExpression::compile(
                expression,
                row.len(),
                true,
                limits(),
                ExecutionLimits::default(),
                1 << 20,
            )
            .map_err(|error| (Datum::Null, error))
            .and_then(|mut plan| plan.eval_with_error_value(context, row))
            .map_err(|(value, error)| match error.into_evaluator_error() {
                EvaluatorError::Eval(error) => (value, error),
                other => panic!("unexpected {other:?}"),
            })
        },
        false,
    );
    assert_eq!((actual.len(), digest(&actual)), (6720, 0x9DB38EEAA92DDE88));
}

#[test]
fn root_error_value_and_native_cause_are_consumed_together_for_the_actual_origin() {
    use crate::tikv::typed_dynamic_tests::Context as Script;
    use tidb_query_expr::local::{HostInvocation, HostStart, LocalHostServices};
    let expression = expression(1, 0, None, &Datum::new_string("cached"));
    let source = lower_source_plan(
        &expression,
        SourceInputs::PhysicalRow { width: 0 },
        true,
        1,
        limits(),
        Some(NativeView::RootValue),
        SourceConsumer::Scalar,
    )
    .unwrap();
    let LocalExpr::HostCall {
        slot,
        args,
        return_type,
    } = &source.expr
    else {
        panic!("actual root host")
    };
    assert!(args.is_empty());
    let store = RefCell::new(NativeValueStore::new().unwrap());
    store.borrow_mut().begin_window().unwrap();
    let context = Script::new(0, vec![Datum::Int(7), Datum::Null, Datum::Int(9)]);
    let mut provider = NativeBatchHosts::new(
        source.hosts.clone().unwrap(),
        &source.host_metadata,
        &context,
    )
    .with_native_values(Some(&store))
    .with_root_error_values(true);
    let row = InputRow {
        occurrence: 0,
        input_row: 0,
    };
    let origin = NativeValueOrigin { node: 0, row };
    let mut eval = EvalContext::default();
    let start = |provider: &mut NativeBatchHosts<'_>, eval: &mut EvalContext| {
        provider.start(
            eval,
            HostInvocation {
                slot: *slot,
                row,
                arg_types: &[],
                return_type,
            },
        )
    };
    assert!(start(&mut provider, &mut eval).is_err());
    // A failed invocation must be drained before another can read callbacks.
    assert!(matches!(
        start(&mut provider, &mut eval),
        Err(LocalError::BindingContract(_))
    ));
    assert_eq!(context.inner.calls.get(), 1);
    for wrong in [
        NativeValueOrigin { node: 1, row },
        NativeValueOrigin {
            node: 0,
            row: InputRow {
                occurrence: 1,
                input_row: 0,
            },
        },
        NativeValueOrigin {
            node: 0,
            row: InputRow {
                occurrence: 0,
                input_row: 1,
            },
        },
    ] {
        assert!(matches!(
            provider.take_native_failure_with_value(wrong),
            Err(LocalError::BindingContract(_))
        ));
    }
    let (error, value) = provider
        .take_native_failure_with_value(origin)
        .unwrap()
        .unwrap();
    assert_eq!(
        error,
        EvalError::Unsupported("deferred constant has no result type")
    );
    assert_eq!(value, Some(Datum::Int(7)));
    assert!(provider
        .take_native_failure_with_value(origin)
        .unwrap()
        .is_none());
    store.borrow_mut().begin_window().unwrap();
    let HostStart::Ready(VectorValue::Bytes(values)) = start(&mut provider, &mut eval).unwrap()
    else {
        panic!("NULL result")
    };
    assert_eq!(
        *store
            .borrow()
            .get(values.get(0).unwrap(), NativeValueRole::Output, origin)
            .unwrap(),
        Datum::Null
    );
    assert!(provider
        .take_native_failure_with_value(origin)
        .unwrap()
        .is_none());
    store.borrow_mut().begin_window().unwrap();
    assert!(start(&mut provider, &mut eval).is_err());
    let (_, value) = provider
        .take_native_failure_with_value(origin)
        .unwrap()
        .unwrap();
    assert_eq!(value, Some(Datum::Int(9)));
    assert_eq!(context.inner.calls.get(), 3);
}

#[test]
fn public_error_datum_keeps_the_original_opaque_cause_identity() {
    use super::super::super::scalar_source::BorrowedExpression;
    struct Cause {
        error: EvalError,
        calls: Cell<usize>,
    }
    impl Columns for Cause {
        fn get(&self, _: &[String]) -> Option<Datum> {
            panic!("no lookup")
        }
        fn param_value(&self, _: usize) -> std::result::Result<Datum, EvalError> {
            self.calls.set(self.calls.get() + 1);
            Err(self.error.clone())
        }
        fn enable_vectorized_expression(&self) -> bool {
            panic!("no vector hook")
        }
    }
    let cause =
        EvalError::ExpressionRuntimeFailure(crate::ExpressionRuntimeFailure::from_local_eval(
            LocalError::BindingContract("original opaque parameter failure".into()),
            None,
        ));
    for function in [false, true] {
        let parameter = expression(0, 0, None, &Datum::new_string("actual cached"));
        let expression = if function { abs(parameter) } else { parameter };
        let context = Cause {
            error: cause.clone(),
            calls: Cell::new(0),
        };
        let mut plan = BorrowedExpression::compile(
            &expression,
            0,
            true,
            limits(),
            ExecutionLimits::default(),
            64 << 10,
        )
        .unwrap();
        let (value, error) = plan
            .eval_with_error_value(&context, Row::empty())
            .unwrap_err();
        let EvaluatorError::Eval(error) = error.into_evaluator_error() else {
            panic!("native cause")
        };
        assert_eq!(error, cause);
        assert_eq!(
            value,
            if function {
                Datum::Null
            } else {
                Datum::new_string("actual cached")
            }
        );
        assert_eq!(context.calls.get(), 1);
    }
}

#[test]
fn public_error_pair_plan_recovers_from_unwind_and_bounded_failure_copies() {
    use super::super::super::scalar_source::BorrowedExpression;
    struct Live {
        value: RefCell<Datum>,
        calls: Cell<usize>,
        panic: Cell<bool>,
        read_error: Cell<bool>,
    }
    impl Columns for Live {
        fn get(&self, _: &[String]) -> Option<Datum> {
            panic!("no lookup")
        }
        fn param_value(&self, _: usize) -> std::result::Result<Datum, EvalError> {
            self.calls.set(self.calls.get() + 1);
            assert!(!self.panic.get(), "original parameter callback unwind");
            if self.read_error.get() {
                Err(EvalError::Unsupported("original read error"))
            } else {
                Ok(self.value.borrow().clone())
            }
        }
        fn enable_vectorized_expression(&self) -> bool {
            panic!("no vector hook")
        }
    }
    let context = Live {
        value: RefCell::new(Datum::Null),
        calls: Cell::new(0),
        panic: Cell::new(true),
        read_error: Cell::new(false),
    };
    let expression = expression(1, 0, None, &Datum::new_string("cached"));
    let mut plan = BorrowedExpression::compile(
        &expression,
        0,
        true,
        limits(),
        ExecutionLimits::default(),
        64 << 10,
    )
    .unwrap();
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(
        || plan.eval_with_error_value(&context, Row::empty())
    ))
    .is_err());
    context.panic.set(false);
    assert_eq!(
        plan.eval_with_error_value(&context, Row::empty()).unwrap(),
        Datum::Null
    );
    *context.value.borrow_mut() = Datum::Int(7);
    let (value, _) = plan
        .eval_with_error_value(&context, Row::empty())
        .unwrap_err();
    assert_eq!(value, Datum::Int(7));
    context.read_error.set(true);
    let (value, _) = plan
        .eval_with_error_value(&context, Row::empty())
        .unwrap_err();
    assert_eq!(value, Datum::new_string("cached"));
    context.read_error.set(false);
    *context.value.borrow_mut() = Datum::Bytes(vec![b'x'; 2 << 20]);
    let (value, error) = plan
        .eval_with_error_value(&context, Row::empty())
        .unwrap_err();
    assert_eq!(value, *context.value.borrow());
    assert_eq!(error.refusal(), BatchRefusal::Native);
    *context.value.borrow_mut() = Datum::Null;
    assert_eq!(
        plan.eval_with_error_value(&context, Row::empty()).unwrap(),
        Datum::Null
    );
    assert_eq!(context.calls.get(), 6);
}

#[test]
fn failure_datum_copy_limits_are_opt_in_and_do_not_copy_unused_cached_values() {
    use super::super::super::scalar_source::BorrowedExpression;
    let big = Datum::Bytes(vec![b'x'; 2 << 20]);
    let deferred = expression(1, 0, None, &Datum::new_string("cached"));
    let mut plan = BorrowedExpression::compile(
        &deferred,
        0,
        true,
        limits(),
        ExecutionLimits::default(),
        64 << 10,
    )
    .unwrap();
    let old = leaf::Context::new(0, big.clone());
    let expected = deferred.eval(&old, Row::empty()).unwrap_err();
    let ordinary = leaf::Context::new(0, big.clone());
    let EvaluatorError::Eval(error) = plan
        .eval(&ordinary, Row::empty())
        .unwrap_err()
        .into_evaluator_error()
    else {
        panic!("ordinary error")
    };
    assert_eq!(error, expected);
    assert_eq!(ordinary.calls.get(), 1);
    let captured = leaf::Context::new(0, big.clone());
    let (value, error) = plan
        .eval_with_error_value(&captured, Row::empty())
        .unwrap_err();
    assert_eq!(value, big);
    assert_eq!(error.refusal(), BatchRefusal::Native);
    assert_eq!(captured.calls.get(), 1);

    // Cached parameter snapshots are not copied by source compilation or the
    // ordinary error consumer. The demanded failure value keeps its native error.
    let cached = expression(0, 0, None, &big);
    let mut plan = BorrowedExpression::compile(
        &cached,
        0,
        true,
        limits(),
        ExecutionLimits::default(),
        64 << 10,
    )
    .unwrap();
    let ordinary = leaf::Context::new(1, Datum::Null);
    let EvaluatorError::Eval(error) = plan
        .eval(&ordinary, Row::empty())
        .unwrap_err()
        .into_evaluator_error()
    else {
        panic!("ordinary cached error")
    };
    assert_eq!(
        error,
        EvalError::Unsupported("dynamic parameter unavailable")
    );
    let captured = leaf::Context::new(1, Datum::Null);
    let (value, error) = plan
        .eval_with_error_value(&captured, Row::empty())
        .unwrap_err();
    assert_eq!(value, big);
    assert_eq!(error.refusal(), BatchRefusal::Native);
    assert_eq!(captured.calls.get(), 1);
}

#[test]
fn public_error_datum_contracts_match_frozen_results() {
    let actual = (|| error_records(|e, c, r| e.eval_with_error_value(c, r)))();
    assert_eq!((actual.len(), digest(&actual)), (3240, 0xFB8415D6D51EBFBD));
}
