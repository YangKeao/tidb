// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Original legacy bulk consumers coexist with the selected scalar PB body.
use super::*;

fn declaration(code: i32, unsigned: bool) -> wire::FieldType {
    let mut ft = field(code, -46);
    ft.flag = Some(if unsigned { 32 } else { 0 });
    if code == 8 {
        ft.flen = Some(20);
        ft.decimal = Some(0);
    }
    if code == 246 {
        ft.flen = Some(65);
        ft.decimal = Some(5);
    }
    ft
}
fn make(
    name: &str,
    result: i32,
    source: i32,
    unsigned: bool,
    shape: usize,
    value: &Datum,
) -> EvaluatorSuite {
    let stored = Datum::Bytes(b"stored".to_vec());
    let mut arg = atom(
        if shape == 0 { value } else { &stored },
        Some(declaration(source, unsigned)),
    );
    if shape == 3 {
        let mut bytes = vec![];
        tidb_codec::encode_int(&mut bytes, 0);
        arg = wire::Expr {
            tp: Some(wire::ExprType::ColumnRef as i32),
            val: Some(bytes),
            field_type: Some(declaration(source, unsigned)),
            ..Default::default()
        };
    }
    let mut root = crate::distsql_builtin::pb_to_expr(
        &pb_call(vec![arg], Some(declaration(result, unsigned))),
        &[string_ty()],
    )
    .unwrap();
    let Expression::ScalarFunction(f) = &mut root else {
        unreachable!()
    };
    f.func_name = CiString::new(name);
    if matches!(shape, 1 | 2) {
        let Expression::Constant(c) = &mut f.args[0] else {
            unreachable!()
        };
        if shape == 1 {
            c.param_marker = Some(crate::constant::ParamMarker { order: 0 });
        } else {
            c.deferred_expr = Some(Box::new(call(
                "setvar",
                string_ty(),
                vec![
                    literal(Datum::new_string("legacy-unary"), string_ty()),
                    parameter(0, string_ty()),
                ],
            )));
        }
    }
    suite(root)
}
#[test]
fn pb_unary_source_original_phases() {
    if isolate_common_corpus(
        "stateful_tests::pb_regexp_tests::unary_tests::pb_unary_source_original_phases",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    matrix(false, false);
}
#[test]
fn pb_unary_source_actual_phases() {
    if isolate_common_corpus(
        "stateful_tests::pb_regexp_tests::unary_tests::pb_unary_source_actual_phases",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    matrix(true, false);
}
#[test]
fn pb_unary_source_original_full() {
    if isolate_common_corpus(
        "stateful_tests::pb_regexp_tests::unary_tests::pb_unary_source_original_full",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    matrix(false, true);
}
#[test]
fn pb_unary_source_actual_full() {
    if isolate_common_corpus(
        "stateful_tests::pb_regexp_tests::unary_tests::pb_unary_source_actual_full",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    matrix(true, true);
}
fn matrix(compare: bool, full: bool) {
    let now = chrono::DateTime::parse_from_rfc3339("2026-10-21T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    tidb_datatype::with_test_conversion_clock(now, || {
        let values = if full {
            crate::tikv::byte_unary_value_tests::values()
        } else {
            vec![
                Datum::Null,
                Datum::Int(0),
                Datum::Int(-4),
                Datum::Int(i64::MIN),
                Datum::new_string("4"),
                Datum::new_string("bad"),
                Datum::Bytes(vec![0xff]),
                Datum::new_string("{\"a\":1}"),
            ]
        };
        let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
        let mut gaps = vec![];
        for (name, result) in [("abs", 8), ("cast_decimal", 246), ("cast_json", 245)] {
            for source in [8, 253, 5, 246, 245, 12, 11] {
                for unsigned in [false, true] {
                    for shape in if full { vec![1] } else { vec![0, 1, 2, 3] } {
                        if shape == 3 && source != 253 {
                            continue;
                        }
                        for (vi, value) in values.iter().enumerate() {
                            for profile in if full { vec![0] } else { vec![0, 2, 3, 6, 7] } {
                                for enabled in [false, true] {
                                    for selection in [None, Some(vec![2, 0, 2]), Some(vec![])] {
                                        let original =
                                            make(name, result, source, unsigned, shape, value);
                                        let actual =
                                            make(name, result, source, unsigned, shape, value);
                                        // Initial closure is the actual original eligible bulk profile.
                                        if crate::scalar_function::static_batch_target(
                                            &original.program.calculated[0],
                                        )
                                        .is_none()
                                        {
                                            continue;
                                        }
                                        let ty = output_type(&original);
                                        let args = [value.clone(), Datum::Null, Datum::Null];
                                        let old = RegexpSourceContext::new(
                                            enabled,
                                            args.clone(),
                                            profile,
                                        );
                                        let new = RegexpSourceContext::new(enabled, args, profile);
                                        for reuse in 0..2 {
                                            let label=format!("pb-unary/{full}/{name}/{source}/{unsigned}/{shape}/{vi}/{profile}/{enabled}/{selection:?}/{reuse}");
                                            let mut a = input(&selection);
                                            let mut left = output_for(&ty);
                                            left.append_null(0);
                                            let expected = original_session_read_outcome(|| {
                                                original.run(&old, &mut a, &mut left)
                                            });
                                            if compare {
                                                let mut b = input(&selection);
                                                let mut right = output_for(&ty);
                                                right.append_null(0);
                                                let got = session_read_outcome(|| {
                                                    actual.run_with_tikv_batch(
                                                        &new,
                                                        &mut b,
                                                        &mut right,
                                                        &[string_ty()],
                                                    )
                                                });
                                                if got != expected
                                                    || left.column(0) != right.column(0)
                                                    || old.snapshot() != new.snapshot()
                                                {
                                                    eprintln!("UNARY DIFFERENCE {label} old={expected:?} new={got:?} left={:?} right={:?} oldevents={} newevents={}",left.column(0),right.column(0),old.snapshot(),new.snapshot());
                                                }
                                                assert_eq!(got, expected, "{label}");
                                                assert_eq!(
                                                    right.column(0),
                                                    left.column(0),
                                                    "{label}"
                                                );
                                                assert_eq!(
                                                    new.snapshot(),
                                                    old.snapshot(),
                                                    "{label}"
                                                );
                                                let route = actual.batch_route_snapshot();
                                                if (route.prepared == 0
                                                    || route.fallback != 0
                                                    || route.refusals != 0)
                                                    && gaps.len() < 8
                                                {
                                                    gaps.push(format!("{label}:{route:?}"));
                                                }
                                            }
                                            observe(
                                                &mut hash,
                                                &format!(
                                                    "{label}|{expected}|{:?}|{}",
                                                    left.column(0),
                                                    old.snapshot()
                                                ),
                                            );
                                            count += 1;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        eprintln!("PB UNARY ORIGINAL full={full} {count}/{hash:016X}");
        assert_eq!(
            (count, hash),
            if full {
                (30396, 0x2B62F258D6BD863E)
            } else {
                (17100, 0xE360D79CBFDEF4DA)
            }
        );
        if !gaps.is_empty() {
            eprintln!("PB UNARY SOURCE RED {gaps:?}");
        }
        assert!(gaps.is_empty());
    });
}

#[test]
fn pb_unary_source_original_long() {
    if isolate_common_corpus(
        "stateful_tests::pb_regexp_tests::unary_tests::pb_unary_source_original_long",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    long(false);
}
#[test]
fn pb_unary_source_actual_long() {
    if isolate_common_corpus(
        "stateful_tests::pb_regexp_tests::unary_tests::pb_unary_source_actual_long",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    long(true);
}
fn long(compare: bool) {
    let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
    let mut gaps = vec![];
    for (name, result, source) in [
        ("abs", 8, 8),
        ("cast_decimal", 246, 8),
        ("cast_json", 245, 253),
    ] {
        for shape in [1, 2] {
            for unsigned in [false, true] {
                for profile in [0, 3, 5, 7] {
                    let original = make(name, result, source, unsigned, shape, &Datum::Int(4));
                    let actual = make(name, result, source, unsigned, shape, &Datum::Int(4));
                    // Deferred side effects can disable bulk eligibility; retain
                    // their original ordered scalar header as a control.
                    let ty = output_type(&original);
                    let args = [Datum::Int(4), Datum::Null, Datum::Null];
                    let old = RegexpSourceContext::new(false, args.clone(), profile);
                    let new = RegexpSourceContext::new(false, args, profile);
                    for rows in [0, 1, 3, 1026, 2051] {
                        for (round, enabled) in [false, true, true, false].into_iter().enumerate() {
                            old.base.enabled.set(enabled);
                            new.base.enabled.set(enabled);
                            old.id.set(if round == 2 { 18 } else { 17 });
                            new.id.set(old.id.get());
                            let selection = if rows > 1 {
                                Some(
                                    (0..rows)
                                        .map(|i| (rows - 1 - i / 2) % rows)
                                        .collect::<Vec<_>>(),
                                )
                            } else {
                                None
                            };
                            let data = (0..rows)
                                .map(|_| vec![Datum::new_string("4")])
                                .collect::<Vec<_>>();
                            for reuse in 0..2 {
                                let label=format!("pb-unary-long/{name}/{shape}/{unsigned}/{profile}/{rows}/{round}/{reuse}");
                                let mut a = chunk(&[string_ty()], &data);
                                a.set_sel(selection.clone());
                                let mut left = output_for(&ty);
                                left.append_null(0);
                                let expected = original_session_read_outcome(|| {
                                    original.run(&old, &mut a, &mut left)
                                });
                                if compare {
                                    let mut b = chunk(&[string_ty()], &data);
                                    b.set_sel(selection.clone());
                                    let mut right = output_for(&ty);
                                    right.append_null(0);
                                    let got = session_read_outcome(|| {
                                        actual.run_with_tikv_batch(
                                            &new,
                                            &mut b,
                                            &mut right,
                                            &[string_ty()],
                                        )
                                    });
                                    if got != expected
                                        || left.column(0) != right.column(0)
                                        || old.snapshot() != new.snapshot()
                                    {
                                        eprintln!("UNARY LONG DIFFERENCE {label} old={expected:?} new={got:?} oldevents={} newevents={}",old.snapshot(),new.snapshot());
                                    }
                                    assert_eq!(got, expected, "{label}");
                                    assert_eq!(right.column(0), left.column(0), "{label}");
                                    assert_eq!(new.snapshot(), old.snapshot(), "{label}");
                                    let route = actual.batch_route_snapshot();
                                    if (route.prepared == 0
                                        || route.fallback != 0
                                        || route.refusals != 0)
                                        && gaps.len() < 8
                                    {
                                        gaps.push(format!("{label}:{route:?}"));
                                    }
                                }
                                observe(
                                    &mut hash,
                                    &format!(
                                        "{label}|{expected}|{:?}|{}",
                                        left.column(0),
                                        old.snapshot()
                                    ),
                                );
                                count += 1;
                            }
                        }
                    }
                }
            }
        }
    }
    eprintln!("PB UNARY LONG ORIGINAL {count}/{hash:016X}");
    assert_eq!((count, hash), (1920, 0xFFE2C6F976CF6CD5));
    if !gaps.is_empty() {
        eprintln!("PB UNARY LONG SOURCE RED {gaps:?}");
    }
    assert!(gaps.is_empty());
}

#[test]
fn pb_unary_source_direct_profiles_and_same_cap_recovery() {
    for (name, result, source) in [
        ("abs", 8, 8),
        ("cast_decimal", 246, 8),
        ("cast_json", 245, 253),
    ] {
        let owner = make(name, result, source, false, 1, &Datum::Int(4));
        let ty = output_type(&owner);
        let mut direct = PreparedBatch::compile(
            &owner,
            &[string_ty()],
            true,
            942,
            limits(),
            ExecutionLimits::default(),
            1 << 20,
        )
        .unwrap();
        assert!(
            direct.row_profile.is_some(),
            "{name}: genuine bulk and scalar profiles"
        );
        for enabled in [true] {
            let args = [Datum::Int(4), Datum::Null, Datum::Null];
            let old = RegexpSourceContext::new(enabled, args.clone(), 0);
            let new = RegexpSourceContext::new(enabled, args, 0);
            let mut a = input(&Some(vec![2, 0, 2]));
            let mut left = output_for(&ty);
            left.append_null(0);
            let expected = original_session_read_outcome(|| {
                owner.run_with_consumer(
                    &old,
                    &mut a,
                    &mut left,
                    &mut crate::evaluator::NativeBatchConsumer,
                )
            });
            let mut b = input(&Some(vec![2, 0, 2]));
            let mut right = output_for(&ty);
            right.append_null(0);
            let got = session_read_outcome(|| {
                owner
                    .run_batch(
                        &new,
                        &mut EvalContext::default(),
                        &mut direct,
                        &mut b,
                        &mut right,
                        &[string_ty()],
                    )
                    .map_err(BatchFailure::into_evaluator_error)
            });
            assert_eq!(got, expected, "{name}/{enabled}");
            assert_eq!(right.column(0), left.column(0));
            assert_eq!(new.snapshot(), old.snapshot(), "{name}/{enabled}");
        }
        let mut prepared = PreparedBatchSuite::compile(
            &owner,
            &[string_ty()],
            true,
            942,
            limits(),
            ExecutionLimits::default(),
            1 << 20,
        )
        .unwrap();

        let bad = input(&Some(vec![3; 2051]));
        let mut prefix = output_for(&ty);
        prefix.append_null(0);
        let failure = prepared.workers[0]
            .validate_invocation(&owner, &bad, &prefix, &[string_ty()])
            .unwrap_err();
        assert!(
            matches!(failure.local_error(), Some(LocalError::InvalidBatch(_))),
            "physical bounds retain priority"
        );
        assert_eq!(prefix.num_rows(), 1);
        assert_eq!(prepared.workers[0].selection.capacity(), 0);
        let mut ctx = RegexpSourceContext::new(true, [Datum::Int(4), Datum::Null, Datum::Null], 0);
        // A large real selection executes normally without a Demo storage cap.
        let mut a = input(&Some(vec![0; 2051]));
        let mut output = output_for(&ty);
        output.append_null(0);
        let native = RegexpSourceContext::new(true, ctx.base.values.clone(), 0);
        let mut original_input = input(&Some(vec![0; 2051]));
        let mut original_output = output_for(&ty);
        original_output.append_null(0);
        let expected = original_session_read_outcome(|| {
            owner.run(&native, &mut original_input, &mut original_output)
        });
        let got = session_read_outcome(|| {
            prepared
                .run(
                    &owner,
                    &ctx,
                    &mut EvalContext::default(),
                    &mut a,
                    &mut output,
                    &[string_ty()],
                )
                .map_err(BatchFailure::into_evaluator_error)
        });
        assert_eq!(got, expected, "large unary/{name}");
        assert_eq!(output.column(0), original_output.column(0));
        assert_eq!(ctx.snapshot(), native.snapshot());
        assert_eq!(
            prepared.workers[0].selection.capacity(),
            0,
            "physical selection is shared"
        );
        for value in [Datum::Null, Datum::Int(4)] {
            ctx.base.values[0] = value;
            ctx.base.events.borrow_mut().clear();
            ctx.context_reads.set(0);
            for n in &ctx.base.reads {
                n.set(0);
            }
            let old = RegexpSourceContext::new(true, ctx.base.values.clone(), 0);
            let mut a = input(&None);
            let mut left = output_for(&ty);
            left.append_null(0);
            let expected = original_session_read_outcome(|| owner.run(&old, &mut a, &mut left));
            let mut b = input(&None);
            let mut right = output_for(&ty);
            right.append_null(0);
            let got = session_read_outcome(|| {
                prepared
                    .run(
                        &owner,
                        &ctx,
                        &mut EvalContext::default(),
                        &mut b,
                        &mut right,
                        &[string_ty()],
                    )
                    .map_err(BatchFailure::into_evaluator_error)
            });
            assert_eq!(got, expected, "{name}/recovery");
            assert_eq!(right.column(0), left.column(0));
            assert_eq!(ctx.snapshot(), old.snapshot(), "{name}/recovery");
        }
    }
}

#[test]
fn pb_unary_source_scope_callback_auth_and_cleanup() {
    use tidb_query_datatype::codec::data_type::ScalarValueRef;
    use tidb_query_expr::local::{
        HostStart, HostStep, HostWindowArgReply, HostWindowInvocation, LocalHostServices,
    };
    let owner = make("abs", 8, 8, false, 0, &Datum::Int(4));
    let compiled = PreparedBatch::compile(
        &owner,
        &[string_ty()],
        true,
        942,
        limits(),
        ExecutionLimits::default(),
        1 << 20,
    )
    .unwrap();
    let source = &compiled.source;
    let NativeHostMetadata::UnaryWindow {
        argument_node,
        output_node,
        output_role,
        ..
    } = &source.host_metadata[0]
    else {
        panic!("actual unary window")
    };
    let catalog = source.hosts.as_ref().unwrap().clone();
    let slot = catalog.slot(0).unwrap();
    let sig = catalog.signature(slot).unwrap();
    struct C<'a> {
        store: &'a RefCell<NativeValueStore>,
        cache: crate::ReadyValueCache,
        mode: usize,
        calls: Cell<usize>,
    }
    impl Columns for C<'_> {
        fn get(&self, _: &[String]) -> Option<Datum> {
            panic!("no expression callback")
        }
        fn ready_value_cache(&self) -> Option<&crate::ReadyValueCache> {
            drop(self.store.borrow_mut());
            self.calls.set(self.calls.get() + 1);
            if self.mode == 1 {
                self.store.borrow_mut().begin_window().unwrap();
            }
            if self.mode == 2 {
                panic!("original ABS scope callback");
            }
            Some(&self.cache)
        }
    }
    for mode in 0..3 {
        let store = RefCell::new(NativeValueStore::new().unwrap());
        store.borrow_mut().begin_window().unwrap();
        let ctx = C {
            store: &store,
            cache: crate::ReadyValueCache::new(),
            mode,
            calls: Cell::new(0),
        };
        let mut hosts = NativeBatchHosts::new(catalog.clone(), &source.host_metadata, &ctx)
            .with_native_values(Some(&store))
            .with_constant_probes(&source.host_probes);
        let mut eval = EvalContext::default();
        let row = InputRow {
            occurrence: 7,
            input_row: 0,
        };
        let HostStart::Pending { task, .. } = hosts
            .start_window(
                &mut eval,
                HostWindowInvocation {
                    slot,
                    occurrence_start: 7,
                    physical_rows: &[0],
                    arg_types: &sig.arg_types,
                    return_type: &sig.return_type,
                },
            )
            .unwrap()
        else {
            panic!("pending unary")
        };
        let token = store
            .borrow_mut()
            .insert_clone(
                &Datum::Int(4),
                NativeValueRole::CharInput,
                NativeValueOrigin {
                    node: *argument_node,
                    row,
                },
            )
            .unwrap();
        let values = VectorValue::from_scalar(&ScalarValue::Bytes(Some(token.to_vec())), 1);
        struct Cancel<'a, 'b> {
            hosts: &'a mut NativeBatchHosts<'b>,
            task: &'a tidb_query_expr::local::HostTaskId,
        }
        impl Drop for Cancel<'_, '_> {
            fn drop(&mut self) {
                self.hosts.cancel(self.task);
            }
        }
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let guard = Cancel {
                hosts: &mut hosts,
                task: &task,
            };
            guard.hosts.resume_window(
                &mut eval,
                &task,
                HostWindowArgReply {
                    index: 0,
                    occurrence_start: 7,
                    physical_rows: &[0],
                    field_type: &sig.arg_types[0],
                    values: &values,
                },
            )
        }));
        assert_eq!(ctx.calls.get(), 1, "mode{mode}");
        match mode {
            0 => {
                let HostStep::Ready(value) = result.unwrap().unwrap() else {
                    panic!("result")
                };
                let ScalarValueRef::Bytes(Some(token)) = value.get_scalar_ref(0) else {
                    panic!("token")
                };
                assert_eq!(
                    store
                        .borrow()
                        .get(
                            token,
                            *output_role,
                            NativeValueOrigin {
                                node: *output_node,
                                row
                            }
                        )
                        .unwrap(),
                    &Datum::Int(4)
                );
            }
            1 => assert!(
                matches!(result.unwrap(), Err(LocalError::BindingContract(_))),
                "callback must invalidate original input capability"
            ),
            _ => assert!(result.is_err()),
        }
        assert!(hosts
            .resume_window(
                &mut eval,
                &task,
                HostWindowArgReply {
                    index: 0,
                    occurrence_start: 7,
                    physical_rows: &[0],
                    field_type: &sig.arg_types[0],
                    values: &values,
                }
            )
            .is_err());
        assert!(tidb_query_expr::local::value_kernel_work_remaining().is_none());
    }
}
