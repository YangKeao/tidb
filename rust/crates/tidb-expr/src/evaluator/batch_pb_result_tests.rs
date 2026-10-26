// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! PB integer carriers are interpreted before the original result conversion.
use super::*;

fn declaration(code: i32, flags: Option<u32>) -> wire::FieldType {
    let mut ft = field(code, -46);
    ft.flag = flags;
    match code {
        8 => {
            ft.flen = Some(20);
            ft.decimal = Some(0);
        }
        246 => {
            ft.flen = Some(65);
            ft.decimal = Some(5);
        }
        13 => {
            ft.flen = Some(4);
            ft.decimal = Some(0);
        }
        16 => ft.flen = Some(8),
        247 | 248 => ft.elems = vec!["zero".into(), "one".into()],
        _ => (),
    };
    ft
}
fn make(
    arity: usize,
    source: i32,
    result: i32,
    flags: Option<u32>,
    display: &str,
    shape: usize,
    values: &[Datum; 3],
) -> EvaluatorSuite {
    let args = (0..arity)
        .map(|i| {
            atom(
                if shape == 0 { &values[i] } else { &Datum::Null },
                Some(declaration(source, Some(0))),
            )
        })
        .collect();
    let pb = pb_call(args, Some(declaration(result, flags)));
    let mut root = crate::distsql_builtin::pb_to_expr(&pb, &[]).unwrap();
    let Expression::ScalarFunction(f) = &mut root else {
        unreachable!()
    };
    f.func_name = CiString::new(display);
    for (i, arg) in f.args.iter_mut().enumerate() {
        let Expression::Constant(c) = arg else {
            unreachable!()
        };
        if shape == 1 {
            c.param_marker = Some(crate::constant::ParamMarker { order: i as i64 });
        }
        if shape == 2 {
            c.deferred_expr = Some(Box::new(call(
                "setvar",
                string_ty(),
                vec![
                    literal(Datum::new_string(format!("result-{i}")), string_ty()),
                    parameter(i, string_ty()),
                ],
            )));
        }
    }
    suite(root)
}
#[test]
fn pb_result_source_original_phases() {
    if isolate_common_corpus(
        "stateful_tests::pb_regexp_tests::result_tests::pb_result_source_original_phases",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    matrix(false, false);
}
#[test]
fn pb_result_source_actual_phases() {
    if isolate_common_corpus(
        "stateful_tests::pb_regexp_tests::result_tests::pb_result_source_actual_phases",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    matrix(true, false);
}
#[test]
fn pb_result_source_original_full() {
    if isolate_common_corpus(
        "stateful_tests::pb_regexp_tests::result_tests::pb_result_source_original_full",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    matrix(false, true);
}
#[test]
fn pb_result_source_actual_full() {
    if isolate_common_corpus(
        "stateful_tests::pb_regexp_tests::result_tests::pb_result_source_actual_full",
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
        let sources = if full {
            vec![8, 253, 5]
        } else {
            vec![8, 253, 5, 245]
        };
        let results = if full {
            vec![245, 13, 8]
        } else {
            vec![8, 5, 246, 245, 253, 6, 13, 16, 247, 248]
        };
        let flags = if full {
            vec![Some(0), Some(32)]
        } else {
            vec![None, Some(0), Some(32), Some(33)]
        };
        let displays = if full {
            vec!["wire display", "getvar"]
        } else {
            vec!["wire display", "regexp_like", "getvar", "ifnull"]
        };
        let samples = if full {
            crate::tikv::byte_unary_value_tests::values()
                .into_iter()
                .enumerate()
                .flat_map(|(vi, value)| {
                    (0..3).map(move |position| {
                        let mut values = [
                            Datum::new_string("abc"),
                            Datum::new_string("^a"),
                            Datum::new_string(""),
                        ];
                        values[position] = value.clone();
                        (vi * 3 + position, position, values)
                    })
                })
                .collect::<Vec<_>>()
        } else {
            vec![
                (
                    0,
                    0,
                    [
                        Datum::new_string("abc"),
                        Datum::new_string("^a"),
                        Datum::new_string(""),
                    ],
                ),
                (
                    1,
                    0,
                    [
                        Datum::new_string("abc"),
                        Datum::new_string("^z"),
                        Datum::new_string("c"),
                    ],
                ),
                (
                    2,
                    0,
                    [
                        Datum::Null,
                        Datum::Bytes(vec![255]),
                        Datum::Bytes(vec![255]),
                    ],
                ),
                (
                    3,
                    0,
                    [
                        Datum::Bytes(vec![255]),
                        Datum::Null,
                        Datum::Bytes(vec![255]),
                    ],
                ),
                (
                    4,
                    0,
                    [
                        Datum::Int(1),
                        Datum::new_string("1"),
                        Datum::new_string("i"),
                    ],
                ),
                (
                    5,
                    0,
                    [
                        Datum::new_string("abc"),
                        Datum::new_string("("),
                        Datum::new_string(""),
                    ],
                ),
                (
                    6,
                    0,
                    [
                        Datum::new_string("ABC"),
                        Datum::new_string("a"),
                        Datum::new_string("i"),
                    ],
                ),
                (
                    7,
                    0,
                    [
                        Datum::new_string("ABC"),
                        Datum::new_string("a"),
                        Datum::new_string("z"),
                    ],
                ),
            ]
        };
        let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
        let mut gaps = vec![];
        for arity in [2, 3] {
            for &source in &sources {
                for &result in &results {
                    for &flags in &flags {
                        for &display in &displays {
                            for shape in if full { vec![1] } else { vec![0, 1, 2] } {
                                for (vi, position, values) in &samples {
                                    if full && *position >= arity {
                                        continue;
                                    }
                                    for profile in if full { vec![0] } else { vec![0, 1, 6, 7] } {
                                        for enabled in [false, true] {
                                            for selection in
                                                [None, Some(vec![2, 0, 2]), Some(vec![])]
                                            {
                                                let original = make(
                                                    arity, source, result, flags, display, shape,
                                                    values,
                                                );
                                                let actual = make(
                                                    arity, source, result, flags, display, shape,
                                                    values,
                                                );
                                                let ty = output_type(&original);
                                                let old = RegexpSourceContext::new(
                                                    enabled,
                                                    values.clone(),
                                                    profile,
                                                );
                                                let new = RegexpSourceContext::new(
                                                    enabled,
                                                    values.clone(),
                                                    profile,
                                                );
                                                for reuse in 0..2 {
                                                    let mut a = input(&selection);
                                                    let mut left = output_for(&ty);
                                                    left.append_null(0);
                                                    let expected =
                                                        original_session_read_outcome(|| {
                                                            original.run(&old, &mut a, &mut left)
                                                        });
                                                    let label=format!("pb-result/{full}/{arity}/{source}/{result}/{flags:?}/{display}/{shape}/{vi}/{profile}/{enabled}/{selection:?}/{reuse}");
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
                                                            eprintln!("PB RESULT DIFFERENCE {label} old={expected:?} new={got:?} oldcol={:?} newcol={:?} oldevents={} newevents={}",left.column(0),right.column(0),old.snapshot(),new.snapshot());
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
                                                        if (route.fallback != 0
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
            }
        }
        eprintln!("PB RESULT ORIGINAL full={full} {count}/{hash:016X}");
        assert_eq!(
            (count, hash),
            if full {
                (321840, 0xC125E5697C81E883)
            } else {
                (1474560, 0x36AB4F4C21EA7B05)
            }
        );
        if !gaps.is_empty() {
            eprintln!("PB RESULT SOURCE RED {gaps:?}");
        }
        assert!(gaps.is_empty());
    });
}
#[test]
fn pb_result_source_original_long() {
    if isolate_common_corpus(
        "stateful_tests::pb_regexp_tests::result_tests::pb_result_source_original_long",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    long(false);
}
#[test]
fn pb_result_source_actual_long() {
    if isolate_common_corpus(
        "stateful_tests::pb_regexp_tests::result_tests::pb_result_source_actual_long",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    long(true);
}
fn long(compare: bool) {
    let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
    for arity in [2, 3] {
        for result in [245, 13] {
            for flags in [None, Some(0), Some(32), Some(33)] {
                for profile in [0, 1, 6, 7] {
                    for rows in [0usize, 1, 3, 1026, 2051] {
                        for first in [false, true] {
                            let values = [
                                Datum::new_string("abc"),
                                Datum::new_string("^a"),
                                Datum::new_string(""),
                            ];
                            let original =
                                make(arity, 253, result, flags, "wire display", 1, &values);
                            let actual =
                                make(arity, 253, result, flags, "wire display", 1, &values);
                            let ty = output_type(&original);
                            let mut old = RegexpSourceContext::new(first, values.clone(), profile);
                            let mut new = RegexpSourceContext::new(first, values, profile);
                            for (round, enabled) in
                                [first, !first, first, !first].into_iter().enumerate()
                            {
                                old.base.enabled.set(enabled);
                                new.base.enabled.set(enabled);
                                old.id.set(if round == 2 { 18 } else { 17 });
                                new.id.set(old.id.get());
                                old.base.values[1] =
                                    Datum::new_string(if round == 1 { "[" } else { "^a" });
                                new.base.values[1] = old.base.values[1].clone();
                                let selection = Some((0..rows).map(|i| [2, 0, 2][i % 3]).collect());
                                let mut a = input(&selection);
                                let mut left = output_for(&ty);
                                left.append_null(0);
                                let expected = original_session_read_outcome(|| {
                                    original.run(&old, &mut a, &mut left)
                                });
                                let label=format!("pb-result-long/{arity}/{result}/{flags:?}/{profile}/{rows}/{first}/{round}");
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
                                        eprintln!("PB RESULT LONG DIFFERENCE {label} old={expected:?} new={got:?} oldcol={:?} newcol={:?} oldevents={} newevents={}",left.column(0),right.column(0),old.snapshot(),new.snapshot());
                                    }
                                    assert_eq!(got, expected, "{label}");
                                    assert_eq!(right.column(0), left.column(0), "{label}");
                                    assert_eq!(new.snapshot(), old.snapshot(), "{label}");
                                    let route = actual.batch_route_snapshot();
                                    assert_eq!(
                                        (route.fallback, route.refusals),
                                        (0, 0),
                                        "{label}/{route:?}"
                                    );
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
    eprintln!("PB RESULT LONG ORIGINAL {count}/{hash:016X}");
    assert_eq!((count, hash), (2560, 0x4CF8E7A778781BF1));
}

fn effective_parameter_owner(
    arity: usize,
    source: i32,
    result: i32,
    flags: Option<u32>,
    display: &str,
) -> EvaluatorSuite {
    let args = (0..arity)
        .map(|_| {
            atom(
                &Datum::new_string("real wire value"),
                Some(declaration(source, Some(0))),
            )
        })
        .collect();
    let pb = pb_call(args, Some(declaration(result, flags)));
    let mut root = crate::distsql_builtin::pb_to_expr(&pb, &[]).unwrap();
    let Expression::ScalarFunction(f) = &mut root else {
        unreachable!()
    };
    f.func_name = CiString::new(display);
    for (i, arg) in f.args.iter_mut().enumerate() {
        let Expression::Constant(c) = arg else {
            unreachable!()
        };
        c.param_marker = Some(crate::constant::ParamMarker { order: i as i64 });
    }
    suite(root)
}
#[test]
fn pb_result_source_ordinary_controls_and_same_cap_recovery() {
    if isolate_common_corpus("stateful_tests::pb_regexp_tests::result_tests::pb_result_source_ordinary_controls_and_same_cap_recovery"){return;}
    std::panic::set_hook(Box::new(|_| {}));
    let mut count = 0;
    for (name, arity) in [("regexp", 2), ("regexp_like", 2), ("regexp_like", 3)] {
        for result in [8, 5, 246, 245, 253, 6, 13, 16, 247, 248] {
            for flags in [Some(0), Some(32), Some(33)] {
                for values in [
                    [
                        Datum::new_string("abc"),
                        Datum::new_string("^a"),
                        Datum::new_string(""),
                    ],
                    [
                        Datum::new_string("abc"),
                        Datum::new_string("^z"),
                        Datum::new_string("c"),
                    ],
                    [
                        Datum::Null,
                        Datum::Bytes(vec![255]),
                        Datum::Bytes(vec![255]),
                    ],
                    [Datum::Bytes(vec![255]), Datum::Null, Datum::new_string("")],
                ] {
                    for enabled in [false, true] {
                        for selection in [None, Some(vec![2, 0, 2]), Some(vec![])] {
                            let field = crate::distsql_builtin::pb_type_to_field_type(
                                &declaration(result, flags),
                            );
                            let build = || {
                                suite(call(
                                    name,
                                    field.clone(),
                                    (0..arity).map(|i| parameter(i, string_ty())).collect(),
                                ))
                            };
                            let original = build();
                            let actual = build();
                            let old = RegexpSourceContext::new(enabled, values.clone(), 0);
                            let new = RegexpSourceContext::new(enabled, values.clone(), 0);
                            for reuse in 0..2 {
                                let mut a = input(&selection);
                                let mut left = output_for(&field);
                                left.append_null(0);
                                let expected = original_session_read_outcome(|| {
                                    original.run(&old, &mut a, &mut left)
                                });
                                let mut b = input(&selection);
                                let mut right = output_for(&field);
                                right.append_null(0);
                                let got = session_read_outcome(|| {
                                    actual.run_with_tikv_batch(
                                        &new,
                                        &mut b,
                                        &mut right,
                                        &[string_ty()],
                                    )
                                });
                                let label=format!("ordinary/{name}/{result}/{flags:?}/{enabled}/{selection:?}/{reuse}");
                                if got != expected
                                    || right.column(0) != left.column(0)
                                    || new.snapshot() != old.snapshot()
                                {
                                    eprintln!("ORDINARY RESULT DIFFERENCE {label} old={expected:?} new={got:?} oldcol={:?} newcol={:?} oldevents={} newevents={}",left.column(0),right.column(0),old.snapshot(),new.snapshot());
                                }
                                assert_eq!(got, expected, "{label}");
                                assert_eq!(right.column(0), left.column(0), "{label}");
                                assert_eq!(new.snapshot(), old.snapshot(), "{label}");
                                let route = actual.batch_route_snapshot();
                                assert_eq!(
                                    (route.fallback, route.refusals),
                                    (0, 0),
                                    "{label}/{route:?}"
                                );
                                count += 1;
                            }
                        }
                    }
                }
            }
        }
    }
    eprintln!("ORDINARY RESULT CONTROLS {count}");
    assert_eq!(count, 4320);
    // The new PB stage is also exercised with non-NULL decoded argument types,
    // alongside the fixed corpus whose contextual null atoms retain SomeNull.
    for (source, result, flags, display) in [
        (8, 245, Some(32), "wire display"),
        (253, 245, Some(33), "wire display"),
        (8, 13, Some(32), "wire display"),
        (8, 16, Some(0), "ifnull"),
    ] {
        eprintln!("RECOVERY START {source}/{result}/{flags:?}/{display}");
        let owner = effective_parameter_owner(2, source, result, flags, display);
        let field = output_type(&owner);
        let mut prepared = PreparedBatchSuite::compile(
            &owner,
            &[string_ty()],
            true,
            941,
            limits(),
            ExecutionLimits::default(),
            1 << 20,
        )
        .unwrap();
        prepared.workers[0].max_materialization_retained_bytes = 16 * 1024;
        let mut ctx = RegexpSourceContext::new(
            true,
            [
                Datum::new_string("x".repeat(32768)),
                Datum::new_string("x"),
                Datum::new_string(""),
            ],
            0,
        );
        let mut first = input(&None);
        let mut output = output_for(&field);
        output.append_null(0);
        let failure = prepared
            .run(
                &owner,
                &ctx,
                &mut EvalContext::default(),
                &mut first,
                &mut output,
                &[string_ty()],
            )
            .unwrap_err();
        assert!(matches!(
            failure.local_error(),
            Some(LocalError::ResourceLimit(_))
        ));
        assert_eq!(output.num_rows(), 1);
        for value in [Datum::Null, Datum::new_string("abc")] {
            ctx.base.values[0] = value;
            ctx.base.values[1] = Datum::new_string("^a");
            ctx.base.enabled.set(false);
            ctx.base.events.borrow_mut().clear();
            for n in &ctx.base.reads {
                n.set(0);
            }
            ctx.context_reads.set(0);
            let old = RegexpSourceContext::new(false, ctx.base.values.clone(), 0);
            let mut a = input(&None);
            let mut left = output_for(&field);
            left.append_null(0);
            let expected = original_session_read_outcome(|| owner.run(&old, &mut a, &mut left));
            let mut b = input(&None);
            let mut right = output_for(&field);
            right.append_null(0);
            let mut resource = false;
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
                    .map_err(|error| {
                        resource = error.refusal() == BatchRefusal::Resource;
                        error.into_evaluator_error()
                    })
            });
            if result == 16 && !ctx.base.values[0].is_null() {
                // The existing BIT return formatter reserves 255 bytes of
                // padding work. Its fixed 16KiB allowance still refuses this
                // non-NULL conversion; NULL recovery above remains exact.
                assert!(resource);
                assert_eq!(right.num_rows(), 1);
                assert_eq!(
                    prepared.workers[0].max_materialization_retained_bytes,
                    16 * 1024
                );
                continue;
            }
            assert_eq!(got, expected);
            assert_eq!(right.column(0), left.column(0));
            assert_eq!(ctx.snapshot(), old.snapshot());
            assert_eq!(
                prepared.workers[0].max_materialization_retained_bytes,
                16 * 1024
            );
        }
    }
}

#[test]
fn pb_result_source_presentation_auth_callbacks_and_cleanup() {
    use tidb_query_datatype::codec::data_type::ScalarValueRef;
    use tidb_query_expr::local::{
        HostArgReply, HostInvocation, HostStart, HostStep, LocalHostServices,
    };
    let owner = make(
        2,
        8,
        16,
        None,
        "ifnull",
        0,
        &[
            Datum::new_string("abc"),
            Datum::new_string("^a"),
            Datum::new_string(""),
        ],
    );
    let compiled = PreparedBatch::compile_calculation_profile(
        &owner,
        0,
        &[],
        true,
        941,
        limits(),
        ExecutionLimits::default(),
        1 << 20,
        true,
    )
    .unwrap();
    let source = &compiled.source;
    let NativeHostMetadata::RegexpValues(meta) = &source.host_metadata[0] else {
        panic!("PB integer presentation")
    };
    assert!(meta.protobuf_result && meta.integer_batch_result && !meta.operator);
    assert_eq!(
        meta.result_type.as_ref().unwrap().code(),
        FieldTypeCode::Bit
    );
    let row_meta = match &compiled.row_profile.as_ref().unwrap().source.host_metadata[0] {
        NativeHostMetadata::RegexpValues(meta) => meta,
        _ => panic!("paired raw profile"),
    };
    assert!(row_meta.protobuf_result && !row_meta.integer_batch_result);
    assert_eq!(
        row_meta.result_type.as_ref().unwrap().code(),
        FieldTypeCode::Bit
    );
    let catalog = source.hosts.as_ref().unwrap().clone();
    let slot = catalog.slot(0).unwrap();
    let sig = catalog.signature(slot).unwrap();
    let row = InputRow {
        occurrence: 7,
        input_row: 0,
    };
    struct NoReady;
    impl Columns for NoReady {
        fn get(&self, _: &[String]) -> Option<Datum> {
            panic!("no expression callback")
        }
        fn ready_value_cache(&self) -> Option<&crate::ReadyValueCache> {
            panic!("bad metadata must precede cache getter")
        }
    }
    for bad in 0..5 {
        let mut forged = hosts::NativeRegexpValues {
            arguments: meta.arguments.clone(),
            source_types: meta.source_types.clone(),
            operator: meta.operator,
            protobuf_result: meta.protobuf_result,
            integer_batch_result: meta.integer_batch_result,
            invocation: meta.invocation.clone(),
            collation: meta.collation,
            result_type: meta.result_type.clone(),
            output_node: meta.output_node,
            output_role: meta.output_role,
        };
        match bad {
            0 => forged.operator = true,
            1 => forged.protobuf_result = false,
            2 => forged.output_role = NativeValueRole::CharInput,
            3 => forged.result_type = None,
            _ => forged.result_type = Some(FieldType::new(FieldTypeCode::Json)),
        };
        let metadata = vec![NativeHostMetadata::RegexpValues(Box::new(forged))];
        let store = RefCell::new(NativeValueStore::new(1 << 20).unwrap());
        store.borrow_mut().begin_window().unwrap();
        let mut hosts = NativeBatchHosts::new(catalog.clone(), &metadata, &NoReady, 8192)
            .with_native_values(Some(&store))
            .with_constant_probes(&source.host_probes);
        assert!(matches!(
            hosts.start(
                &mut EvalContext::default(),
                HostInvocation {
                    slot,
                    row,
                    arg_types: &sig.arg_types,
                    return_type: &sig.return_type
                }
            ),
            Err(LocalError::BindingContract(_))
        ));
    }
    struct Ctx<'a> {
        store: &'a RefCell<NativeValueStore>,
        ready: crate::ReadyValueCache,
        mode: usize,
        calls: Cell<usize>,
    }
    impl Ctx<'_> {
        fn callback(&self, where_: usize) {
            drop(self.store.borrow_mut());
            self.calls.set(self.calls.get() + 1);
            if self.mode == where_ {
                self.store.borrow_mut().begin_window().unwrap();
            }
            if self.mode == where_ + 1 {
                panic!("integer presentation callback")
            }
        }
    }
    impl Columns for Ctx<'_> {
        fn get(&self, _: &[String]) -> Option<Datum> {
            panic!("no expression callback")
        }
        fn ready_value_cache(&self) -> Option<&crate::ReadyValueCache> {
            drop(self.store.borrow_mut());
            Some(&self.ready)
        }
        fn context_id(&self) -> u64 {
            drop(self.store.borrow_mut());
            17
        }
        fn time_zone(&self) -> SessionTimeZone {
            self.callback(1);
            SessionTimeZone::utc()
        }
        fn type_flags(&self) -> tidb_datatype::ConversionFlags {
            self.callback(3);
            tidb_datatype::DEFAULT_STATEMENT_FLAGS
        }
    }
    for mode in 0..5 {
        let store = RefCell::new(NativeValueStore::new(1 << 20).unwrap());
        store.borrow_mut().begin_window().unwrap();
        let ctx = Ctx {
            store: &store,
            ready: crate::ReadyValueCache::new(),
            mode,
            calls: Cell::new(0),
        };
        let mut hosts =
            NativeBatchHosts::new(catalog.clone(), &source.host_metadata, &ctx, 64 * 1024)
                .with_native_values(Some(&store))
                .with_constant_probes(&source.host_probes);
        let mut eval = EvalContext::default();
        let HostStart::Pending { task, request } = hosts
            .start(
                &mut eval,
                HostInvocation {
                    slot,
                    row,
                    arg_types: &sig.arg_types,
                    return_type: &sig.return_type,
                },
            )
            .unwrap()
        else {
            panic!("pending")
        };
        assert_eq!(request.index, 0);
        let token = store
            .borrow_mut()
            .insert_clone(
                &Datum::new_string("abc"),
                NativeValueRole::CharInput,
                NativeValueOrigin {
                    node: meta.arguments[0],
                    row,
                },
            )
            .unwrap();
        let input = VectorValue::from_scalar(&ScalarValue::Bytes(Some(token)), 1);
        assert!(
            matches!(hosts.resume(&mut eval,&task,HostArgReply{index:0,field_type:&sig.arg_types[0],values:&input}).unwrap(),HostStep::NeedArg(request)if request.index==1)
        );
        let token = store
            .borrow_mut()
            .insert_clone(
                &Datum::new_string("^a"),
                NativeValueRole::CharInput,
                NativeValueOrigin {
                    node: meta.arguments[1],
                    row,
                },
            )
            .unwrap();
        let input = VectorValue::from_scalar(&ScalarValue::Bytes(Some(token)), 1);
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
            guard.hosts.resume(
                &mut eval,
                &task,
                HostArgReply {
                    index: 1,
                    field_type: &sig.arg_types[1],
                    values: &input,
                },
            )
        }));
        match mode {
            1 | 3 => assert!(matches!(result, Ok(Err(LocalError::BindingContract(_))))),
            2 | 4 => {
                assert!(result.is_err());
                assert!(ctx.ready.original_scope_status().is_err());
            }
            _ => {
                let value = match result {
                    Ok(Ok(HostStep::Ready(value))) => value,
                    other => panic!(
                        "typed presentation mode{mode}: {other:?}, native={:?},calls={}",
                        hosts.take_native_error(),
                        ctx.calls.get()
                    ),
                };
                let ScalarValueRef::Bytes(Some(token)) = value.get_scalar_ref(0) else {
                    panic!("typed presentation capsule")
                };
                assert_eq!(
                    *store
                        .borrow()
                        .get(
                            token,
                            NativeValueRole::Output,
                            NativeValueOrigin {
                                node: meta.output_node,
                                row
                            }
                        )
                        .unwrap(),
                    Datum::Int(1)
                );
            }
        }
        assert!(ctx.calls.get() > 0);
        assert!(tidb_query_expr::local::value_kernel_work_remaining().is_none());
        assert!(hosts
            .resume(
                &mut eval,
                &task,
                HostArgReply {
                    index: 1,
                    field_type: &sig.arg_types[1],
                    values: &input
                }
            )
            .is_err());
    }
}
