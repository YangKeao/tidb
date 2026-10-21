// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Independent original stateful demand before shared-frame source integration.
use super::*;
use crate::tikv::dynamic_leaf_tests as leaf;

const NAMES: [&str; 8] = [
    "values",
    "benchmark",
    "sleep",
    "get_lock",
    "release_lock",
    "is_free_lock",
    "is_used_lock",
    "release_all_locks",
];
struct C {
    ready: crate::ReadyValueCache,
    profile: usize,
    enabled: bool,
    values: [Datum; 2],
    reads: [Cell<usize>; 2],
    events: RefCell<Vec<String>>,
}
impl C {
    fn new(profile: usize, enabled: bool, values: [Datum; 2]) -> Self {
        Self {
            ready: crate::ReadyValueCache::new(),
            profile,
            enabled,
            values,
            reads: [Cell::new(0), Cell::new(0)],
            events: RefCell::new(vec![]),
        }
    }
    fn event(&self, text: String) {
        self.events.borrow_mut().push(text);
    }
    fn service<T>(&self, text: String, value: T) -> std::result::Result<T, EvalError> {
        self.event(text);
        match self.profile {
            1 => Err(EvalError::Unsupported("stateful session unavailable")),
            3 => panic!("stateful service callback"),
            _ => Ok(value),
        }
    }
    fn snapshot(&self) -> String {
        format!(
            "{}:{}|{:?}",
            self.reads[0].get(),
            self.reads[1].get(),
            self.events.borrow()
        )
    }
}
impl Columns for C {
    fn ready_value_cache(&self) -> Option<&crate::ReadyValueCache> {
        Some(&self.ready)
    }
    fn get(&self, _: &[String]) -> Option<Datum> {
        panic!("no expression callback")
    }
    fn param_value(&self, order: usize) -> std::result::Result<Datum, EvalError> {
        let count = self.reads[order].get();
        self.reads[order].set(count + 1);
        self.event(format!("param:{order}:{count}"));
        if self.profile == 4 && order == 1 && count == 1 {
            return Err(EvalError::Unsupported("late stateful child failure"));
        }
        if self.profile == 5 && order == 0 && count == 2 {
            panic!("late stateful parameter callback")
        }
        Ok(self.values[order].clone())
    }
    fn enable_vectorized_expression(&self) -> bool {
        self.event(format!("flag:{}", self.enabled));
        self.enabled
    }
    fn time_zone(&self) -> SessionTimeZone {
        self.event("zone".into());
        SessionTimeZone::utc()
    }
    fn type_flags(&self) -> tidb_datatype::ConversionFlags {
        self.event("flags".into());
        tidb_datatype::DEFAULT_STATEMENT_FLAGS.with_ignore_truncate_err(true)
    }
    fn truncate_level(&self) -> crate::ErrorLevel {
        self.event("truncate-level".into());
        crate::ErrorLevel::Warn
    }
    fn append_warning(&self, code: u16, message: &str) {
        self.event(format!("warning:{code}:{message}"));
    }
    fn set_uservar(&self, name: &str, value: Datum) {
        self.event(format!("set:{name}:{}", leaf::frame(&value)));
    }
    fn current_insert_value(&self, offset: usize) -> std::result::Result<Option<Datum>, EvalError> {
        self.service(
            format!("insert:{offset}"),
            if self.profile == 2 {
                None
            } else {
                Some(self.values[0].clone())
            },
        )
    }
    fn handle_sleep_incorrect_argument(&self) -> std::result::Result<(), EvalError> {
        self.service("sleep-incorrect".into(), ())
    }
    fn sleep_for(&self, duration: std::time::Duration) -> bool {
        self.service(format!("sleep:{}", duration.as_nanos()), self.profile == 2)
            .unwrap()
    }
    fn acquire_advisory_lock(
        &self,
        name: &str,
        timeout: std::time::Duration,
    ) -> std::result::Result<bool, EvalError> {
        self.service(
            format!("acquire:{name}:{}", timeout.as_secs()),
            self.profile != 2,
        )
    }
    fn advisory_lock_owner(&self, name: &str) -> std::result::Result<Option<u64>, EvalError> {
        self.service(
            format!("owner:{name}"),
            (self.profile == 2).then_some(u64::MAX),
        )
    }
    fn release_advisory_lock(&self, name: &str) -> std::result::Result<bool, EvalError> {
        self.service(format!("release:{name}"), self.profile != 2)
    }
    fn release_all_advisory_locks(&self) -> std::result::Result<usize, EvalError> {
        self.service(
            "release-all".into(),
            if self.profile == 2 { usize::MAX } else { 3 },
        )
    }
}
fn parameter(order: usize, field: FieldType) -> Expression {
    let mut c = Constant::new(Datum::Int(99), field);
    c.param_marker = Some(ParamMarker {
        order: i64::try_from(order).unwrap(),
    });
    Expression::Constant(c)
}
fn root(name: &str, shape: usize) -> Expression {
    let p = |order| parameter(order, ty());
    let name_arg = || {
        if shape == 3 {
            leaf::assignment("state", parameter(0, string_ty()), &string_ty())
        } else {
            parameter(0, string_ty())
        }
    };
    let mut f = if name == "values" {
        if shape == 2 {
            ScalarFunction::new(tidb_ast::CiString::new(name), ty(), vec![])
        } else {
            ScalarFunction::new_values(if shape == 1 { 9 } else { 0 }, ty())
        }
    } else {
        let args = match name {
            "benchmark" => match shape {
                1 => vec![
                    p(0),
                    parameter(1, FieldType::new(FieldTypeCode::VectorFloat32)),
                ],
                2 => {
                    let mut c = Constant::new(Datum::Int(99), ty());
                    c.deferred_expr = Some(Box::new(leaf::assignment("state", p(1), &ty())));
                    vec![p(0), Expression::Constant(c)]
                }
                3 => vec![p(0)],
                _ => vec![p(0), p(1)],
            },
            "sleep" => {
                if shape == 1 {
                    vec![]
                } else if shape == 3 {
                    vec![leaf::assignment("state", p(0), &ty())]
                } else {
                    vec![p(0)]
                }
            }
            "get_lock" => {
                if shape == 1 {
                    vec![name_arg()]
                } else {
                    vec![name_arg(), p(1)]
                }
            }
            "release_all_locks" => {
                if shape == 1 {
                    vec![p(0)]
                } else {
                    vec![]
                }
            }
            _ => {
                if shape == 1 {
                    vec![]
                } else {
                    vec![name_arg()]
                }
            }
        };
        ScalarFunction::new(tidb_ast::CiString::new(name), ty(), args)
    };
    if name == "values" && shape == 3 {
        f.args.push(p(0));
    }
    Expression::ScalarFunction(f)
}
fn values() -> Vec<[Datum; 2]> {
    vec![
        [Datum::Null, Datum::Int(3)],
        [Datum::Int(0), Datum::Int(1)],
        [Datum::Int(1), Datum::Int(3)],
        [Datum::Int(-1), Datum::Null],
        [Datum::new_string("AbC"), Datum::Int(0)],
        [Datum::Bytes(vec![255]), Datum::Int(-1)],
        [Datum::new_string("x".repeat(65)), Datum::Int(3)],
        [Datum::new_string("2bad"), Datum::MinNotNull],
        [Datum::new_string("Ké"), Datum::Real(0.5)],
    ]
}
#[test]
fn stateful_original_rows_demands_callbacks_errors_and_output_prefixes() {
    if isolate_common_corpus(
        "stateful_tests::stateful_original_rows_demands_callbacks_errors_and_output_prefixes",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_stateful_matrix(false);
}
#[test]
fn stateful_actual_matrix_keeps_original_demand_and_context_effects() {
    if isolate_common_corpus(
        "stateful_tests::stateful_actual_matrix_keeps_original_demand_and_context_effects",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_stateful_matrix(true);
}
fn run_stateful_matrix(compare: bool) {
    let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
    for name in NAMES {
        for shape in 0..4 {
            let actual = compare.then(|| suite(root(name, shape)));
            for profile in 0..6 {
                for enabled in [false, true] {
                    for (vi, values) in values().into_iter().enumerate() {
                        for selection in [None, Some(vec![2, 0, 2]), Some(vec![])] {
                            let owner = suite(root(name, shape));
                            let ctx = C::new(profile, enabled, values.clone());
                            let mut input = chunk(
                                &[ty()],
                                &[
                                    vec![Datum::Int(1)],
                                    vec![Datum::Int(2)],
                                    vec![Datum::Int(3)],
                                ],
                            );
                            input.set_sel(selection.clone());
                            let mut output = output_for(&ty());
                            output.append_null(0);
                            let result =
                                session_read_outcome(|| owner.run(&ctx, &mut input, &mut output));
                            if let Some(actual) = &actual {
                                let actual_ctx = C::new(profile, enabled, values.clone());
                                let mut actual_input = chunk(
                                    &[ty()],
                                    &[
                                        vec![Datum::Int(1)],
                                        vec![Datum::Int(2)],
                                        vec![Datum::Int(3)],
                                    ],
                                );
                                actual_input.set_sel(selection.clone());
                                let mut actual_output = output_for(&ty());
                                actual_output.append_null(0);
                                let got = session_read_outcome(|| {
                                    actual.run_with_tikv_batch(
                                        &actual_ctx,
                                        &mut actual_input,
                                        &mut actual_output,
                                        &[ty()],
                                    )
                                });
                                let label = format!(
                                    "{name}/{shape}/{profile}/{enabled}/{vi}/{selection:?}"
                                );
                                assert_eq!(got, result, "result {label}");
                                assert_eq!(
                                    actual_output.column(0),
                                    output.column(0),
                                    "output {label}"
                                );
                                assert_eq!(
                                    actual_ctx.snapshot(),
                                    ctx.snapshot(),
                                    "effects {label}"
                                );
                                let route = actual.batch_route_snapshot();
                                assert_eq!(
                                    (route.refusals, route.fallback),
                                    (0, 0),
                                    "source {label}: {route:?}"
                                );
                            }
                            let observed = format!("{name}/{shape}/{profile}/{enabled}/{vi}/{selection:?}|{result}|{:?}|{}|{:?}", output.column(0),ctx.snapshot(),input.sel());
                            for b in observed.bytes() {
                                hash = (hash ^ u64::from(b)).wrapping_mul(0x100000001b3);
                            }
                            count += 1;
                        }
                    }
                }
            }
        }
    }
    eprintln!("STATEFUL ORIGINAL {count}/{hash:016X}");
    assert_eq!((count, hash), (10368, 0xC0B8ADE1ECE31EEF));
}
#[test]
fn stateful_actual_sources_do_not_fallback() {
    let mut failures = vec![];
    for name in NAMES {
        let owner = suite(root(name, 0));
        let ctx = C::new(0, true, [Datum::Int(1), Datum::Int(1)]);
        let mut input = chunk(&[ty()], &[vec![Datum::Int(1)]]);
        let mut output = output_for(&ty());
        let _ = session_read_outcome(|| {
            owner.run_with_tikv_batch(&ctx, &mut input, &mut output, &[ty()])
        });
        let route = owner.batch_route_snapshot();
        if (route.refusals, route.fallback) != (0, 0) {
            failures.push((name, route));
        }
    }
    assert!(failures.is_empty(), "stateful source gaps: {failures:?}");
}

#[test]
fn stateful_value_phases_independently_match_original_before_caller_retirement() {
    if isolate_common_corpus("stateful_tests::stateful_value_phases_independently_match_original_before_caller_retirement") { return }
    std::panic::set_hook(Box::new(|_| {}));
    use crate::scalar_function::stateful::{Descriptor, Operation as O};
    let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
    for name in NAMES {
        for shape in 0..4 {
            for profile in 0..6 {
                for values in values() {
                    let Expression::ScalarFunction(function) = root(name, shape) else {
                        unreachable!()
                    };
                    let descriptor = Descriptor::capture(&function).unwrap();
                    let left = C::new(profile, false, values.clone());
                    let right = C::new(profile, false, values);
                    let input = chunk(&[ty()], &[vec![Datum::Int(1)]]);
                    let row = input.get_row(0);
                    let old = session_read_outcome(|| {
                        left.ready.with_columns(&left, |ctx| {
                            crate::scalar_function::stateful::original_eval(&function, ctx, row)
                        })
                    });
                    let new = session_read_outcome(|| {
                        right.ready.with_columns(
                            &right,
                            |ctx| -> std::result::Result<Datum, EvalError> {
                                descriptor.validate_arity()?;
                                let arg = |i: usize| function.args[i].eval(ctx, row);
                                let value = match descriptor.operation {
                                    O::Values | O::ReleaseAll => descriptor.immediate(ctx)?,
                                    O::Benchmark => {
                                        match descriptor.benchmark_count(&arg(0)?, ctx)? {
                                            None => Datum::Null,
                                            Some(n) => {
                                                for _ in 0..n {
                                                    arg(1)?;
                                                }
                                                Datum::Int(0)
                                            }
                                        }
                                    }
                                    O::Sleep => descriptor.sleep(&arg(0)?, ctx)?,
                                    O::GetLock => {
                                        let name = descriptor.lock_name(&arg(0)?, ctx)?;
                                        descriptor.acquire(&name, &arg(1)?, ctx)?
                                    }
                                    _ => {
                                        let name = descriptor.lock_name(&arg(0)?, ctx)?;
                                        descriptor.lock_service(&name, ctx)?
                                    }
                                };
                                crate::scalar_function::coerce_function_result(
                                    name,
                                    function.get_static_type(),
                                    value,
                                )
                            },
                        )
                    });
                    assert_eq!(new, old, "{name}/{shape}/{profile}");
                    assert_eq!(
                        right.snapshot(),
                        left.snapshot(),
                        "effects {name}/{shape}/{profile}"
                    );
                    for b in format!("{name}/{shape}/{profile}|{old}|{}", left.snapshot()).bytes() {
                        hash = (hash ^ u64::from(b)).wrapping_mul(0x100000001b3);
                    }
                    count += 1;
                }
            }
        }
    }
    eprintln!("STATEFUL INDEPENDENT PHASES {count}/{hash:016X}");
    assert_eq!((count, hash), (1728, 0x90EA3BA8B5A947B2));
}

#[test]
fn stateful_benchmark_drops_ignored_child_storage_between_fresh_demands() {
    let reference = suite(root("benchmark", 0));
    let actual = suite(root("benchmark", 0));
    let pair = [Datum::Int(10), Datum::Bytes(vec![b'x'; 1 << 20])];
    let old = C::new(0, false, pair.clone());
    let new = C::new(0, false, pair);
    let mut a = chunk(&[ty()], &[vec![Datum::Int(1)]]);
    let mut b = chunk(&[ty()], &[vec![Datum::Int(1)]]);
    let mut left = output_for(&ty());
    let mut right = output_for(&ty());
    reference.run(&old, &mut a, &mut left).unwrap();
    actual
        .run_with_tikv_batch(&new, &mut b, &mut right, &[ty()])
        .unwrap();
    assert_eq!(right.column(0), left.column(0));
    assert_eq!(new.snapshot(), old.snapshot());
    let route = actual.batch_route_snapshot();
    assert_eq!((route.refusals, route.fallback), (0, 0));
}

#[test]
fn stateful_benchmark_large_literals_columns_nested_calls_and_many_small_reads() {
    let bytes = Datum::Bytes(vec![b'x'; 1 << 20]);
    for shape in 0..5 {
        let body = match shape {
            0 => literal(Datum::Bytes(vec![b'x'; 128 << 10]), ty()),
            1 => typed_column(0, &string_ty()),
            2 => call(
                "ifnull",
                string_ty(),
                vec![parameter(1, string_ty()), literal(Datum::Null, string_ty())],
            ),
            3 => call("benchmark", ty(), vec![int(3), parameter(1, ty())]),
            _ => parameter(1, ty()),
        };
        let iterations = if shape == 4 { 100_000 } else { 64 };
        let expression = call("benchmark", ty(), vec![int(iterations), body]);
        let reference = suite(expression.clone());
        let actual = suite(expression);
        let data = if shape == 4 {
            Datum::Int(1)
        } else {
            bytes.clone()
        };
        let old = C::new(0, false, [Datum::Null, data.clone()]);
        let new = C::new(0, false, [Datum::Null, data]);
        let mut a = chunk(&[string_ty()], &[vec![bytes.clone()]]);
        let mut b = chunk(&[string_ty()], &[vec![bytes.clone()]]);
        let mut left = output_for(&ty());
        let mut right = output_for(&ty());
        let want = session_read_outcome(|| reference.run(&old, &mut a, &mut left));
        let got = session_read_outcome(|| {
            actual.run_with_tikv_batch(&new, &mut b, &mut right, &[string_ty()])
        });
        assert_eq!(got, want, "shape {shape}");
        assert_eq!(right.column(0), left.column(0), "shape {shape}");
        assert_eq!(new.snapshot(), old.snapshot(), "effects {shape}");
        let route = actual.batch_route_snapshot();
        assert_eq!(
            (route.refusals, route.fallback),
            (0, 0),
            "{shape}: {route:?}"
        );
    }
}

#[test]
fn stateful_host_capabilities_consumption_callbacks_and_recovery() {
    use super::hosts::{NativeBatchHosts, NativeHostMetadata, NativeStateful};
    use tidb_query_expr::local::{
        HostArgReply, HostInvocation, HostSignature, HostStart, HostStep, HostTaskId,
        LocalHostServices,
    };
    struct D<'a> {
        store: &'a RefCell<NativeValueStore>,
        action: Cell<u8>,
        events: RefCell<Vec<&'static str>>,
    }
    impl D<'_> {
        fn effect(&self, label: &'static str) -> std::result::Result<(), EvalError> {
            let mut store = self.store.borrow_mut();
            self.events.borrow_mut().push(label);
            match self.action.get() {
                1 => Err(EvalError::Unsupported("actual session failure")),
                2 => Err(EvalError::ExpressionRuntimeFailure(
                    crate::ExpressionRuntimeFailure::from_local_eval(
                        LocalError::ResourceLimit("actual session quota".into()),
                        None,
                    ),
                )),
                3 => panic!("stateful callback"),
                4 => {
                    store.begin_window().unwrap();
                    Ok(())
                }
                _ => Ok(()),
            }
        }
    }
    impl Columns for D<'_> {
        fn get(&self, _: &[String]) -> Option<Datum> {
            panic!("no expression callback")
        }
        fn ready_value_cache(&self) -> Option<&crate::ReadyValueCache> {
            self.effect("ready").unwrap();
            None
        }
        fn time_zone(&self) -> SessionTimeZone {
            self.effect("zone").unwrap();
            SessionTimeZone::utc()
        }
        fn type_flags(&self) -> tidb_datatype::ConversionFlags {
            self.effect("flags").unwrap();
            tidb_datatype::DEFAULT_STATEMENT_FLAGS.with_ignore_truncate_err(true)
        }
        fn append_warning(&self, _: u16, _: &str) {
            self.effect("warning").unwrap();
        }
        fn current_insert_value(&self, _: usize) -> std::result::Result<Option<Datum>, EvalError> {
            self.effect("insert")?;
            Ok(None)
        }
        fn handle_sleep_incorrect_argument(&self) -> std::result::Result<(), EvalError> {
            self.effect("sleep-incorrect")
        }
        fn acquire_advisory_lock(
            &self,
            _: &str,
            _: std::time::Duration,
        ) -> std::result::Result<bool, EvalError> {
            self.effect("acquire")?;
            Ok(true)
        }
        fn advisory_lock_owner(&self, _: &str) -> std::result::Result<Option<u64>, EvalError> {
            self.effect("owner")?;
            Ok(Some(u64::MAX))
        }
        fn release_advisory_lock(&self, _: &str) -> std::result::Result<bool, EvalError> {
            self.effect("release")?;
            Ok(true)
        }
        fn release_all_advisory_locks(&self) -> std::result::Result<usize, EvalError> {
            self.effect("release-all")?;
            Ok(3)
        }
    }
    for optional in [false, true] {
        for name in NAMES {
            let Expression::ScalarFunction(mut function) = root(name, 0) else {
                unreachable!()
            };
            if name == "benchmark" {
                function.args[0] = parameter(0, string_ty());
            }
            if optional {
                function.ret_type = None;
            }
            let descriptor =
                crate::scalar_function::stateful::Descriptor::capture(&function).unwrap();
            let arity = descriptor.argument_types.len();
            let field = projected(&native_bytes_type(), true).unwrap();
            let args = vec![field.clone(); arity];
            let catalog = HostCatalog::for_sql_batch(vec![(
                SqlBatchHostOperation::EagerValuesNative,
                HostSignature {
                    arg_types: args.clone().into(),
                    return_type: field.clone(),
                },
            )])
            .unwrap();
            let slot = catalog.slot(0).unwrap();
            let metadata = [NativeHostMetadata::Stateful(Box::new(NativeStateful {
                arguments: (3..3 + arity).collect(),
                descriptor,
                repeated_subtree: (name == "benchmark").then_some(4..5),
                result_type: (!optional).then(ty),
                output_node: 2,
                output_role: NativeValueRole::Output,
            }))];
            let store = RefCell::new(NativeValueStore::new(8 << 20).unwrap());
            store.borrow_mut().begin_window().unwrap();
            let ctx = D {
                store: &store,
                action: Cell::new(0),
                events: RefCell::new(vec![]),
            };
            let mut p = NativeBatchHosts::new(catalog, &metadata, &ctx, 16384)
                .with_native_values(Some(&store));
            let mut eval = EvalContext::default();
            let row = InputRow {
                occurrence: 2,
                input_row: 7,
            };
            let start = |p: &mut NativeBatchHosts<'_>, eval: &mut EvalContext| {
                p.start(
                    eval,
                    HostInvocation {
                        slot,
                        row,
                        arg_types: &args,
                        return_type: &field,
                    },
                )
            };
            let begin = |p: &mut NativeBatchHosts<'_>, eval: &mut EvalContext| {
                let HostStart::Pending { task, request } = start(p, eval).unwrap() else {
                    panic!()
                };
                assert_eq!(request.index, 0);
                task
            };
            let reply = |p: &mut NativeBatchHosts<'_>,
                         eval: &mut EvalContext,
                         task: &HostTaskId,
                         index,
                         token| {
                p.resume(
                    eval,
                    task,
                    HostArgReply {
                        index,
                        field_type: &field,
                        values: &VectorValue::from_scalar(&ScalarValue::Bytes(token), 1),
                    },
                )
            };
            let insert = |value: &Datum, role, node, row| {
                store
                    .borrow_mut()
                    .insert_clone(value, role, NativeValueOrigin { node, row })
                    .unwrap()
            };
            if arity == 0 {
                for action in [1, 2] {
                    ctx.action.set(action);
                    assert!(start(&mut p, &mut eval).is_err());
                    assert!(p.take_native_error().is_some());
                }
                for _ in 0..128 {
                    ctx.action.set(3);
                    assert!(
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| start(
                            &mut p, &mut eval
                        )))
                        .is_err()
                    );
                }
                ctx.action.set(0);
                assert!(matches!(
                    start(&mut p, &mut eval).unwrap(),
                    HostStart::Ready(_)
                ));
                continue;
            }
            let good = insert(&Datum::Null, NativeValueRole::CharInput, 3, row);
            let mut corrupt = good.clone();
            corrupt[0] ^= 1;
            let mut short = good.clone();
            short.pop();
            let mut long = good;
            long.push(0);
            let stale = insert(&Datum::Null, NativeValueRole::CharInput, 3, row);
            store.borrow_mut().begin_window().unwrap();
            let mut foreign = NativeValueStore::new(4096).unwrap();
            foreign.begin_window().unwrap();
            let foreign = foreign
                .insert_clone(
                    &Datum::Null,
                    NativeValueRole::CharInput,
                    NativeValueOrigin { node: 3, row },
                )
                .unwrap();
            for bad in [
                None,
                Some(corrupt),
                Some(short),
                Some(long),
                Some(stale),
                Some(foreign),
                Some(insert(&Datum::Null, NativeValueRole::Output, 3, row)),
                Some(insert(&Datum::Null, NativeValueRole::CharInput, 4, row)),
                Some(insert(
                    &Datum::Null,
                    NativeValueRole::CharInput,
                    3,
                    InputRow {
                        occurrence: 3,
                        ..row
                    },
                )),
                Some(insert(
                    &Datum::Null,
                    NativeValueRole::CharInput,
                    3,
                    InputRow {
                        input_row: 8,
                        ..row
                    },
                )),
            ] {
                let task = begin(&mut p, &mut eval);
                assert!(
                    matches!(
                        reply(&mut p, &mut eval, &task, 0, bad),
                        Err(LocalError::BindingContract(_))
                    ),
                    "{name}"
                );
                assert!(ctx.events.borrow().is_empty());
                p.cancel(&task);
            }
            // Convert a real count/name or invoke a real service after consuming
            // the phase. Both panic and generation mutation must reject replay.
            for action in [3, 4] {
                ctx.action.set(action);
                ctx.events.borrow_mut().clear();
                let task = begin(&mut p, &mut eval);
                let first = if name == "sleep" {
                    Datum::Null
                } else {
                    Datum::new_string(if name == "benchmark" { "3" } else { "L" })
                };
                let token = insert(&first, NativeValueRole::CharInput, 3, row);
                let got = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let result = reply(&mut p, &mut eval, &task, 0, Some(token.clone()));
                    if let Ok(HostStep::NeedArg(request)) = result {
                        let second = insert(&Datum::Int(1), NativeValueRole::CharInput, 4, row);
                        reply(&mut p, &mut eval, &task, request.index, Some(second))
                    } else {
                        result
                    }
                }));
                if action == 3 {
                    assert!(got.is_err(), "panic {name}");
                } else {
                    assert!(got.unwrap().is_err(), "generation {name}");
                }
                let before = ctx.events.borrow().clone();
                assert!(reply(&mut p, &mut eval, &task, 0, Some(token)).is_err());
                assert_eq!(*ctx.events.borrow(), before);
                p.cancel(&task);
            }
            ctx.action.set(0);
            ctx.events.borrow_mut().clear();
            let old = begin(&mut p, &mut eval);
            p.cancel(&old);
            let task = begin(&mut p, &mut eval);
            p.cancel(&old);
            let first = if name == "sleep" {
                Datum::Null
            } else {
                Datum::new_string(if name == "benchmark" { "3" } else { "L" })
            };
            let first = insert(&first, NativeValueRole::CharInput, 3, row);
            let mut step = reply(&mut p, &mut eval, &task, 0, Some(first.clone())).unwrap();
            let mut count = 0;
            while let HostStep::NeedArg(request) = step {
                assert_eq!(request.index, 1);
                let token = insert(&Datum::Int(1), NativeValueRole::CharInput, 4, row);
                step = reply(&mut p, &mut eval, &task, 1, Some(token.clone())).unwrap();
                count += 1;
                if name == "benchmark" {
                    assert!(store
                        .borrow()
                        .get(
                            &token,
                            NativeValueRole::CharInput,
                            NativeValueOrigin { node: 4, row }
                        )
                        .is_err());
                }
            }
            if name == "benchmark" {
                assert_eq!(count, 3);
            } else if name == "get_lock" {
                assert_eq!(count, 1);
            } else {
                assert_eq!(count, 0);
            }
            let before = ctx.events.borrow().clone();
            assert!(reply(&mut p, &mut eval, &task, 0, Some(first)).is_err());
            assert_eq!(*ctx.events.borrow(), before);
            p.cancel(&task);
            if matches!(name, "benchmark" | "get_lock") {
                for _ in 0..128 {
                    store.borrow_mut().begin_window().unwrap();
                    let abandoned = begin(&mut p, &mut eval);
                    let first = if name == "benchmark" {
                        Datum::Int(3)
                    } else {
                        Datum::new_string("L")
                    };
                    let first = insert(&first, NativeValueRole::CharInput, 3, row);
                    assert!(matches!(
                        reply(&mut p, &mut eval, &abandoned, 0, Some(first)),
                        Ok(HostStep::NeedArg(_))
                    ));
                    p.cancel(&abandoned); // Repeated body / owned-name timeout is pending.
                    let current = begin(&mut p, &mut eval);
                    p.cancel(&abandoned); // Its reused slot now belongs to this call.
                    let before = ctx.events.borrow().clone();
                    let unused = insert(&Datum::Int(0), NativeValueRole::CharInput, 4, row);
                    assert!(reply(&mut p, &mut eval, &abandoned, 1, Some(unused)).is_err());
                    assert_eq!(*ctx.events.borrow(), before);
                    let first = if name == "benchmark" {
                        Datum::Null
                    } else {
                        Datum::new_string("L")
                    };
                    let first = insert(&first, NativeValueRole::CharInput, 3, row);
                    let mut step = reply(&mut p, &mut eval, &current, 0, Some(first)).unwrap();
                    if name == "get_lock" {
                        assert!(matches!(step, HostStep::NeedArg(_)));
                        let timeout = insert(&Datum::Null, NativeValueRole::CharInput, 4, row);
                        step = reply(&mut p, &mut eval, &current, 1, Some(timeout)).unwrap();
                    }
                    assert!(matches!(step, HostStep::Ready(_)));
                    p.cancel(&current);
                }
            }
            assert!(tidb_query_expr::local::value_kernel_work_remaining().is_none());
        }
    }
}

#[test]
fn stateful_sleep_full_values_keep_original_coercion_services_and_return_types() {
    if isolate_common_corpus("stateful_tests::stateful_sleep_full_values_keep_original_coercion_services_and_return_types") { return }
    std::panic::set_hook(Box::new(|_| {}));
    let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
    for (fi, field) in crate::tikv::eager_scalar_value_tests::types()
        .into_iter()
        .enumerate()
    {
        for (vi, value) in crate::tikv::byte_unary_value_tests::values()
            .into_iter()
            .enumerate()
        {
            let source = crate::tikv::misc_identity_value_tests::field(&value);
            let expression = call("sleep", field.clone(), vec![parameter(0, source)]);
            let actual = suite(expression.clone());
            for profile in 0..6 {
                for enabled in [false, true] {
                    for selection in [None, Some(vec![2, 0, 2]), Some(vec![])] {
                        let original = suite(expression.clone());
                        let pair = [value.clone(), Datum::Null];
                        let ctx = C::new(profile, enabled, pair.clone());
                        let new_ctx = C::new(profile, enabled, pair);
                        let input = || {
                            let mut c = chunk(
                                &[ty()],
                                &[
                                    vec![Datum::Int(1)],
                                    vec![Datum::Int(2)],
                                    vec![Datum::Int(3)],
                                ],
                            );
                            c.set_sel(selection.clone());
                            c
                        };
                        let mut a = input();
                        let mut b = input();
                        let mut left = output_for(&field);
                        let mut right = output_for(&field);
                        left.append_null(0);
                        right.append_null(0);
                        let old = session_read_outcome(|| original.run(&ctx, &mut a, &mut left));
                        let got = session_read_outcome(|| {
                            actual.run_with_tikv_batch(&new_ctx, &mut b, &mut right, &[ty()])
                        });
                        let label = format!("{fi}/{vi}/{profile}/{enabled}/{selection:?}");
                        assert_eq!(got, old, "result {label}");
                        assert_eq!(right.column(0), left.column(0), "output {label}");
                        assert_eq!(new_ctx.snapshot(), ctx.snapshot(), "effects {label}");
                        let route = actual.batch_route_snapshot();
                        assert_eq!(
                            (route.refusals, route.fallback),
                            (0, 0),
                            "{label}: {route:?}"
                        );
                        for byte in
                            format!("{label}|{old}|{:?}|{}", left.column(0), ctx.snapshot()).bytes()
                        {
                            hash = (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3);
                        }
                        count += 1;
                    }
                }
            }
        }
    }
    eprintln!("STATEFUL SLEEP ORIGINAL {count}/{hash:016X}");
    assert_eq!((count, hash), (59004, 0x8C582504AD4E5981));
}

#[test]
fn stateful_sleep_unused_vector_payload_keeps_original_error_without_copy() {
    let value = Datum::VectorFloat32(tidb_datatype::VectorFloat32::init(524_288));
    let expression = call(
        "sleep",
        ty(),
        vec![parameter(0, FieldType::new(FieldTypeCode::VectorFloat32))],
    );
    let original = suite(expression.clone());
    let actual = suite(expression);
    let pair = [value, Datum::Null];
    let old = C::new(0, false, pair.clone());
    let new = C::new(0, false, pair);
    let mut a = chunk(&[ty()], &[vec![Datum::Int(1)]]);
    let mut b = chunk(&[ty()], &[vec![Datum::Int(1)]]);
    let mut left = output_for(&ty());
    let mut right = output_for(&ty());
    let expected = session_read_outcome(|| original.run(&old, &mut a, &mut left));
    assert!(expected.contains("numeric argument conversion"));
    let got =
        session_read_outcome(|| actual.run_with_tikv_batch(&new, &mut b, &mut right, &[ty()]));
    assert_eq!(got, expected);
    assert_eq!(right.column(0), left.column(0));
    assert_eq!(new.snapshot(), old.snapshot());
}

#[test]
fn stateful_nontext_preparation_matches_independent_active_original_conversion() {
    if isolate_common_corpus("stateful_tests::stateful_nontext_preparation_matches_independent_active_original_conversion") { return }
    std::panic::set_hook(Box::new(|_| {}));
    let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
    for (i, value) in crate::tikv::byte_unary_value_tests::values()
        .into_iter()
        .enumerate()
    {
        if !crate::scalar_function::stateful::borrowed_sleep_input(&value) {
            assert!(crate::ops::to_f64_nontext(&value).is_none());
            continue;
        }
        for maximum in [None, Some(8192)] {
            let old = C::new(0, false, [Datum::Null, Datum::Null]);
            let new = C::new(0, false, [Datum::Null, Datum::Null]);
            let observe = |original: bool| {
                let run = || {
                    if original {
                        crate::ops::original_to_f64_with_mysql_string(&value, &old)
                    } else {
                        crate::ops::to_f64_nontext(&value).unwrap()
                    }
                };
                session_read_outcome(|| {
                    if let Some(maximum) = maximum {
                        tidb_query_expr::local::with_value_kernel_work_limit(maximum, || {
                            run().map(f64::to_bits)
                        })
                    } else {
                        run().map(f64::to_bits)
                    }
                })
            };
            let a = observe(true);
            let b = observe(false);
            assert_eq!(b, a, "{i}/{maximum:?}");
            assert_eq!(new.snapshot(), old.snapshot());
            for byte in format!("{i}/{maximum:?}|{a}|{}", old.snapshot()).bytes() {
                hash = (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3);
            }
            count += 1;
        }
    }
    eprintln!("STATEFUL INDEPENDENT NONTEXT {count}/{hash:016X}");
    assert_eq!((count, hash), (174, 0x0DF653FAA526C2B2));
}

#[test]
fn stateful_integer_cast_rejections_do_not_clone_unused_vector_payload() {
    let mut failures = Vec::new();
    let vector = Datum::VectorFloat32(tidb_datatype::VectorFloat32::init(524_288));
    for timeout in [false, true] {
        let name = if timeout { "get_lock" } else { "benchmark" };
        let Expression::ScalarFunction(mut f) = root(name, 0) else {
            unreachable!()
        };
        f.args[usize::from(timeout)] = parameter(
            usize::from(timeout),
            FieldType::new(FieldTypeCode::VectorFloat32),
        );
        let expression = Expression::ScalarFunction(f);
        let original = suite(expression.clone());
        let actual = suite(expression);
        let pair = if timeout {
            [Datum::new_string("L"), vector.clone()]
        } else {
            [vector.clone(), Datum::Int(1)]
        };
        let old = C::new(0, false, pair.clone());
        let new = C::new(0, false, pair);
        let mut a = chunk(&[ty()], &[vec![Datum::Int(1)]]);
        let mut b = chunk(&[ty()], &[vec![Datum::Int(1)]]);
        let mut left = output_for(&ty());
        let mut right = output_for(&ty());
        let expected = session_read_outcome(|| original.run(&old, &mut a, &mut left));
        assert!(expected.contains("a vector can only be cast to string or vector"));
        let got =
            session_read_outcome(|| actual.run_with_tikv_batch(&new, &mut b, &mut right, &[ty()]));
        if got != expected {
            failures.push((name, got, expected));
        }
        assert_eq!(right.column(0), left.column(0));
        assert_eq!(new.snapshot(), old.snapshot());
    }
    assert!(failures.is_empty(), "unused integer payloads: {failures:?}");
}

#[test]
fn stateful_other_services_full_values_and_return_types_keep_original_outputs() {
    if isolate_common_corpus("stateful_tests::stateful_other_services_full_values_and_return_types_keep_original_outputs") { return }
    std::panic::set_hook(Box::new(|_| {}));
    let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
    for (fi, field) in crate::tikv::eager_scalar_value_tests::types()
        .into_iter()
        .enumerate()
    {
        for (vi, value) in crate::tikv::byte_unary_value_tests::values()
            .into_iter()
            .enumerate()
        {
            let source = crate::tikv::misc_identity_value_tests::field(&value);
            for variant in 0..8 {
                let name = [
                    "values",
                    "benchmark",
                    "get_lock",
                    "get_lock",
                    "release_lock",
                    "is_free_lock",
                    "is_used_lock",
                    "release_all_locks",
                ][variant];
                let Expression::ScalarFunction(mut function) = root(name, 0) else {
                    unreachable!()
                };
                function.ret_type = Some(field.clone());
                let pair = if variant == 3 {
                    [Datum::new_string("L"), value.clone()]
                } else {
                    [value.clone(), Datum::Int(3)]
                };
                if variant == 1 {
                    // Every positive count validates the original unsupported
                    // vector body before looping; no enormous count can run.
                    function.args[0] = parameter(0, source.clone());
                    function.args[1] = parameter(1, FieldType::new(FieldTypeCode::VectorFloat32));
                } else if name != "values" && name != "release_all_locks" {
                    function.args[usize::from(variant == 3)] =
                        parameter(usize::from(variant == 3), source.clone());
                }
                let expression = Expression::ScalarFunction(function);
                let actual = suite(expression.clone());
                for profile in 0..6 {
                    for enabled in [false, true] {
                        for selection in [None, Some(vec![2, 0, 2]), Some(vec![])] {
                            let original = suite(expression.clone());
                            let old = C::new(profile, enabled, pair.clone());
                            let new = C::new(profile, enabled, pair.clone());
                            let input = || {
                                let mut c = chunk(
                                    &[ty()],
                                    &[
                                        vec![Datum::Int(1)],
                                        vec![Datum::Int(2)],
                                        vec![Datum::Int(3)],
                                    ],
                                );
                                c.set_sel(selection.clone());
                                c
                            };
                            let mut a = input();
                            let mut b = input();
                            let mut left = output_for(&field);
                            let mut right = output_for(&field);
                            left.append_null(0);
                            right.append_null(0);
                            let expected =
                                session_read_outcome(|| original.run(&old, &mut a, &mut left));
                            let got = session_read_outcome(|| {
                                actual.run_with_tikv_batch(&new, &mut b, &mut right, &[ty()])
                            });
                            let label =
                                format!("{fi}/{vi}/{variant}/{profile}/{enabled}/{selection:?}");
                            if got != expected
                                || right.column(0) != left.column(0)
                                || new.snapshot() != old.snapshot()
                            {
                                eprintln!("STATEFUL OTHER DIFFERENCE {label} input={} expected={expected:?} got={got:?} left={:?} right={:?} oldeffects={} neweffects={}",leaf::frame(&value), left.column(0), right.column(0),old.snapshot(),new.snapshot());
                            }
                            assert_eq!(got, expected, "result {label}");
                            assert_eq!(right.column(0), left.column(0), "output {label}");
                            assert_eq!(new.snapshot(), old.snapshot(), "effects {label}");
                            let route = actual.batch_route_snapshot();
                            if (route.refusals, route.fallback) != (0, 0) {
                                eprintln!("STATEFUL OTHER ROUTE {label}: {route:?}");
                            }
                            assert_eq!(
                                (route.refusals, route.fallback),
                                (0, 0),
                                "{label}: {route:?}"
                            );
                            for byte in format!(
                                "{label}|{expected}|{:?}|{}",
                                left.column(0),
                                old.snapshot()
                            )
                            .bytes()
                            {
                                hash = (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3);
                            }
                            count += 1;
                        }
                    }
                }
            }
        }
    }
    eprintln!("STATEFUL OTHER ORIGINAL {count}/{hash:016X}");
    assert_eq!((count, hash), (472032, 0xE1A862511376506D));
}

#[test]
fn stateful_long_selections_reuse_sources_across_public_and_frame_invocations() {
    if isolate_common_corpus("stateful_tests::stateful_long_selections_reuse_sources_across_public_and_frame_invocations") { return }
    std::panic::set_hook(Box::new(|_| {}));
    for name in NAMES {
        for shape in [0, 2, 3] {
            let mut expression = root(name, shape);
            if name == "benchmark" && shape == 0 {
                let Expression::ScalarFunction(f) = &mut expression else {
                    unreachable!()
                };
                f.args[1] = call("plus", ty(), vec![column(0), parameter(1, ty())]);
            }
            let actual = suite(expression.clone());
            for rows in [2051usize, 2057] {
                let selections = [
                    None,
                    Some((0..rows).step_by(2).rev().chain([0, rows - 1, 0]).collect()),
                    Some(vec![rows - 1, 0, rows - 1]),
                    Some(vec![]),
                ];
                for selection in selections {
                    for profile in [0, 4, 5] {
                        for cycle in 0..4 {
                            let enabled = cycle >= 2;
                            let values = [
                                Datum::Int(if cycle % 2 == 0 { 3 } else { 2 }),
                                Datum::Int(7),
                            ];
                            let old = C::new(profile, enabled, values.clone());
                            let new = C::new(profile, enabled, values);
                            let input = || {
                                let mut c = chunk(
                                    &[ty()],
                                    &(0..rows)
                                        .map(|i| vec![Datum::Int(i as i64)])
                                        .collect::<Vec<_>>(),
                                );
                                c.set_sel(selection.clone());
                                c
                            };
                            let mut a = input();
                            let mut b = input();
                            let mut left = output_for(&ty());
                            let mut right = output_for(&ty());
                            left.append_null(0);
                            right.append_null(0);
                            let reference = suite(expression.clone());
                            let expected =
                                session_read_outcome(|| reference.run(&old, &mut a, &mut left));
                            let got = session_read_outcome(|| {
                                if cycle % 2 == 0 {
                                    actual.run_with_tikv_batch(&new, &mut b, &mut right, &[ty()])
                                } else {
                                    actual.run(&new, &mut b, &mut right)
                                }
                            });
                            let label = format!("{name}/{shape}/{rows}/{profile}/{cycle}");
                            if got != expected
                                || right.column(0) != left.column(0)
                                || new.snapshot() != old.snapshot()
                            {
                                eprintln!("STATEFUL LONG DIFFERENCE {label} expected={expected:?} got={got:?} old={} new={}",old.snapshot(),new.snapshot());
                            }
                            assert_eq!(got, expected, "result {label}");
                            assert_eq!(right.column(0), left.column(0), "output {label}");
                            assert_eq!(new.snapshot(), old.snapshot(), "effects {label}");
                            assert_eq!(a.sel(), b.sel(), "selection {label}");
                            let route = actual.batch_route_snapshot();
                            assert_eq!(
                                (route.refusals, route.fallback),
                                (0, 0),
                                "{label}: {route:?}"
                            );
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn optional_phase_stateful_original_and_source() {
    if isolate_common_corpus("stateful_tests::optional_phase_stateful_original_and_source") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    let mut observed = (0usize, 0xcbf29ce484222325u64, Vec::new());
    for name in NAMES {
        for shape in 0..4 {
            for mask in 0..8 {
                for mode in [0usize] {
                    for profile in 0..6 {
                        for enabled in [false, true] {
                            for (vi, values) in values().into_iter().enumerate() {
                                for selection in [None, Some(vec![2, 0, 2]), Some(vec![])] {
                                    let make = || {
                                        let mut e = root(name, shape);
                                        erase_optional_call_declarations(&mut e, mask);
                                        suite(e)
                                    };
                                    let owner = make();
                                    let actual = make();
                                    let old = C::new(profile, enabled, values.clone());
                                    let new = C::new(profile, enabled, values.clone());
                                    let field = ty();
                                    let schema = [ty()];
                                    let input = || {
                                        let mut c = chunk(
                                            &schema,
                                            &vec![
                                                vec![Datum::Int(1)],
                                                vec![Datum::Int(2)],
                                                vec![Datum::Int(3)],
                                            ],
                                        );
                                        c.set_sel(selection.clone());
                                        c
                                    };
                                    let label = format!("stateful/{name}/{shape}/{mask}/{mode:?}/{profile}/{enabled}/{vi}/{selection:?}");
                                    observe_optional_phase_case(
                                        &label,
                                        [&owner, &actual],
                                        [&old, &new],
                                        |c| c.snapshot(),
                                        input,
                                        (&field, &schema),
                                        &mut observed,
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    eprintln!(
        "OPTIONAL PHASE stateful ORIGINAL {}/{:016X}",
        observed.0, observed.1
    );
    assert_eq!((observed.0, observed.1), (165888, 0xA8DFEEFEACCBFF75));
    if !observed.2.is_empty() {
        eprintln!("OPTIONAL PHASE stateful ADMISSION RED {:?}", observed.2);
    }
    assert!(observed.2.is_empty());
}

#[test]
fn optional_phase_stateful_full_actual_values_keep_raw_returns_and_casts() {
    if isolate_common_corpus(
        "stateful_tests::optional_phase_stateful_full_actual_values_keep_raw_returns_and_casts",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    let mut observed = (0usize, 0xcbf29ce484222325u64, Vec::new());
    for name in NAMES {
        for mask in [1, 2, 3, 7] {
            for (vi, value) in crate::tikv::byte_unary_value_tests::values()
                .into_iter()
                .enumerate()
            {
                for enabled in [false, true] {
                    for selection in [None, Some(vec![2, 0, 2]), Some(vec![])] {
                        // BENCHMARK receives one genuine iteration; all 149
                        // values are actual discarded body results, never an
                        // unbounded loop count in the original reference.
                        let values = if name == "benchmark" {
                            [Datum::Int(1), value.clone()]
                        } else {
                            [value.clone(), Datum::Int(0)]
                        };
                        let make = || {
                            let mut e = root(name, 0);
                            erase_optional_call_declarations(&mut e, mask);
                            suite(e)
                        };
                        let owner = make();
                        let actual = make();
                        let old = C::new(0, enabled, values.clone());
                        let new = C::new(0, enabled, values);
                        let schema = [ty()];
                        let input = || {
                            let mut c = chunk(
                                &schema,
                                &[
                                    vec![Datum::Int(1)],
                                    vec![Datum::Int(2)],
                                    vec![Datum::Int(3)],
                                ],
                            );
                            c.set_sel(selection.clone());
                            c
                        };
                        let label =
                            format!("full-stateful/{name}/{mask}/{vi}/{enabled}/{selection:?}");
                        observe_optional_phase_case(
                            &label,
                            [&owner, &actual],
                            [&old, &new],
                            |c| c.snapshot(),
                            input,
                            (&ty(), &schema),
                            &mut observed,
                        );
                    }
                }
            }
        }
    }
    eprintln!(
        "OPTIONAL STATEFUL FULL ACTUAL {}/{:016X}",
        observed.0, observed.1
    );
    assert_eq!((observed.0, observed.1), (57216, 0xD17BF84FC6D34845));
    assert!(observed.2.is_empty(), "{:?}", observed.2);
}

#[test]
fn remaining_optional_admission_inventory_keeps_original_behavior() {
    if isolate_common_corpus(
        "stateful_tests::remaining_optional_admission_inventory_keeps_original_behavior",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    use std::collections::{BTreeMap, BTreeSet};
    let mut routes = BTreeMap::<String, (usize, usize, BTreeSet<String>)>::new();
    let mut mismatches = vec![];
    let mut count = 0;
    for name in crate::scalar_function::eager_policy_tests::names() {
        for arity in crate::scalar_function::eager_policy_tests::arities(&name) {
            for mask in [0u8, 1, 2, 3, 4, 7] {
                for kind in ["literal", "param", "column"] {
                    for enabled in [false, true] {
                        let make = || {
                            let args = (0..arity)
                                .map(|_| match kind {
                                    "literal" => literal(Datum::Null, string_ty()),
                                    "param" => parameter(0, string_ty()),
                                    _ => string_column(0),
                                })
                                .collect();
                            let mut e = call(&name, string_ty(), args);
                            erase_optional_call_declarations(&mut e, mask);
                            suite(e)
                        };
                        let original = make();
                        let actual = make();
                        // Real mock session providers avoid sleeping/locking the
                        // machine while preserving their original call records.
                        let old = C::new(0, enabled, [Datum::Null, Datum::Null]);
                        let new = C::new(0, enabled, [Datum::Null, Datum::Null]);
                        let input = || chunk(&[string_ty()], &[vec![Datum::Null]]);
                        let mut left = output_for(&string_ty());
                        let mut right = output_for(&string_ty());
                        let expected =
                            session_read_outcome(|| original.run(&old, &mut input(), &mut left));
                        let got = session_read_outcome(|| {
                            actual.run_with_tikv_batch(
                                &new,
                                &mut input(),
                                &mut right,
                                &[string_ty()],
                            )
                        });
                        let label = format!("{name}/{arity}/mask{mask}/{kind}/{enabled}");
                        // Actual entropy contracts are covered independently by
                        // optional_common_entropy; two OS draws differ in bytes.
                        let entropy =
                            arity == 0 && matches!(name.as_str(), "uuid" | "uuid_v4" | "uuid_v7");
                        if got != expected
                            || (!entropy && left.column(0) != right.column(0))
                            || old.snapshot() != new.snapshot()
                        {
                            if mismatches.len() < 12 {
                                mismatches.push(format!("{label}: old={expected} new={got}; oldeffects={} neweffects={}",old.snapshot(),new.snapshot()));
                            }
                        }
                        let route = actual.batch_route_snapshot();
                        let entry = routes.entry(name.clone()).or_default();
                        entry.0 += 1;
                        if route.fallback != 0 || route.refusals != 0 {
                            entry.1 += 1;
                            entry.2.insert(format!(
                                "arity{arity}/mask{mask}/{kind}/{:?}",
                                route.refusal
                            ));
                        }
                        count += 1;
                    }
                }
            }
        }
    }
    let refused: usize = routes.values().map(|v| v.1).sum();
    eprintln!(
        "REMAINING ADMISSION INVENTORY {count} cases / {refused} refused / {} names",
        routes.values().filter(|v| v.1 > 0).count()
    );
    for (name, (total, refused, examples)) in routes {
        if refused > 0 {
            eprintln!(
                "REMAINING {name} {refused}/{total} {:?}",
                examples.iter().take(8).collect::<Vec<_>>()
            );
        }
    }
    if !mismatches.is_empty() {
        eprintln!("REMAINING INVENTORY DIFFERENCES {mismatches:?}");
    }
    assert!(mismatches.is_empty());
}

#[test]
fn common_full_declarations_wrong_numeric_arities_original() {
    if isolate_common_corpus(
        "stateful_tests::common_full_declarations_wrong_numeric_arities_original",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_common_invalid_numeric_arity_matrix(false);
}
#[test]
fn common_full_declarations_wrong_numeric_arities_source() {
    if isolate_common_corpus(
        "stateful_tests::common_full_declarations_wrong_numeric_arities_source",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_common_invalid_numeric_arity_matrix(true);
}
fn run_common_invalid_numeric_arity_matrix(compare: bool) {
    let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
    let mut gaps = vec![];
    for name in ["plus", "minus", "mul", "div", "mod", "intdiv"] {
        for arity in [0, 1, 3, 4, 5, 6] {
            for kind in ["literal", "param", "deferred"] {
                for (vi, value) in [Datum::Null, Datum::Int(7), Datum::new_string("abc")]
                    .into_iter()
                    .enumerate()
                {
                    for enabled in [false, true] {
                        for selection in [None, Some(vec![2, 0, 2]), Some(vec![])] {
                            let make = || {
                                let args = (0..arity)
                                    .map(|_| match kind {
                                        "literal" => literal(value.clone(), string_ty()),
                                        "param" => parameter(0, string_ty()),
                                        _ => {
                                            let mut c = Constant::new(
                                                Datum::new_string("stored"),
                                                string_ty(),
                                            );
                                            c.deferred_expr = Some(Box::new(leaf::assignment(
                                                "wrong-arity",
                                                parameter(0, string_ty()),
                                                &string_ty(),
                                            )));
                                            Expression::Constant(c)
                                        }
                                    })
                                    .collect();
                                suite(call(name, string_ty(), args))
                            };
                            let original = make();
                            let actual = make();
                            let old = C::new(0, enabled, [value.clone(), Datum::Null]);
                            let new = C::new(0, enabled, [value.clone(), Datum::Null]);
                            for reuse in 0..2 {
                                let input = || {
                                    let mut c = chunk(
                                        &[ty()],
                                        &[
                                            vec![Datum::Int(1)],
                                            vec![Datum::Int(2)],
                                            vec![Datum::Int(3)],
                                        ],
                                    );
                                    c.set_sel(selection.clone());
                                    c
                                };
                                let mut a = input();
                                let mut left = output_for(&string_ty());
                                left.append_null(0);
                                let expected =
                                    session_read_outcome(|| original.run(&old, &mut a, &mut left));
                                let label=format!("invalid-numeric/{name}/{arity}/{kind}/{vi}/{enabled}/{selection:?}/{reuse}");
                                if compare {
                                    let mut b = input();
                                    let mut right = output_for(&string_ty());
                                    right.append_null(0);
                                    let got = session_read_outcome(|| {
                                        actual.run_with_tikv_batch(
                                            &new,
                                            &mut b,
                                            &mut right,
                                            &[ty()],
                                        )
                                    });
                                    if got != expected
                                        || left.column(0) != right.column(0)
                                        || old.snapshot() != new.snapshot()
                                    {
                                        eprintln!("INVALID NUMERIC DIFFERENCE {label} old={expected:?} new={got:?} oldeffects={} neweffects={}",old.snapshot(),new.snapshot());
                                    }
                                    assert_eq!(got, expected, "{label}");
                                    assert_eq!(right.column(0), left.column(0), "{label}");
                                    assert_eq!(new.snapshot(), old.snapshot(), "{label}");
                                    let route = actual.batch_route_snapshot();
                                    if (route.fallback != 0 || route.refusals != 0)
                                        && gaps.len() < 20
                                    {
                                        gaps.push(format!("{label}: {route:?}"));
                                    }
                                }
                                for byte in format!(
                                    "{label}|{expected}|{:?}|{}",
                                    left.column(0),
                                    old.snapshot()
                                )
                                .bytes()
                                {
                                    hash = (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3);
                                }
                                count += 1;
                            }
                        }
                    }
                }
            }
        }
    }
    eprintln!("INVALID NUMERIC ORIGINAL {count}/{hash:016X}");
    assert_eq!((count, hash), (3888, 0x18148BA3F9A21EC9));
    if !gaps.is_empty() {
        eprintln!("INVALID NUMERIC ADMISSION RED {gaps:?}");
    }
    assert!(gaps.is_empty());
}

#[test]
fn optional_selection_original_metadata_and_lazy_demands() {
    if isolate_common_corpus(
        "stateful_tests::optional_selection_original_metadata_and_lazy_demands",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_optional_selection_matrix(false);
}
#[test]
fn optional_selection_actual_metadata_and_lazy_demands() {
    if isolate_common_corpus("stateful_tests::optional_selection_actual_metadata_and_lazy_demands")
    {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_optional_selection_matrix(true);
}
fn run_optional_selection_matrix(compare: bool) {
    let names = [
        "if",
        "case",
        "ifnull",
        "coalesce",
        "and",
        "or",
        "not",
        "isnull",
        "istrue",
        "isfalse",
        "isnottrue",
        "isnotfalse",
        "isunknown",
        "isnotunknown",
        "istrue_with_null",
    ];
    let values = [
        [Datum::Null, Datum::new_string("selected")],
        [Datum::Int(0), Datum::Int(7)],
        [Datum::Int(1), Datum::new_string("7")],
        [Datum::new_string("1tail"), Datum::Bytes(vec![255])],
        [Datum::MinNotNull, Datum::Null],
        [Datum::Real(f64::NAN), Datum::new_string("null")],
    ];
    let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
    let mut gaps = vec![];
    let mut retained_bulk_gaps = 0usize;
    for name in names {
        for shape in 0..5 {
            for mask in 0..8 {
                for (fi, field) in [ty(), string_ty(), FieldType::new(FieldTypeCode::Json)]
                    .into_iter()
                    .enumerate()
                {
                    for (vi, values) in values.iter().cloned().enumerate() {
                        for profile in [0, 4, 5] {
                            for enabled in [false, true] {
                                for selection in [None, Some(vec![2, 0, 2]), Some(vec![])] {
                                    let make = || {
                                        let p0 = parameter(0, string_ty());
                                        let p1 = parameter(1, string_ty());
                                        let fallback =
                                            literal(Datum::new_string("fallback"), string_ty());
                                        let mut args = match name {
                                            "if" | "case" => vec![p0, p1, fallback],
                                            "ifnull" | "coalesce" | "and" | "or" => vec![p0, p1],
                                            _ => vec![p0],
                                        };
                                        match shape {
                                            1 => {
                                                for (index, arg) in args.iter_mut().enumerate() {
                                                    let mut c = Constant::new(
                                                        Datum::new_string("stored deferred"),
                                                        string_ty(),
                                                    );
                                                    c.deferred_expr =
                                                        Some(Box::new(leaf::assignment(
                                                            &format!("selected{index}"),
                                                            arg.clone(),
                                                            &string_ty(),
                                                        )));
                                                    *arg = Expression::Constant(c);
                                                }
                                            }
                                            2 => {
                                                let last = args.len() - 1;
                                                args[last] = call(
                                                    "__actual_unselected_error__",
                                                    string_ty(),
                                                    vec![],
                                                );
                                            }
                                            3 => {
                                                let index = usize::from(args.len() > 1);
                                                args[index] = string_column(0);
                                            }
                                            4 => {
                                                let index = usize::from(args.len() > 1);
                                                args[index] = call(
                                                    "ifnull",
                                                    string_ty(),
                                                    vec![
                                                        args[index].clone(),
                                                        literal(
                                                            Datum::new_string("nested"),
                                                            string_ty(),
                                                        ),
                                                    ],
                                                );
                                            }
                                            _ => (),
                                        }
                                        let mut e = call(name, field.clone(), args);
                                        erase_optional_call_declarations(&mut e, mask);
                                        suite(e)
                                    };
                                    let original = make();
                                    let actual = make();
                                    let old = C::new(profile, enabled, values.clone());
                                    let new = C::new(profile, enabled, values.clone());
                                    // Record retained whole-selection profiles
                                    // separately; they are an explicit existing
                                    // entry gap, not an optional row declaration.
                                    let root = &original.program.calculated[0];
                                    let whole = original.program.vectorizable
                                        && crate::scalar_function::static_batch_target(root)
                                            .is_some()
                                        && contains_context_value(root, 128, &mut 4096);
                                    for reuse in 0..2 {
                                        let input = || {
                                            let mut c = chunk(
                                                &[string_ty()],
                                                &[
                                                    vec![Datum::new_string("a")],
                                                    vec![Datum::new_string("b")],
                                                    vec![Datum::new_string("c")],
                                                ],
                                            );
                                            c.set_sel(selection.clone());
                                            c
                                        };
                                        let mut a = input();
                                        let mut left = output_for(&field);
                                        left.append_null(0);
                                        let expected = session_read_outcome(|| {
                                            original.run(&old, &mut a, &mut left)
                                        });
                                        let label=format!("optional-selection/{name}/{shape}/{mask}/{fi}/{vi}/{profile}/{enabled}/{selection:?}/{reuse}");
                                        if compare {
                                            let mut b = input();
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
                                            if got != expected
                                                || left.column(0) != right.column(0)
                                                || old.snapshot() != new.snapshot()
                                            {
                                                eprintln!("OPTIONAL SELECTION DIFFERENCE {label} old={expected:?} new={got:?} oldcol={:?} newcol={:?} oldeffects={} neweffects={}",left.column(0),right.column(0),old.snapshot(),new.snapshot());
                                            }
                                            assert_eq!(got, expected, "{label}");
                                            assert_eq!(right.column(0), left.column(0), "{label}");
                                            assert_eq!(new.snapshot(), old.snapshot(), "{label}");
                                            let route = actual.batch_route_snapshot();
                                            if route.fallback != 0 || route.refusals != 0 {
                                                if whole {
                                                    retained_bulk_gaps += 1;
                                                } else if gaps.len() < 20 {
                                                    gaps.push(format!("{label}: {route:?}"));
                                                }
                                            }
                                        }
                                        for byte in format!(
                                            "{label}|{expected}|{:?}|{}",
                                            left.column(0),
                                            old.snapshot()
                                        )
                                        .bytes()
                                        {
                                            hash = (hash ^ u64::from(byte))
                                                .wrapping_mul(0x100000001b3);
                                        }
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
    eprintln!("OPTIONAL SELECTION ORIGINAL {count}/{hash:016X}; retained whole profiles {retained_bulk_gaps}");
    assert_eq!((count, hash), (388800, 0xED53E168732558ED));
    if !gaps.is_empty() {
        eprintln!("OPTIONAL SELECTION ROW ADMISSION RED {gaps:?}");
    }
    assert!(gaps.is_empty());
}
