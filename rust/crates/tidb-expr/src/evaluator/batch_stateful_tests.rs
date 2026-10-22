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
    // Duration return conversion uses the real current calendar date. Replay
    // the historical witness's explicit date with the existing test-only scope;
    // keep its original values, raw comparisons and digest unchanged.
    let now = chrono::DateTime::parse_from_rfc3339("2026-10-21T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    tidb_datatype::with_test_conversion_clock(now, || {
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
                        function.args[1] =
                            parameter(1, FieldType::new(FieldTypeCode::VectorFloat32));
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
                                let label = format!(
                                    "{fi}/{vi}/{variant}/{profile}/{enabled}/{selection:?}"
                                );
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
    });
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
    run_optional_control_matrix(
        compare,
        &names,
        &values,
        "optional-selection",
        "OPTIONAL SELECTION",
        Some((388800, 0xED53E168732558ED)),
    );
}

#[test]
fn optional_operator_original_metadata_and_demands() {
    if isolate_common_corpus("stateful_tests::optional_operator_original_metadata_and_demands") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_optional_operator_matrix(false);
}
#[test]
fn optional_operator_actual_metadata_and_demands() {
    if isolate_common_corpus("stateful_tests::optional_operator_actual_metadata_and_demands") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_optional_operator_matrix(true);
}
const OPTIONAL_OPERATOR_NAMES: [&str; 17] = [
    "bitand",
    "bitor",
    "bitxor",
    "leftshift",
    "rightshift",
    "bitneg",
    "unaryplus",
    "unaryminus",
    "xor",
    "eq",
    "ne",
    "lt",
    "le",
    "gt",
    "ge",
    "nulleq",
    "nullif",
];

fn run_optional_operator_matrix(compare: bool) {
    let names = OPTIONAL_OPERATOR_NAMES;
    let binary =
        |bytes: Vec<u8>| Datum::new_binary_literal(tidb_datatype::BinaryLiteral::from(bytes));
    let values = [
        [Datum::Null, Datum::new_string("selected")],
        [Datum::Int(0), Datum::Int(7)],
        [Datum::Int(1), Datum::new_string("7")],
        [Datum::new_string("1tail"), Datum::Bytes(vec![255])],
        [Datum::MinNotNull, Datum::Null],
        [Datum::Real(f64::NAN), Datum::new_string("null")],
        [binary(vec![255; 8]), Datum::UInt(u64::MAX)],
        [binary(vec![0, 255]), Datum::Int(255)],
        [binary(vec![255; 9]), binary(vec![128, 0, 0, 0, 0, 0, 0, 0])],
        [Datum::UInt(u64::MAX), Datum::Int(-1)],
    ];
    run_optional_control_matrix(
        compare,
        &names,
        &values,
        "optional-operator",
        "OPTIONAL OPERATOR",
        Some((734400, 0x13E31BEF014E40C9)),
    );
}
fn run_optional_control_matrix(
    compare: bool,
    names: &[&str],
    values: &[[Datum; 2]],
    prefix: &str,
    heading: &str,
    golden: Option<(usize, u64)>,
) {
    let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
    let mut gaps = vec![];
    let mut retained_bulk_gaps = 0usize;
    for &name in names {
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
                                            "not" | "isnull" | "istrue" | "isfalse"
                                            | "isnottrue" | "isnotfalse" | "isunknown"
                                            | "isnotunknown" | "istrue_with_null" | "bitneg"
                                            | "unaryplus" | "unaryminus" | "charset"
                                            | "collation" | "coercibility" => vec![p0],
                                            _ => vec![p0, p1],
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
                                        let label=format!("{prefix}/{name}/{shape}/{mask}/{fi}/{vi}/{profile}/{enabled}/{selection:?}/{reuse}");
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
                                                eprintln!("{heading} DIFFERENCE {label} old={expected:?} new={got:?} oldcol={:?} newcol={:?} oldeffects={} neweffects={}",left.column(0),right.column(0),old.snapshot(),new.snapshot());
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
                                        if matches!(name, "charset" | "collation" | "coercibility")
                                        {
                                            assert_eq!(
                                                (old.reads[0].get(), old.reads[1].get()),
                                                (0, 0),
                                                "{label}"
                                            );
                                            if compare {
                                                assert_eq!(
                                                    (new.reads[0].get(), new.reads[1].get()),
                                                    (0, 0),
                                                    "{label}"
                                                );
                                            }
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
    eprintln!(
        "{heading} ORIGINAL {count}/{hash:016X}; retained whole profiles {retained_bulk_gaps}"
    );
    if let Some(golden) = golden {
        assert_eq!((count, hash), golden);
    }
    if !gaps.is_empty() {
        eprintln!("{heading} ROW ADMISSION RED {gaps:?}");
    }
    assert!(gaps.is_empty());
}

#[test]
fn optional_operator_full_actual_values_keep_raw_domains_and_facts() {
    if isolate_common_corpus(
        "stateful_tests::optional_operator_full_actual_values_keep_raw_domains_and_facts",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
    for name in OPTIONAL_OPERATOR_NAMES {
        for mask in [1, 2, 3, 7] {
            for (vi, value) in crate::tikv::byte_unary_value_tests::values()
                .into_iter()
                .enumerate()
            {
                for enabled in [false, true] {
                    for selection in [None, Some(vec![2, 0, 2]), Some(vec![])] {
                        let make = || {
                            let mut args = vec![parameter(0, string_ty())];
                            if !matches!(name, "bitneg" | "unaryplus" | "unaryminus") {
                                args.push(parameter(1, string_ty()));
                            }
                            let mut expression = call(name, ty(), args);
                            erase_optional_call_declarations(&mut expression, mask);
                            suite(expression)
                        };
                        let original = make();
                        let actual = make();
                        let old = C::new(0, enabled, [value.clone(), Datum::Int(1)]);
                        let new = C::new(0, enabled, [value.clone(), Datum::Int(1)]);
                        for reuse in 0..2 {
                            let input = || {
                                let mut input = chunk(
                                    &[ty()],
                                    &[
                                        vec![Datum::Int(1)],
                                        vec![Datum::Int(2)],
                                        vec![Datum::Int(3)],
                                    ],
                                );
                                input.set_sel(selection.clone());
                                input
                            };
                            let mut a = input();
                            let mut left = output_for(&ty());
                            left.append_null(0);
                            let expected =
                                session_read_outcome(|| original.run(&old, &mut a, &mut left));
                            let mut b = input();
                            let mut right = output_for(&ty());
                            right.append_null(0);
                            let got = session_read_outcome(|| {
                                actual.run_with_tikv_batch(&new, &mut b, &mut right, &[ty()])
                            });
                            let label = format!("operator-full-actual/{name}/{mask}/{vi}/{enabled}/{selection:?}/{reuse}");
                            if got != expected
                                || right.column(0) != left.column(0)
                                || new.snapshot() != old.snapshot()
                            {
                                eprintln!("OPERATOR FULL DIFFERENCE {label} value={} old={expected:?} new={got:?} oldcol={:?} newcol={:?} oldeffects={} neweffects={}",leaf::frame(&value),left.column(0),right.column(0),old.snapshot(),new.snapshot());
                            }
                            assert_eq!(got, expected, "{label}");
                            assert_eq!(right.column(0), left.column(0), "{label}");
                            assert_eq!(new.snapshot(), old.snapshot(), "{label}");
                            let route = actual.batch_route_snapshot();
                            assert_eq!((route.fallback, route.refusals), (0, 0), "{label}");
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
    eprintln!("OPERATOR FULL ACTUAL {count}/{hash:016X}");
    assert_eq!((count, hash), (121584, 0x762EE08F41914785));
}

#[test]
fn no_argument_header_original_outcomes_and_skipped_children() {
    if isolate_common_corpus(
        "stateful_tests::no_argument_header_original_outcomes_and_skipped_children",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_no_argument_header_matrix(false);
}
#[test]
fn no_argument_header_actual_outcomes_and_skipped_children() {
    if isolate_common_corpus(
        "stateful_tests::no_argument_header_actual_outcomes_and_skipped_children",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_no_argument_header_matrix(true);
}
fn run_no_argument_header_matrix(compare: bool) {
    let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
    let mut gaps = vec![];
    for name in [
        "istrue",
        "istrue_with_null",
        "isnottrue",
        "isfalse",
        "isnotfalse",
        "isunknown",
        "isnotunknown",
        "case",
        "coalesce",
    ] {
        for arity in 0..=6 {
            if if matches!(name, "case" | "coalesce") {
                arity != 0
            } else {
                arity == 1
            } {
                continue;
            }
            for shape in 0..4 {
                for mask in 0..8 {
                    for (fi, field) in [ty(), string_ty(), FieldType::new(FieldTypeCode::Json)]
                        .into_iter()
                        .enumerate()
                    {
                        for profile in [0, 4, 5] {
                            for enabled in [false, true] {
                                for selection in [None, Some(vec![2, 0, 2]), Some(vec![])] {
                                    let make = || {
                                        let args = (0..arity)
                                            .map(|index| {
                                                let param = parameter(index % 2, string_ty());
                                                match shape {
                                                    1 => {
                                                        let mut c = Constant::new(
                                                            Datum::new_string(
                                                                "unread stored value",
                                                            ),
                                                            string_ty(),
                                                        );
                                                        c.deferred_expr =
                                                            Some(Box::new(leaf::assignment(
                                                                &format!("unread{index}"),
                                                                param,
                                                                &string_ty(),
                                                            )));
                                                        Expression::Constant(c)
                                                    }
                                                    2 => string_column(0),
                                                    3 => literal(
                                                        Datum::Decimal(
                                                            tidb_datatype::Decimal::from_raw_parts(
                                                                true,
                                                                vec![0, 255, b'0'],
                                                                u32::MAX,
                                                                3,
                                                            ),
                                                        ),
                                                        string_ty(),
                                                    ),
                                                    _ => param,
                                                }
                                            })
                                            .collect();
                                        let mut root = call(name, field.clone(), args);
                                        erase_optional_call_declarations(&mut root, mask);
                                        suite(root)
                                    };
                                    let original = make();
                                    let actual = make();
                                    let values =
                                        [Datum::new_string("1bad"), Datum::new_string("bad")];
                                    let old = C::new(profile, enabled, values.clone());
                                    let new = C::new(profile, enabled, values);
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
                                        let label=format!("no-argument-header/{name}/{arity}/{shape}/{mask}/{fi}/{profile}/{enabled}/{selection:?}/{reuse}");
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
                                                || right.column(0) != left.column(0)
                                                || new.snapshot() != old.snapshot()
                                            {
                                                eprintln!("HEADER DIFFERENCE {label} old={expected:?} new={got:?} oldcol={:?} newcol={:?} oldeffects={} neweffects={}",left.column(0),right.column(0),old.snapshot(),new.snapshot());
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
                                        assert_eq!(
                                            (old.reads[0].get(), old.reads[1].get()),
                                            (0, 0),
                                            "{label}"
                                        );
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
    eprintln!("HEADER ORIGINAL {count}/{hash:016X}");
    assert_eq!((count, hash), (152064, 0x0A12BC1F0AB942C5));
    if !gaps.is_empty() {
        eprintln!("HEADER ADMISSION RED {gaps:?}");
    }
    assert!(gaps.is_empty());
}

#[test]
fn no_argument_header_nested_consumers_keep_skips_and_prefixes() {
    if isolate_common_corpus(
        "stateful_tests::no_argument_header_nested_consumers_keep_skips_and_prefixes",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
    for name in [
        "istrue",
        "istrue_with_null",
        "isnottrue",
        "isfalse",
        "isnotfalse",
        "isunknown",
        "isnotunknown",
        "case",
        "coalesce",
    ] {
        for mask in 0..8 {
            for wrapper in 0..5 {
                for profile in [0, 4, 5] {
                    for enabled in [false, true] {
                        for selection in [None, Some(vec![2, 0, 2]), Some(vec![])] {
                            let make = || {
                                let args = if matches!(name, "case" | "coalesce") {
                                    vec![]
                                } else {
                                    vec![parameter(0, string_ty()), parameter(1, string_ty())]
                                };
                                let mut header = call(name, ty(), args);
                                erase_optional_call_declarations(&mut header, mask);
                                let root = match wrapper {
                                    0 => call("any_value", ty(), vec![header]),
                                    1 => call("if", ty(), vec![column(0), header, int(9)]),
                                    2 => call("if", ty(), vec![int(0), header, int(9)]),
                                    3 => call("coalesce", ty(), vec![int(7), header]),
                                    _ => {
                                        let mut c = Constant::new(Datum::Int(99), ty());
                                        c.deferred_expr = Some(Box::new(header));
                                        Expression::Constant(c)
                                    }
                                };
                                suite(root)
                            };
                            let original = make();
                            let actual = make();
                            let values = [Datum::new_string("1bad"), Datum::new_string("bad")];
                            let old = C::new(profile, enabled, values.clone());
                            let new = C::new(profile, enabled, values);
                            for reuse in 0..2 {
                                let input = || {
                                    let mut c = chunk(
                                        &[ty()],
                                        &[
                                            vec![Datum::Int(0)],
                                            vec![Datum::Int(1)],
                                            vec![Datum::Int(2)],
                                        ],
                                    );
                                    c.set_sel(selection.clone());
                                    c
                                };
                                let mut a = input();
                                let mut left = output_for(&ty());
                                left.append_null(0);
                                let expected =
                                    session_read_outcome(|| original.run(&old, &mut a, &mut left));
                                let mut b = input();
                                let mut right = output_for(&ty());
                                right.append_null(0);
                                let got = session_read_outcome(|| {
                                    actual.run_with_tikv_batch(&new, &mut b, &mut right, &[ty()])
                                });
                                let label=format!("nested-header/{name}/{mask}/{wrapper}/{profile}/{enabled}/{selection:?}/{reuse}");
                                if got != expected
                                    || left.column(0) != right.column(0)
                                    || old.snapshot() != new.snapshot()
                                {
                                    eprintln!("NESTED HEADER DIFFERENCE {label} old={expected:?} new={got:?} oldcol={:?} newcol={:?} oldeffects={} neweffects={}",left.column(0),right.column(0),old.snapshot(),new.snapshot());
                                }
                                assert_eq!(got, expected, "{label}");
                                assert_eq!(right.column(0), left.column(0), "{label}");
                                assert_eq!(new.snapshot(), old.snapshot(), "{label}");
                                let route = actual.batch_route_snapshot();
                                assert_eq!(
                                    (route.fallback, route.refusals),
                                    (0, 0),
                                    "{label}: {route:?}"
                                );
                                assert_eq!(
                                    (old.reads[0].get(), old.reads[1].get()),
                                    (0, 0),
                                    "{label}"
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
    }
    eprintln!("NESTED HEADER {count}/{hash:016X}");
    assert_eq!((count, hash), (12960, 0xBAEC627349AA50FD));
}

#[test]
fn metadata_property_original_declaration_source() {
    if isolate_common_corpus("stateful_tests::metadata_property_original_declaration_source") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_metadata_property_declarations(false);
}
#[test]
fn metadata_property_actual_declaration_source() {
    if isolate_common_corpus("stateful_tests::metadata_property_actual_declaration_source") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_metadata_property_declarations(true);
}
fn run_metadata_property_declarations(compare: bool) {
    let values = [
        [Datum::Null, Datum::new_string("unread")],
        [Datum::Int(0), Datum::Int(7)],
        [Datum::Int(1), Datum::new_string("7")],
        [Datum::new_string("1tail"), Datum::Bytes(vec![255])],
        [Datum::MinNotNull, Datum::Null],
        [Datum::Real(f64::NAN), Datum::new_string("null")],
    ];
    run_optional_control_matrix(
        compare,
        &["charset", "collation", "coercibility"],
        &values,
        "metadata-property",
        "PROPERTY DECLARATION",
        Some((77760, 0xE2A74FA0BA7D91F5)),
    );
}
#[test]
fn metadata_property_original_stored_attributes() {
    if isolate_common_corpus("stateful_tests::metadata_property_original_stored_attributes") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_metadata_property_stored(false);
}
#[test]
fn metadata_property_actual_stored_attributes() {
    if isolate_common_corpus("stateful_tests::metadata_property_actual_stored_attributes") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_metadata_property_stored(true);
}
fn run_metadata_property_stored(compare: bool) {
    use crate::expr_collation::Coercibility;
    let custom = |charset: &str, collation: &str| {
        let mut f = string_ty();
        f.set_charset_name(charset);
        f.set_collation_name(collation);
        f
    };
    let fields = [
        string_ty(),
        native_bytes_type(),
        ty(),
        FieldType::new(FieldTypeCode::Json),
        custom("ascii", "ascii_bin"),
        custom("gb18030", "gb18030_bin"),
        custom("unregistered-长charset", "unregistered-长collation"),
    ];
    let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
    let mut gaps = vec![];
    for name in ["charset", "collation", "coercibility"] {
        let result = if name == "coercibility" {
            ty()
        } else {
            string_ty()
        };
        for (fi, field) in fields.iter().enumerate() {
            for shape in 0..7 {
                for stored in 0..3 {
                    for mask in [0, 1, 2, 3] {
                        for enabled in [false, true] {
                            for selection in [None, Some(vec![2, 0, 2]), Some(vec![])] {
                                let make = || {
                                    let mut arg = match shape {
                                        0 => literal(Datum::Null, field.clone()),
                                        1 => parameter(99, field.clone()),
                                        2 => {
                                            let mut c = Constant::new(
                                                Datum::new_string("unread"),
                                                field.clone(),
                                            );
                                            c.deferred_expr = Some(Box::new(leaf::assignment(
                                                "unread",
                                                parameter(99, field.clone()),
                                                field,
                                            )));
                                            Expression::Constant(c)
                                        }
                                        3 => {
                                            let mut c =
                                                crate::column::Column::new(1, field.clone());
                                            c.index = 0;
                                            Expression::Column(c)
                                        }
                                        4 => call(
                                            "any_value",
                                            field.clone(),
                                            vec![parameter(99, field.clone())],
                                        ),
                                        5 => leaf::expression(
                                            "correlated_absent",
                                            0,
                                            &Datum::Int(99),
                                            Some(field),
                                            None,
                                        ),
                                        _ => literal(
                                            Datum::Decimal(tidb_datatype::Decimal::from_raw_parts(
                                                true,
                                                vec![0, 255, b'0'],
                                                u32::MAX,
                                                3,
                                            )),
                                            field.clone(),
                                        ),
                                    };
                                    let info = match &mut arg {
                                        Expression::Constant(c) => &mut c.collation,
                                        Expression::Column(c) => &mut c.collation,
                                        Expression::CorrelatedColumn(c) => &mut c.column.collation,
                                        Expression::ScalarFunction(f) => &mut f.collation,
                                    };
                                    info.set_charset_and_collation(
                                        "stored-charset",
                                        "stored-collation",
                                    );
                                    if stored == 1 {
                                        info.set_coercibility(Coercibility::EXPLICIT);
                                    } else if stored == 2 {
                                        info.set_coercibility(Coercibility::NUMERIC);
                                    }
                                    let mut root = call(name, result.clone(), vec![arg]);
                                    erase_optional_call_declarations(&mut root, mask);
                                    suite(root)
                                };
                                let original = make();
                                let actual = make();
                                let values =
                                    [Datum::new_string("unread0"), Datum::new_string("unread1")];
                                let old = C::new(0, enabled, values.clone());
                                let new = C::new(0, enabled, values);
                                for reuse in 0..2 {
                                    let input = || {
                                        let mut c = chunk(
                                            &[string_ty()],
                                            &[
                                                vec![Datum::new_string("rowa")],
                                                vec![Datum::new_string("rowb")],
                                                vec![Datum::new_string("rowc")],
                                            ],
                                        );
                                        c.set_sel(selection.clone());
                                        c
                                    };
                                    let mut a = input();
                                    let mut left = output_for(&result);
                                    left.append_null(0);
                                    let expected = session_read_outcome(|| {
                                        original.run(&old, &mut a, &mut left)
                                    });
                                    let label=format!("property-stored/{name}/{fi}/{shape}/{stored}/{mask}/{enabled}/{selection:?}/{reuse}");
                                    if compare {
                                        let mut b = input();
                                        let mut right = output_for(&result);
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
                                            eprintln!("PROPERTY STORED DIFFERENCE {label} old={expected:?} new={got:?} oldcol={:?} newcol={:?} oldeffects={} neweffects={}",left.column(0),right.column(0),old.snapshot(),new.snapshot());
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
                                    assert_eq!(
                                        (old.reads[0].get(), old.reads[1].get()),
                                        (0, 0),
                                        "{label}"
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
        }
    }
    eprintln!("PROPERTY STORED ORIGINAL {count}/{hash:016X}");
    assert_eq!((count, hash), (21168, 0x0FF4CB511A024679));
    if !gaps.is_empty() {
        eprintln!("PROPERTY STORED ADMISSION RED {gaps:?}");
    }
    assert!(gaps.is_empty());
}

#[test]
fn metadata_property_nested_consumers_keep_original_domains_and_diagnostics() {
    if isolate_common_corpus(
        "stateful_tests::metadata_property_nested_consumers_keep_original_domains_and_diagnostics",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    let duration = FieldType::new(FieldTypeCode::Duration).with_decimal(6);
    let (mut count, mut hash, mut retained_numeric) = (0usize, 0xcbf29ce484222325u64, 0usize);
    for name in ["charset", "collation", "coercibility"] {
        for text in ["utf8mb4", "-9223372036854775808", "0"] {
            for wrapper in 0..8 {
                for mask in [0, 1, 2, 3] {
                    for enabled in [false, true] {
                        for selection in [None, Some(vec![2, 0, 2]), Some(vec![])] {
                            let make = || {
                                let mut field = string_ty();
                                field.set_charset_name(text);
                                field.set_collation_name(text);
                                let mut c = crate::column::Column::new(1, field);
                                c.index = 0;
                                c.orig_name = "t.metadata".into();
                                let result = if wrapper == 6 { string_ty() } else { ty() };
                                let mut property = call(name, result, vec![Expression::Column(c)]);
                                erase_optional_call_declarations(&mut property, mask);
                                let root = match wrapper {
                                    0 => call("abs", ty(), vec![property]),
                                    1 => call("cot", real_ty(), vec![property]),
                                    2 => call("pow", real_ty(), vec![property, int(-1)]),
                                    3 => call("minus", ty(), vec![property, int(1)]),
                                    4 => call("any_value", ty(), vec![property]),
                                    5 => call("coalesce", ty(), vec![int(7), property]),
                                    6 => {
                                        let mut c = crate::column::Column::new(2, duration.clone());
                                        c.index = 1;
                                        c.orig_name = "t.duration".into();
                                        call("eq", ty(), vec![Expression::Column(c), property])
                                    }
                                    _ => call("if", ty(), vec![int(0), property, int(9)]),
                                };
                                let field = root.static_type().unwrap().clone();
                                (suite(root), field)
                            };
                            let (original, field) = make();
                            let (actual, _) = make();
                            let old = C::new(0, enabled, [Datum::Int(0), Datum::Int(1)]);
                            let new = C::new(0, enabled, [Datum::Int(0), Datum::Int(1)]);
                            for reuse in 0..2 {
                                let input = || {
                                    let mut c=chunk(&[string_ty(),duration.clone()],&(0..3).map(|i|vec![Datum::new_string(format!("unread{i}")),Datum::Duration(tidb_datatype::MySqlDuration::from_raw_parts((i+1)*1_000_000_000,6))]).collect::<Vec<_>>());
                                    c.set_sel(selection.clone());
                                    c
                                };
                                let mut a = input();
                                let mut left = output_for(&field);
                                left.append_null(0);
                                let expected =
                                    session_read_outcome(|| original.run(&old, &mut a, &mut left));
                                let mut b = input();
                                let mut right = output_for(&field);
                                right.append_null(0);
                                let got = session_read_outcome(|| {
                                    actual.run_with_tikv_batch(
                                        &new,
                                        &mut b,
                                        &mut right,
                                        &[string_ty(), duration.clone()],
                                    )
                                });
                                let label=format!("property-nested/{name}/{text}/{wrapper}/{mask}/{enabled}/{selection:?}/{reuse}");
                                if got != expected
                                    || left.column(0) != right.column(0)
                                    || old.snapshot() != new.snapshot()
                                {
                                    eprintln!("PROPERTY NESTED DIFFERENCE {label} old={expected:?} new={got:?} oldcol={:?} newcol={:?} oldeffects={} neweffects={}",left.column(0),right.column(0),old.snapshot(),new.snapshot());
                                }
                                assert_eq!(got, expected, "{label}");
                                assert_eq!(right.column(0), left.column(0), "{label}");
                                assert_eq!(new.snapshot(), old.snapshot(), "{label}");
                                let route = actual.batch_route_snapshot();
                                if wrapper == 3 && mask & 1 != 0 {
                                    if route.fallback != 0 || route.refusals != 0 {
                                        retained_numeric += 1;
                                    }
                                } else {
                                    assert_eq!(
                                        (route.fallback, route.refusals),
                                        (0, 0),
                                        "{label}: {route:?}"
                                    );
                                }
                                assert_eq!(
                                    (old.reads[0].get(), old.reads[1].get()),
                                    (0, 0),
                                    "{label}"
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
    }
    eprintln!("PROPERTY NESTED {count}/{hash:016X}; retained numeric {retained_numeric}");
    assert_eq!((count, hash), (3456, 0x3C005CEFBC8C583D));
}

#[test]
fn optional_grouping_original_values_and_metadata() {
    if isolate_common_corpus("stateful_tests::optional_grouping_original_values_and_metadata") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_optional_grouping_matrix(false, false);
}
#[test]
fn optional_grouping_actual_values_and_metadata() {
    if isolate_common_corpus("stateful_tests::optional_grouping_actual_values_and_metadata") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_optional_grouping_matrix(true, false);
}
#[test]
fn optional_grouping_full_actual_values() {
    if isolate_common_corpus("stateful_tests::optional_grouping_full_actual_values") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_optional_grouping_matrix(true, true);
}
fn run_optional_grouping_matrix(compare: bool, full: bool) {
    let metadata = std::iter::once(None)
        .chain(
            crate::tikv::metadata_value_tests::grouping_cases()
                .into_iter()
                .enumerate()
                .filter(|(i, _)| [1, 8, 13, 18, 21, 29].contains(i))
                .map(|(_, m)| Some(m)),
        )
        .collect::<Vec<_>>();
    let values = if full {
        crate::tikv::byte_unary_value_tests::values()
    } else {
        vec![
            Datum::Null,
            Datum::Int(0),
            Datum::UInt(u64::MAX),
            Datum::Int(i64::MIN),
            Datum::new_string("2tail"),
            Datum::Bytes(vec![255]),
            Datum::Decimal(tidb_datatype::Decimal::from_literal("2.5")),
            Datum::Real(f64::NAN),
        ]
    };
    let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
    let mut gaps = vec![];
    for (mi, metadata) in metadata.iter().enumerate() {
        for shape in 0..if full { 1 } else { 5 } {
            for mask in if full { 1..4 } else { 0..8 } {
                for (fi, field) in [ty(), string_ty(), FieldType::new(FieldTypeCode::Json)]
                    .into_iter()
                    .enumerate()
                {
                    for (vi, value) in values.iter().enumerate() {
                        for &profile in if full { &[0][..] } else { &[0, 5][..] } {
                            for enabled in [false, true] {
                                for selection in [None, Some(vec![2, 0, 2]), Some(vec![])] {
                                    let make = || {
                                        let p = parameter(0, string_ty());
                                        let arg = match shape {
                                            1 => {
                                                let mut c = Constant::new(
                                                    Datum::new_string("unread stored"),
                                                    string_ty(),
                                                );
                                                c.deferred_expr = Some(Box::new(leaf::assignment(
                                                    "group-id",
                                                    p,
                                                    &string_ty(),
                                                )));
                                                Expression::Constant(c)
                                            }
                                            2 => call(
                                                "__actual_grouping_id_error__",
                                                string_ty(),
                                                vec![],
                                            ),
                                            3 => string_column(0),
                                            4 => call(
                                                "ifnull",
                                                string_ty(),
                                                vec![p, literal(Datum::Int(0), string_ty())],
                                            ),
                                            _ => p,
                                        };
                                        let mut f = ScalarFunction::new(
                                            CiString::new("grouping"),
                                            field.clone(),
                                            vec![arg],
                                        );
                                        if let Some(m) = metadata {
                                            f.set_grouping_metadata(
                                                m.mode(),
                                                m.grouping_marks().to_vec(),
                                            )
                                            .unwrap();
                                        }
                                        let mut root = Expression::ScalarFunction(f);
                                        erase_optional_call_declarations(&mut root, mask);
                                        suite(root)
                                    };
                                    let original = make();
                                    let actual = make();
                                    let old =
                                        C::new(profile, enabled, [value.clone(), Datum::Int(1)]);
                                    let new =
                                        C::new(profile, enabled, [value.clone(), Datum::Int(1)]);
                                    for reuse in 0..2 {
                                        let input = || {
                                            let mut c = chunk(
                                                &[string_ty()],
                                                &[
                                                    vec![Datum::new_string("0")],
                                                    vec![Datum::new_string("1")],
                                                    vec![Datum::new_string("2")],
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
                                        let label=format!("optional-grouping/{mi}/{shape}/{mask}/{fi}/{vi}/{profile}/{enabled}/{selection:?}/{reuse}");
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
                                                eprintln!("OPTIONAL GROUPING DIFFERENCE {label} old={expected:?} new={got:?} oldcol={:?} newcol={:?} oldeffects={} neweffects={}",left.column(0),right.column(0),old.snapshot(),new.snapshot());
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
    if full {
        assert_eq!((count, hash), (112644, 0x398CB5D82320FEA4));
        eprintln!("OPTIONAL GROUPING FULL {count}/{hash:016X}");
    } else {
        assert_eq!((count, hash), (161280, 0x3563DD9E5E3C3E43));
        eprintln!("OPTIONAL GROUPING ORIGINAL {count}/{hash:016X}");
    }
    if !gaps.is_empty() {
        eprintln!("OPTIONAL GROUPING ADMISSION RED {gaps:?}");
    }
    assert!(gaps.is_empty());
}
#[test]
fn grouping_header_original_without_demand() {
    if isolate_common_corpus("stateful_tests::grouping_header_original_without_demand") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_grouping_header_matrix(false);
}
#[test]
fn grouping_header_actual_without_demand() {
    if isolate_common_corpus("stateful_tests::grouping_header_actual_without_demand") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_grouping_header_matrix(true);
}
fn run_grouping_header_matrix(compare: bool) {
    let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
    let mut gaps = vec![];
    for arity in [0, 2, 3, 4, 5, 6] {
        for shape in 0..4 {
            for mask in 0..8 {
                for (fi, field) in [ty(), string_ty(), FieldType::new(FieldTypeCode::Json)]
                    .into_iter()
                    .enumerate()
                {
                    for profile in [0, 4, 5] {
                        for enabled in [false, true] {
                            for selection in [None, Some(vec![2, 0, 2]), Some(vec![])] {
                                let make = || {
                                    let args = (0..arity)
                                        .map(|index| {
                                            let p = parameter(index % 2, string_ty());
                                            match shape {
                                                1 => {
                                                    let mut c = Constant::new(
                                                        Datum::new_string("unread stored"),
                                                        string_ty(),
                                                    );
                                                    c.deferred_expr =
                                                        Some(Box::new(leaf::assignment(
                                                            "unread-group-id",
                                                            p,
                                                            &string_ty(),
                                                        )));
                                                    Expression::Constant(c)
                                                }
                                                2 => string_column(0),
                                                3 => literal(
                                                    Datum::Decimal(
                                                        tidb_datatype::Decimal::from_raw_parts(
                                                            true,
                                                            vec![0, 255, b'0'],
                                                            u32::MAX,
                                                            3,
                                                        ),
                                                    ),
                                                    string_ty(),
                                                ),
                                                _ => p,
                                            }
                                        })
                                        .collect();
                                    let mut root = call("grouping", field.clone(), args);
                                    erase_optional_call_declarations(&mut root, mask);
                                    suite(root)
                                };
                                let original = make();
                                let actual = make();
                                let values = [Datum::Int(0), Datum::Int(1)];
                                let old = C::new(profile, enabled, values.clone());
                                let new = C::new(profile, enabled, values);
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
                                    let label=format!("grouping-header/{arity}/{shape}/{mask}/{fi}/{profile}/{enabled}/{selection:?}/{reuse}");
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
                                            eprintln!("GROUPING HEADER DIFFERENCE {label} old={expected:?} new={got:?} oldeffects={} neweffects={}",old.snapshot(),new.snapshot());
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
                                    assert_eq!(
                                        (old.reads[0].get(), old.reads[1].get()),
                                        (0, 0),
                                        "{label}"
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
        }
    }
    assert_eq!((count, hash), (20736, 0x7020D56952346925));
    eprintln!("GROUPING HEADER ORIGINAL {count}/{hash:016X}");
    if !gaps.is_empty() {
        eprintln!("GROUPING HEADER ADMISSION RED {gaps:?}");
    }
    assert!(gaps.is_empty());
}

#[test]
fn optional_wildcard_original_metadata_and_demands() {
    if isolate_common_corpus("stateful_tests::optional_wildcard_original_metadata_and_demands") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_optional_wildcard_matrix(false);
}
#[test]
fn optional_wildcard_actual_metadata_and_demands() {
    if isolate_common_corpus("stateful_tests::optional_wildcard_actual_metadata_and_demands") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_optional_wildcard_matrix(true);
}
fn run_optional_wildcard_matrix(compare: bool) {
    let cases = [
        [
            Datum::new_string("ab_C"),
            Datum::new_string("a%_c"),
            Datum::Int(92),
        ],
        [Datum::Null, Datum::Bytes(vec![255]), Datum::Int(92)],
        [
            Datum::new_string("test"),
            Datum::Null,
            Datum::Bytes(vec![255]),
        ],
        [
            Datum::new_string("test"),
            Datum::new_string("te%"),
            Datum::Null,
        ],
        [
            Datum::Bytes(vec![255, b'a']),
            Datum::Bytes(vec![255, b'%']),
            Datum::UInt(u64::MAX),
        ],
        [
            Datum::Decimal(tidb_datatype::Decimal::from_literal("2.5")),
            Datum::new_string("%"),
            Datum::new_string("999tail"),
        ],
        [
            Datum::new_string("中国"),
            Datum::new_string("中_"),
            Datum::Decimal(tidb_datatype::Decimal::from_literal("9223372036854775808")),
        ],
        [
            Datum::Real(f64::NAN),
            Datum::new_string("%"),
            Datum::Int(i64::MIN),
        ],
    ];
    let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
    let mut gaps = vec![];
    for name in ["like", "ilike"] {
        for shape in 0..5 {
            for mask in 0..16 {
                for (fi, field) in [ty(), string_ty(), FieldType::new(FieldTypeCode::Json)]
                    .into_iter()
                    .enumerate()
                {
                    for (vi, values) in cases.iter().enumerate() {
                        for profile in [0, 5] {
                            for enabled in [false, true] {
                                for selection in [None, Some(vec![2, 0, 2]), Some(vec![])] {
                                    let make = || {
                                        let bad = || {
                                            call(
                                                "__actual_unread_wildcard_argument__",
                                                string_ty(),
                                                vec![],
                                            )
                                        };
                                        let deferred = |name, expr| {
                                            let mut c = Constant::new(
                                                Datum::new_string("unread stored"),
                                                string_ty(),
                                            );
                                            c.deferred_expr = Some(Box::new(leaf::assignment(
                                                name,
                                                expr,
                                                &string_ty(),
                                            )));
                                            Expression::Constant(c)
                                        };
                                        let p0 = parameter(0, string_ty());
                                        let p1 = parameter(1, string_ty());
                                        let escape = literal(values[2].clone(), string_ty());
                                        let args = match shape {
                                            1 => vec![
                                                deferred("wildcard-text", p0),
                                                deferred("wildcard-pattern", p1),
                                                deferred("wildcard-escape", escape),
                                            ],
                                            2 => vec![p0, literal(Datum::Null, string_ty()), bad()],
                                            3 => vec![
                                                literal(Datum::Null, string_ty()),
                                                bad(),
                                                bad(),
                                            ],
                                            4 => vec![p0, p1, bad()],
                                            _ => vec![p0, p1, escape],
                                        };
                                        let mut f = ScalarFunction::new(
                                            CiString::new(name),
                                            field.clone(),
                                            args,
                                        );
                                        if mask & 1 != 0 {
                                            f.ret_type = None;
                                        }
                                        for (i, arg) in f.args.iter_mut().enumerate() {
                                            if mask & (2 << i) != 0 {
                                                match arg {
                                                    Expression::Constant(c) => c.ret_type = None,
                                                    Expression::ScalarFunction(f) => {
                                                        f.ret_type = None
                                                    }
                                                    _ => unreachable!(),
                                                }
                                            }
                                        }
                                        suite(Expression::ScalarFunction(f))
                                    };
                                    let original = make();
                                    let actual = make();
                                    let old = C::new(
                                        profile,
                                        enabled,
                                        [values[0].clone(), values[1].clone()],
                                    );
                                    let new = C::new(
                                        profile,
                                        enabled,
                                        [values[0].clone(), values[1].clone()],
                                    );
                                    for reuse in 0..2 {
                                        let input = || {
                                            let mut c = chunk(
                                                &[ty()],
                                                &[
                                                    vec![Datum::Int(0)],
                                                    vec![Datum::Int(1)],
                                                    vec![Datum::Int(2)],
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
                                        let label = format!("optional-wildcard/{name}/{shape}/{mask}/{fi}/{vi}/{profile}/{enabled}/{selection:?}/{reuse}");
                                        if compare {
                                            let mut b = input();
                                            let mut right = output_for(&field);
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
                                                eprintln!("OPTIONAL WILDCARD DIFFERENCE {label} old={expected:?} new={got:?} oldcol={:?} newcol={:?} oldeffects={} neweffects={}",left.column(0),right.column(0),old.snapshot(),new.snapshot());
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
    assert_eq!((count, hash), (92160, 0xFFB7855F523B1F05));
    eprintln!("OPTIONAL WILDCARD ORIGINAL {count}/{hash:016X}");
    if !gaps.is_empty() {
        eprintln!("OPTIONAL WILDCARD ADMISSION RED {gaps:?}");
    }
    assert!(gaps.is_empty());
}

#[test]
fn optional_wildcard_full_actual_values_in_each_operand() {
    if isolate_common_corpus("stateful_tests::optional_wildcard_full_actual_values_in_each_operand")
    {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
    for name in ["like", "ilike"] {
        for position in 0..3 {
            for mask in [1, 2, 8, 15] {
                for (vi, value) in crate::tikv::byte_unary_value_tests::values()
                    .into_iter()
                    .enumerate()
                {
                    let mut values = [
                        Datum::new_string("abcd"),
                        Datum::new_string("a%"),
                        Datum::Int(92),
                    ];
                    values[position] = value;
                    for enabled in [false, true] {
                        for selection in [None, Some(vec![2, 0, 2]), Some(vec![])] {
                            let make = || {
                                let mut f = ScalarFunction::new(
                                    CiString::new(name),
                                    ty(),
                                    vec![
                                        parameter(0, string_ty()),
                                        parameter(1, string_ty()),
                                        literal(values[2].clone(), string_ty()),
                                    ],
                                );
                                if mask & 1 != 0 {
                                    f.ret_type = None;
                                }
                                for (i, arg) in f.args.iter_mut().enumerate() {
                                    if mask & (2 << i) != 0 {
                                        let Expression::Constant(c) = arg else {
                                            unreachable!()
                                        };
                                        c.ret_type = None;
                                    }
                                }
                                suite(Expression::ScalarFunction(f))
                            };
                            let original = make();
                            let actual = make();
                            let old = C::new(0, enabled, [values[0].clone(), values[1].clone()]);
                            let new = C::new(0, enabled, [values[0].clone(), values[1].clone()]);
                            for reuse in 0..2 {
                                let input = || {
                                    let mut c = chunk(
                                        &[ty()],
                                        &[
                                            vec![Datum::Int(0)],
                                            vec![Datum::Int(1)],
                                            vec![Datum::Int(2)],
                                        ],
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
                                let expected =
                                    session_read_outcome(|| original.run(&old, &mut a, &mut left));
                                let got = session_read_outcome(|| {
                                    actual.run_with_tikv_batch(&new, &mut b, &mut right, &[ty()])
                                });
                                let label=format!("optional-wildcard-full/{name}/{position}/{mask}/{vi}/{enabled}/{selection:?}/{reuse}");
                                if got != expected
                                    || right.column(0) != left.column(0)
                                    || new.snapshot() != old.snapshot()
                                {
                                    eprintln!("OPTIONAL WILDCARD FULL DIFFERENCE {label} old={expected:?} new={got:?} oldcol={:?} newcol={:?} oldeffects={} neweffects={}",left.column(0),right.column(0),old.snapshot(),new.snapshot());
                                }
                                assert_eq!(got, expected, "{label}");
                                assert_eq!(right.column(0), left.column(0), "{label}");
                                assert_eq!(new.snapshot(), old.snapshot(), "{label}");
                                let route = actual.batch_route_snapshot();
                                assert_eq!(
                                    (route.fallback, route.refusals),
                                    (0, 0),
                                    "{label} {route:?}"
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
    }
    assert_eq!((count, hash), (42912, 0x65B1761CB7A9C6A1));
    eprintln!("OPTIONAL WILDCARD FULL {count}/{hash:016X}");
}

struct ConcatContext {
    base: C,
    limit: u64,
    failure: usize,
    packet_reads: Cell<usize>,
}
impl ConcatContext {
    fn new(enabled: bool, values: [Datum; 2], limit: u64, failure: usize) -> Self {
        Self {
            base: C::new(0, enabled, values),
            limit,
            failure,
            packet_reads: Cell::new(0),
        }
    }
    fn snapshot(&self) -> String {
        self.base.snapshot()
    }
}
impl Columns for ConcatContext {
    fn get(&self, _: &[String]) -> Option<Datum> {
        panic!("no concat expression callback")
    }
    fn ready_value_cache(&self) -> Option<&crate::ReadyValueCache> {
        Some(&self.base.ready)
    }
    fn param_value(&self, order: usize) -> std::result::Result<Datum, EvalError> {
        self.base.param_value(order)
    }
    fn enable_vectorized_expression(&self) -> bool {
        self.base.enable_vectorized_expression()
    }
    fn time_zone(&self) -> SessionTimeZone {
        self.base.time_zone()
    }
    fn type_flags(&self) -> tidb_datatype::ConversionFlags {
        self.base.type_flags()
    }
    fn truncate_level(&self) -> crate::ErrorLevel {
        self.base.truncate_level()
    }
    fn append_warning(&self, code: u16, message: &str) {
        self.base.append_warning(code, message)
    }
    fn set_uservar(&self, name: &str, value: Datum) {
        self.base.set_uservar(name, value)
    }
    fn max_allowed_packet(&self) -> u64 {
        let n = self.packet_reads.get();
        self.packet_reads.set(n + 1);
        self.base.event(format!("packet:{n}:{}", self.limit));
        if self.failure == 2 && n == 0 {
            panic!("original packet limit callback")
        }
        self.limit
    }
    fn handle_allowed_packet_overflowed(&self, name: &str) -> std::result::Result<(), EvalError> {
        self.base.event(format!("packet-overflow:{name}"));
        if self.failure == 1 {
            Err(EvalError::Unsupported("original packet overflow callback"))
        } else {
            Ok(())
        }
    }
}
#[test]
fn optional_concat_original_packet_metadata_and_demands() {
    if isolate_common_corpus("stateful_tests::optional_concat_original_packet_metadata_and_demands")
    {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_optional_concat_matrix(false);
}
#[test]
fn optional_concat_actual_packet_metadata_and_demands() {
    if isolate_common_corpus("stateful_tests::optional_concat_actual_packet_metadata_and_demands") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_optional_concat_matrix(true);
}
fn run_optional_concat_matrix(compare: bool) {
    let cases = [
        [
            Datum::new_string("a"),
            Datum::new_string("B"),
            Datum::new_string("c"),
        ],
        [
            Datum::Null,
            Datum::new_string("long"),
            Datum::Bytes(vec![255]),
        ],
        [
            Datum::new_string("abcdef"),
            Datum::Null,
            Datum::UInt(u64::MAX),
        ],
        [
            Datum::Bytes(vec![255]),
            Datum::Decimal(tidb_datatype::Decimal::from_literal("2.5")),
            Datum::Real(f64::NAN),
        ],
    ];
    let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
    let mut gaps = vec![];
    let mut retained_whole = 0usize;
    for name in ["concat", "concat_ws"] {
        for shape in 0..4 {
            for mask in 0..16 {
                for (fi, field) in [string_ty(), ty(), FieldType::new(FieldTypeCode::Json)]
                    .into_iter()
                    .enumerate()
                {
                    for (vi, values) in cases.iter().enumerate() {
                        for limit in [0, 1, 4, u64::MAX] {
                            for failure in 0..3 {
                                for enabled in [false, true] {
                                    for selection in [None, Some(vec![2, 0, 2]), Some(vec![])] {
                                        let make = || {
                                            let deferred = |variable, expr| {
                                                let mut c =
                                                    Constant::new(Datum::Int(99), string_ty());
                                                c.deferred_expr = Some(Box::new(leaf::assignment(
                                                    variable,
                                                    expr,
                                                    &string_ty(),
                                                )));
                                                Expression::Constant(c)
                                            };
                                            let p0 = parameter(0, string_ty());
                                            let p1 = parameter(1, string_ty());
                                            let tail = literal(values[2].clone(), string_ty());
                                            let args = match shape {
                                                1 => vec![
                                                    deferred("concat-first", p0),
                                                    deferred("concat-second", p1),
                                                    deferred("concat-last", tail),
                                                ],
                                                2 => vec![
                                                    p0,
                                                    literal(Datum::Null, string_ty()),
                                                    call(
                                                        "__unread_concat_tail__",
                                                        string_ty(),
                                                        vec![],
                                                    ),
                                                ],
                                                3 => vec![
                                                    p0,
                                                    p1,
                                                    call(
                                                        "__actual_concat_tail_error__",
                                                        string_ty(),
                                                        vec![],
                                                    ),
                                                ],
                                                _ => vec![p0, p1, tail],
                                            };
                                            let mut f = ScalarFunction::new(
                                                CiString::new(name),
                                                field.clone(),
                                                args,
                                            );
                                            if mask & 1 != 0 {
                                                f.ret_type = None
                                            }
                                            for (i, a) in f.args.iter_mut().enumerate() {
                                                if mask & (2 << i) != 0 {
                                                    match a {
                                                        Expression::Constant(c) => {
                                                            c.ret_type = None
                                                        }
                                                        Expression::ScalarFunction(f) => {
                                                            f.ret_type = None
                                                        }
                                                        _ => unreachable!(),
                                                    }
                                                }
                                            }
                                            suite(Expression::ScalarFunction(f))
                                        };
                                        let original = make();
                                        let actual = make();
                                        let old = ConcatContext::new(
                                            enabled,
                                            [values[0].clone(), values[1].clone()],
                                            limit,
                                            failure,
                                        );
                                        let new = ConcatContext::new(
                                            enabled,
                                            [values[0].clone(), values[1].clone()],
                                            limit,
                                            failure,
                                        );
                                        for reuse in 0..2 {
                                            let input = || {
                                                let mut c = chunk(
                                                    &[ty()],
                                                    &[
                                                        vec![Datum::Int(0)],
                                                        vec![Datum::Int(1)],
                                                        vec![Datum::Int(2)],
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
                                            let label=format!("optional-concat/{name}/{shape}/{mask}/{fi}/{vi}/{limit}/{failure}/{enabled}/{selection:?}/{reuse}");
                                            if compare {
                                                let mut b = input();
                                                let mut right = output_for(&field);
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
                                                    || right.column(0) != left.column(0)
                                                    || new.snapshot() != old.snapshot()
                                                {
                                                    eprintln!("OPTIONAL CONCAT DIFFERENCE {label} old={expected:?} new={got:?} oldcol={:?} newcol={:?} oldeffects={} neweffects={}",left.column(0),right.column(0),old.snapshot(),new.snapshot());
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
                                                if route.fallback != 0 || route.refusals != 0 {
                                                    // The unchanged contextual typed-root guard
                                                    // retains this original whole-operand profile.
                                                    if shape == 0
                                                        && mask == 0
                                                        && fi == 0
                                                        && route.refusal
                                                            == Some(BatchRefusal::Admission)
                                                    {
                                                        retained_whole += 1;
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
                                                    .wrapping_mul(0x100000001b3)
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
    }
    assert_eq!((count, hash), (221184, 0xEE3A7F821B85B619));
    eprintln!("OPTIONAL CONCAT ORIGINAL {count}/{hash:016X}; retained whole {retained_whole}");
    if !gaps.is_empty() {
        eprintln!("OPTIONAL CONCAT ADMISSION RED {gaps:?}")
    }
    assert!(gaps.is_empty());
}

#[test]
fn optional_concat_full_actual_values_in_each_operand() {
    if isolate_common_corpus("stateful_tests::optional_concat_full_actual_values_in_each_operand") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
    for name in ["concat", "concat_ws"] {
        for position in 0..3 {
            for mask in [1, 2, 8, 15] {
                for (vi, value) in crate::tikv::byte_unary_value_tests::values()
                    .into_iter()
                    .enumerate()
                {
                    let mut values = [
                        Datum::new_string("a"),
                        Datum::new_string("b"),
                        Datum::new_string("c"),
                    ];
                    values[position] = value;
                    for limit in [0, 4, u64::MAX] {
                        for enabled in [false, true] {
                            for selection in [None, Some(vec![2, 0, 2]), Some(vec![])] {
                                let make = || {
                                    let mut f = ScalarFunction::new(
                                        CiString::new(name),
                                        string_ty(),
                                        vec![
                                            parameter(0, string_ty()),
                                            parameter(1, string_ty()),
                                            literal(values[2].clone(), string_ty()),
                                        ],
                                    );
                                    if mask & 1 != 0 {
                                        f.ret_type = None;
                                    }
                                    for (i, arg) in f.args.iter_mut().enumerate() {
                                        if mask & (2 << i) != 0 {
                                            let Expression::Constant(c) = arg else {
                                                unreachable!()
                                            };
                                            c.ret_type = None;
                                        }
                                    }
                                    suite(Expression::ScalarFunction(f))
                                };
                                let original = make();
                                let actual = make();
                                let old = ConcatContext::new(
                                    enabled,
                                    [values[0].clone(), values[1].clone()],
                                    limit,
                                    0,
                                );
                                let new = ConcatContext::new(
                                    enabled,
                                    [values[0].clone(), values[1].clone()],
                                    limit,
                                    0,
                                );
                                for reuse in 0..2 {
                                    let input = || {
                                        let mut c = chunk(
                                            &[ty()],
                                            &[
                                                vec![Datum::Int(0)],
                                                vec![Datum::Int(1)],
                                                vec![Datum::Int(2)],
                                            ],
                                        );
                                        c.set_sel(selection.clone());
                                        c
                                    };
                                    let mut a = input();
                                    let mut b = input();
                                    let mut left = output_for(&string_ty());
                                    let mut right = output_for(&string_ty());
                                    left.append_null(0);
                                    right.append_null(0);
                                    let expected = session_read_outcome(|| {
                                        original.run(&old, &mut a, &mut left)
                                    });
                                    let got = session_read_outcome(|| {
                                        actual.run_with_tikv_batch(
                                            &new,
                                            &mut b,
                                            &mut right,
                                            &[ty()],
                                        )
                                    });
                                    let label=format!("optional-concat-full/{name}/{position}/{mask}/{vi}/{limit}/{enabled}/{selection:?}/{reuse}");
                                    if got != expected
                                        || right.column(0) != left.column(0)
                                        || new.snapshot() != old.snapshot()
                                    {
                                        eprintln!("OPTIONAL CONCAT FULL DIFFERENCE {label} old={expected:?} new={got:?} oldcol={:?} newcol={:?} oldeffects={} neweffects={}",left.column(0),right.column(0),old.snapshot(),new.snapshot());
                                    }
                                    assert_eq!(got, expected, "{label}");
                                    assert_eq!(right.column(0), left.column(0), "{label}");
                                    assert_eq!(new.snapshot(), old.snapshot(), "{label}");
                                    let route = actual.batch_route_snapshot();
                                    assert_eq!(
                                        (route.fallback, route.refusals),
                                        (0, 0),
                                        "{label} {route:?}"
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
        }
    }
    assert_eq!((count, hash), (128736, 0xD556346B4B92D109));
    eprintln!("OPTIONAL CONCAT FULL {count}/{hash:016X}");
}

#[test]
fn optional_concat_actual_arities_keep_raw_values_and_packet_demand() {
    if isolate_common_corpus(
        "stateful_tests::optional_concat_actual_arities_keep_raw_values_and_packet_demand",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
    for name in ["concat", "concat_ws"] {
        for arity in [1, 2, 3, 5, 6, 9] {
            if name == "concat_ws" && arity == 1 {
                continue;
            }
            for mask in [1, 2, 3] {
                for (vi, value) in vec![
                    Datum::Null,
                    Datum::new_string("ab"),
                    Datum::UInt(u64::MAX),
                    Datum::Bytes(vec![255]),
                ]
                .into_iter()
                .enumerate()
                {
                    let mut values = [
                        Datum::new_string("a"),
                        Datum::new_string("b"),
                        Datum::new_string("c"),
                    ];
                    values[0] = value;
                    for limit in [0, 4, u64::MAX] {
                        for enabled in [false, true] {
                            for selection in [None, Some(vec![2, 0, 2]), Some(vec![])] {
                                let make = || {
                                    let mut f = ScalarFunction::new(
                                        CiString::new(name),
                                        string_ty(),
                                        (0..arity)
                                            .map(|i| {
                                                if i % 3 == 2 {
                                                    literal(values[2].clone(), string_ty())
                                                } else {
                                                    parameter(i % 2, string_ty())
                                                }
                                            })
                                            .collect(),
                                    );
                                    if mask & 1 != 0 {
                                        f.ret_type = None;
                                    }
                                    for (i, arg) in f.args.iter_mut().enumerate() {
                                        if mask & (2 << i) != 0 {
                                            let Expression::Constant(c) = arg else {
                                                unreachable!()
                                            };
                                            c.ret_type = None;
                                        }
                                    }
                                    suite(Expression::ScalarFunction(f))
                                };
                                let original = make();
                                let actual = make();
                                let old = ConcatContext::new(
                                    enabled,
                                    [values[0].clone(), values[1].clone()],
                                    limit,
                                    0,
                                );
                                let new = ConcatContext::new(
                                    enabled,
                                    [values[0].clone(), values[1].clone()],
                                    limit,
                                    0,
                                );
                                for reuse in 0..2 {
                                    let input = || {
                                        let mut c = chunk(
                                            &[ty()],
                                            &[
                                                vec![Datum::Int(0)],
                                                vec![Datum::Int(1)],
                                                vec![Datum::Int(2)],
                                            ],
                                        );
                                        c.set_sel(selection.clone());
                                        c
                                    };
                                    let mut a = input();
                                    let mut b = input();
                                    let mut left = output_for(&string_ty());
                                    let mut right = output_for(&string_ty());
                                    left.append_null(0);
                                    right.append_null(0);
                                    let expected = session_read_outcome(|| {
                                        original.run(&old, &mut a, &mut left)
                                    });
                                    let got = session_read_outcome(|| {
                                        actual.run_with_tikv_batch(
                                            &new,
                                            &mut b,
                                            &mut right,
                                            &[ty()],
                                        )
                                    });
                                    let label=format!("optional-concat-arity/{name}/{arity}/{mask}/{vi}/{limit}/{enabled}/{selection:?}/{reuse}");
                                    if got != expected
                                        || right.column(0) != left.column(0)
                                        || new.snapshot() != old.snapshot()
                                    {
                                        eprintln!("CONCAT ARITIES DIFFERENCE {label} old={expected:?} new={got:?} oldcol={:?} newcol={:?} oldeffects={} neweffects={}",left.column(0),right.column(0),old.snapshot(),new.snapshot());
                                    }
                                    assert_eq!(got, expected, "{label}");
                                    assert_eq!(right.column(0), left.column(0), "{label}");
                                    assert_eq!(new.snapshot(), old.snapshot(), "{label}");
                                    let route = actual.batch_route_snapshot();
                                    assert_eq!(
                                        (route.fallback, route.refusals),
                                        (0, 0),
                                        "{label} {route:?}"
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
        }
    }
    assert_eq!((count, hash), (4752, 0x344B99C07DBAD21D));
    eprintln!("CONCAT ARITIES {count}/{hash:016X}");
}

struct WholeConcatContext {
    ready: crate::ReadyValueCache,
    enabled: Cell<bool>,
    values: [Datum; 3],
    reads: [Cell<usize>; 3],
    packet_reads: Cell<usize>,
    limit: u64,
    profile: usize,
    events: RefCell<Vec<String>>,
}
impl WholeConcatContext {
    fn new(enabled: bool, values: [Datum; 3], limit: u64, profile: usize) -> Self {
        Self {
            ready: crate::ReadyValueCache::new(),
            enabled: Cell::new(enabled),
            values,
            reads: [Cell::new(0), Cell::new(0), Cell::new(0)],
            packet_reads: Cell::new(0),
            limit,
            profile,
            events: RefCell::new(vec![]),
        }
    }
    fn snapshot(&self) -> String {
        format!(
            "{}:{}:{}|{:?}",
            self.reads[0].get(),
            self.reads[1].get(),
            self.reads[2].get(),
            self.events.borrow()
        )
    }
}
impl Columns for WholeConcatContext {
    fn get(&self, _: &[String]) -> Option<Datum> {
        panic!("no whole expression callback")
    }
    fn ready_value_cache(&self) -> Option<&crate::ReadyValueCache> {
        Some(&self.ready)
    }
    fn enable_vectorized_expression(&self) -> bool {
        self.events
            .borrow_mut()
            .push(format!("flag:{}", self.enabled.get()));
        self.enabled.get()
    }
    fn param_value(&self, order: usize) -> std::result::Result<Datum, EvalError> {
        let n = self.reads[order].get();
        self.reads[order].set(n + 1);
        self.events.borrow_mut().push(format!("param:{order}:{n}"));
        if self.profile == 1 && order == 1 && n == 1 {
            return Err(EvalError::Unsupported("original whole parameter failure"));
        }
        if self.profile == 6 && order == 1 && n == 1026 {
            return Err(EvalError::Unsupported(
                "original late batch parameter failure",
            ));
        }
        if self.profile == 2 && n > 0 {
            return Ok(Datum::new_string(format!("changed:{order}:{n}")));
        }
        Ok(self.values[order].clone())
    }
    fn max_allowed_packet(&self) -> u64 {
        let n = self.packet_reads.get();
        self.packet_reads.set(n + 1);
        self.events
            .borrow_mut()
            .push(format!("packet:{n}:{}", self.limit));
        assert!(
            !((self.profile == 3 && n == 0) || (self.profile == 7 && n == 3073)),
            "original whole packet callback"
        );
        self.limit
    }
    fn handle_allowed_packet_overflowed(&self, name: &str) -> std::result::Result<(), EvalError> {
        self.events.borrow_mut().push(format!("overflow:{name}"));
        if self.profile == 4 {
            Err(EvalError::Unsupported("original whole overflow callback"))
        } else {
            Ok(())
        }
    }
}
#[test]
fn concat_whole_original_parameter_phases() {
    if isolate_common_corpus("stateful_tests::concat_whole_original_parameter_phases") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_concat_whole_matrix(false, false, None);
}
#[test]
fn concat_whole_actual_parameter_phases() {
    if isolate_common_corpus("stateful_tests::concat_whole_actual_parameter_phases") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_concat_whole_matrix(true, false, None);
}
#[test]
fn concat_whole_original_all_actual_values() {
    if isolate_common_corpus("stateful_tests::concat_whole_original_all_actual_values") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_concat_whole_matrix(false, true, None);
}
#[test]
fn concat_whole_actual_all_actual_values() {
    if isolate_common_corpus("stateful_tests::concat_whole_actual_all_actual_values") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_concat_whole_matrix(true, true, None);
}
#[test]
fn concat_whole_original_arities() {
    if isolate_common_corpus("stateful_tests::concat_whole_original_arities") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    for arity in [1, 2, 5, 6, 9] {
        run_concat_whole_matrix(false, false, Some(arity));
    }
}
#[test]
fn concat_whole_actual_arities() {
    if isolate_common_corpus("stateful_tests::concat_whole_actual_arities") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    for arity in [1, 2, 5, 6, 9] {
        run_concat_whole_matrix(true, false, Some(arity));
    }
}
fn run_concat_whole_matrix(compare: bool, full: bool, arity: Option<usize>) {
    let values = if full {
        crate::tikv::byte_unary_value_tests::values()
    } else {
        vec![
            Datum::new_string("ab"),
            Datum::Null,
            Datum::Int(123),
            Datum::Bytes(vec![255]),
            Datum::Decimal(tidb_datatype::Decimal::from_literal("2.5")),
            Datum::Real(f64::NAN),
        ]
    };
    let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
    let mut gaps = vec![];
    for name in ["concat", "concat_ws"] {
        if name == "concat_ws" && arity == Some(1) {
            continue;
        }
        for shape in 0..if full || arity.is_some() { 1 } else { 3 } {
            for position in 0..3 {
                for (vi, value) in values.iter().enumerate() {
                    let mut actual_values = [
                        Datum::new_string("ab"),
                        Datum::new_string("c"),
                        Datum::new_string("d"),
                    ];
                    actual_values[position] = value.clone();
                    for profile in 0..if full { 1 } else { 5 } {
                        for limit in [0, 4, u64::MAX] {
                            for enabled in [false, true] {
                                for selection in [None, Some(vec![2, 0, 2]), Some(vec![])] {
                                    let make = || {
                                        let args = match shape {
                                            1 => vec![
                                                parameter(0, string_ty()),
                                                parameter(0, string_ty()),
                                                parameter(0, string_ty()),
                                            ],
                                            2 => vec![
                                                parameter(0, string_ty()),
                                                string_column(0),
                                                literal(actual_values[2].clone(), string_ty()),
                                            ],
                                            _ => (0..arity.unwrap_or(3))
                                                .map(|i| parameter(i % 3, string_ty()))
                                                .collect(),
                                        };
                                        suite(call(name, string_ty(), args))
                                    };
                                    let original = make();
                                    let actual = make();
                                    let old = WholeConcatContext::new(
                                        enabled,
                                        actual_values.clone(),
                                        limit,
                                        profile,
                                    );
                                    let new = WholeConcatContext::new(
                                        enabled,
                                        actual_values.clone(),
                                        limit,
                                        profile,
                                    );
                                    for reuse in 0..2 {
                                        let input = || {
                                            let mut c = chunk(
                                                &[string_ty()],
                                                &[
                                                    vec![Datum::new_string("r0")],
                                                    vec![Datum::new_string("r1")],
                                                    vec![Datum::new_string("r2")],
                                                ],
                                            );
                                            c.set_sel(selection.clone());
                                            c
                                        };
                                        let mut a = input();
                                        let mut left = output_for(&string_ty());
                                        left.append_null(0);
                                        let expected = session_read_outcome(|| {
                                            original.run(&old, &mut a, &mut left)
                                        });
                                        let mut label=format!("concat-whole/{full}/{name}/{shape}/{position}/{vi}/{profile}/{limit}/{enabled}/{selection:?}/{reuse}");
                                        if let Some(arity) = arity {
                                            label.push_str(&format!("/arity-{arity}"));
                                        }
                                        if compare {
                                            let mut b = input();
                                            let mut right = output_for(&string_ty());
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
                                                || right.column(0) != left.column(0)
                                                || new.snapshot() != old.snapshot()
                                            {
                                                eprintln!("CONCAT WHOLE DIFFERENCE {label} old={expected:?} new={got:?} oldcol={:?} newcol={:?} oldeffects={} neweffects={}",left.column(0),right.column(0),old.snapshot(),new.snapshot())
                                            }
                                            assert_eq!(got, expected, "{label}");
                                            assert_eq!(right.column(0), left.column(0), "{label}");
                                            assert_eq!(new.snapshot(), old.snapshot(), "{label}");
                                            let route = actual.batch_route_snapshot();
                                            if (route.fallback != 0 || route.refusals != 0)
                                                && gaps.len() < 20
                                            {
                                                gaps.push(format!("{label}: {route:?}"))
                                            }
                                        }
                                        for byte in format!(
                                            "{label}|{expected}|{:?}|{}",
                                            left.column(0),
                                            old.snapshot()
                                        )
                                        .bytes()
                                        {
                                            hash =
                                                (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3)
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
    if let Some(arity) = arity {
        let expected = match arity {
            1 => (3240, 0x9B21916B002CA553),
            2 => (6480, 0x8947CBC47EC5D18D),
            5 => (6480, 0x9746E47C0EBFF03F),
            6 => (6480, 0x3B7D87B0EA8F1545),
            9 => (6480, 0x7BA6E0F9E0044141),
            _ => unreachable!("explicit original arity cohort"),
        };
        assert_eq!((count, hash), expected);
        eprintln!("CONCAT WHOLE ARITY {arity} {count}/{hash:016X}");
    } else {
        assert_eq!(
            (count, hash),
            if full {
                (32184, 0xE1B29ADC97CAEEC9)
            } else {
                (19440, 0x3CE163458ACA2747)
            }
        );
        eprintln!("CONCAT WHOLE ORIGINAL full={full} {count}/{hash:016X}");
    }
    if !gaps.is_empty() {
        eprintln!("CONCAT WHOLE ADMISSION RED full={full} {gaps:?}")
    }
    assert!(gaps.is_empty());
}

#[test]
fn concat_row_batch_original_long_reuse_and_flag_switch() {
    if isolate_common_corpus("stateful_tests::concat_row_batch_original_long_reuse_and_flag_switch")
    {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_concat_row_batch_long(false);
}
#[test]
fn concat_row_batch_actual_long_reuse_and_flag_switch() {
    if isolate_common_corpus("stateful_tests::concat_row_batch_actual_long_reuse_and_flag_switch") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_concat_row_batch_long(true);
}
fn run_concat_row_batch_long(compare: bool) {
    let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
    for name in ["concat", "concat_ws"] {
        let make = || {
            suite(call(
                name,
                string_ty(),
                vec![
                    parameter(0, string_ty()),
                    parameter(1, string_ty()),
                    parameter(2, string_ty()),
                ],
            ))
        };
        let original = make();
        let actual = make();
        for profile in [0, 2, 6, 7] {
            for rows in [0, 1, 3, 1026, 2051] {
                for first in [false, true] {
                    let values = [
                        Datum::new_string("ab"),
                        Datum::new_string("c"),
                        Datum::new_string("d"),
                    ];
                    let old = WholeConcatContext::new(first, values.clone(), u64::MAX, profile);
                    let new = WholeConcatContext::new(first, values, u64::MAX, profile);
                    for round in 0..4 {
                        let enabled = if round % 2 == 0 { first } else { !first };
                        old.enabled.set(enabled);
                        new.enabled.set(enabled);
                        let input = || {
                            let mut c = chunk(
                                &[string_ty()],
                                &[
                                    vec![Datum::new_string("r0")],
                                    vec![Datum::new_string("r1")],
                                    vec![Datum::new_string("r2")],
                                ],
                            );
                            c.set_sel(Some((0..rows).map(|i| [2, 0, 2, 1][i % 4]).collect()));
                            c
                        };
                        let mut a = input();
                        let mut left = output_for(&string_ty());
                        left.append_null(0);
                        let expected =
                            session_read_outcome(|| original.run(&old, &mut a, &mut left));
                        let label=format!("concat-row-batch-long/{name}/{profile}/{rows}/{first}/{round}/{enabled}");
                        if compare {
                            let mut b = input();
                            let mut right = output_for(&string_ty());
                            right.append_null(0);
                            let got = session_read_outcome(|| {
                                actual.run_with_tikv_batch(&new, &mut b, &mut right, &[string_ty()])
                            });
                            if got != expected
                                || right.column(0) != left.column(0)
                                || new.snapshot() != old.snapshot()
                            {
                                eprintln!("CONCAT ROW BATCH LONG DIFFERENCE {label} old={expected:?} new={got:?} oldrows={} newrows={} oldeffects={} neweffects={}",left.num_rows(),right.num_rows(),old.snapshot(),new.snapshot())
                            }
                            assert_eq!(got, expected, "{label}");
                            assert_eq!(right.column(0), left.column(0), "{label}");
                            assert_eq!(new.snapshot(), old.snapshot(), "{label}");
                            let route = actual.batch_route_snapshot();
                            assert_eq!(
                                (route.fallback, route.refusals),
                                (0, 0),
                                "{label} {route:?}"
                            );
                        }
                        for byte in
                            format!("{label}|{expected}|{:?}|{}", left.column(0), old.snapshot())
                                .bytes()
                        {
                            hash = (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3)
                        }
                        count += 1;
                    }
                }
            }
        }
    }
    assert_eq!((count, hash), (320, 0x11E1_A259_0FCF_4497));
    eprintln!("CONCAT ROW BATCH LONG {count}/{hash:016X}");
}

// This witness keeps the result declaration absent and both source declarations
// present. The caller's actual column/storage declarations are independent.
struct ArithmeticAbsentContext {
    base: C,
}
impl ArithmeticAbsentContext {
    fn new(profile: usize, enabled: bool, values: [Datum; 2]) -> Self {
        Self {
            base: C::new(profile, enabled, values),
        }
    }
    fn snapshot(&self) -> String {
        self.base.snapshot()
    }
}
impl Columns for ArithmeticAbsentContext {
    fn ready_value_cache(&self) -> Option<&crate::ReadyValueCache> {
        Some(&self.base.ready)
    }
    fn get(&self, _: &[String]) -> Option<Datum> {
        panic!("no expression callback")
    }
    fn param_value(&self, order: usize) -> std::result::Result<Datum, EvalError> {
        self.base.param_value(order)
    }
    fn enable_vectorized_expression(&self) -> bool {
        self.base.enable_vectorized_expression()
    }
    fn time_zone(&self) -> SessionTimeZone {
        self.base.time_zone()
    }
    fn type_flags(&self) -> tidb_datatype::ConversionFlags {
        self.base.type_flags()
    }
    fn truncate_level(&self) -> crate::ErrorLevel {
        self.base.truncate_level()
    }
    fn append_warning(&self, code: u16, message: &str) {
        self.base.append_warning(code, message)
    }
    fn set_uservar(&self, name: &str, value: Datum) {
        self.base.set_uservar(name, value)
    }
    fn division_by_zero_level(&self) -> crate::ErrorLevel {
        self.base.event("division-level".into());
        match self.base.profile {
            6 => crate::ErrorLevel::Error,
            7 => crate::ErrorLevel::Ignore,
            _ => crate::ErrorLevel::Warn,
        }
    }
    fn div_precision_increment(&self) -> u32 {
        self.base.event("precision".into());
        if self.base.profile == 3 {
            panic!("original absent arithmetic precision callback")
        }
        if self.base.profile == 2 {
            12
        } else {
            4
        }
    }
}
#[test]
fn arithmetic_absent_result_original_phases() {
    if isolate_common_corpus("stateful_tests::arithmetic_absent_result_original_phases") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_arithmetic_absent_result(false, false);
}
#[test]
fn arithmetic_absent_result_actual_phases() {
    if isolate_common_corpus("stateful_tests::arithmetic_absent_result_actual_phases") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_arithmetic_absent_result(true, false);
}
#[test]
fn arithmetic_absent_result_original_all_actual_values() {
    if isolate_common_corpus("stateful_tests::arithmetic_absent_result_original_all_actual_values")
    {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_arithmetic_absent_result(false, true);
}
#[test]
fn arithmetic_absent_result_actual_all_actual_values() {
    if isolate_common_corpus("stateful_tests::arithmetic_absent_result_actual_all_actual_values") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_arithmetic_absent_result(true, true);
}
fn run_arithmetic_absent_result(compare: bool, full: bool) {
    let now = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_761_004_800);
    tidb_datatype::with_test_conversion_clock(now.into(), || {
        let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
        let mut gaps = vec![];
        let fields = [
            ty(),
            ty().with_flags(tidb_datatype::FieldTypeFlags::UNSIGNED),
            FieldType::new(FieldTypeCode::Double),
            FieldType::new(FieldTypeCode::NewDecimal)
                .with_flen(65)
                .with_decimal(5),
            string_ty(),
        ];
        let storage = FieldType::new(FieldTypeCode::NewDecimal)
            .with_flen(65)
            .with_decimal(6);
        let pairs = if full {
            let mut pairs = vec![];
            for value in crate::tikv::byte_unary_value_tests::values() {
                pairs.push([value.clone(), Datum::Int(3)]);
                pairs.push([Datum::Int(2), value]);
            }
            pairs
        } else {
            vec![
                [Datum::Int(i64::MAX), Datum::Int(1)],
                [Datum::UInt(u64::MAX), Datum::Int(1)],
                [Datum::Null, Datum::new_string("not-number")],
                [Datum::Real(f64::MAX), Datum::Int(2)],
                [
                    Datum::Decimal(tidb_datatype::Decimal::from_int(2)),
                    Datum::Int(0),
                ],
                [
                    Datum::BinaryLiteral(tidb_datatype::BinaryLiteral::from(vec![
                        128, 0, 0, 0, 0, 0, 0, 1,
                    ])),
                    Datum::BinaryLiteral(tidb_datatype::BinaryLiteral::from(vec![255])),
                ],
                [Datum::new_string("not-number"), Datum::Int(0)],
                [Datum::Real(f64::NAN), Datum::Real(f64::INFINITY)],
            ]
        };
        for name in ["plus", "minus", "mul", "mod", "div", "intdiv"] {
            for (fi, field) in fields.iter().enumerate() {
                for (vi, values) in pairs.iter().enumerate() {
                    for shape in 0..if full { 1 } else { 4 } {
                        for profile in if full {
                            vec![0]
                        } else {
                            vec![0, 2, 3, 4, 5, 6, 7]
                        } {
                            for enabled in [false, true] {
                                for selection in [None, Some(vec![2, 0, 2]), Some(vec![])] {
                                    let make = || {
                                        let deferred = |order| {
                                            let mut c =
                                                Constant::new(Datum::Int(99), field.clone());
                                            c.deferred_expr = Some(Box::new(leaf::assignment(
                                                if order == 0 {
                                                    "absent-left"
                                                } else {
                                                    "absent-right"
                                                },
                                                parameter(order, field.clone()),
                                                field,
                                            )));
                                            Expression::Constant(c)
                                        };
                                        let actual_column = |index| {
                                            let mut c = crate::column::Column::new(
                                                index + 1,
                                                field.clone(),
                                            );
                                            c.index = index;
                                            c.orig_name = format!("t.absent{index}");
                                            Expression::Column(c)
                                        };
                                        let args = match shape {
                                            1 => vec![
                                                parameter(0, field.clone()),
                                                literal(values[1].clone(), field.clone()),
                                            ],
                                            2 => vec![actual_column(0), actual_column(1)],
                                            3 => vec![deferred(0), deferred(1)],
                                            _ => vec![
                                                parameter(0, field.clone()),
                                                parameter(1, field.clone()),
                                            ],
                                        };
                                        let mut f = ScalarFunction::new(
                                            CiString::new(name),
                                            field.clone(),
                                            args,
                                        );
                                        f.ret_type = None;
                                        suite(Expression::ScalarFunction(f))
                                    };
                                    let original = make();
                                    let actual = make();
                                    let old = ArithmeticAbsentContext::new(
                                        profile,
                                        enabled,
                                        values.clone(),
                                    );
                                    let new = ArithmeticAbsentContext::new(
                                        profile,
                                        enabled,
                                        values.clone(),
                                    );
                                    for reuse in 0..2 {
                                        let input = || {
                                            let numeric = match field.eval_type() {
                                                NativeEvalType::Int if field.is_unsigned() => {
                                                    [Datum::UInt(u64::MAX), Datum::UInt(3)]
                                                }
                                                NativeEvalType::Int => {
                                                    [Datum::Int(2), Datum::Int(3)]
                                                }
                                                NativeEvalType::Real => {
                                                    [Datum::Real(2.5), Datum::Real(0.0)]
                                                }
                                                NativeEvalType::Decimal => [
                                                    Datum::Decimal(
                                                        tidb_datatype::Decimal::from_int(2),
                                                    ),
                                                    Datum::Decimal(
                                                        tidb_datatype::Decimal::from_int(0),
                                                    ),
                                                ],
                                                NativeEvalType::String => [
                                                    Datum::new_string("2.5"),
                                                    Datum::new_string("0"),
                                                ],
                                                _ => unreachable!(),
                                            };
                                            let mut c = chunk(
                                                &[field.clone(), field.clone()],
                                                &[
                                                    numeric.to_vec(),
                                                    vec![Datum::Null, numeric[1].clone()],
                                                    numeric.to_vec(),
                                                ],
                                            );
                                            c.set_sel(selection.clone());
                                            c
                                        };
                                        let mut a = input();
                                        let mut left = output_for(&storage);
                                        left.append_null(0);
                                        let expected = session_read_outcome(|| {
                                            original.run(&old, &mut a, &mut left)
                                        });
                                        let label=format!("arithmetic-absent/{full}/{name}/{fi}/{vi}/{shape}/{profile}/{enabled}/{selection:?}/{reuse}");
                                        if compare {
                                            let mut b = input();
                                            let mut right = output_for(&storage);
                                            right.append_null(0);
                                            let got = session_read_outcome(|| {
                                                actual.run_with_tikv_batch(
                                                    &new,
                                                    &mut b,
                                                    &mut right,
                                                    &[field.clone(), field.clone()],
                                                )
                                            });
                                            if got != expected
                                                || right.column(0) != left.column(0)
                                                || new.snapshot() != old.snapshot()
                                            {
                                                eprintln!("ARITHMETIC ABSENT DIFFERENCE {label} old={expected:?} new={got:?} oldcol={:?} newcol={:?} oldeffects={} neweffects={}",left.column(0),right.column(0),old.snapshot(),new.snapshot());
                                            }
                                            assert_eq!(got, expected, "{label}");
                                            assert_eq!(right.column(0), left.column(0), "{label}");
                                            assert_eq!(new.snapshot(), old.snapshot(), "{label}");
                                            let route = actual.batch_route_snapshot();
                                            if (route.fallback, route.refusals) != (0, 0)
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
        assert_eq!(
            (count, hash),
            if full {
                (107280, 0x65E3_A879_08E5_1839)
            } else {
                (80640, 0x80D3_DF5F_B7E2_AFFF)
            }
        );
        eprintln!("ARITHMETIC ABSENT ORIGINAL full={full} {count}/{hash:016X}");
        if !gaps.is_empty() {
            eprintln!("ARITHMETIC ABSENT ADMISSION RED full={full} {gaps:?}");
        }
        assert!(gaps.is_empty());
    });
}

#[test]
fn arithmetic_absent_result_original_raw_datums() {
    if isolate_common_corpus("stateful_tests::arithmetic_absent_result_original_raw_datums") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_arithmetic_absent_raw_datums(false);
}
#[test]
fn arithmetic_absent_result_actual_raw_datums() {
    if isolate_common_corpus("stateful_tests::arithmetic_absent_result_actual_raw_datums") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_arithmetic_absent_raw_datums(true);
}
fn run_arithmetic_absent_raw_datums(compare: bool) {
    use tidb_query_datatype::codec::data_type::ScalarValueRef;
    use tidb_query_expr::local::{HostArgReply, HostInvocation, HostStart, HostStep};
    let now = chrono::DateTime::parse_from_rfc3339("2026-10-21T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    tidb_datatype::with_test_conversion_clock(now, || {
        let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
        let mut gaps = vec![];
        let mut resources = 0;
        let fields = [
            ty(),
            ty().with_unsigned(true),
            FieldType::new(FieldTypeCode::Double),
            FieldType::new(FieldTypeCode::NewDecimal)
                .with_flen(65)
                .with_decimal(5),
            string_ty(),
        ];
        for name in ["plus", "minus", "mul", "mod", "div", "intdiv"] {
            for (fi, field) in fields.iter().enumerate() {
                for (vi, value) in crate::tikv::byte_unary_value_tests::values()
                    .into_iter()
                    .enumerate()
                {
                    for position in 0..2 {
                        let mut values = [Datum::Int(2), Datum::Int(3)];
                        values[position] = value.clone();
                        let mut function = ScalarFunction::new(
                            CiString::new(name),
                            field.clone(),
                            values
                                .iter()
                                .map(|v| literal(v.clone(), field.clone()))
                                .collect(),
                        );
                        function.ret_type = None;
                        let mut input = Chunk::new_with_capacity(&[], 1);
                        input.set_num_virtual_rows(1);
                        let old = ArithmeticAbsentContext::new(0, false, values.clone());
                        let expected = session_read_outcome(|| {
                            function
                                .eval(&old, input.get_row(0))
                                .map(|v| leaf::frame(&v))
                        });
                        let label = format!("arithmetic-absent-raw/{name}/{fi}/{vi}/{position}");
                        if compare {
                            let actual = suite(Expression::ScalarFunction(function.clone()));
                            let before_compile =
                                crate::scalar_function::static_numeric_formats_for_test();
                            match PreparedBatch::compile(
                                &actual,
                                &[],
                                true,
                                319,
                                limits(),
                                ExecutionLimits::default(),
                                1 << 24,
                            ) {
                                Err(error) => {
                                    if matches!(
                                        error.local_error(),
                                        Some(LocalError::ResourceLimit(_))
                                    ) {
                                        assert!(
                                            matches!(&value,Datum::Decimal(d) if d.scale()==u32::MAX && d.storage_scale()==3 && d.is_negative() && d.coefficient_bytes()==[0,255,b'0'] && d.declared_shape()==Some((-1,i64::MAX))),
                                            "unexpected resource {label}: {error:?}"
                                        );
                                        assert_eq!(
                                            crate::scalar_function::static_numeric_formats_for_test(
                                            ),
                                            before_compile,
                                            "resource gate must precede formatting {label}"
                                        );
                                        eprintln!("ARITHMETIC ABSENT RESOURCE {label}");
                                        resources += 1;
                                    } else if gaps.len() < 20 {
                                        gaps.push(format!("{label}:{error:?}"));
                                    }
                                }
                                Ok(compiled) => {
                                    let source = &compiled.source;
                                    assert_eq!(source.host_metadata.len(), 1);
                                    let new =
                                        ArithmeticAbsentContext::new(0, false, values.clone());
                                    let store =
                                        RefCell::new(NativeValueStore::new(1 << 24).unwrap());
                                    store.borrow_mut().begin_window().unwrap();
                                    let catalog = source.hosts.as_ref().unwrap().clone();
                                    let mut hosts = NativeBatchHosts::new(
                                        catalog.clone(),
                                        &source.host_metadata,
                                        &new,
                                        1 << 24,
                                    )
                                    .with_native_values(Some(&store));
                                    let slot = catalog.slot(0).unwrap();
                                    let signature = catalog.signature(slot).unwrap();
                                    let row = InputRow {
                                        occurrence: 19,
                                        input_row: 0,
                                    };
                                    let mut eval = EvalContext::default();
                                    let got = session_read_outcome(
                                        || -> std::result::Result<String, EvalError> {
                                            let HostStart::Pending { task, mut request } = hosts
                                                .start(
                                                    &mut eval,
                                                    HostInvocation {
                                                        slot,
                                                        row,
                                                        arg_types: &signature.arg_types,
                                                        return_type: &signature.return_type,
                                                    },
                                                )
                                                .unwrap()
                                            else {
                                                panic!("arithmetic must demand actual arguments")
                                            };
                                            for _ in 0..8 {
                                                assert!(request.index < 2);
                                                let node = match &source.host_metadata[0] {
                                                    NativeHostMetadata::Arithmetic {
                                                        argument_nodes,
                                                        ..
                                                    } => argument_nodes[request.index],
                                                    _ => panic!("original arithmetic metadata"),
                                                };
                                                let token = store
                                                    .borrow_mut()
                                                    .insert_clone(
                                                        &values[request.index],
                                                        NativeValueRole::CharInput,
                                                        NativeValueOrigin { node, row },
                                                    )
                                                    .unwrap();
                                                let fetched = VectorValue::from_scalar(
                                                    &ScalarValue::Bytes(Some(token)),
                                                    1,
                                                );
                                                match hosts.resume(
                                                    &mut eval,
                                                    &task,
                                                    HostArgReply {
                                                        index: request.index,
                                                        field_type: &signature.arg_types
                                                            [request.index],
                                                        values: &fetched,
                                                    },
                                                ) {
                                                    Ok(HostStep::NeedArg(next)) => request = next,
                                                    Ok(HostStep::Ready(output)) => {
                                                        let ScalarValueRef::Bytes(Some(token)) =
                                                            output.get_scalar_ref(0)
                                                        else {
                                                            panic!("native arithmetic token")
                                                        };
                                                        let value = store.borrow();
                                                        return Ok(leaf::frame(
                                                            value
                                                                .get(
                                                                    token,
                                                                    NativeValueRole::Output,
                                                                    NativeValueOrigin {
                                                                        node: 0,
                                                                        row,
                                                                    },
                                                                )
                                                                .unwrap(),
                                                        ));
                                                    }
                                                    Ok(HostStep::NeedArgRow { .. }) => panic!(
                                                        "row arithmetic cannot request another row"
                                                    ),
                                                    Err(error) => {
                                                        return Err(hosts
                                                            .take_native_error()
                                                            .unwrap_or_else(|| {
                                                                panic!(
                                                                "unexpected host failure {error:?}"
                                                            )
                                                            }))
                                                    }
                                                }
                                            }
                                            panic!("arithmetic demand did not terminate")
                                        },
                                    );
                                    if got != expected || new.snapshot() != old.snapshot() {
                                        eprintln!("ARITHMETIC ABSENT RAW DIFFERENCE {label} old={expected} new={got} oldeffects={} neweffects={}",old.snapshot(),new.snapshot());
                                    }
                                    assert_eq!(got, expected, "{label}");
                                    assert_eq!(new.snapshot(), old.snapshot(), "{label}");
                                }
                            }
                        }
                        for byte in format!("{label}|{expected}|{}", old.snapshot()).bytes() {
                            hash = (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3);
                        }
                        count += 1;
                    }
                }
            }
        }
        assert_eq!((count, hash), (8940, 0x07DA_5D6E_BAB0_FE48));
        eprintln!("ARITHMETIC ABSENT RAW ORIGINAL {count}/{hash:016X}");
        if compare {
            assert_eq!(resources, 60);
            eprintln!("ARITHMETIC ABSENT RAW actual8880/terminal_resource60/before_formatting60");
        }
        if !gaps.is_empty() {
            eprintln!("ARITHMETIC ABSENT RAW ADMISSION RED {gaps:?}");
        }
        assert!(gaps.is_empty());
    });
}

#[test]
fn arithmetic_absent_result_resource_failure_keeps_same_owner_reusable() {
    let storage = FieldType::new(FieldTypeCode::NewDecimal)
        .with_flen(65)
        .with_decimal(6);
    for name in ["plus", "minus", "mul", "mod", "div", "intdiv"] {
        let mut f = ScalarFunction::new(
            CiString::new(name),
            storage.clone(),
            vec![
                parameter(0, string_ty()),
                literal(Datum::Int(1), string_ty()),
            ],
        );
        f.ret_type = None;
        let owner = suite(Expression::ScalarFunction(f));
        let mut prepared = PreparedBatchSuite::compile(
            &owner,
            &[ty()],
            true,
            919,
            limits(),
            ExecutionLimits::default(),
            1 << 20,
        )
        .unwrap();
        prepared.workers[0].max_materialization_retained_bytes = 16 * 1024;
        let mut ctx = ArithmeticAbsentContext::new(
            0,
            true,
            [Datum::new_string("1".repeat(32768)), Datum::Int(1)],
        );
        let input = || {
            chunk(
                &[ty()],
                &[
                    vec![Datum::Int(0)],
                    vec![Datum::Int(1)],
                    vec![Datum::Int(2)],
                ],
            )
        };
        let mut first = input();
        let mut output = output_for(&storage);
        output.append_null(0);
        let error = prepared
            .run(
                &owner,
                &ctx,
                &mut EvalContext::default(),
                &mut first,
                &mut output,
                &[ty()],
            )
            .unwrap_err();
        assert!(
            matches!(error.local_error(), Some(LocalError::ResourceLimit(_))),
            "{name}: {error:?}"
        );
        assert_eq!(output.num_rows(), 1);
        assert!(!ctx.base.events.borrow().iter().any(|e| e == "precision"));
        ctx.base.values[0] = Datum::Null;
        ctx.base.enabled = false;
        ctx.base.events.borrow_mut().clear();
        for reads in &ctx.base.reads {
            reads.set(0);
        }
        let old = ArithmeticAbsentContext::new(0, false, [Datum::Null, Datum::Int(1)]);
        let mut original_input = input();
        let mut expected = output_for(&storage);
        expected.append_null(0);
        owner.run(&old, &mut original_input, &mut expected).unwrap();
        let mut second = input();
        let mut got = output_for(&storage);
        got.append_null(0);
        prepared
            .run(
                &owner,
                &ctx,
                &mut EvalContext::default(),
                &mut second,
                &mut got,
                &[ty()],
            )
            .unwrap();
        assert_eq!(got.column(0), expected.column(0), "{name}");
        assert_eq!(ctx.snapshot(), old.snapshot(), "{name}");
        assert_eq!(
            prepared.workers[0].max_materialization_retained_bytes,
            16 * 1024
        );
    }
}

// The changed-read profile exposes eager type probes, fresh retries and
// diagnostic re-demands. Existing numeric session behavior stays unchanged.
struct ArithmeticMissingContext {
    base: ArithmeticAbsentContext,
}
impl ArithmeticMissingContext {
    fn new(profile: usize, enabled: bool, values: [Datum; 2]) -> Self {
        Self {
            base: ArithmeticAbsentContext::new(profile, enabled, values),
        }
    }
    fn snapshot(&self) -> String {
        self.base.snapshot()
    }
}
impl Columns for ArithmeticMissingContext {
    fn ready_value_cache(&self) -> Option<&crate::ReadyValueCache> {
        self.base.ready_value_cache()
    }
    fn get(&self, _: &[String]) -> Option<Datum> {
        panic!("no expression callback")
    }
    fn param_value(&self, order: usize) -> std::result::Result<Datum, EvalError> {
        let value = self.base.param_value(order)?;
        let n = self.base.base.reads[order].get();
        if self.base.base.profile == 10 && order == 1 && n == 1027 {
            return Ok(Datum::Real(2.5));
        }
        if self.base.base.profile == 8 && n > 1 {
            Ok(Datum::Int(if order == 0 {
                17 + n as i64
            } else {
                3 + n as i64
            }))
        } else {
            Ok(value)
        }
    }
    fn enable_vectorized_expression(&self) -> bool {
        self.base.enable_vectorized_expression()
    }
    fn time_zone(&self) -> SessionTimeZone {
        self.base.time_zone()
    }
    fn type_flags(&self) -> tidb_datatype::ConversionFlags {
        self.base.type_flags()
    }
    fn truncate_level(&self) -> crate::ErrorLevel {
        self.base.truncate_level()
    }
    fn append_warning(&self, code: u16, message: &str) {
        self.base.append_warning(code, message)
    }
    fn set_uservar(&self, name: &str, value: Datum) {
        self.base.set_uservar(name, value)
    }
    fn division_by_zero_level(&self) -> crate::ErrorLevel {
        self.base.division_by_zero_level()
    }
    fn div_precision_increment(&self) -> u32 {
        self.base.div_precision_increment()
    }
}

#[test]
fn arithmetic_missing_argument_original_phases() {
    if isolate_common_corpus("stateful_tests::arithmetic_missing_argument_original_phases") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_arithmetic_missing_argument(false, false);
}

#[test]
fn arithmetic_missing_argument_original_all_actual_values() {
    if isolate_common_corpus(
        "stateful_tests::arithmetic_missing_argument_original_all_actual_values",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_arithmetic_missing_argument(false, true);
}

#[test]
fn arithmetic_missing_argument_actual_phases() {
    if isolate_common_corpus("stateful_tests::arithmetic_missing_argument_actual_phases") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_arithmetic_missing_argument(true, false);
}

#[test]
fn arithmetic_missing_argument_actual_all_actual_values() {
    if isolate_common_corpus("stateful_tests::arithmetic_missing_argument_actual_all_actual_values")
    {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_arithmetic_missing_argument(true, true);
}

fn run_arithmetic_missing_argument(compare: bool, full: bool) {
    let now = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_761_004_800);
    tidb_datatype::with_test_conversion_clock(now.into(), || {
        let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
        let mut gaps = vec![];
        let fields = [
            ty(),
            ty().with_flags(tidb_datatype::FieldTypeFlags::UNSIGNED),
            FieldType::new(FieldTypeCode::Double),
            FieldType::new(FieldTypeCode::NewDecimal)
                .with_flen(65)
                .with_decimal(5),
            string_ty(),
        ];
        let storage = FieldType::new(FieldTypeCode::NewDecimal)
            .with_flen(65)
            .with_decimal(6);
        let pairs = if full {
            let mut pairs = vec![];
            for value in crate::tikv::byte_unary_value_tests::values() {
                pairs.push([value.clone(), Datum::Int(3)]);
                pairs.push([Datum::Int(2), value]);
            }
            pairs
        } else {
            vec![
                [Datum::Int(i64::MAX), Datum::Int(1)],
                [Datum::UInt(u64::MAX), Datum::Int(1)],
                [Datum::Null, Datum::new_string("not-number")],
                [Datum::Real(f64::MAX), Datum::Int(2)],
                [
                    Datum::Decimal(tidb_datatype::Decimal::from_int(2)),
                    Datum::Int(0),
                ],
                [
                    Datum::BinaryLiteral(tidb_datatype::BinaryLiteral::from(vec![
                        128, 0, 0, 0, 0, 0, 0, 1,
                    ])),
                    Datum::BinaryLiteral(tidb_datatype::BinaryLiteral::from(vec![255])),
                ],
                [Datum::new_string("not-number"), Datum::Int(0)],
                [Datum::Real(f64::NAN), Datum::Real(f64::INFINITY)],
            ]
        };
        for name in ["plus", "minus", "mul", "mod", "div", "intdiv"] {
            for (fi, field) in fields.iter().enumerate() {
                for mask in [2u8, 4, 6, 3, 5, 7] {
                    for (vi, values) in pairs.iter().enumerate() {
                        for shape in 0..if full { 1 } else { 4 } {
                            for profile in if full {
                                vec![0]
                            } else {
                                vec![0, 2, 3, 4, 5, 6, 7, 8]
                            } {
                                for enabled in [false, true] {
                                    for selection in [None, Some(vec![2, 0, 2]), Some(vec![])] {
                                        let make = || {
                                            let deferred = |order| {
                                                let mut c =
                                                    Constant::new(Datum::Int(99), field.clone());
                                                c.deferred_expr = Some(Box::new(leaf::assignment(
                                                    if order == 0 {
                                                        "missing-left"
                                                    } else {
                                                        "missing-right"
                                                    },
                                                    parameter(order, field.clone()),
                                                    field,
                                                )));
                                                Expression::Constant(c)
                                            };
                                            let actual_column = |index| {
                                                let mut c = crate::column::Column::new(
                                                    index + 1,
                                                    field.clone(),
                                                );
                                                c.index = index;
                                                c.orig_name = format!("t.absent{index}");
                                                Expression::Column(c)
                                            };
                                            let args = match shape {
                                                1 => vec![
                                                    parameter(0, field.clone()),
                                                    literal(values[1].clone(), field.clone()),
                                                ],
                                                2 => vec![actual_column(0), actual_column(1)],
                                                3 => vec![deferred(0), deferred(1)],
                                                _ => vec![
                                                    parameter(0, field.clone()),
                                                    parameter(1, field.clone()),
                                                ],
                                            };
                                            let mut f = ScalarFunction::new(
                                                CiString::new(name),
                                                field.clone(),
                                                args,
                                            );
                                            if mask & 1 != 0 {
                                                f.ret_type = None;
                                            }
                                            for (i, arg) in f.args.iter_mut().enumerate() {
                                                if mask & (2 << i) != 0 {
                                                    match arg {
                                                        Expression::Constant(c) => {
                                                            c.ret_type = None
                                                        }
                                                        Expression::Column(c) => c.ret_type = None,
                                                        Expression::ScalarFunction(c) => {
                                                            c.ret_type = None
                                                        }
                                                        _ => unreachable!(),
                                                    }
                                                }
                                            }
                                            assert_eq!(f.native_arithmetic_mode(),crate::scalar_function::NativeArithmeticInputMode::Raw);
                                            let expression = Expression::ScalarFunction(f);
                                            let target =
                                                crate::scalar_function::static_batch_target(
                                                    &expression,
                                                );
                                            (suite(expression), target)
                                        };
                                        let (original, target) = make();
                                        let (actual, actual_target) = make();
                                        assert_eq!(actual_target, target);
                                        let old = ArithmeticMissingContext::new(
                                            profile,
                                            enabled,
                                            values.clone(),
                                        );
                                        let new = ArithmeticMissingContext::new(
                                            profile,
                                            enabled,
                                            values.clone(),
                                        );
                                        for reuse in 0..2 {
                                            let input = || {
                                                let numeric = match field.eval_type() {
                                                    NativeEvalType::Int if field.is_unsigned() => {
                                                        [Datum::UInt(u64::MAX), Datum::UInt(3)]
                                                    }
                                                    NativeEvalType::Int => {
                                                        [Datum::Int(2), Datum::Int(3)]
                                                    }
                                                    NativeEvalType::Real => {
                                                        [Datum::Real(2.5), Datum::Real(0.0)]
                                                    }
                                                    NativeEvalType::Decimal => [
                                                        Datum::Decimal(
                                                            tidb_datatype::Decimal::from_int(2),
                                                        ),
                                                        Datum::Decimal(
                                                            tidb_datatype::Decimal::from_int(0),
                                                        ),
                                                    ],
                                                    NativeEvalType::String => [
                                                        Datum::new_string("2.5"),
                                                        Datum::new_string("0"),
                                                    ],
                                                    _ => unreachable!(),
                                                };
                                                let mut c = chunk(
                                                    &[field.clone(), field.clone()],
                                                    &[
                                                        numeric.to_vec(),
                                                        vec![Datum::Null, numeric[1].clone()],
                                                        numeric.to_vec(),
                                                    ],
                                                );
                                                c.set_sel(selection.clone());
                                                c
                                            };
                                            let mut a = input();
                                            let mut left = output_for(if mask & 1 == 0 {
                                                field
                                            } else {
                                                &storage
                                            });
                                            left.append_null(0);
                                            let expected = session_read_outcome(|| {
                                                original.run(&old, &mut a, &mut left)
                                            });
                                            let label=format!("arithmetic-missing/{full}/{name}/{fi}/mask{mask}/{vi}/{shape}/{profile}/{enabled}/{selection:?}/{reuse}/target{target:?}");
                                            if compare {
                                                let mut b = input();
                                                let mut right = output_for(if mask & 1 == 0 {
                                                    field
                                                } else {
                                                    &storage
                                                });
                                                right.append_null(0);
                                                let got = session_read_outcome(|| {
                                                    actual.run_with_tikv_batch(
                                                        &new,
                                                        &mut b,
                                                        &mut right,
                                                        &[field.clone(), field.clone()],
                                                    )
                                                });
                                                if got != expected
                                                    || right.column(0) != left.column(0)
                                                    || new.snapshot() != old.snapshot()
                                                {
                                                    eprintln!("ARITHMETIC MISSING DIFFERENCE {label} old={expected:?} new={got:?} oldcol={:?} newcol={:?} oldeffects={} neweffects={}",left.column(0),right.column(0),old.snapshot(),new.snapshot());
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
                                                if (route.fallback, route.refusals) != (0, 0)
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
        assert_eq!(count, if full { 643680 } else { 552960 });
        assert_eq!(
            hash,
            if full {
                0x1D09_4031_497F_3DBD
            } else {
                0x20C3_3635_8DC2_6867
            }
        );
        eprintln!("ARITHMETIC MISSING ORIGINAL full={full} {count}/{hash:016X}");
        if !gaps.is_empty() {
            eprintln!("ARITHMETIC MISSING ADMISSION RED full={full} {gaps:?}");
        }
        assert!(gaps.is_empty());
    });
}

#[test]
fn arithmetic_missing_argument_original_raw_datums() {
    if isolate_common_corpus("stateful_tests::arithmetic_missing_argument_original_raw_datums") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_arithmetic_missing_raw_datums(false);
}

#[test]
fn arithmetic_missing_argument_actual_raw_datums() {
    if isolate_common_corpus("stateful_tests::arithmetic_missing_argument_actual_raw_datums") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_arithmetic_missing_raw_datums(true);
}

fn run_arithmetic_missing_raw_datums(compare: bool) {
    use tidb_query_datatype::codec::data_type::ScalarValueRef;
    use tidb_query_expr::local::{HostArgReply, HostInvocation, HostStart, HostStep};
    let now = chrono::DateTime::parse_from_rfc3339("2026-10-21T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    tidb_datatype::with_test_conversion_clock(now, || {
        let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
        let mut gaps = vec![];
        let fields = [
            ty(),
            ty().with_unsigned(true),
            FieldType::new(FieldTypeCode::Double),
            FieldType::new(FieldTypeCode::NewDecimal)
                .with_flen(65)
                .with_decimal(5),
            string_ty(),
        ];
        for name in ["plus", "minus", "mul", "mod", "div", "intdiv"] {
            for (fi, field) in fields.iter().enumerate() {
                for mask in [2u8, 4, 6, 3, 5, 7] {
                    for (vi, value) in crate::tikv::byte_unary_value_tests::values()
                        .into_iter()
                        .enumerate()
                    {
                        for position in 0..2 {
                            let mut values = [Datum::Int(2), Datum::Int(3)];
                            values[position] = value.clone();
                            let mut function = ScalarFunction::new(
                                CiString::new(name),
                                field.clone(),
                                values
                                    .iter()
                                    .map(|v| literal(v.clone(), field.clone()))
                                    .collect(),
                            );
                            if mask & 1 != 0 {
                                function.ret_type = None;
                            }
                            for (i, arg) in function.args.iter_mut().enumerate() {
                                if mask & (2 << i) != 0 {
                                    let Expression::Constant(c) = arg else {
                                        unreachable!()
                                    };
                                    c.ret_type = None;
                                }
                            }
                            assert_eq!(
                                function.native_arithmetic_mode(),
                                crate::scalar_function::NativeArithmeticInputMode::Raw
                            );
                            let mut input = Chunk::new_with_capacity(&[], 1);
                            input.set_num_virtual_rows(1);
                            let old = ArithmeticMissingContext::new(0, false, values.clone());
                            let expected = session_read_outcome(|| {
                                function
                                    .eval(&old, input.get_row(0))
                                    .map(|v| leaf::frame(&v))
                            });
                            let label = format!(
                                "arithmetic-missing-raw/{name}/{fi}/mask{mask}/{vi}/{position}"
                            );
                            if compare {
                                let actual = suite(Expression::ScalarFunction(function.clone()));
                                let preparation =
                                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                        PreparedBatch::compile(
                                            &actual,
                                            &[],
                                            true,
                                            319,
                                            limits(),
                                            ExecutionLimits::default(),
                                            1 << 24,
                                        )
                                    }));
                                let preparation = match preparation {
                                    Ok(result) => result,
                                    Err(payload) => {
                                        eprintln!("ARITHMETIC MISSING RAW COMPILE PANIC {label}");
                                        std::panic::resume_unwind(payload)
                                    }
                                };
                                match preparation {
                                    Err(error) => {
                                        if gaps.len() < 20 {
                                            gaps.push(format!("{label}:{error:?}"));
                                        }
                                    }
                                    Ok(compiled) => {
                                        let source = &compiled.source;
                                        if source.host_metadata.len() != 1 {
                                            eprintln!(
                                                "ARITHMETIC MISSING RAW CATALOG {label}: {} hosts",
                                                source.host_metadata.len()
                                            );
                                        }
                                        assert_eq!(source.host_metadata.len(), 1);
                                        let new =
                                            ArithmeticMissingContext::new(0, false, values.clone());
                                        let store =
                                            RefCell::new(NativeValueStore::new(1 << 24).unwrap());
                                        store.borrow_mut().begin_window().unwrap();
                                        let catalog = source.hosts.as_ref().unwrap().clone();
                                        let mut hosts = NativeBatchHosts::new(
                                            catalog.clone(),
                                            &source.host_metadata,
                                            &new,
                                            1 << 24,
                                        )
                                        .with_native_values(Some(&store))
                                        .with_numeric_parents(
                                            source
                                                .context_policy
                                                .as_deref()
                                                .map_or(&[], |policy| {
                                                    policy.numeric_parents.as_ref()
                                                }),
                                        )
                                        .unwrap_or_else(|error| {
                                            eprintln!(
                                                "ARITHMETIC MISSING RAW PARENT {label}: {error:?}"
                                            );
                                            panic!("numeric parent rejected")
                                        });
                                        let slot = catalog.slot(0).unwrap();
                                        let signature = catalog.signature(slot).unwrap();
                                        let row = InputRow {
                                            occurrence: 19,
                                            input_row: 0,
                                        };
                                        let mut eval = EvalContext::default();
                                        let got = session_read_outcome(
                                            || -> std::result::Result<String, EvalError> {
                                                let HostStart::Pending { task, mut request } =
                                                    hosts
                                                        .start(
                                                            &mut eval,
                                                            HostInvocation {
                                                                slot,
                                                                row,
                                                                arg_types: &signature.arg_types,
                                                                return_type: &signature.return_type,
                                                            },
                                                        )
                                                        .unwrap()
                                                else {
                                                    panic!(
                                                        "arithmetic must demand actual arguments"
                                                    )
                                                };
                                                for _ in 0..8 {
                                                    assert!(request.index < 2);
                                                    let node = match &source.host_metadata[0] {
                                                        NativeHostMetadata::Arithmetic {
                                                            argument_nodes,
                                                            ..
                                                        } => argument_nodes[request.index],
                                                        _ => panic!("original arithmetic metadata"),
                                                    };
                                                    let token = store
                                                        .borrow_mut()
                                                        .insert_clone(
                                                            &values[request.index],
                                                            NativeValueRole::CharInput,
                                                            NativeValueOrigin { node, row },
                                                        )
                                                        .unwrap();
                                                    let fetched = VectorValue::from_scalar(
                                                        &ScalarValue::Bytes(Some(token)),
                                                        1,
                                                    );
                                                    match hosts.resume(
                                                        &mut eval,
                                                        &task,
                                                        HostArgReply {
                                                            index: request.index,
                                                            field_type: &signature.arg_types
                                                                [request.index],
                                                            values: &fetched,
                                                        },
                                                    ) {
                                                        Ok(HostStep::NeedArg(next)) => {
                                                            request = next
                                                        }
                                                        Ok(HostStep::Ready(output)) => {
                                                            let ScalarValueRef::Bytes(Some(token)) =
                                                                output.get_scalar_ref(0)
                                                            else {
                                                                panic!("native arithmetic token")
                                                            };
                                                            let value = store.borrow();
                                                            return Ok(leaf::frame(
                                                                value
                                                                    .get(
                                                                        token,
                                                                        NativeValueRole::Output,
                                                                        NativeValueOrigin {
                                                                            node: 0,
                                                                            row,
                                                                        },
                                                                    )
                                                                    .unwrap(),
                                                            ));
                                                        }
                                                        Ok(HostStep::NeedArgRow { .. }) => panic!(
                                                        "row arithmetic cannot request another row"
                                                    ),
                                                        Err(error) => {
                                                            return Err(hosts
                                                                .take_native_error()
                                                                .unwrap_or_else(|| {
                                                                    panic!(
                                                                "unexpected host failure {error:?}"
                                                            )
                                                                }))
                                                        }
                                                    }
                                                }
                                                panic!("arithmetic demand did not terminate")
                                            },
                                        );
                                        if got != expected || new.snapshot() != old.snapshot() {
                                            eprintln!("ARITHMETIC MISSING RAW DIFFERENCE {label} old={expected} new={got} oldeffects={} neweffects={}",old.snapshot(),new.snapshot());
                                        }
                                        assert_eq!(got, expected, "{label}");
                                        assert_eq!(new.snapshot(), old.snapshot(), "{label}");
                                    }
                                }
                            }
                            for byte in format!("{label}|{expected}|{}", old.snapshot()).bytes() {
                                hash = (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3);
                            }
                            count += 1;
                        }
                    }
                }
            }
        }
        eprintln!("ARITHMETIC MISSING RAW FINAL {compare} {count}/{hash:016X}");
        assert_eq!(count, 53640);
        assert_eq!(hash, 0x7623_E8AA_8B9D_6A50);
        eprintln!("ARITHMETIC MISSING RAW ORIGINAL {count}/{hash:016X}");
        if !gaps.is_empty() {
            eprintln!("ARITHMETIC MISSING RAW ADMISSION RED {gaps:?}");
        }
        assert!(gaps.is_empty());
    });
}

#[test]
fn arithmetic_missing_argument_resource_failure_keeps_same_owner_reusable() {
    let storage = FieldType::new(FieldTypeCode::NewDecimal)
        .with_flen(65)
        .with_decimal(6);
    for name in ["plus", "minus", "mul", "mod", "div", "intdiv"] {
        let mut f = ScalarFunction::new(
            CiString::new(name),
            storage.clone(),
            vec![
                parameter(0, string_ty()),
                literal(Datum::Int(1), string_ty()),
            ],
        );
        f.ret_type = None;
        for arg in &mut f.args {
            let Expression::Constant(c) = arg else {
                unreachable!()
            };
            c.ret_type = None;
        }
        let owner = suite(Expression::ScalarFunction(f));
        let mut prepared = PreparedBatchSuite::compile(
            &owner,
            &[ty()],
            true,
            919,
            limits(),
            ExecutionLimits::default(),
            1 << 20,
        )
        .unwrap();
        prepared.workers[0].max_materialization_retained_bytes = 16 * 1024;
        let mut ctx = ArithmeticMissingContext::new(
            0,
            true,
            [Datum::new_string("1".repeat(32768)), Datum::Int(1)],
        );
        let input = || {
            chunk(
                &[ty()],
                &[
                    vec![Datum::Int(0)],
                    vec![Datum::Int(1)],
                    vec![Datum::Int(2)],
                ],
            )
        };
        let mut first = input();
        let mut output = output_for(&storage);
        output.append_null(0);
        let error = prepared
            .run(
                &owner,
                &ctx,
                &mut EvalContext::default(),
                &mut first,
                &mut output,
                &[ty()],
            )
            .unwrap_err();
        assert!(
            matches!(error.local_error(), Some(LocalError::ResourceLimit(_))),
            "{name}: {error:?}"
        );
        assert_eq!(output.num_rows(), 1);
        assert!(!ctx
            .base
            .base
            .events
            .borrow()
            .iter()
            .any(|e| e == "precision"));
        ctx.base.base.values[0] = Datum::Null;
        ctx.base.base.enabled = false;
        ctx.base.base.events.borrow_mut().clear();
        for reads in &ctx.base.base.reads {
            reads.set(0);
        }
        let old = ArithmeticMissingContext::new(0, false, [Datum::Null, Datum::Int(1)]);
        let mut original_input = input();
        let mut expected = output_for(&storage);
        expected.append_null(0);
        owner.run(&old, &mut original_input, &mut expected).unwrap();
        let mut second = input();
        let mut got = output_for(&storage);
        got.append_null(0);
        prepared
            .run(
                &owner,
                &ctx,
                &mut EvalContext::default(),
                &mut second,
                &mut got,
                &[ty()],
            )
            .unwrap();
        assert_eq!(got.column(0), expected.column(0), "{name}");
        assert_eq!(ctx.snapshot(), old.snapshot(), "{name}");
        assert_eq!(
            prepared.workers[0].max_materialization_retained_bytes,
            16 * 1024
        );
    }
}

#[test]
fn arithmetic_missing_argument_late_real_div_type_failure_keeps_long_reuse_and_flags() {
    if isolate_common_corpus("stateful_tests::arithmetic_missing_argument_late_real_div_type_failure_keeps_long_reuse_and_flags") {return;}
    std::panic::set_hook(Box::new(|_| {}));
    let integer = ty();
    let storage = FieldType::new(FieldTypeCode::NewDecimal)
        .with_flen(65)
        .with_decimal(6);
    let mut count = 0;
    for absent in [false, true] {
        let make = || {
            let mut args = vec![parameter(0, integer.clone()), parameter(1, integer.clone())];
            for arg in &mut args {
                let Expression::Constant(c) = arg else {
                    unreachable!()
                };
                c.ret_type = None;
            }
            let mut f = ScalarFunction::new(CiString::new("intdiv"), integer.clone(), args);
            if absent {
                f.ret_type = None;
            }
            suite(Expression::ScalarFunction(f))
        };
        let original = make();
        let actual = make();
        for rows in [0, 3, 2051] {
            for first in [false, true] {
                let mut old =
                    ArithmeticMissingContext::new(10, first, [Datum::Int(10), Datum::Int(3)]);
                let mut new =
                    ArithmeticMissingContext::new(10, first, [Datum::Int(10), Datum::Int(3)]);
                let input = || {
                    let mut c = chunk(
                        &[integer.clone()],
                        &[
                            vec![Datum::Int(0)],
                            vec![Datum::Int(1)],
                            vec![Datum::Int(2)],
                        ],
                    );
                    c.set_sel(Some((0..rows).map(|i| [2, 0, 2, 1][i % 4]).collect()));
                    c
                };
                for round in 0..4 {
                    let enabled = if round % 2 == 0 { first } else { !first };
                    old.base.base.enabled = enabled;
                    new.base.base.enabled = enabled;
                    let field = if absent { &storage } else { &integer };
                    let mut a = input();
                    let mut left = output_for(field);
                    left.append_null(0);
                    let expected = session_read_outcome(|| original.run(&old, &mut a, &mut left));
                    let mut b = input();
                    let mut right = output_for(field);
                    right.append_null(0);
                    let got = session_read_outcome(|| {
                        actual.run_with_tikv_batch(&new, &mut b, &mut right, &[integer.clone()])
                    });
                    let label = format!("missing-long/{absent}/{rows}/{first}/{round}/{enabled}");
                    assert_eq!(got, expected, "{label}");
                    assert_eq!(right.column(0), left.column(0), "{label}");
                    assert_eq!(new.snapshot(), old.snapshot(), "{label}");
                    if rows == 2051 && round == 0 {
                        assert!(
                            expected.contains("numeric argument has a type"),
                            "{expected}"
                        );
                        assert_eq!(left.num_rows(), 1027);
                    }
                    let route = actual.batch_route_snapshot();
                    assert_eq!(
                        (route.fallback, route.refusals),
                        (0, 0),
                        "{label}: {route:?}"
                    );
                    assert!(tidb_query_expr::local::value_kernel_work_remaining().is_none());
                    count += 1;
                }
            }
        }
    }
    assert_eq!(count, 48);
}

#[test]
fn optional_membership_original_phases() {
    if isolate_common_corpus("stateful_tests::optional_membership_original_phases") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_optional_membership_phases(false);
}
#[test]
fn optional_membership_actual_phases() {
    if isolate_common_corpus("stateful_tests::optional_membership_actual_phases") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_optional_membership_phases(true);
}
fn run_optional_membership_phases(compare: bool) {
    let values = [
        [Datum::Null, Datum::new_string("selected")],
        [Datum::Int(0), Datum::Int(7)],
        [Datum::Int(1), Datum::new_string("7")],
        [Datum::new_string("1tail"), Datum::Bytes(vec![255])],
        [Datum::MinNotNull, Datum::Null],
        [Datum::Real(f64::NAN), Datum::new_string("null")],
        [Datum::UInt(u64::MAX), Datum::Int(-1)],
        [
            Datum::BinaryLiteral(tidb_datatype::BinaryLiteral::from(vec![
                128, 0, 0, 0, 0, 0, 0, 1,
            ])),
            Datum::Int(1),
        ],
    ];
    run_optional_control_matrix(
        compare,
        &["in", "interval", "greatest", "least"],
        &values,
        "optional-membership",
        "OPTIONAL MEMBERSHIP",
        Some((138240, 0x6D23E22B6F5BBE49)),
    );
}

#[test]
fn optional_membership_original_all_actual_values() {
    if isolate_common_corpus("stateful_tests::optional_membership_original_all_actual_values") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_optional_membership_all_actual_values(false);
}
#[test]
fn optional_membership_actual_all_actual_values() {
    if isolate_common_corpus("stateful_tests::optional_membership_actual_all_actual_values") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_optional_membership_all_actual_values(true);
}
fn run_optional_membership_all_actual_values(compare: bool) {
    let now = chrono::DateTime::parse_from_rfc3339("2026-10-21T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    tidb_datatype::with_test_conversion_clock(now, || {
        let fields = [
            ty(),
            ty().with_unsigned(true),
            string_ty(),
            FieldType::new(FieldTypeCode::Double),
            FieldType::new(FieldTypeCode::NewDecimal)
                .with_flen(65)
                .with_decimal(5),
            FieldType::new(FieldTypeCode::Datetime).with_decimal(3),
            FieldType::new(FieldTypeCode::Duration).with_decimal(3),
            FieldType::new(FieldTypeCode::Json),
        ];
        let mut gaps = vec![];
        let mut bounded_resources = 0usize;
        let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
        for name in ["in", "interval", "greatest", "least"] {
            for (fi, field) in fields.iter().enumerate() {
                for arity in [2, 4] {
                    for mask in [0, 1, 2, 4, 7] {
                        for (vi, value) in crate::tikv::byte_unary_value_tests::values()
                            .into_iter()
                            .enumerate()
                        {
                            for position in 0..2 {
                                let mut values = [Datum::Int(2), Datum::Int(3)];
                                values[position] = value.clone();
                                for enabled in [false, true] {
                                    for selection in [None, Some(vec![2, 0, 2]), Some(vec![])] {
                                        let make = || {
                                            let args = (0..arity)
                                                .map(|i| {
                                                    if i < 2 {
                                                        parameter(i, field.clone())
                                                    } else {
                                                        literal(
                                                            if i == 2 {
                                                                Datum::Null
                                                            } else {
                                                                Datum::Int(10)
                                                            },
                                                            field.clone(),
                                                        )
                                                    }
                                                })
                                                .collect();
                                            let mut e = call(name, field.clone(), args);
                                            erase_optional_call_declarations(&mut e, mask);
                                            suite(e)
                                        };
                                        let old_owner = make();
                                        let new_owner = make();
                                        let old = C::new(0, enabled, values.clone());
                                        let new = C::new(0, enabled, values.clone());
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
                                            let mut left = output_for(field);
                                            left.append_null(0);
                                            let expected = session_read_outcome(|| {
                                                old_owner.run(&old, &mut a, &mut left)
                                            });
                                            let label=format!("membership-full/{name}/{fi}/{arity}/{mask}/{vi}/{position}/{enabled}/{selection:?}/{reuse}");
                                            if compare {
                                                let mut b = input();
                                                let mut right = output_for(field);
                                                right.append_null(0);
                                                let got = session_read_outcome(|| {
                                                    new_owner.run_with_tikv_batch(
                                                        &new,
                                                        &mut b,
                                                        &mut right,
                                                        &[ty()],
                                                    )
                                                });
                                                if got.contains("class: ResourceLimit") {
                                                    assert_eq!(vi,110,"unexpected resource refusal {label}: {got}");
                                                    assert!(selection
                                                        .as_ref()
                                                        .is_none_or(|s| !s.is_empty()));
                                                    assert_eq!(
                                                        right.num_rows(),
                                                        1,
                                                        "resource output {label}"
                                                    );
                                                    let route = new_owner.batch_route_snapshot();
                                                    assert_eq!(
                                                        (route.fallback, route.refusals),
                                                        (0, 0),
                                                        "{label}: {route:?}"
                                                    );
                                                    bounded_resources += 1;
                                                } else {
                                                    if got != expected
                                                        || right.column(0) != left.column(0)
                                                        || new.snapshot() != old.snapshot()
                                                    {
                                                        eprintln!("MEMBERSHIP FULL DIFFERENCE {label} old={expected} new={got} oldcol={:?} newcol={:?} oldeffects={} neweffects={}",left.column(0),right.column(0),old.snapshot(),new.snapshot());
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
                                                    let route = new_owner.batch_route_snapshot();
                                                    if (route.fallback != 0 || route.refusals != 0)
                                                        && gaps.len() < 20
                                                    {
                                                        gaps.push(format!("{label}: {route:?}"));
                                                    }
                                                }
                                            }
                                            for b in format!(
                                                "{label}|{expected}|{:?}|{}",
                                                left.column(0),
                                                old.snapshot()
                                            )
                                            .bytes()
                                            {
                                                hash = (hash ^ u64::from(b))
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
        eprintln!("OPTIONAL MEMBERSHIP FULL ORIGINAL {count}/{hash:016X}");
        eprintln!("MEMBERSHIP FULL BOUNDED RESOURCES {bounded_resources}");
        assert_eq!(bounded_resources, if compare { 2720 } else { 0 });
        assert_eq!((count, hash), (1144320, 0x0926C1DF1FCFFBE1));
        if !gaps.is_empty() {
            eprintln!("MEMBERSHIP FULL ADMISSION RED {gaps:?}");
        }
        assert!(gaps.is_empty());
    });
}
#[test]
fn optional_membership_original_raw_datums() {
    if isolate_common_corpus("stateful_tests::optional_membership_original_raw_datums") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_optional_membership_raw_datums(false);
}

#[test]
fn optional_membership_actual_raw_datums() {
    if isolate_common_corpus("stateful_tests::optional_membership_actual_raw_datums") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_optional_membership_raw_datums(true);
}

fn run_optional_membership_raw_datums(compare: bool) {
    use tidb_query_datatype::codec::data_type::ScalarValueRef;
    use tidb_query_expr::local::{HostArgReply, HostInvocation, HostStart, HostStep};
    let now = chrono::DateTime::parse_from_rfc3339("2026-10-21T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    tidb_datatype::with_test_conversion_clock(now, || {
        let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
        let mut gaps = vec![];
        let mut bounded_resources = 0usize;
        let fields = [
            ty(),
            ty().with_unsigned(true),
            FieldType::new(FieldTypeCode::Double),
            FieldType::new(FieldTypeCode::NewDecimal)
                .with_flen(65)
                .with_decimal(5),
            string_ty(),
        ];
        for name in ["in", "interval", "greatest", "least"] {
            for (fi, field) in fields.iter().enumerate() {
                for mask in [0u8, 1, 2, 4, 7] {
                    for (vi, value) in crate::tikv::byte_unary_value_tests::values()
                        .into_iter()
                        .enumerate()
                    {
                        for position in 0..2 {
                            let mut values = [Datum::Int(2), Datum::Int(3)];
                            values[position] = value.clone();
                            let mut function = ScalarFunction::new(
                                CiString::new(name),
                                field.clone(),
                                values
                                    .iter()
                                    .map(|v| literal(v.clone(), field.clone()))
                                    .collect(),
                            );
                            if mask & 1 != 0 {
                                function.ret_type = None;
                            }
                            for (i, arg) in function.args.iter_mut().enumerate() {
                                if mask & (2 << i) != 0 {
                                    let Expression::Constant(c) = arg else {
                                        unreachable!()
                                    };
                                    c.ret_type = None;
                                }
                            }
                            function.prepare_in_string_hash_set();
                            let mut input = Chunk::new_with_capacity(&[], 1);
                            input.set_num_virtual_rows(1);
                            let old = C::new(0, false, values.clone());
                            let expected = session_read_outcome(|| {
                                function
                                    .eval(&old, input.get_row(0))
                                    .map(|v| leaf::frame(&v))
                            });
                            let label =
                                format!("membership-raw/{name}/{fi}/mask{mask}/{vi}/{position}");
                            if compare {
                                let actual = suite(Expression::ScalarFunction(function.clone()));
                                let preparation =
                                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                        PreparedBatch::compile_calculation_profile(
                                            &actual,
                                            0,
                                            &[],
                                            true,
                                            319,
                                            limits(),
                                            ExecutionLimits::default(),
                                            1 << 24,
                                            true,
                                        )
                                    }));
                                let preparation = match preparation {
                                    Ok(result) => result,
                                    Err(payload) => {
                                        eprintln!("MEMBERSHIP RAW COMPILE PANIC {label}");
                                        std::panic::resume_unwind(payload)
                                    }
                                };
                                match preparation {
                                    Err(error) => {
                                        if error.refusal() == BatchRefusal::Resource {
                                            assert_eq!(vi,110,"unexpected preparation resource {label}: {error:?}");
                                            bounded_resources += 1;
                                        } else if gaps.len() < 20 {
                                            gaps.push(format!("{label}:{error:?}"));
                                        }
                                    }
                                    Ok(compiled) => {
                                        let source = &compiled.source;
                                        if source.host_metadata.len() != 1 {
                                            eprintln!(
                                                "MEMBERSHIP RAW CATALOG {label}: {} hosts",
                                                source.host_metadata.len()
                                            );
                                        }
                                        assert_eq!(source.host_metadata.len(), 1);
                                        let new = C::new(0, false, values.clone());
                                        let store =
                                            RefCell::new(NativeValueStore::new(1 << 24).unwrap());
                                        store.borrow_mut().begin_window().unwrap();
                                        let catalog = source.hosts.as_ref().unwrap().clone();
                                        let mut hosts = NativeBatchHosts::new(
                                            catalog.clone(),
                                            &source.host_metadata,
                                            &new,
                                            1 << 24,
                                        )
                                        .with_native_values(Some(&store))
                                        .with_constant_probes(&source.host_probes)
                                        .with_numeric_parents(
                                            source
                                                .context_policy
                                                .as_deref()
                                                .map_or(&[], |policy| {
                                                    policy.numeric_parents.as_ref()
                                                }),
                                        )
                                        .unwrap_or_else(|error| {
                                            eprintln!("MEMBERSHIP RAW PARENT {label}: {error:?}");
                                            panic!("numeric parent rejected")
                                        });
                                        let slot = catalog.slot(0).unwrap();
                                        let signature = catalog.signature(slot).unwrap();
                                        let row = InputRow {
                                            occurrence: 19,
                                            input_row: 0,
                                        };
                                        let mut eval = EvalContext::default();
                                        let got = session_read_outcome(
                                            || -> std::result::Result<String, EvalError> {
                                                let HostStart::Pending { task, mut request } =
                                                    hosts
                                                        .start(
                                                            &mut eval,
                                                            HostInvocation {
                                                                slot,
                                                                row,
                                                                arg_types: &signature.arg_types,
                                                                return_type: &signature.return_type,
                                                            },
                                                        )
                                                        .unwrap()
                                                else {
                                                    panic!(
                                                        "arithmetic must demand actual arguments"
                                                    )
                                                };
                                                for _ in 0..8 {
                                                    assert!(request.index < 2);
                                                    let node = match &source.host_metadata[0] {
                                                        NativeHostMetadata::In(metadata) => {
                                                            metadata.arguments[request.index].node
                                                        }
                                                        NativeHostMetadata::Interval(metadata) => {
                                                            metadata.arguments[request.index].node
                                                        }
                                                        NativeHostMetadata::Extremum(metadata) => {
                                                            metadata.arguments[request.index]
                                                        }
                                                        _ => panic!("original arithmetic metadata"),
                                                    };
                                                    let token = store
                                                        .borrow_mut()
                                                        .insert_clone(
                                                            &values[request.index],
                                                            NativeValueRole::CharInput,
                                                            NativeValueOrigin { node, row },
                                                        )
                                                        .unwrap();
                                                    let fetched = VectorValue::from_scalar(
                                                        &ScalarValue::Bytes(Some(token)),
                                                        1,
                                                    );
                                                    match hosts.resume(
                                                        &mut eval,
                                                        &task,
                                                        HostArgReply {
                                                            index: request.index,
                                                            field_type: &signature.arg_types
                                                                [request.index],
                                                            values: &fetched,
                                                        },
                                                    ) {
                                                        Ok(HostStep::NeedArg(next)) => {
                                                            request = next
                                                        }
                                                        Ok(HostStep::Ready(output)) => {
                                                            let ScalarValueRef::Bytes(Some(token)) =
                                                                output.get_scalar_ref(0)
                                                            else {
                                                                panic!("native arithmetic token")
                                                            };
                                                            let value = store.borrow();
                                                            return Ok(leaf::frame(
                                                                value
                                                                    .get(
                                                                        token,
                                                                        NativeValueRole::Output,
                                                                        NativeValueOrigin {
                                                                            node: 0,
                                                                            row,
                                                                        },
                                                                    )
                                                                    .unwrap(),
                                                            ));
                                                        }
                                                        Ok(HostStep::NeedArgRow { .. }) => panic!(
                                                        "row arithmetic cannot request another row"
                                                    ),
                                                        Err(error) => {
                                                            if matches!(
                                                                error,
                                                                LocalError::ResourceLimit(_)
                                                            ) {
                                                                return Err(EvalError::ExpressionRuntimeFailure(crate::ExpressionRuntimeFailure::from_local_eval(error,None)));
                                                            }
                                                            return Err(hosts
                                                                .take_native_error()
                                                                .unwrap_or_else(|| {
                                                                    panic!(
                                                                "unexpected host failure {error:?}"
                                                            )
                                                                }));
                                                        }
                                                    }
                                                }
                                                panic!("arithmetic demand did not terminate")
                                            },
                                        );
                                        if got.contains("class: ResourceLimit") {
                                            assert_eq!(
                                                vi, 110,
                                                "unexpected direct resource {label}: {got}"
                                            );
                                            bounded_resources += 1;
                                        } else {
                                            if got != expected || new.snapshot() != old.snapshot() {
                                                eprintln!("MEMBERSHIP RAW DIFFERENCE {label} old={expected} new={got} oldeffects={} neweffects={}",old.snapshot(),new.snapshot());
                                            }
                                            assert_eq!(got, expected, "{label}");
                                            assert_eq!(new.snapshot(), old.snapshot(), "{label}");
                                        }
                                    }
                                }
                            }
                            for byte in format!("{label}|{expected}|{}", old.snapshot()).bytes() {
                                hash = (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3);
                            }
                            count += 1;
                        }
                    }
                }
            }
        }
        eprintln!("MEMBERSHIP RAW FINAL {compare} {count}/{hash:016X}");
        assert_eq!(count, 29800);
        assert_eq!(hash, 0x0BBCF87024DEBD9F);
        eprintln!("MEMBERSHIP RAW BOUNDED RESOURCES {bounded_resources}");
        assert_eq!(bounded_resources, if compare { 79 } else { 0 });
        eprintln!("MEMBERSHIP RAW ORIGINAL {count}/{hash:016X}");
        if !gaps.is_empty() {
            eprintln!("MEMBERSHIP RAW ADMISSION RED {gaps:?}");
        }
        assert!(gaps.is_empty());
    });
}

#[test]
fn optional_membership_resource_failure_keeps_same_owner_reusable() {
    for name in ["in", "interval", "greatest", "least"] {
        let mut root = call(
            name,
            ty(),
            vec![
                parameter(0, string_ty()),
                literal(Datum::Int(1), string_ty()),
            ],
        );
        erase_optional_call_declarations(&mut root, 7);
        let owner = suite(root);
        let mut prepared = PreparedBatchSuite::compile(
            &owner,
            &[ty()],
            true,
            919,
            limits(),
            ExecutionLimits::default(),
            1 << 20,
        )
        .unwrap();
        prepared.workers[0].max_materialization_retained_bytes = 16 * 1024;
        let mut ctx = C::new(
            0,
            true,
            [Datum::new_string("1".repeat(32768)), Datum::Int(1)],
        );
        let input = || {
            chunk(
                &[ty()],
                &[
                    vec![Datum::Int(0)],
                    vec![Datum::Int(1)],
                    vec![Datum::Int(2)],
                ],
            )
        };
        let mut first = input();
        let mut output = output_for(&ty());
        output.append_null(0);
        let error = prepared
            .run(
                &owner,
                &ctx,
                &mut EvalContext::default(),
                &mut first,
                &mut output,
                &[ty()],
            )
            .unwrap_err();
        assert!(
            matches!(error.local_error(), Some(LocalError::ResourceLimit(_))),
            "{name}: {error:?}"
        );
        assert_eq!(output.num_rows(), 1);
        ctx.values[0] = Datum::Null;
        ctx.enabled = false;
        ctx.events.borrow_mut().clear();
        for r in &ctx.reads {
            r.set(0);
        }
        let old = C::new(0, false, [Datum::Null, Datum::Int(1)]);
        let mut a = input();
        let mut expected = output_for(&ty());
        expected.append_null(0);
        owner.run(&old, &mut a, &mut expected).unwrap();
        let mut b = input();
        let mut got = output_for(&ty());
        got.append_null(0);
        prepared
            .run(
                &owner,
                &ctx,
                &mut EvalContext::default(),
                &mut b,
                &mut got,
                &[ty()],
            )
            .unwrap();
        assert_eq!(got.column(0), expected.column(0), "{name}");
        assert_eq!(ctx.snapshot(), old.snapshot(), "{name}");
        assert_eq!(
            prepared.workers[0].max_materialization_retained_bytes,
            16 * 1024
        );
    }
}

#[test]
fn optional_membership_long_late_errors_recover_across_flags() {
    if isolate_common_corpus(
        "stateful_tests::optional_membership_long_late_errors_recover_across_flags",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    let mut count = 0;
    for name in ["in", "interval", "greatest", "least"] {
        for mask in [0, 1, 2, 4, 7] {
            for rows in [0usize, 3, 2051] {
                let make = || {
                    let mut e = call(name, ty(), vec![parameter(0, ty()), parameter(1, ty())]);
                    erase_optional_call_declarations(&mut e, mask);
                    suite(e)
                };
                let original = make();
                let actual = make();
                let mut old = C::new(4, true, [Datum::Int(10), Datum::Int(3)]);
                let mut new = C::new(4, true, [Datum::Int(10), Datum::Int(3)]);
                for (round, enabled) in [true, false, true, false].into_iter().enumerate() {
                    old.enabled = enabled;
                    new.enabled = enabled;
                    old.profile = if round == 0 { 4 } else { 0 };
                    new.profile = old.profile;
                    let input = || {
                        let mut c = chunk(
                            &[ty()],
                            &[
                                vec![Datum::Int(0)],
                                vec![Datum::Int(1)],
                                vec![Datum::Int(2)],
                            ],
                        );
                        c.set_sel(Some((0..rows).map(|i| [2, 0, 2][i % 3]).collect()));
                        c
                    };
                    let mut a = input();
                    let mut left = output_for(&ty());
                    left.append_null(0);
                    let expected = session_read_outcome(|| original.run(&old, &mut a, &mut left));
                    let mut b = input();
                    let mut right = output_for(&ty());
                    right.append_null(0);
                    let got = session_read_outcome(|| {
                        actual.run_with_tikv_batch(&new, &mut b, &mut right, &[ty()])
                    });
                    let label = format!("{name}/mask{mask}/rows{rows}/round{round}");
                    assert_eq!(got, expected, "{label}");
                    assert_eq!(right.column(0), left.column(0), "{label}");
                    assert_eq!(new.snapshot(), old.snapshot(), "{label}");
                    let route = actual.batch_route_snapshot();
                    assert_eq!(
                        (route.fallback, route.refusals),
                        (0, 0),
                        "{label}: {route:?}"
                    );
                    count += 1;
                }
            }
        }
    }
    assert_eq!(count, 240);
    eprintln!("MEMBERSHIP LONG REUSE {count}");
}

// The original prefix branches can demand the same live parameter again.
struct PrefixContext {
    base: C,
}
impl PrefixContext {
    fn new(profile: usize, enabled: bool, values: [Datum; 2]) -> Self {
        Self {
            base: C::new(profile, enabled, values),
        }
    }
    fn snapshot(&self) -> String {
        self.base.snapshot()
    }
}
impl Columns for PrefixContext {
    fn ready_value_cache(&self) -> Option<&crate::ReadyValueCache> {
        self.base.ready_value_cache()
    }
    fn get(&self, _: &[String]) -> Option<Datum> {
        panic!("no expression callback")
    }
    fn param_value(&self, order: usize) -> std::result::Result<Datum, EvalError> {
        let value = self.base.param_value(order)?;
        let n = self.base.reads[order].get();
        if order == 0 && ((self.base.profile == 8 && n > 1) || (self.base.profile == 9 && n == 1)) {
            Ok(Datum::Null)
        } else {
            Ok(value)
        }
    }
    fn enable_vectorized_expression(&self) -> bool {
        self.base.enable_vectorized_expression()
    }
    fn time_zone(&self) -> SessionTimeZone {
        self.base.time_zone()
    }
    fn type_flags(&self) -> tidb_datatype::ConversionFlags {
        self.base.type_flags()
    }
    fn truncate_level(&self) -> crate::ErrorLevel {
        self.base.truncate_level()
    }
    fn append_warning(&self, code: u16, message: &str) {
        self.base.append_warning(code, message)
    }
    fn set_uservar(&self, name: &str, value: Datum) {
        self.base.set_uservar(name, value)
    }
}
#[test]
fn optional_reserved_prefix_original_phases() {
    if isolate_common_corpus("stateful_tests::optional_reserved_prefix_original_phases") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_optional_reserved_prefix_matrix(false);
}
#[test]
fn optional_reserved_prefix_actual_phases() {
    if isolate_common_corpus("stateful_tests::optional_reserved_prefix_actual_phases") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_optional_reserved_prefix_matrix(true);
}
fn run_optional_reserved_prefix_matrix(compare: bool) {
    let pairs = [
        [Datum::Null, Datum::new_string("selected")],
        [Datum::Int(0), Datum::Int(7)],
        [Datum::Int(1), Datum::new_string("7")],
        [Datum::new_string("1tail"), Datum::Bytes(vec![255])],
        [Datum::MinNotNull, Datum::Null],
        [Datum::Real(f64::NAN), Datum::new_string("null")],
        [Datum::UInt(u64::MAX), Datum::Int(-1)],
        [
            Datum::new_string("2026-10-21 01:02:03"),
            Datum::new_string("1"),
        ],
    ];
    reserved_prefix_phase_observer(
        compare,
        &[
            "cast_unknown",
            "cast_unknown_in_union",
            "unknown_in_union",
            "date_add_invalid_unit",
            "date_sub_invalid_unit",
        ],
        &pairs,
        "reserved-prefix",
        "RESERVED PREFIX",
        Some((288000, 0x0F79E3203A9C6199)),
    );
}
fn reserved_prefix_phase_observer(
    compare: bool,
    names: &[&str],
    values: &[[Datum; 2]],
    prefix: &str,
    heading: &str,
    golden: Option<(usize, u64)>,
) {
    let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
    let mut gaps = vec![];
    let retained_bulk_gaps = 0usize;
    for &name in names {
        for shape in 0..5 {
            for mask in 0..8 {
                for (fi, field) in [ty(), string_ty(), FieldType::new(FieldTypeCode::Json)]
                    .into_iter()
                    .enumerate()
                {
                    for (vi, values) in values.iter().cloned().enumerate() {
                        for profile in [0, 4, 5, 8, 9] {
                            for enabled in [false, true] {
                                for selection in [None, Some(vec![2, 0, 2]), Some(vec![])] {
                                    let make = || {
                                        let p0 = parameter(0, string_ty());
                                        let p1 = parameter(1, string_ty());
                                        let fallback =
                                            literal(Datum::new_string("fallback"), string_ty());
                                        let mut args = match name {
                                            name if name.starts_with("cast_")
                                                || name.ends_with("_in_union") =>
                                            {
                                                vec![p0]
                                            }
                                            "if" | "case" => vec![p0, p1, fallback],
                                            "not" | "isnull" | "istrue" | "isfalse"
                                            | "isnottrue" | "isnotfalse" | "isunknown"
                                            | "isnotunknown" | "istrue_with_null" | "bitneg"
                                            | "unaryplus" | "unaryminus" | "charset"
                                            | "collation" | "coercibility" => vec![p0],
                                            _ => vec![p0, p1],
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
                                    let old = PrefixContext::new(profile, enabled, values.clone());
                                    let new = PrefixContext::new(profile, enabled, values.clone());
                                    // Record retained whole-selection profiles
                                    // separately; they are an explicit existing
                                    // entry gap, not an optional row declaration.
                                    let root = &original.program.calculated[0];
                                    let _whole = original.program.vectorizable
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
                                        let label=format!("{prefix}/{name}/{shape}/{mask}/{fi}/{vi}/{profile}/{enabled}/{selection:?}/{reuse}");
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
                                                eprintln!("{heading} DIFFERENCE {label} old={expected:?} new={got:?} oldcol={:?} newcol={:?} oldeffects={} neweffects={}",left.column(0),right.column(0),old.snapshot(),new.snapshot());
                                            }
                                            assert_eq!(got, expected, "{label}");
                                            assert_eq!(right.column(0), left.column(0), "{label}");
                                            assert_eq!(new.snapshot(), old.snapshot(), "{label}");
                                            let route = actual.batch_route_snapshot();
                                            if route.fallback != 0 || route.refusals != 0 {
                                                if gaps.len() < 20 {
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
                                        if matches!(name, "charset" | "collation" | "coercibility")
                                        {
                                            assert_eq!(
                                                (old.base.reads[0].get(), old.base.reads[1].get()),
                                                (0, 0),
                                                "{label}"
                                            );
                                            if compare {
                                                assert_eq!(
                                                    (
                                                        new.base.reads[0].get(),
                                                        new.base.reads[1].get()
                                                    ),
                                                    (0, 0),
                                                    "{label}"
                                                );
                                            }
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
    eprintln!(
        "{heading} ORIGINAL {count}/{hash:016X}; retained whole profiles {retained_bulk_gaps}"
    );
    if let Some(golden) = golden {
        assert_eq!((count, hash), golden);
    }
    if !gaps.is_empty() {
        eprintln!("{heading} ROW ADMISSION RED {gaps:?}");
    }
    assert!(gaps.is_empty());
}

#[test]
fn optional_reserved_prefix_original_all_actual_values() {
    if isolate_common_corpus("stateful_tests::optional_reserved_prefix_original_all_actual_values")
    {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_optional_reserved_prefix_all_actual_values(false);
}
#[test]
fn optional_reserved_prefix_actual_all_actual_values() {
    if isolate_common_corpus("stateful_tests::optional_reserved_prefix_actual_all_actual_values") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_optional_reserved_prefix_all_actual_values(true);
}
fn run_optional_reserved_prefix_all_actual_values(compare: bool) {
    let now = chrono::DateTime::parse_from_rfc3339("2026-10-21T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    tidb_datatype::with_test_conversion_clock(now, || {
        let fields = [
            ty(),
            ty().with_unsigned(true),
            string_ty(),
            FieldType::new(FieldTypeCode::Double),
            FieldType::new(FieldTypeCode::NewDecimal)
                .with_flen(65)
                .with_decimal(5),
            FieldType::new(FieldTypeCode::Datetime).with_decimal(3),
            FieldType::new(FieldTypeCode::Duration).with_decimal(3),
            FieldType::new(FieldTypeCode::Json),
        ];
        let mut gaps = vec![];
        let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
        for name in [
            "cast_unknown",
            "cast_unknown_in_union",
            "unknown_in_union",
            "date_add_invalid_unit",
            "date_sub_invalid_unit",
        ] {
            for (fi, field) in fields.iter().enumerate() {
                for arity in [if name.starts_with("date_") { 2 } else { 1 }] {
                    for mask in [0, 1, 2, 4, 7] {
                        for (vi, value) in crate::tikv::byte_unary_value_tests::values()
                            .into_iter()
                            .enumerate()
                        {
                            for position in 0..arity {
                                let mut values = [Datum::Int(2), Datum::Int(3)];
                                values[position] = value.clone();
                                for enabled in [false, true] {
                                    for selection in [None, Some(vec![2, 0, 2]), Some(vec![])] {
                                        let make = || {
                                            let args = (0..arity)
                                                .map(|i| {
                                                    if i < 2 {
                                                        parameter(i, field.clone())
                                                    } else {
                                                        literal(
                                                            if i == 2 {
                                                                Datum::Null
                                                            } else {
                                                                Datum::Int(10)
                                                            },
                                                            field.clone(),
                                                        )
                                                    }
                                                })
                                                .collect();
                                            let mut e = call(name, field.clone(), args);
                                            erase_optional_call_declarations(&mut e, mask);
                                            suite(e)
                                        };
                                        let old_owner = make();
                                        let new_owner = make();
                                        let old = PrefixContext::new(0, enabled, values.clone());
                                        let new = PrefixContext::new(0, enabled, values.clone());
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
                                            let mut left = output_for(field);
                                            left.append_null(0);
                                            let expected = session_read_outcome(|| {
                                                old_owner.run(&old, &mut a, &mut left)
                                            });
                                            let label=format!("reserved-prefix-full/{name}/{fi}/{arity}/{mask}/{vi}/{position}/{enabled}/{selection:?}/{reuse}");
                                            if compare {
                                                let mut b = input();
                                                let mut right = output_for(field);
                                                right.append_null(0);
                                                let got = session_read_outcome(|| {
                                                    new_owner.run_with_tikv_batch(
                                                        &new,
                                                        &mut b,
                                                        &mut right,
                                                        &[ty()],
                                                    )
                                                });
                                                if got != expected
                                                    || right.column(0) != left.column(0)
                                                    || new.snapshot() != old.snapshot()
                                                {
                                                    eprintln!("RESERVED PREFIX FULL DIFFERENCE {label} old={expected} new={got} oldcol={:?} newcol={:?} oldeffects={} neweffects={}",left.column(0),right.column(0),old.snapshot(),new.snapshot());
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
                                                let route = new_owner.batch_route_snapshot();
                                                if (route.fallback != 0 || route.refusals != 0)
                                                    && gaps.len() < 20
                                                {
                                                    gaps.push(format!("{label}: {route:?}"));
                                                }
                                            }
                                            for b in format!(
                                                "{label}|{expected}|{:?}|{}",
                                                left.column(0),
                                                old.snapshot()
                                            )
                                            .bytes()
                                            {
                                                hash = (hash ^ u64::from(b))
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
        eprintln!("RESERVED PREFIX FULL ORIGINAL {count}/{hash:016X}");
        assert_eq!((count, hash), (500640, 0x34F5F154574439E5));
        if !gaps.is_empty() {
            eprintln!("RESERVED PREFIX FULL ADMISSION RED {gaps:?}");
        }
        assert!(gaps.is_empty());
    });
}

#[test]
fn optional_reserved_prefix_extended_original_phases() {
    if isolate_common_corpus("stateful_tests::optional_reserved_prefix_extended_original_phases") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_reserved_prefix_extended_phases(false);
}
#[test]
fn optional_reserved_prefix_extended_actual_phases() {
    if isolate_common_corpus("stateful_tests::optional_reserved_prefix_extended_actual_phases") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_reserved_prefix_extended_phases(true);
}
fn run_reserved_prefix_extended_phases(compare: bool) {
    let pairs = [
        [Datum::Null, Datum::new_string("selected")],
        [Datum::Int(0), Datum::Int(7)],
        [Datum::Int(1), Datum::new_string("7")],
        [Datum::new_string("1tail"), Datum::Bytes(vec![255])],
        [Datum::MinNotNull, Datum::Null],
        [Datum::Real(f64::NAN), Datum::new_string("null")],
        [Datum::UInt(u64::MAX), Datum::Int(-1)],
        [
            Datum::new_string("2026-10-21 01:02:03"),
            Datum::new_string("1"),
        ],
    ];
    reserved_prefix_phase_observer(
        compare,
        &N36_EXTENDED_PREFIX_NAMES,
        &pairs,
        "reserved-prefix-extended",
        "RESERVED PREFIX EXTENDED",
        Some((691200, 0x5AD410F03E154979)),
    );
}
const N36_EXTENDED_PREFIX_NAMES: [&str; 12] = [
    "cast_unsigned_in_union",
    "cast_real_to_decimal_in_union",
    "cast_int_to_decimal_in_union",
    "cast_string_to_decimal_in_union",
    "cast_decimal_in_union",
    "cast_real_int_in_union",
    "cast_real_in_union",
    "cast_unrecognized_target",
    "cast_unrecognized_in_union",
    "yet_unregistered_in_union",
    "date_add_zz_unknown",
    "date_sub_zz_unknown",
];
#[test]
fn optional_reserved_prefix_extended_original_all_actual_values() {
    if isolate_common_corpus(
        "stateful_tests::optional_reserved_prefix_extended_original_all_actual_values",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_optional_reserved_prefix_extended_all_actual_values(false);
}
#[test]
fn optional_reserved_prefix_extended_actual_all_actual_values() {
    if isolate_common_corpus(
        "stateful_tests::optional_reserved_prefix_extended_actual_all_actual_values",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_optional_reserved_prefix_extended_all_actual_values(true);
}
fn run_optional_reserved_prefix_extended_all_actual_values(compare: bool) {
    let now = chrono::DateTime::parse_from_rfc3339("2026-10-21T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    tidb_datatype::with_test_conversion_clock(now, || {
        let fields = [
            ty(),
            ty().with_unsigned(true),
            string_ty(),
            FieldType::new(FieldTypeCode::Double),
            FieldType::new(FieldTypeCode::NewDecimal)
                .with_flen(65)
                .with_decimal(5),
            FieldType::new(FieldTypeCode::Datetime).with_decimal(3),
            FieldType::new(FieldTypeCode::Duration).with_decimal(3),
            FieldType::new(FieldTypeCode::Json),
        ];
        let mut gaps = vec![];
        let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
        for name in N36_EXTENDED_PREFIX_NAMES {
            for (fi, field) in fields.iter().enumerate() {
                for arity in [if name.starts_with("date_") { 2 } else { 1 }] {
                    for mask in [0, 1, 2, 4, 7] {
                        for (vi, value) in crate::tikv::byte_unary_value_tests::values()
                            .into_iter()
                            .enumerate()
                        {
                            for position in 0..arity {
                                let mut values = [Datum::Int(2), Datum::Int(3)];
                                values[position] = value.clone();
                                for enabled in [false, true] {
                                    for selection in [None, Some(vec![2, 0, 2]), Some(vec![])] {
                                        let make = || {
                                            let args = (0..arity)
                                                .map(|i| {
                                                    if i < 2 {
                                                        parameter(i, field.clone())
                                                    } else {
                                                        literal(
                                                            if i == 2 {
                                                                Datum::Null
                                                            } else {
                                                                Datum::Int(10)
                                                            },
                                                            field.clone(),
                                                        )
                                                    }
                                                })
                                                .collect();
                                            let mut e = call(name, field.clone(), args);
                                            erase_optional_call_declarations(&mut e, mask);
                                            suite(e)
                                        };
                                        let old_owner = make();
                                        let new_owner = make();
                                        let old = PrefixContext::new(0, enabled, values.clone());
                                        let new = PrefixContext::new(0, enabled, values.clone());
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
                                            let mut left = output_for(field);
                                            left.append_null(0);
                                            let expected = session_read_outcome(|| {
                                                old_owner.run(&old, &mut a, &mut left)
                                            });
                                            let label=format!("reserved-prefix-extended-full/{name}/{fi}/{arity}/{mask}/{vi}/{position}/{enabled}/{selection:?}/{reuse}");
                                            if compare {
                                                let mut b = input();
                                                let mut right = output_for(field);
                                                right.append_null(0);
                                                let got = session_read_outcome(|| {
                                                    new_owner.run_with_tikv_batch(
                                                        &new,
                                                        &mut b,
                                                        &mut right,
                                                        &[ty()],
                                                    )
                                                });
                                                if got != expected
                                                    || right.column(0) != left.column(0)
                                                    || new.snapshot() != old.snapshot()
                                                {
                                                    eprintln!("RESERVED PREFIX EXTENDED FULL DIFFERENCE {label} old={expected} new={got} oldcol={:?} newcol={:?} oldeffects={} neweffects={}",left.column(0),right.column(0),old.snapshot(),new.snapshot());
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
                                                let route = new_owner.batch_route_snapshot();
                                                if (route.fallback != 0 || route.refusals != 0)
                                                    && gaps.len() < 20
                                                {
                                                    gaps.push(format!("{label}: {route:?}"));
                                                }
                                            }
                                            for b in format!(
                                                "{label}|{expected}|{:?}|{}",
                                                left.column(0),
                                                old.snapshot()
                                            )
                                            .bytes()
                                            {
                                                hash = (hash ^ u64::from(b))
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
        eprintln!("RESERVED PREFIX EXTENDED FULL ORIGINAL {count}/{hash:016X}");
        assert_eq!((count, hash), (1001280, 0x0883DF2991A572B5));
        if !gaps.is_empty() {
            eprintln!("RESERVED PREFIX EXTENDED FULL ADMISSION RED {gaps:?}");
        }
        assert!(gaps.is_empty());
    });
}
#[test]
fn optional_reserved_prefix_dates_original_phases() {
    if isolate_common_corpus("stateful_tests::optional_reserved_prefix_dates_original_phases") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_reserved_prefix_dates_phases(false);
}
#[test]
fn optional_reserved_prefix_dates_actual_phases() {
    if isolate_common_corpus("stateful_tests::optional_reserved_prefix_dates_actual_phases") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_reserved_prefix_dates_phases(true);
}
fn run_reserved_prefix_dates_phases(compare: bool) {
    let pairs = [
        [Datum::Null, Datum::new_string("selected")],
        [Datum::Int(0), Datum::Int(7)],
        [Datum::Int(1), Datum::new_string("7")],
        [Datum::new_string("1tail"), Datum::Bytes(vec![255])],
        [Datum::MinNotNull, Datum::Null],
        [Datum::Real(f64::NAN), Datum::new_string("null")],
        [Datum::UInt(u64::MAX), Datum::Int(-1)],
        [
            Datum::new_string("2026-10-21 01:02:03"),
            Datum::new_string("1"),
        ],
    ];
    let now = chrono::DateTime::parse_from_rfc3339("2026-10-21T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    tidb_datatype::with_test_conversion_clock(now, || {
        reserved_prefix_phase_observer(
            compare,
            &N36_DATE_PREFIX_NAMES,
            &pairs,
            "reserved-prefix-dates",
            "RESERVED PREFIX DATES",
            Some((288000, 0xA62BA14E9B59AC03)),
        );
    });
}
const N36_DATE_PREFIX_NAMES: [&str; 5] = [
    "date_add_day",
    "date_sub_month",
    "date_add_second_microsecond",
    "date_sub_hour",
    "date_add_year",
];
#[test]
fn optional_reserved_prefix_dates_original_all_actual_values() {
    if isolate_common_corpus(
        "stateful_tests::optional_reserved_prefix_dates_original_all_actual_values",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_optional_reserved_prefix_dates_all_actual_values(false);
}
#[test]
fn optional_reserved_prefix_dates_actual_all_actual_values() {
    if isolate_common_corpus(
        "stateful_tests::optional_reserved_prefix_dates_actual_all_actual_values",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_optional_reserved_prefix_dates_all_actual_values(true);
}
fn run_optional_reserved_prefix_dates_all_actual_values(compare: bool) {
    let now = chrono::DateTime::parse_from_rfc3339("2026-10-21T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    tidb_datatype::with_test_conversion_clock(now, || {
        let fields = [
            ty(),
            ty().with_unsigned(true),
            string_ty(),
            FieldType::new(FieldTypeCode::Double),
            FieldType::new(FieldTypeCode::NewDecimal)
                .with_flen(65)
                .with_decimal(5),
            FieldType::new(FieldTypeCode::Datetime).with_decimal(3),
            FieldType::new(FieldTypeCode::Duration).with_decimal(3),
            FieldType::new(FieldTypeCode::Json),
        ];
        let mut gaps = vec![];
        let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
        for name in N36_DATE_PREFIX_NAMES {
            for (fi, field) in fields.iter().enumerate() {
                for arity in [if name.starts_with("date_") { 2 } else { 1 }] {
                    for mask in [0, 1, 2, 4, 7] {
                        for (vi, value) in crate::tikv::byte_unary_value_tests::values()
                            .into_iter()
                            .enumerate()
                        {
                            for position in 0..arity {
                                let mut values = [Datum::Int(2), Datum::Int(3)];
                                values[position] = value.clone();
                                for enabled in [false, true] {
                                    for selection in [None, Some(vec![2, 0, 2]), Some(vec![])] {
                                        let make = || {
                                            let args = (0..arity)
                                                .map(|i| {
                                                    if i < 2 {
                                                        parameter(i, field.clone())
                                                    } else {
                                                        literal(
                                                            if i == 2 {
                                                                Datum::Null
                                                            } else {
                                                                Datum::Int(10)
                                                            },
                                                            field.clone(),
                                                        )
                                                    }
                                                })
                                                .collect();
                                            let mut e = call(name, field.clone(), args);
                                            erase_optional_call_declarations(&mut e, mask);
                                            suite(e)
                                        };
                                        let old_owner = make();
                                        let new_owner = make();
                                        let old = PrefixContext::new(0, enabled, values.clone());
                                        let new = PrefixContext::new(0, enabled, values.clone());
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
                                            let mut left = output_for(field);
                                            left.append_null(0);
                                            let expected = session_read_outcome(|| {
                                                old_owner.run(&old, &mut a, &mut left)
                                            });
                                            let label=format!("reserved-prefix-dates-full/{name}/{fi}/{arity}/{mask}/{vi}/{position}/{enabled}/{selection:?}/{reuse}");
                                            if compare {
                                                let mut b = input();
                                                let mut right = output_for(field);
                                                right.append_null(0);
                                                let got = session_read_outcome(|| {
                                                    new_owner.run_with_tikv_batch(
                                                        &new,
                                                        &mut b,
                                                        &mut right,
                                                        &[ty()],
                                                    )
                                                });
                                                if got != expected
                                                    || right.column(0) != left.column(0)
                                                    || new.snapshot() != old.snapshot()
                                                {
                                                    eprintln!("RESERVED PREFIX DATES FULL DIFFERENCE {label} old={expected} new={got} oldcol={:?} newcol={:?} oldeffects={} neweffects={}",left.column(0),right.column(0),old.snapshot(),new.snapshot());
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
                                                let route = new_owner.batch_route_snapshot();
                                                if (route.fallback != 0 || route.refusals != 0)
                                                    && gaps.len() < 20
                                                {
                                                    gaps.push(format!("{label}: {route:?}"));
                                                }
                                            }
                                            for b in format!(
                                                "{label}|{expected}|{:?}|{}",
                                                left.column(0),
                                                old.snapshot()
                                            )
                                            .bytes()
                                            {
                                                hash = (hash ^ u64::from(b))
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
        eprintln!("RESERVED PREFIX DATES FULL ORIGINAL {count}/{hash:016X}");
        assert_eq!((count, hash), (715200, 0xDC98E2D526F3CF37));
        if !gaps.is_empty() {
            eprintln!("RESERVED PREFIX DATES FULL ADMISSION RED {gaps:?}");
        }
        assert!(gaps.is_empty());
    });
}
#[test]
fn optional_reserved_prefix_original_raw_datums() {
    if isolate_common_corpus("stateful_tests::optional_reserved_prefix_original_raw_datums") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_optional_reserved_prefix_raw_datums(false);
}

#[test]
fn optional_reserved_prefix_actual_raw_datums() {
    if isolate_common_corpus("stateful_tests::optional_reserved_prefix_actual_raw_datums") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_optional_reserved_prefix_raw_datums(true);
}

fn run_optional_reserved_prefix_raw_datums(compare: bool) {
    use tidb_query_datatype::codec::data_type::ScalarValueRef;
    use tidb_query_expr::local::{HostArgReply, HostInvocation, HostStart, HostStep};
    let now = chrono::DateTime::parse_from_rfc3339("2026-10-21T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    tidb_datatype::with_test_conversion_clock(now, || {
        let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
        let mut gaps = vec![];
        let fields = [
            ty(),
            ty().with_unsigned(true),
            FieldType::new(FieldTypeCode::Double),
            FieldType::new(FieldTypeCode::NewDecimal)
                .with_flen(65)
                .with_decimal(5),
            string_ty(),
            FieldType::new(FieldTypeCode::Datetime).with_decimal(3),
            FieldType::new(FieldTypeCode::Duration).with_decimal(3),
            FieldType::new(FieldTypeCode::Json),
        ];
        for name in [
            "cast_unknown",
            "cast_unknown_in_union",
            "unknown_in_union",
            "date_add_invalid_unit",
            "date_sub_invalid_unit",
        ]
        .into_iter()
        .chain(N36_EXTENDED_PREFIX_NAMES)
        .chain(N36_DATE_PREFIX_NAMES)
        {
            let arity = if name.starts_with("date_") { 2 } else { 1 };
            for (fi, field) in fields.iter().enumerate() {
                for mask in [0u8, 1, 2, 4, 7] {
                    for (vi, value) in crate::tikv::byte_unary_value_tests::values()
                        .into_iter()
                        .enumerate()
                    {
                        for position in 0..arity {
                            let mut values = [Datum::Int(2), Datum::Int(3)];
                            values[position] = value.clone();
                            let mut function = ScalarFunction::new(
                                CiString::new(name),
                                field.clone(),
                                values[..arity]
                                    .iter()
                                    .map(|v| literal(v.clone(), field.clone()))
                                    .collect(),
                            );
                            if mask & 1 != 0 {
                                function.ret_type = None;
                            }
                            for (i, arg) in function.args.iter_mut().enumerate() {
                                if mask & (2 << i) != 0 {
                                    let Expression::Constant(c) = arg else {
                                        unreachable!()
                                    };
                                    c.ret_type = None;
                                }
                            }
                            function.prepare_in_string_hash_set();
                            let mut input = Chunk::new_with_capacity(&[], 1);
                            input.set_num_virtual_rows(1);
                            let old = C::new(0, false, values.clone());
                            let expected = session_read_outcome(|| {
                                function
                                    .eval(&old, input.get_row(0))
                                    .map(|v| leaf::frame(&v))
                            });
                            let label = format!(
                                "reserved-prefix-raw/{name}/{fi}/mask{mask}/{vi}/{position}"
                            );
                            if compare {
                                let actual = suite(Expression::ScalarFunction(function.clone()));
                                let preparation =
                                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                        PreparedBatch::compile_calculation_profile(
                                            &actual,
                                            0,
                                            &[],
                                            true,
                                            319,
                                            limits(),
                                            ExecutionLimits::default(),
                                            1 << 24,
                                            true,
                                        )
                                    }));
                                let preparation = match preparation {
                                    Ok(result) => result,
                                    Err(payload) => {
                                        eprintln!("RESERVED PREFIX RAW COMPILE PANIC {label}");
                                        std::panic::resume_unwind(payload)
                                    }
                                };
                                match preparation {
                                    Err(error) => {
                                        if gaps.len() < 20 {
                                            gaps.push(format!("{label}:{error:?}"));
                                        }
                                    }
                                    Ok(compiled) => {
                                        let source = &compiled.source;
                                        if source.host_metadata.len() != 1 {
                                            eprintln!(
                                                "RESERVED PREFIX RAW CATALOG {label}: {} hosts",
                                                source.host_metadata.len()
                                            );
                                        }
                                        assert_eq!(source.host_metadata.len(), 1);
                                        let new = C::new(0, false, values.clone());
                                        let store =
                                            RefCell::new(NativeValueStore::new(1 << 24).unwrap());
                                        store.borrow_mut().begin_window().unwrap();
                                        let catalog = source.hosts.as_ref().unwrap().clone();
                                        let mut hosts = NativeBatchHosts::new(
                                            catalog.clone(),
                                            &source.host_metadata,
                                            &new,
                                            1 << 24,
                                        )
                                        .with_native_values(Some(&store))
                                        .with_constant_probes(&source.host_probes)
                                        .with_numeric_parents(
                                            source
                                                .context_policy
                                                .as_deref()
                                                .map_or(&[], |policy| {
                                                    policy.numeric_parents.as_ref()
                                                }),
                                        )
                                        .unwrap_or_else(|error| {
                                            eprintln!(
                                                "RESERVED PREFIX RAW PARENT {label}: {error:?}"
                                            );
                                            panic!("numeric parent rejected")
                                        });
                                        let slot = catalog.slot(0).unwrap();
                                        let signature = catalog.signature(slot).unwrap();
                                        let row = InputRow {
                                            occurrence: 19,
                                            input_row: 0,
                                        };
                                        let mut eval = EvalContext::default();
                                        let got = session_read_outcome(
                                            || -> std::result::Result<String, EvalError> {
                                                let HostStart::Pending { task, mut request } =
                                                    hosts
                                                        .start(
                                                            &mut eval,
                                                            HostInvocation {
                                                                slot,
                                                                row,
                                                                arg_types: &signature.arg_types,
                                                                return_type: &signature.return_type,
                                                            },
                                                        )
                                                        .unwrap()
                                                else {
                                                    panic!(
                                                        "arithmetic must demand actual arguments"
                                                    )
                                                };
                                                for _ in 0..8 {
                                                    assert!(request.index < 2);
                                                    let node = match &source.host_metadata[0] {
                                                        NativeHostMetadata::EagerValues {
                                                            arguments,
                                                            ..
                                                        } => arguments[request.index],
                                                        NativeHostMetadata::DateArithmetic {
                                                            arguments,
                                                            ..
                                                        } => arguments[request.index],
                                                        NativeHostMetadata::ValueCast {
                                                            argument_node,
                                                            ..
                                                        } => {
                                                            assert_eq!(request.index, 0);
                                                            *argument_node
                                                        }
                                                        _ => panic!("original arithmetic metadata"),
                                                    };
                                                    let token = store
                                                        .borrow_mut()
                                                        .insert_clone(
                                                            &values[request.index],
                                                            NativeValueRole::CharInput,
                                                            NativeValueOrigin { node, row },
                                                        )
                                                        .unwrap();
                                                    let fetched = VectorValue::from_scalar(
                                                        &ScalarValue::Bytes(Some(token)),
                                                        1,
                                                    );
                                                    match hosts.resume(
                                                        &mut eval,
                                                        &task,
                                                        HostArgReply {
                                                            index: request.index,
                                                            field_type: &signature.arg_types
                                                                [request.index],
                                                            values: &fetched,
                                                        },
                                                    ) {
                                                        Ok(HostStep::NeedArg(next)) => {
                                                            request = next
                                                        }
                                                        Ok(HostStep::Ready(output)) => {
                                                            let ScalarValueRef::Bytes(Some(token)) =
                                                                output.get_scalar_ref(0)
                                                            else {
                                                                panic!("native arithmetic token")
                                                            };
                                                            let value = store.borrow();
                                                            return Ok(leaf::frame(
                                                                value
                                                                    .get(
                                                                        token,
                                                                        NativeValueRole::Output,
                                                                        NativeValueOrigin {
                                                                            node: 0,
                                                                            row,
                                                                        },
                                                                    )
                                                                    .unwrap(),
                                                            ));
                                                        }
                                                        Ok(HostStep::NeedArgRow { .. }) => panic!(
                                                        "row arithmetic cannot request another row"
                                                    ),
                                                        Err(error) => {
                                                            if matches!(
                                                                error,
                                                                LocalError::ResourceLimit(_)
                                                            ) {
                                                                return Err(EvalError::ExpressionRuntimeFailure(crate::ExpressionRuntimeFailure::from_local_eval(error,None)));
                                                            }
                                                            return Err(hosts
                                                                .take_native_error()
                                                                .unwrap_or_else(|| {
                                                                    panic!(
                                                                "unexpected host failure {error:?}"
                                                            )
                                                                }));
                                                        }
                                                    }
                                                }
                                                panic!("arithmetic demand did not terminate")
                                            },
                                        );
                                        if got != expected || new.snapshot() != old.snapshot() {
                                            eprintln!("RESERVED PREFIX RAW DIFFERENCE {label} old={expected} new={got} oldeffects={} neweffects={}",old.snapshot(),new.snapshot());
                                        }
                                        assert_eq!(got, expected, "{label}");
                                        assert_eq!(new.snapshot(), old.snapshot(), "{label}");
                                    }
                                }
                            }
                            for byte in format!("{label}|{expected}|{}", old.snapshot()).bytes() {
                                hash = (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3);
                            }
                            count += 1;
                        }
                    }
                }
            }
        }
        eprintln!("RESERVED PREFIX RAW FINAL {compare} {count}/{hash:016X}");
        eprintln!("RESERVED PREFIX RAW ORIGINAL {count}/{hash:016X}");
        assert_eq!((count, hash), (184760, 0xD58D2A1C0604554C));
        if !gaps.is_empty() {
            eprintln!("RESERVED PREFIX RAW ADMISSION RED {gaps:?}");
        }
        assert!(gaps.is_empty());
    });
}

#[test]
fn optional_reserved_prefix_host_auth_callbacks_and_reuse() {
    use tidb_query_expr::local::{
        HostArgReply, HostInvocation, HostStart, HostStep, LocalHostServices,
    };
    struct Context<'a> {
        store: &'a RefCell<NativeValueStore>,
        mode: Cell<u8>,
        warnings: Cell<usize>,
    }
    impl Columns for Context<'_> {
        fn get(&self, _: &[String]) -> Option<Datum> {
            panic!("no expression callback")
        }
        fn ready_value_cache(&self) -> Option<&crate::ReadyValueCache> {
            panic!("no Ready worker")
        }
        fn append_warning(&self, _: u16, _: &str) {
            drop(self.store.borrow_mut());
            self.warnings.set(self.warnings.get() + 1);
            if self.mode.get() == 1 {
                self.store.borrow_mut().begin_window().unwrap();
            }
            if self.mode.get() == 2 {
                panic!("prefix warning callback");
            }
        }
        fn time_zone(&self) -> SessionTimeZone {
            drop(self.store.borrow_mut());
            SessionTimeZone::utc()
        }
    }
    for name in ["cast_unknown_in_union", "cast_unsigned_in_union"] {
        for mask in [0, 7] {
            let mut root = call(name, ty(), vec![parameter(0, string_ty())]);
            erase_optional_call_declarations(&mut root, mask);
            let owner = suite(root);
            let compiled = PreparedBatch::compile_calculation_profile(
                &owner,
                0,
                &[],
                true,
                919,
                limits(),
                ExecutionLimits::default(),
                1 << 20,
                true,
            )
            .unwrap();
            let source = &compiled.source;
            let NativeHostMetadata::EagerValues {
                arguments, kernel, ..
            } = &source.host_metadata[0]
            else {
                panic!("original prefix owner")
            };
            assert!(kernel.prefix_phase().is_some());
            let node = arguments[0];
            let store = RefCell::new(NativeValueStore::new(1 << 20).unwrap());
            store.borrow_mut().begin_window().unwrap();
            let row = InputRow {
                occurrence: 7,
                input_row: 0,
            };
            let origin = NativeValueOrigin { node, row };
            let stale = store
                .borrow_mut()
                .insert_clone(&Datum::Int(1), NativeValueRole::CharInput, origin)
                .unwrap();
            store.borrow_mut().begin_window().unwrap();
            let context = Context {
                store: &store,
                mode: Cell::new(0),
                warnings: Cell::new(0),
            };
            let catalog = source.hosts.as_ref().unwrap().clone();
            let slot = catalog.slot(0).unwrap();
            let sig = catalog.signature(slot).unwrap();
            let mut hosts =
                NativeBatchHosts::new(catalog.clone(), &source.host_metadata, &context, 8192)
                    .with_native_values(Some(&store))
                    .with_constant_probes(&source.host_probes);
            let mut eval = EvalContext::default();
            let insert = |value: &Datum, role, node, row| {
                store
                    .borrow_mut()
                    .insert_clone(value, role, NativeValueOrigin { node, row })
                    .unwrap()
            };
            let wrong_role = insert(&Datum::Int(1), NativeValueRole::Output, node, row);
            let wrong_node = insert(&Datum::Int(1), NativeValueRole::CharInput, node + 1, row);
            let wrong_row = insert(
                &Datum::Int(1),
                NativeValueRole::CharInput,
                node,
                InputRow {
                    occurrence: 8,
                    input_row: 0,
                },
            );
            let large = insert(
                &Datum::Bytes(vec![b'X'; 16384]),
                NativeValueRole::CharInput,
                node,
                row,
            );
            for (token, resource) in [
                (stale, false),
                (wrong_role, false),
                (wrong_node, false),
                (wrong_row, false),
                (large, true),
            ] {
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
                    panic!("prefix pending")
                };
                assert_eq!(request.index, 0);
                let value = VectorValue::from_scalar(&ScalarValue::Bytes(Some(token)), 1);
                let error = hosts
                    .resume(
                        &mut eval,
                        &task,
                        HostArgReply {
                            index: 0,
                            field_type: &sig.arg_types[0],
                            values: &value,
                        },
                    )
                    .unwrap_err();
                assert!(
                    if resource {
                        matches!(error, LocalError::ResourceLimit(_))
                    } else {
                        matches!(error, LocalError::BindingContract(_))
                    },
                    "{name}/{mask}: {error:?}"
                );
                assert_eq!(context.warnings.get(), 0);
                hosts.cancel(&task);
                assert!(hosts
                    .resume(
                        &mut eval,
                        &task,
                        HostArgReply {
                            index: 0,
                            field_type: &sig.arg_types[0],
                            values: &value
                        }
                    )
                    .is_err());
            }
            // Cancelled and refused stages preserve the same descriptor/cap.
            let HostStart::Pending { task, .. } = hosts
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
                panic!("prefix pending")
            };
            let token = insert(&Datum::Int(1), NativeValueRole::CharInput, node, row);
            let fetched = VectorValue::from_scalar(&ScalarValue::Bytes(Some(token)), 1);
            let first = hosts
                .resume(
                    &mut eval,
                    &task,
                    HostArgReply {
                        index: 0,
                        field_type: &sig.arg_types[0],
                        values: &fetched,
                    },
                )
                .unwrap();
            if name == "cast_unknown_in_union" {
                let HostStep::NeedArg(request) = first else {
                    panic!("original live second demand")
                };
                assert_eq!(request.index, 0);
                assert!(matches!(
                    request.mode,
                    tidb_query_expr::local::ArgMode::Fresh
                ));
                let token = insert(&Datum::Null, NativeValueRole::CharInput, node, row);
                let value = VectorValue::from_scalar(&ScalarValue::Bytes(Some(token)), 1);
                assert!(matches!(
                    hosts
                        .resume(
                            &mut eval,
                            &task,
                            HostArgReply {
                                index: 0,
                                field_type: &sig.arg_types[0],
                                values: &value
                            }
                        )
                        .unwrap(),
                    HostStep::Ready(_)
                ));
            } else {
                assert!(matches!(first, HostStep::Ready(_)));
            }
            assert!(hosts
                .resume(
                    &mut eval,
                    &task,
                    HostArgReply {
                        index: 0,
                        field_type: &sig.arg_types[0],
                        values: &fetched
                    }
                )
                .is_err());
            hosts.cancel(&task);
            if name == "cast_unsigned_in_union" {
                // Warning callbacks may mutate the arena or unwind. No store
                // Ref or previous work scope may survive either boundary.
                for mode in [1, 2, 0] {
                    context.mode.set(mode);
                    let token = insert(
                        &Datum::Real(f64::MAX),
                        NativeValueRole::CharInput,
                        node,
                        row,
                    );
                    let fetched = VectorValue::from_scalar(&ScalarValue::Bytes(Some(token)), 1);
                    let HostStart::Pending { task, .. } = hosts
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
                        panic!("prefix pending")
                    };
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        hosts.resume(
                            &mut eval,
                            &task,
                            HostArgReply {
                                index: 0,
                                field_type: &sig.arg_types[0],
                                values: &fetched,
                            },
                        )
                    }));
                    match mode {
                        1 => assert!(matches!(result, Ok(Err(LocalError::BindingContract(_))))),
                        2 => assert!(result.is_err()),
                        _ => assert!(matches!(result, Ok(Ok(HostStep::Ready(_))))),
                    }
                    hosts.cancel(&task);
                    assert!(tidb_query_expr::local::value_kernel_work_remaining().is_none());
                }
                assert_eq!(context.warnings.get(), 3);
            }
        }
    }
}

#[test]
fn optional_reserved_prefix_resources_long_reuse_and_flags() {
    if isolate_common_corpus(
        "stateful_tests::optional_reserved_prefix_resources_long_reuse_and_flags",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    for name in [
        "cast_unknown",
        "cast_unknown_in_union",
        "unknown_in_union",
        "date_add_invalid_unit",
        "date_add_day",
    ] {
        let arity = if name.starts_with("date_") { 2 } else { 1 };
        let make = |mask| {
            let mut root = call(
                name,
                string_ty(),
                (0..arity).map(|i| parameter(i, string_ty())).collect(),
            );
            erase_optional_call_declarations(&mut root, mask);
            suite(root)
        };
        let owner = make(7);
        let mut prepared = PreparedBatchSuite::compile(
            &owner,
            &[ty()],
            true,
            919,
            limits(),
            ExecutionLimits::default(),
            1 << 20,
        )
        .unwrap();
        prepared.workers[0].max_materialization_retained_bytes = 16 * 1024;
        let mut ctx = PrefixContext::new(
            0,
            true,
            [Datum::new_string("1".repeat(32768)), Datum::Int(1)],
        );
        let input = || {
            chunk(
                &[ty()],
                &[
                    vec![Datum::Int(0)],
                    vec![Datum::Int(1)],
                    vec![Datum::Int(2)],
                ],
            )
        };
        let mut first = input();
        let mut output = output_for(&string_ty());
        output.append_null(0);
        let error = prepared
            .run(
                &owner,
                &ctx,
                &mut EvalContext::default(),
                &mut first,
                &mut output,
                &[ty()],
            )
            .unwrap_err();
        assert!(
            matches!(error.local_error(), Some(LocalError::ResourceLimit(_))),
            "{name}: {error:?}"
        );
        assert_eq!(output.num_rows(), 1);
        ctx.base.values[0] = Datum::Null;
        ctx.base.enabled = false;
        ctx.base.events.borrow_mut().clear();
        for n in &ctx.base.reads {
            n.set(0);
        }
        let old = PrefixContext::new(0, false, [Datum::Null, Datum::Int(1)]);
        let mut a = input();
        let mut left = output_for(&string_ty());
        left.append_null(0);
        let expected = session_read_outcome(|| owner.run(&old, &mut a, &mut left));
        let mut b = input();
        let mut right = output_for(&string_ty());
        right.append_null(0);
        let got = session_read_outcome(|| {
            prepared
                .run(
                    &owner,
                    &ctx,
                    &mut EvalContext::default(),
                    &mut b,
                    &mut right,
                    &[ty()],
                )
                .map_err(BatchFailure::into_evaluator_error)
        });
        assert_eq!(got, expected, "{name}");
        assert_eq!(right.column(0), left.column(0), "{name}");
        assert_eq!(ctx.snapshot(), old.snapshot(), "{name}");
        assert_eq!(
            prepared.workers[0].max_materialization_retained_bytes,
            16 * 1024
        );
        for mask in [0, 1, 2, 4, 7] {
            for rows in [0usize, 3, 2051] {
                let original = make(mask);
                let actual = make(mask);
                let mut old = PrefixContext::new(5, true, [Datum::Null, Datum::Null]);
                let mut new = PrefixContext::new(5, true, [Datum::Null, Datum::Null]);
                for (round, enabled) in [true, false, true, false].into_iter().enumerate() {
                    old.base.enabled = enabled;
                    new.base.enabled = enabled;
                    old.base.profile = if round == 0 { 5 } else { 0 };
                    new.base.profile = old.base.profile;
                    let input = || {
                        let mut c = input();
                        c.set_sel(Some((0..rows).map(|i| [2, 0, 2][i % 3]).collect()));
                        c
                    };
                    let mut a = input();
                    let mut left = output_for(&string_ty());
                    left.append_null(0);
                    let expected = session_read_outcome(|| original.run(&old, &mut a, &mut left));
                    let mut b = input();
                    let mut right = output_for(&string_ty());
                    right.append_null(0);
                    let got = session_read_outcome(|| {
                        actual.run_with_tikv_batch(&new, &mut b, &mut right, &[ty()])
                    });
                    let label = format!("{name}/{mask}/{rows}/{round}");
                    assert_eq!(got, expected, "{label}");
                    assert_eq!(right.column(0), left.column(0), "{label}");
                    assert_eq!(new.snapshot(), old.snapshot(), "{label}");
                    let route = actual.batch_route_snapshot();
                    assert_eq!(
                        (route.fallback, route.refusals),
                        (0, 0),
                        "{label}: {route:?}"
                    );
                }
            }
        }
    }
    eprintln!("RESERVED PREFIX LONG REUSE 300; SAME CAP RECOVERY 5");
}

#[test]
fn optional_reserved_prefix_date_typed_claims_require_genuine_fields() {
    use tidb_query_expr::local::{
        HostInvocation, HostStart, LocalHostServices, SqlTemporalOutput as Output,
    };
    let context = C::new(0, false, [Datum::Null, Datum::Null]);
    for output in [
        Output::Bytes,
        Output::DateTime,
        Output::Duration,
        Output::Value,
    ] {
        let result = match output {
            Output::DateTime => FieldType::new(FieldTypeCode::Datetime).with_decimal(3),
            Output::Duration => FieldType::new(FieldTypeCode::Duration).with_decimal(6),
            _ => string_ty(),
        };
        let field = projected(&native_bytes_type(), true).unwrap();
        let args = vec![field.clone(); 2];
        let ret = if output == Output::Value {
            field.clone()
        } else {
            projected(&result, true).unwrap()
        };
        let catalog = HostCatalog::for_sql_batch(vec![(
            SqlBatchHostOperation::DateArithmeticNative { output },
            HostSignature {
                arg_types: args.clone().into_boxed_slice(),
                return_type: ret.clone(),
            },
        )])
        .unwrap();
        let slot = catalog.slot(0).unwrap();
        for mask in [0u8, 1, 2, 4, 7] {
            let metadata = [NativeHostMetadata::DateArithmetic {
                arguments: [1, 2],
                argument_types: [
                    (mask & 2 == 0).then(string_ty),
                    (mask & 4 == 0).then(string_ty),
                ],
                unit: "DAY".into(),
                sign: 1,
                result_fsp: None,
                result_type: (mask & 1 == 0).then(|| result.clone()),
                output_node: 0,
                output_role: (output == Output::Value).then_some(NativeValueRole::Output),
                output,
            }];
            let store = RefCell::new(NativeValueStore::new(1 << 20).unwrap());
            store.borrow_mut().begin_window().unwrap();
            let mut hosts = NativeBatchHosts::new(catalog.clone(), &metadata, &context, 1 << 20)
                .with_native_values(Some(&store));
            let start = hosts.start(
                &mut EvalContext::default(),
                HostInvocation {
                    slot,
                    row: InputRow {
                        occurrence: 0,
                        input_row: 0,
                    },
                    arg_types: &args,
                    return_type: &ret,
                },
            );
            if output == Output::Value || mask == 0 {
                let HostStart::Pending { task, .. } = start.unwrap() else {
                    panic!("date demand")
                };
                hosts.cancel(&task);
            } else {
                assert!(matches!(start, Err(LocalError::BindingContract(_))));
            }
        }
    }
    assert_eq!(context.snapshot(), "0:0|[]");
}

// Capture padding-before-value, numeric NULL signatures, and original context
// timing before admitting WEIGHT_STRING into the existing native frame.
struct WeightContext {
    base: WholeConcatContext,
}
impl WeightContext {
    fn new(enabled: bool, values: [Datum; 3], limit: u64, profile: usize) -> Self {
        Self {
            base: WholeConcatContext::new(enabled, values, limit, profile),
        }
    }
    fn event(&self, s: String) {
        self.base.events.borrow_mut().push(s);
    }
    fn snapshot(&self) -> String {
        format!(
            "{}|workers:{}",
            self.base.snapshot(),
            self.base.ready.prepared_worker_count()
        )
    }
}
impl Columns for WeightContext {
    fn get(&self, _: &[String]) -> Option<Datum> {
        panic!("no expression callback")
    }
    fn ready_value_cache(&self) -> Option<&crate::ReadyValueCache> {
        self.event("ready".into());
        Some(&self.base.ready)
    }
    fn param_value(&self, order: usize) -> std::result::Result<Datum, EvalError> {
        if self.base.profile == 5 && order == 2 {
            self.event("length-panic".into());
            panic!("original weight length callback")
        }
        if self.base.profile == 8 && order == 0 && self.base.reads[0].get() == 2 {
            self.event("value-error".into());
            return Err(EvalError::Unsupported("original weight value callback"));
        }
        self.base.param_value(order)
    }
    fn enable_vectorized_expression(&self) -> bool {
        self.base.enable_vectorized_expression()
    }
    fn max_allowed_packet(&self) -> u64 {
        self.base.max_allowed_packet()
    }
    fn handle_allowed_packet_overflowed(&self, name: &str) -> std::result::Result<(), EvalError> {
        self.base.handle_allowed_packet_overflowed(name)
    }
    fn connection_charset_info(&self) -> (&str, &str) {
        self.event("connection".into());
        if self.base.profile == 9 {
            panic!("original weight connection callback")
        };
        if self.base.profile == 10 {
            ("utf8mb4", "utf8mb4_general_ci")
        } else {
            ("utf8mb4", "utf8mb4_bin")
        }
    }
    fn type_flags(&self) -> tidb_datatype::ConversionFlags {
        self.event("flags".into());
        tidb_datatype::DEFAULT_STATEMENT_FLAGS.with_ignore_truncate_err(true)
    }
    fn truncate_level(&self) -> crate::ErrorLevel {
        self.event("truncate-level".into());
        crate::ErrorLevel::Warn
    }
    fn time_zone(&self) -> SessionTimeZone {
        self.event("zone".into());
        SessionTimeZone::utc()
    }
    fn append_warning(&self, code: u16, message: &str) {
        self.event(format!("warning:{code}:{message}"));
    }
    fn set_uservar(&self, name: &str, value: Datum) {
        self.event(format!("set:{name}:{}", leaf::frame(&value)));
    }
}
#[test]
fn weight_source_original_phases() {
    if isolate_common_corpus("stateful_tests::weight_source_original_phases") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_weight_source(false, false);
}
#[test]
fn weight_source_actual_phases() {
    if isolate_common_corpus("stateful_tests::weight_source_actual_phases") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_weight_source(true, false);
}
#[test]
fn weight_source_original_full() {
    if isolate_common_corpus("stateful_tests::weight_source_original_full") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_weight_source(false, true);
}
#[test]
fn weight_source_actual_full() {
    if isolate_common_corpus("stateful_tests::weight_source_actual_full") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_weight_source(true, true);
}
fn run_weight_source(compare: bool, full: bool) {
    let now = chrono::DateTime::parse_from_rfc3339("2026-10-21T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    tidb_datatype::with_test_conversion_clock(now, || {
        let fields = if full {
            vec![
                ty(),
                ty().with_unsigned(true),
                string_ty(),
                FieldType::new(FieldTypeCode::Double),
                FieldType::new(FieldTypeCode::NewDecimal)
                    .with_flen(65)
                    .with_decimal(5),
                FieldType::new(FieldTypeCode::Datetime).with_decimal(3),
                FieldType::new(FieldTypeCode::Duration).with_decimal(3),
                FieldType::new(FieldTypeCode::Json),
            ]
        } else {
            vec![ty(), string_ty(), FieldType::new(FieldTypeCode::Double)]
        };
        let samples = if full {
            crate::tikv::byte_unary_value_tests::values()
                .into_iter()
                .enumerate()
                .flat_map(|(vi, v)| {
                    (0..3).map(move |pos| {
                        let mut a = [
                            Datum::new_string("aÉ"),
                            Datum::new_string("BINARY"),
                            Datum::Int(2),
                        ];
                        a[pos] = v.clone();
                        (vi * 3 + pos, a)
                    })
                })
                .collect::<Vec<_>>()
        } else {
            vec![
                (0, [Datum::Null, Datum::new_string("CHAR"), Datum::Int(2)]),
                (
                    1,
                    [
                        Datum::new_string("aÉ"),
                        Datum::new_string("BINARY"),
                        Datum::Int(8),
                    ],
                ),
                (
                    2,
                    [
                        Datum::Bytes(vec![255, b'A']),
                        Datum::new_string("unknown"),
                        Datum::Int(-1),
                    ],
                ),
                (
                    3,
                    [Datum::Int(4), Datum::MinNotNull, Datum::new_string("bad")],
                ),
            ]
        };
        let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
        let mut gaps = vec![];
        for (fi, field) in fields.iter().enumerate() {
            for arity in if full { vec![3] } else { vec![1, 2, 3, 4, 6] } {
                for mask in [0, 1, 2, 4, 7] {
                    for shape in if full { vec![0] } else { vec![0, 1, 2, 3, 4] } {
                        for (vi, values) in &samples {
                            for profile in if full {
                                vec![0]
                            } else {
                                vec![0, 1, 3, 4, 5, 8, 9, 10]
                            } {
                                for enabled in [false, true] {
                                    for selection in [None, Some(vec![2, 0, 2]), Some(vec![])] {
                                        let result = native_bytes_type();
                                        let make = || {
                                            let mut args = (0..arity)
                                                .map(|i| {
                                                    if i < 3 {
                                                        parameter(
                                                            i,
                                                            if i == 0 {
                                                                field.clone()
                                                            } else if i == 1 {
                                                                string_ty()
                                                            } else {
                                                                ty()
                                                            },
                                                        )
                                                    } else {
                                                        call(
                                                            "__unused_weight_error__",
                                                            string_ty(),
                                                            vec![],
                                                        )
                                                    }
                                                })
                                                .collect::<Vec<_>>();
                                            match shape {
                                                1 => {
                                                    for (i, arg) in
                                                        args.iter_mut().take(3).enumerate()
                                                    {
                                                        let mut c = Constant::new(
                                                            values[i].clone(),
                                                            arg.static_type().unwrap().clone(),
                                                        );
                                                        c.deferred_expr =
                                                            Some(Box::new(leaf::assignment(
                                                                &format!("weight{i}"),
                                                                arg.clone(),
                                                                &string_ty(),
                                                            )));
                                                        *arg = Expression::Constant(c);
                                                    }
                                                }
                                                2 => {
                                                    let mut c = Column::new(100, field.clone());
                                                    c.index = 99;
                                                    args[0] = Expression::Column(c);
                                                }
                                                3 => args[0] = string_column(0),
                                                4 => {
                                                    for (i, arg) in
                                                        args.iter_mut().take(3).enumerate()
                                                    {
                                                        *arg = literal(
                                                            values[i].clone(),
                                                            arg.static_type().unwrap().clone(),
                                                        );
                                                    }
                                                }
                                                _ => (),
                                            }
                                            let mut e = call("weight_string", result.clone(), args);
                                            erase_optional_call_declarations(&mut e, mask);
                                            suite(e)
                                        };
                                        let old =
                                            WeightContext::new(enabled, values.clone(), 4, profile);
                                        let new =
                                            WeightContext::new(enabled, values.clone(), 4, profile);
                                        let original = make();
                                        let actual = make();
                                        for reuse in 0..2 {
                                            let input = || {
                                                let mut c = chunk(
                                                    &[string_ty()],
                                                    &[
                                                        vec![Datum::new_string("a")],
                                                        vec![Datum::new_string("É")],
                                                        vec![Datum::Null],
                                                    ],
                                                );
                                                c.set_sel(selection.clone());
                                                c
                                            };
                                            let mut a = input();
                                            let mut left = output_for(&result);
                                            left.append_null(0);
                                            let expected = session_read_outcome(|| {
                                                original.run(&old, &mut a, &mut left)
                                            });
                                            let label=format!("weight/{full}/{fi}/{arity}/{mask}/{shape}/{vi}/{profile}/{enabled}/{selection:?}/{reuse}");
                                            if compare {
                                                let mut b = input();
                                                let mut right = output_for(&result);
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
                                                    eprintln!("WEIGHT DIFFERENCE {label} old={expected:?} new={got:?} oldcol={:?} newcol={:?} oldevents={} newevents={}",left.column(0),right.column(0),old.snapshot(),new.snapshot());
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
        eprintln!("WEIGHT ORIGINAL full={full} {count}/{hash:016X}");
        assert_eq!(
            (count, hash),
            if full {
                (214560, 0xAA5EFD1A5E872C25)
            } else {
                (144000, 0xA5235AE1014C7EBB)
            }
        );
        if !gaps.is_empty() {
            eprintln!("WEIGHT SOURCE RED {gaps:?}");
        }
        assert!(gaps.is_empty());
    });
}

#[test]
fn weight_source_host_auth_callbacks_and_recovery() {
    use tidb_query_expr::local::{
        HostArgReply, HostInvocation, HostStart, HostStep, LocalHostServices,
    };
    struct Context<'a> {
        store: &'a RefCell<NativeValueStore>,
        ready: crate::ReadyValueCache,
        mode: Cell<usize>,
        connections: Cell<usize>,
        packets: Cell<usize>,
    }
    impl Columns for Context<'_> {
        fn get(&self, _: &[String]) -> Option<Datum> {
            panic!("no expression callback")
        }
        fn ready_value_cache(&self) -> Option<&crate::ReadyValueCache> {
            drop(self.store.borrow_mut());
            Some(&self.ready)
        }
        fn connection_charset_info(&self) -> (&str, &str) {
            drop(self.store.borrow_mut());
            self.connections.set(self.connections.get() + 1);
            if self.mode.get() == 1 {
                self.store.borrow_mut().begin_window().unwrap();
            }
            if self.mode.get() == 2 {
                panic!("weight connection panic")
            }
            ("utf8mb4", "utf8mb4_bin")
        }
        fn max_allowed_packet(&self) -> u64 {
            drop(self.store.borrow_mut());
            self.packets.set(self.packets.get() + 1);
            if self.mode.get() == 3 {
                self.store.borrow_mut().begin_window().unwrap();
            }
            if self.mode.get() == 4 {
                panic!("weight packet panic")
            }
            1024
        }
    }
    let mut root = call(
        "weight_string",
        native_bytes_type(),
        vec![
            parameter(0, string_ty()),
            parameter(1, string_ty()),
            parameter(2, ty()),
        ],
    );
    erase_optional_call_declarations(&mut root, 7);
    let owner = suite(root);
    let compiled = PreparedBatch::compile_calculation_profile(
        &owner,
        0,
        &[],
        true,
        919,
        limits(),
        ExecutionLimits::default(),
        1 << 20,
        true,
    )
    .unwrap();
    let source = &compiled.source;
    let NativeHostMetadata::WeightString(meta) = &source.host_metadata[0] else {
        panic!("weight owner")
    };
    assert_eq!((meta.logical_arity, meta.arguments.len()), (3, 3));
    let row = InputRow {
        occurrence: 7,
        input_row: 0,
    };
    let catalog = source.hosts.as_ref().unwrap().clone();
    let slot = catalog.slot(0).unwrap();
    let sig = catalog.signature(slot).unwrap();
    for mode in 0..5 {
        let store = RefCell::new(NativeValueStore::new(1 << 20).unwrap());
        store.borrow_mut().begin_window().unwrap();
        let context = Context {
            store: &store,
            ready: crate::ReadyValueCache::new(),
            mode: Cell::new(0),
            connections: Cell::new(0),
            packets: Cell::new(0),
        };
        let insert = |value: &Datum, role, node, row| {
            store
                .borrow_mut()
                .insert_clone(value, role, NativeValueOrigin { node, row })
                .unwrap()
        };
        let mut hosts =
            NativeBatchHosts::new(catalog.clone(), &source.host_metadata, &context, 8192)
                .with_native_values(Some(&store))
                .with_constant_probes(&source.host_probes);
        let mut eval = EvalContext::default();
        let start = |hosts: &mut NativeBatchHosts<'_>, eval: &mut EvalContext| {
            let HostStart::Pending { task, request } = hosts
                .start(
                    eval,
                    HostInvocation {
                        slot,
                        row,
                        arg_types: &sig.arg_types,
                        return_type: &sig.return_type,
                    },
                )
                .unwrap()
            else {
                panic!("weight pending")
            };
            assert_eq!(request.index, 1);
            task
        };
        let feed = |hosts: &mut NativeBatchHosts<'_>,
                    eval: &mut EvalContext,
                    task: &tidb_query_expr::local::HostTaskId,
                    index,
                    token: Option<Vec<u8>>| {
            let value = VectorValue::from_scalar(&ScalarValue::Bytes(token), 1);
            hosts.resume(
                eval,
                task,
                HostArgReply {
                    index,
                    field_type: &sig.arg_types[index],
                    values: &value,
                },
            )
        };
        if mode == 0 {
            for stage in [1, 2, 0] {
                for bad in 0..5 {
                    let node = meta.arguments[stage];
                    let stale = insert(&Datum::Int(1), NativeValueRole::CharInput, node, row);
                    store.borrow_mut().begin_window().unwrap();
                    let task = start(&mut hosts, &mut eval);
                    for index in [1, 2].into_iter().take(if stage == 1 {
                        0
                    } else if stage == 2 {
                        1
                    } else {
                        2
                    }) {
                        let value = if index == 1 {
                            Datum::new_string("BINARY")
                        } else {
                            Datum::Int(2)
                        };
                        let token = insert(
                            &value,
                            NativeValueRole::CharInput,
                            meta.arguments[index],
                            row,
                        );
                        assert!(matches!(
                            feed(&mut hosts, &mut eval, &task, index, Some(token)).unwrap(),
                            HostStep::NeedArg(_)
                        ));
                    }
                    let token = match bad {
                        0 => Some(stale),
                        1 => Some(insert(&Datum::Int(1), NativeValueRole::Output, node, row)),
                        2 => Some(insert(
                            &Datum::Int(1),
                            NativeValueRole::CharInput,
                            node + 999,
                            row,
                        )),
                        3 => Some(insert(
                            &Datum::Int(1),
                            NativeValueRole::CharInput,
                            node,
                            InputRow {
                                occurrence: 8,
                                input_row: 0,
                            },
                        )),
                        _ => None,
                    };
                    assert!(matches!(
                        feed(&mut hosts, &mut eval, &task, stage, token),
                        Err(LocalError::BindingContract(_))
                    ));
                    hosts.cancel(&task);
                    assert_eq!(context.connections.get(), 0);
                    assert_eq!(context.packets.get(), 0);
                }
            }
            // A large padding kind is retained only as a handle until length arrives.
            let task = start(&mut hosts, &mut eval);
            let kind = insert(
                &Datum::Bytes(vec![b'X'; 16384]),
                NativeValueRole::CharInput,
                meta.arguments[1],
                row,
            );
            assert!(matches!(
                feed(&mut hosts, &mut eval, &task, 1, Some(kind)).unwrap(),
                HostStep::NeedArg(_)
            ));
            let length = insert(
                &Datum::Int(2),
                NativeValueRole::CharInput,
                meta.arguments[2],
                row,
            );
            assert!(matches!(
                feed(&mut hosts, &mut eval, &task, 2, Some(length)),
                Err(LocalError::ResourceLimit(_))
            ));
            hosts.cancel(&task);
        }
        context.mode.set(mode);
        let task = start(&mut hosts, &mut eval);
        for (index, value) in [(1, Datum::new_string("BINARY")), (2, Datum::Int(8))] {
            let token = insert(
                &value,
                NativeValueRole::CharInput,
                meta.arguments[index],
                row,
            );
            assert!(matches!(
                feed(&mut hosts, &mut eval, &task, index, Some(token)).unwrap(),
                HostStep::NeedArg(_)
            ));
        }
        let token = insert(
            &Datum::new_string("a"),
            NativeValueRole::CharInput,
            meta.arguments[0],
            row,
        );
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            feed(&mut hosts, &mut eval, &task, 0, Some(token.clone()))
        }));
        match mode {
            1 | 3 => assert!(matches!(result, Ok(Err(LocalError::BindingContract(_))))),
            2 | 4 => assert!(result.is_err()),
            _ => assert!(matches!(result, Ok(Ok(HostStep::Ready(_))))),
        }
        hosts.cancel(&task);
        assert!(feed(&mut hosts, &mut eval, &task, 0, Some(token)).is_err());
        assert!(tidb_query_expr::local::value_kernel_work_remaining().is_none());
    }
}

#[test]
fn weight_source_resources_long_reuse_and_skipped_bindings() {
    if isolate_common_corpus(
        "stateful_tests::weight_source_resources_long_reuse_and_skipped_bindings",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    for arity in [1, 3, 6] {
        let make = |mask| {
            let mut root = call(
                "weight_string",
                native_bytes_type(),
                (0..arity)
                    .map(|i| {
                        if i < 3 {
                            parameter(
                                i,
                                if i == 0 {
                                    string_ty()
                                } else if i == 1 {
                                    string_ty()
                                } else {
                                    ty()
                                },
                            )
                        } else {
                            call("__unused_weight_error__", string_ty(), vec![])
                        }
                    })
                    .collect(),
            );
            erase_optional_call_declarations(&mut root, mask);
            suite(root)
        };
        let owner = make(7);
        let mut prepared = PreparedBatchSuite::compile(
            &owner,
            &[ty()],
            true,
            919,
            limits(),
            ExecutionLimits::default(),
            1 << 20,
        )
        .unwrap();
        prepared.workers[0].max_materialization_retained_bytes = 16 * 1024;
        let mut ctx = WeightContext::new(
            true,
            [
                Datum::new_string("X".repeat(32768)),
                Datum::new_string("BINARY"),
                Datum::Int(2),
            ],
            1024,
            0,
        );
        let input = || {
            chunk(
                &[ty()],
                &[
                    vec![Datum::Int(0)],
                    vec![Datum::Int(1)],
                    vec![Datum::Int(2)],
                ],
            )
        };
        let mut first = input();
        let mut output = output_for(&native_bytes_type());
        output.append_null(0);
        let error = prepared
            .run(
                &owner,
                &ctx,
                &mut EvalContext::default(),
                &mut first,
                &mut output,
                &[ty()],
            )
            .unwrap_err();
        assert!(matches!(
            error.local_error(),
            Some(LocalError::ResourceLimit(_))
        ));
        assert_eq!(output.num_rows(), 1);
        ctx.base.values[0] = Datum::Null;
        ctx.base.enabled.set(false);
        ctx.base.events.borrow_mut().clear();
        for n in &ctx.base.reads {
            n.set(0);
        }
        ctx.base.packet_reads.set(0);
        let old = WeightContext::new(false, ctx.base.values.clone(), 1024, 0);
        let mut a = input();
        let mut left = output_for(&native_bytes_type());
        left.append_null(0);
        let expected = session_read_outcome(|| owner.run(&old, &mut a, &mut left));
        let mut b = input();
        let mut right = output_for(&native_bytes_type());
        right.append_null(0);
        let got = session_read_outcome(|| {
            prepared
                .run(
                    &owner,
                    &ctx,
                    &mut EvalContext::default(),
                    &mut b,
                    &mut right,
                    &[ty()],
                )
                .map_err(BatchFailure::into_evaluator_error)
        });
        assert_eq!(got, expected);
        assert_eq!(right.column(0), left.column(0));
        assert_eq!(ctx.snapshot(), old.snapshot());
        assert_eq!(
            prepared.workers[0].max_materialization_retained_bytes,
            16 * 1024
        );
        for mask in [0, 1, 2, 4, 7] {
            for rows in [0usize, 3, 2051] {
                let original = make(mask);
                let actual = make(mask);
                let mut old = WeightContext::new(
                    true,
                    [
                        Datum::new_string("É"),
                        Datum::new_string("BINARY"),
                        Datum::Int(8),
                    ],
                    4,
                    8,
                );
                let mut new = WeightContext::new(true, old.base.values.clone(), 4, 8);
                for (round, enabled) in [true, false, true, false].into_iter().enumerate() {
                    old.base.enabled.set(enabled);
                    new.base.enabled.set(enabled);
                    old.base.profile = if round == 0 { 8 } else { 0 };
                    new.base.profile = old.base.profile;
                    let input = || {
                        let mut c = input();
                        c.set_sel(Some((0..rows).map(|i| [2, 0, 2][i % 3]).collect()));
                        c
                    };
                    let mut a = input();
                    let mut left = output_for(&native_bytes_type());
                    left.append_null(0);
                    let expected = session_read_outcome(|| original.run(&old, &mut a, &mut left));
                    let mut b = input();
                    let mut right = output_for(&native_bytes_type());
                    right.append_null(0);
                    let got = session_read_outcome(|| {
                        actual.run_with_tikv_batch(&new, &mut b, &mut right, &[ty()])
                    });
                    assert_eq!(got, expected, "{arity}/{mask}/{rows}/{round}");
                    assert_eq!(right.column(0), left.column(0));
                    assert_eq!(new.snapshot(), old.snapshot());
                    let route = actual.batch_route_snapshot();
                    assert_eq!((route.fallback, route.refusals), (0, 0));
                }
            }
        }
    }
    // Truly unused source subtrees need no value ABI or row binding. They retain
    // their real shape and logical arity; no replacement expression is created.
    for arity in [1, 2, 4, 6] {
        let mut c = Column::new(1, ty());
        c.index = 99;
        let mut args = vec![Expression::Column(c)];
        for _ in 1..arity {
            let mut c = Column::new(2, ty());
            c.index = 99;
            args.push(Expression::Column(c));
        }
        let owner = suite(call("weight_string", native_bytes_type(), args));
        let old = WeightContext::new(true, [Datum::Null, Datum::Null, Datum::Null], 4, 0);
        let new = WeightContext::new(true, [Datum::Null, Datum::Null, Datum::Null], 4, 0);
        let input = || chunk(&[ty()], &[vec![Datum::Int(0)]]);
        let mut a = input();
        let mut left = output_for(&native_bytes_type());
        owner.run(&old, &mut a, &mut left).unwrap();
        let mut b = input();
        let mut right = output_for(&native_bytes_type());
        owner
            .run_with_tikv_batch(&new, &mut b, &mut right, &[ty()])
            .unwrap();
        assert_eq!(right.column(0), left.column(0));
        assert_eq!(new.snapshot(), old.snapshot());
        let route = owner.batch_route_snapshot();
        assert_eq!((route.fallback, route.refusals), (0, 0));
    }
}

// Declared string batch entries still evaluate these functions row by row,
// including lazy argument selection, and publish only after collection succeeds.
struct StringRowContext {
    base: WholeConcatContext,
}
impl StringRowContext {
    fn new(enabled: bool, values: [Datum; 3], profile: usize) -> Self {
        Self {
            base: WholeConcatContext::new(enabled, values, u64::MAX, profile),
        }
    }
    fn event(&self, s: String) {
        self.base.events.borrow_mut().push(s);
    }
    fn snapshot(&self) -> String {
        format!(
            "{}|workers:{}",
            self.base.snapshot(),
            self.base.ready.prepared_worker_count()
        )
    }
}
impl Columns for StringRowContext {
    fn get(&self, _: &[String]) -> Option<Datum> {
        panic!("no expression callback")
    }
    fn ready_value_cache(&self) -> Option<&crate::ReadyValueCache> {
        Some(&self.base.ready)
    }
    fn enable_vectorized_expression(&self) -> bool {
        self.base.enable_vectorized_expression()
    }
    fn param_value(&self, order: usize) -> std::result::Result<Datum, EvalError> {
        let value = self.base.param_value(order)?;
        let n = self.base.reads[order].get();
        if self.base.profile == 8 && order == 0 && n == 1026 {
            panic!("original string late value callback")
        }
        if order == 0 && ((self.base.profile == 9 && n == 1) || (self.base.profile == 10 && n > 1))
        {
            Ok(Datum::Null)
        } else {
            Ok(value)
        }
    }
    fn append_warning(&self, code: u16, message: &str) {
        self.event(format!("warning:{code}:{message}"));
    }
    fn type_flags(&self) -> tidb_datatype::ConversionFlags {
        self.event("flags".into());
        tidb_datatype::DEFAULT_STATEMENT_FLAGS.with_ignore_truncate_err(true)
    }
    fn truncate_level(&self) -> crate::ErrorLevel {
        self.event("truncate-level".into());
        crate::ErrorLevel::Warn
    }
    fn time_zone(&self) -> SessionTimeZone {
        self.event("zone".into());
        SessionTimeZone::utc()
    }
}
fn string_row_arities(name: &str) -> Vec<usize> {
    match name {
        "coalesce" => vec![1, 2, 3, 4, 6, 9],
        "ifnull" => vec![2],
        _ => vec![1],
    }
}
#[test]
fn string_row_batch_original_phases() {
    if isolate_common_corpus("stateful_tests::string_row_batch_original_phases") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_string_row_matrix(false, false);
}
#[test]
fn string_row_batch_actual_phases() {
    if isolate_common_corpus("stateful_tests::string_row_batch_actual_phases") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_string_row_matrix(true, false);
}
#[test]
fn string_row_batch_original_full() {
    if isolate_common_corpus("stateful_tests::string_row_batch_original_full") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_string_row_matrix(false, true);
}
#[test]
fn string_row_batch_actual_full() {
    if isolate_common_corpus("stateful_tests::string_row_batch_actual_full") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_string_row_matrix(true, true);
}
fn run_string_row_matrix(compare: bool, full: bool) {
    let now = chrono::DateTime::parse_from_rfc3339("2026-10-21T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    tidb_datatype::with_test_conversion_clock(now, || {
        let values = if full {
            crate::tikv::byte_unary_value_tests::values()
        } else {
            vec![
                Datum::Null,
                Datum::new_string("aÉß"),
                Datum::Int(123),
                Datum::Bytes(vec![255, b'a']),
                Datum::Decimal(tidb_datatype::Decimal::from_literal("2.5")),
                Datum::Real(f64::NAN),
            ]
        };
        let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
        let mut gaps = vec![];
        for name in ["any_value", "lower", "upper", "ifnull", "coalesce"] {
            for arity in string_row_arities(name) {
                for (fi, field) in [
                    string_ty(),
                    string_ty().with_collation(tidb_datatype::Collation::Utf8Mb4GeneralCi),
                    native_bytes_type(),
                ]
                .into_iter()
                .enumerate()
                {
                    for shape in 0..if full || arity == 1 { 1 } else { 3 } {
                        for position in 0..arity {
                            for (vi, value) in values.iter().enumerate() {
                                let mut actual_values = [
                                    Datum::new_string("aÉ"),
                                    Datum::Null,
                                    Datum::new_string("selected"),
                                ];
                                actual_values[if full { 0 } else { position % 3 }] = value.clone();
                                for profile in if full {
                                    vec![0]
                                } else {
                                    vec![0, 1, 2, 6, 8, 9, 10]
                                } {
                                    for enabled in [false, true] {
                                        for selection in [None, Some(vec![2, 0, 2]), Some(vec![])] {
                                            let make = || {
                                                let args = (0..arity)
                                                    .map(|i| {
                                                        if full {
                                                            if i == position {
                                                                parameter(0, field.clone())
                                                            } else {
                                                                literal(Datum::Null, field.clone())
                                                            }
                                                        } else {
                                                            match shape {
                                                                1 => parameter(0, field.clone()),
                                                                2 => match i {
                                                                    0 => {
                                                                        parameter(0, field.clone())
                                                                    }
                                                                    1 => typed_column(0, &field),
                                                                    _ => literal(
                                                                        actual_values[i % 3]
                                                                            .clone(),
                                                                        field.clone(),
                                                                    ),
                                                                },
                                                                _ => {
                                                                    parameter(i % 3, field.clone())
                                                                }
                                                            }
                                                        }
                                                    })
                                                    .collect();
                                                suite(call(name, field.clone(), args))
                                            };
                                            let original = make();
                                            let actual = make();
                                            let old = StringRowContext::new(
                                                enabled,
                                                actual_values.clone(),
                                                profile,
                                            );
                                            let new = StringRowContext::new(
                                                enabled,
                                                actual_values.clone(),
                                                profile,
                                            );
                                            for reuse in 0..2 {
                                                let input = || {
                                                    let mut c = chunk(
                                                        &[field.clone()],
                                                        &[
                                                            vec![Datum::new_string("a")],
                                                            vec![Datum::new_string("É")],
                                                            vec![Datum::Null],
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
                                                let label=format!("string-row/{full}/{name}/{arity}/{fi}/{shape}/{position}/{vi}/{profile}/{enabled}/{selection:?}/{reuse}");
                                                if compare {
                                                    let mut b = input();
                                                    let mut right = output_for(&field);
                                                    right.append_null(0);
                                                    let got = session_read_outcome(|| {
                                                        actual.run_with_tikv_batch(
                                                            &new,
                                                            &mut b,
                                                            &mut right,
                                                            &[field.clone()],
                                                        )
                                                    });
                                                    if got != expected
                                                        || right.column(0) != left.column(0)
                                                        || new.snapshot() != old.snapshot()
                                                    {
                                                        eprintln!("STRING ROW DIFFERENCE {label} old={expected:?} new={got:?} oldcol={:?} newcol={:?} oldevents={} newevents={}",left.column(0),right.column(0),old.snapshot(),new.snapshot());
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
        }
        eprintln!("STRING ROW ORIGINAL full={full} {count}/{hash:016X}");
        assert_eq!(
            (count, hash),
            if full {
                (160920, 0xDCC6C0EF888189A7)
            } else {
                (123984, 0x4E87BDA7A984C4DD)
            }
        );
        if !gaps.is_empty() {
            eprintln!("STRING ROW SOURCE RED {gaps:?}");
        }
        assert!(gaps.is_empty());
    });
}
#[test]
fn string_row_batch_original_long() {
    if isolate_common_corpus("stateful_tests::string_row_batch_original_long") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_string_row_long(false);
}
#[test]
fn string_row_batch_actual_long() {
    if isolate_common_corpus("stateful_tests::string_row_batch_actual_long") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_string_row_long(true);
}
fn run_string_row_long(compare: bool) {
    let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
    for name in ["any_value", "lower", "upper", "ifnull", "coalesce"] {
        for arity in string_row_arities(name) {
            let make = || {
                suite(call(
                    name,
                    string_ty(),
                    (0..arity).map(|i| parameter(i % 3, string_ty())).collect(),
                ))
            };
            let original = make();
            let actual = make();
            for profile in [0, 2, 6, 8, 9, 10] {
                for rows in [0usize, 1, 3, 1026, 2051] {
                    for first in [false, true] {
                        let values = [
                            Datum::new_string("aÉ"),
                            Datum::Null,
                            Datum::new_string("selected"),
                        ];
                        let old = StringRowContext::new(first, values.clone(), profile);
                        let new = StringRowContext::new(first, values, profile);
                        for round in 0..4 {
                            let enabled = if round % 2 == 0 { first } else { !first };
                            old.base.enabled.set(enabled);
                            new.base.enabled.set(enabled);
                            let input = || {
                                let mut c = chunk(
                                    &[string_ty()],
                                    &[
                                        vec![Datum::new_string("a")],
                                        vec![Datum::new_string("b")],
                                        vec![Datum::Null],
                                    ],
                                );
                                c.set_sel(Some((0..rows).map(|i| [2, 0, 2, 1][i % 4]).collect()));
                                c
                            };
                            let mut a = input();
                            let mut left = output_for(&string_ty());
                            left.append_null(0);
                            let expected =
                                session_read_outcome(|| original.run(&old, &mut a, &mut left));
                            let label=format!("string-row-long/{name}/{arity}/{profile}/{rows}/{first}/{round}/{enabled}");
                            if compare {
                                let mut b = input();
                                let mut right = output_for(&string_ty());
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
                                    eprintln!("STRING ROW LONG DIFFERENCE {label} old={expected:?} new={got:?} oldrows={} newrows={} oldevents={} newevents={}",left.num_rows(),right.num_rows(),old.snapshot(),new.snapshot());
                                }
                                assert_eq!(got, expected, "{label}");
                                assert_eq!(right.column(0), left.column(0), "{label}");
                                assert_eq!(new.snapshot(), old.snapshot(), "{label}");
                                let route = actual.batch_route_snapshot();
                                assert_eq!(
                                    (route.fallback, route.refusals),
                                    (0, 0),
                                    "{label}: {route:?}"
                                );
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
    eprintln!("STRING ROW LONG ORIGINAL {count}/{hash:016X}");
    assert_eq!((count, hash), (2400, 0x523058AAC5E4A519));
}

#[test]
fn string_row_batch_same_cap_recovery_and_template_guards() {
    for name in ["any_value", "lower", "upper", "ifnull", "coalesce"] {
        let arity = if name == "ifnull" {
            2
        } else if name == "coalesce" {
            3
        } else {
            1
        };
        let make = || {
            call(
                name,
                string_ty(),
                (0..arity).map(|i| parameter(i, string_ty())).collect(),
            )
        };
        assert!(native_string_row_batch(&make()));
        let mut missing = make();
        erase_optional_call_declarations(&mut missing, 2);
        assert!(!native_string_row_batch(&missing));
        let mut deferred = make();
        let Expression::ScalarFunction(f) = &mut deferred else {
            unreachable!()
        };
        let Expression::Constant(c) = &mut f.args[0] else {
            unreachable!()
        };
        c.deferred_expr = Some(Box::new(parameter(0, string_ty())));
        assert!(!native_string_row_batch(&deferred));
        let literal_only = call(
            name,
            string_ty(),
            (0..arity)
                .map(|_| literal(Datum::new_string("constant"), string_ty()))
                .collect(),
        );
        assert!(!native_string_row_batch(&literal_only));
        let owner = suite(make());
        let mut prepared = PreparedBatchSuite::compile(
            &owner,
            &[ty()],
            true,
            919,
            limits(),
            ExecutionLimits::default(),
            1 << 20,
        )
        .unwrap();
        prepared.workers[0].max_materialization_retained_bytes = 16 * 1024;
        let mut ctx = StringRowContext::new(
            true,
            [
                Datum::new_string("X".repeat(32768)),
                Datum::Null,
                Datum::new_string("selected"),
            ],
            0,
        );
        let input = || {
            chunk(
                &[ty()],
                &[
                    vec![Datum::Int(0)],
                    vec![Datum::Int(1)],
                    vec![Datum::Int(2)],
                ],
            )
        };
        let mut first = input();
        let mut output = output_for(&string_ty());
        output.append_null(0);
        let error = prepared
            .run(
                &owner,
                &ctx,
                &mut EvalContext::default(),
                &mut first,
                &mut output,
                &[ty()],
            )
            .unwrap_err();
        assert!(
            matches!(error.local_error(), Some(LocalError::ResourceLimit(_))),
            "{name}: {error:?}"
        );
        assert_eq!(output.num_rows(), 1);
        ctx.base.values[0] = Datum::Null;
        ctx.base.enabled.set(false);
        ctx.base.events.borrow_mut().clear();
        for n in &ctx.base.reads {
            n.set(0);
        }
        let old = StringRowContext::new(false, ctx.base.values.clone(), 0);
        let mut a = input();
        let mut left = output_for(&string_ty());
        left.append_null(0);
        let expected = session_read_outcome(|| owner.run(&old, &mut a, &mut left));
        let mut b = input();
        let mut right = output_for(&string_ty());
        right.append_null(0);
        let got = session_read_outcome(|| {
            prepared
                .run(
                    &owner,
                    &ctx,
                    &mut EvalContext::default(),
                    &mut b,
                    &mut right,
                    &[ty()],
                )
                .map_err(BatchFailure::into_evaluator_error)
        });
        assert_eq!(got, expected, "{name}");
        assert_eq!(right.column(0), left.column(0), "{name}");
        assert_eq!(ctx.snapshot(), old.snapshot(), "{name}");
        assert_eq!(
            prepared.workers[0].max_materialization_retained_bytes,
            16 * 1024
        );
    }
}

// Observe original SQL/cache demand rather than the number of implementation
// workers: the already admitted typed regexp frame has a direct native kernel.
struct RegexpSourceContext {
    base: WholeConcatContext,
    profile: usize,
    id: Cell<u64>,
    context_reads: Cell<usize>,
}
impl RegexpSourceContext {
    fn new(enabled: bool, values: [Datum; 3], profile: usize) -> Self {
        Self {
            base: WholeConcatContext::new(enabled, values, u64::MAX, 0),
            profile,
            id: Cell::new(17),
            context_reads: Cell::new(0),
        }
    }
    fn event(&self, s: String) {
        self.base.events.borrow_mut().push(s);
    }
    fn snapshot(&self) -> String {
        format!(
            "{}|id:{}:{}",
            self.base.snapshot(),
            self.id.get(),
            self.context_reads.get()
        )
    }
}
impl Columns for RegexpSourceContext {
    fn get(&self, _: &[String]) -> Option<Datum> {
        panic!("no expression callback")
    }
    fn ready_value_cache(&self) -> Option<&crate::ReadyValueCache> {
        self.event("ready".into());
        if self.profile == 7 {
            None
        } else {
            Some(&self.base.ready)
        }
    }
    fn context_id(&self) -> u64 {
        let n = self.context_reads.get();
        self.context_reads.set(n + 1);
        let id = if self.profile == 2 {
            17 + ((n / 2) % 2) as u64
        } else {
            self.id.get()
        };
        self.event(format!("context:{n}:{id}"));
        if self.profile == 6 && n == 1 {
            panic!("original regexp context callback")
        };
        id
    }
    fn enable_vectorized_expression(&self) -> bool {
        self.base.enable_vectorized_expression()
    }
    fn param_value(&self, order: usize) -> std::result::Result<Datum, EvalError> {
        let value = self.base.param_value(order)?;
        let n = self.base.reads[order].get();
        if self.profile == 3 && order == 0 && n == 3 {
            panic!("original regexp value callback")
        }
        if self.profile == 4 && order == 1 && n == 2 {
            return Err(EvalError::Unsupported("original regexp pattern callback"));
        }
        if self.profile == 5 && order == 0 && n == 1026 {
            panic!("original regexp late value callback")
        }
        if self.profile == 1 && order == 1 && n > 1 {
            return Ok(Datum::new_string(if n % 2 == 0 { "[" } else { "^a" }));
        }
        if self.profile == 8 && order == 0 && n == 1 {
            return Ok(Datum::Null);
        }
        Ok(value)
    }
    fn append_warning(&self, code: u16, message: &str) {
        self.event(format!("warning:{code}:{message}"));
    }
    fn type_flags(&self) -> tidb_datatype::ConversionFlags {
        self.event("flags".into());
        tidb_datatype::DEFAULT_STATEMENT_FLAGS.with_ignore_truncate_err(true)
    }
    fn truncate_level(&self) -> crate::ErrorLevel {
        self.event("truncate-level".into());
        crate::ErrorLevel::Warn
    }
    fn time_zone(&self) -> SessionTimeZone {
        self.event("zone".into());
        SessionTimeZone::utc()
    }
    fn connection_charset_info(&self) -> (&str, &str) {
        self.event("connection".into());
        ("utf8mb4", "utf8mb4_bin")
    }
    fn set_uservar(&self, name: &str, value: Datum) {
        self.event(format!("set:{name}:{}", leaf::frame(&value)));
    }
}
fn regexp_erase_fields(root: &mut Expression, mask: usize) {
    let Expression::ScalarFunction(f) = root else {
        unreachable!()
    };
    if mask & 1 != 0 {
        f.ret_type = None;
    }
    for (i, arg) in f.args.iter_mut().enumerate() {
        if mask & (2 << i) == 0 {
            continue;
        }
        match arg {
            Expression::Constant(c) => c.ret_type = None,
            Expression::Column(c) => c.ret_type = None,
            Expression::ScalarFunction(f) => f.ret_type = None,
            Expression::CorrelatedColumn(c) => c.column.ret_type = None,
        }
    }
}
#[test]
fn regexp_source_original_phases() {
    if isolate_common_corpus("stateful_tests::regexp_source_original_phases") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_regexp_source_matrix(false, false);
}
#[test]
fn regexp_source_actual_phases() {
    if isolate_common_corpus("stateful_tests::regexp_source_actual_phases") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_regexp_source_matrix(true, false);
}
#[test]
fn regexp_source_original_full() {
    if isolate_common_corpus("stateful_tests::regexp_source_original_full") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_regexp_source_matrix(false, true);
}
#[test]
fn regexp_source_actual_full() {
    if isolate_common_corpus("stateful_tests::regexp_source_actual_full") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_regexp_source_matrix(true, true);
}
fn run_regexp_source_matrix(compare: bool, full: bool) {
    let now = chrono::DateTime::parse_from_rfc3339("2026-10-21T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    tidb_datatype::with_test_conversion_clock(now, || {
        let fields = if full {
            vec![
                ty(),
                ty().with_unsigned(true),
                string_ty(),
                FieldType::new(FieldTypeCode::Double),
                FieldType::new(FieldTypeCode::NewDecimal)
                    .with_flen(65)
                    .with_decimal(5),
                FieldType::new(FieldTypeCode::Datetime).with_decimal(3),
                FieldType::new(FieldTypeCode::Duration).with_decimal(3),
                FieldType::new(FieldTypeCode::Json),
            ]
        } else {
            vec![ty(), string_ty(), FieldType::new(FieldTypeCode::Double)]
        };
        let results = if full {
            vec![ty()]
        } else {
            vec![
                ty(),
                string_ty().with_collation(tidb_datatype::Collation::Utf8Mb4GeneralCi),
                FieldType::new(FieldTypeCode::Json),
            ]
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
                        Datum::Null,
                        Datum::Bytes(vec![255]),
                        Datum::Bytes(vec![255]),
                    ],
                ),
                (
                    1,
                    0,
                    [
                        Datum::Bytes(vec![255]),
                        Datum::Null,
                        Datum::Bytes(vec![255]),
                    ],
                ),
                (
                    2,
                    0,
                    [
                        Datum::new_string("abc"),
                        Datum::new_string("^a"),
                        Datum::new_string(""),
                    ],
                ),
                (
                    3,
                    0,
                    [
                        Datum::new_string("a\nb"),
                        Datum::new_string("^b"),
                        Datum::new_string("m"),
                    ],
                ),
                (
                    4,
                    0,
                    [
                        Datum::new_string("abc"),
                        Datum::new_string("("),
                        Datum::new_string("i"),
                    ],
                ),
                (
                    5,
                    0,
                    [
                        Datum::Int(123),
                        Datum::new_string("1"),
                        Datum::new_string("c"),
                    ],
                ),
                (
                    6,
                    0,
                    [
                        Datum::new_string("ABC"),
                        Datum::new_string("a"),
                        Datum::new_string("z"),
                    ],
                ),
                (7, 0, [Datum::MinNotNull, Datum::Null, Datum::MinNotNull]),
            ]
        };
        let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
        let mut gaps = vec![];
        for (name, arity) in [("regexp", 2usize), ("regexp_like", 2), ("regexp_like", 3)] {
            for (fi, field) in fields.iter().enumerate() {
                for (ri, result) in results.iter().enumerate() {
                    for mask in 0..(1usize << (arity + 1)) {
                        for shape in 0..if full { 1 } else { 5 } {
                            for (vi, position, values) in &samples {
                                if full && *position >= arity {
                                    continue;
                                }
                                for profile in if full {
                                    vec![0]
                                } else {
                                    vec![0, 1, 2, 3, 4, 6, 7, 8]
                                } {
                                    for enabled in [false, true] {
                                        for selection in [None, Some(vec![2, 0, 2]), Some(vec![])] {
                                            let make = || {
                                                let mut args = (0..arity)
                                                    .map(|i| parameter(i, field.clone()))
                                                    .collect::<Vec<_>>();
                                                match shape {
                                                    1 => {
                                                        for (i, arg) in args.iter_mut().enumerate()
                                                        {
                                                            let mut c = Constant::new(
                                                                values[i].clone(),
                                                                field.clone(),
                                                            );
                                                            c.deferred_expr =
                                                                Some(Box::new(leaf::assignment(
                                                                    &format!("regexp{i}"),
                                                                    arg.clone(),
                                                                    &string_ty(),
                                                                )));
                                                            *arg = Expression::Constant(c);
                                                        }
                                                    }
                                                    2 => {
                                                        args[1] = call(
                                                            "__regexp_late_source_error__",
                                                            string_ty(),
                                                            vec![],
                                                        )
                                                    }
                                                    3 => args[1] = string_column(0),
                                                    4 => {
                                                        for (i, arg) in args.iter_mut().enumerate()
                                                        {
                                                            *arg = literal(
                                                                values[i].clone(),
                                                                field.clone(),
                                                            );
                                                        }
                                                    }
                                                    _ => (),
                                                }
                                                let mut root = call(name, result.clone(), args);
                                                regexp_erase_fields(&mut root, mask);
                                                suite(root)
                                            };
                                            let original = make();
                                            let actual = make();
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
                                                let input = || {
                                                    let mut c = chunk(
                                                        &[string_ty()],
                                                        &[
                                                            vec![Datum::new_string("^a")],
                                                            vec![Datum::new_string("(")],
                                                            vec![Datum::Null],
                                                        ],
                                                    );
                                                    c.set_sel(selection.clone());
                                                    c
                                                };
                                                let mut a = input();
                                                let mut left = output_for(result);
                                                left.append_null(0);
                                                let expected = session_read_outcome(|| {
                                                    original.run(&old, &mut a, &mut left)
                                                });
                                                let label=format!("regexp-source/{full}/{name}/{arity}/{fi}/{ri}/{mask}/{shape}/{vi}/{profile}/{enabled}/{selection:?}/{reuse}");
                                                if compare {
                                                    let mut b = input();
                                                    let mut right = output_for(result);
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
                                                        eprintln!("REGEXP SOURCE DIFFERENCE {label} old={expected:?} new={got:?} oldcol={:?} newcol={:?} oldevents={} newevents={}",left.column(0),right.column(0),old.snapshot(),new.snapshot());
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
        }
        eprintln!("REGEXP SOURCE ORIGINAL full={full} {count}/{hash:016X}");
        assert_eq!(
            (count, hash),
            if full {
                (1144320, 0x19332738CD338935)
            } else {
                (1105920, 0x443B33688A2FD934)
            }
        );
        if !gaps.is_empty() {
            eprintln!("REGEXP SOURCE RED {gaps:?}");
        }
        assert!(gaps.is_empty());
    });
}
#[test]
fn regexp_source_original_long() {
    if isolate_common_corpus("stateful_tests::regexp_source_original_long") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_regexp_source_long(false);
}
#[test]
fn regexp_source_actual_long() {
    if isolate_common_corpus("stateful_tests::regexp_source_actual_long") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_regexp_source_long(true);
}
fn run_regexp_source_long(compare: bool) {
    let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
    for (name, arity) in [("regexp", 2usize), ("regexp_like", 2), ("regexp_like", 3)] {
        let make = || {
            suite(call(
                name,
                ty(),
                (0..arity).map(|i| parameter(i, string_ty())).collect(),
            ))
        };
        let original = make();
        let actual = make();
        for profile in 0..8 {
            for rows in [0usize, 1, 3, 1026, 2051] {
                for first in [false, true] {
                    let values = [
                        Datum::new_string("abc"),
                        Datum::new_string("^a"),
                        Datum::new_string(""),
                    ];
                    let mut old = RegexpSourceContext::new(first, values.clone(), profile);
                    let mut new = RegexpSourceContext::new(first, values, profile);
                    for round in 0..4 {
                        let enabled = if round % 2 == 0 { first } else { !first };
                        old.base.enabled.set(enabled);
                        new.base.enabled.set(enabled);
                        old.id.set(if round == 2 { 18 } else { 17 });
                        new.id.set(old.id.get());
                        old.base.values[1] = Datum::new_string(if round == 1 { "[" } else { "^a" });
                        new.base.values[1] = old.base.values[1].clone();
                        let input = || {
                            let mut c = chunk(
                                &[ty()],
                                &[
                                    vec![Datum::Int(0)],
                                    vec![Datum::Int(1)],
                                    vec![Datum::Int(2)],
                                ],
                            );
                            c.set_sel(Some((0..rows).map(|i| [2, 0, 2, 1][i % 4]).collect()));
                            c
                        };
                        let mut a = input();
                        let mut left = output_for(&ty());
                        left.append_null(0);
                        let expected =
                            session_read_outcome(|| original.run(&old, &mut a, &mut left));
                        let label=format!("regexp-source-long/{name}/{arity}/{profile}/{rows}/{first}/{round}/{enabled}");
                        if compare {
                            let mut b = input();
                            let mut right = output_for(&ty());
                            right.append_null(0);
                            let got = session_read_outcome(|| {
                                actual.run_with_tikv_batch(&new, &mut b, &mut right, &[ty()])
                            });
                            if got != expected
                                || left.column(0) != right.column(0)
                                || old.snapshot() != new.snapshot()
                            {
                                eprintln!("REGEXP SOURCE LONG DIFFERENCE {label} old={expected:?} new={got:?} oldrows={} newrows={} oldevents={} newevents={}",left.num_rows(),right.num_rows(),old.snapshot(),new.snapshot());
                            }
                            assert_eq!(got, expected, "{label}");
                            assert_eq!(right.column(0), left.column(0), "{label}");
                            assert_eq!(new.snapshot(), old.snapshot(), "{label}");
                            let route = actual.batch_route_snapshot();
                            assert_eq!(
                                (route.fallback, route.refusals),
                                (0, 0),
                                "{label}: {route:?}"
                            );
                        }
                        for byte in
                            format!("{label}|{expected}|{:?}|{}", left.column(0), old.snapshot())
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
    eprintln!("REGEXP SOURCE LONG ORIGINAL {count}/{hash:016X}");
    assert_eq!((count, hash), (960, 0xEFE51D028E234646));
}

#[test]
fn regexp_source_host_auth_callbacks_cancel_and_recovery() {
    use tidb_query_expr::local::{
        HostArgReply, HostInvocation, HostStart, HostStep, LocalHostServices,
    };
    struct Context<'a> {
        store: &'a RefCell<NativeValueStore>,
        ready: crate::ReadyValueCache,
        mode: Cell<usize>,
        calls: Cell<usize>,
    }
    impl Columns for Context<'_> {
        fn get(&self, _: &[String]) -> Option<Datum> {
            panic!("no expression callback")
        }
        fn ready_value_cache(&self) -> Option<&crate::ReadyValueCache> {
            drop(self.store.borrow_mut());
            Some(&self.ready)
        }
        fn context_id(&self) -> u64 {
            drop(self.store.borrow_mut());
            self.calls.set(self.calls.get() + 1);
            if self.mode.get() == 1 {
                self.store.borrow_mut().begin_window().unwrap();
            }
            if self.mode.get() == 2 {
                panic!("regexp context panic")
            }
            17
        }
    }
    for (name, arity) in [("regexp", 2), ("regexp_like", 2), ("regexp_like", 3)] {
        let mut root = call(
            name,
            ty(),
            (0..arity).map(|i| parameter(i, string_ty())).collect(),
        );
        regexp_erase_fields(&mut root, (1 << (arity + 1)) - 1);
        let owner = suite(root);
        let compiled = PreparedBatch::compile_calculation_profile(
            &owner,
            0,
            &[],
            true,
            939,
            limits(),
            ExecutionLimits::default(),
            1 << 20,
            true,
        )
        .unwrap();
        let source = &compiled.source;
        let NativeHostMetadata::RegexpValues(meta) = &source.host_metadata[0] else {
            panic!("regexp owner")
        };
        assert_eq!(meta.arguments.len(), arity);
        let row = InputRow {
            occurrence: 7,
            input_row: 0,
        };
        let catalog = source.hosts.as_ref().unwrap().clone();
        let slot = catalog.slot(0).unwrap();
        let sig = catalog.signature(slot).unwrap();
        for mode in 0..3 {
            let store = RefCell::new(NativeValueStore::new(1 << 20).unwrap());
            store.borrow_mut().begin_window().unwrap();
            let context = Context {
                store: &store,
                ready: crate::ReadyValueCache::new(),
                mode: Cell::new(0),
                calls: Cell::new(0),
            };
            let insert = |value: &Datum, role, node, row| {
                store
                    .borrow_mut()
                    .insert_clone(value, role, NativeValueOrigin { node, row })
                    .unwrap()
            };
            let mut hosts =
                NativeBatchHosts::new(catalog.clone(), &source.host_metadata, &context, 8192)
                    .with_native_values(Some(&store))
                    .with_constant_probes(&source.host_probes);
            let mut eval = EvalContext::default();
            let start = |hosts: &mut NativeBatchHosts<'_>, eval: &mut EvalContext| {
                let HostStart::Pending { task, request } = hosts
                    .start(
                        eval,
                        HostInvocation {
                            slot,
                            row,
                            arg_types: &sig.arg_types,
                            return_type: &sig.return_type,
                        },
                    )
                    .unwrap()
                else {
                    panic!("regexp pending")
                };
                assert_eq!(request.index, 0);
                task
            };
            let feed = |hosts: &mut NativeBatchHosts<'_>,
                        eval: &mut EvalContext,
                        task: &tidb_query_expr::local::HostTaskId,
                        index,
                        token: Option<Vec<u8>>| {
                let value = VectorValue::from_scalar(&ScalarValue::Bytes(token), 1);
                hosts.resume(
                    eval,
                    task,
                    HostArgReply {
                        index,
                        field_type: &sig.arg_types[index],
                        values: &value,
                    },
                )
            };
            let ordinary = |index| {
                Datum::new_string(match index {
                    0 => "abc",
                    1 => "^a",
                    _ => "",
                })
            };
            if mode == 0 {
                for stage in 0..arity {
                    for bad in 0..6 {
                        let node = meta.arguments[stage];
                        let stale = insert(&ordinary(stage), NativeValueRole::CharInput, node, row);
                        store.borrow_mut().begin_window().unwrap();
                        let task = start(&mut hosts, &mut eval);
                        for index in 0..stage {
                            let token = insert(
                                &ordinary(index),
                                NativeValueRole::CharInput,
                                meta.arguments[index],
                                row,
                            );
                            let step =
                                feed(&mut hosts, &mut eval, &task, index, Some(token)).unwrap();
                            assert!(
                                matches!(step,HostStep::NeedArg(request) if request.index==index+1)
                            );
                        }
                        let token = match bad {
                            0 => Some(stale),
                            1 => Some(insert(&ordinary(stage), NativeValueRole::Output, node, row)),
                            2 => Some(insert(
                                &ordinary(stage),
                                NativeValueRole::CharInput,
                                node + 999,
                                row,
                            )),
                            3 => Some(insert(
                                &ordinary(stage),
                                NativeValueRole::CharInput,
                                node,
                                InputRow {
                                    occurrence: 8,
                                    input_row: 0,
                                },
                            )),
                            4 => None,
                            _ => Some(insert(
                                &ordinary(stage),
                                NativeValueRole::CharInput,
                                node,
                                row,
                            )),
                        };
                        let index = if bad == 5 { (stage + 1) % arity } else { stage };
                        let denied = feed(&mut hosts, &mut eval, &task, index, token);
                        if bad == 5 {
                            assert!(
                                matches!(denied, Err(LocalError::HostContract(_))),
                                "{name}/{stage}/{bad}"
                            );
                        } else {
                            assert!(
                                matches!(denied, Err(LocalError::BindingContract(_))),
                                "{name}/{stage}/{bad}"
                            );
                        }
                        hosts.cancel(&task);
                        assert_eq!(context.calls.get(), 0);
                    }
                }
                if name == "regexp" {
                    // No formatting or clone of the first raw value before the
                    // second raw NULL gate, even under the same small host cap.
                    let task = start(&mut hosts, &mut eval);
                    let first = insert(
                        &Datum::Bytes(vec![b'x'; 32768]),
                        NativeValueRole::CharInput,
                        meta.arguments[0],
                        row,
                    );
                    assert!(matches!(
                        feed(&mut hosts, &mut eval, &task, 0, Some(first)).unwrap(),
                        HostStep::NeedArg(_)
                    ));
                    let null = insert(
                        &Datum::Null,
                        NativeValueRole::CharInput,
                        meta.arguments[1],
                        row,
                    );
                    assert!(matches!(
                        feed(&mut hosts, &mut eval, &task, 1, Some(null)).unwrap(),
                        HostStep::Ready(_)
                    ));
                    // The retained first capability must be reauthenticated.
                    let task = start(&mut hosts, &mut eval);
                    let first = insert(
                        &ordinary(0),
                        NativeValueRole::CharInput,
                        meta.arguments[0],
                        row,
                    );
                    feed(&mut hosts, &mut eval, &task, 0, Some(first)).unwrap();
                    store.borrow_mut().begin_window().unwrap();
                    let null = insert(
                        &Datum::Null,
                        NativeValueRole::CharInput,
                        meta.arguments[1],
                        row,
                    );
                    assert!(matches!(
                        feed(&mut hosts, &mut eval, &task, 1, Some(null)),
                        Err(LocalError::BindingContract(_))
                    ));
                    hosts.cancel(&task);
                }
                // Cancellation at every suspension is terminal and reusable.
                for stage in 0..arity {
                    let task = start(&mut hosts, &mut eval);
                    for index in 0..stage {
                        let token = insert(
                            &ordinary(index),
                            NativeValueRole::CharInput,
                            meta.arguments[index],
                            row,
                        );
                        feed(&mut hosts, &mut eval, &task, index, Some(token)).unwrap();
                    }
                    hosts.cancel(&task);
                    let token = insert(
                        &ordinary(stage),
                        NativeValueRole::CharInput,
                        meta.arguments[stage],
                        row,
                    );
                    assert!(feed(&mut hosts, &mut eval, &task, stage, Some(token)).is_err());
                }
            }
            context.mode.set(mode);
            let task = start(&mut hosts, &mut eval);
            for index in 0..arity - 1 {
                let token = insert(
                    &ordinary(index),
                    NativeValueRole::CharInput,
                    meta.arguments[index],
                    row,
                );
                feed(&mut hosts, &mut eval, &task, index, Some(token)).unwrap();
            }
            let index = arity - 1;
            let token = insert(
                &ordinary(index),
                NativeValueRole::CharInput,
                meta.arguments[index],
                row,
            );
            // The broker cancels its task while unwinding. Reproduce that
            // ownership contract when calling the host directly here.
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
                feed(guard.hosts, &mut eval, &task, index, Some(token.clone()))
            }));
            match mode {
                1 => assert!(matches!(result, Ok(Err(LocalError::BindingContract(_))))),
                2 => assert!(result.is_err()),
                _ => assert!(matches!(result, Ok(Ok(HostStep::Ready(_))))),
            }
            hosts.cancel(&task);
            assert!(feed(&mut hosts, &mut eval, &task, index, Some(token)).is_err());
            assert!(tidb_query_expr::local::value_kernel_work_remaining().is_none());
            if mode == 2 {
                assert!(context.ready.original_scope_status().is_err());
            } else {
                assert!(context.ready.original_scope_status().is_ok());
            }
        }
    }
}

#[test]
fn regexp_source_same_cap_resource_then_null_recovery() {
    if isolate_common_corpus("stateful_tests::regexp_source_same_cap_resource_then_null_recovery") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    for (name, arity) in [("regexp", 2), ("regexp_like", 2), ("regexp_like", 3)] {
        let mut root = call(
            name,
            ty(),
            (0..arity).map(|i| parameter(i, string_ty())).collect(),
        );
        regexp_erase_fields(&mut root, (1 << (arity + 1)) - 1);
        let owner = suite(root);
        let mut prepared = PreparedBatchSuite::compile(
            &owner,
            &[string_ty()],
            true,
            939,
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
        let input = || {
            chunk(
                &[string_ty()],
                &[
                    vec![Datum::new_string("a")],
                    vec![Datum::new_string("b")],
                    vec![Datum::new_string("c")],
                ],
            )
        };
        let mut first = input();
        let mut output = output_for(&ty());
        output.append_null(0);
        let error = prepared
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
            error.local_error(),
            Some(LocalError::ResourceLimit(_))
        ));
        assert_eq!(output.num_rows(), 1);
        ctx.base.values[0] = Datum::Null;
        ctx.base.enabled.set(false);
        ctx.base.events.borrow_mut().clear();
        for n in &ctx.base.reads {
            n.set(0);
        }
        ctx.context_reads.set(0);
        let old = RegexpSourceContext::new(false, ctx.base.values.clone(), 0);
        let mut a = input();
        let mut left = output_for(&ty());
        left.append_null(0);
        let expected = session_read_outcome(|| owner.run(&old, &mut a, &mut left));
        let mut b = input();
        let mut right = output_for(&ty());
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
        assert_eq!(got, expected);
        assert_eq!(right.column(0), left.column(0));
        assert_eq!(ctx.snapshot(), old.snapshot());
        assert_eq!(
            prepared.workers[0].max_materialization_retained_bytes,
            16 * 1024
        );
    }
}

#[path = "batch_pb_regexp_tests.rs"]
mod pb_regexp_tests;
