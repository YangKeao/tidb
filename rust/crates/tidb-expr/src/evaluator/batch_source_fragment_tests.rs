// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Real native subgraphs under a protocol-only parent, in one SDK program.
use super::*;
use std::cell::Cell;
use tidb_query_expr::local::{ArgMode, HostArgFailure, HostArgRequest, HostCatalogKey};

fn field() -> FieldType {
    FieldType::new(FieldTypeCode::LongLong)
}
fn literal(value: i64) -> Expression {
    Expression::Constant(crate::constant::Constant::new(Datum::Int(value), field()))
}
fn param(order: i64) -> Expression {
    let mut c = crate::constant::Constant::new(Datum::Int(7), field());
    c.param_marker = Some(crate::constant::ParamMarker { order });
    Expression::Constant(c)
}
fn call(name: &str, args: Vec<Expression>) -> Expression {
    Expression::ScalarFunction(ScalarFunction::new(
        tidb_ast::CiString::new(name),
        field(),
        args,
    ))
}
struct Context {
    cache: crate::ReadyValueCache,
    reads: RefCell<Vec<usize>>,
    mode: Cell<u8>,
    failure: Option<EvalError>,
}
impl Columns for Context {
    fn get(&self, _: &[String]) -> Option<Datum> {
        None
    }
    fn ready_value_cache(&self) -> Option<&crate::ReadyValueCache> {
        Some(&self.cache)
    }
    fn param_value(&self, index: usize) -> std::result::Result<Datum, EvalError> {
        self.reads.borrow_mut().push(index);
        match (self.mode.get(), index) {
            (_, 99) => panic!("undemanded real parameter"),
            (1, 0) => Err(EvalError::Unsupported("actual fragment parameter refusal")),
            (2, 0) => panic!("actual fragment parameter panic"),
            (3, 0) => Err(self.failure.as_ref().unwrap().clone()),
            (_, 0) => Ok(Datum::Int(10)),
            (_, 1) => Ok(Datum::Int(30)),
            _ => unreachable!(),
        }
    }
}

struct Route {
    generation: u64,
    target: Option<(usize, HostTaskId)>,
}
struct Services<'a> {
    catalog: HostCatalog,
    schema: Vec<tipb::FieldType>,
    natives: Vec<NativeFragmentBindings<'a>>,
    host_ends: Vec<usize>,
    input_ends: Vec<usize>,
    tasks: Vec<Route>,
    epoch: u64,
    root_live: bool,
    first: Option<VectorValue>,
    caught: Option<EvalError>,
    cancelled: Vec<(usize, HostTaskId)>,
}
impl Services<'_> {
    fn route_start(&mut self, target: usize, start: HostStart) -> HostStart {
        match start {
            HostStart::Ready(v) => HostStart::Ready(v),
            HostStart::Pending { task, request } => {
                self.epoch += 1;
                let slot = self.tasks.len();
                self.tasks.push(Route {
                    generation: self.epoch,
                    target: Some((target, task)),
                });
                HostStart::Pending {
                    task: HostTaskId {
                        slot: slot + 1,
                        generation: self.epoch,
                    },
                    request,
                }
            }
        }
    }
    fn target(&self, task: &HostTaskId) -> LocalResult<(usize, HostTaskId)> {
        let route = task
            .slot
            .checked_sub(1)
            .and_then(|i| self.tasks.get(i))
            .filter(|r| r.generation == task.generation)
            .ok_or_else(|| binding("stale composed task"))?;
        route
            .target
            .ok_or_else(|| binding("finished composed task"))
    }
}
impl LocalRuntimeServices for Services<'_> {
    fn binding_schema(&self) -> &[tipb::FieldType] {
        &self.schema
    }
    fn host_services(&mut self) -> Option<&mut dyn LocalHostServices> {
        Some(self)
    }
    fn read_input(
        &mut self,
        ctx: &mut EvalContext,
        slot: usize,
        row: InputRow,
        expected: &tipb::FieldType,
    ) -> LocalResult<VectorValue> {
        let target = self
            .input_ends
            .iter()
            .position(|end| slot < *end)
            .ok_or_else(|| binding("unknown composed input"))?;
        self.natives[target].read_input(ctx, slot, row, expected)
    }
}
impl LocalHostServices for Services<'_> {
    fn catalog_key(&self) -> &HostCatalogKey {
        self.catalog.key()
    }
    fn start(
        &mut self,
        ctx: &mut EvalContext,
        invocation: HostInvocation<'_>,
    ) -> LocalResult<HostStart> {
        self.catalog.signature(invocation.slot)?;
        if invocation.slot.index() == 0 {
            assert!(!self.root_live);
            self.root_live = true;
            return Ok(HostStart::Pending {
                task: HostTaskId {
                    slot: 0,
                    generation: 1,
                },
                request: HostArgRequest {
                    index: 0,
                    mode: ArgMode::FreshResult,
                },
            });
        }
        let target = self
            .host_ends
            .iter()
            .position(|end| invocation.slot.index() < *end)
            .ok_or_else(|| binding("unknown composed host"))?;
        let start = self.natives[target].start(ctx, invocation)?;
        Ok(self.route_start(target, start))
    }
    fn resume(
        &mut self,
        ctx: &mut EvalContext,
        task: &HostTaskId,
        reply: HostArgReply<'_>,
    ) -> LocalResult<HostStep> {
        if task.slot == 0 {
            assert!(self.root_live);
            if reply.index == 0 {
                self.first = Some(reply.values.clone());
                return Ok(HostStep::NeedArg(HostArgRequest {
                    index: 1,
                    mode: ArgMode::Fresh,
                }));
            }
            self.root_live = false;
            return Ok(HostStep::Ready(reply.values.clone()));
        }
        let (target, local) = self.target(task)?;
        let result = self.natives[target].resume(ctx, &local, reply);
        if matches!(result, Ok(HostStep::Ready(_))) {
            self.tasks[task.slot - 1].target = None;
        }
        result
    }
    fn resume_error(
        &mut self,
        _: &mut EvalContext,
        task: &HostTaskId,
        failure: HostArgFailure,
    ) -> LocalResult<HostStep> {
        assert_eq!(task.slot, 0);
        assert_eq!(failure.index(), 0);
        self.caught = self.natives[0].take_native_error();
        assert!(
            self.caught.is_some(),
            "move the actual native failure, not SDK transport refusal"
        );
        Ok(HostStep::NeedArg(HostArgRequest {
            index: 1,
            mode: ArgMode::Fresh,
        }))
    }
    fn cancel(&mut self, task: &HostTaskId) {
        if task.slot == 0 {
            self.root_live = false;
            return;
        }
        if let Ok((target, local)) = self.target(task) {
            self.natives[target].cancel(&local);
            self.cancelled.push((target, local));
            self.tasks[task.slot - 1].target = None;
        }
    }
}

fn fragments() -> Vec<NativeSourceFragment> {
    let col = Expression::Column(crate::column::Column::new(0, field()));
    vec![
        NativeSourceFragment::prepare(Arc::new(call("plus", vec![param(0), col])), 1).unwrap(),
        NativeSourceFragment::prepare(
            Arc::new(call("if", vec![literal(0), param(99), param(1)])),
            1,
        )
        .unwrap(),
    ]
}

fn combined(
    fragments: &[NativeSourceFragment],
) -> (
    HostCatalog,
    Vec<NativeFragmentMount>,
    Vec<tipb::FieldType>,
    LocalBatchProgram,
) {
    let bytes = fragments[0].source.expr.field_type().clone();
    let mut entries = vec![(
        SqlBatchHostOperation::EagerValuesNative,
        HostSignature {
            arg_types: vec![bytes.clone(); 2].into_boxed_slice(),
            return_type: bytes.clone(),
        },
    )];
    let starts: Vec<_> = fragments
        .iter()
        .map(|f| f.append_hosts(&mut entries).unwrap())
        .collect();
    let catalog = HostCatalog::for_sql_batch(entries).unwrap();
    let mut mounts = Vec::new();
    let mut schema = Vec::new();
    let mut arguments = Vec::new();
    let mut sites = vec![OrdinaryCallSite::sql_native_batch(
        0,
        OrdinarySourceId::new(99, 0),
    )];
    let mut ordinal = 1;
    for (fragment, start) in fragments.iter().zip(starts) {
        let mount = fragment.mount(&catalog, start, schema.len()).unwrap();
        let (graph, mut actual_sites) = fragment.graph(&mount, ordinal).unwrap();
        ordinal += fragment.source.facts.node_count();
        arguments.push(graph);
        sites.append(&mut actual_sites);
        schema.extend_from_slice(fragment.input_schema());
        mounts.push(mount);
    }
    let graph = LocalExpr::HostCall {
        slot: catalog.slot(0).unwrap(),
        args: arguments.into_boxed_slice(),
        return_type: bytes,
    };
    let facts = BatchFacts::sql_native_batch_with_hosts(
        &graph,
        &schema,
        sites,
        CompileLimits::default(),
        &catalog,
    )
    .unwrap();
    let program = compile_batch(&graph, &schema, LocalCompileContext::default(), &facts).unwrap();
    (catalog, mounts, schema, program)
}

fn services<'a>(
    fragments: &'a [NativeSourceFragment],
    mounts: &'a [NativeFragmentMount],
    states: &'a [NativeFragmentState],
    context: &'a Context,
    row: Row<'a>,
    catalog: &HostCatalog,
    schema: &[tipb::FieldType],
) -> Services<'a> {
    let natives = fragments
        .iter()
        .zip(mounts)
        .zip(states)
        .map(|((f, m), s)| f.bind(m, s, context, row).unwrap())
        .collect();
    Services {
        catalog: catalog.clone(),
        schema: schema.to_vec(),
        natives,
        host_ends: mounts.iter().map(|m| m.host_base + m.host_count).collect(),
        input_ends: mounts
            .iter()
            .map(|m| m.input_base + m.input_count)
            .collect(),
        tasks: vec![],
        epoch: 0,
        root_live: false,
        first: None,
        caught: None,
        cancelled: vec![],
    }
}

fn actual_failure() -> EvalError {
    let cache = crate::ReadyValueCache::new();
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        cache.with_columns(&crate::NoColumns, |_| {
            panic!("actual original cache poison")
        });
    }))
    .is_err());
    cache
        .with_columns(&crate::NoColumns, |columns| crate::eval_pi_in(columns))
        .unwrap_err()
}

#[test]
fn native_fragment_composes_real_children_in_one_driver_and_keeps_late_errors() {
    let fragments = fragments();
    let (catalog, mounts, schema, mut program) = combined(&fragments);
    let mut states: Vec<_> = fragments.iter().map(|f| f.new_state().unwrap()).collect();
    let input = tidb_chunk::mutrow::MutRow::from_datums(&[Datum::Int(4)]);
    let context = Context {
        cache: crate::ReadyValueCache::new(),
        reads: RefCell::new(vec![]),
        mode: Cell::new(0),
        failure: Some(actual_failure()),
    };
    let mut previous: Option<Vec<u8>> = None;
    for mode in [0, 1, 3, 0] {
        context.mode.set(mode);
        context.reads.borrow_mut().clear();
        let original_context = Context {
            cache: crate::ReadyValueCache::new(),
            reads: RefCell::new(vec![]),
            mode: Cell::new(mode),
            failure: context.failure.clone(),
        };
        let expected: Vec<_> = (|| {
            fragments
                .iter()
                .map(|f| f.owner.eval(&original_context, input.to_row()))
                .collect()
        })();
        for state in &mut states {
            state.reset().unwrap();
        }
        let natives = fragments
            .iter()
            .zip(&mounts)
            .zip(&states)
            .map(|((f, m), s)| f.bind(m, s, &context, input.to_row()).unwrap())
            .collect();
        let host_ends = mounts
            .iter()
            .zip(&fragments)
            .map(|(m, f)| m.host_base + f.source.host_metadata.len())
            .collect();
        let input_ends = mounts
            .iter()
            .zip(&fragments)
            .map(|(m, f)| m.input_base + f.input_schema().len())
            .collect();
        let mut services = Services {
            catalog: catalog.clone(),
            schema: schema.clone(),
            natives,
            host_ends,
            input_ends,
            tasks: vec![],
            epoch: 0,
            root_live: false,
            first: None,
            caught: None,
            cancelled: vec![],
        };
        let entries_before = crate::test_evaluation::shared_entries();
        let preparations_before = crate::test_evaluation::scalar_preparations();
        let output = program
            .eval_cursor_with_bindings(
                ExecutionLimits::default(),
                &mut EvalContext::default(),
                0,
                &mut services,
            )
            .unwrap();
        assert_eq!(
            crate::test_evaluation::shared_entries(),
            entries_before,
            "composed driver called a public or original subtree evaluator"
        );
        assert_eq!(
            crate::test_evaluation::scalar_preparations(),
            preparations_before,
            "composed invocation prepared a hidden scalar program"
        );
        let row = InputRow {
            occurrence: 0,
            input_row: 0,
        };
        let VectorValue::Bytes(values) = output else {
            panic!("native output token")
        };
        let token = values.get(0).unwrap();
        if let Some(old) = previous.as_deref() {
            assert!(
                services.natives[1].value(old, row).is_err(),
                "old generation refused"
            );
        }
        previous = Some(token.to_vec());
        assert_eq!(
            services.natives[1].value(token, row).unwrap(),
            Datum::Int(30)
        );
        assert!(
            services.natives[0].value(token, row).is_err(),
            "foreign namespace refused"
        );
        assert_eq!(*context.reads.borrow(), *original_context.reads.borrow());
        if mode == 1 || mode == 3 {
            assert_eq!(services.caught, expected[0].as_ref().err().cloned());
            assert!(!services.cancelled.is_empty());
        } else {
            let VectorValue::Bytes(first) = services.first.as_ref().unwrap() else {
                panic!("first token")
            };
            assert_eq!(
                services.natives[0]
                    .value(first.get(0).unwrap(), row)
                    .unwrap(),
                *expected[0].as_ref().unwrap()
            );
            assert!(services.caught.is_none());
        }
        assert!(!services.root_live);
        assert!(services.tasks.iter().all(|t| t.target.is_none()));
    }
}

#[test]
fn native_fragment_rejects_foreign_mount_schema_coordinates_and_stale_values() {
    let fragments = fragments();
    let (catalog, mounts, _, _) = combined(&fragments);
    assert!(fragments[1].graph(&mounts[0], 0).is_err());
    assert!(fragments[0]
        .mount(&catalog, mounts[1].host_base, 0)
        .is_err());
    assert!(fragments[0].graph(&mounts[0], usize::MAX).is_err());
    let state = fragments[0].new_state().unwrap();
    let input = tidb_chunk::mutrow::MutRow::from_datums(&[Datum::Int(4)]);
    assert!(fragments[0]
        .bind(&mounts[0], &state, &crate::NoColumns, Row::empty())
        .is_err());
    assert!(fragments[1]
        .bind(&mounts[1], &state, &crate::NoColumns, input.to_row())
        .is_err());
    let mut bound = fragments[0]
        .bind(&mounts[0], &state, &crate::NoColumns, input.to_row())
        .unwrap();
    let field = fragments[0].input_schema()[0].clone();
    assert!(bound
        .read_input(
            &mut EvalContext::default(),
            mounts[0].input_base,
            InputRow {
                occurrence: 1,
                input_row: 0
            },
            &field
        )
        .is_err());
    assert!(bound
        .read_input(
            &mut EvalContext::default(),
            mounts[0].input_base,
            InputRow {
                occurrence: 0,
                input_row: 1
            },
            &field
        )
        .is_err());
    assert!(bound
        .read_input(
            &mut EvalContext::default(),
            mounts[0].input_base,
            InputRow {
                occurrence: 0,
                input_row: 0
            },
            &tipb::FieldType::default()
        )
        .is_err());
    let mut duplicate_entries = vec![];
    fragments[0].append_hosts(&mut duplicate_entries).unwrap();
    let other_catalog = HostCatalog::for_sql_batch(duplicate_entries).unwrap();
    assert!(mounts[0].owns_host(other_catalog.slot(0).unwrap()).is_err());
    let types = vec![];
    assert!(bound
        .start(
            &mut EvalContext::default(),
            HostInvocation {
                slot: catalog.slot(mounts[0].host_base).unwrap(),
                row: InputRow {
                    occurrence: 0,
                    input_row: 0
                },
                arg_types: &types,
                return_type: &field
            }
        )
        .is_err());
    assert_ne!(other_catalog.key(), catalog.key());
}

#[test]
fn native_fragment_cancellation_and_selected_scope_preserve_panic_poisoning() {
    let fragments = fragments();
    let (catalog, mounts, schema, mut program) = combined(&fragments);
    let states: Vec<_> = fragments.iter().map(|f| f.new_state().unwrap()).collect();
    let input = tidb_chunk::mutrow::MutRow::from_datums(&[Datum::Int(4)]);
    let context = Context {
        cache: crate::ReadyValueCache::new(),
        reads: RefCell::new(vec![]),
        mode: Cell::new(2),
        failure: None,
    };
    let fallback = crate::ReadyValueCache::new();
    let mut services = services(
        &fragments,
        &mounts,
        &states,
        &context,
        input.to_row(),
        &catalog,
        &schema,
    );
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let scope = fallback.select_columns_scope(&context);
        assert!(std::ptr::eq(scope.cache(), &context.cache));
        program.eval_cursor_with_bindings(
            ExecutionLimits::default(),
            &mut EvalContext::default(),
            0,
            &mut services,
        )
    }));
    assert!(failed.is_err());
    assert!(!services.root_live);
    assert!(services.tasks.iter().all(|t| t.target.is_none()));
    assert!(context.cache.original_scope_status().is_err());
    assert!(
        fallback.original_scope_status().is_ok(),
        "unselected lane stays healthy"
    );
    context.mode.set(0);
    context.reads.borrow_mut().clear();
    // Poisoning is a lane state, not an invented blanket refusal. Preserve
    // whether the actual current Shared sources request that capability.
    let expected: Vec<_> = fragments
        .iter()
        .map(|f| {
            context
                .cache
                .with_columns(&context, |columns| f.owner.eval(columns, input.to_row()))
        })
        .collect();
    let expected_reads = context.reads.borrow().clone();
    context.reads.borrow_mut().clear();
    let output = program
        .eval_cursor_with_bindings(
            ExecutionLimits::default(),
            &mut EvalContext::default(),
            0,
            &mut services,
        )
        .unwrap();
    assert!(matches!(output, VectorValue::Bytes(_)));
    match &expected[0] {
        Err(error) => assert_eq!(
            format!("{:?}", services.caught.as_ref().unwrap()),
            format!("{error:?}")
        ),
        Ok(value) => {
            assert!(services.caught.is_none());
            let VectorValue::Bytes(first) = services.first.as_ref().unwrap() else {
                panic!("first token")
            };
            assert_eq!(
                &services.natives[0]
                    .value(
                        first.get(0).unwrap(),
                        InputRow {
                            occurrence: 0,
                            input_row: 0
                        }
                    )
                    .unwrap(),
                value
            );
        }
    }
    assert_eq!(*context.reads.borrow(), expected_reads);
    assert!(services.tasks.iter().all(|t| t.target.is_none()));
    {
        let scope = fallback.select_columns_scope(&crate::NoColumns);
        assert!(std::ptr::eq(scope.cache(), &fallback));
    }
    assert!(
        fallback.original_scope_status().is_ok(),
        "normal scope cancellation is reusable"
    );
}

#[test]
fn native_fragment_affine_binding_releases_completed_child_buffers_and_tokens() {
    let fragment = NativeSourceFragment::prepare(Arc::new(literal(7)), 0).unwrap();
    let mut entries = vec![];
    let base = fragment.append_hosts(&mut entries).unwrap();
    let catalog = HostCatalog::for_sql_batch(entries).unwrap();
    let mount = fragment.mount(&catalog, base, 0).unwrap();
    let state = fragment.new_state().unwrap();
    let mut binding = fragment
        .bind(&mount, &state, &crate::NoColumns, Row::empty())
        .unwrap();
    assert!(fragment
        .bind(&mount, &state, &crate::NoColumns, Row::empty())
        .is_err());
    assert!(state.release_values().is_err());
    let row = InputRow {
        occurrence: 0,
        input_row: 0,
    };
    let output = binding
        .read_input(
            &mut EvalContext::default(),
            0,
            row,
            &fragment.input_schema()[0],
        )
        .unwrap();
    let VectorValue::Bytes(output) = output else {
        panic!("root native token")
    };
    let token = output.get(0).unwrap().to_vec();
    assert_eq!(binding.value(&token, row).unwrap(), Datum::Int(7));
    drop(binding);
    state.release_values().unwrap();
    let binding = fragment
        .bind(&mount, &state, &crate::NoColumns, Row::empty())
        .unwrap();
    assert!(
        binding.value(&token, row).is_err(),
        "completed child token cannot survive owner release"
    );
}

#[test]
fn selected_scope_matches_original_some_none_retired_queries_and_forwarding() {
    struct Capability {
        cache: crate::ReadyValueCache,
        mode: u8,
        events: RefCell<Vec<&'static str>>,
    }
    impl Columns for Capability {
        fn get(&self, _: &[String]) -> Option<Datum> {
            self.events.borrow_mut().push("get");
            Some(Datum::Int(3))
        }
        fn ready_value_cache(&self) -> Option<&crate::ReadyValueCache> {
            self.events.borrow_mut().push("cache");
            match self.mode {
                0 => Some(&self.cache),
                1 => None,
                _ => panic!("actual retired capability"),
            }
        }
        fn append_warning(&self, _: u16, _: &str) {
            self.events.borrow_mut().push("warning");
        }
    }
    for mode in 0..3 {
        let mut traces = vec![];
        for candidate in [false, true] {
            let context = Capability {
                cache: crate::ReadyValueCache::new(),
                mode,
                events: RefCell::new(vec![]),
            };
            let fallback = crate::ReadyValueCache::new();
            let body = |columns: &dyn Columns| {
                assert!(columns.ready_value_cache().is_some());
                assert!(columns.ready_value_cache().is_some());
                assert_eq!(columns.get(&[]), Some(Datum::Int(3)));
                columns.append_warning(7, "actual forwarding");
            };
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                if candidate {
                    let scope = fallback.select_columns_scope(&context);
                    let columns = scope.bind_columns(&context);
                    body(&columns);
                } else {
                    fallback.with_columns(&context, |columns| body(columns));
                }
            }));
            assert_eq!(outcome.is_err(), mode == 2);
            assert!(fallback.original_scope_status().is_ok());
            assert!(context.cache.original_scope_status().is_ok());
            traces.push(context.events.into_inner());
        }
        assert_eq!(traces[0], traces[1]);
        assert_eq!(traces[0].iter().filter(|e| **e == "cache").count(), 1);
    }
}
