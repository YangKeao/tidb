// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Original cached-list demand, including misses on the semantic empty row.
use super::*;
use crate::tikv::dynamic_leaf_tests as leaf;
use std::collections::BTreeMap;

struct ListContext {
    ready: Option<crate::ReadyValueCache>,
    id: u64,
    enabled: bool,
    fault: usize,
    needle: Vec<Datum>,
    list: Datum,
    reads: [Cell<usize>; 2],
    events: RefCell<Vec<String>>,
    vars: RefCell<BTreeMap<String, Datum>>,
}
impl ListContext {
    fn new(id: u64, enabled: bool, fault: usize, needle: Vec<Datum>, list: Datum) -> Self {
        Self {
            ready: None,
            id,
            enabled,
            fault,
            needle,
            list,
            reads: [Cell::new(0), Cell::new(0)],
            events: RefCell::new(vec![]),
            vars: RefCell::new(BTreeMap::new()),
        }
    }
    fn fresh_ready(mut self) -> Self {
        self.ready = Some(crate::ReadyValueCache::new());
        self
    }
    fn event(&self, value: String) {
        self.events.borrow_mut().push(value);
    }
    fn snapshot(&self) -> String {
        format!(
            "{:?}|{}:{}|{:?}",
            self.events.borrow(),
            self.reads[0].get(),
            self.reads[1].get(),
            self.vars
                .borrow()
                .iter()
                .map(|(k, v)| (k.clone(), leaf::frame(v)))
                .collect::<Vec<_>>()
        )
    }
}
impl Columns for ListContext {
    fn ready_value_cache(&self) -> Option<&crate::ReadyValueCache> {
        self.ready.as_ref()
    }
    fn get(&self, _: &[String]) -> Option<Datum> {
        panic!("no expression callback")
    }
    fn param_value(&self, order: usize) -> std::result::Result<Datum, EvalError> {
        let count = self.reads[order].get();
        self.reads[order].set(count + 1);
        self.event(format!("param:{order}:{count}"));
        if order == 1 {
            if self.fault == 1 {
                return Err(EvalError::Unsupported("cached list unavailable"));
            }
            if self.fault == 2 {
                panic!("cached list callback")
            }
            Ok(self.list.clone())
        } else {
            Ok(self.needle[count.min(self.needle.len() - 1)].clone())
        }
    }
    fn context_id(&self) -> u64 {
        self.event(format!("context:{}", self.id));
        self.id
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
    fn div_precision_increment(&self) -> u32 {
        self.event("precision".into());
        4
    }
    fn truncate_level(&self) -> crate::ErrorLevel {
        self.event("truncate-level".into());
        crate::ErrorLevel::Warn
    }
    fn append_warning(&self, code: u16, message: &str) {
        self.event(format!("warning:{code}:{message}"));
    }
    fn get_uservar(&self, name: &str) -> Option<Datum> {
        self.event(format!("get:{name}"));
        self.vars.borrow().get(name).cloned()
    }
    fn set_uservar(&self, name: &str, value: Datum) {
        self.event(format!("set:{name}:{}", leaf::frame(&value)));
        self.vars.borrow_mut().insert(name.into(), value);
    }
}
fn expression(shape: usize, collation: tidb_datatype::Collation) -> Expression {
    let field = string_ty().with_collation(collation);
    let param = |order| {
        let mut value = Constant::new(Datum::new_string("a,b,c"), field.clone());
        value.param_marker = Some(ParamMarker { order });
        Expression::Constant(value)
    };
    let column = || {
        let mut column = Column::new(1, field.clone());
        column.index = 0;
        Expression::Column(column)
    };
    let deferred = |child| {
        let mut value = Constant::new(Datum::new_string("saved"), field.clone());
        value.deferred_expr = Some(Box::new(child));
        Expression::Constant(value)
    };
    let list = match shape {
        0 => literal(Datum::new_string("a,b,c"), field.clone()),
        1 => param(1),
        2 => {
            let mut value = Constant::new(Datum::new_string("saved"), field.clone());
            value.param_marker = Some(ParamMarker { order: 1 });
            value.deferred_expr = Some(Box::new(literal(
                Datum::new_string("ignored"),
                field.clone(),
            )));
            Expression::Constant(value)
        }
        3 => deferred(leaf::assignment("list", param(1), &field)),
        4 => deferred(column()),
        5 => call("lower", field.clone(), vec![param(1)]),
        6 => column(),
        7 => leaf::assignment("list", param(1), &field),
        8 => deferred(call(
            "coalesce",
            field.clone(),
            vec![leaf::assignment("list", param(1), &field), column()],
        )),
        9 => deferred(call("ifnull", field.clone(), vec![param(1), column()])),
        _ => unreachable!(),
    };
    call(
        "find_in_set",
        ty().with_collation(collation),
        vec![param(0), list],
    )
}
fn input(selection: Option<Vec<usize>>) -> Chunk {
    let mut input = Chunk::new_with_capacity(&[string_ty()], 3);
    for value in ["a,b,c", "b,A", "x,,b"] {
        input.append_datum(0, &Datum::new_string(value));
    }
    input.set_sel(selection);
    input
}
fn needles() -> Vec<Vec<Datum>> {
    vec![
        vec![Datum::new_string("b")],
        vec![Datum::Null],
        vec![Datum::Bytes(vec![0xff])],
        vec![Datum::MinNotNull],
        vec![Datum::new_string("b"), Datum::Null, Datum::new_string("A")],
        vec![Datum::new_string("b,c")],
    ]
}
fn lists() -> Vec<Datum> {
    vec![
        Datum::new_string("A,b,c"),
        Datum::new_string(""),
        Datum::Null,
        Datum::MinNotNull,
        Datum::Bytes(vec![0xff, b',', b'b']),
    ]
}
fn epoch(epoch: usize) -> (u64, usize) {
    [
        (11, 0),
        (11, 1),
        (22, 1),
        (11, 2),
        (22, 0),
        (22, 1),
        (33, 2),
        (22, 1),
        (11, 0),
    ][epoch]
}
#[test]
fn cached_list_original_demands_empty_rows_context_switches_and_failed_initializers() {
    if isolate_common_corpus("cached_list_tests::cached_list_original_demands_empty_rows_context_switches_and_failed_initializers") { return }
    std::panic::set_hook(Box::new(|_| {}));
    let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
    for shape in 0..10 {
        for (ci, collation) in [
            tidb_datatype::Collation::Binary,
            tidb_datatype::Collation::Utf8Mb4Bin,
            tidb_datatype::Collation::Utf8Mb4GeneralCi,
        ]
        .into_iter()
        .enumerate()
        {
            for (ni, needle) in needles().into_iter().enumerate() {
                for (li, list) in lists().into_iter().enumerate() {
                    for selection in [None, Some(vec![2, 0, 2]), Some(vec![])] {
                        for enabled in [false, true] {
                            let owner = suite(expression(shape, collation));
                            for round in 0..9 {
                                let (id, fault) = epoch(round);
                                let ctx = ListContext::new(
                                    id,
                                    enabled,
                                    fault,
                                    needle.clone(),
                                    list.clone(),
                                );
                                let mut input = input(selection.clone());
                                let mut output = output_for(&ty());
                                output.append_null(0);
                                let result = session_read_outcome(|| {
                                    owner.run(&ctx, &mut input, &mut output)
                                });
                                let observed=format!("{shape}/{ci}/{ni}/{li}/{selection:?}/{enabled}/{round}|{result:?}|{:?}|{}|{:?}",output.column(0),ctx.snapshot(),input.sel());
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
    }
    eprintln!("CACHED LIST ORIGINAL {count}/{hash:016X}");
    assert_eq!((count, hash), (48600, 0x1BE95C69A586382F));
}
#[test]
fn cached_list_actual_sources_keep_original_cache_and_empty_row_demand() {
    let mut failures = Vec::new();
    for shape in 0..10 {
        let actual = suite(expression(shape, tidb_datatype::Collation::Utf8Mb4Bin));
        let reference = suite(expression(shape, tidb_datatype::Collation::Utf8Mb4Bin));
        for round in 0..9 {
            let (id, fault) = epoch(round);
            let make = || {
                ListContext::new(
                    id,
                    true,
                    fault,
                    vec![Datum::new_string("b")],
                    Datum::new_string("a,b,c"),
                )
            };
            let (ctx, old) = (make(), make().fresh_ready());
            let (mut input, mut oldinput) = (input(None), input(None));
            let (mut output, mut expected) = (output_for(&ty()), output_for(&ty()));
            output.append_null(0);
            expected.append_null(0);
            let want = session_read_outcome(|| reference.run(&old, &mut oldinput, &mut expected));
            let got = session_read_outcome(|| {
                actual.run_with_tikv_batch(&ctx, &mut input, &mut output, &[string_ty()])
            });
            let route = actual.batch_route_snapshot();
            if got != want
                || output.column(0) != expected.column(0)
                || ctx.snapshot() != old.snapshot()
                || (route.refusals, route.fallback) != (0, 0)
            {
                failures.push(format!("shape={shape}/round={round} got={got:?} want={want:?} route={route:?} effects:{} vs{}",ctx.snapshot(),old.snapshot()));
            }
        }
    }
    assert!(
        failures.is_empty(),
        "cached list source gaps:\n{}",
        failures.join("\n")
    );
}

#[test]
fn cached_list_clean_sql_original_keeps_warm_sql_cache_and_records_legacy_history() {
    if isolate_common_corpus("cached_list_tests::cached_list_clean_sql_original_keeps_warm_sql_cache_and_records_legacy_history") { return }
    std::panic::set_hook(Box::new(|_| {}));
    run_cached_list_clean_comparison(false);
}
#[test]
fn cached_list_actual_matrix_matches_clean_sql_with_original_cache_history() {
    if isolate_common_corpus("cached_list_tests::cached_list_actual_matrix_matches_clean_sql_with_original_cache_history") { return }
    std::panic::set_hook(Box::new(|_| {}));
    run_cached_list_clean_comparison(true);
}
fn run_cached_list_clean_comparison(compare: bool) {
    let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
    let mut differences = 0usize;
    let mut historical_hash = 0xcbf29ce484222325u64;
    for shape in 0..10 {
        for (ci, collation) in [
            tidb_datatype::Collation::Binary,
            tidb_datatype::Collation::Utf8Mb4Bin,
            tidb_datatype::Collation::Utf8Mb4GeneralCi,
        ]
        .into_iter()
        .enumerate()
        {
            for (ni, needle) in needles().into_iter().enumerate() {
                for (li, list) in lists().into_iter().enumerate() {
                    for selection in [None, Some(vec![2, 0, 2]), Some(vec![])] {
                        for enabled in [false, true] {
                            let owner = suite(expression(shape, collation));
                            let actual = compare.then(|| suite(expression(shape, collation)));
                            let historical = suite(expression(shape, collation));
                            for round in 0..9 {
                                let (id, fault) = epoch(round);
                                let ctx = ListContext::new(
                                    id,
                                    enabled,
                                    fault,
                                    needle.clone(),
                                    list.clone(),
                                );
                                let history_ctx = ListContext::new(
                                    id,
                                    enabled,
                                    fault,
                                    needle.clone(),
                                    list.clone(),
                                );
                                let ctx = ctx.fresh_ready();
                                let mut input = input(selection.clone());
                                let mut output = output_for(&ty());
                                output.append_null(0);
                                let result = session_read_outcome(|| {
                                    owner.run(&ctx, &mut input, &mut output)
                                });
                                if let Some(actual) = &actual {
                                    let actual_ctx = ListContext::new(
                                        id,
                                        enabled,
                                        fault,
                                        needle.clone(),
                                        list.clone(),
                                    );
                                    let mut actual_input =
                                        super::cached_list_tests::input(selection.clone());
                                    let mut actual_output = output_for(&ty());
                                    actual_output.append_null(0);
                                    let got = session_read_outcome(|| {
                                        actual.run_with_tikv_batch(
                                            &actual_ctx,
                                            &mut actual_input,
                                            &mut actual_output,
                                            &[string_ty()],
                                        )
                                    });
                                    let route = actual.batch_route_snapshot();
                                    let label = format!(
                                        "{shape}/{ci}/{ni}/{li}/{selection:?}/{enabled}/{round}"
                                    );
                                    if got != result
                                        || actual_output.column(0) != output.column(0)
                                        || actual_ctx.snapshot() != ctx.snapshot()
                                        || (route.refusals, route.fallback) != (0, 0)
                                    {
                                        eprintln!("CACHED LIST ACTUAL difference {label}: {got:?} vs {result:?} route={route:?} events:{} vs {}", actual_ctx.snapshot(),ctx.snapshot());
                                    }
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
                                    assert_eq!(actual_input.sel(), input.sel());
                                    assert_eq!(
                                        (route.refusals, route.fallback),
                                        (0, 0),
                                        "source {label}: {route:?}"
                                    );
                                }

                                let mut history_input =
                                    super::cached_list_tests::input(selection.clone());
                                let mut history_output = output_for(&ty());
                                history_output.append_null(0);
                                let history_result = session_read_outcome(|| {
                                    historical.run(
                                        &history_ctx,
                                        &mut history_input,
                                        &mut history_output,
                                    )
                                });
                                let history_observed = format!("{shape}/{ci}/{ni}/{li}/{selection:?}/{enabled}/{round}|{history_result:?}|{:?}|{}|{:?}", history_output.column(0), history_ctx.snapshot(), history_input.sel());
                                for b in history_observed.bytes() {
                                    historical_hash = (historical_hash ^ u64::from(b))
                                        .wrapping_mul(0x100000001b3);
                                }
                                if result != history_result
                                    || output.column(0) != history_output.column(0)
                                    || ctx.snapshot() != history_ctx.snapshot()
                                {
                                    assert!(history_result.contains("ExpressionAdapterFailure { class: ScopePoisoned, origin: Scope }"), "only the retired lane may differ: {history_result} versus {result}");
                                    assert_eq!(history_output.column(0).rows(), 1);
                                    assert!(
                                        ctx.events
                                            .borrow()
                                            .starts_with(&history_ctx.events.borrow()),
                                        "history may stop at the first poisoned Ready call"
                                    );
                                    differences += 1;
                                }

                                let observed=format!("{shape}/{ci}/{ni}/{li}/{selection:?}/{enabled}/{round}|{result:?}|{:?}|{}|{:?}",output.column(0),ctx.snapshot(),input.sel());
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
    }
    eprintln!("CACHED LIST CLEAN SQL ORIGINAL {count}/{hash:016X}; immutable history {historical_hash:016X}; exact ScopePoisoned differences {differences}");
    assert_eq!(
        (count, hash, differences),
        (48600, 0x98391F5AC2C266FB, 2932)
    );
    assert_eq!(historical_hash, 0x1BE95C69A586382F);
    if compare {
        eprintln!("CACHED LIST ACTUAL CLEAN SQL PAIRS {count}");
    }
}

#[test]
fn cached_list_preparation_matches_independent_original_value_closures() {
    if isolate_common_corpus(
        "cached_list_tests::cached_list_preparation_matches_independent_original_value_closures",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    let mut values = crate::tikv::byte_unary_value_tests::values();
    values.extend([
        Datum::new_string("A,a ,b,,中"),
        Datum::new_string("a,b"),
        Datum::new_string("b"),
    ]);
    let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
    for collation in [
        tidb_datatype::Collation::Binary,
        tidb_datatype::Collation::Utf8Mb4Bin,
        tidb_datatype::Collation::Utf8Mb4GeneralCi,
    ] {
        for (li, list) in values.iter().enumerate() {
            for (ni, needle) in values.iter().enumerate() {
                let old = session_read_outcome(|| {
                    crate::builtin_ext::string2::original_find_in_set_with_collation_in(
                        &[needle.clone(), list.clone()],
                        collation,
                        &crate::NoColumns,
                    )
                });
                let new = session_read_outcome(|| {
                    crate::builtin_ext::prepare_find_in_set_tuple_values(needle, list, collation)
                        .and_then(|args| {
                            crate::builtin_ext::eval_find_in_set_prepared_values(
                                args,
                                false,
                                &crate::NoColumns,
                            )
                        })
                });
                assert_eq!(new, old, "tuple {collation:?}/{li}/{ni}");
                for b in format!("tuple/{collation:?}/{li}/{ni}|{old}").bytes() {
                    hash = (hash ^ u64::from(b)).wrapping_mul(0x100000001b3);
                }
                count += 1;
                // Constructor errors and panics are observed separately by the
                // original cache corpus; compare every successfully owned list.
                if let Ok(Ok(lookup)) =
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        crate::builtin_ext::build_find_in_set_lookup(list, collation)
                    }))
                {
                    let old = session_read_outcome(|| {
                        crate::builtin_ext::string2::original_find_in_set_lookup_in(
                            needle,
                            &lookup,
                            collation,
                            &crate::NoColumns,
                        )
                    });
                    let new = session_read_outcome(|| {
                        crate::builtin_ext::prepare_find_in_set_lookup_value(
                            needle, &lookup, collation,
                        )
                        .and_then(|args| {
                            crate::builtin_ext::eval_find_in_set_prepared_values(
                                args,
                                true,
                                &crate::NoColumns,
                            )
                        })
                    });
                    assert_eq!(new, old, "lookup {collation:?}/{li}/{ni}");
                    for b in format!("lookup/{collation:?}/{li}/{ni}|{old}").bytes() {
                        hash = (hash ^ u64::from(b)).wrapping_mul(0x100000001b3);
                    }
                    count += 1;
                }
            }
        }
    }
    eprintln!("CACHED LIST INDEPENDENT VALUE ORIGINAL {count}/{hash:016X}");
    assert_eq!((count, hash), (135888, 0x4913F925E3CA7FDA));
}

#[test]
fn cached_list_long_selections_and_public_canonical_cache_alternation() {
    if isolate_common_corpus(
        "cached_list_tests::cached_list_long_selections_and_public_canonical_cache_alternation",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    for shape in 0..10 {
        for enabled in [false, true] {
            for selected in [false, true] {
                for alternate in [false, true] {
                    let reference = suite(expression(
                        shape,
                        tidb_datatype::Collation::Utf8Mb4GeneralCi,
                    ));
                    let actual = suite(expression(
                        shape,
                        tidb_datatype::Collation::Utf8Mb4GeneralCi,
                    ));
                    for round in 0..9 {
                        let (id, fault) = epoch(round);
                        let mut needle = vec![Datum::new_string("b"); 1025];
                        needle.push(if round % 3 == 0 {
                            Datum::MinNotNull
                        } else {
                            Datum::Null
                        });
                        let list = if round == 4 {
                            Datum::Null
                        } else {
                            Datum::new_string("A,b,c")
                        };
                        let old_ctx =
                            ListContext::new(id, enabled, fault, needle.clone(), list.clone())
                                .fresh_ready();
                        let new_ctx =
                            ListContext::new(id, enabled, fault, needle, list).fresh_ready();
                        let input = || {
                            let mut input = Chunk::new_with_capacity(&[string_ty()], 2051);
                            for row in 0..2051 {
                                input.append_datum(
                                    0,
                                    &Datum::new_string(if row % 2 == 0 {
                                        "A,b,c"
                                    } else {
                                        "c,b,A"
                                    }),
                                );
                            }
                            if selected {
                                input.set_sel(Some((0..2057).map(|i| (i * 17) % 2051).collect()));
                            }
                            input
                        };
                        let mut a = input();
                        let mut b = input();
                        let mut left = output_for(&ty());
                        let mut right = output_for(&ty());
                        left.append_null(0);
                        right.append_null(0);
                        let old =
                            session_read_outcome(|| reference.run(&old_ctx, &mut a, &mut left));
                        let canonical = !alternate || round % 2 == 1;
                        let new = session_read_outcome(|| {
                            if canonical {
                                actual.run_with_tikv_batch(
                                    &new_ctx,
                                    &mut b,
                                    &mut right,
                                    &[string_ty()],
                                )
                            } else {
                                actual.run(&new_ctx, &mut b, &mut right)
                            }
                        });
                        let label = format!("{shape}/{enabled}/{selected}/{alternate}/{round}");
                        assert_eq!(new, old, "result {label}");
                        assert_eq!(right.column(0), left.column(0), "output {label}");
                        assert_eq!(new_ctx.snapshot(), old_ctx.snapshot(), "effects {label}");
                        assert_eq!(a.sel(), b.sel());
                        if canonical {
                            let route = actual.batch_route_snapshot();
                            assert_eq!(
                                (route.refusals, route.fallback),
                                (0, 0),
                                "source {label}: {route:?}"
                            );
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn cached_list_host_authentication_callback_consumption_and_owned_cancellation() {
    use super::hosts::{NativeBatchHosts, NativeHostMetadata};
    use tidb_query_expr::local::{
        HostArgReply, HostInvocation, HostSignature, HostStart, HostStep, HostTaskId,
        LocalHostServices,
    };
    use tidb_query_expr::{NativeCacheAccess, NativeContextCache};
    struct C<'a> {
        store: &'a RefCell<NativeValueStore>,
        action: Cell<u8>,
        calls: Cell<usize>,
        id: Cell<u64>,
        ready_calls: Cell<usize>,
    }
    impl Columns for C<'_> {
        fn get(&self, _: &[String]) -> Option<Datum> {
            panic!("no expression service")
        }
        fn context_id(&self) -> u64 {
            let mut store = self.store.borrow_mut();
            self.calls.set(self.calls.get() + 1);
            match self.action.get() {
                1 => panic!("context identity callback"),
                2 => {
                    store.begin_window().unwrap();
                }
                _ => {}
            }
            self.id.get()
        }
        fn ready_value_cache(&self) -> Option<&crate::ReadyValueCache> {
            let mut store = self.store.borrow_mut();
            self.ready_calls.set(self.ready_calls.get() + 1);
            match self.action.get() {
                3 => panic!("cached value capability callback"),
                4 => {
                    store.begin_window().unwrap();
                }
                _ => {}
            }
            None
        }
    }
    for optional in [false, true] {
        for cacheable in [false, true] {
            let field = projected(&native_bytes_type(), true).unwrap();
            let args = vec![field.clone(); 2];
            let catalog = HostCatalog::for_sql_batch(vec![(
                SqlBatchHostOperation::EagerValuesNative,
                HostSignature {
                    arg_types: args.clone().into(),
                    return_type: field.clone(),
                },
            )])
            .unwrap();
            let slot = catalog.slot(0).unwrap();
            let cache = NativeContextCache::new();
            let metadata = [NativeHostMetadata::CachedList(Box::new(NativeCachedList {
                arguments: [3, 4],
                cacheable,
                cache: cache.share_for_invocation(),
                collation: tidb_datatype::Collation::Utf8Mb4Bin,
                result_type: (!optional).then(ty),
                output_node: 2,
                output_role: NativeValueRole::Output,
            }))];
            let store = RefCell::new(NativeValueStore::new().unwrap());
            store.borrow_mut().begin_window().unwrap();
            let row = InputRow {
                occurrence: 2,
                input_row: 7,
            };
            let insert = |value: &Datum, role, node, row| {
                store
                    .borrow_mut()
                    .insert_clone(value, role, NativeValueOrigin { node, row })
                    .unwrap()
            };
            let stale = insert(&Datum::Null, NativeValueRole::CharInput, 3, row);
            store.borrow_mut().begin_window().unwrap();
            let mut foreign = NativeValueStore::new().unwrap();
            foreign.begin_window().unwrap();
            let foreign = foreign
                .insert_clone(
                    &Datum::Null,
                    NativeValueRole::CharInput,
                    NativeValueOrigin { node: 3, row },
                )
                .unwrap();
            let ctx = C {
                store: &store,
                action: Cell::new(0),
                calls: Cell::new(0),
                id: Cell::new(9),
                ready_calls: Cell::new(0),
            };
            let mut p =
                NativeBatchHosts::new(catalog, &metadata, &ctx).with_native_values(Some(&store));
            let mut eval = EvalContext::default();
            let begin = |p: &mut NativeBatchHosts<'_>, eval: &mut EvalContext| {
                let HostStart::Pending { task, request } = p
                    .start(
                        eval,
                        HostInvocation {
                            slot,
                            row,
                            arg_types: &args,
                            return_type: &field,
                        },
                    )
                    .unwrap()
                else {
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
            let good = insert(&Datum::Null, NativeValueRole::CharInput, 3, row);
            let mut corrupt = good.clone();
            corrupt[0] ^= 1;
            let mut short = good.to_vec();
            short.pop();
            let mut long = good.to_vec();
            long.push(0);
            for bad in [
                None,
                Some((corrupt).to_vec()),
                Some((short).to_vec()),
                Some((long).to_vec()),
                Some((stale).to_vec()),
                Some((foreign).to_vec()),
                Some((insert(&Datum::Null, NativeValueRole::Output, 3, row)).to_vec()),
                Some((insert(&Datum::Null, NativeValueRole::CharInput, 99, row)).to_vec()),
                Some(
                    (insert(
                        &Datum::Null,
                        NativeValueRole::CharInput,
                        3,
                        InputRow {
                            occurrence: 3,
                            ..row
                        },
                    ))
                    .to_vec(),
                ),
                Some(
                    (insert(
                        &Datum::Null,
                        NativeValueRole::CharInput,
                        3,
                        InputRow {
                            input_row: 8,
                            ..row
                        },
                    ))
                    .to_vec(),
                ),
            ] {
                let task = begin(&mut p, &mut eval);
                assert!(matches!(
                    reply(&mut p, &mut eval, &task, 0, bad.map(|bytes| bytes.to_vec())),
                    Err(LocalError::BindingContract(_))
                ));
                assert_eq!(ctx.calls.get(), 0);
                p.cancel(&task);
                p.cancel(&task);
            }
            let task = begin(&mut p, &mut eval);
            assert!(reply(
                &mut p,
                &mut eval,
                &task,
                1,
                Some(insert(&Datum::Null, NativeValueRole::CharInput, 3, row).to_vec())
            )
            .is_err());
            p.cancel(&task);
            assert_eq!(ctx.calls.get(), 0);
            if cacheable {
                for action in [1, 2] {
                    ctx.action.set(action);
                    let task = begin(&mut p, &mut eval);
                    let token = insert(&Datum::Null, NativeValueRole::CharInput, 3, row);
                    let got = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        reply(&mut p, &mut eval, &task, 0, Some(token.to_vec()))
                    }));
                    if action == 1 {
                        assert!(got.is_err());
                    } else {
                        assert!(got.unwrap().is_err());
                    }
                    let before = ctx.calls.get();
                    assert!(reply(&mut p, &mut eval, &task, 0, Some(token.to_vec())).is_err());
                    assert_eq!(ctx.calls.get(), before);
                    p.cancel(&task);
                    assert!(cache.get_cache(9).is_none());
                }
                ctx.action.set(0);
            }
            // NULL needle still requests construction. Cancellation must release
            // the real initializer before another owner may acquire it.
            let task = begin(&mut p, &mut eval);
            let needle = insert(&Datum::Null, NativeValueRole::CharInput, 3, row);
            let HostStep::NeedArg(request) =
                reply(&mut p, &mut eval, &task, 0, Some(needle.to_vec())).unwrap()
            else {
                panic!()
            };
            assert_eq!(request.index, 1);
            assert!(reply(&mut p, &mut eval, &task, 1, Some(needle.to_vec())).is_err());
            p.cancel(&task);
            if cacheable {
                let NativeCacheAccess::Initialize(lease) = cache.begin_initialization(9) else {
                    panic!()
                };
                drop(lease);
            }
            let next = begin(&mut p, &mut eval);
            p.cancel(&task); // stale cancel cannot kill next
            let needle = insert(&Datum::new_string("b"), NativeValueRole::CharInput, 3, row);
            assert!(matches!(
                reply(&mut p, &mut eval, &next, 0, Some(needle.to_vec())).unwrap(),
                HostStep::NeedArg(_)
            ));
            let list = insert(&Datum::Null, NativeValueRole::CharInput, 4, row);
            assert!(matches!(
                reply(&mut p, &mut eval, &next, 1, Some(list.to_vec())).unwrap(),
                HostStep::Ready(_)
            ));
            let before = ctx.calls.get();
            assert!(reply(&mut p, &mut eval, &next, 1, Some(list.to_vec())).is_err());
            assert_eq!(ctx.calls.get(), before);
            p.cancel(&next);
            // A warm NULL cache bypasses both the list request and unused giant
            // needle coercion under a small host work allowance.
            if cacheable {
                let next = begin(&mut p, &mut eval);
                let token = insert(
                    &Datum::Bytes(vec![b'x'; 1 << 20]),
                    NativeValueRole::CharInput,
                    3,
                    row,
                );
                let HostStep::Ready(value) =
                    reply(&mut p, &mut eval, &next, 0, Some(token.to_vec())).unwrap()
                else {
                    panic!()
                };
                use tidb_query_datatype::codec::data_type::ScalarValueRef;
                let ScalarValueRef::Bytes(Some(token)) = value.get_scalar_ref(0) else {
                    panic!()
                };
                assert_eq!(
                    store
                        .borrow()
                        .get(
                            token,
                            NativeValueRole::Output,
                            NativeValueOrigin { node: 2, row }
                        )
                        .unwrap(),
                    &Datum::Null
                );
                p.cancel(&next);
            } else {
                assert_eq!(ctx.calls.get(), 0);
            }
            // The prepared-value capability callback runs after a successful
            // constructor has committed. Unwind or invalidation consumes this task
            // while preserving that SQL cache item for a later fresh invocation.
            for action in [3, 4] {
                ctx.id.set(20 + u64::from(action));
                ctx.action.set(action);
                let next = begin(&mut p, &mut eval);
                let needle = insert(&Datum::Null, NativeValueRole::CharInput, 3, row);
                assert!(matches!(
                    reply(&mut p, &mut eval, &next, 0, Some(needle.to_vec())).unwrap(),
                    HostStep::NeedArg(_)
                ));
                let list = insert(&Datum::Null, NativeValueRole::CharInput, 4, row);
                let got = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    reply(&mut p, &mut eval, &next, 1, Some(list.to_vec()))
                }));
                if action == 3 {
                    assert!(got.is_err());
                } else {
                    assert!(got.unwrap().is_err());
                }
                let before = (ctx.calls.get(), ctx.ready_calls.get());
                assert!(reply(&mut p, &mut eval, &next, 1, Some(list.to_vec())).is_err());
                assert_eq!((ctx.calls.get(), ctx.ready_calls.get()), before);
                p.cancel(&next);
                if cacheable {
                    assert!(cache.get_cache(ctx.id.get()).is_some());
                }
            }
            ctx.action.set(0);
            ctx.id.set(99);
            let next = begin(&mut p, &mut eval);
            let needle = insert(&Datum::Null, NativeValueRole::CharInput, 3, row);
            assert!(matches!(
                reply(&mut p, &mut eval, &next, 0, Some(needle.to_vec())).unwrap(),
                HostStep::NeedArg(_)
            ));
            let list = insert(
                &Datum::new_string("x".repeat(1 << 20)),
                NativeValueRole::CharInput,
                4,
                row,
            );
            assert!(matches!(
                reply(&mut p, &mut eval, &next, 1, Some(list.to_vec())).unwrap(),
                HostStep::Ready(_)
            ));
            p.cancel(&next);
            if cacheable {
                assert!(cache.get_cache(99).is_some());
                assert!(cache.get_cache(24).is_none());
            }
            let next = begin(&mut p, &mut eval);
            let needle = insert(&Datum::Null, NativeValueRole::CharInput, 3, row);
            let first = reply(&mut p, &mut eval, &next, 0, Some(needle.to_vec())).unwrap();
            if cacheable {
                assert!(matches!(first, HostStep::Ready(_)));
            } else {
                assert!(matches!(first, HostStep::NeedArg(_)));
                assert!(matches!(
                    reply(&mut p, &mut eval, &next, 1, Some(list.to_vec())).unwrap(),
                    HostStep::Ready(_)
                ));
            }
            p.cancel(&next);
            assert!(tidb_query_expr::local::value_kernel_work_remaining().is_none());
        }
    }
}

#[test]
fn cached_list_literal_roots_keep_late_context_panic_publication() {
    if isolate_common_corpus(
        "cached_list_tests::cached_list_literal_roots_keep_late_context_panic_publication",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    struct C {
        calls: Cell<usize>,
        fail: usize,
        enabled: bool,
    }
    impl Columns for C {
        fn get(&self, _: &[String]) -> Option<Datum> {
            panic!("no expression callback")
        }
        fn context_id(&self) -> u64 {
            let n = self.calls.get();
            self.calls.set(n + 1);
            if n == self.fail {
                panic!("late literal cache identity")
            }
            11
        }
        fn enable_vectorized_expression(&self) -> bool {
            self.enabled
        }
    }
    for enabled in [false, true] {
        for fail in [0, 1025, usize::MAX] {
            for selected in [false, true] {
                let expression = || {
                    call(
                        "find_in_set",
                        ty(),
                        vec![
                            literal(Datum::new_string("b"), string_ty()),
                            literal(Datum::new_string("a,b,c"), string_ty()),
                        ],
                    )
                };
                let original = suite(expression());
                let actual = suite(expression());
                let ctx = C {
                    calls: Cell::new(0),
                    fail,
                    enabled,
                };
                let new_ctx = C {
                    calls: Cell::new(0),
                    fail,
                    enabled,
                };
                let input = || {
                    let mut value = Chunk::new_with_capacity(&[ty()], 2051);
                    for _ in 0..2051 {
                        value.append_null(0);
                    }
                    if selected {
                        value.set_sel(Some((0..2057).map(|i| (i * 17) % 2051).collect()));
                    }
                    value
                };
                let mut a = input();
                let mut b = input();
                let mut left = output_for(&ty());
                let mut right = output_for(&ty());
                left.append_null(0);
                right.append_null(0);
                let old = session_read_outcome(|| original.run(&ctx, &mut a, &mut left));
                let new = session_read_outcome(|| {
                    actual.run_with_tikv_batch(&new_ctx, &mut b, &mut right, &[ty()])
                });
                assert_eq!(new, old, "{enabled}/{fail}/{selected}");
                assert_eq!(
                    right.column(0),
                    left.column(0),
                    "publication {enabled}/{fail}/{selected}"
                );
                assert_eq!(new_ctx.calls.get(), ctx.calls.get());
                let route = actual.batch_route_snapshot();
                assert_eq!((route.refusals, route.fallback), (0, 0), "{route:?}");
            }
        }
    }
}

#[test]
fn cached_list_actual_complete_values_keep_cached_and_nonconstant_demand() {
    if isolate_common_corpus(
        "cached_list_tests::cached_list_actual_complete_values_keep_cached_and_nonconstant_demand",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    let values = crate::tikv::byte_unary_value_tests::values();
    for shape in [1, 7] {
        for collation in [
            tidb_datatype::Collation::Binary,
            tidb_datatype::Collation::Utf8Mb4Bin,
            tidb_datatype::Collation::Utf8Mb4GeneralCi,
        ] {
            for enabled in [false, true] {
                for (ni, needle) in values.iter().enumerate() {
                    let old = suite(expression(shape, collation));
                    let new = suite(expression(shape, collation));
                    for (li, list) in values.iter().enumerate() {
                        let ctx = ListContext::new(
                            li as u64,
                            enabled,
                            0,
                            vec![needle.clone()],
                            list.clone(),
                        )
                        .fresh_ready();
                        let actual_ctx = ListContext::new(
                            li as u64,
                            enabled,
                            0,
                            vec![needle.clone()],
                            list.clone(),
                        );
                        let mut a = input(Some(vec![2, 0, 2]));
                        let mut b = input(Some(vec![2, 0, 2]));
                        let mut left = output_for(&ty());
                        let mut right = output_for(&ty());
                        let reference = session_read_outcome(|| old.run(&ctx, &mut a, &mut left));
                        let actual = session_read_outcome(|| {
                            new.run_with_tikv_batch(&actual_ctx, &mut b, &mut right, &[string_ty()])
                        });
                        let label = format!("{shape}/{collation:?}/{enabled}/{ni}/{li}");
                        assert_eq!(actual, reference, "result {label}");
                        assert_eq!(right.column(0), left.column(0), "output {label}");
                        assert_eq!(actual_ctx.snapshot(), ctx.snapshot(), "effects {label}");
                        let route = new.batch_route_snapshot();
                        assert_eq!(
                            (route.refusals, route.fallback),
                            (0, 0),
                            "source {label}: {route:?}"
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn cached_list_actual_sparse_decimal_limits_keep_old_cache_and_null_recovery() {
    let sparse = Datum::Decimal(Decimal::from_raw_parts(
        false,
        vec![b'1'],
        1_000_000_000,
        1_000_000_000,
    ));
    for shape in [1, 7] {
        for enabled in [false, true] {
            let actual = suite(expression(shape, tidb_datatype::Collation::Utf8Mb4Bin));
            // Keep the huge value only on paths that leave its formatter undemanded.
            let mut cases = vec![(22, 0, Datum::Null, Datum::Null)];
            if shape == 1 {
                cases.extend([
                    (11, 0, sparse.clone(), Datum::Null),
                    (11, 1, sparse.clone(), Datum::Null),
                    (11, 0, sparse.clone(), Datum::Null),
                ]);
            }
            for (id, fault, needle, list) in cases {
                let ctx = ListContext::new(id, enabled, fault, vec![needle], list);
                let mut input = input(Some(vec![2, 0, 2]));
                let mut output = output_for(&ty());
                output.append_null(0);
                let got = actual.run_with_tikv_batch(&ctx, &mut input, &mut output, &[string_ty()]);
                got.unwrap();
                assert!(output.column(0).is_null(1));
                let route = actual.batch_route_snapshot();
                assert_eq!(
                    (route.refusals, route.fallback),
                    (0, 0),
                    "{shape}/{id}: {route:?}"
                );
            }
        }
    }
}

#[test]
fn cached_list_constructor_encoded_growth_releases_owner_before_publication() {
    let cache = tidb_query_expr::NativeContextCache::new();
    let previous = cache
        .get_or_init_cache(9, || {
            crate::builtin_ext::build_find_in_set_lookup(
                &Datum::Null,
                tidb_datatype::Collation::Binary,
            )
        })
        .unwrap();
    let grown = tidb_query_expr::local::with_value_kernel_work_limit(0, || {
        cache.get_or_init_cache(10, || {
            crate::builtin_ext::build_find_in_set_lookup(
                &Datum::Bytes(vec![b','; 256]),
                tidb_datatype::Collation::Binary,
            )
        })
    })
    .unwrap();
    assert!(!grown.is_null);
    assert!(cache.get_cache(9).is_none());
    assert!(previous.is_null);
    let hit = cache
        .get_or_init_cache(10, || -> std::result::Result<_, EvalError> {
            panic!("warm constructor must remain undemanded")
        })
        .unwrap();
    assert!(std::sync::Arc::ptr_eq(&grown, &hit));
}

#[test]
fn cached_list_return_declarations_match_original_conversion_and_output() {
    if isolate_common_corpus(
        "cached_list_tests::cached_list_return_declarations_match_original_conversion_and_output",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    for field in crate::tikv::eager_scalar_value_tests::types() {
        for shape in 0..10 {
            for enabled in [false, true] {
                let root = || {
                    let Expression::ScalarFunction(mut f) =
                        expression(shape, tidb_datatype::Collation::Utf8Mb4Bin)
                    else {
                        unreachable!()
                    };
                    f.ret_type = Some(field.clone());
                    Expression::ScalarFunction(f)
                };
                let original = suite(root());
                let actual = suite(root());
                for round in 0..9 {
                    let (id, fault) = epoch(round);
                    let ctx = ListContext::new(
                        id,
                        enabled,
                        fault,
                        vec![Datum::new_string("b")],
                        Datum::new_string("a,b,c"),
                    )
                    .fresh_ready();
                    let new_ctx = ListContext::new(
                        id,
                        enabled,
                        fault,
                        vec![Datum::new_string("b")],
                        Datum::new_string("a,b,c"),
                    );
                    let mut a = input(Some(vec![2, 0, 2]));
                    let mut b = input(Some(vec![2, 0, 2]));
                    let mut left = output_for(&field);
                    let mut right = output_for(&field);
                    left.append_null(0);
                    right.append_null(0);
                    let old = session_read_outcome(|| original.run(&ctx, &mut a, &mut left));
                    let new = session_read_outcome(|| {
                        actual.run_with_tikv_batch(&new_ctx, &mut b, &mut right, &[string_ty()])
                    });
                    let label = format!("{field:?}/{shape}/{enabled}/{round}");
                    assert_eq!(new, old, "result {label}");
                    assert_eq!(right.column(0), left.column(0), "output {label}");
                    assert_eq!(new_ctx.snapshot(), ctx.snapshot(), "effects {label}");
                    let route = actual.batch_route_snapshot();
                    assert_eq!(
                        (route.refusals, route.fallback),
                        (0, 0),
                        "source {label}: {route:?}"
                    );
                }
            }
        }
    }
}

#[test]
fn optional_cached_list_original_metadata_cache_and_empty_row_demands() {
    if isolate_common_corpus(
        "cached_list_tests::optional_cached_list_original_metadata_cache_and_empty_row_demands",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_optional_cached_list_matrix(false);
}
#[test]
fn optional_cached_list_actual_metadata_cache_and_empty_row_demands() {
    if isolate_common_corpus(
        "cached_list_tests::optional_cached_list_actual_metadata_cache_and_empty_row_demands",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_optional_cached_list_matrix(true);
}
fn run_optional_cached_list_matrix(compare: bool) {
    let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
    let mut gaps = vec![];
    for shape in 0..10 {
        for mask in 0..8 {
            for (ci, collation) in [
                tidb_datatype::Collation::Binary,
                tidb_datatype::Collation::Utf8Mb4Bin,
                tidb_datatype::Collation::Utf8Mb4GeneralCi,
            ]
            .into_iter()
            .enumerate()
            {
                for (ni, needle) in needles().into_iter().enumerate() {
                    for (li, list) in lists().into_iter().enumerate() {
                        for enabled in [false, true] {
                            for selection in [None, Some(vec![2, 0, 2]), Some(vec![])] {
                                let make = || {
                                    let mut e = expression(shape, collation);
                                    erase_optional_call_declarations(&mut e, mask);
                                    suite(e)
                                };
                                let owner = make();
                                let actual = make();
                                // Preserve one real SQL-cache owner across all
                                // context/failure epochs, independently per path.
                                for round in 0..9 {
                                    let (id, fault) = epoch(round);
                                    let old = ListContext::new(
                                        id,
                                        enabled,
                                        fault,
                                        needle.clone(),
                                        list.clone(),
                                    )
                                    .fresh_ready();
                                    let new = ListContext::new(
                                        id,
                                        enabled,
                                        fault,
                                        needle.clone(),
                                        list.clone(),
                                    )
                                    .fresh_ready();
                                    for reuse in 0..2 {
                                        let mut a = input(selection.clone());
                                        let mut left = output_for(&ty());
                                        left.append_null(0);
                                        let expected = original_session_read_outcome(|| {
                                            owner.run(&old, &mut a, &mut left)
                                        });
                                        let label=format!("optional-list/{shape}/{mask}/{ci}/{ni}/{li}/{enabled}/{selection:?}/{round}/{reuse}");
                                        if compare {
                                            let mut b = input(selection.clone());
                                            let mut right = output_for(&ty());
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
                                                eprintln!("OPTIONAL LIST DIFFERENCE {label} old={expected:?} new={got:?} oldcol={:?} newcol={:?} oldeffects={} neweffects={}",left.column(0),right.column(0),old.snapshot(),new.snapshot());
                                            }
                                            assert_eq!(got, expected, "outcome {label}");
                                            assert_eq!(
                                                right.column(0),
                                                left.column(0),
                                                "column {label}"
                                            );
                                            assert_eq!(
                                                new.snapshot(),
                                                old.snapshot(),
                                                "effects {label}"
                                            );
                                            let route = actual.batch_route_snapshot();
                                            if (route.fallback != 0 || route.refusals != 0)
                                                && gaps.len() < 20
                                            {
                                                gaps.push(format!("{label}: {route:?}"));
                                            }
                                        }
                                        for byte in format!(
                                            "{label}|{expected}|{:?}|{}|{:?}",
                                            left.column(0),
                                            old.snapshot(),
                                            a.sel()
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
    eprintln!("OPTIONAL LIST ORIGINAL {count}/{hash:016X}");
    assert_eq!((count, hash), (777600, 0xE5A69B0AAD36F371));
    if !gaps.is_empty() {
        eprintln!("OPTIONAL LIST ADMISSION RED {gaps:?}");
    }
    assert!(gaps.is_empty());
}
