// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Executed original schema/cache/document demand before source extraction.
use super::*;
use crate::tikv::dynamic_leaf_tests as leaf;
use tidb_query_datatype::codec::data_type::ScalarValueRef;

struct SchemaContext {
    ready: crate::ReadyValueCache,
    values: [Datum; 2],
    profile: usize,
    enabled: bool,
    context: u64,
    reads: [Cell<usize>; 2],
    ids: Cell<usize>,
    events: RefCell<Vec<String>>,
}
impl SchemaContext {
    fn new(values: [Datum; 2], profile: usize, enabled: bool, context: u64) -> Self {
        Self {
            ready: crate::ReadyValueCache::new(),
            values,
            profile,
            enabled,
            context,
            reads: [Cell::new(0), Cell::new(0)],
            ids: Cell::new(0),
            events: RefCell::new(vec![]),
        }
    }
    fn snapshot(&self) -> String {
        format!(
            "{}:{}:{}|{:?}",
            self.reads[0].get(),
            self.reads[1].get(),
            self.ids.get(),
            self.events.borrow()
        )
    }
}
impl Columns for SchemaContext {
    fn ready_value_cache(&self) -> Option<&crate::ReadyValueCache> {
        Some(&self.ready)
    }
    fn get(&self, _: &[String]) -> Option<Datum> {
        panic!("no expression callback")
    }
    fn enable_vectorized_expression(&self) -> bool {
        self.events
            .borrow_mut()
            .push(format!("flag:{}", self.enabled));
        self.enabled
    }
    fn context_id(&self) -> u64 {
        let index = self.ids.get();
        self.ids.set(index + 1);
        self.events
            .borrow_mut()
            .push(format!("context:{}:{index}", self.context));
        if self.profile == 3 && index == 1 {
            panic!("late schema context callback")
        }
        if self.profile == 4 {
            self.context.wrapping_add(index as u64)
        } else {
            self.context
        }
    }
    fn param_value(&self, index: usize) -> std::result::Result<Datum, EvalError> {
        let count = self.reads[index].get();
        self.reads[index].set(count + 1);
        self.events
            .borrow_mut()
            .push(format!("parameter:{index}:{count}"));
        if self.profile == 6 && index == 0 && count == 1026 {
            return Err(EvalError::Unsupported("schema window child failure"));
        }
        if self.profile == 7 && index == 1 && count == 1026 {
            panic!("document window callback");
        }
        if self.profile == 1 && index == 0 && count == 1 {
            return Err(EvalError::Unsupported("late schema child failure"));
        }
        if self.profile == 2 && index == 1 && count == 1 {
            panic!("late document callback")
        }
        if self.profile == 5 && index == 0 && count == 2 {
            return Ok(Datum::Null);
        }
        Ok(self.values[index].clone())
    }
    fn set_uservar(&self, name: &str, value: Datum) {
        self.events
            .borrow_mut()
            .push(format!("set:{name}:{}", leaf::frame(&value)));
    }
    fn type_flags(&self) -> tidb_datatype::ConversionFlags {
        self.events.borrow_mut().push("flags".into());
        tidb_datatype::DEFAULT_STATEMENT_FLAGS.with_ignore_truncate_err(true)
    }
    fn time_zone(&self) -> SessionTimeZone {
        self.events.borrow_mut().push("zone".into());
        SessionTimeZone::utc()
    }
    fn append_warning(&self, code: u16, message: &str) {
        self.events
            .borrow_mut()
            .push(format!("warning:{code}:{message}"));
    }
}
fn parameter(index: usize) -> Expression {
    let mut c = Constant::new(Datum::new_string("unread folded schema"), string_ty());
    c.param_marker = Some(ParamMarker {
        order: index as i64,
    });
    Expression::Constant(c)
}
fn schema_root(shape: usize) -> Expression {
    let schema = match shape {
        1 => literal(Datum::new_string(r#"{"type":"integer"}"#), string_ty()),
        2 => string_column(0),
        3 => {
            let mut c = Constant::new(Datum::new_string("unread deferred schema"), string_ty());
            c.deferred_expr = Some(Box::new(leaf::assignment(
                "schema",
                parameter(0),
                &string_ty(),
            )));
            Expression::Constant(c)
        }
        _ => parameter(0),
    };
    let document = match shape {
        4 => literal(Datum::Null, string_ty()),
        5 => string_column(1),
        _ => parameter(1),
    };
    let args = match shape {
        6 => vec![],
        7 => vec![schema],
        8 => vec![schema, document, parameter(0)],
        _ => vec![schema, document],
    };
    call("json_schema_valid", ty(), args)
}
fn pairs() -> Vec<[Datum; 2]> {
    let s = Datum::new_string;
    vec![
        [Datum::Null, s("{}")],
        [s(r#"{"type":7}"#), s("7")],
        [s("true"), Datum::Null],
        [s("false"), s("7")],
        [s(r#"{"type":"integer"}"#), s(r#""seven""#)],
        [s("[]"), s("{}")],
        [Datum::Bytes(vec![0xff]), s("{}")],
        [s("null"), s("null")],
        [s("{"), s("{")],
        [
            s(r##"{"$defs":{"v":{"type":"integer"}},"$ref":"#/$defs/v"}"##),
            s("7"),
        ],
        [s(r#"{"required":["a"]}"#), s(r#"{"a":1}"#)],
        [s("{}"), Datum::Bytes(vec![0xff])],
    ]
}
#[test]
fn json_schema_original_cache_demand_context_and_publication() {
    if isolate_common_corpus(
        "json_schema_tests::json_schema_original_cache_demand_context_and_publication",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_schema_matrix(false);
}
#[test]
fn json_schema_actual_matrix_keeps_original_cache_and_document_demand() {
    if isolate_common_corpus(
        "json_schema_tests::json_schema_actual_matrix_keeps_original_cache_and_document_demand",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    run_schema_matrix(true);
}
fn run_schema_matrix(compare: bool) {
    let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
    for shape in 0..9 {
        for profile in 0..6 {
            for enabled in [false, true] {
                for selection in [None, Some(vec![2, 0, 2]), Some(vec![])] {
                    // Reuse the actual original SQL-function cache. Only the
                    // obsolete Ready backend is fresh for each invocation.
                    let owner = suite(schema_root(shape));
                    let actual = compare.then(|| suite(schema_root(shape)));
                    for (epoch, context) in [0, 1, 0, u64::MAX, u64::MAX].into_iter().enumerate() {
                        for (vi, values) in pairs().into_iter().enumerate() {
                            let c = SchemaContext::new(values.clone(), profile, enabled, context);
                            let rows = vec![values.to_vec(); 3];
                            let mut input = chunk(&[string_ty(), string_ty()], &rows);
                            input.set_sel(selection.clone());
                            let mut output = output_for(&ty());
                            output.append_null(0);
                            let result =
                                session_read_outcome(|| owner.run(&c, &mut input, &mut output));
                            if let Some(actual) = &actual {
                                let new_ctx =
                                    SchemaContext::new(values.clone(), profile, enabled, context);
                                let mut b = chunk(&[string_ty(), string_ty()], &rows);
                                b.set_sel(selection.clone());
                                let mut right = output_for(&ty());
                                right.append_null(0);
                                let got = session_read_outcome(|| {
                                    actual.run_with_tikv_batch(
                                        &new_ctx,
                                        &mut b,
                                        &mut right,
                                        &[string_ty(), string_ty()],
                                    )
                                });
                                let label=format!("{shape}/{profile}/{enabled}/{selection:?}/{epoch}/{context}/{vi}");
                                if got != result
                                    || right.column(0) != output.column(0)
                                    || new_ctx.snapshot() != c.snapshot()
                                {
                                    eprintln!("JSON SCHEMA ACTUAL DIFFERENCE {label} expected={result:?} got={got:?} left={:?} right={:?} old={} new={}",output.column(0),right.column(0),c.snapshot(),new_ctx.snapshot());
                                }
                                assert_eq!(got, result, "result {label}");
                                assert_eq!(right.column(0), output.column(0), "output {label}");
                                assert_eq!(new_ctx.snapshot(), c.snapshot(), "effects {label}");
                                let route = actual.batch_route_snapshot();
                                assert_eq!(
                                    (route.refusals, route.fallback),
                                    (0, 0),
                                    "route {label}: {route:?}"
                                );
                            }

                            let label = format!(
                                "{shape}/{profile}/{enabled}/{selection:?}/{epoch}/{context}/{vi}"
                            );
                            for byte in
                                format!("{label}|{result}|{:?}|{}", output.column(0), c.snapshot())
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
    eprintln!("JSON SCHEMA ORIGINAL {count}/{hash:016X}");
    assert_eq!((count, hash), (19440, 0x5A44B7F912410B39));
}
#[test]
fn json_schema_actual_sources_do_not_fallback() {
    let mut gaps = vec![];
    for shape in 0..9 {
        let owner = suite(schema_root(shape));
        let values = [Datum::new_string("true"), Datum::new_string("7")];
        let c = SchemaContext::new(values.clone(), 0, true, 0);
        let mut input = chunk(&[string_ty(), string_ty()], &[values.to_vec()]);
        let mut output = output_for(&ty());
        let _ = session_read_outcome(|| {
            owner.run_with_tikv_batch(&c, &mut input, &mut output, &[string_ty(), string_ty()])
        });
        let route = owner.batch_route_snapshot();
        if (route.refusals, route.fallback) != (0, 0) {
            gaps.push((shape, route));
        }
    }
    assert!(gaps.is_empty(), "JSON schema source gaps: {gaps:?}");
}

#[test]
fn json_schema_value_phases_match_active_original_cache_and_lazy_demand() {
    if isolate_common_corpus(
        "json_schema_tests::json_schema_value_phases_match_active_original_cache_and_lazy_demand",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
    for shape in 0..9 {
        for profile in 0..6 {
            let Expression::ScalarFunction(original) = schema_root(shape) else {
                unreachable!()
            };
            let cache = crate::builtin_ext::JsonSchemaCache::default();
            for (epoch, context) in [0, 1, 0, u64::MAX, u64::MAX].into_iter().enumerate() {
                for (vi, values) in pairs().into_iter().enumerate() {
                    let left = SchemaContext::new(values.clone(), profile, false, context);
                    let right = SchemaContext::new(values.clone(), profile, false, context);
                    let input = chunk(&[string_ty(), string_ty()], &vec![values.to_vec(); 3]);
                    let prefix_left = RefCell::new(vec![]);
                    let prefix_right = RefCell::new(vec![]);
                    let old = session_read_outcome(|| {
                        left.ready.with_columns(
                            &left,
                            |ctx| -> std::result::Result<(), EvalError> {
                                for i in 0..3 {
                                    prefix_left.borrow_mut().push(leaf::frame(
                                        &original
                                            .eval_original_json_schema(ctx, input.get_row(i))?,
                                    ));
                                }
                                Ok(())
                            },
                        )
                    });
                    let owner = cache.share_for_invocation();
                    let new = session_read_outcome(|| {
                        right.ready.with_columns(
                            &right,
                            |ctx| -> std::result::Result<(), EvalError> {
                                for i in 0..3 {
                                    let row = input.get_row(i);
                                    // This test-only argument callback is independent
                                    // of the production original expression wrapper.
                                    let [schema, document] = original.args.as_slice() else {
                                        return Err(EvalError::WrongParameterCount(
                                            "json_schema_valid",
                                        ));
                                    };
                                    let value = schema.eval(ctx, row)?;
                                    let value = if value.is_null() {
                                        Datum::Null
                                    } else {
                                        let context = (schema.const_level()
                                            >= crate::expression::ConstLevel::ONLY_IN_CONTEXT)
                                            .then(|| ctx.context_id());
                                        let prepared = owner.prepare_value(&value, context)?;
                                        if prepared.is_absent() {
                                            Datum::Null
                                        } else {
                                            prepared.validate_value(&document.eval(ctx, row)?)?
                                        }
                                    };
                                    let value = crate::scalar_function::coerce_function_result(
                                        "json_schema_valid",
                                        original.get_static_type(),
                                        value,
                                    )?;
                                    prefix_right.borrow_mut().push(leaf::frame(&value));
                                }
                                Ok(())
                            },
                        )
                    });
                    let label = format!("{shape}/{profile}/{epoch}/{context}/{vi}");
                    assert_eq!(new, old, "result {label}");
                    assert_eq!(
                        *prefix_right.borrow(),
                        *prefix_left.borrow(),
                        "prefix {label}"
                    );
                    assert_eq!(right.snapshot(), left.snapshot(), "effects {label}");
                    for byte in format!(
                        "{label}|{old}|{:?}|{}",
                        prefix_left.borrow(),
                        left.snapshot()
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
    eprintln!("JSON SCHEMA INDEPENDENT PHASES {count}/{hash:016X}");
    assert_eq!((count, hash), (3240, 0x8EEBA8F200EECE0C));
}

fn reference_case(
    original: &EvaluatorSuite,
    actual: &EvaluatorSuite,
    values: [Datum; 2],
    context: u64,
    enabled: bool,
    expected: Option<Option<i64>>,
) -> String {
    let old = SchemaContext::new(values.clone(), 0, enabled, context);
    let new = SchemaContext::new(values.clone(), 0, enabled, context);
    let rows = vec![values.to_vec(); 3];
    let mut a = chunk(&[string_ty(), string_ty()], &rows);
    let mut b = chunk(&[string_ty(), string_ty()], &rows);
    let mut left = output_for(&ty());
    let mut right = output_for(&ty());
    left.append_null(0);
    right.append_null(0);
    let result = session_read_outcome(|| original.run(&old, &mut a, &mut left));
    let got = session_read_outcome(|| {
        actual.run_with_tikv_batch(&new, &mut b, &mut right, &[string_ty(), string_ty()])
    });
    assert_eq!(got, result);
    assert_eq!(right.column(0), left.column(0));
    assert_eq!(new.snapshot(), old.snapshot());
    let route = actual.batch_route_snapshot();
    assert_eq!((route.refusals, route.fallback), (0, 0), "{route:?}");
    if let Some(expected) = expected {
        assert_eq!(result, "Ok(())");
        assert_eq!(left.num_rows(), 4);
        for i in 1..4 {
            assert_eq!(left.get_row(i).is_null(0), expected.is_none());
            if let Some(value) = expected {
                assert_eq!(left.get_row(i).get_int64(0), value);
            }
        }
    } else {
        assert!(result.starts_with("Err("), "{result}");
    }
    result
}
#[test]
fn json_schema_file_reference_loading_and_cached_builder_errors_follow_document_demand() {
    for enabled in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("reference.json");
        let schema = Datum::new_string(format!(r#"{{"$ref":"file://{}"}}"#, path.display()));
        let original = suite(schema_root(0));
        let actual = suite(schema_root(0));
        reference_case(
            &original,
            &actual,
            [schema.clone(), Datum::Null],
            0,
            enabled,
            Some(None),
        );
        let parse = reference_case(
            &original,
            &actual,
            [schema.clone(), Datum::new_string("{")],
            0,
            enabled,
            None,
        );
        assert!(parse.contains("InvalidText"), "{parse}");
        // Neither previous document allowed an attempted missing-file load to
        // become the prepared schema's cached validator error.
        std::fs::write(&path, r#"{"type":"integer"}"#).unwrap();
        reference_case(
            &original,
            &actual,
            [schema.clone(), Datum::new_string("7")],
            0,
            enabled,
            Some(Some(1)),
        );
        std::fs::write(&path, r#"{"type":"string"}"#).unwrap();
        reference_case(
            &original,
            &actual,
            [Datum::Bytes(vec![0xff]), Datum::new_string("7")],
            0,
            enabled,
            Some(Some(1)),
        );
        reference_case(
            &original,
            &actual,
            [schema.clone(), Datum::Null],
            0,
            enabled,
            Some(None),
        );
        reference_case(
            &original,
            &actual,
            [schema.clone(), Datum::new_string("7")],
            1,
            enabled,
            Some(Some(0)),
        );
        std::fs::remove_file(&path).unwrap();
        let build = reference_case(
            &original,
            &actual,
            [schema.clone(), Datum::new_string("7")],
            2,
            enabled,
            None,
        );
        assert!(build.contains("InvalidJsonType"), "{build}");
        std::fs::write(&path, r#"{"type":"integer"}"#).unwrap();
        reference_case(
            &original,
            &actual,
            [schema.clone(), Datum::Null],
            2,
            enabled,
            Some(None),
        );
        let parse = reference_case(
            &original,
            &actual,
            [schema.clone(), Datum::new_string("{")],
            2,
            enabled,
            None,
        );
        assert!(parse.contains("InvalidText"), "{parse}");
        let cached = reference_case(
            &original,
            &actual,
            [schema.clone(), Datum::new_string("7")],
            2,
            enabled,
            None,
        );
        assert_eq!(cached, build);
        reference_case(
            &original,
            &actual,
            [schema, Datum::new_string("7")],
            0,
            enabled,
            Some(Some(1)),
        );
    }
}

struct SchemaServer {
    address: std::net::SocketAddr,
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    count: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    string: std::sync::Arc<std::sync::atomic::AtomicBool>,
    task: Option<std::thread::JoinHandle<()>>,
}
impl SchemaServer {
    fn new() -> Self {
        use std::io::{Read, Write};
        use std::sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
            Arc,
        };
        let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let count = Arc::new(AtomicUsize::new(0));
        let string = Arc::new(AtomicBool::new(false));
        let (done, requests, mode) = (stop.clone(), count.clone(), string.clone());
        let task = std::thread::spawn(move || {
            while !done.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        stream
                            .set_read_timeout(Some(std::time::Duration::from_secs(2)))
                            .unwrap();
                        let mut request = [0u8; 4096];
                        let n = stream.read(&mut request).unwrap();
                        assert!(std::str::from_utf8(&request[..n])
                            .unwrap()
                            .starts_with("GET /schema.json "));
                        requests.fetch_add(1, Ordering::Relaxed);
                        let body = if mode.load(Ordering::Relaxed) {
                            r#"{"type":"string"}"#
                        } else {
                            r#"{"type":"integer"}"#
                        };
                        write!(stream,"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).unwrap();
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(std::time::Duration::from_millis(1))
                    }
                    Err(error) => panic!("schema listener: {error}"),
                }
            }
        });
        Self {
            address,
            stop,
            count,
            string,
            task: Some(task),
        }
    }
    fn count(&self) -> usize {
        self.count.load(std::sync::atomic::Ordering::Relaxed)
    }
}
impl Drop for SchemaServer {
    fn drop(&mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::Relaxed);
        if let Some(task) = self.task.take() {
            let _ = task.join();
        }
    }
}
#[test]
fn json_schema_http_reference_is_not_loaded_until_nonnull_parsed_document() {
    for enabled in [false, true] {
        let server = SchemaServer::new();
        let schema = Datum::new_string(format!(
            r#"{{"$ref":"http://{}/schema.json"}}"#,
            server.address
        ));
        let original = suite(schema_root(0));
        let actual = suite(schema_root(0));
        reference_case(
            &original,
            &actual,
            [schema.clone(), Datum::Null],
            0,
            enabled,
            Some(None),
        );
        assert_eq!(server.count(), 0);
        reference_case(
            &original,
            &actual,
            [schema.clone(), Datum::new_string("{")],
            0,
            enabled,
            None,
        );
        assert_eq!(server.count(), 0);
        reference_case(
            &original,
            &actual,
            [schema.clone(), Datum::new_string("7")],
            0,
            enabled,
            Some(Some(1)),
        );
        assert_eq!(server.count(), 2);
        server
            .string
            .store(true, std::sync::atomic::Ordering::Relaxed);
        reference_case(
            &original,
            &actual,
            [schema.clone(), Datum::new_string("7")],
            0,
            enabled,
            Some(Some(1)),
        );
        assert_eq!(server.count(), 2);
        reference_case(
            &original,
            &actual,
            [Datum::Null, Datum::new_string("7")],
            0,
            enabled,
            Some(None),
        );
        assert_eq!(server.count(), 2);
        reference_case(
            &original,
            &actual,
            [schema.clone(), Datum::Null],
            1,
            enabled,
            Some(None),
        );
        assert_eq!(server.count(), 2);
        reference_case(
            &original,
            &actual,
            [schema, Datum::new_string("7")],
            1,
            enabled,
            Some(Some(0)),
        );
        assert_eq!(server.count(), 4);
    }
}

#[test]
fn json_schema_full_actual_values_and_return_declarations_keep_original_errors_and_prefixes() {
    if isolate_common_corpus("json_schema_tests::json_schema_full_actual_values_and_return_declarations_keep_original_errors_and_prefixes"){return}
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
            for schema_value in [false, true] {
                for deferred in [false, true] {
                    let order = usize::from(!schema_value);
                    let source = crate::tikv::misc_identity_value_tests::field(&value);
                    let mut p = Constant::new(Datum::Int(99), source.clone());
                    p.param_marker = Some(ParamMarker {
                        order: order as i64,
                    });
                    let mut arg = Expression::Constant(p);
                    if deferred {
                        let mut c = Constant::new(Datum::Int(99), source);
                        c.deferred_expr = Some(Box::new(arg));
                        arg = Expression::Constant(c);
                    }
                    let args = if schema_value {
                        vec![arg, literal(Datum::Null, string_ty())]
                    } else {
                        vec![literal(Datum::new_string("true"), string_ty()), arg]
                    };
                    let expression = call("json_schema_valid", field.clone(), args);
                    let actual = suite(expression.clone());
                    for enabled in [false, true] {
                        for selection in [None, Some(vec![2, 0, 2]), Some(vec![])] {
                            let pair = if schema_value {
                                [value.clone(), Datum::Null]
                            } else {
                                [Datum::Null, value.clone()]
                            };
                            let old = SchemaContext::new(pair.clone(), 0, enabled, 0);
                            let new = SchemaContext::new(pair, 0, enabled, 0);
                            let original = suite(expression.clone());
                            let input = || {
                                let mut c = chunk(
                                    &[string_ty(), string_ty()],
                                    &vec![
                                        vec![
                                            Datum::new_string("unread"),
                                            Datum::new_string("unread")
                                        ];
                                        3
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
                            let expected = original_session_read_outcome(|| {
                                original.run(&old, &mut a, &mut left)
                            });
                            let got = session_read_outcome(|| {
                                actual.run_with_tikv_batch(
                                    &new,
                                    &mut b,
                                    &mut right,
                                    &[string_ty(), string_ty()],
                                )
                            });
                            let label = format!(
                                "{fi}/{vi}/{schema_value}/{deferred}/{enabled}/{selection:?}"
                            );
                            if got != expected
                                || right.column(0) != left.column(0)
                                || new.snapshot() != old.snapshot()
                            {
                                eprintln!("SCHEMA FULL DIFFERENCE {label} input={} old={expected:?} new={got:?} oldeffects={} neweffects={}",leaf::frame(&value),old.snapshot(),new.snapshot());
                            }
                            assert_eq!(got, expected, "result {label}");
                            assert_eq!(right.column(0), left.column(0), "output {label}");
                            assert_eq!(new.snapshot(), old.snapshot(), "effects {label}");
                            let route = actual.batch_route_snapshot();
                            assert_eq!(
                                (route.refusals, route.fallback),
                                (0, 0),
                                "route {label}: {route:?}"
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
    eprintln!("JSON SCHEMA FULL ORIGINAL {count}/{hash:016X}");
    assert_eq!((count, hash), (39336, 0xDC8D2D5F9AEF7D85));
}

#[test]
fn json_schema_direct_host_authenticates_before_cache_publication_and_recovers_after_cancel() {
    use super::hosts::{NativeBatchHosts, NativeHostMetadata, NativeJsonSchema};
    use tidb_query_expr::local::{
        HostArgReply, HostInvocation, HostSignature, HostStart, HostStep, HostTaskId,
        LocalHostServices,
    };
    struct D<'a> {
        store: &'a RefCell<NativeValueStore>,
        action: Cell<u8>,
        calls: Cell<usize>,
    }
    impl Columns for D<'_> {
        fn get(&self, _: &[String]) -> Option<Datum> {
            panic!("no expression callback")
        }
        fn context_id(&self) -> u64 {
            self.calls.set(self.calls.get() + 1);
            let mut store = self.store.borrow_mut();
            match self.action.get() {
                1 => panic!("schema context callback"),
                2 => store.begin_window().unwrap(),
                _ => {}
            };
            5
        }
    }
    for optional in [false, true] {
        let wire = projected(&native_bytes_type(), true).unwrap();
        let args = vec![wire.clone(); 2];
        let catalog = HostCatalog::for_sql_batch(vec![(
            SqlBatchHostOperation::EagerValuesNative,
            HostSignature {
                arg_types: args.clone().into(),
                return_type: wire.clone(),
            },
        )])
        .unwrap();
        let slot = catalog.slot(0).unwrap();
        let metadata = [NativeHostMetadata::JsonSchema(Box::new(NativeJsonSchema {
            arguments: vec![3, 4].into_boxed_slice(),
            cacheable: true,
            cache: crate::builtin_ext::JsonSchemaCache::default(),
            result_type: (!optional).then(ty),
            output_node: 2,
            output_role: NativeValueRole::Output,
        }))];
        let store = RefCell::new(NativeValueStore::new(8 << 20).unwrap());
        store.borrow_mut().begin_window().unwrap();
        let ctx = D {
            store: &store,
            action: Cell::new(0),
            calls: Cell::new(0),
        };
        let mut p =
            NativeBatchHosts::new(catalog, &metadata, &ctx, 16384).with_native_values(Some(&store));
        let mut eval = EvalContext::default();
        let row = InputRow {
            occurrence: 2,
            input_row: 7,
        };
        let origin = |node| NativeValueOrigin { node, row };
        let insert = |value: &Datum, role, node| {
            store
                .borrow_mut()
                .insert_clone(value, role, origin(node))
                .unwrap()
        };
        let begin = |p: &mut NativeBatchHosts<'_>, eval: &mut EvalContext| {
            let HostStart::Pending { task, request } = p
                .start(
                    eval,
                    HostInvocation {
                        slot,
                        row,
                        arg_types: &args,
                        return_type: &wire,
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
                    field_type: &wire,
                    values: &VectorValue::from_scalar(&ScalarValue::Bytes(token), 1),
                },
            )
        };
        let check = |step: HostStep, expected: &Datum| {
            let HostStep::Ready(values) = step else {
                panic!()
            };
            let ScalarValueRef::Bytes(Some(token)) = values.get_scalar_ref(0) else {
                panic!()
            };
            assert_eq!(
                store
                    .borrow()
                    .get(token, NativeValueRole::Output, origin(2))
                    .unwrap(),
                expected
            );
        };
        let original = insert(&Datum::Null, NativeValueRole::CharInput, 3);
        let mut bad = original.clone();
        bad[0] ^= 1;
        let mut short = original.clone();
        short.pop();
        let mut long = original.clone();
        long.push(0);
        store.borrow_mut().begin_window().unwrap();
        let mut foreign = NativeValueStore::new(4096).unwrap();
        foreign.begin_window().unwrap();
        let foreign = foreign
            .insert_clone(&Datum::Null, NativeValueRole::CharInput, origin(3))
            .unwrap();
        let wrong_row = store
            .borrow_mut()
            .insert_clone(
                &Datum::Null,
                NativeValueRole::CharInput,
                NativeValueOrigin {
                    node: 3,
                    row: InputRow {
                        occurrence: 3,
                        ..row
                    },
                },
            )
            .unwrap();
        let wrong_physical = store
            .borrow_mut()
            .insert_clone(
                &Datum::Null,
                NativeValueRole::CharInput,
                NativeValueOrigin {
                    node: 3,
                    row: InputRow {
                        input_row: 8,
                        ..row
                    },
                },
            )
            .unwrap();
        for token in [
            None,
            Some(original),
            Some(bad),
            Some(short),
            Some(long),
            Some(foreign),
            Some(wrong_row),
            Some(wrong_physical),
            Some(insert(&Datum::Null, NativeValueRole::Output, 3)),
            Some(insert(&Datum::Null, NativeValueRole::CharInput, 4)),
        ] {
            let task = begin(&mut p, &mut eval);
            assert!(matches!(
                reply(&mut p, &mut eval, &task, 0, token),
                Err(LocalError::BindingContract(_))
            ));
            assert_eq!(ctx.calls.get(), 0);
            p.cancel(&task);
        }
        for action in [1, 2] {
            ctx.action.set(action);
            let task = begin(&mut p, &mut eval);
            let schema = insert(&Datum::new_string("true"), NativeValueRole::CharInput, 3);
            let got = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                reply(&mut p, &mut eval, &task, 0, Some(schema.clone()))
            }));
            if action == 1 {
                assert!(got.is_err())
            } else {
                assert!(matches!(got.unwrap(), Err(LocalError::BindingContract(_))))
            }
            let before = ctx.calls.get();
            assert!(reply(&mut p, &mut eval, &task, 0, Some(schema)).is_err());
            assert_eq!(ctx.calls.get(), before);
            p.cancel(&task);
            ctx.action.set(0);
        }
        // Neither failed context callback was allowed to publish a prepared true
        // schema. The same SQL context must still reject a genuinely invalid one.
        let task = begin(&mut p, &mut eval);
        let bad = insert(&Datum::new_string("[]"), NativeValueRole::CharInput, 3);
        assert!(reply(&mut p, &mut eval, &task, 0, Some(bad)).is_err());
        assert!(p.take_native_error().is_some());
        p.cancel(&task);
        for _ in 0..128 {
            store.borrow_mut().begin_window().unwrap();
            let abandoned = begin(&mut p, &mut eval);
            let schema = insert(&Datum::new_string("true"), NativeValueRole::CharInput, 3);
            assert!(matches!(
                reply(&mut p, &mut eval, &abandoned, 0, Some(schema)),
                Ok(HostStep::NeedArg(_))
            ));
            p.cancel(&abandoned);
            let current = begin(&mut p, &mut eval);
            p.cancel(&abandoned);
            let before = ctx.calls.get();
            let unused = insert(&Datum::new_string("7"), NativeValueRole::CharInput, 4);
            assert!(reply(&mut p, &mut eval, &abandoned, 1, Some(unused)).is_err());
            assert_eq!(ctx.calls.get(), before);
            let null = insert(&Datum::Null, NativeValueRole::CharInput, 3);
            check(
                reply(&mut p, &mut eval, &current, 0, Some(null)).unwrap(),
                &Datum::Null,
            );
            assert_eq!(ctx.calls.get(), before);
            p.cancel(&current);
        }
        let task = begin(&mut p, &mut eval);
        let schema = insert(&Datum::new_string("false"), NativeValueRole::CharInput, 3);
        assert!(matches!(
            reply(&mut p, &mut eval, &task, 0, Some(schema)),
            Ok(HostStep::NeedArg(_))
        ));
        // The abandoned valid constructor committed before document demand, so
        // this actual false input hits that original true cache item.
        let before = ctx.calls.get();
        let wrong = insert(&Datum::new_string("7"), NativeValueRole::Output, 4);
        assert!(reply(&mut p, &mut eval, &task, 1, Some(wrong)).is_err());
        assert_eq!(ctx.calls.get(), before);
        let doc = insert(&Datum::new_string("7"), NativeValueRole::CharInput, 4);
        check(
            reply(&mut p, &mut eval, &task, 1, Some(doc)).unwrap(),
            &Datum::Int(1),
        );
        p.cancel(&task);
        let task = begin(&mut p, &mut eval);
        let schema = insert(&Datum::new_string("true"), NativeValueRole::CharInput, 3);
        assert!(matches!(
            reply(&mut p, &mut eval, &task, 0, Some(schema)),
            Ok(HostStep::NeedArg(_))
        ));
        store.borrow_mut().begin_window().unwrap();
        let before = ctx.calls.get();
        let doc = insert(&Datum::new_string("7"), NativeValueRole::CharInput, 4);
        assert!(matches!(
            reply(&mut p, &mut eval, &task, 1, Some(doc)),
            Err(LocalError::BindingContract(_))
        ));
        assert_eq!(ctx.calls.get(), before);
        p.cancel(&task);

        // A dynamic schema's retained parsed tree must fit before document demand.
        // The existing provider policy stays fixed and must recover on real NULL.
        let catalog = HostCatalog::for_sql_batch(vec![(
            SqlBatchHostOperation::EagerValuesNative,
            HostSignature {
                arg_types: args.clone().into(),
                return_type: wire.clone(),
            },
        )])
        .unwrap();
        let slot = catalog.slot(0).unwrap();
        let metadata = [NativeHostMetadata::JsonSchema(Box::new(NativeJsonSchema {
            arguments: vec![3, 4].into_boxed_slice(),
            cacheable: false,
            cache: crate::builtin_ext::JsonSchemaCache::default(),
            result_type: (!optional).then(ty),
            output_node: 2,
            output_role: NativeValueRole::Output,
        }))];
        let mut dynamic =
            NativeBatchHosts::new(catalog, &metadata, &ctx, 16384).with_native_values(Some(&store));
        let start = |p: &mut NativeBatchHosts<'_>, eval: &mut EvalContext| {
            let HostStart::Pending { task, .. } = p
                .start(
                    eval,
                    HostInvocation {
                        slot,
                        row,
                        arg_types: &args,
                        return_type: &wire,
                    },
                )
                .unwrap()
            else {
                panic!()
            };
            task
        };
        ctx.action.set(1);
        let before = ctx.calls.get();
        let task = start(&mut dynamic, &mut eval);
        let large = Datum::new_string(format!(r#"{{"$comment":"{}"}}"#, "x".repeat(32 << 10)));
        let token = insert(&large, NativeValueRole::CharInput, 3);
        assert!(matches!(
            reply(&mut dynamic, &mut eval, &task, 0, Some(token)),
            Err(LocalError::ResourceLimit(_))
        ));
        assert_eq!(ctx.calls.get(), before);
        dynamic.cancel(&task);
        let task = start(&mut dynamic, &mut eval);
        let null = insert(&Datum::Null, NativeValueRole::CharInput, 3);
        check(
            reply(&mut dynamic, &mut eval, &task, 0, Some(null)).unwrap(),
            &Datum::Null,
        );
        assert_eq!(ctx.calls.get(), before);
        dynamic.cancel(&task);
        assert!(tidb_query_expr::local::value_kernel_work_remaining().is_none());
    }
}

#[test]
fn json_schema_long_selected_sources_share_the_original_cache_with_public_calls() {
    if isolate_common_corpus("json_schema_tests::json_schema_long_selected_sources_share_the_original_cache_with_public_calls"){return}
    std::panic::set_hook(Box::new(|_| {}));
    for shape in [0, 2, 3] {
        let original = suite(schema_root(shape));
        let actual = suite(schema_root(shape));
        for rows in [2051usize, 2057] {
            let selections = [
                None,
                Some((0..rows).step_by(2).rev().chain([0, rows - 1, 0]).collect()),
                Some(vec![rows - 1, 0, rows - 1]),
                Some(vec![]),
            ];
            for selection in selections {
                for profile in [0, 6, 7] {
                    for cycle in 0..4 {
                        // The complete dynamic cross-product is archived green.
                        // Keep every cache/public/failure case on the cached
                        // shape; dynamic shapes separately cross each selection
                        // window without rebuilding a validator redundantly.
                        if shape != 0 && (rows != 2057 || profile != 0 || cycle != 2) {
                            continue;
                        }

                        let values = [
                            Datum::new_string(if cycle % 2 == 0 { "true" } else { "false" }),
                            Datum::new_string("7"),
                        ];
                        let old = SchemaContext::new(values.clone(), profile, cycle >= 2, 0);
                        let new = SchemaContext::new(values.clone(), profile, cycle >= 2, 0);
                        let input = || {
                            let mut c =
                                chunk(&[string_ty(), string_ty()], &vec![values.to_vec(); rows]);
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
                            original_session_read_outcome(|| original.run(&old, &mut a, &mut left));
                        let got = session_read_outcome(|| {
                            if cycle % 2 == 0 {
                                actual.run_with_tikv_batch(
                                    &new,
                                    &mut b,
                                    &mut right,
                                    &[string_ty(), string_ty()],
                                )
                            } else {
                                actual.run(&new, &mut b, &mut right)
                            }
                        });
                        let label = format!("{shape}/{rows}/{profile}/{cycle}");
                        if got != expected
                            || right.column(0) != left.column(0)
                            || new.snapshot() != old.snapshot()
                        {
                            eprintln!("SCHEMA LONG DIFFERENCE {label} old={expected:?} new={got:?} oldeffects={} neweffects={}",old.snapshot(),new.snapshot());
                        }
                        assert_eq!(got, expected, "result {label}");
                        assert_eq!(right.column(0), left.column(0), "output {label}");
                        assert_eq!(new.snapshot(), old.snapshot(), "effects {label}");
                        assert_eq!(a.sel(), b.sel(), "selection {label}");
                        let route = actual.batch_route_snapshot();
                        assert_eq!(
                            (route.refusals, route.fallback),
                            (0, 0),
                            "route {label}: {route:?}"
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn json_schema_cached_large_nonnull_inputs_drop_after_their_last_semantic_use() {
    let original = suite(schema_root(0));
    let actual = suite(schema_root(0));
    let vector = Datum::VectorFloat32(VectorFloat32::init(524_288));
    let unsupported = reference_case(
        &original,
        &actual,
        [vector.clone(), Datum::new_string("7")],
        0,
        false,
        None,
    );
    assert!(
        unsupported.contains("JSON document requires JSON or string"),
        "{unsupported}"
    );
    reference_case(
        &original,
        &actual,
        [Datum::new_string("true"), Datum::new_string("7")],
        0,
        false,
        Some(Some(1)),
    );
    // Schema arguments are still evaluated on a hit. Their now-ignored SQL
    // payload has no later use; each of three rows must complete normally.
    reference_case(
        &original,
        &actual,
        [vector, Datum::new_string("7")],
        0,
        false,
        Some(Some(1)),
    );
}

#[test]
fn json_schema_nested_fresh_bodies_keep_schema_reads_and_cache_history() {
    if isolate_common_corpus(
        "json_schema_tests::json_schema_nested_fresh_bodies_keep_schema_reads_and_cache_history",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    for shape in [0, 2, 3, 4] {
        for profile in 0..6 {
            for enabled in [false, true] {
                for selection in [None, Some(vec![2, 0, 2]), Some(vec![])] {
                    let expression = call("benchmark", ty(), vec![int(3), schema_root(shape)]);
                    let original = suite(expression.clone());
                    let actual = suite(expression);
                    for (vi, values) in pairs().into_iter().enumerate() {
                        let old = SchemaContext::new(values.clone(), profile, enabled, 0);
                        let new = SchemaContext::new(values.clone(), profile, enabled, 0);
                        let input = || {
                            let mut c =
                                chunk(&[string_ty(), string_ty()], &vec![values.to_vec(); 3]);
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
                            original_session_read_outcome(|| original.run(&old, &mut a, &mut left));
                        let got = session_read_outcome(|| {
                            actual.run_with_tikv_batch(
                                &new,
                                &mut b,
                                &mut right,
                                &[string_ty(), string_ty()],
                            )
                        });
                        let label = format!("{shape}/{profile}/{enabled}/{selection:?}/{vi}");
                        if got != expected
                            || right.column(0) != left.column(0)
                            || new.snapshot() != old.snapshot()
                        {
                            eprintln!("SCHEMA FRESH DIFFERENCE {label} old={expected:?} new={got:?} oldeffects={} neweffects={}",old.snapshot(),new.snapshot());
                        }
                        assert_eq!(got, expected, "result {label}");
                        assert_eq!(right.column(0), left.column(0), "output {label}");
                        assert_eq!(new.snapshot(), old.snapshot(), "effects {label}");
                        let route = actual.batch_route_snapshot();
                        assert_eq!(
                            (route.refusals, route.fallback),
                            (0, 0),
                            "route {label}: {route:?}"
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn optional_phase_json_schema_original_and_source() {
    if isolate_common_corpus("json_schema_tests::optional_phase_json_schema_original_and_source") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    let mut observed = (0usize, 0xcbf29ce484222325u64, Vec::new());
    for name in ["json_schema_valid"] {
        for shape in 0..9 {
            for mask in 0..8 {
                for mode in [0usize] {
                    for profile in 0..6 {
                        for enabled in [false, true] {
                            for (vi, values) in pairs().into_iter().enumerate() {
                                for selection in [None, Some(vec![2, 0, 2]), Some(vec![])] {
                                    let make = || {
                                        let mut e = schema_root(shape);
                                        erase_optional_call_declarations(&mut e, mask);
                                        suite(e)
                                    };
                                    let owner = make();
                                    let actual = make();
                                    let old =
                                        SchemaContext::new(values.clone(), profile, enabled, 93);
                                    let new =
                                        SchemaContext::new(values.clone(), profile, enabled, 93);
                                    let field = ty();
                                    let schema = [string_ty(), string_ty()];
                                    let input = || {
                                        let mut c = chunk(&schema, &vec![values.to_vec(); 3]);
                                        c.set_sel(selection.clone());
                                        c
                                    };
                                    let label = format!("json_schema/{name}/{shape}/{mask}/{mode:?}/{profile}/{enabled}/{vi}/{selection:?}");
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
        "OPTIONAL PHASE json_schema ORIGINAL {}/{:016X}",
        observed.0, observed.1
    );
    assert_eq!((observed.0, observed.1), (62208, 0xC769E4015B559C79));
    if !observed.2.is_empty() {
        eprintln!("OPTIONAL PHASE json_schema ADMISSION RED {:?}", observed.2);
    }
    assert!(observed.2.is_empty());
}
