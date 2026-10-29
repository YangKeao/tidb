// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Complete source conformance against public one-shot and prepared entrypoints.
//! No test or production route retries through the old evaluator.
use super::*;
use std::cell::RefCell;
use std::panic::{catch_unwind, AssertUnwindSafe};
use tidb_datatype::{Datum, Decimal, FieldType, FieldTypeCode};

const VIEWS: [View; 9] = [
    View::Int,
    View::Decimal,
    View::Json,
    View::Real,
    View::Duration,
    View::Time,
    View::Bytes,
    View::Datum,
    View::FoldedInt,
];
fn public(e: &LegacyEvaluator<'_>, root: &SimpleExpr, view: View) -> String {
    match view {
        View::Int => format!("{:?}", e.eval_expr(root)),
        View::Decimal => format!("{:?}", e.eval_decimal(Some(root))),
        View::Json => format!("{:?}", e.eval_json(Some(root))),
        View::Real => format!("{:?}", e.eval_real(Some(root)).map(|v| v.map(f64::to_bits))),
        View::Duration => format!("{:?}", e.eval_duration(Some(root))),
        View::Time => format!("{:?}", e.eval_time(Some(root))),
        View::Bytes => format!("{:?}", e.eval_bytes(Some(root))),
        View::Datum => format!("{:?}", e.eval_datum(root)),
        View::FoldedInt => format!(
            "{:?}",
            fold_legacy_sql(e.eval_expr(root)).map(Option::flatten)
        ),
    }
}
fn actual(result: LegacyResult<Value>) -> String {
    match result {
        Err(error) => format!("Err({error:?})"),
        Ok(Value::Int(v)) => format!("Ok({v:?})"),
        Ok(Value::Decimal(v)) => format!("Ok({v:?})"),
        Ok(Value::Json(v)) => format!("Ok({v:?})"),
        Ok(Value::Real(v)) => format!("Ok({:?})", v.map(f64::to_bits)),
        Ok(Value::Duration(v)) => format!("Ok({v:?})"),
        Ok(Value::Time(v)) => format!("Ok({v:?})"),
        Ok(Value::Bytes(v)) => format!("Ok({v:?})"),
        Ok(Value::Datum(v)) => format!("Ok({v:?})"),
    }
}
fn observed(body: impl FnOnce() -> String) -> String {
    match catch_unwind(AssertUnwindSafe(body)) {
        Ok(v) => v,
        Err(p) => format!(
            "panic:{:?}",
            p.downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| p.downcast_ref::<&str>().copied())
                .unwrap_or("non-string panic")
        ),
    }
}
fn assert_candidate(plan: &mut Plan<'_>, e: &LegacyEvaluator<'_>) -> String {
    let result = observed(|| actual(plan.evaluate(e)));
    result
}

#[test]
fn legacy_frame_candidate_keeps_leaf_views_and_integer_kernel_profiles() {
    let zone = tidb_datatype::SessionTimeZone::utc();
    let rows = [
        vec![],
        vec![Datum::Int(-9)],
        vec![Datum::UInt(u64::MAX)],
        vec![Datum::Null],
        vec![Datum::Bytes(vec![0xff, 7])],
        vec![Datum::Real(f64::from_bits(0x7ff8_0000_0000_0017))],
        vec![Datum::Decimal(Decimal::parse_mysql("1.2500").0)],
    ];
    let mut roots = vec![
        SimpleExpr::Null,
        SimpleExpr::Int(7),
        SimpleExpr::Column(0),
        SimpleExpr::Column(99),
        SimpleExpr::Bytes(vec![0xff, 9]),
        SimpleExpr::Decimal(Decimal::parse_mysql("-1.20").0),
        SimpleExpr::Real(-0.0),
        SimpleExpr::Json(
            tidb_datatype::BinaryJSON::from_typed_value(&tidb_datatype::BinaryJSONValue::Int64(3))
                .unwrap(),
        ),
    ];
    use SimpleSig::*;
    let signatures = [
        PlusInt,
        PlusIntUnsignedUnsigned,
        PlusIntUnsignedSigned,
        PlusIntSignedUnsigned,
        PlusIntSignedSigned,
        MinusInt,
        MinusIntUnsignedUnsigned,
        MinusIntUnsignedSigned,
        MinusIntSignedUnsigned,
        MinusIntSignedSigned,
        MinusIntForcedUnsignedUnsigned,
        MinusIntForcedUnsignedSigned,
        MinusIntForcedSignedUnsigned,
        MultiplyInt,
        MultiplyIntUnsigned,
        ModIntUnsignedUnsigned,
        ModIntUnsignedSigned,
        ModIntSignedUnsigned,
        ModIntSignedSigned,
        IntDivideInt,
        IntDivideIntUnsignedUnsigned,
        IntDivideIntUnsignedSigned,
        IntDivideIntSignedSigned,
        IntDivideIntSignedUnsigned,
        LtInt,
        LeInt,
        GtInt,
        GeInt,
        EqInt,
        NeInt,
        LogicalAnd,
        LogicalOr,
        CastIntAsInt,
        UnaryNot,
        IntIsNull,
        RoundInt,
    ];
    for signature in signatures {
        for children in [
            vec![],
            vec![SimpleExpr::Null],
            vec![SimpleExpr::Column(0), SimpleExpr::Int(0)],
            vec![
                SimpleExpr::Int(i64::MAX),
                SimpleExpr::Int(9),
                SimpleExpr::Column(99),
            ],
            vec![
                SimpleExpr::Func(PlusInt, vec![SimpleExpr::Int(-2), SimpleExpr::Column(0)]),
                SimpleExpr::Int(-1),
            ],
        ] {
            roots.push(SimpleExpr::Func(signature, children));
        }
    }
    let mut records = 0;
    for root in &roots {
        for view in VIEWS {
            for row in &rows {
                let mut plan = match Plan::prepare(root, view, row.len()) {
                    Ok(plan) => plan,
                    Err(error) => panic!("{}", error.message()),
                };
                let original = LegacyEvaluator::new(row, 4, &zone);
                let candidate = LegacyEvaluator::new(row, 4, &zone);
                for _ in 0..2 {
                    let expected = observed(|| public(&original, root, view));
                    assert_eq!(
                        assert_candidate(&mut plan, &candidate),
                        expected,
                        "{view:?}/{root:?}/{row:?}"
                    );
                    records += 1;
                }
            }
        }
    }
    assert!(records > 5_000);
    eprintln!("N65 candidate leaf/integer records {records}");
    let member = SimpleExpr::Func(JsonMemberOfSig, vec![SimpleExpr::Int(1)]);
    assert!(Plan::prepare(&member, View::Int, 0).is_ok());
}

struct Context {
    cache: tidb_expr::ReadyValueCache,
    cache_mode: usize,
    failure: usize,
    error: tidb_expr::EvalError,
    events: RefCell<Vec<String>>,
}
impl tidb_expr::Columns for Context {
    fn get(&self, _: &[String]) -> Option<Datum> {
        None
    }
    fn ready_value_cache(&self) -> Option<&tidb_expr::ReadyValueCache> {
        self.events.borrow_mut().push("cache".into());
        match self.cache_mode {
            0 => Some(&self.cache),
            1 => None,
            _ => panic!("actual retired candidate context"),
        }
    }
    fn param_value(&self, index: usize) -> Result<Datum, tidb_expr::EvalError> {
        self.events.borrow_mut().push(format!("p{index}"));
        match (index, self.failure) {
            (0, 1) => Err(tidb_expr::EvalError::Unsupported(
                "actual candidate SQL producer",
            )),
            (0, 2) => Err(self.error.clone()),
            (0, 3) => panic!("actual candidate callback panic"),
            _ => Ok(Datum::Int(index as i64 + 7)),
        }
    }
}
fn field() -> FieldType {
    FieldType::new(FieldTypeCode::LongLong)
}
fn parameter(index: i64) -> tidb_expr::expression::Expression {
    let mut c = tidb_expr::constant::Constant::new(Datum::Int(2), field());
    c.param_marker = Some(tidb_expr::constant::ParamMarker { order: index });
    tidb_expr::expression::Expression::Constant(c)
}
fn native(expr: tidb_expr::expression::Expression) -> SimpleExpr {
    SimpleExpr::Shared(Arc::new(SharedExpression {
        expression: Arc::new(expr),
        context: Arc::new(RequestEvalContext::new(
            tidb_datatype::SessionTimeZone::utc(),
            4,
            0,
        )),
    }))
}
fn cause() -> tidb_expr::EvalError {
    let cache = tidb_expr::ReadyValueCache::new();
    assert!(catch_unwind(AssertUnwindSafe(
        || cache.with_columns(&tidb_expr::NoColumns, |_| panic!("actual error producer"))
    ))
    .is_err());
    cache
        .with_columns(&tidb_expr::NoColumns, |ctx| tidb_expr::eval_pi_in(ctx))
        .unwrap_err()
}

#[test]
fn legacy_frame_candidate_composes_shared_errors_cache_scopes_and_reuse() {
    let zone = tidb_datatype::SessionTimeZone::utc();
    let row = [Datum::Int(3)];
    let error = cause();
    let mut roots = vec![native(parameter(0))];
    for sig in [
        SimpleSig::PlusInt,
        SimpleSig::IntDivideInt,
        SimpleSig::ModIntSignedSigned,
        SimpleSig::LogicalAnd,
        SimpleSig::LogicalOr,
        SimpleSig::EqInt,
        SimpleSig::RoundInt,
    ] {
        roots.push(SimpleExpr::Func(
            sig,
            vec![native(parameter(0)), native(parameter(1))],
        ));
    }
    let mut deep = parameter(0);
    for _ in 0..1100 {
        deep = tidb_expr::expression::Expression::ScalarFunction(
            tidb_expr::scalar_function::ScalarFunction::new(
                tidb_ast::CiString::new("abs"),
                field(),
                vec![deep],
            ),
        );
    }
    roots.push(SimpleExpr::Func(
        SimpleSig::PlusInt,
        vec![native(deep), native(parameter(1))],
    ));
    let mut records = 0;
    for root in &roots {
        for view in VIEWS {
            let mut plan = match Plan::prepare(root, view, row.len()) {
                Ok(plan) => plan,
                Err(error) => panic!("{}", error.message()),
            };
            for cache_mode in 0..3 {
                for failure in 0..4 {
                    let context = || Context {
                        cache: tidb_expr::ReadyValueCache::new(),
                        cache_mode,
                        failure,
                        error: error.clone(),
                        events: RefCell::new(vec![]),
                    };
                    let old_context = context();
                    let new_context = context();
                    let mut original = LegacyEvaluator::new(&row, 4, &zone);
                    original.raw_columns = &old_context;
                    original.shared_override = Some(&old_context);
                    let mut candidate = LegacyEvaluator::new(&row, 4, &zone);
                    candidate.raw_columns = &new_context;
                    candidate.shared_override = Some(&new_context);
                    for _ in 0..2 {
                        let expected = observed(|| public(&original, root, view));
                        let value = assert_candidate(&mut plan, &candidate);
                        assert_eq!(value, expected, "{view:?}/{cache_mode}/{failure}");
                        assert_eq!(
                            *new_context.events.borrow(),
                            *old_context.events.borrow(),
                            "native demand trace {view:?}/{cache_mode}/{failure}"
                        );
                        records += 1;
                    }
                }
            }
        }
    }
    assert!(records >= 500);
    eprintln!("N65 candidate Shared/scope records {records}");
}

#[test]
fn legacy_frame_prepared_signature_views_match_public_matrix() {
    use super::super::legacy_source_tests as original;
    let mut records = 0usize;
    for signature in original::signatures() {
        for shape in 0..6 {
            let root = SimpleExpr::Func(signature, original::children(shape));
            for row in original::rows() {
                for (precision, zone) in [
                    (0, tidb_datatype::SessionTimeZone::utc()),
                    (
                        4,
                        tidb_datatype::SessionTimeZone::Named(chrono_tz::America::New_York),
                    ),
                    (
                        30,
                        tidb_datatype::SessionTimeZone::Named(chrono_tz::Asia::Shanghai),
                    ),
                ] {
                    for view in VIEWS {
                        let mut plan = match Plan::prepare(&root, view, row.len()) {
                            Ok(plan) => plan,
                            Err(error) => panic!(
                                "candidate preparation {signature:?}/{shape}/{view:?}: {}",
                                error.message()
                            ),
                        };
                        let old_context = original::Context::primary();
                        let new_context = original::Context::primary();
                        let mut old_e = LegacyEvaluator::new(&row, precision, &zone);
                        let mut new_e = LegacyEvaluator::new(&row, precision, &zone);
                        old_e.raw_columns = &old_context;
                        new_e.raw_columns = &new_context;
                        for reuse in 0..2 {
                            let expected = observed(|| public(&old_e, &root, view));
                            let actual = assert_candidate(&mut plan, &new_e);
                            assert_eq!(actual, expected, "{signature:?}/{shape}/{row:?}/{precision}/{zone:?}/{view:?}/{reuse}");
                            assert_eq!(
                                new_context.trace(),
                                old_context.trace(),
                                "trace {signature:?}/{shape}/{row:?}/{precision}/{view:?}/{reuse}"
                            );
                            records += 1;
                        }
                    }
                }
            }
        }
    }
    assert!(records > 100_000);
    eprintln!(
        "N65 admitted candidate records {records}; explicit unimplemented preparation cells 0"
    );
}

#[test]
fn legacy_frame_candidate_matches_entire_fixed_scope_cohort() {
    use super::super::legacy_scope_tests as original;
    let row = [Datum::Int(9)];
    let zone = tidb_datatype::SessionTimeZone::Named(chrono_tz::America::New_York);
    let mut records = 0;
    for text in [false, true] {
        for (label, root) in original::roots(text) {
            for cache in 0..3 {
                for failure in 0..4 {
                    for precision in [-1, 0, 4, 30] {
                        for view in VIEWS.into_iter().filter(|v| *v != View::FoldedInt) {
                            let old_context =
                                original::ScopeContext::candidate_fixture(cache, failure, text);
                            let new_context =
                                original::ScopeContext::candidate_fixture(cache, failure, text);
                            let mut old_e = LegacyEvaluator::new(&row, precision, &zone);
                            old_e.raw_columns = &old_context;
                            old_e.shared_override = Some(&old_context);
                            let mut new_e = LegacyEvaluator::new(&row, precision, &zone);
                            new_e.raw_columns = &new_context;
                            new_e.shared_override = Some(&new_context);
                            let mut plan = Plan::prepare(&root, view, row.len())
                                .unwrap_or_else(|e| panic!("{label}/{view:?}: {}", e.message()));
                            for reuse in 0..2 {
                                let expected = observed(|| public(&old_e, &root, view));
                                let actual = assert_candidate(&mut plan, &new_e);
                                assert_eq!(
                                    actual, expected,
                                    "{text}/{label}/{cache}/{failure}/{precision}/{view:?}/{reuse}"
                                );
                                assert_eq!(new_context.trace(), old_context.trace(), "trace {text}/{label}/{cache}/{failure}/{precision}/{view:?}/{reuse}");
                                records += 1;
                            }
                        }
                    }
                }
            }
        }
    }
    assert_eq!(records, 66_048);
}

#[test]
fn legacy_frame_prepared_root_reuses_rows_and_allows_parameter_reentry() {
    struct Reentrant<'a> {
        callback: RefCell<Option<&'a dyn Fn()>>,
    }
    impl tidb_expr::Columns for Reentrant<'_> {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn param_value(&self, _: usize) -> Result<Datum, tidb_expr::EvalError> {
            let callback = self.callback.borrow_mut().take();
            if let Some(callback) = callback {
                callback();
            }
            Ok(Datum::Int(7))
        }
    }
    let zone = tidb_datatype::SessionTimeZone::utc();
    let source = SimpleExpr::Func(
        SimpleSig::PlusInt,
        vec![native(parameter(0)), SimpleExpr::Column(0)],
    );
    let prepared = PreparedRoot::new(&source, View::Int);
    let context = Reentrant {
        callback: RefCell::new(None),
    };
    let nested = || {
        let row = [Datum::Int(10)];
        let mut e = LegacyEvaluator::new(&row, 4, &zone);
        e.raw_columns = &context;
        e.shared_override = Some(&context);
        assert_eq!(prepared.integer(&e).unwrap(), Some(17));
    };
    *context.callback.borrow_mut() = Some(&nested);
    for v in [2, 9, -5] {
        let row = [Datum::Int(v)];
        let mut e = LegacyEvaluator::new(&row, 4, &zone);
        e.raw_columns = &context;
        e.shared_override = Some(&context);

        assert_eq!(prepared.integer(&e).unwrap(), Some(i128::from(v + 7)));
    }
}
