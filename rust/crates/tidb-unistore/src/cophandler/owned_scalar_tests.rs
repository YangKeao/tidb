// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Genuine PB/shared-expression request observations before owning-plan reuse.
use super::*;
use tidb_datatype::{Datum, FieldType, FieldTypeCode};

fn field(code: i32) -> tipb::FieldType {
    tipb::FieldType {
        tp: Some(code),
        flen: Some(if code == 253 { 128 } else { 20 }),
        decimal: Some(if code == 5 { -1 } else { 0 }),
        collate: Some(46),
        charset: Some("utf8mb4".into()),
        ..Default::default()
    }
}
fn column(index: i64, code: i32) -> tipb::Expr {
    let mut val = vec![];
    tidb_codec::encode_int(&mut val, index);
    tipb::Expr {
        tp: Some(tipb::ExprType::ColumnRef as i32),
        val: Some(val),
        field_type: Some(field(code)),
        ..Default::default()
    }
}
fn integer(value: i64) -> tipb::Expr {
    let mut val = vec![];
    tidb_codec::encode_int(&mut val, value);
    tipb::Expr {
        tp: Some(tipb::ExprType::Int64 as i32),
        val: Some(val),
        field_type: Some(field(8)),
        ..Default::default()
    }
}
fn text(value: &str) -> tipb::Expr {
    tipb::Expr {
        tp: Some(tipb::ExprType::String as i32),
        val: Some(value.as_bytes().to_vec()),
        field_type: Some(field(253)),
        ..Default::default()
    }
}
fn call(sig: tipb::ScalarFuncSig, children: Vec<tipb::Expr>, code: i32) -> tipb::Expr {
    tipb::Expr {
        tp: Some(tipb::ExprType::ScalarFunc as i32),
        sig: Some(sig as i32),
        children,
        field_type: Some(field(code)),
        ..Default::default()
    }
}
fn expressions() -> Vec<tipb::Expr> {
    use tipb::ScalarFuncSig as S;
    vec![
        call(S::PlusInt, vec![column(0, 8), integer(2)], 8),
        call(S::AbsInt, vec![column(0, 8)], 8),
        call(S::CoalesceInt, vec![column(0, 8), integer(7)], 8),
        call(S::IfInt, vec![column(0, 8), integer(11), integer(22)], 8),
        call(S::CastStringAsInt, vec![column(2, 253)], 8),
        call(S::CastIntAsReal, vec![column(0, 8)], 5),
        call(S::CastRealAsInt, vec![column(1, 5)], 8),
        call(S::CastStringAsTime, vec![column(2, 253)], 7),
        call(S::RegexpLikeSig, vec![column(2, 253), text("a+")], 8),
    ]
}
fn rows() -> Vec<Vec<Datum>> {
    vec![
        vec![Datum::Int(-1), Datum::Real(1.5), Datum::new_string("12")],
        vec![Datum::Int(0), Datum::Real(-2.5), Datum::new_string("bad")],
        vec![Datum::Null, Datum::Null, Datum::Null],
        vec![
            Datum::Int(i64::MAX),
            Datum::Real(f64::NAN),
            Datum::Bytes(vec![0xff]),
        ],
        vec![
            Datum::Int(i64::MIN),
            Datum::Real(-0.0),
            Datum::new_string("2024-03-10 01:30:00"),
        ],
        vec![
            Datum::Int(7),
            Datum::Real(f64::INFINITY),
            Datum::new_string("aaaa"),
        ],
        vec![Datum::Int(1), Datum::Real(2.5), Datum::new_string("")],
        vec![
            Datum::Int(3),
            Datum::Real(3.0),
            Datum::new_string("2024-03-10 02:30:00"),
        ],
    ]
}
fn records() -> Vec<String> {
    use tidb_model::flags::*;
    let mut records = vec![];
    for (kind, wire) in expressions().into_iter().enumerate() {
        for optional in [false, true] {
            for (zi, zone) in [
                tidb_datatype::SessionTimeZone::utc(),
                tidb_datatype::SessionTimeZone::Named(chrono_tz::Asia::Shanghai),
                tidb_datatype::SessionTimeZone::Named(chrono_tz::America::New_York),
            ]
            .into_iter()
            .enumerate()
            {
                for (fi, flags) in [
                    0,
                    FLAG_TRUNCATE_AS_WARNING,
                    FLAG_IGNORE_TRUNCATE,
                    FLAG_DIVIDED_BY_ZERO_AS_WARNING,
                    FLAG_IN_INSERT_STMT,
                ]
                .into_iter()
                .enumerate()
                {
                    for precision in [0, 4, 8] {
                        let mut context = RequestEvalContext::new(zone.clone(), precision, flags);
                        context.column_types = vec![
                            FieldType::new(FieldTypeCode::LongLong),
                            FieldType::new(FieldTypeCode::Double),
                            FieldType::new(FieldTypeCode::VarString)
                                .with_collation(tidb_datatype::Collation::Utf8Mb4Bin),
                        ];
                        let context = Arc::new(context);
                        let mut wire = wire.clone();
                        if optional {
                            wire.field_type = None;
                        }
                        let converted = convert_expr_with_context(&wire, &context);
                        let expr = match converted {
                            Ok(SimpleExpr::Shared(shared)) => shared,
                            Ok(_) => panic!(
                                "catalog fixture {kind} did not use genuine SharedExpression"
                            ),
                            Err(error) => {
                                records.push(format!(
                                    "{kind}/{optional}/{zi}/{fi}/{precision}|CONVERT:{error}"
                                ));
                                continue;
                            }
                        };
                        let ready_values = std::rc::Rc::new(tidb_expr::ReadyValueCache::new());
                        for repeat in 0..2 {
                            for (ri, row) in rows().iter().enumerate() {
                                // The real scan constructs this outer evaluator for each row.
                                let evaluator = LegacyEvaluator::with_ready_values(
                                    row,
                                    precision as i64,
                                    &zone,
                                    std::rc::Rc::clone(&ready_values),
                                );
                                let result =
                                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                        evaluator.eval_shared(&expr)
                                    }));
                                let result = match result {
                                    Ok(value) => format!("{value:?}"),
                                    Err(payload) => format!(
                                        "PANIC:{:?}",
                                        payload
                                            .downcast_ref::<String>()
                                            .map(String::as_str)
                                            .or_else(|| payload.downcast_ref::<&str>().copied())
                                    ),
                                };
                                records.push(format!("{kind}/{optional}/{zi}/{fi}/{precision}/{repeat}/{ri}|{result}|warnings:{:?}",context.take_warnings()));
                            }
                        }
                    }
                }
            }
        }
    }
    records
}
fn digest(records: &[String]) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for record in records {
        for byte in record.bytes().chain([b'\n']) {
            hash = (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3);
        }
    }
    hash
}
#[test]
fn owned_unistore_original_request_observations() {
    let records = records();
    println!(
        "N56 unistore original count={} digest={:016X}",
        records.len(),
        digest(&records)
    );
    assert_eq!(records.len(), 10_260);
    assert_eq!(digest(&records), 0x51B0824478C18677);
}

fn simple_shared() -> (Arc<SharedExpression>, Arc<RequestEvalContext>) {
    let mut context = RequestEvalContext::new(tidb_datatype::SessionTimeZone::utc(), 4, 0);
    context.column_types = vec![FieldType::new(FieldTypeCode::LongLong)];
    let context = Arc::new(context);
    let wire = call(
        tipb::ScalarFuncSig::PlusInt,
        vec![column(0, 8), integer(2)],
        8,
    );
    let SimpleExpr::Shared(shared) = convert_expr_with_context(&wire, &context).unwrap() else {
        panic!("real shared PB");
    };
    (shared, context)
}
#[test]
fn owned_shared_clone_keeps_the_original_deep_expression_contract() {
    let (shared, context) = simple_shared();
    let alias = Arc::clone(&shared);
    assert!(Arc::ptr_eq(&shared.expression, &alias.expression));
    let mut copied = shared.as_ref().clone();
    assert!(!Arc::ptr_eq(&shared.expression, &copied.expression));
    assert!(Arc::ptr_eq(&context, &copied.context));
    let changed = Arc::get_mut(&mut copied.expression).expect("deep clone has an independent root");
    let tidb_expr::expression::Expression::ScalarFunction(function) = changed else {
        panic!("plus");
    };
    function.args[1] = tidb_expr::expression::Expression::Constant(
        tidb_expr::constant::Constant::new(Datum::Int(4), FieldType::new(FieldTypeCode::LongLong)),
    );
    let zone = tidb_datatype::SessionTimeZone::utc();
    let row = [Datum::Int(3)];
    let evaluator = LegacyEvaluator::new(&row, 4, &zone);
    assert_eq!(evaluator.eval_shared(&shared).unwrap(), Datum::Int(5));
    assert_eq!(evaluator.eval_shared(&copied).unwrap(), Datum::Int(7));
}
#[test]
fn owned_request_plan_survives_each_temporary_row_evaluator() {
    let (shared, _context) = simple_shared();
    let ready = std::rc::Rc::new(tidb_expr::ReadyValueCache::new());
    let zone = tidb_datatype::SessionTimeZone::utc();
    for value in 0..8 {
        let row = [Datum::Int(value)];
        let evaluator =
            LegacyEvaluator::with_ready_values(&row, 4, &zone, std::rc::Rc::clone(&ready));
        assert_eq!(
            evaluator.eval_shared(&shared).unwrap(),
            Datum::Int(value + 2)
        );
    }
    assert_eq!(
        Arc::strong_count(&shared.expression),
        2,
        "request lane did not keep the genuine expression alive for reuse"
    );
    let weak = Arc::downgrade(&shared.expression);
    drop(shared);
    assert!(weak.upgrade().is_some());
    drop(ready);
    assert!(weak.upgrade().is_none());
}

#[test]
fn owned_request_reuse_matches_uncached_shared_and_reports_timing() {
    fn uncached(evaluator: &LegacyEvaluator<'_>, expr: &SharedExpression) -> LegacyResult<Datum> {
        // Exact pre-cache eval_shared path: same ingested tree, actual row,
        // current Columns binding and SDK; only preparation reuse differs.
        let row = tidb_chunk::mutrow::MutRow::from_datums(evaluator.row);
        let columns: &dyn tidb_expr::Columns = expr.context.as_ref();
        let columns = evaluator.shared_override.unwrap_or(columns);
        evaluator
            .ready_values
            .with_columns(columns, |bound| expr.expression.eval(bound, row.to_row()))
            .map_err(LegacyEvalError::from)
    }
    for rows in [32, 1024, 8192] {
        let (shared, _context) = simple_shared();
        let zone = tidb_datatype::SessionTimeZone::utc();
        let run = |optimized| {
            let ready = std::rc::Rc::new(tidb_expr::ReadyValueCache::new());
            let mut values = Vec::with_capacity(rows);
            for physical in 0..rows {
                let row = [Datum::Int(physical as i64)];
                let evaluator =
                    LegacyEvaluator::with_ready_values(&row, 4, &zone, std::rc::Rc::clone(&ready));
                values.push(
                    if optimized {
                        evaluator.eval_shared(&shared)
                    } else {
                        uncached(&evaluator, &shared)
                    }
                    .unwrap(),
                );
            }
            values
        };
        let expected: Vec<_> = (0..rows).map(|n| Datum::Int(n as i64 + 2)).collect();
        assert_eq!(run(false), expected);
        assert_eq!(run(true), expected);
        let mut baseline_ns = vec![];
        let mut prepared_ns = vec![];
        for round in 0..7 {
            for optimized in if round % 2 == 0 {
                [false, true]
            } else {
                [true, false]
            } {
                let start = std::time::Instant::now();
                let actual = run(optimized);
                std::hint::black_box(&actual);
                let elapsed = start.elapsed().as_nanos();
                assert_eq!(actual, expected);
                if optimized {
                    prepared_ns.push(elapsed);
                } else {
                    baseline_ns.push(elapsed);
                }
            }
        }
        println!("N56 actual unistore rows={rows} baseline_ns={baseline_ns:?} prepared_ns={prepared_ns:?}");
    }
}
