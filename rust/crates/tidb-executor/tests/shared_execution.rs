//! Shared expression execution through the statement context and SQL driver.

#![cfg(feature = "shared-kernel")]

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tidb_datatype::Datum;
use tidb_executor::StmtContext;
use tidb_expr::{Columns, MysqlRng};

#[test]
fn shared_execution_borrows_real_statement_effects() {
    use tidb_ast::CiString;
    use tidb_chunk::chunk::Chunk;
    use tidb_datatype::{Decimal, FieldType, FieldTypeCode};
    use tidb_expr::{
        constant::{Constant, ParamMarker},
        expression::Expression,
        scalar_function::ScalarFunction,
        shared_kernel::SharedProjection,
    };
    let integer = FieldType::new(FieldTypeCode::LongLong);
    let decimal = FieldType::new(FieldTypeCode::NewDecimal);
    let real = FieldType::new(FieldTypeCode::Double);
    let text = FieldType::new(FieldTypeCode::VarString);
    let literal = |value, ty: &FieldType| Expression::Constant(Constant::new(value, ty.clone()));
    let call = |name, ty: &FieldType, args| {
        Expression::ScalarFunction(ScalarFunction::new(CiString::new(name), ty.clone(), args))
    };
    let mut parameter = Constant::new(Datum::Null, decimal.clone());
    parameter.param_marker = Some(ParamMarker { order: 0 });
    let projection = SharedProjection::compile(&[
        call("rand", &real, vec![literal(Datum::Int(7), &integer)]),
        call("rand", &real, vec![]),
        call(
            "mul",
            &decimal,
            vec![
                Expression::Constant(parameter),
                literal(Datum::Decimal(Decimal::from_int(1)), &decimal),
            ],
        ),
        call(
            "setvar",
            &text,
            vec![
                literal(Datum::new_string(b"state".to_vec()), &text),
                literal(Datum::new_string(b"stored".to_vec()), &text),
            ],
        ),
    ])
    .unwrap();
    let vars = Arc::new(Mutex::new(HashMap::new()));
    let rng = Arc::new(MysqlRng::new_with_seed(1));
    let context = StmtContext::for_query()
        .with_rand_session(Arc::clone(&rng))
        .with_user_vars(Arc::clone(&vars))
        .with_prepared_params(Arc::from([Datum::Int(42)]));
    let mut execution = projection.start_execution(&context);
    let seeded = MysqlRng::new_with_seed(7);
    let unseeded = MysqlRng::new_with_seed(1);
    for _ in 0..2 {
        let mut input = Chunk::new_empty(&[]);
        input.set_num_virtual_rows(1);
        let result = execution
            .evaluate(&input, &[0], &mut Default::default())
            .unwrap();
        assert_eq!(
            result,
            vec![
                vec![Datum::Real(seeded.gen())],
                vec![Datum::Real(unseeded.gen())],
                vec![Datum::Decimal(Decimal::from_int(42))],
                vec![Datum::new_string(b"stored".to_vec())]
            ]
        );
        assert_eq!(
            vars.lock().unwrap().get("state"),
            Some(&Datum::new_string(b"stored".to_vec()))
        );
    }
    let mut input = Chunk::new_empty(&[]);
    input.set_num_virtual_rows(1);
    let result = projection
        .start_execution(&context)
        .evaluate(&input, &[0], &mut Default::default())
        .unwrap();
    assert_eq!(
        result[0],
        vec![Datum::Real(MysqlRng::new_with_seed(7).gen())]
    );
    assert_eq!(result[1], vec![Datum::Real(unseeded.gen())]);
    assert_eq!(context.param_value(0).unwrap(), Datum::Int(42));
    assert_eq!(rng.gen(), unseeded.gen());
}

#[test]
fn shared_sql_projection_keeps_state_across_batches() {
    use tidb_ast::Stmt;
    use tidb_executor::driver::open_query_meta_stmt_with_physical;
    use tidb_executor::{
        plan_query_meta_stmt, run_create_table_on, run_insert_on, run_select_meta_on, Catalog,
        DEFAULT_DATABASE,
    };

    let mut catalog = Catalog::default();
    let setup = StmtContext::for_query();
    run_create_table_on(
        "CREATE TABLE samples (n BIGINT PRIMARY KEY, v DOUBLE)",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO samples VALUES (1,1),(2,2),(3,3),(4,4),(5,5)",
        &mut catalog,
        &setup,
    )
    .unwrap();
    let sql = "SELECT RAND(7), RAND(), n + 1, v / 0e0 FROM samples ORDER BY n LIMIT 5";
    let context = |rng| {
        StmtContext::for_query()
            .with_executor_chunk_sizes(2, 8)
            .with_rand_session(rng)
    };
    let native = context(Arc::new(MysqlRng::new_with_seed(1)));
    let expected = run_select_meta_on(sql, &catalog, &native).unwrap();
    let warnings = native.take_warnings();
    assert_eq!(warnings.len(), 5);
    assert!(warnings.iter().all(|(_, code, _)| *code == 1365));
    let shared_rng = Arc::new(MysqlRng::new_with_seed(1));
    let unseeded = MysqlRng::new_with_seed(1);
    let Stmt::Query(query) = tidb_parser::parse(sql).unwrap() else {
        unreachable!();
    };
    let plan = plan_query_meta_stmt(&query, &catalog, DEFAULT_DATABASE, &native).unwrap();
    eprintln!("success physical plan: {plan:#?}");

    for attempt in 0..2 {
        let shared = context(Arc::clone(&shared_rng)).with_shared_projection();
        let mut records =
            open_query_meta_stmt_with_physical(&query, None, &catalog, DEFAULT_DATABASE, &shared)
                .unwrap();
        assert_eq!(records.columns(), expected.0);
        let types: Vec<_> = records.columns().iter().map(|(_, tp)| tp.clone()).collect();
        let seeded = MysqlRng::new_with_seed(7);
        let mut req = records.new_chunk();
        let mut rows = Vec::new();
        let mut batches = Vec::new();
        loop {
            req.set_required_rows(2, 8);
            records.next(&mut req).unwrap();
            if req.num_rows() == 0 {
                break;
            }
            batches.push(req.num_rows());
            for row in 0..req.num_rows() {
                let actual = req.get_row(row).get_datum_row(&types);
                let original = &expected.1[rows.len()];
                assert_eq!(actual[0], Datum::Real(seeded.gen()));
                assert_eq!(actual[1], Datum::Real(unseeded.gen()));
                assert_eq!(actual[0], original[0]);
                if attempt == 0 {
                    assert_eq!(actual[1], original[1]);
                }
                assert_eq!(&actual[2..], &original[2..]);
                rows.push(actual);
            }
        }
        records.finish().unwrap();
        eprintln!(
            "execution {attempt}: batches {batches:?}, route {:?}",
            shared.shared_projection_counts()
        );
        assert_eq!(rows.len(), 5);
        assert!(batches.len() >= 2);
        assert_eq!(
            shared.shared_projection_counts(),
            Some((1, batches.len() as u64))
        );
        assert_eq!(shared.take_warnings(), warnings);
    }
    assert!(native.shared_projection_counts().is_none());

    let parallel = StmtContext::for_query()
        .with_executor_chunk_sizes(2, 2)
        .with_shared_projection();
    let sql = "SELECT RAND(7) FROM samples";
    let Stmt::Query(query) = tidb_parser::parse(sql).unwrap() else {
        unreachable!();
    };
    let plan = plan_query_meta_stmt(&query, &catalog, DEFAULT_DATABASE, &parallel).unwrap();
    assert!(matches!(
        plan,
        tidb_planner::physical::PhysicalPlan::Projection(_)
    ));
    assert!(plan.stats_count().unwrap() >= 2.0);
    assert!(parallel.projection_concurrency() > 0);
    assert_eq!(
        run_select_meta_on(sql, &catalog, &parallel)
            .unwrap()
            .1
            .len(),
        5
    );
    assert_eq!(parallel.shared_projection_counts(), Some((0, 0)));
}

#[test]
fn shared_sql_projection_preserves_failure_and_declines_whole_groups() {
    use tidb_ast::Stmt;
    use tidb_executor::{
        plan_query_meta_stmt, run_create_table_on, run_insert_on, run_select_meta_on, Catalog,
        DEFAULT_DATABASE,
    };

    let mut catalog = Catalog::default();
    let setup = StmtContext::for_query();
    run_create_table_on(
        "CREATE TABLE limits_case (n BIGINT PRIMARY KEY)",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO limits_case VALUES (9223372036854775807)",
        &mut catalog,
        &setup,
    )
    .unwrap();
    for sql in [
        "SELECT RAND(), n + 1 FROM limits_case LIMIT 1",
        "SELECT RAND(), n * 2 FROM limits_case LIMIT 1",
        "SELECT RAND(), (n + 0) + 1 FROM limits_case LIMIT 1",
    ] {
        let Stmt::Query(query) = tidb_parser::parse(sql).unwrap() else {
            unreachable!();
        };
        let plan = plan_query_meta_stmt(&query, &catalog, DEFAULT_DATABASE, &setup).unwrap();
        eprintln!("failure physical plan: {plan:#?}");

        let native_rng = Arc::new(MysqlRng::new_with_seed(1));
        let shared_rng = Arc::new(MysqlRng::new_with_seed(1));
        let native = StmtContext::for_query().with_rand_session(Arc::clone(&native_rng));
        let shared = StmtContext::for_query()
            .with_rand_session(Arc::clone(&shared_rng))
            .with_shared_projection();
        let expected = run_select_meta_on(sql, &catalog, &native)
            .unwrap_err()
            .to_mysql_error();
        let actual = run_select_meta_on(sql, &catalog, &shared)
            .unwrap_err()
            .to_mysql_error();
        eprintln!(
            "failure: {actual:?}, route {:?}",
            shared.shared_projection_counts()
        );
        assert_eq!(shared.shared_projection_counts(), Some((1, 1)));
        assert_eq!((actual.code, actual.state), (1690, *b"22003"));
        assert_eq!(actual, expected);
        assert!(actual.is_from_evaluation());
        let rng = MysqlRng::new_with_seed(1);
        rng.gen();
        let next = rng.gen();
        assert_eq!(native_rng.gen(), next);
        assert_eq!(shared_rng.gen(), next);
        assert!(native.take_warnings().is_empty());
        assert!(shared.take_warnings().is_empty());
    }

    for sql in [
        "SELECT RAND(7), ABS(n) FROM limits_case LIMIT 1",
        "SELECT RAND(7), n FROM limits_case LIMIT 1",
    ] {
        let shared = StmtContext::for_query().with_shared_projection();
        assert_eq!(
            run_select_meta_on(sql, &catalog, &shared).unwrap(),
            run_select_meta_on(sql, &catalog, &StmtContext::for_query()).unwrap()
        );
        assert_eq!(shared.shared_projection_counts(), Some((0, 0)), "{sql}");
    }
    let oversized = StmtContext::for_query()
        .with_executor_chunk_sizes(32, 2048)
        .with_shared_projection();
    let sql = "SELECT RAND(7) FROM limits_case LIMIT 1";
    assert_eq!(
        run_select_meta_on(sql, &catalog, &oversized).unwrap(),
        run_select_meta_on(sql, &catalog, &StmtContext::for_query()).unwrap()
    );
    assert_eq!(oversized.shared_projection_counts(), Some((0, 0)));
}

#[test]
fn shared_projection_chunk_bridge_preserves_selected_rows_on_reopen() {
    use tidb_ast::CiString;
    use tidb_chunk::chunk::Chunk;
    use tidb_datatype::{FieldType, FieldTypeCode};
    use tidb_expr::{
        column::Column, constant::Constant, expression::Expression,
        scalar_function::ScalarFunction, shared_kernel::SharedProjection,
    };

    let int = FieldType::new(FieldTypeCode::LongLong);
    let real = FieldType::new(FieldTypeCode::Double);
    let literal = |n| Expression::Constant(Constant::new(Datum::Int(n), int.clone()));
    let call = |name, ty: &FieldType, args| {
        Expression::ScalarFunction(ScalarFunction::new(CiString::new(name), ty.clone(), args))
    };
    let mut col = Column::new(1, int.clone());
    col.index = 0;
    let projection = SharedProjection::compile(&[
        call("plus", &int, vec![Expression::Column(col), literal(1)]),
        call("rand", &real, vec![literal(7)]),
    ])
    .unwrap();
    let mut execution = projection.into_owned_execution(StmtContext::for_query());
    let mut input = Chunk::new(std::slice::from_ref(&int), 3, 3);
    for n in [10, 20, 30] {
        input.append_int64(0, n);
    }
    input.set_sel(Some(vec![2, 0, 2]));
    let mut output = Chunk::new(&[int, real], 3, 3);
    for _ in 0..2 {
        execution.reset();
        let rng = MysqlRng::new_with_seed(7);
        for _ in 0..2 {
            execution.evaluate_into_chunk(&input, &mut output).unwrap();
            assert_eq!(output.num_rows(), 3);
            for (i, expected) in [31, 11, 31].into_iter().enumerate() {
                let row = output.get_row(i);
                assert_eq!(row.get_int64(0), expected);
                assert_eq!(row.get_float64(1), rng.gen());
            }
        }
    }
}
