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
        "SELECT n, RAND(), n + 1 FROM limits_case LIMIT 1",
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

    for (sql, route) in [
        ("SELECT RAND(7), ABS(n) FROM limits_case LIMIT 1", (0, 0)),
        ("SELECT RAND(7), n FROM limits_case LIMIT 1", (1, 1)),
    ] {
        let shared = StmtContext::for_query().with_shared_projection();
        assert_eq!(
            run_select_meta_on(sql, &catalog, &shared).unwrap(),
            run_select_meta_on(sql, &catalog, &StmtContext::for_query()).unwrap()
        );
        assert_eq!(shared.shared_projection_counts(), Some(route), "{sql}");
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

#[test]
fn shared_sql_projection_reports_original_runtime_counters() {
    use tidb_ast::{QueryStmt, Stmt};
    use tidb_executor::{explain_analyze_select_stmt, Catalog, ExplainFormat, DEFAULT_DATABASE};

    let Stmt::Query(query) = tidb_parser::parse("SELECT RAND(7)").unwrap() else {
        unreachable!();
    };
    let QueryStmt::Select(select) = query.as_ref() else {
        unreachable!();
    };
    let catalog = Catalog::default();
    let native = StmtContext::for_query();
    let shared = StmtContext::for_query().with_shared_projection();
    let explain = |ctx| {
        let (columns, rows) = explain_analyze_select_stmt(
            select,
            &catalog,
            DEFAULT_DATABASE,
            ctx,
            ExplainFormat::Row,
        )
        .unwrap();
        let row = rows.first().unwrap();
        assert!(row[0].sql_string().unwrap().starts_with("Projection_"));
        let field = |name| {
            let i = columns.iter().position(|(label, _)| label == name).unwrap();
            row[i].sql_string().unwrap()
        };
        let rows = field("actRows");
        let info = field("execution info");
        eprintln!("projection runtime: actRows {rows}, {info}");
        (rows, info)
    };
    let (native_rows, native_info) = explain(&native);
    let (shared_rows, shared_info) = explain(&shared);
    assert_eq!(shared.shared_projection_counts(), Some((1, 1)));
    assert_eq!(native_rows, "1");
    assert!(native_info.contains("loops:2"), "{native_info}");
    assert_eq!(shared_rows, native_rows);
    assert!(shared_info.contains("loops:2"), "{shared_info}");
}

#[test]
fn shared_projection_declines_reader_child_plan() {
    use tidb_ast::Stmt;
    use tidb_executor::driver::open_query_meta_stmt_with_physical;
    use tidb_executor::{plan_query_meta_stmt, Catalog, DEFAULT_DATABASE};
    use tidb_planner::physical::{BasePhysicalPlan, PhysicalPlan, PhysicalTableReader};
    use tidb_planner::physical_table_reader::{ReadReqType, StoreType};

    let Stmt::Query(query) = tidb_parser::parse("SELECT RAND(7)").unwrap() else {
        unreachable!();
    };
    let catalog = Catalog::default();
    let outer = StmtContext::for_query().with_shared_projection();
    let inner = StmtContext::for_query().with_shared_projection();
    let mut projection = plan_query_meta_stmt(&query, &catalog, DEFAULT_DATABASE, &outer).unwrap();
    assert!(matches!(projection, PhysicalPlan::Projection(_)));
    let mut base = BasePhysicalPlan::with_id(100, "TableReader", 0);
    base.base.set_schema(projection.schema().cloned());
    let mut reader = PhysicalPlan::TableReader(PhysicalTableReader {
        base,
        table_plan: Some(Box::new(projection.clone())),
        store_type: StoreType::TiKv,
        is_common_handle: false,
        read_req_type: ReadReqType::Cop,
    });
    let run = |ctx, plan| {
        open_query_meta_stmt_with_physical(&query, Some(plan), &catalog, DEFAULT_DATABASE, ctx)
            .unwrap()
            .collect()
            .unwrap()
    };
    assert_eq!(run(&outer, &mut projection), run(&inner, &mut reader));
    eprintln!(
        "manual reader fixture: outside {:?}, inside {:?}",
        outer.shared_projection_counts(),
        inner.shared_projection_counts()
    );
    assert_eq!(outer.shared_projection_counts(), Some((1, 1)));
    assert_eq!(inner.shared_projection_counts(), Some((0, 0)));
}

#[test]
fn shared_sql_projection_declines_nested_string_variable_reads() {
    use tidb_datatype::Collation;
    use tidb_executor::{run_select_meta_on, Catalog};

    // getvar_string is the read emitted by the session binder for both carriers.
    let sql = "SELECT RAND(), @copy := getvar_string('v')";
    let catalog = Catalog::default();
    for value in [
        Datum::new_bytes(b"abc"),
        Datum::new_collation_string(b"abc", Collation::Binary),
    ] {
        let run = |opt_in| {
            let vars = Arc::new(Mutex::new(HashMap::from([("v".to_owned(), value.clone())])));
            let rng = Arc::new(MysqlRng::new_with_seed(1));
            let mut ctx = StmtContext::for_query()
                .with_user_vars(Arc::clone(&vars))
                .with_rand_session(Arc::clone(&rng));
            if opt_in {
                ctx = ctx.with_shared_projection();
            }
            let result = run_select_meta_on(sql, &catalog, &ctx);
            let copied = ctx.get_uservar("copy");
            (result, copied, rng.gen(), ctx.shared_projection_counts())
        };
        let (expected, stored, next, _) = run(false);
        let expected = expected.unwrap();
        assert_eq!(stored, Some(value.clone()));
        let rng = MysqlRng::new_with_seed(1);
        rng.gen();
        assert_eq!(next, rng.gen());
        let (actual, copied, next_shared, counters) = run(true);
        eprintln!(
            "string variable copy of {value:?}: {actual:?}, stored {copied:?}, path {counters:?}"
        );
        assert_eq!(counters, Some((0, 0)));
        assert_eq!(actual.unwrap(), expected);
        assert_eq!(copied, Some(value));
        assert_eq!(next_shared, next);
    }
    let shared = StmtContext::for_query().with_shared_projection();
    run_select_meta_on("SELECT RAND(7)", &catalog, &shared).unwrap();
    assert_eq!(shared.shared_projection_counts(), Some((1, 1)));
}

#[test]
fn shared_sql_projection_runs_signed_logic_and_declines_unsigned_group() {
    use tidb_ast::Stmt;
    use tidb_datatype::{FieldTypeCode, FieldTypeFlags};
    use tidb_executor::{
        driver::open_query_meta_stmt_with_physical, run_create_table_on, run_insert_on,
        run_select_meta_on, Catalog, DEFAULT_DATABASE,
    };

    let mut catalog = Catalog::default();
    let context = || StmtContext::for_query().with_executor_chunk_sizes(2, 8);
    run_create_table_on(
        "CREATE TABLE logic_samples (
            id BIGINT PRIMARY KEY, a BIGINT, b BIGINT, c BIGINT, u BIGINT UNSIGNED
        )",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO logic_samples VALUES
         (1,-2,0,-3,0),(2,0,NULL,-1,1),(3,NULL,0,0,1),(4,0,2,NULL,0),(5,-1,NULL,-9,0)",
        &mut catalog,
        &context(),
    )
    .unwrap();

    let sql = "SELECT (a OR b) AND c, RAND(7) FROM logic_samples ORDER BY id LIMIT 5";
    let native = context();
    let (columns, expected) = run_select_meta_on(sql, &catalog, &native).unwrap();
    assert_eq!(native.shared_projection_counts(), None);
    let field = &columns[0].1;
    assert_eq!(field.code(), FieldTypeCode::LongLong);
    assert_eq!(field.flen(), 1);
    assert_ne!(field.flags() & FieldTypeFlags::IS_BOOLEAN, 0);
    assert!(!field.is_unsigned());
    assert!(!field.is_array());
    assert_eq!(
        expected
            .iter()
            .map(|row| row[0].clone())
            .collect::<Vec<_>>(),
        [
            Datum::Int(1),
            Datum::Null,
            Datum::Int(0),
            Datum::Null,
            Datum::Int(1)
        ],
    );

    let Stmt::Query(query) = tidb_parser::parse(sql).unwrap() else {
        unreachable!();
    };
    let shared = context().with_shared_projection();
    let mut records =
        open_query_meta_stmt_with_physical(&query, None, &catalog, DEFAULT_DATABASE, &shared)
            .unwrap();
    assert_eq!(records.columns(), columns);
    let types: Vec<_> = columns.iter().map(|(_, field)| field.clone()).collect();
    let mut request = records.new_chunk();
    let mut actual = Vec::new();
    let mut batches = Vec::new();
    loop {
        request.set_required_rows(2, 8);
        records.next(&mut request).unwrap();
        if request.num_rows() == 0 {
            break;
        }
        batches.push(request.num_rows());
        actual.extend((0..request.num_rows()).map(|i| request.get_row(i).get_datum_row(&types)));
    }
    records.finish().unwrap();
    assert_eq!(actual, expected);
    assert!(batches.len() >= 2);
    eprintln!(
        "batches={batches:?}, route={:?}, rows={actual:?}, field={field:?}",
        shared.shared_projection_counts()
    );
    assert_eq!(
        shared.shared_projection_counts(),
        Some((1, batches.len() as u64))
    );

    let declined = context().with_shared_projection();
    let mixed = "SELECT a OR u, RAND(7) FROM logic_samples ORDER BY id LIMIT 5";
    assert_eq!(
        run_select_meta_on(mixed, &catalog, &declined).unwrap(),
        run_select_meta_on(mixed, &catalog, &context()).unwrap(),
    );
    assert_eq!(declined.shared_projection_counts(), Some((0, 0)));
    eprintln!(
        "unsigned group route={:?}",
        declined.shared_projection_counts()
    );
}

#[test]
fn shared_sql_selection_keeps_filter_demand_and_declines_decimal() {
    use tidb_executor::{run_create_table_on, run_insert_on, run_select_meta_on, Catalog};

    let mut catalog = Catalog::default();
    run_create_table_on(
        "CREATE TABLE filter_samples (
          id BIGINT PRIMARY KEY, a BIGINT, b BIGINT, u BIGINT UNSIGNED
        )",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO filter_samples VALUES
          (1,-5,-7,0),(2,-1,1,1),(3,NULL,NULL,2),(4,3,1,3),(5,4,5,4),(6,7,3,5)",
        &mut catalog,
        &StmtContext::for_query(),
    )
    .unwrap();

    let sql = "SELECT id FROM
        (SELECT id,a,b,u FROM filter_samples ORDER BY id LIMIT 6) AS retained
        WHERE (a >= b OR a <=> b) AND b > BENCHMARK(2, RAND()) ORDER BY id";
    let compare = |sql: &str, expected_selected| {
        let original_rng = Arc::new(MysqlRng::new_with_seed(1));
        let shared_rng = Arc::new(MysqlRng::new_with_seed(1));
        let context = |rng| {
            StmtContext::for_query()
                .with_executor_chunk_sizes(2, 2)
                .with_rand_session(rng)
        };
        let original = context(Arc::clone(&original_rng));
        let shared = context(Arc::clone(&shared_rng)).with_shared_selection();
        let expected = run_select_meta_on(sql, &catalog, &original).unwrap();
        let actual = run_select_meta_on(sql, &catalog, &shared).unwrap();
        assert_eq!(actual, expected);
        assert_eq!(shared.take_warnings(), original.take_warnings());
        assert_eq!(original.shared_selection_counts(), None);
        let counts = shared.shared_selection_counts().unwrap();
        eprintln!("filter rows={:?}, route={counts:?}", actual.1);
        assert_eq!(counts.0, expected_selected);
        if expected_selected == 0 {
            assert_eq!(counts.1, 0);
        } else {
            assert!(counts.1 >= 2);
        }
        assert_eq!(shared_rng.gen(), original_rng.gen());
        actual.1
    };

    assert_eq!(compare(sql, 1), [vec![Datum::Int(4)], vec![Datum::Int(6)]]);
    assert_eq!(
        compare(&sql.replace("AND b >", "AND u >"), 1),
        [
            vec![Datum::Int(3)],
            vec![Datum::Int(4)],
            vec![Datum::Int(6)]
        ],
    );
    compare(&sql.replace("AND b >", "AND CAST(u AS DECIMAL) >"), 0);
}

#[test]
fn shared_sql_projection_runs_bigint_conditionals() {
    use tidb_ast::Stmt;
    use tidb_executor::{
        driver::open_query_meta_stmt_with_physical, run_create_table_on, run_insert_on,
        run_select_meta_on, Catalog, DEFAULT_DATABASE,
    };

    let mut catalog = Catalog::default();
    let context = || StmtContext::for_query().with_executor_chunk_sizes(2, 8);
    run_create_table_on(
        "CREATE TABLE nullable_samples (id BIGINT PRIMARY KEY, x BIGINT, y BIGINT)",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO nullable_samples VALUES (1,-1,NULL),(2,0,-8),(3,NULL,9),(4,NULL,NULL),(5,3,4)",
        &mut catalog,
        &context(),
    )
    .unwrap();

    let sql =
        "SELECT CASE WHEN x THEN y WHEN y THEN id END, IFNULL(x,id), COALESCE(x,y,id), RAND(7)
        FROM nullable_samples ORDER BY id LIMIT 5";
    let original = context();
    let expected = run_select_meta_on(sql, &catalog, &original).unwrap();
    let shared = context().with_shared_projection();
    let Stmt::Query(query) = tidb_parser::parse(sql).unwrap() else {
        unreachable!();
    };
    let mut records =
        open_query_meta_stmt_with_physical(&query, None, &catalog, DEFAULT_DATABASE, &shared)
            .unwrap();
    assert_eq!(records.columns(), expected.0);
    let types = expected
        .0
        .iter()
        .map(|(_, tp)| tp.clone())
        .collect::<Vec<_>>();
    let mut request = records.new_chunk();
    let mut actual = Vec::new();
    loop {
        request.set_required_rows(2, 8);
        records.next(&mut request).unwrap();
        if request.num_rows() == 0 {
            break;
        }
        actual.extend((0..request.num_rows()).map(|i| request.get_row(i).get_datum_row(&types)));
    }
    records.finish().unwrap();
    assert_eq!(actual, expected.1);
    assert_eq!(shared.take_warnings(), original.take_warnings());
    use Datum::{Int, Null};
    assert_eq!(
        actual.iter().map(|row| &row[..3]).collect::<Vec<_>>(),
        [
            &[Null, Int(-1), Int(-1)][..],
            &[Int(2), Int(0), Int(0)][..],
            &[Int(3), Int(3), Int(9)][..],
            &[Null, Int(4), Int(4)][..],
            &[Int(4), Int(3), Int(3)][..],
        ],
    );
    let route = shared.shared_projection_counts().unwrap();
    eprintln!("conditional projection route={route:?}, rows={actual:?}");
    assert_eq!(route.0, 1);
    assert!(route.1 >= 2);
    let mixed = sql.replace("COALESCE(x,y,id)", "COALESCE(x,CAST(y AS DOUBLE),id)");
    let fallback = context().with_shared_projection();
    assert_eq!(
        run_select_meta_on(&mixed, &catalog, &fallback).unwrap(),
        run_select_meta_on(&mixed, &catalog, &context()).unwrap(),
    );
    assert_eq!(fallback.shared_projection_counts(), Some((0, 0)));
}

#[test]
fn shared_sql_projection_runs_real_and_decimal_conditionals() {
    use tidb_executor::{run_create_table_on, run_insert_on, run_select_meta_on, Catalog};

    let mut catalog = Catalog::default();
    run_create_table_on(
        "CREATE TABLE numeric_choice (id BIGINT PRIMARY KEY, r DOUBLE, s DOUBLE,
          d DECIMAL(20,8), e DECIMAL(20,8))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO numeric_choice VALUES
          (1,NULL,2.5,NULL,1.22500000),(2,0,NULL,-1.23123456,0.0),(3,NULL,NULL,NULL,NULL)",
        &mut catalog,
        &StmtContext::for_query(),
    )
    .unwrap();

    let sql = "SELECT CASE WHEN id < 3 THEN r END, IFNULL(r,s), COALESCE(d,e)
        FROM numeric_choice ORDER BY id LIMIT 3";
    let original = StmtContext::for_query();
    let shared = StmtContext::for_query().with_shared_projection();
    let expected = run_select_meta_on(sql, &catalog, &original).unwrap();
    let actual = run_select_meta_on(sql, &catalog, &shared).unwrap();
    assert_eq!(actual, expected);
    assert_eq!(shared.take_warnings(), original.take_warnings());
    eprintln!(
        "numeric conditional route={:?}, rows={:?}",
        shared.shared_projection_counts(),
        actual.1
    );
    assert_eq!(shared.shared_projection_counts(), Some((1, 1)));
}

#[test]
fn shared_sql_projection_passes_columns_to_topn_with_computed_keys() {
    use tidb_ast::Stmt;
    use tidb_executor::{
        plan_query_meta_stmt, run_create_table_on, run_insert_on, run_select_meta_on, Catalog,
        DEFAULT_DATABASE,
    };
    use tidb_planner::physical::PhysicalPlan;

    fn has_topn(plan: &PhysicalPlan) -> bool {
        matches!(plan, PhysicalPlan::TopN(_)) || plan.children().iter().any(has_topn)
    }

    let mut catalog = Catalog::default();
    run_create_table_on(
        "CREATE TABLE mixed_projection (id BIGINT PRIMARY KEY, v BIGINT, b VARBINARY(8))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO mixed_projection VALUES (1,8,'a'),(2,NULL,NULL),(3,4,'b'),(4,0,'c')",
        &mut catalog,
        &StmtContext::for_query(),
    )
    .unwrap();

    let sql = "SELECT b, COALESCE(v,id) AS k, id, b
        FROM mixed_projection ORDER BY k, id LIMIT 3";
    let context = || StmtContext::for_query().with_executor_chunk_sizes(2, 8);
    let native = context();
    let shared = context().with_shared_projection();
    let Stmt::Query(query) = tidb_parser::parse(sql).unwrap() else {
        unreachable!();
    };
    let plan = plan_query_meta_stmt(&query, &catalog, DEFAULT_DATABASE, &native).unwrap();
    assert!(has_topn(&plan));
    let expected = run_select_meta_on(sql, &catalog, &native).unwrap();
    let actual = run_select_meta_on(sql, &catalog, &shared).unwrap();
    assert_eq!(actual, expected);
    assert_eq!(shared.take_warnings(), native.take_warnings());
    assert_eq!(
        actual.1.iter().map(|row| &row[2]).collect::<Vec<_>>(),
        [&Datum::Int(4), &Datum::Int(2), &Datum::Int(3)],
    );
    assert_eq!(actual.1[1][0], Datum::Null);
    assert!(actual.1.iter().all(|row| row[0] == row[3]));
    eprintln!(
        "mixed TopN projection route={:?}",
        shared.shared_projection_counts()
    );
    assert_eq!(shared.shared_projection_counts(), Some((2, 2)));

    // A second execution reuses the same layout for successive child batches.
    // The repeated direct slot stays aliased while the seeded stream advances.
    let Stmt::Query(query) =
        tidb_parser::parse("SELECT b, RAND(7), b, id FROM mixed_projection ORDER BY id LIMIT 4")
            .unwrap()
    else {
        unreachable!();
    };
    let read_batches = |ctx: &StmtContext| {
        let mut records = tidb_executor::driver::open_query_meta_stmt_with_physical(
            &query,
            None,
            &catalog,
            DEFAULT_DATABASE,
            ctx,
        )
        .unwrap();
        let types: Vec<_> = records.columns().iter().map(|(_, ty)| ty.clone()).collect();
        let mut request = records.new_chunk();
        let mut results = Vec::new();
        loop {
            request.set_required_rows(2, 8);
            records.next(&mut request).unwrap();
            if request.num_rows() == 0 {
                break;
            }
            results.push(
                (0..request.num_rows())
                    .map(|i| request.get_row(i).get_datum_row(&types))
                    .collect::<Vec<_>>(),
            );
        }
        records.finish().unwrap();
        results
    };
    let shared = context().with_shared_projection();
    let batches = read_batches(&shared);
    assert_eq!(batches, read_batches(&context()));
    assert_eq!(batches.iter().map(Vec::len).collect::<Vec<_>>(), [2, 2]);
    assert_eq!(shared.shared_projection_counts(), Some((1, 2)));

    for sql in [
        "SELECT id FROM mixed_projection",
        "SELECT b, COALESCE(v,id), ABS(v) FROM mixed_projection",
    ] {
        let fallback = context().with_shared_projection();
        assert_eq!(
            run_select_meta_on(sql, &catalog, &fallback).unwrap(),
            run_select_meta_on(sql, &catalog, &context()).unwrap(),
        );
        assert_eq!(fallback.shared_projection_counts(), Some((0, 0)));
    }
}

#[test]
fn shared_sql_bigint_truth_tests_run_in_projection_and_selection() {
    use tidb_executor::{run_create_table_on, run_insert_on, run_select_meta_on, Catalog};

    let mut catalog = Catalog::default();
    run_create_table_on(
        "CREATE TABLE truth_values (id BIGINT PRIMARY KEY, n BIGINT, u BIGINT UNSIGNED)",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO truth_values VALUES (1,NULL,NULL),(2,0,0),(3,-1,18446744073709551615),(4,1,1)",
        &mut catalog,
        &StmtContext::for_query(),
    )
    .unwrap();

    let ctx = StmtContext::for_query();
    let shared = ctx.clone().with_shared_projection().with_shared_selection();
    let sql = "SELECT id, NOT n, n IS NULL, u IS TRUE, u IS NOT FALSE
        FROM (SELECT id,n,u FROM truth_values ORDER BY id LIMIT 4) AS kept
        WHERE (n IS NULL OR u IS TRUE) ORDER BY id";
    let expected = run_select_meta_on(sql, &catalog, &ctx).unwrap();
    let actual = run_select_meta_on(sql, &catalog, &shared).unwrap();
    assert_eq!(actual, expected);
    assert_eq!(shared.take_warnings(), ctx.take_warnings());
    use Datum::{Int, Null};
    assert_eq!(
        actual.1,
        [
            vec![Int(1), Null, Int(1), Int(0), Int(1)],
            vec![Int(3), Int(0), Int(0), Int(1), Int(1)],
            vec![Int(4), Int(0), Int(0), Int(1), Int(1)],
        ]
    );
    let projected = shared.shared_projection_counts().unwrap();
    let selected = shared.shared_selection_counts().unwrap();
    eprintln!("BIGINT truth route: projection={projected:?}, selection={selected:?}");
    assert_eq!(projected, (1, 1));
    assert_eq!(selected, (1, 1));

    let sql = "SELECT id FROM (SELECT id,n FROM truth_values ORDER BY id LIMIT 4) AS kept
        WHERE n IS NULL OR BENCHMARK(2,RAND()) IS FALSE ORDER BY id";
    let original_rng = Arc::new(MysqlRng::new_with_seed(7));
    let shared_rng = Arc::new(MysqlRng::new_with_seed(7));
    let original = StmtContext::for_query().with_rand_session(Arc::clone(&original_rng));
    let shared = StmtContext::for_query()
        .with_rand_session(Arc::clone(&shared_rng))
        .with_shared_selection();
    let expected = run_select_meta_on(sql, &catalog, &original).unwrap();
    assert_eq!(
        run_select_meta_on(sql, &catalog, &shared).unwrap(),
        expected
    );
    assert_eq!(
        expected.1,
        [vec![Int(1)], vec![Int(2)], vec![Int(3)], vec![Int(4)]]
    );
    assert_eq!(shared.shared_selection_counts(), Some((1, 1)));
    let rng = MysqlRng::new_with_seed(7);
    for _ in 0..6 {
        rng.gen();
    }
    let next = rng.gen();
    assert_eq!(original_rng.gen(), next);
    assert_eq!(shared_rng.gen(), next);
}

#[test]
fn shared_sql_byte_counts_preserve_invalid_utf8() {
    use tidb_executor::{run_create_table_on, run_insert_on, run_select_meta_on, Catalog};

    let mut catalog = Catalog::default();
    run_create_table_on(
        "CREATE TABLE byte_counts (id BIGINT PRIMARY KEY, b VARBINARY(8), t VARCHAR(8))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO byte_counts VALUES (1,X'FF00','界'),(2,'',''),(3,NULL,NULL)",
        &mut catalog,
        &StmtContext::for_query(),
    )
    .unwrap();
    let sql = "SELECT id, LENGTH(b), OCTET_LENGTH(b), BIT_LENGTH(b), ASCII(b), CHAR_LENGTH(b)
        FROM byte_counts ORDER BY id LIMIT 3";
    let native = StmtContext::for_query();
    let shared = StmtContext::for_query().with_shared_projection();
    let expected = run_select_meta_on(sql, &catalog, &native).unwrap();
    let actual = run_select_meta_on(sql, &catalog, &shared).unwrap();
    assert_eq!(actual, expected);
    assert_eq!(shared.take_warnings(), native.take_warnings());
    use Datum::{Int, Null};
    assert_eq!(
        actual.1,
        [
            vec![Int(1), Int(2), Int(2), Int(16), Int(255), Int(2)],
            vec![Int(2), Int(0), Int(0), Int(0), Int(0), Int(0)],
            vec![Int(3), Null, Null, Null, Null, Null],
        ]
    );
    eprintln!(
        "binary byte count route={:?}",
        shared.shared_projection_counts()
    );
    assert_eq!(shared.shared_projection_counts(), Some((1, 1)));

    let text_bytes = StmtContext::for_query().with_shared_projection();
    let sql = "SELECT LENGTH(t), ASCII(t) FROM byte_counts ORDER BY id LIMIT 3";
    assert_eq!(
        run_select_meta_on(sql, &catalog, &text_bytes).unwrap(),
        run_select_meta_on(sql, &catalog, &StmtContext::for_query()).unwrap(),
    );
    assert_eq!(text_bytes.shared_projection_counts(), Some((1, 1)));

    let text = StmtContext::for_query().with_shared_projection();
    let sql = "SELECT LENGTH(t), CHAR_LENGTH(t) FROM byte_counts ORDER BY id LIMIT 3";
    let actual = run_select_meta_on(sql, &catalog, &text).unwrap();
    assert_eq!(
        actual.1,
        [vec![Int(3), Int(1)], vec![Int(0), Int(0)], vec![Null, Null],]
    );
    assert_eq!(text.shared_projection_counts(), Some((0, 0)));
}

#[test]
fn shared_sql_materializes_aggregate_arguments_and_keys() {
    use tidb_ast::Stmt;
    use tidb_executor::{
        plan_query_meta_stmt, run_create_table_on, run_insert_on, run_select_meta_on, Catalog,
        DEFAULT_DATABASE,
    };
    use tidb_expr::expression::Expression;
    use tidb_planner::physical::PhysicalPlan;

    fn input_projection_id(plan: &PhysicalPlan, stream: bool, computed: usize) -> Option<i32> {
        let matching_aggregate = matches!(
            (plan, stream),
            (PhysicalPlan::HashAgg(_), false) | (PhysicalPlan::StreamAgg(_), true)
        );
        if matching_aggregate {
            if let [projection @ PhysicalPlan::Projection(physical)] = plan.children() {
                if physical.exprs.iter()
                    .filter(|expr| matches!(expr, Expression::ScalarFunction(f) if f.func_name.lowercase() == "coalesce"))
                    .count() == computed
                {
                    return Some(projection.id());
                }
            }
        }
        plan.children()
            .iter()
            .find_map(|child| input_projection_id(child, stream, computed))
    }

    let mut catalog = Catalog::default();
    run_create_table_on(
        "CREATE TABLE shared_agg (id BIGINT PRIMARY KEY, g BIGINT, a BIGINT)",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO shared_agg VALUES (1,1,8),(2,NULL,NULL),(3,1,4),(4,2,NULL),(5,NULL,-3),(6,9,99)",
        &mut catalog,
        &StmtContext::for_query(),
    )
    .unwrap();

    use Datum::Int;
    for (sql, stream, computed, expected_rows) in [
        (
            "SELECT /*+ HASH_AGG() */ COALESCE(g,id) AS k, MAX(COALESCE(a,id)), COUNT(*)
             FROM (SELECT id,g,a FROM shared_agg ORDER BY id LIMIT 5) AS retained
             GROUP BY k ORDER BY k",
            false,
            2,
            vec![
                vec![Int(1), Int(8), Int(2)],
                vec![Int(2), Int(4), Int(2)],
                vec![Int(5), Int(-3), Int(1)],
            ],
        ),
        (
            "SELECT /*+ STREAM_AGG() */ MAX(COALESCE(a,id))
             FROM (SELECT id,a FROM shared_agg ORDER BY id LIMIT 5) AS retained",
            true,
            1,
            vec![vec![Int(8)]],
        ),
    ] {
        let native = StmtContext::for_query();
        let shared = StmtContext::for_query().with_shared_projection();
        let Stmt::Query(query) = tidb_parser::parse(sql).unwrap() else {
            unreachable!();
        };
        let plan = plan_query_meta_stmt(&query, &catalog, DEFAULT_DATABASE, &native).unwrap();
        let projection = input_projection_id(&plan, stream, computed)
            .unwrap_or_else(|| panic!("expected aggregate-input projection: {plan:#?}"));
        eprintln!("aggregate input projection {projection}");
        let expected = run_select_meta_on(sql, &catalog, &native).unwrap();
        let actual = run_select_meta_on(sql, &catalog, &shared).unwrap();
        assert_eq!(actual, expected);
        assert_eq!(actual.1, expected_rows);
        assert_eq!(shared.take_warnings(), native.take_warnings());
        eprintln!(
            "aggregate stream={stream} route={:?}",
            shared.shared_projection_counts()
        );
        assert_eq!(
            shared.shared_projection_counts(),
            Some(if stream { (2, 2) } else { (2, 3) })
        );
    }
}

#[test]
fn shared_sql_signed_subtraction_keeps_null_demand_and_overflow() {
    use tidb_executor::{run_create_table_on, run_insert_on, run_select_meta_on, Catalog};

    let mut catalog = Catalog::default();
    run_create_table_on(
        "CREATE TABLE sub_values (id BIGINT PRIMARY KEY, n BIGINT)",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO sub_values VALUES (1,5),(2,NULL),(3,-9223372036854775808),(4,9223372036854775807)",
        &mut catalog,
        &StmtContext::for_query(),
    )
    .unwrap();

    let sql = "SELECT id, n - BENCHMARK(2,RAND()) FROM sub_values ORDER BY id LIMIT 4";
    let native_rng = Arc::new(MysqlRng::new_with_seed(7));
    let shared_rng = Arc::new(MysqlRng::new_with_seed(7));
    let native = StmtContext::for_query().with_rand_session(Arc::clone(&native_rng));
    let shared = StmtContext::for_query()
        .with_rand_session(Arc::clone(&shared_rng))
        .with_shared_projection();
    let expected = run_select_meta_on(sql, &catalog, &native).unwrap();
    let actual = run_select_meta_on(sql, &catalog, &shared).unwrap();
    assert_eq!(actual, expected);
    assert_eq!(shared.take_warnings(), native.take_warnings());
    use Datum::{Int, Null};
    assert_eq!(
        actual.1,
        [
            vec![Int(1), Int(5)],
            vec![Int(2), Null],
            vec![Int(3), Int(i64::MIN)],
            vec![Int(4), Int(i64::MAX)],
        ]
    );
    let route = shared.shared_projection_counts();
    eprintln!("subtraction route={route:?}");
    assert_eq!(route, Some((1, 1)));
    let oracle = MysqlRng::new_with_seed(7);
    for _ in 0..6 {
        oracle.gen();
    }
    let next = oracle.gen();
    assert_eq!(shared_rng.gen(), next);
    assert_eq!(native_rng.gen(), next);

    for sql in [
        "SELECT n - 1 FROM sub_values WHERE id = 3",
        "SELECT n - (-1) FROM sub_values WHERE id = 4",
    ] {
        let native = StmtContext::for_query();
        let shared = StmtContext::for_query().with_shared_projection();
        let expected = run_select_meta_on(sql, &catalog, &native)
            .unwrap_err()
            .to_mysql_error();
        let actual = run_select_meta_on(sql, &catalog, &shared)
            .unwrap_err()
            .to_mysql_error();
        assert_eq!(actual, expected);
        assert_eq!((actual.code, actual.state), (1690, *b"22003"));
        assert_eq!(shared.shared_projection_counts(), Some((1, 1)));
        assert_eq!(shared.take_warnings(), native.take_warnings());
    }

    let sql = "SELECT CAST(n AS UNSIGNED) - CAST(n AS UNSIGNED) FROM sub_values WHERE id = 4";
    let fallback = StmtContext::for_query().with_shared_projection();
    assert_eq!(
        run_select_meta_on(sql, &catalog, &fallback).unwrap(),
        run_select_meta_on(sql, &catalog, &StmtContext::for_query()).unwrap(),
    );
    assert_eq!(fallback.shared_projection_counts(), Some((0, 0)));
}

#[test]
fn shared_sql_string_conditionals_keep_non_utf8_results() {
    use tidb_executor::{run_create_table_on, run_insert_on, run_select_meta_on, Catalog};

    let mut catalog = Catalog::default();
    run_create_table_on(
        "CREATE TABLE string_choices (id BIGINT PRIMARY KEY, flag BIGINT, b VARBINARY(8), c VARBINARY(8))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO string_choices VALUES (1,1,NULL,X'80'),(2,0,X'FF00',''),(3,NULL,NULL,NULL)",
        &mut catalog,
        &StmtContext::for_query(),
    )
    .unwrap();

    let sql =
        "SELECT id, IF(flag,b,c), CASE WHEN flag THEN b ELSE c END, IFNULL(b,c), COALESCE(b,c)
        FROM string_choices ORDER BY id LIMIT 3";
    let native = StmtContext::for_query();
    let shared = StmtContext::for_query().with_shared_projection();
    let expected = run_select_meta_on(sql, &catalog, &native).unwrap();
    let actual = run_select_meta_on(sql, &catalog, &shared).unwrap();
    assert_eq!(actual, expected);
    assert_eq!(shared.take_warnings(), native.take_warnings());
    let rows = &actual.1;
    assert_eq!(rows[0][1], Datum::Null);
    assert_eq!(rows[0][2], Datum::Null);
    assert_eq!(rows[0][3].as_raw_bytes(), Some(&[0x80][..]));
    assert_eq!(rows[1][1].as_raw_bytes(), Some(&[][..]));
    assert_eq!(rows[1][4].as_raw_bytes(), Some(&[0xff, 0x00][..]));
    assert!(rows[2][1..].iter().all(Datum::is_null));
    eprintln!(
        "string conditional route={:?}",
        shared.shared_projection_counts()
    );
    assert_eq!(shared.shared_projection_counts(), Some((1, 1)));

    let sql = "SELECT CASE WHEN flag THEN b END FROM string_choices ORDER BY id LIMIT 3";
    let shared_case = StmtContext::for_query().with_shared_projection();
    assert_eq!(
        run_select_meta_on(sql, &catalog, &shared_case).unwrap(),
        run_select_meta_on(sql, &catalog, &StmtContext::for_query()).unwrap(),
    );
    assert_eq!(shared_case.shared_projection_counts(), Some((1, 1)));
}

#[test]
fn shared_sql_json_type_reads_binary_json_column() {
    use tidb_executor::{run_create_table_on, run_insert_on, run_select_meta_on, Catalog};

    let mut catalog = Catalog::default();
    run_create_table_on(
        "CREATE TABLE json_types (id BIGINT PRIMARY KEY, j JSON)",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        r#"INSERT INTO json_types VALUES (1,NULL),(2,'null'),(3,'18446744073709551615'),
         (4,'{"k":2}'),(5,'[1]'),(6,'true'),(7,'"abc"')"#,
        &mut catalog,
        &StmtContext::for_query(),
    )
    .unwrap();

    let sql = "SELECT id, JSON_TYPE(j) FROM json_types ORDER BY id LIMIT 7";
    let native = StmtContext::for_query();
    let shared = StmtContext::for_query().with_shared_projection();
    let expected = run_select_meta_on(sql, &catalog, &native).unwrap();
    let actual = run_select_meta_on(sql, &catalog, &shared).unwrap();
    assert_eq!(actual, expected);
    assert_eq!(shared.take_warnings(), native.take_warnings());
    assert_eq!(
        actual
            .1
            .iter()
            .map(|row| row[1].as_raw_bytes())
            .collect::<Vec<_>>(),
        [
            None,
            Some(&b"NULL"[..]),
            Some(&b"UNSIGNED INTEGER"[..]),
            Some(&b"OBJECT"[..]),
            Some(&b"ARRAY"[..]),
            Some(&b"BOOLEAN"[..]),
            Some(&b"STRING"[..])
        ]
    );
    let route = shared.shared_projection_counts();
    eprintln!("JSON_TYPE route={route:?}");
    assert_eq!(route, Some((1, 1)));
}

#[test]
fn shared_sql_mixed_string_if_runs_md5_and_json_type() {
    use tidb_executor::{run_create_table_on, run_insert_on, run_select_meta_on, Catalog};

    let mut catalog = Catalog::default();
    run_create_table_on(
        "CREATE TABLE mixed_string_if (id BIGINT PRIMARY KEY, flag BIGINT, s VARBINARY(8), j JSON)",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO mixed_string_if VALUES
         (1,1,'abc','true'),(2,0,X'FF','18446744073709551615'),(3,NULL,'abc',NULL),(4,1,NULL,'true')",
        &mut catalog,
        &StmtContext::for_query(),
    )
    .unwrap();

    let sql = "SELECT id, IF(flag, MD5(s), JSON_TYPE(j)) FROM mixed_string_if ORDER BY id LIMIT 4";
    let native = StmtContext::for_query();
    let shared = StmtContext::for_query().with_shared_projection();
    let original = run_select_meta_on(sql, &catalog, &native).unwrap();
    let actual = run_select_meta_on(sql, &catalog, &shared).unwrap();
    assert_eq!(actual, original);
    assert_eq!(shared.take_warnings(), native.take_warnings());
    assert_eq!(
        actual
            .1
            .iter()
            .map(|row| row[1].as_raw_bytes())
            .collect::<Vec<_>>(),
        [
            Some(&b"900150983cd24fb0d6963f7d28e17f72"[..]),
            Some(&b"UNSIGNED INTEGER"[..]),
            None,
            None,
        ]
    );
    let route = shared.shared_projection_counts();
    eprintln!("mixed IF route={route:?}");
    assert_eq!(route, Some((1, 1)));
}

#[test]
fn shared_sql_bit_operations_keep_raw_bigint_bits() {
    use tidb_executor::{run_create_table_on, run_insert_on, run_select_meta_on, Catalog};

    let mut catalog = Catalog::default();
    run_create_table_on(
        "CREATE TABLE bit_values (id BIGINT PRIMARY KEY, a BIGINT, b BIGINT UNSIGNED)",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO bit_values VALUES (1,-1,63),(2,-9223372036854775808,64),
         (3,NULL,1),(4,1,18446744073709551615)",
        &mut catalog,
        &StmtContext::for_query(),
    )
    .unwrap();

    let sql = "SELECT id, a & b, a | b, a ^ b, ~a, a << b, a >> b, BIT_COUNT(a)
        FROM bit_values ORDER BY id LIMIT 4";
    let native = StmtContext::for_query();
    let shared = StmtContext::for_query().with_shared_projection();
    let original = run_select_meta_on(sql, &catalog, &native).unwrap();
    let actual = run_select_meta_on(sql, &catalog, &shared).unwrap();
    assert_eq!(actual, original);
    assert_eq!(shared.take_warnings(), native.take_warnings());
    assert!(actual.0[1..7].iter().all(|(_, ty)| ty.is_unsigned()));
    assert!(!actual.0[7].1.is_unsigned());
    use Datum::{Int, Null, UInt};
    assert_eq!(
        actual.1[0],
        [
            Int(1),
            UInt(63),
            UInt(u64::MAX),
            UInt(u64::MAX ^ 63),
            UInt(0),
            UInt(1 << 63),
            UInt(1),
            Int(64)
        ],
    );
    assert_eq!(actual.1[2][1..], [Null, Null, Null, Null, Null, Null, Null]);
    let route = shared.shared_projection_counts();
    eprintln!("bitwise route={route:?}");
    assert_eq!(route, Some((1, 1)));

    let sql = "SELECT a & BENCHMARK(1,RAND()) FROM bit_values ORDER BY id LIMIT 4";
    let native_rng = Arc::new(MysqlRng::new_with_seed(19));
    let shared_rng = Arc::new(MysqlRng::new_with_seed(19));
    let native = StmtContext::for_query().with_rand_session(Arc::clone(&native_rng));
    let shared = StmtContext::for_query()
        .with_rand_session(Arc::clone(&shared_rng))
        .with_shared_projection();
    assert_eq!(
        run_select_meta_on(sql, &catalog, &shared).unwrap(),
        run_select_meta_on(sql, &catalog, &native).unwrap(),
    );
    assert_eq!(shared.shared_projection_counts(), Some((1, 1)));
    let oracle = MysqlRng::new_with_seed(19);
    for _ in 0..4 {
        oracle.gen();
    }
    let next = oracle.gen();
    assert_eq!(native_rng.gen(), next);
    assert_eq!(shared_rng.gen(), next);
}

#[test]
fn shared_sql_double_comparisons_run_in_projection_and_selection() {
    use tidb_executor::{run_create_table_on, run_insert_on, run_select_meta_on, Catalog};

    let mut catalog = Catalog::default();
    run_create_table_on(
        "CREATE TABLE compare_doubles (id BIGINT PRIMARY KEY, a DOUBLE, b DOUBLE)",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO compare_doubles VALUES
          (1,-0e0,0e0),(2,NULL,NULL),(3,3.25,-1.5),(4,-2,6),(5,NULL,0)",
        &mut catalog,
        &StmtContext::for_query(),
    )
    .unwrap();
    let sql = "SELECT id,a=b,a<>b,a<b,a<=b,a>b,a>=b,a<=>b
        FROM (SELECT id,a,b FROM compare_doubles ORDER BY id LIMIT 5) AS kept
        WHERE a>=b OR a<=>b ORDER BY id";
    let native = StmtContext::for_query();
    let shared = StmtContext::for_query()
        .with_shared_projection()
        .with_shared_selection();
    let expected = run_select_meta_on(sql, &catalog, &native).unwrap();
    let actual = run_select_meta_on(sql, &catalog, &shared).unwrap();
    assert_eq!(actual, expected);
    assert_eq!(shared.take_warnings(), native.take_warnings());
    use Datum::{Int, Null};
    assert_eq!(
        actual.1,
        [
            vec![
                Int(1),
                Int(1),
                Int(0),
                Int(0),
                Int(1),
                Int(0),
                Int(1),
                Int(1)
            ],
            vec![Int(2), Null, Null, Null, Null, Null, Null, Int(1)],
            vec![
                Int(3),
                Int(0),
                Int(1),
                Int(0),
                Int(0),
                Int(1),
                Int(1),
                Int(0)
            ],
        ]
    );
    let projection = shared.shared_projection_counts();
    let selection = shared.shared_selection_counts();
    eprintln!("double compare projection={projection:?}, selection={selection:?}");
    assert_eq!(projection, Some((1, 1)));
    assert_eq!(selection, Some((1, 1)));
}

#[test]
fn shared_sql_decimal_comparisons_keep_precision_in_selection() {
    use tidb_executor::{run_create_table_on, run_insert_on, run_select_meta_on, Catalog};

    let mut catalog = Catalog::default();
    run_create_table_on(
        "CREATE TABLE compare_decimals (id BIGINT PRIMARY KEY, a DECIMAL(60,8), b DECIMAL(65,10))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO compare_decimals VALUES
          (1,1.5,1.50),(2,NULL,NULL),
          (3,184467440737095516160000000000000000000001.26,184467440737095516160000000000000000000001.25),
          (4,-2,6),(5,NULL,0)",
        &mut catalog,
        &StmtContext::for_query(),
    )
    .unwrap();
    let sql = "SELECT id,a=b,a<>b,a<b,a<=b,a>b,a>=b,a<=>b
        FROM (SELECT id,a,b FROM compare_decimals ORDER BY id LIMIT 5) AS kept
        WHERE a>=b OR a<=>b ORDER BY id";
    let native = StmtContext::for_query();
    let shared = StmtContext::for_query()
        .with_shared_projection()
        .with_shared_selection();
    let expected = run_select_meta_on(sql, &catalog, &native).unwrap();
    let actual = run_select_meta_on(sql, &catalog, &shared).unwrap();
    assert_eq!(actual, expected);
    assert_eq!(shared.take_warnings(), native.take_warnings());
    use Datum::{Int, Null};
    assert_eq!(
        actual.1,
        [
            vec![
                Int(1),
                Int(1),
                Int(0),
                Int(0),
                Int(1),
                Int(0),
                Int(1),
                Int(1)
            ],
            vec![Int(2), Null, Null, Null, Null, Null, Null, Int(1)],
            vec![
                Int(3),
                Int(0),
                Int(1),
                Int(0),
                Int(0),
                Int(1),
                Int(1),
                Int(0)
            ],
        ]
    );
    let projection = shared.shared_projection_counts();
    let selection = shared.shared_selection_counts();
    eprintln!("decimal compare projection={projection:?}, selection={selection:?}");
    assert_eq!(projection, Some((1, 1)));
    assert_eq!(selection, Some((1, 1)));
}

#[test]
fn shared_sql_binary_comparisons_preserve_no_pad_and_invalid_utf8() {
    use tidb_executor::{run_create_table_on, run_insert_on, run_select_meta_on, Catalog};

    let mut catalog = Catalog::default();
    run_create_table_on(
        "CREATE TABLE compare_bytes (id BIGINT PRIMARY KEY, a VARBINARY(8), b VARBINARY(8))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO compare_bytes VALUES
          (1,X'FF00',X'FF01'),(2,'A','A '),(3,NULL,NULL),(4,'Z','A'),(5,NULL,'')",
        &mut catalog,
        &StmtContext::for_query(),
    )
    .unwrap();
    let sql = "SELECT id,a=b,a<>b,a<b,a<=b,a>b,a>=b,a<=>b
        FROM (SELECT id,a,b FROM compare_bytes ORDER BY id LIMIT 5) AS kept
        WHERE a<b OR a<=>b ORDER BY id";
    let native = StmtContext::for_query();
    let shared = StmtContext::for_query()
        .with_shared_projection()
        .with_shared_selection();
    let expected = run_select_meta_on(sql, &catalog, &native).unwrap();
    let actual = run_select_meta_on(sql, &catalog, &shared).unwrap();
    assert_eq!(actual, expected);
    assert_eq!(shared.take_warnings(), native.take_warnings());
    use Datum::{Int, Null};
    assert_eq!(
        actual.1,
        [
            vec![
                Int(1),
                Int(0),
                Int(1),
                Int(1),
                Int(1),
                Int(0),
                Int(0),
                Int(0)
            ],
            vec![
                Int(2),
                Int(0),
                Int(1),
                Int(1),
                Int(1),
                Int(0),
                Int(0),
                Int(0)
            ],
            vec![Int(3), Null, Null, Null, Null, Null, Null, Int(1)],
        ]
    );
    let projection = shared.shared_projection_counts();
    let selection = shared.shared_selection_counts();
    eprintln!("binary compare projection={projection:?}, selection={selection:?}");
    assert_eq!(projection, Some((1, 1)));
    assert_eq!(selection, Some((1, 1)));
}

#[test]
fn shared_sql_sha_keep_bytes_fields_and_invalid_length_semantics() {
    use tidb_executor::{run_create_table_on, run_insert_on, run_select_meta_on, Catalog};

    let mut catalog = Catalog::default();
    run_create_table_on(
        "CREATE TABLE hash_bytes (id BIGINT PRIMARY KEY, s VARBINARY(8), t VARCHAR(8), n BIGINT)",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO hash_bytes VALUES (1,X'FF00','界',-1),(2,'','',256),(3,NULL,NULL,NULL)",
        &mut catalog,
        &StmtContext::for_query(),
    )
    .unwrap();
    let native = StmtContext::for_query();
    let shared = StmtContext::for_query().with_shared_projection();
    let sql =
        "SELECT id,SHA(s),SHA1(s),SHA1(t),SHA2(s,0),SHA2(s,224),SHA2(s,256),SHA2(s,384),SHA2(s,512)
        FROM hash_bytes ORDER BY id LIMIT 3";
    let expected = run_select_meta_on(sql, &catalog, &native).unwrap();
    let actual = run_select_meta_on(sql, &catalog, &shared).unwrap();
    assert_eq!(actual, expected);
    assert!(shared.take_warnings().is_empty());
    assert!(native.take_warnings().is_empty());
    let rows = actual.1;
    assert_eq!(rows[0][1], rows[0][2]);
    assert_eq!(rows[0][4], rows[0][6]);
    assert_eq!(
        rows[0][1..]
            .iter()
            .map(|value| value.as_raw_bytes().unwrap().len())
            .collect::<Vec<_>>(),
        [40, 40, 40, 64, 56, 64, 96, 128]
    );
    assert_eq!(rows[2][1..], [const { Datum::Null }; 8]);
    eprintln!("SHA projection={:?}", shared.shared_projection_counts());
    assert_eq!(shared.shared_projection_counts(), Some((1, 1)));

    let fallback = StmtContext::for_query().with_shared_projection();
    let sql = "SELECT SHA2(s,-1),SHA2(s,n) FROM hash_bytes ORDER BY id LIMIT 3";
    let actual = run_select_meta_on(sql, &catalog, &fallback).unwrap();
    assert_eq!(actual, run_select_meta_on(sql, &catalog, &native).unwrap());
    assert_eq!(actual.1[0], [Datum::Null, Datum::Null]);
    assert!(fallback.take_warnings().is_empty());
    assert!(native.take_warnings().is_empty());
    assert_eq!(fallback.shared_projection_counts(), Some((0, 0)));
}

#[test]
fn shared_sql_hex_unhex_keep_raw_bytes_and_integer_bits() {
    use tidb_executor::{run_create_table_on, run_insert_on, run_select_meta_on, Catalog};

    let mut catalog = Catalog::default();
    run_create_table_on(
        "CREATE TABLE hex_values
         (id BIGINT PRIMARY KEY, b VARBINARY(8), s VARCHAR(8), n BIGINT, u BIGINT UNSIGNED)",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO hex_values VALUES
         (1,X'FF00','abc',-1,18446744073709551615),
         (2,'','G',-9223372036854775808,9223372036854775808),
         (3,X'FF','',0,0),(4,NULL,NULL,NULL,NULL)",
        &mut catalog,
        &StmtContext::for_query(),
    )
    .unwrap();
    let sql = "SELECT id,HEX(b),HEX(s),HEX(n),HEX(u),UNHEX(s),UNHEX(b)
        FROM hex_values ORDER BY id LIMIT 4";
    let native = StmtContext::for_query();
    let shared = StmtContext::for_query().with_shared_projection();
    let expected = run_select_meta_on(sql, &catalog, &native).unwrap();
    let actual = run_select_meta_on(sql, &catalog, &shared).unwrap();
    assert_eq!(actual, expected);
    assert_eq!(shared.take_warnings(), native.take_warnings());
    let bytes = actual
        .1
        .iter()
        .map(|row| row[1..].iter().map(Datum::as_raw_bytes).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    let bits = &b"FFFFFFFFFFFFFFFF"[..];
    let half = &b"8000000000000000"[..];
    assert_eq!(
        bytes,
        [
            vec![
                Some(&b"FF00"[..]),
                Some(b"616263"),
                Some(bits),
                Some(bits),
                Some(&[0x0a, 0xbc]),
                None
            ],
            vec![
                Some(&b""[..]),
                Some(b"47"),
                Some(half),
                Some(half),
                None,
                Some(b"")
            ],
            vec![
                Some(&b"FF"[..]),
                Some(b""),
                Some(b"0"),
                Some(b"0"),
                Some(b""),
                None
            ],
            vec![None; 6],
        ]
    );
    let route = shared.shared_projection_counts();
    eprintln!("HEX projection={route:?}");
    assert_eq!(route, Some((1, 1)));

    let fallback = StmtContext::for_query().with_shared_projection();
    let sql = "SELECT HEX(CAST(n AS DECIMAL(20,0))),UNHEX(n) FROM hex_values ORDER BY id LIMIT 4";
    assert_eq!(
        run_select_meta_on(sql, &catalog, &fallback).unwrap(),
        run_select_meta_on(sql, &catalog, &native).unwrap()
    );
    assert_eq!(fallback.take_warnings(), native.take_warnings());
    assert_eq!(fallback.shared_projection_counts(), Some((0, 0)));
}

#[test]
fn shared_sql_binary_string_operations_keep_bytes_and_counts() {
    use tidb_executor::{run_create_table_on, run_insert_on, run_select_meta_on, Catalog};

    let mut catalog = Catalog::default();
    run_create_table_on(
        "CREATE TABLE byte_ops (id BIGINT PRIMARY KEY, b VARBINARY(8), n BIGINT, t VARCHAR(8))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO byte_ops VALUES
         (1,X'20FF000920',2,'界a'),(2,'  ',-1,'a'),(3,X'E7958C',9223372036854775807,'界'),
         (4,'a',NULL,'a'),(5,NULL,1,NULL),(6,'',0,'')",
        &mut catalog,
        &StmtContext::for_query(),
    )
    .unwrap();
    let native = StmtContext::for_query();
    let shared = StmtContext::for_query().with_shared_projection();
    let sql = "SELECT id,LTRIM(b),RTRIM(b),REVERSE(b),LEFT(b,n),RIGHT(b,n)
        FROM byte_ops ORDER BY id LIMIT 6";
    let expected = run_select_meta_on(sql, &catalog, &native).unwrap();
    let actual = run_select_meta_on(sql, &catalog, &shared).unwrap();
    assert_eq!(actual, expected);
    assert_eq!(shared.take_warnings(), native.take_warnings());
    let bytes: Vec<Vec<Option<&[u8]>>> = actual
        .1
        .iter()
        .map(|row| row[1..].iter().map(Datum::as_raw_bytes).collect())
        .collect();
    assert_eq!(
        bytes,
        [
            vec![
                Some(&[0xff, 0, 9, 32][..]),
                Some(&[32, 0xff, 0, 9]),
                Some(&[32, 9, 0, 0xff, 32]),
                Some(&[32, 0xff]),
                Some(&[9, 32])
            ],
            vec![Some(&b""[..]), Some(b""), Some(b"  "), Some(b""), Some(b"")],
            vec![
                Some(&[0xe7, 0x95, 0x8c][..]),
                Some(&[0xe7, 0x95, 0x8c]),
                Some(&[0x8c, 0x95, 0xe7]),
                Some(&[0xe7, 0x95, 0x8c]),
                Some(&[0xe7, 0x95, 0x8c])
            ],
            vec![Some(&b"a"[..]), Some(b"a"), Some(b"a"), None, None],
            vec![None; 5],
            vec![Some(&b""[..]); 5],
        ]
    );
    let route = shared.shared_projection_counts();
    eprintln!("binary string operations projection={route:?}");
    assert_eq!(route, Some((1, 1)));

    let fallback = StmtContext::for_query().with_shared_projection();
    let sql = "SELECT REVERSE(t),LEFT(t,1),RIGHT(t,1) FROM byte_ops ORDER BY id LIMIT 6";
    let actual = run_select_meta_on(sql, &catalog, &fallback).unwrap();
    assert_eq!(actual, run_select_meta_on(sql, &catalog, &native).unwrap());
    assert_eq!(actual.1[0][0].as_raw_bytes(), Some("a界".as_bytes()));
    assert_eq!(actual.1[0][1].as_raw_bytes(), Some("界".as_bytes()));
    assert_eq!(fallback.shared_projection_counts(), Some((0, 0)));
}

#[test]
fn shared_sql_ip_conversions_keep_typed_values_and_fields() {
    use tidb_executor::{run_create_table_on, run_insert_on, run_select_meta_on, Catalog};

    let mut catalog = Catalog::default();
    run_create_table_on(
        "CREATE TABLE ip_values
         (id BIGINT PRIMARY KEY, n BIGINT, u BIGINT UNSIGNED, ip VARBINARY(64), bin VARBINARY(16))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO ip_values VALUES
         (1,4294967295,2130706433,'127.0.0.1',X'7F000001'),
         (2,-1,18446744073709551615,'::ffff:127.0.0.1',X'00000000000000000000FFFF7F000001'),
         (3,4294967296,0,'2001:db8::1',X'20010DB8000000000000000000000001'),
         (4,0,4294967296,X'FF00',X'FF00'),
         (5,NULL,NULL,NULL,NULL)",
        &mut catalog,
        &StmtContext::for_query(),
    )
    .unwrap();
    let native = StmtContext::for_query();
    let shared = StmtContext::for_query().with_shared_projection();
    let sql = "SELECT id,INET_NTOA(n),INET_NTOA(u),INET6_ATON(ip),INET6_NTOA(bin)
        FROM ip_values ORDER BY id LIMIT 5";
    let expected = run_select_meta_on(sql, &catalog, &native).unwrap();
    let actual = run_select_meta_on(sql, &catalog, &shared).unwrap();
    assert_eq!(actual, expected);
    assert_eq!(shared.take_warnings(), native.take_warnings());
    let values = actual
        .1
        .iter()
        .map(|row| row[1..].iter().map(Datum::as_raw_bytes).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    let mapped = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 0x7f, 0, 0, 1];
    let v6 = [0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
    assert_eq!(
        values,
        [
            vec![
                Some(&b"255.255.255.255"[..]),
                Some(b"127.0.0.1"),
                Some(&[0x7f, 0, 0, 1]),
                Some(b"127.0.0.1")
            ],
            vec![None, None, Some(&mapped), Some(b"::ffff:127.0.0.1")],
            vec![None, Some(&b"0.0.0.0"[..]), Some(&v6), Some(b"2001:db8::1")],
            vec![Some(&b"0.0.0.0"[..]), None, None, None],
            vec![None; 4],
        ]
    );
    let route = shared.shared_projection_counts();
    eprintln!("IP conversions projection={route:?}");
    assert_eq!(route, Some((1, 1)));

    let fallback = StmtContext::for_query().with_shared_projection();
    let sql = "SELECT INET_NTOA(ip),IS_IPV4(bin),IS_IPV4_MAPPED(bin),IS_IPV6(bin)
        FROM ip_values WHERE id IN (1,5) ORDER BY id";
    assert_eq!(
        run_select_meta_on(sql, &catalog, &fallback).unwrap(),
        run_select_meta_on(sql, &catalog, &native).unwrap()
    );
    assert_eq!(fallback.take_warnings(), native.take_warnings());
    assert_eq!(fallback.shared_projection_counts(), Some((0, 0)));

    let sql = "SELECT IS_IPV4(ip) FROM ip_values WHERE id = 4";
    let expected = run_select_meta_on(sql, &catalog, &native).unwrap_err();
    let actual = run_select_meta_on(sql, &catalog, &fallback).unwrap_err();
    let actual = actual.to_mysql_error();
    let expected = expected.to_mysql_error();
    assert_eq!(actual.code, expected.code);
    assert_eq!(actual.state, expected.state);
    assert_eq!(actual.message, expected.message);
    assert_eq!(actual.message, "invalid UTF-8 string datum");
}

#[test]
fn shared_sql_ipv4_prefix_predicates_preserve_null_and_raw_bytes() {
    use tidb_executor::{run_create_table_on, run_insert_on, run_select_meta_on, Catalog};

    let mut catalog = Catalog::default();
    run_create_table_on(
        "CREATE TABLE ip_prefixes (id BIGINT PRIMARY KEY, b VARBINARY(20))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO ip_prefixes VALUES
         (1,X'0000000000000000000000007F000001'),
         (2,X'00000000000000000000FFFF7F000001'),
         (3,X'7F000001'),(4,X'FF00000000000000000000007F000001'),
         (5,NULL),(6,''),(7,X'00000000000000000000FFFF7F00000100')",
        &mut catalog,
        &StmtContext::for_query(),
    )
    .unwrap();
    let sql = "SELECT id,IS_IPV4_COMPAT(b),IS_IPV4_MAPPED(b)
        FROM ip_prefixes ORDER BY id LIMIT 7";
    let native = StmtContext::for_query();
    let shared = StmtContext::for_query().with_shared_projection();
    let expected = run_select_meta_on(sql, &catalog, &native).unwrap();
    let actual = run_select_meta_on(sql, &catalog, &shared).unwrap();
    assert_eq!(actual, expected);
    assert_eq!(shared.take_warnings(), native.take_warnings());
    use Datum::{Int, Null};
    assert_eq!(
        actual.1,
        [
            vec![Int(1), Int(1), Int(0)],
            vec![Int(2), Int(0), Int(1)],
            vec![Int(3), Int(0), Int(0)],
            vec![Int(4), Int(0), Int(0)],
            vec![Int(5), Null, Null],
            vec![Int(6), Int(0), Int(0)],
            vec![Int(7), Int(0), Int(0)],
        ]
    );
    let route = shared.shared_projection_counts();
    eprintln!("IP prefix projection={route:?}");
    assert_eq!(route, Some((1, 1)));
}

#[test]
fn shared_sql_ip_text_functions_keep_legacy_parse_and_diagnostics() {
    use tidb_executor::{run_create_table_on, run_insert_on, run_select_meta_on, Catalog};

    let mut catalog = Catalog::default();
    run_create_table_on(
        "CREATE TABLE ip_text (id BIGINT PRIMARY KEY, b VARBINARY(64))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO ip_text VALUES
         (1,'001.002.003.004'),(2,'127.1'),(3,'::ffff:127.0.0.1'),(4,'255.255.255.255'),
         (5,'1..2.3'),(6,'256.1.1.1'),(7,NULL),(8,''),(9,X'FF00')",
        &mut catalog,
        &StmtContext::for_query(),
    )
    .unwrap();
    let sql = "SELECT id,INET_ATON(b),IS_IPV4(b),IS_IPV6(b)
        FROM ip_text ORDER BY id LIMIT 8";
    let native = StmtContext::for_query();
    let shared = StmtContext::for_query().with_shared_projection();
    let expected = run_select_meta_on(sql, &catalog, &native).unwrap();
    let actual = run_select_meta_on(sql, &catalog, &shared).unwrap();
    assert_eq!(actual, expected);
    assert_eq!(shared.take_warnings(), native.take_warnings());
    use Datum::{Int, Null, UInt};
    assert_eq!(
        actual.1,
        [
            vec![Int(1), UInt(0x01020304), Int(1), Int(0)],
            vec![Int(2), UInt(0x7F000001), Int(0), Int(0)],
            vec![Int(3), Null, Int(0), Int(1)],
            vec![Int(4), UInt(u32::MAX.into()), Int(1), Int(0)],
            vec![Int(5), UInt(0x01000203), Int(0), Int(0)],
            vec![Int(6), Null, Int(0), Int(0)],
            vec![Int(7), Null, Null, Null],
            vec![Int(8), Null, Int(0), Int(0)],
        ]
    );
    let route = shared.shared_projection_counts();
    eprintln!("text IP projection={route:?}");
    assert_eq!(route, Some((1, 1)));

    for function in ["INET_ATON", "IS_IPV4", "IS_IPV6"] {
        let native = StmtContext::for_query();
        let shared = StmtContext::for_query().with_shared_projection();
        let sql = format!("SELECT {function}(b) FROM ip_text WHERE id=9");
        let expected = run_select_meta_on(&sql, &catalog, &native)
            .unwrap_err()
            .to_mysql_error();
        let actual = run_select_meta_on(&sql, &catalog, &shared)
            .unwrap_err()
            .to_mysql_error();
        assert_eq!(actual.code, expected.code);
        assert_eq!(actual.state, expected.state);
        assert_eq!(actual.message, "invalid UTF-8 string datum");
        assert_eq!(actual.message, expected.message);
        assert_eq!(actual.is_from_evaluation(), expected.is_from_evaluation());
        assert_eq!(
            shared.shared_projection_counts(),
            Some((1, 1)),
            "{function}"
        );
        assert_eq!(shared.take_warnings(), native.take_warnings());
    }
}

#[test]
fn shared_sql_bin_and_oct_keep_integer_bits_and_string_conversion() {
    use tidb_executor::{run_create_table_on, run_insert_on, run_select_meta_on, Catalog};

    let mut catalog = Catalog::default();
    run_create_table_on(
        "CREATE TABLE radix_values (id BIGINT PRIMARY KEY, n BIGINT, u BIGINT UNSIGNED, s VARCHAR(8))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO radix_values VALUES
         (1,-1,18446744073709551615,'12bad'),
         (2,-9223372036854775808,9223372036854775808,''),
         (3,0,0,'0'),(4,NULL,NULL,NULL)",
        &mut catalog,
        &StmtContext::for_query(),
    )
    .unwrap();
    let sql = "SELECT id,BIN(n),BIN(u),OCT(n),OCT(u)
        FROM radix_values ORDER BY id LIMIT 4";
    let native = StmtContext::for_query();
    let shared = StmtContext::for_query().with_shared_projection();
    let expected = run_select_meta_on(sql, &catalog, &native).unwrap();
    let actual = run_select_meta_on(sql, &catalog, &shared).unwrap();
    assert_eq!(actual, expected);
    assert_eq!(native.take_warnings(), shared.take_warnings());
    for (row, bits) in [(0, u64::MAX), (1, 1 << 63), (2, 0)] {
        let bin = format!("{bits:b}");
        let oct = format!("{bits:o}");
        for (value, expected) in actual.1[row][1..].iter().zip([&bin, &bin, &oct, &oct]) {
            assert_eq!(value.as_raw_bytes(), Some(expected.as_bytes()));
        }
    }
    assert!(actual.1[3][1..].iter().all(Datum::is_null));
    let route = shared.shared_projection_counts();
    eprintln!("radix projection={route:?}");
    assert_eq!(route, Some((1, 1)));

    let fallback = StmtContext::for_query().with_shared_projection();
    let sql = "SELECT BIN(s),OCT(s) FROM radix_values ORDER BY id LIMIT 4";
    assert_eq!(
        run_select_meta_on(sql, &catalog, &fallback).unwrap(),
        run_select_meta_on(sql, &catalog, &native).unwrap()
    );
    let warnings = fallback.take_warnings();
    assert_eq!(warnings, native.take_warnings());
    assert_eq!(warnings.len(), 1);
    assert_eq!(fallback.shared_projection_counts(), Some((0, 0)));
}

#[test]
fn shared_sql_abs_preserves_bigint_sign_and_overflow_diagnostic() {
    use tidb_executor::{run_create_table_on, run_insert_on, run_select_meta_on, Catalog};
    let mut catalog = Catalog::default();
    run_create_table_on(
        "CREATE TABLE abs_values (id BIGINT PRIMARY KEY, n BIGINT, u BIGINT UNSIGNED)",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO abs_values VALUES (1,-7,18446744073709551615),(2,NULL,NULL),(3,-9223372036854775808,0)",
        &mut catalog, &StmtContext::for_query(),
    ).unwrap();
    let native = StmtContext::for_query();
    let shared = StmtContext::for_query().with_shared_projection();
    let sql = "SELECT id,ABS(n),ABS(u) FROM abs_values WHERE id<3 ORDER BY id";
    let actual = run_select_meta_on(sql, &catalog, &shared).unwrap();
    assert_eq!(actual, run_select_meta_on(sql, &catalog, &native).unwrap());
    assert_eq!(
        actual.1,
        [
            vec![Datum::Int(1), Datum::Int(7), Datum::UInt(u64::MAX)],
            vec![Datum::Int(2), Datum::Null, Datum::Null],
        ]
    );
    assert_eq!(shared.shared_projection_counts(), Some((1, 1)));
    let shared = StmtContext::for_query().with_shared_projection();
    let sql = "SELECT ABS(n) FROM abs_values WHERE id=3";
    let actual = run_select_meta_on(sql, &catalog, &shared)
        .unwrap_err()
        .to_mysql_error();
    let expected = run_select_meta_on(sql, &catalog, &native)
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(actual.code, 1690);
    assert_eq!(
        (actual.code, actual.state, actual.message),
        (expected.code, expected.state, expected.message)
    );
    assert_eq!(shared.shared_projection_counts(), Some((1, 1)));
}
