// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Verify the default expression path under the SQL driver.
//! aggregate-test: standalone

use std::sync::Arc;

use tidb_datatype::Datum;
use tidb_executor::{run_create_table_on, run_insert_on, run_select_meta_on, Catalog, StmtContext};

#[test]
fn typed_time_composition_and_seeded_if_in_default_sql() {
    let rng = Arc::new(tidb_expr::MysqlRng::new_with_seed(13));
    let ctx = StmtContext::for_query()
        .with_executor_chunk_sizes(2, 2)
        .with_rand_session(rng.clone());
    let mut catalog = Catalog::default();
    run_create_table_on(
        "CREATE TABLE samples (id BIGINT PRIMARY KEY, flag BIGINT, a TIME(3), b TIME(3))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO samples VALUES
          (1,1,'-25:04:05.123','00:00:01.123'),
          (2,0,'-25:04:05.123','01:23:45.678'),
          (3,1,NULL,'02:03:04.567'),
          (4,NULL,'06:07:08.123','07:08:09.123')",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    let query = "
        SELECT HOUR(a), HOUR(IF(flag,a,b)), IF(flag,RAND(7),0)
        FROM samples ORDER BY id LIMIT 4
    ";
    for vectorized in [true, false] {
        let projection_context = ctx.clone().with_enable_vectorized_expression(vectorized);
        let (_, rows) = run_select_meta_on(query, &catalog, &projection_context).unwrap();
        assert_eq!(
            rows,
            [
                vec![
                    Datum::Int(25),
                    Datum::Int(25),
                    Datum::Real(0.9065021936842261)
                ],
                vec![Datum::Int(25), Datum::Int(1), Datum::Real(0.0)],
                vec![Datum::Null, Datum::Null, Datum::Real(0.37600881361962185)],
                vec![Datum::Int(6), Datum::Int(7), Datum::Real(0.0)],
            ]
        );
    }
    let (_, selected) = run_select_meta_on(
        "SELECT id FROM (SELECT id,flag,a,b FROM samples ORDER BY id LIMIT 4) x
         WHERE HOUR(IF(flag,a,b)) > 7 ORDER BY id",
        &catalog,
        &ctx,
    )
    .unwrap();
    assert_eq!(selected, vec![vec![Datum::Int(1)]]);
    let (_, session_rows) = run_select_meta_on(
        "SELECT RAND() FROM samples ORDER BY id LIMIT 4",
        &catalog,
        &ctx,
    )
    .unwrap();
    let sequence = tidb_expr::MysqlRng::new_with_seed(13);
    assert_eq!(
        session_rows,
        (0..4)
            .map(|_| vec![Datum::Real(sequence.gen())])
            .collect::<Vec<_>>()
    );
    assert_eq!(rng.gen(), sequence.gen());
    let template =
        tidb_parser::parse("SELECT IF(flag,id + ?,id) FROM samples ORDER BY id LIMIT 4").unwrap();
    let plan = Arc::new(
        tidb_executor::build_prepared_select_plan(
            &template,
            1,
            &catalog,
            tidb_executor::DEFAULT_DATABASE,
            &ctx,
        )
        .unwrap(),
    );
    let environment = tidb_executor::PreparedPlanCacheEnvironment::default();
    for (index, (parameter, expected)) in [(10, [11, 2, 13, 4]), (-3, [-2, 2, 0, 4])]
        .into_iter()
        .enumerate()
    {
        let parameters = [Datum::Int(parameter)];
        let execution = if index == 0 {
            plan.bind(
                &parameters,
                &catalog,
                tidb_executor::DEFAULT_DATABASE,
                &ctx,
                &environment,
            )
        } else {
            plan.bind_cached(
                &parameters,
                &catalog,
                tidb_executor::DEFAULT_DATABASE,
                &environment,
            )
        }
        .unwrap();
        assert_eq!(execution.cache_hit(), index != 0);
        let context = StmtContext::for_query().with_prepared_params(execution.parameters());
        let (_, actual) = execution
            .with_plan(|statement, physical| {
                let tidb_ast::Stmt::Query(query) = statement else {
                    panic!("expected prepared SELECT");
                };
                tidb_executor::run_query_meta_stmt_with_physical(
                    query,
                    Some(physical),
                    &catalog,
                    tidb_executor::DEFAULT_DATABASE,
                    &context,
                )
                .unwrap()
            })
            .unwrap();
        assert_eq!(actual, expected.map(|value| vec![Datum::Int(value)]));
    }
    assert!(ctx.take_warnings().is_empty());
}

#[test]
fn where_keeps_one_demanded_seeded_stream_across_input_batches() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "CREATE TABLE samples (id BIGINT PRIMARY KEY, flag BIGINT)",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO samples VALUES (1,0),(2,1),(3,0),(4,1),(5,1),(6,0),(7,1),(8,0)",
        &mut catalog,
        &StmtContext::for_query(),
    )
    .unwrap();

    let expected_rng = tidb_expr::MysqlRng::new_with_seed(7);
    let expected = [2, 4, 5, 7]
        .into_iter()
        .filter(|_| expected_rng.gen() > 0.5)
        .map(|key| vec![Datum::Int(key)])
        .collect::<Vec<_>>();
    let query = "
        SELECT id FROM (SELECT id,flag FROM samples ORDER BY id LIMIT 8) sample
        WHERE IF(flag,RAND(7),0) > 0.5 ORDER BY id
    ";
    for batch_size in [1, 2, 8] {
        let ctx = StmtContext::for_query().with_executor_chunk_sizes(batch_size, batch_size);
        for _ in 0..2 {
            let (_, rows) = run_select_meta_on(query, &catalog, &ctx).unwrap();
            assert_eq!(rows, expected, "batch size {batch_size}");
        }
    }
}

#[test]
fn shared_projection_checks_session_packet_limit_before_string_allocation() {
    let mut catalog = Catalog::default();
    let ctx = StmtContext::for_query().with_max_allowed_packet(4);
    run_create_table_on(
        "CREATE TABLE samples (id BIGINT PRIMARY KEY, n BIGINT)",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO samples VALUES (1,2), (2,3)",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    let (_, rows) = run_select_meta_on(
        "SELECT REPEAT('xy',n) FROM samples ORDER BY id",
        &catalog,
        &ctx,
    )
    .unwrap();
    assert_eq!(rows, [vec![Datum::new_string("xyxy")], vec![Datum::Null]]);
    let warnings = ctx.take_warnings();
    assert_eq!(warnings.len(), 1);
    assert_eq!(warnings[0].1, 1301);
}

#[test]
fn shared_projection_accepts_wide_decimal_literals_and_vector_extrema() {
    let mut catalog = Catalog::default();
    let ctx = StmtContext::for_query();
    run_create_table_on(
        "CREATE TABLE vectors (id BIGINT PRIMARY KEY, v VECTOR(2))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO vectors VALUES (1,'[1,2]'), (2,NULL)",
        &mut catalog,
        &ctx,
    )
    .unwrap();

    let query = "
        SELECT FORMAT(12332.12341111111111111111111111111111111111111, 4),
               FORMAT(1.00000000000000000000000000000059, 30),
               GREATEST(v,'[1,3]'), LEAST(v,'[1,3]')
        FROM vectors ORDER BY id
    ";
    for vectorized in [false, true] {
        let ctx = ctx.clone().with_enable_vectorized_expression(vectorized);
        let (_, rows) = run_select_meta_on(query, &catalog, &ctx).unwrap();
        assert_eq!(rows[0][0].sql_string().unwrap(), "12,332.1234");
        assert_eq!(
            rows[0][1].sql_string().unwrap(),
            "1.000000000000000000000000000001"
        );
        assert_eq!(rows[0][2].sql_string().unwrap(), "[1,3]");
        assert_eq!(rows[0][3].sql_string().unwrap(), "[1,2]");
        assert_eq!(rows[1][2..], [Datum::Null, Datum::Null]);

        let (_, rows) = run_select_meta_on(
            "SELECT GREATEST(v, 'bad vector') FROM vectors WHERE id = 2",
            &catalog,
            &ctx,
        )
        .unwrap();
        assert_eq!(rows, [vec![Datum::Null]]);
    }
}

#[test]
fn shared_projection_keeps_all_warnings_in_a_batch() {
    let mut catalog = Catalog::default();
    let ctx = StmtContext::for_query().with_executor_chunk_sizes(128, 128);
    run_create_table_on(
        "CREATE TABLE warning_rows (id BIGINT PRIMARY KEY, s VARCHAR(32))",
        &mut catalog,
    )
    .unwrap();
    let values = (0..128)
        .map(|row| format!("({row},'bad{row:03}')"))
        .collect::<Vec<_>>()
        .join(",");
    run_insert_on(
        &format!("INSERT INTO warning_rows VALUES {values}"),
        &mut catalog,
        &ctx,
    )
    .unwrap();

    for vectorized in [false, true] {
        let ctx = ctx.clone().with_enable_vectorized_expression(vectorized);
        let (_, rows) = run_select_meta_on(
            "SELECT CAST(s AS SIGNED) FROM warning_rows ORDER BY id",
            &catalog,
            &ctx,
        )
        .unwrap();
        assert_eq!(rows, vec![vec![Datum::Int(0)]; 128]);
        assert_eq!(ctx.warning_count(), 128, "vectorized={vectorized}");
        for (row, (_, code, message)) in ctx.take_warnings().iter().enumerate() {
            assert_eq!(*code, 1292);
            assert!(message.contains(&format!("'bad{row:03}'")), "{message}");
        }
    }
}

#[test]
fn shared_projection_preserves_if_rand_row_order_and_stops_at_error() {
    const SEED: i64 = 13;
    let mut catalog = Catalog::default();
    let ctx = StmtContext::for_query().with_executor_chunk_sizes(2, 2);
    run_create_table_on(
        "CREATE TABLE rand_rows (id BIGINT PRIMARY KEY, flag BIGINT, x DOUBLE)",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO rand_rows VALUES (1,0,10000), (2,1,0)",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    for vectorized in [false, true] {
        let rng = Arc::new(tidb_expr::MysqlRng::new_with_seed(SEED));
        let ctx = ctx
            .clone()
            .with_enable_vectorized_expression(vectorized)
            .with_rand_session(rng.clone());
        let unexpected = run_select_meta_on(
            "SELECT IF(flag,RAND(),EXP(x)) FROM rand_rows ORDER BY id",
            &catalog,
            &ctx,
        )
        .unwrap_err();
        assert_eq!(unexpected.to_mysql_error().code, 1690);
        let expected = tidb_expr::MysqlRng::new_with_seed(SEED);
        assert_eq!(rng.gen(), expected.gen(), "vectorized={vectorized}");

        let (_, rows) = run_select_meta_on(
            "SELECT IF(flag,RAND(),RAND()) FROM rand_rows ORDER BY id",
            &catalog,
            &ctx,
        )
        .unwrap();
        assert_eq!(
            rows,
            [
                vec![Datum::Real(expected.gen())],
                vec![Datum::Real(expected.gen())]
            ],
            "vectorized={vectorized}",
        );
        assert_eq!(rng.gen(), expected.gen());
    }
}

#[test]
fn deeply_nested_if_preserves_logical_short_circuit() {
    let mut catalog = Catalog::default();
    let ctx = StmtContext::for_query();
    run_create_table_on(
        "CREATE TABLE deep_if (k BIGINT, z BIGINT, n BIGINT)",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO deep_if VALUES (1,0,9223372036854775807)",
        &mut catalog,
        &ctx,
    )
    .unwrap();

    for depth in [31, 32] {
        let expr = (0..depth).fold("z AND (n+1)".to_owned(), |child, _| {
            format!("IF(k,{child},0)")
        });
        for vectorized in [false, true] {
            let ctx = ctx.clone().with_enable_vectorized_expression(vectorized);
            let (_, rows) =
                run_select_meta_on(&format!("SELECT {expr} FROM deep_if"), &catalog, &ctx).unwrap();
            assert_eq!(rows, [vec![Datum::Int(0)]], "depth={depth}");
            assert_eq!(ctx.warning_count(), 0);
        }
    }
}

#[test]
fn addtime_with_datetime_rhs_leaves_both_operands_unread() {
    let mut catalog = Catalog::default();
    let ctx = StmtContext::for_query();
    run_create_table_on(
        "CREATE TABLE temporal_rows (t TIME, d DATETIME, x DOUBLE)",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO temporal_rows VALUES ('01:02:03','2024-01-01 12:30:00',10000)",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    for vectorized in [false, true] {
        let ctx = ctx.clone().with_enable_vectorized_expression(vectorized);
        let (_, rows) = run_select_meta_on(
            "SELECT ADDTIME(t,d), ADDTIME(MAKETIME(0,0,EXP(x)),d),
                    ADDTIME(t,CAST(EXP(x) AS DATETIME)) FROM temporal_rows",
            &catalog,
            &ctx,
        )
        .unwrap();
        assert_eq!(rows, [vec![Datum::Null, Datum::Null, Datum::Null]]);
        assert_eq!(ctx.warning_count(), 0);
    }
}

#[test]
fn row_ordered_rand_respects_scalar_null_demand_in_all_chunk_modes() {
    const SEED: i64 = 13;
    let mut catalog = Catalog::default();
    let ctx = StmtContext::for_query();
    run_create_table_on(
        "CREATE TABLE rng_demo (id BIGINT PRIMARY KEY, x DOUBLE)",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO rng_demo VALUES (1,NULL),(2,2)",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    for size in [1, 2] {
        for vectorized in [false, true] {
            for expression in ["RAND()-RAND()", "x-RAND()", "x+RAND()"] {
                let rng = Arc::new(tidb_expr::MysqlRng::new_with_seed(SEED));
                let expected = tidb_expr::MysqlRng::new_with_seed(SEED);
                let want = match expression {
                    "RAND()-RAND()" => [0, 1].map(|_| Datum::Real(expected.gen() - expected.gen())),
                    "x-RAND()" => [Datum::Null, Datum::Real(2.0 - expected.gen())],
                    "x+RAND()" => {
                        // Real addition reads the right operand even after NULL.
                        expected.gen();
                        [Datum::Null, Datum::Real(2.0 + expected.gen())]
                    }
                    _ => unreachable!(),
                };
                let ctx = StmtContext::for_query()
                    .with_executor_chunk_sizes(size, size)
                    .with_enable_vectorized_expression(vectorized)
                    .with_rand_session(rng.clone());
                let (_, rows) = run_select_meta_on(
                    &format!("SELECT {expression} FROM rng_demo ORDER BY id"),
                    &catalog,
                    &ctx,
                )
                .unwrap();
                assert_eq!(
                    rows,
                    want.map(|value| vec![value]),
                    "{expression}, {size}, {vectorized}"
                );
                assert_eq!(rng.gen(), expected.gen());
            }
        }
    }
}

#[cfg(test)]
mod reviewed_boundaries {
    use std::cell::Cell;

    use tidb_ast::CiString;
    use tidb_chunk::chunk::Chunk;
    use tidb_datatype::{Datum, FieldType, FieldTypeCode};
    use tidb_executor::{
        run_create_table_on, run_insert_on, run_select_meta_on, Catalog, StmtContext,
    };
    use tidb_expr::{
        column::Column, expression::Expression, scalar_function::ScalarFunction, Columns,
        EvalError, NoColumns,
    };

    #[test]
    fn mutate_packet_settings_on_the_same_statement() {
        let integer = FieldType::new(FieldTypeCode::LongLong);
        let text = FieldType::new(FieldTypeCode::VarString);
        let expression = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("space"),
            text,
            vec![Expression::Column(Column::new(0, integer.clone()))],
        ));
        let mut chunk = Chunk::new(&[integer], 1, 1);
        chunk.append_datum(0, &Datum::Int(2000));
        let ctx = StmtContext::for_query();
        assert_eq!(
            expression.eval_reusing(&ctx, chunk.get_row(0)).unwrap(),
            Datum::new_string(" ".repeat(2000))
        );
        let ctx = ctx.with_max_allowed_packet(1024);
        assert_eq!(
            expression.eval_reusing(&ctx, chunk.get_row(0)).unwrap(),
            Datum::Null
        );
        assert_eq!(ctx.take_warnings()[0].1, 1301);
    }

    #[test]
    fn ddl_owner_reads_provider_only_when_logically_demanded() {
        #[derive(Default)]
        struct Owner(Cell<usize>);

        impl Columns for Owner {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            fn ddl_owner_info(&self) -> Result<bool, EvalError> {
                self.0.set(self.0.get() + 1);
                NoColumns.ddl_owner_info()
            }
        }

        let integer = FieldType::new(FieldTypeCode::LongLong);
        let mut input = Chunk::new(&[integer.clone()], 2, 2);
        for value in [0, 1] {
            input.append_datum(0, &Datum::Int(value));
        }
        for (operator, skip, expected) in [("and", 0, 0), ("or", 1, 1)] {
            let owner = Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("tidb_is_ddl_owner"),
                integer.clone(),
                vec![],
            ));
            let expression = Expression::ScalarFunction(ScalarFunction::new(
                CiString::new(operator),
                integer.clone(),
                vec![Expression::Column(Column::new(0, integer.clone())), owner],
            ));
            let context = Owner::default();
            assert_eq!(
                expression.eval(&context, input.get_row(skip)),
                Ok(Datum::Int(expected))
            );
            assert_eq!(context.0.get(), 0);
            assert_eq!(
                expression.eval(&context, input.get_row(1 - skip)),
                Err(NoColumns.ddl_owner_info().unwrap_err())
            );
            assert_eq!(context.0.get(), 1);
        }
    }

    #[test]
    fn addtime_preserves_date_vs_datetime_rhs_demand() {
        let mut catalog = Catalog::default();
        let ctx = StmtContext::for_query();
        run_create_table_on(
            "CREATE TABLE a (d DATE, dt DATETIME, ts TIMESTAMP, x DOUBLE)",
            &mut catalog,
        )
        .unwrap();
        run_insert_on(
            "INSERT INTO a VALUES ('2024-01-01','2024-01-01 00:00:00','2024-01-01 00:00:00',10000)",
            &mut catalog,
            &ctx,
        )
        .unwrap();
        for mode in [false, true] {
            let ctx = ctx.clone().with_enable_vectorized_expression(mode);
            let error = run_select_meta_on(
                "SELECT ADDTIME(MAKETIME(0,0,EXP(x)),d) FROM a",
                &catalog,
                &ctx,
            )
            .unwrap_err();
            assert_eq!(error.to_mysql_error().code, 1690);
            let (_, date) = run_select_meta_on(
                "SELECT ADDTIME(CAST('01:00:00' AS TIME),d) FROM a",
                &catalog,
                &ctx,
            )
            .unwrap();
            assert_eq!(date, [vec![Datum::Null]]);
            assert_eq!(ctx.warning_count(), 0);
            let (_, rows) = run_select_meta_on(
                "SELECT ADDTIME(MAKETIME(0,0,EXP(x)),dt), ADDTIME(MAKETIME(0,0,EXP(x)),ts) FROM a",
                &catalog,
                &ctx,
            )
            .unwrap();
            assert_eq!(rows, [vec![Datum::Null, Datum::Null]]);
        }
    }

    #[test]
    fn datetime_addtime_preserves_string_duration_precision_and_diagnostics() {
        let mut catalog = Catalog::default();
        let ctx = StmtContext::for_query();
        run_create_table_on(
            "CREATE TABLE addtime_values (id BIGINT PRIMARY KEY, d DATETIME(3), delay VARCHAR(24))",
            &mut catalog,
        )
        .unwrap();
        run_insert_on(
            "INSERT INTO addtime_values VALUES
                (1,'2024-01-01 12:00:00.123','00:00:30.456'),
                (2,'2024-01-01 12:00:00.123','bad-duration'),
                (3,'2024-01-01 12:00:00.123','00:00:30.456789'),
                (4,'2024-01-01 12:00:00.123','2024-01-01 00:00:30'),
                (5,'2024-01-01 12:00:00.123','839:00:00')",
            &mut catalog,
            &ctx,
        )
        .unwrap();
        for vectorized in [false, true] {
            let ctx = ctx.clone().with_enable_vectorized_expression(vectorized);
            let (_, rows) = run_select_meta_on(
                "SELECT ADDTIME(d,delay) FROM addtime_values ORDER BY id",
                &catalog,
                &ctx,
            )
            .unwrap();
            let Datum::Time(value) = &rows[0][0] else {
                panic!("{rows:?}")
            };
            assert_eq!(value.to_string(), "2024-01-01 12:00:30.579");
            assert_eq!(rows[1][0], Datum::Null);
            let Datum::Time(value) = &rows[2][0] else {
                panic!("{rows:?}")
            };
            assert_eq!(value.to_string(), "2024-01-01 12:00:30.579789");
            assert_eq!(rows[3][0], Datum::Null);
            assert_eq!(rows[4][0], Datum::Null);
            let warnings = ctx.take_warnings();
            assert_eq!(warnings.len(), 1);
            assert_eq!(warnings[0].1, 1292);
            assert_eq!(warnings[0].2, "Truncated incorrect time value: '839:00:00'");
        }
    }

    #[test]
    fn rand_text_constant_and_column_seed_have_distinct_lifetimes() {
        let mut catalog = Catalog::default();
        let ctx = StmtContext::for_query();
        run_create_table_on(
            "CREATE TABLE seeds (id BIGINT PRIMARY KEY, seed BIGINT)",
            &mut catalog,
        )
        .unwrap();
        run_insert_on(
            "INSERT INTO seeds VALUES (1,9007199254740993),(2,9007199254740993)",
            &mut catalog,
            &ctx,
        )
        .unwrap();
        for size in [1, 2] {
            for mode in [false, true] {
                let ctx = ctx
                    .clone()
                    .with_executor_chunk_sizes(size, size)
                    .with_enable_vectorized_expression(mode);
                let (_, rows) = run_select_meta_on(
                    "SELECT RAND(9007199254740993), RAND('9007199254740993'), RAND(seed) FROM seeds ORDER BY id",
                    &catalog,
                    &ctx,
                )
                .unwrap();
                let rng = tidb_expr::MysqlRng::new_with_seed(9_007_199_254_740_993);
                let first = Datum::Real(rng.gen());
                let second = Datum::Real(rng.gen());
                assert_eq!(
                    rows,
                    [
                        vec![first.clone(), first.clone(), first.clone()],
                        vec![second.clone(), second, first]
                    ],
                    "{size} {mode}"
                );
            }
        }
    }

    #[test]
    fn statement_rand_shares_first_demand_across_scalar_entry_points() {
        use std::sync::{Arc, Barrier};
        use tidb_expr::{
            rewriter::ColumnResolver,
            simple_expr::{parse_simple_expr, BuildOptions},
        };

        struct FoldContext(StmtContext);

        impl ColumnResolver for FoldContext {
            fn resolve(&self, _: &[String]) -> Option<(usize, FieldType, i64)> {
                None
            }

            fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
                tidb_datatype::SessionTimeZone::utc()
            }

            fn param_value(&self, order: usize) -> Result<Datum, EvalError> {
                self.0.param_value(order)
            }

            fn fold_constant(&self, expr: &mut Expression, mode: tidb_expr::ConstantFoldMode) {
                tidb_expr::fold_constant_in_mode(expr, &self.0, mode);
            }
        }

        let bind = |text: &str| {
            StmtContext::for_query().with_prepared_params(Arc::from([Datum::new_string(text)]))
        };
        let planning = FoldContext(bind("7"));
        let options = BuildOptions::new();
        let integer = FieldType::new(FieldTypeCode::LongLong);
        let mut input = Chunk::new(&[integer], 2, 2);
        input.append_int64(0, 1);
        input.append_int64(0, 2);

        for sql in ["RAND(?)", "RAND(CAST(? AS SIGNED))"] {
            let expression = parse_simple_expr(&planning, sql, &options).unwrap();
            if sql.contains("CAST") {
                assert!(matches!(
                    &expression,
                    Expression::ScalarFunction(rand)
                        if matches!(rand.args.as_slice(), [Expression::Constant(seed)]
                            if matches!(seed.deferred_expr.as_deref(), Some(Expression::ScalarFunction(cast))
                                if matches!(cast.args.as_slice(), [Expression::Constant(marker)]
                                    if marker.param_marker.is_some())))
                ));
            }
            let evaluate = |reuse, context: &StmtContext, row| {
                if reuse {
                    expression.eval_reusing(context, input.get_row(row))
                } else {
                    expression.eval(context, input.get_row(row))
                }
                .unwrap()
            };
            for (text, seed) in [("7.9", 7), ("11.9", 11)] {
                for routes in [[false, false], [true, true], [false, true], [true, false]] {
                    let context = bind(text);
                    let rng = tidb_expr::MysqlRng::new_with_seed(seed);
                    for (row, reuse) in routes.into_iter().enumerate() {
                        assert_eq!(evaluate(reuse, &context, row), Datum::Real(rng.gen()));
                        assert_eq!(context.warning_count(), 1, "{sql} {routes:?}");
                    }
                    let warnings = context.take_warnings();
                    assert_eq!(warnings[0].1, 1292);
                    assert_eq!(
                        warnings[0].2,
                        format!("Truncated incorrect INTEGER value: '{text}'")
                    );
                }
            }

            let real = FieldType::new(FieldTypeCode::Double);
            let zero = tidb_expr::constant::Constant::new(Datum::Real(0.0), real.clone());
            let dead = Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("if"),
                real,
                vec![
                    Expression::Column(Column::new(0, FieldType::new(FieldTypeCode::LongLong))),
                    Expression::Constant(zero),
                    expression.clone(),
                ],
            ));
            let unbound = StmtContext::for_query();
            assert_eq!(
                dead.eval(&unbound, input.get_row(0)).unwrap(),
                Datum::Real(0.0)
            );
            assert_eq!(unbound.warning_count(), 0);

            let context = bind("7.9");
            let barrier = Barrier::new(2);
            let mut draws = std::thread::scope(|scope| {
                let run = |reuse, row| {
                    let context = &context;
                    let barrier = &barrier;
                    scope.spawn(move || {
                        barrier.wait();
                        evaluate(reuse, context, row).label()
                    })
                };
                [run(false, 0), run(true, 1)].map(|thread| thread.join().unwrap())
            });
            draws.sort();
            let rng = tidb_expr::MysqlRng::new_with_seed(7);
            let mut expected = [
                Datum::Real(rng.gen()).label(),
                Datum::Real(rng.gen()).label(),
            ];
            expected.sort();
            assert_eq!(draws, expected);
            assert_eq!(context.warning_count(), 1);

            let next = context
                .clone()
                .with_prepared_params(Arc::from([Datum::new_string("11.9")]));
            let next_rng = tidb_expr::MysqlRng::new_with_seed(11);
            assert_eq!(evaluate(false, &next, 0), Datum::Real(next_rng.gen()));
            assert_eq!(evaluate(true, &next, 1), Datum::Real(next_rng.gen()));
            assert_eq!(evaluate(false, &context, 1), Datum::Real(rng.gen()));
            let warnings = context.take_warnings();
            assert_eq!(warnings.len(), 2);
            assert_eq!(warnings[1].1, 1292);
            assert_eq!(warnings[1].2, "Truncated incorrect INTEGER value: '11.9'");
        }
    }
}

#[test]
fn computed_count_replays_diagnostics_without_repeating_effects() {
    let rng = Arc::new(tidb_expr::MysqlRng::new_with_seed(31));
    let ctx = StmtContext::for_query()
        .with_executor_chunk_sizes(4, 4)
        .with_enable_vectorized_expression(false)
        .with_rand_session(rng.clone());
    let mut catalog = Catalog::default();
    run_create_table_on(
        "CREATE TABLE count_inputs (id BIGINT PRIMARY KEY, z BIGINT)",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO count_inputs VALUES (1,2),(2,0),(3,NULL),(4,3)",
        &mut catalog,
        &ctx,
    )
    .unwrap();

    for query in [
        "SELECT COUNT(7 MOD z) FROM count_inputs",
        "SELECT COUNT(CAST(RAND()*10 AS SIGNED) MOD z) FROM count_inputs",
    ] {
        let (_, rows) = run_select_meta_on(query, &catalog, &ctx).unwrap();
        assert_eq!(rows, vec![vec![Datum::Int(2)]]);
        let warnings = ctx.take_warnings();
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].1, 1365);
    }
    let sequence = tidb_expr::MysqlRng::new_with_seed(31);
    for _ in 0..4 {
        sequence.gen();
    }
    assert_eq!(rng.gen(), sequence.gen());

    let (_, rows) =
        run_select_meta_on("SELECT SUM(z+1) FROM count_inputs", &catalog, &ctx).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].sql_string().unwrap(), "8");
    assert!(ctx.take_warnings().is_empty());
}

mod extract_speculation {
    use super::*;
    use tidb_ast::CiString;
    use tidb_chunk::chunk::Chunk;
    use tidb_datatype::{FieldType, FieldTypeCode};
    use tidb_executor::{
        AggFunc, AggKind, ExecError, Executor, ExecutorMeta, HashAggExec, MemTableSourceExec,
        StatementMemory,
    };
    use tidb_expr::{
        column::Column, expression::Expression, scalar_function::ScalarFunction, schema::Schema,
        EvalError,
    };

    fn long() -> FieldType {
        FieldType::new(FieldTypeCode::LongLong)
    }

    fn typed_col(index: i64, field: FieldType) -> Expression {
        let mut column = Column::new(index + 1, field);
        column.index = index;
        Expression::Column(column)
    }

    fn col(index: i64) -> Expression {
        typed_col(index, long())
    }

    fn meta(fields: &[FieldType], rows: usize) -> ExecutorMeta {
        let columns = fields
            .iter()
            .enumerate()
            .map(|(index, field)| {
                let Expression::Column(column) = typed_col(index as i64, field.clone()) else {
                    unreachable!();
                };
                column
            })
            .collect();
        ExecutorMeta::new(Schema::new(columns), 0, rows, 1024)
    }

    #[test]
    fn computed_count_does_not_speculate_host_casts() {
        let extract = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("extract"),
            long(),
            vec![col(0), col(1)],
        ));
        let direct = crate::StmtContext::for_query();
        let mut inputs = Chunk::new_with_capacity(&[long(), long()], 2);
        for (unit, value) in [(7, -1), (7, 20240315)] {
            inputs.append_int64(0, unit);
            inputs.append_int64(1, value);
        }
        assert_eq!(
            extract.eval(&direct, inputs.get_row(0)).unwrap(),
            Datum::Null
        );
        let direct_error = extract.eval(&direct, inputs.get_row(1)).unwrap_err();
        let EvalError::Conversion(ref unit_error) = direct_error else {
            panic!("expected invalid-unit conversion error, got {direct_error:?}");
        };
        assert_eq!(unit_error.to_sql_error().code, 1105);
        assert_eq!(unit_error.to_sql_error().message, "invalid unit 7");
        let expected_warnings = direct.take_warnings();
        assert_eq!(expected_warnings.len(), 1);
        assert_eq!(expected_warnings[0].1, 1292);

        let ctx = StmtContext::for_query().with_hashagg_concurrency(1, 1);
        let mut agg = HashAggExec::new(
            meta(&[long()], 1),
            vec![],
            vec![AggFunc::new(AggKind::Count, Some(extract))],
            Box::new(MemTableSourceExec::new(
                meta(&[long(), long()], 2),
                vec![
                    vec![Datum::Int(7), Datum::Int(-1)],
                    vec![Datum::Int(7), Datum::Int(20240315)],
                ],
            )),
            ctx.clone(),
            StatementMemory::default(),
        );
        let mut output = agg.new_chunk();
        agg.open().unwrap();
        let error = agg.next(&mut output).unwrap_err();
        agg.close().unwrap();
        let ExecError::Eval(EvalError::Conversion(error)) = error else {
            panic!("expected executor invalid-unit conversion error, got {error:?}");
        };
        assert_eq!(error.to_string(), unit_error.to_string());
        assert_eq!(ctx.take_warnings(), expected_warnings);
    }

    #[test]
    fn computed_count_skips_null_dead_host_cast_and_keeps_integer_batch() {
        // Column 1 is the NULL left operand, column 2 the invalid date.
        let ctx = StmtContext::for_query().with_hashagg_concurrency(1, 1);
        let extract = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("extract"),
            long(),
            vec![col(0), col(2)],
        ));
        let add = |left, right| {
            Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("plus"),
                long(),
                vec![left, right],
            ))
        };
        let hazardous = add(col(1), extract);
        let ordinary = add(col(1), col(2));
        let fields = [long(), long(), long()];
        let mut data = Chunk::new_with_capacity(&fields, 1);
        data.append_int64(0, 7);
        data.append_null(1);
        data.append_int64(2, -1);
        let child = MemTableSourceExec::new(
            meta(&fields, 1),
            vec![vec![Datum::Int(7), Datum::Null, Datum::Int(-1)]],
        );
        let mut agg = HashAggExec::new(
            meta(&[long()], 1),
            vec![],
            vec![AggFunc::new(AggKind::Count, Some(hazardous))],
            Box::new(child),
            ctx.clone(),
            StatementMemory::default(),
        );
        let mut presence = Vec::new();
        ordinary.append_quiet_int_presence(&ctx, &data, &mut presence);
        assert_eq!(presence, [false]);
        let mut output = agg.new_chunk();
        agg.open().unwrap();
        agg.next(&mut output).unwrap();
        assert_eq!(output.get_row(0).get_int64(0), 0);
        assert_eq!(ctx.take_warnings(), []);
        agg.close().unwrap();
    }

    #[test]
    fn dynamic_extract_bit_unit_keeps_original_diagnostic() {
        use tidb_datatype::{BinaryLiteral, BinaryLiteralWidth};

        let bit = FieldType::new(FieldTypeCode::Bit).with_flen(8);
        let input_types = [bit.clone(), long()];
        let mut input = Chunk::new_with_capacity(&input_types, 2);
        for value in [20240315, -1] {
            input.append_datum(
                0,
                &Datum::Bit(BinaryLiteral::from_uint(
                    255,
                    Some(BinaryLiteralWidth::try_from(1u8).unwrap()),
                )),
            );
            input.append_int64(1, value);
        }

        let expression = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("extract"),
            long(),
            vec![typed_col(0, bit), col(1)],
        ));
        let ctx = StmtContext::for_query();
        let error = expression.eval(&ctx, input.get_row(0)).unwrap_err();
        assert!(
            matches!(&error, EvalError::Conversion(error)
                if error.to_sql_error().code == 1105
                && error.to_sql_error().message == "invalid unit �"),
            "{error:?}"
        );
        assert_eq!(ctx.warning_count(), 0);
        assert_eq!(
            expression.eval(&ctx, input.get_row(1)).unwrap(),
            Datum::Null
        );
        let warnings = ctx.take_warnings();
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].1, 1292);
    }
}
