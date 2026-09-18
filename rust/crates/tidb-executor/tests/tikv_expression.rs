// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Local SQL -> planner -> projection coverage; no TiKV server is needed.
//! Run from `rust/`: `cargo test -p tidb-executor --features tikv-expr --test all tikv_expression`.
#![cfg(feature = "tikv-expr")]

use tidb_datatype::{Datum, SessionTimeZone};
use tidb_executor::{run_create_table_on, run_insert_on, run_select_on, Catalog, StmtContext};
use tidb_expr::Columns;

fn fixture() -> Catalog {
    let mut catalog = Catalog::default();
    run_create_table_on("CREATE TABLE t (a BIGINT, b BIGINT)", &mut catalog).unwrap();
    run_insert_on(
        "INSERT INTO t VALUES (1,2),(3,NULL),(-4,5),(7,0)",
        &mut catalog,
        &StmtContext::for_query(),
    )
    .unwrap();
    catalog
}

#[test]
fn tikv_expression_sql_projection_matches_native_and_is_opt_in() {
    let catalog = fixture();
    let native = StmtContext::for_query();
    let tikv = StmtContext::for_query().with_tikv_expression(true);
    let sql = "SELECT a+b, (a+b)+3, a<b FROM t WHERE a<>7 ORDER BY a";
    let expected = run_select_on(sql, &catalog, &native).unwrap();
    assert_eq!(
        expected,
        vec![
            vec![Datum::Int(1), Datum::Int(4), Datum::Int(1)],
            vec![Datum::Int(3), Datum::Int(6), Datum::Int(1)],
            vec![Datum::Null, Datum::Null, Datum::Null],
        ]
    );
    assert_eq!(run_select_on(sql, &catalog, &tikv).unwrap(), expected);
    assert_eq!(native.tikv_expression_rows(), 0);
    assert!(native.tikv_expression_context().is_none());
    assert!(
        tikv.tikv_expression_rows() > 0,
        "must execute TiKV, not just fall back"
    );
    assert!(tikv.take_warnings().is_empty());
}

#[test]
fn tikv_expression_sql_comparison_is_not_only_native_fallback() {
    let catalog = fixture();
    for sql in [
        "SELECT a>b FROM t ORDER BY a",
        "SELECT a=b FROM t ORDER BY a",
    ] {
        let native = StmtContext::for_query();
        let tikv = StmtContext::for_query().with_tikv_expression(true);
        assert_eq!(
            run_select_on(sql, &catalog, &tikv).unwrap(),
            run_select_on(sql, &catalog, &native).unwrap()
        );
        assert!(
            tikv.tikv_expression_rows() >= 4,
            "{sql} must execute in TiKV"
        );
    }
}

#[test]
fn tikv_expression_sql_double_arithmetic_preserves_inferred_types() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "CREATE TABLE doubles (id BIGINT, a DOUBLE, b DOUBLE)",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO doubles VALUES (1,1.5,2.25),(2,-4,0.5),(3,3,NULL)",
        &mut catalog,
        &StmtContext::for_query(),
    )
    .unwrap();
    // Check each expression separately so one supported neighbor cannot hide fallback.
    for expression in ["a+b", "a-b", "a*b", "(a+b)*b", "ABS(a)"] {
        let sql = format!("SELECT {expression} FROM doubles ORDER BY id");
        let native = StmtContext::for_query().with_tikv_expression(false);
        let tikv = StmtContext::for_query().with_tikv_expression(true);
        let expected = tidb_executor::run_select_meta_on(&sql, &catalog, &native).unwrap();
        let actual = tidb_executor::run_select_meta_on(&sql, &catalog, &tikv).unwrap();
        assert_eq!(expected.0[0].1.eval_type(), tidb_datatype::EvalType::Real);
        assert_eq!(expected.1.len(), 3);
        assert_eq!(actual, expected, "{expression}");
        assert_eq!(native.tikv_expression_rows(), 0, "flag-off {expression}");
        assert!(
            tikv.tikv_expression_rows() >= 3,
            "{expression} must execute in TiKV"
        );
        assert!(tikv.take_warnings().is_empty());
    }
}

#[test]
fn tikv_expression_sql_varchar_length_counts_bytes_and_preserves_null() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "CREATE TABLE strings (id BIGINT, s VARCHAR(32)) CHARACTER SET utf8mb4",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO strings VALUES (1,''),(2,'Aé'),(3,'a\\0b'),(4,NULL)",
        &mut catalog,
        &StmtContext::for_query(),
    )
    .unwrap();
    let sql = "SELECT LENGTH(s) FROM strings ORDER BY id";
    let native = StmtContext::for_query().with_tikv_expression(false);
    let tikv = StmtContext::for_query().with_tikv_expression(true);
    let expected = tidb_executor::run_select_meta_on(sql, &catalog, &native).unwrap();
    let actual = tidb_executor::run_select_meta_on(sql, &catalog, &tikv).unwrap();
    assert_eq!(expected.0[0].1.eval_type(), tidb_datatype::EvalType::Int);
    assert_eq!(
        expected.1,
        vec![
            vec![Datum::Int(0)],
            vec![Datum::Int(3)],
            vec![Datum::Int(3)],
            vec![Datum::Null]
        ]
    );
    assert_eq!(actual, expected);
    assert_eq!(native.tikv_expression_rows(), 0);
    assert!(
        tikv.tikv_expression_rows() >= 4,
        "LENGTH must execute in TiKV"
    );
    assert!(tikv.take_warnings().is_empty());
}

#[test]
fn tikv_expression_sql_decimal_reuses_engine_with_backend_enabled() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "CREATE TABLE decimals (id BIGINT, a DECIMAL(20,6), b DECIMAL(20,6))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO decimals VALUES (1,1.234567,2.000001),(2,-4.500000,0.000001),(3,3,NULL)",
        &mut catalog,
        &StmtContext::for_query(),
    )
    .unwrap();
    let sql = "SELECT a+b, a*b, (a+b)*b FROM decimals ORDER BY id";
    let native = StmtContext::for_query().with_tikv_expression(false);
    let tikv = StmtContext::for_query().with_tikv_expression(true);
    let expected = tidb_executor::run_select_meta_on(sql, &catalog, &native).unwrap();
    let actual = tidb_executor::run_select_meta_on(sql, &catalog, &tikv).unwrap();
    assert!(expected
        .0
        .iter()
        .all(|(_, ty)| ty.eval_type() == tidb_datatype::EvalType::Decimal));
    assert_eq!(expected.1.len(), 3);
    assert_eq!(expected.1[2], vec![Datum::Null, Datum::Null, Datum::Null]);
    assert_eq!(actual, expected);
    assert_eq!(native.tikv_expression_rows(), 0);
    assert!(
        tikv.tikv_expression_rows() >= 4,
        "exact Decimal arithmetic must execute in TiKV"
    );
    assert_eq!(tikv.take_warnings(), native.take_warnings());
}

#[test]
fn tikv_expression_sql_large_projection_crosses_chunk_boundaries() {
    let mut catalog = Catalog::default();
    run_create_table_on("CREATE TABLE batch (a BIGINT, b BIGINT)", &mut catalog).unwrap();
    let values = (0..5003)
        .map(|i| {
            if i % 7 == 0 {
                format!("({i},NULL)")
            } else {
                format!("({i},{})", i * 2)
            }
        })
        .collect::<Vec<_>>()
        .join(",");
    run_insert_on(
        &format!("INSERT INTO batch VALUES {values}"),
        &mut catalog,
        &StmtContext::for_query(),
    )
    .unwrap();
    let native = StmtContext::for_query().with_executor_chunk_sizes(32, 128);
    let tikv = StmtContext::for_query()
        .with_executor_chunk_sizes(32, 128)
        .with_tikv_expression(true);
    let sql = "SELECT (a+b)+1 FROM batch WHERE a>=17 ORDER BY a";
    let expected = run_select_on(sql, &catalog, &native).unwrap();
    assert_eq!(expected.len(), 5003 - 17);
    assert_eq!(run_select_on(sql, &catalog, &tikv).unwrap(), expected);
    assert!(tikv.tikv_expression_rows() >= expected.len() as u64);
}

#[test]
fn tikv_expression_lazy_case_runs_in_engine_without_the_dead_branch() {
    let catalog = fixture();
    let native = StmtContext::for_query();
    let tikv = StmtContext::for_query().with_tikv_expression(true);
    let sql = "SELECT CASE WHEN b=0 THEN 7 ELSE a DIV b END FROM t ORDER BY a";
    assert_eq!(
        run_select_on(sql, &catalog, &tikv).unwrap(),
        run_select_on(sql, &catalog, &native).unwrap()
    );
    assert!(
        tikv.take_warnings().is_empty(),
        "the dead division branch must not run"
    );
    assert!(
        tikv.tikv_expression_rows() > 0,
        "CASE now has lazy kernels and must execute in the engine"
    );
}

#[test]
fn tikv_expression_sql_error_keeps_mysql_identity() {
    let mut catalog = Catalog::default();
    run_create_table_on("CREATE TABLE overflowed (a BIGINT)", &mut catalog).unwrap();
    run_insert_on(
        "INSERT INTO overflowed VALUES (9223372036854775807)",
        &mut catalog,
        &StmtContext::for_query(),
    )
    .unwrap();
    let ctx = StmtContext::for_query().with_tikv_expression(true);
    let error = run_select_on("SELECT a+1 FROM overflowed", &catalog, &ctx).unwrap_err();
    assert!(
        matches!(
            &error,
            tidb_executor::DriverError::Exec(tidb_executor::executor::ExecError::Eval(
                tidb_expr::EvalError::ExternalEngine { code: 1690, .. }
            ))
        ),
        "overflow must come from TiKV, not native fallback: {error:?}"
    );
    let error = error.to_mysql_error();
    assert_eq!(error.code, 1690);
    assert_eq!(error.state, *b"22003");
    assert!(!error.message.is_empty());
}

#[test]
fn tikv_expression_settings_are_statement_local_and_clones_share_only_effects() {
    let mode = tidb_mysql::get_sql_mode("NO_UNSIGNED_SUBTRACTION,STRICT_TRANS_TABLES").unwrap();
    let ctx = StmtContext::for_query()
        .with_ddl_sql_mode(mode.0)
        .with_time_zone(SessionTimeZone::Fixed {
            name: "+05:30".to_owned(),
            offset_secs: 19_800,
        })
        .with_week_and_division_scale(0, 6)
        .with_tikv_expression(true);
    let settings = ctx.tikv_expression_context().unwrap();
    assert_eq!(settings.flags, 482);
    assert_eq!(settings.sql_mode, mode.0 as u64);
    assert_eq!(settings.time_zone_name, None);
    assert_eq!(settings.time_zone_offset, 19_800);
    assert_eq!(settings.div_precision_increment, 6);
    assert_eq!(settings.max_warning_count, u16::MAX as usize);
    let disabled = ctx.clone().with_tikv_expression(false);
    assert!(disabled.tikv_expression_context().is_none());
    assert!(ctx.tikv_expression_context().is_some());
    ctx.clone().record_tikv_expression_rows(9);
    assert_eq!(ctx.tikv_expression_rows(), 9);
    assert_eq!(disabled.tikv_expression_rows(), 9);
    assert_eq!(StmtContext::for_query().tikv_expression_rows(), 0);
    let write = StmtContext::for_dml(true, true, false).with_tikv_expression(true);
    assert_eq!(
        write.tikv_expression_context().unwrap().flags,
        write.push_down_flags()
    );
    assert_ne!(
        write.tikv_expression_context().unwrap().flags,
        settings.flags
    );
    let named = ctx.with_time_zone(SessionTimeZone::Named("America/New_York".parse().unwrap()));
    assert_eq!(
        named
            .tikv_expression_context()
            .unwrap()
            .time_zone_name
            .as_deref(),
        Some("America/New_York")
    );
}

#[test]
fn tikv_expression_concurrent_contexts_do_not_enable_each_other() {
    let workers = [false, true, false, true].map(|enabled| {
        std::thread::spawn(move || {
            let catalog = fixture();
            let ctx = StmtContext::for_query().with_tikv_expression(enabled);
            let rows = run_select_on("SELECT a+b FROM t ORDER BY a", &catalog, &ctx).unwrap();
            assert_eq!(rows.len(), 4);
            assert_eq!(ctx.tikv_expression_rows() > 0, enabled);
        })
    });
    for worker in workers {
        worker.join().unwrap();
    }
}

#[test]
fn tikv_expression_selection_preserves_order_nulls_and_duplicate_rows() {
    use std::sync::Arc;
    use tidb_ast::CiString;
    use tidb_chunk::chunk::Chunk;
    use tidb_datatype::{FieldType, FieldTypeCode};
    use tidb_expr::column::Column;
    use tidb_expr::constant::Constant;
    use tidb_expr::evaluator::{EvaluatorProgram, EvaluatorSuite};
    use tidb_expr::expression::{Expression, ScalarFunction};

    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<EvaluatorProgram>();
    assert_send_sync::<StmtContext>();
    let ty = FieldType::new(FieldTypeCode::LongLong);
    let mut column = Column::new(1, ty.clone());
    column.index = 0;
    let expression = Expression::ScalarFunction(ScalarFunction::new(
        CiString::new("plus"),
        ty.clone(),
        vec![
            Expression::Column(column),
            Expression::Constant(Constant::new(Datum::Int(1), ty.clone())),
        ],
    ));
    let program = Arc::new(EvaluatorProgram::new(vec![expression], false));
    let mut input = Chunk::new_with_capacity(std::slice::from_ref(&ty), 4);
    // Overflow in an unselected row must never be evaluated.
    input.append_int64(0, i64::MAX);
    input.append_int64(0, 1);
    input.append_null(0);
    input.append_int64(0, -2);
    input.set_sel(Some(vec![3, 1, 2, 1]));
    for enabled in [false, true] {
        let suite = EvaluatorSuite::from_program(Arc::clone(&program));
        let ctx = StmtContext::for_query().with_tikv_expression(enabled);
        let mut output = Chunk::new_with_capacity(std::slice::from_ref(&ty), 4);
        suite.run(&ctx, &mut input, &mut output).unwrap();
        let rows = (0..output.num_rows())
            .map(|row| output.get_row(row).get_datum(0, &ty))
            .collect::<Vec<_>>();
        assert_eq!(
            rows,
            vec![Datum::Int(-1), Datum::Int(2), Datum::Null, Datum::Int(2)]
        );
        assert_eq!(ctx.tikv_expression_rows(), if enabled { 4 } else { 0 });
        assert_eq!(input.sel(), Some([3, 1, 2, 1].as_slice()));
    }
}
