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

#![cfg(feature = "tikv-expr")]

use tidb_executor::{
    run_create_table_on, run_insert_on, run_select_meta_on, run_select_on, Catalog, StmtContext,
};
use tidb_expr::tikv::Backend;
use tidb_expr::Columns;

fn fixture() -> Catalog {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "CREATE TABLE t (id BIGINT, a BIGINT, b BIGINT, x DOUBLE, y DOUBLE, s VARCHAR(32), d DECIMAL(20,6)) CHARACTER SET utf8mb4",
        &mut catalog,
    ).unwrap();
    run_insert_on(
        "INSERT INTO t VALUES (1,1,2,1.5,2.25,'Aé',1.234567),(2,-4,0,-4,0.5,'a\\0b',2.000001),(3,3,NULL,3,NULL,NULL,NULL),(4,7,5,1.25,2,'',-4.500000)",
        &mut catalog,
        &StmtContext::for_query(),
    ).unwrap();
    catalog
}

#[test]
fn tikv_borrowed_expression_sql_three_modes_preserve_rows_and_inferred_types() {
    let catalog = fixture();
    // Check expressions separately: a supported neighbor cannot hide fallback.
    for expression in [
        "a+b",
        "a-b",
        "a*b",
        "ABS(a)",
        "(a+b)*b",
        "a<b",
        "a=b",
        "a<=>b",
        "x+y",
        "x-y",
        "x*y",
        "ABS(x)",
        "(x+y)*y",
        "LENGTH(s)",
    ] {
        let sql = format!("SELECT {expression} FROM t ORDER BY id");
        let native = StmtContext::for_query();
        let expected = run_select_meta_on(&sql, &catalog, &native).unwrap();
        assert_eq!(expected.1.len(), 4);
        assert_eq!(native.tikv_expression_rows(), 0);
        assert_eq!(native.tikv_borrowed_expression_rows(), 0);
        for backend in [Backend::Copying, Backend::Borrowed] {
            let context = StmtContext::for_query().with_tikv_expression_backend(backend);
            assert_eq!(
                run_select_meta_on(&sql, &catalog, &context).unwrap(),
                expected,
                "{backend:?}: {sql}"
            );
            assert!(
                context.tikv_expression_rows() >= 4,
                "must execute TiKV: {sql}"
            );
            if backend == Backend::Borrowed {
                assert!(
                    context.tikv_borrowed_expression_rows() >= 4,
                    "must borrow instead of copy: {sql}"
                );
            } else {
                assert_eq!(context.tikv_borrowed_expression_rows(), 0);
            }
            assert!(context.take_warnings().is_empty());
        }
    }
}

#[test]
fn tikv_borrowed_expression_sql_crosses_executor_and_engine_batch_boundaries() {
    let mut catalog = Catalog::default();
    run_create_table_on("CREATE TABLE batch (a BIGINT, b BIGINT)", &mut catalog).unwrap();
    let values = (0..2053)
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
    let sql = "SELECT (a+b)+1, a<b FROM batch WHERE a>=17 ORDER BY a";
    let native = StmtContext::for_query().with_executor_chunk_sizes(32, 128);
    let expected = run_select_meta_on(sql, &catalog, &native).unwrap();
    assert_eq!(expected.1.len(), 2053 - 17);
    for maximum in [128, 4096] {
        for backend in [Backend::Copying, Backend::Borrowed] {
            let context = StmtContext::for_query()
                .with_executor_chunk_sizes(32, maximum)
                .with_tikv_expression_backend(backend);
            assert_eq!(
                run_select_meta_on(sql, &catalog, &context).unwrap(),
                expected
            );
            assert!(context.tikv_expression_rows() >= expected.1.len() as u64);
            if backend == Backend::Borrowed {
                assert!(context.tikv_borrowed_expression_rows() >= expected.1.len() as u64);
            } else {
                assert_eq!(context.tikv_borrowed_expression_rows(), 0);
            }
        }
    }
}

#[test]
fn tikv_borrowed_expression_decimal_reuses_engine_and_scalar_case_stays_lazy() {
    let catalog = fixture();
    // Exact DECIMAL arithmetic is part of the broadened surface, but the
    // borrowed loaders still cover only Int/Real/Bytes: the requested Borrowed
    // backend falls back to the copying adapter before any kernel runs, and
    // the counters must report that honestly.
    let sql = "SELECT d+d, d*d FROM t ORDER BY id";
    let native = StmtContext::for_query();
    let expected = run_select_meta_on(sql, &catalog, &native).unwrap();
    for backend in [Backend::Copying, Backend::Borrowed] {
        let context = StmtContext::for_query().with_tikv_expression_backend(backend);
        assert_eq!(
            run_select_meta_on(sql, &catalog, &context).unwrap(),
            expected
        );
        assert!(
            context.tikv_expression_rows() > 0,
            "supported SQL must execute in TiKV: {sql}"
        );
        assert_eq!(context.tikv_borrowed_expression_rows(), 0);
    }
    // A CASE whose branches are not leaves stays native: TiKV RPN evaluates
    // every child eagerly, so the dead DIV must not execute or warn.
    let sql = "SELECT CASE WHEN b=0 THEN 7 ELSE a DIV b END FROM t ORDER BY id";
    let expected = run_select_meta_on(sql, &catalog, &native).unwrap();
    for backend in [Backend::Copying, Backend::Borrowed] {
        let context = StmtContext::for_query().with_tikv_expression_backend(backend);
        assert_eq!(
            run_select_meta_on(sql, &catalog, &context).unwrap(),
            expected
        );
        assert_eq!(
            context.tikv_expression_rows(),
            0,
            "unsupported SQL must remain native: {sql}"
        );
        assert_eq!(context.tikv_borrowed_expression_rows(), 0);
        assert!(
            context.take_warnings().is_empty(),
            "dead division branch must not warn"
        );
    }
}

#[test]
fn tikv_borrowed_expression_backend_configuration_is_local_while_clone_effects_are_shared() {
    let catalog = fixture();
    let base = StmtContext::for_query();
    let copying = base.clone().with_tikv_expression_backend(Backend::Copying);
    let borrowed = base.clone().with_tikv_expression_backend(Backend::Borrowed);
    assert!(base.tikv_expression_context().is_none());
    assert_eq!(copying.tikv_expression_backend(), Backend::Copying);
    assert_eq!(borrowed.tikv_expression_backend(), Backend::Borrowed);
    assert_eq!(
        copying.tikv_expression_context(),
        borrowed.tikv_expression_context(),
        "backend choice must not alter SQL settings"
    );
    let sql = "SELECT a+b FROM t ORDER BY id";
    let expected = run_select_on(sql, &catalog, &base).unwrap();
    assert_eq!(base.tikv_expression_rows(), 0);
    assert_eq!(run_select_on(sql, &catalog, &borrowed).unwrap(), expected);
    let borrowed_count = borrowed.tikv_borrowed_expression_rows();
    let total_count = borrowed.tikv_expression_rows();
    assert!(borrowed_count >= 4);
    assert_eq!(base.tikv_borrowed_expression_rows(), borrowed_count);
    assert_eq!(copying.tikv_borrowed_expression_rows(), borrowed_count);
    assert_eq!(run_select_on(sql, &catalog, &copying).unwrap(), expected);
    assert!(copying.tikv_expression_rows() > total_count);
    assert_eq!(copying.tikv_borrowed_expression_rows(), borrowed_count);
    // A separate statement starts with fresh effects and remains disabled even
    // though another live statement uses the borrowed engine.
    let independent = StmtContext::for_query();
    assert!(independent.tikv_expression_context().is_none());
    assert_eq!(
        run_select_on(sql, &catalog, &independent).unwrap(),
        expected
    );
    assert_eq!(independent.tikv_expression_rows(), 0);
    assert_eq!(independent.tikv_borrowed_expression_rows(), 0);
    let disabled = StmtContext::for_query()
        .with_tikv_expression_backend(Backend::Borrowed)
        .with_tikv_expression(false);
    assert!(disabled.tikv_expression_context().is_none());
    assert_eq!(run_select_on(sql, &catalog, &disabled).unwrap(), expected);
    assert_eq!(disabled.tikv_borrowed_expression_rows(), 0);
}

#[test]
fn tikv_borrowed_expression_sql_error_is_external_and_never_retried_natively() {
    let mut catalog = Catalog::default();
    run_create_table_on("CREATE TABLE overflowed (a BIGINT)", &mut catalog).unwrap();
    run_insert_on(
        "INSERT INTO overflowed VALUES (9223372036854775807)",
        &mut catalog,
        &StmtContext::for_query(),
    )
    .unwrap();
    for backend in [Backend::Copying, Backend::Borrowed] {
        let context = StmtContext::for_query().with_tikv_expression_backend(backend);
        let error = run_select_on("SELECT a+1 FROM overflowed", &catalog, &context).unwrap_err();
        assert!(
            matches!(
                &error,
                tidb_executor::DriverError::Exec(tidb_executor::executor::ExecError::Eval(
                    tidb_expr::EvalError::ExternalEngine { code: 1690, .. }
                ))
            ),
            "must preserve actual engine error, not replay native: {error:?}"
        );
        let mysql = error.to_mysql_error();
        assert_eq!(mysql.code, 1690);
        assert_eq!(mysql.state, *b"22003");
        assert!(!mysql.message.is_empty());
        assert_eq!(context.tikv_expression_rows(), 0);
        assert_eq!(context.tikv_borrowed_expression_rows(), 0);
    }
}
