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

//! SQL-entrypoint coverage of the local engine adapter, not a package-port claim.
//! Each projection is tested independently: a supported neighboring expression
//! cannot hide a native fallback behind a nonzero statement-wide engine counter.

#![cfg(feature = "tikv-expr")]

use tidb_datatype::Datum;
use tidb_executor::{
    run_create_table_on, run_insert_on, run_select_meta_on, run_select_on, Catalog, StmtContext,
};
use tidb_expr::tikv::Backend;

fn table(schema: &str, insert: &str) -> Catalog {
    let mut catalog = Catalog::default();
    run_create_table_on(schema, &mut catalog).unwrap();
    run_insert_on(insert, &mut catalog, &StmtContext::for_query()).unwrap();
    catalog
}

/// Execute through parser, normal rewriting, planning and executor projection.
/// Fresh contexts isolate effects; cloned contexts would share the counters.
fn compare_sql(
    catalog: &Catalog,
    sql: &str,
    context: impl Fn() -> StmtContext,
    engine_expected: bool,
) {
    let native = context();
    let expected = run_select_meta_on(sql, catalog, &native)
        .unwrap_or_else(|error| panic!("native {sql}: {error:?}"));
    let warnings = native.take_warnings();
    assert_eq!(native.tikv_expression_rows(), 0, "native {sql}");
    for backend in [Backend::Copying, Backend::Borrowed] {
        let statement = context().with_tikv_expression_backend(backend);
        let actual = run_select_meta_on(sql, catalog, &statement)
            .unwrap_or_else(|error| panic!("{backend:?} {sql}: {error:?}"));
        assert_eq!(actual, expected, "{backend:?} metadata/values: {sql}");
        assert_eq!(
            statement.take_warnings(),
            warnings,
            "{backend:?} warnings: {sql}"
        );
        if engine_expected {
            assert!(
                statement.tikv_expression_rows() >= expected.1.len() as u64,
                "{backend:?} unexpectedly stayed native: {sql}"
            );
            // The removal gate: an engine-context projection that executed in
            // the engine must not also report a declined expression.
            assert_eq!(
                statement.tikv_not_admitted_fallbacks(),
                0,
                "{backend:?} admitted projection reported a declined expression: {sql}"
            );
            assert_eq!(
                statement.tikv_unrepresentable_input_fallbacks(),
                0,
                "{backend:?} admitted projection declined an input: {sql}"
            );
        } else {
            assert_eq!(
                statement.tikv_expression_rows(),
                0,
                "{backend:?} must stay native: {sql}"
            );
            assert_eq!(statement.tikv_borrowed_expression_rows(), 0, "{sql}");
            // Staying native is only allowed with a recorded reason, so a
            // silent fallback cannot pass as an intentional exclusion.
            assert!(
                statement.tikv_not_admitted_fallbacks()
                    + statement.tikv_unrepresentable_input_fallbacks()
                    > 0,
                "{backend:?} stayed native without recording a reason: {sql}"
            );
        }
        if backend == Backend::Copying {
            assert_eq!(statement.tikv_borrowed_expression_rows(), 0, "{sql}");
        }
        // Rich owned families can legitimately use copying when Borrowed was
        // requested. Do not misreport that fallback as a borrowed-kernel test.
    }
}

#[test]
fn tikv_expression_coverage_sql_decimal_unsigned_and_math() {
    let catalog = table(
        "CREATE TABLE numeric_values (id BIGINT, a BIGINT, b BIGINT, u BIGINT UNSIGNED, d DECIMAL(20,6), e DECIMAL(20,6), x DOUBLE)",
        "INSERT INTO numeric_values VALUES (1,7,2,18446744073709551615,1.234567,2.000001,4),(2,-4,3,9223372036854775808,-4.500000,0.250000,16),(3,0,-2,0,0.000000,-1.000000,0),(4,NULL,NULL,NULL,NULL,NULL,NULL)",
    );
    for expression in [
        "d+d",
        "d-e",
        "d*e",
        "ABS(d)",
        "ROUND(d,2)",
        "d<e",
        "d<=>e",
        "ABS(u)",
        "u+0",
        "u&255",
        "u=u",
        "CAST(u AS DECIMAL(20,0))",
        "SQRT(x)",
        "POW(x,2)",
        "ABS(x)",
        "SIGN(x)",
        "FLOOR(x)",
        "ROUND(x,0)",
        "a DIV b",
        "a MOD b",
        "a^b",
        "a<<1",
    ] {
        compare_sql(
            &catalog,
            &format!("SELECT {expression} FROM numeric_values ORDER BY id"),
            StmtContext::for_query,
            true,
        );
    }
}

#[test]
fn tikv_expression_coverage_sql_string_bytes_unicode_and_collation() {
    let catalog = table(
        "CREATE TABLE string_values (id BIGINT, s VARCHAR(32), other VARCHAR(32)) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin",
        "INSERT INTO string_values VALUES (1,'Abé','Abé'),(2,' xY ','xy'),(3,'',''),(4,NULL,NULL)",
    );
    for expression in [
        "LOWER(s)",
        "UPPER(s)",
        "REVERSE(s)",
        "CHAR_LENGTH(s)",
        "LENGTH(s)",
        "BIT_LENGTH(s)",
        "SUBSTRING(s,2,2)",
        "LEFT(s,2)",
        "RIGHT(s,2)",
        "LTRIM(s)",
        "RTRIM(s)",
        "HEX(s)",
        "s=other",
        "s<=>other",
        "INSTR(s,'b')",
    ] {
        compare_sql(
            &catalog,
            &format!("SELECT {expression} FROM string_values ORDER BY id"),
            StmtContext::for_query,
            true,
        );
    }
}

#[test]
fn tikv_expression_coverage_sql_temporal_columns_and_fsp() {
    let catalog = table(
        "CREATE TABLE temporal_values (id BIGINT, d DATE, dt DATETIME(6), tm TIME(6))",
        "INSERT INTO temporal_values VALUES (1,'2024-02-29','2024-02-29 23:59:58.123456','12:34:56.123456'),(2,'2016-01-01','2016-01-01 01:02:03.000004','-01:02:03.000004'),(3,NULL,NULL,NULL)",
    );
    for expression in [
        "YEAR(dt)",
        "MONTH(dt)",
        "DAYOFMONTH(dt)",
        "DAYOFWEEK(d)",
        "DAYOFYEAR(d)",
        "QUARTER(dt)",
        "DATE_FORMAT(dt,'%Y-%m-%d %H:%i:%s.%f')",
        "DATE(dt)",
        "LAST_DAY(d)",
        "DATEDIFF(dt,d)",
        "HOUR(tm)",
        "MINUTE(tm)",
        "SECOND(tm)",
        "MICROSECOND(tm)",
        "TIME_TO_SEC(tm)",
        "DATE_ADD(dt, INTERVAL 1 DAY)",
    ] {
        compare_sql(
            &catalog,
            &format!("SELECT {expression} FROM temporal_values ORDER BY id"),
            StmtContext::for_query,
            true,
        );
    }
}

#[test]
fn tikv_expression_coverage_sql_json_columns_keep_binary_value_kinds() {
    let catalog = table(
        "CREATE TABLE json_values (id BIGINT, j JSON)",
        r#"INSERT INTO json_values VALUES (1,'{"a":[1,true,null],"b":"text"}'),(2,'[1,2,3]'),(3,'null'),(4,NULL)"#,
    );
    let (columns, rows) = run_select_meta_on(
        "SELECT j FROM json_values ORDER BY id",
        &catalog,
        &StmtContext::for_query(),
    )
    .unwrap();
    // Real DDL uses MySQL's full JSON width, unlike synthetic expression
    // fixtures whose FieldType::new(Json) leaves flen unspecified (-1).
    // Local TiPB lowering must handle this metadata without changing the
    // source schema or quietly keeping every JSON column expression native.
    assert_eq!(columns[0].1.code(), tidb_datatype::FieldTypeCode::Json);
    assert_eq!(columns[0].1.flen(), i64::from(u32::MAX));
    assert!(rows[..3]
        .iter()
        .all(|row| matches!(&row[0], Datum::Json(_))));
    assert_eq!(rows[3][0], Datum::Null);
    for expression in [
        "JSON_TYPE(j)",
        "JSON_DEPTH(j)",
        "JSON_VALID(j)",
        "JSON_LENGTH(j)",
        "JSON_KEYS(j)",
        "JSON_EXTRACT(j,'$.a')",
        "JSON_REMOVE(j,'$.b')",
        "JSON_ARRAY(j)",
    ] {
        compare_sql(
            &catalog,
            &format!("SELECT {expression} FROM json_values ORDER BY id"),
            StmtContext::for_query,
            true,
        );
    }
    // Document parsing and value quoting are different SQL operations. A plain
    // string argument is deliberately not coerced to a JSON document here.
    compare_sql(
        &catalog,
        "SELECT JSON_ARRAY(CAST(id AS CHAR)) FROM json_values ORDER BY id",
        StmtContext::for_query,
        false,
    );
}

#[test]
#[cfg(target_endian = "little")]
fn tikv_expression_coverage_sql_vector_columns_reuse_owned_carriers() {
    let catalog = table(
        "CREATE TABLE vector_values (id BIGINT, v VECTOR(3), w VECTOR(3))",
        "INSERT INTO vector_values VALUES (1,'[3,4,0]','[0,0,5]'),(2,'[1,2,2]','[1,2,2]'),(3,NULL,NULL)",
    );
    for expression in [
        "VEC_DIMS(v)",
        "VEC_AS_TEXT(v)",
        "VEC_L2_NORM(v)",
        "VEC_L1_DISTANCE(v,w)",
        "VEC_L2_DISTANCE(v,w)",
        "VEC_NEGATIVE_INNER_PRODUCT(v,w)",
    ] {
        compare_sql(
            &catalog,
            &format!("SELECT {expression} FROM vector_values ORDER BY id"),
            StmtContext::for_query,
            true,
        );
    }
}

#[test]
fn tikv_expression_coverage_sql_lazy_control_executes_without_the_dead_branch() {
    let catalog = table(
        "CREATE TABLE control_values (id BIGINT, flag BIGINT, a BIGINT, b BIGINT)",
        "INSERT INTO control_values VALUES (1,0,9223372036854775807,7),(2,1,9223372036854775807,8),(3,NULL,9223372036854775807,9)",
    );
    compare_sql(
        &catalog,
        "SELECT IF(flag,a,b) FROM control_values ORDER BY id",
        StmtContext::for_query,
        true,
    );
    // The overflowing child contains a column, so it cannot disappear through
    // constant folding. Every control signature has a lazy kernel, so these
    // now execute in the engine and the dead branch is never entered; an eager
    // regression would raise MySQL 1690 instead of returning the branch.
    for sql in [
        "SELECT IF(FALSE,a+1,b) FROM control_values ORDER BY id",
        "SELECT IF(flag=2,a+1,b) FROM control_values ORDER BY id",
        "SELECT CASE WHEN flag=2 THEN a+1 ELSE b END FROM control_values ORDER BY id",
    ] {
        compare_sql(&catalog, sql, StmtContext::for_query, true);
        for backend in [Backend::Copying, Backend::Borrowed] {
            let context = StmtContext::for_query().with_tikv_expression_backend(backend);
            assert_eq!(
                run_select_on(sql, &catalog, &context).unwrap(),
                vec![
                    vec![Datum::Int(7)],
                    vec![Datum::Int(8)],
                    vec![Datum::Int(9)]
                ],
                "{backend:?}: {sql}"
            );
            assert!(
                context.take_warnings().is_empty(),
                "dead branch emitted diagnostics: {sql}"
            );
            assert!(
                context.tikv_expression_rows() > 0,
                "lazy control must execute in the engine: {sql}"
            );
        }
    }
}

#[test]
fn tikv_expression_coverage_sql_packet_contractions_and_retained_concat_are_explicit() {
    let catalog = table(
        "CREATE TABLE packet_values (id BIGINT, n BIGINT, s VARCHAR(32))",
        "INSERT INTO packet_values VALUES (1,4,'a'),(2,16,'abcdefgh'),(3,NULL,NULL)",
    );
    for expression in ["SPACE(n)", "REPEAT(s,n)"] {
        let sql = format!("SELECT {expression} FROM packet_values ORDER BY id");
        let context = StmtContext::for_query().with_max_allowed_packet(8);
        let error = run_select_on(&sql, &catalog, &context)
            .expect_err("native packet-limited string kernel is deleted")
            .to_string();
        assert!(
            error.contains(
                "native packet-limited string evaluation was removed; function unsupported"
            ),
            "{sql}: {error}"
        );
        assert!(
            context.take_warnings().is_empty(),
            "deleted kernel warned: {sql}"
        );
    }

    let sql = "SELECT CONCAT(s,s) FROM packet_values ORDER BY id";
    compare_sql(
        &catalog,
        sql,
        || StmtContext::for_query().with_max_allowed_packet(8),
        false,
    );
    let native = StmtContext::for_query().with_max_allowed_packet(8);
    let rows = run_select_on(sql, &catalog, &native).unwrap();
    assert_eq!(rows[1][0], Datum::Null, "retained CONCAT overflow");
    assert!(native
        .take_warnings()
        .iter()
        .any(|warning| warning.1 == 1301));
}

#[test]
fn tikv_expression_coverage_sql_default_week_mode_stays_native_explicit_mode_reuses_engine() {
    let catalog = table(
        "CREATE TABLE week_values (id BIGINT, d DATE)",
        "INSERT INTO week_values VALUES (1,'2016-01-01'),(2,'2017-01-01'),(3,NULL)",
    );
    for mode in [0, 3] {
        compare_sql(
            &catalog,
            "SELECT WEEK(d) FROM week_values ORDER BY id",
            || StmtContext::for_query().with_week_and_division_scale(mode, 4),
            false,
        );
        compare_sql(
            &catalog,
            "SELECT WEEK(d,3) FROM week_values ORDER BY id",
            || StmtContext::for_query().with_week_and_division_scale(mode, 4),
            true,
        );
    }
    let rows = |mode| {
        run_select_on(
            "SELECT WEEK(d) FROM week_values ORDER BY id",
            &catalog,
            &StmtContext::for_query().with_week_and_division_scale(mode, 4),
        )
        .unwrap()
    };
    assert_ne!(
        rows(0),
        rows(3),
        "fixture must exercise the session setting"
    );
}
