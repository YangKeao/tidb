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

//! Focused tests for translated `pkg/expression/builtin_compare.go` behavior.

use super::{chunk_e, e};
use crate::{apply_binary, Datum, Decimal};
use tidb_ast::BinaryOp;

fn assert_removed_compare(actual: String, former_expected: &str) {
    assert_eq!(
        actual,
        "Unsupported(\"native LEAST/GREATEST/INTERVAL evaluation was removed; TiKV engine required\")",
        "former expected {former_expected}"
    );
}

#[test]
fn compare_source_vector_promotes_real_and_decimal() {
    // pkg/expression/builtin_compare_test.go:80 TestCompare
    // `realVal` is a Go float64 while `decimalVal` is a MyDecimal.  The
    // compare function therefore selects the ETReal signature and returns a
    // boolean datum, rather than exposing either operand's numeric value.
    let decimal = Datum::new_decimal(Decimal::from_literal("123.123"));
    assert_eq!(
        apply_binary(BinaryOp::Lt, Datum::Real(1.1), decimal),
        Ok(Datum::Int(1))
    );
}

#[test]
fn json_comparison_treats_an_explicit_cast_string_as_a_json_value() {
    // pkg/expression/builtin_compare.go::generateCmpSigs clears
    // ParseToJSONFlag on non-column JSON operands. Therefore the explicit
    // cast contributes the JSON string "1", not the JSON number 1.
    use crate::context::NoColumns;
    use crate::rewriter::{rewrite_expr_resolved, ColumnResolver};
    use tidb_ast::{QueryStmt, SelectField, Stmt};
    use tidb_datatype::{BinaryJSON, FieldType, FieldTypeCode};

    assert_eq!(chunk_e("cast('1' as json)"), "JSON:1");

    struct JsonColumn;
    impl ColumnResolver for JsonColumn {
        fn resolve(&self, path: &[String]) -> Option<(usize, FieldType, i64)> {
            (path.last()? == "j").then(|| (0, FieldType::new(FieldTypeCode::Json), 1))
        }

        fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
            tidb_datatype::SessionTimeZone::utc()
        }
    }

    let stmt = tidb_parser::parse("select j = cast('1' as json)").expect("parse");
    let Stmt::Query(query) = stmt else {
        panic!("not query")
    };
    let QueryStmt::Select(select) = query.into_inner() else {
        panic!("not select")
    };
    let SelectField::Expr { expr, .. } = &select.fields[0] else {
        panic!("not expression")
    };
    let rewritten = rewrite_expr_resolved(expr, &JsonColumn).expect("rewrite");
    let field_type = FieldType::new(FieldTypeCode::Json);
    let mut chunk = tidb_chunk::chunk::Chunk::new_with_capacity(&[field_type], 1);
    chunk.append_json(0, &BinaryJSON::parse(r#""1""#).expect("JSON string"));

    assert_eq!(
        rewritten.eval(&NoColumns, chunk.get_row(0)),
        Ok(Datum::Int(1))
    );
}

#[test]
fn greatest_least_mixed_integer_oracles_now_contract() {
    // pkg/expression/builtin_compare_test.go:286 TestGreatestLeastFunc
    for (sql, former_expected) in [
        (
            "greatest(-9223372036854775808, 9223372036854775809)",
            "DEC:9223372036854775809",
        ),
        (
            "least(-9223372036854775808, 9223372036854775809)",
            "DEC:-9223372036854775808",
        ),
        (
            "greatest(cast(9223372036854775808 as unsigned), cast(9223372036854775809 as unsigned))",
            "UINT:9223372036854775809",
        ),
        (
            "least(cast(9223372036854775808 as unsigned), cast(9223372036854775809 as unsigned))",
            "UINT:9223372036854775808",
        ),
    ] {
        assert_removed_compare(e(sql), former_expected);
    }
}

#[test]
fn greatest_least_mixed_string_oracles_now_contract() {
    // pkg/expression/builtin_compare_test.go:286 TestGreatestLeastFunc
    // Go's aggregateType selects the string signature when any argument is a
    // string, so the numeric 12 is compared as the text "12".
    assert_removed_compare(e("greatest('123a', 'b', 'c', 12)"), "STR:c");
    assert_removed_compare(e("least('123a', 'b', 'c', 12)"), "STR:12");
}

/// A one-row chunk holding a single `DATE`/`DATETIME` column named `d`, and
/// the expression `sql` evaluated over it through the CHUNK tier. This is the
/// direct column path used to exercise the same builtin with stored temporal
/// data rather than a scalar cast expression.
fn eval_over_date_column(sql: &str, code: tidb_datatype::FieldTypeCode, literal: &str) -> String {
    use tidb_ast::{QueryStmt, SelectField, Stmt};
    use tidb_datatype::{FieldType, TimeType};

    struct Resolver(FieldType);
    impl crate::rewriter::ColumnResolver for Resolver {
        fn resolve(&self, path: &[String]) -> Option<(usize, FieldType, i64)> {
            (path.last()? == "d").then(|| (0, self.0.clone(), 1))
        }
        fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
            tidb_datatype::SessionTimeZone::utc()
        }
    }

    let field_type = FieldType::new(code);
    let kind = if code == tidb_datatype::FieldTypeCode::Date {
        TimeType::Date
    } else {
        TimeType::DateTime
    };
    let parsed = tidb_datatype::parse_time(
        literal,
        kind,
        0,
        false,
        true,
        false,
        &tidb_datatype::SessionTimeZone::utc(),
    )
    .expect("date literal");

    let stmt = tidb_parser::parse(&format!("select {sql}")).expect("parse");
    let Stmt::Query(query) = stmt else {
        panic!("not query")
    };
    let QueryStmt::Select(select) = query.into_inner() else {
        panic!("not select")
    };
    let SelectField::Expr { expr, .. } = &select.fields[0] else {
        panic!("no expr")
    };
    let rewritten =
        match crate::rewriter::rewrite_expr_resolved(expr, &Resolver(field_type.clone())) {
            Ok(rewritten) => rewritten,
            Err(err) => return format!("{err:?}"),
        };
    let mut chunk = tidb_chunk::chunk::Chunk::new_with_capacity(&[field_type], 1);
    chunk.append_time(0, parsed.time);
    match rewritten.eval(&crate::context::NoColumns, chunk.get_row(0)) {
        Ok(value) => value.label(),
        Err(err) => format!("{err:?}"),
    }
}

/// Go `resolveType4Extremum` + `builtinGreatestCmpStringAsTimeSig`: once ANY
/// argument carries a date/datetime FieldType and the aggregate is a string
/// kind, EVERY argument is parsed as a time and re-emitted canonically before
/// the comparison — so a byte comparison picks the wrong argument outright.
///
/// CAPTURED from TiDB over `create table tgl(d date, dt datetime)` holding
/// `2020-01-01` / `2020-01-01 10:00:00`:
///
/// ```text
/// select greatest(d, '99-1-1')   -> 2020-01-01
/// select least(d, '99-1-1')      -> 1999-01-01
/// select least(d, '2019-5-5')    -> 2019-05-05
/// select greatest(d, 'zzz')      -> zzz
/// select greatest(dt, '99-1-1')  -> 2020-01-01 10:00:00
/// select least(dt, '99-1-1')     -> 1999-01-01 00:00:00
/// ```
///
/// (the fixture below names its one column `d` whichever type it carries, so
/// the `dt` rows are spelled `d` there.)
///
/// Note `least(d, '99-1-1')` and `least(dt, '99-1-1')`: the SAME text answers
/// `1999-01-01` under the date signature and `1999-01-01 00:00:00` under the
/// datetime one, which is what makes the mode a real distinction and not a
/// formatting detail. `greatest(d, 'zzz')` pins the failed-parse rule --
/// Go keeps the argument's original text rather than dropping it.
#[test]
fn greatest_least_temporal_oracles_now_contract() {
    use tidb_datatype::FieldTypeCode;
    let date = |sql: &str| eval_over_date_column(sql, FieldTypeCode::Date, "2020-01-01");
    let datetime =
        |sql: &str| eval_over_date_column(sql, FieldTypeCode::Datetime, "2020-01-01 10:00:00");

    for (actual, former_expected) in [
        (date("greatest(d, '99-1-1')"), "STR:2020-01-01"),
        (date("least(d, '99-1-1')"), "STR:1999-01-01"),
        (date("least(d, '2019-5-5')"), "STR:2019-05-05"),
        (date("greatest(d, 'zzz')"), "STR:zzz"),
        (datetime("greatest(d, '99-1-1')"), "STR:2020-01-01 10:00:00"),
        (datetime("least(d, '99-1-1')"), "STR:1999-01-01 00:00:00"),
        (chunk_e("greatest('2020-01-01','99-1-1')"), "STR:99-1-1"),
    ] {
        assert_removed_compare(actual, former_expected);
    }
}

/// Go `builtinGreatestStringSig.evalString` compares with
/// `types.CompareString(v, maxv, b.collation)` — the collation
/// `deriveCollation` derived for the FUNCTION, which only the chunk tier
/// knows. CAPTURED from TiDB:
///
/// ```text
/// select greatest('a' collate utf8mb4_general_ci, 'B');  -> B
/// select least('a' collate utf8mb4_general_ci, 'B');     -> a
/// select greatest('a', 'a ');                            -> a
/// ```
///
/// The first two are the exact SWAP of the byte answer (`a` and `B`), so the
/// bug returned the other argument rather than a differently-formatted one.
/// The third is PAD SPACE: the two compare equal, and Go keeps the earlier
/// argument, where a byte comparison prefers the padded one.
#[test]
fn greatest_least_collation_oracles_now_contract() {
    for (sql, former_expected) in [
        ("greatest('a' collate utf8mb4_general_ci, 'B')", "STR:B"),
        ("least('a' collate utf8mb4_general_ci, 'B')", "STR:a"),
        ("greatest('a', 'a ')", "STR:a"),
        ("greatest('123a', 'b', 'c', 12)", "STR:c"),
        ("least('123a', 'b', 'c', 12)", "STR:12"),
    ] {
        assert_removed_compare(chunk_e(sql), former_expected);
    }
}
