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

//! The engine-only corpus ratchet.
//!
//! `TIKV_EXPR_ENGINE_ONLY=1` measures how much of the corpus the engine
//! answers, but it is an environment-gated run: nothing in a normal `cargo
//! test` stops a change from silently adding a native fallback. This test
//! compiles one pinned expression at a time and fails when an expression
//! changes side, in either direction.
//!
//! * An expression pinned as covered that starts declining is a regression.
//! * An expression pinned as declined that starts compiling is progress, and
//!   the failure is the reminder to move it to the covered list in the same
//!   change -- which keeps the count in `tikv-expression-corpus-plan.md`
//!   honest.
//!
//! The lists are the engine-only measurement, not a curated subset: update
//! them with `TIKV_EXPR_ENGINE_ONLY=1 cargo test -p tidb-expr --features
//! tikv-expr` and the diff will show exactly what moved.

use tidb_ast::{QueryStmt, SelectField, Stmt};
use tidb_expr::rewriter::rewrite_expr;
use tidb_expr::tikv::{Context, TikvExpression};

/// Whether the adapter hands this expression to the engine.
///
/// A rewrite or compile *error* counts as "not owned", which is what the
/// corpus harness does: an expression the rewriter cannot build without a
/// column resolver, or one whose wire program the engine refuses outright, is
/// answered natively.
fn engine_owns(expression: &str) -> bool {
    let stmt = tidb_parser::parse(&format!("select {expression}")).expect("parse");
    let Stmt::Query(query) = stmt else {
        panic!("not a query")
    };
    let QueryStmt::Select(select) = query.into_inner() else {
        panic!("not a select")
    };
    let SelectField::Expr { expr, .. } = &select.fields[0] else {
        panic!("no expression")
    };
    let Ok(rewritten) = rewrite_expr(expr) else {
        return false;
    };
    matches!(
        TikvExpression::compile(
            &rewritten,
            Context {
                flags: 482,
                ..Context::default()
            },
        ),
        Ok(Some(_))
    )
}

/// Expressions the engine runs today. Losing one of these is a regression.
const COVERED: &[&str] = &[
    "date('20111213')",
    "month(20240315123045)",
    "last_day(20240315123045)",
    "time('10:10:10.123456')",
    "time('2003-12-31 01:02:03')",
    "timestamp('2020-01-01')",
    "extract(year from 20240315)",
    "NULLIF(1, 1.0)",
    "elt(0, 2, 3, 11, 1)",
    "elt(1, 65)",
    "cast(0e0 as datetime)",
    "cast(1 as signed) < cast(1 as signed)",
    "cast('123' as char) < cast('123' as char)",
    "cast('12:59:59' as time) < cast('12:59:59' as time)",
    "coalesce(cast('12:59:59' as time), cast('12:59:59.555' as time(3)))"
];

/// Expressions that still fall back to native. Gaining one is progress; the
/// test fails so the list and the documented count move together.
const DECLINED: &[&str] = &[
    "0xff like 0xff",
    "(1, 2) = (1, 2, 3)",
    "1.50 or 0e0",
    "7 in (7, -9, 9)",
    "addtime('01:00:00.999999','02:00:00.999998')",
    "addtime('2020-01-01 10:00:00','01:00:00')",
    "b'1111111111111111111111111111111111111111111111111111111111111111' + 0",
    "benchmark(-3, 1)",
    "case when cast('0' as json) then 1 end",
    "case when false then 1.5 else 0 end",
    "cast('\"123\"' as json) < cast('\"123\"' as json)",
    "cast('1' as json)",
    "cast('2019-11-02 22:00:05' as datetime) in (cast('2019-11-02 22:00:04' as datetime), cast('2019-11-02 22:00:05' as datetime))",
    "char(65, 16740, 67.5 using utf8)",
    "coalesce(1, 1.1e0)",
    "coalesce(1, 'x' regexp '[')",
    "coalesce(cast(1 as json), cast(2 as json))",
    "convert(0x1e240 using utf8)",
    "convert('haha' using cp866)",
    "convert_tz(20240315123045,'+00:00','+08:00')",
    "cot(1)",
    "elt(1.1, '2.1', '3.1', '11.1', '1.1')",
    "elt('2abc','x','y','z')",
    "extract(hour from '-25:03:04')",
    "field(1.10, 0, 11e-1)",
    "field(NULL, 2, 3, 11, 1)",
    "find_in_set('a', 'b,a,c,a')",
    "find_in_set(' ', '  , , ,') collate utf8mb4_general_ci",
    "find_in_set(' ' collate utf8mb4_general_ci, '  , , ,' collate utf8mb4_general_ci)",
    "format(12345.67, 2, 'en_us')",
    "format(1234567.89, 2, 'en_US')",
    "greatest('2020-01-01','99-1-1')",
    "greatest(-9223372036854775808, cast('9223372036854775809' as unsigned))",
    "greatest(\"a\", \"b\", \"c\")",
    "greatest('a' collate utf8mb4_general_ci, 'B')",
    "hex(weight_string('a'))",
    "hex(weight_string('aAÁàãăâ' collate utf8mb4_general_ci))",
    "if(cast('2020-10-10 12:59:59' as datetime), 1, 2)",
    "ifnull(1, 'x' regexp '[')",
    "ifnull(null, cast('[1]' as json))",
    "interval(\"9007199254740991\", \"9007199254740992\")",
    "interval(null, 1, 2)",
    "json_schema_valid('{\"required\":[\"a\"]}', '{\"a\":1}')",
    "load_file('')",
    "make_set(1, 'a', 'b', 'c')",
    "NULLIF(1, \"1.0\")",
    "oct(1.0)",
    "regexp_like('abc', 'abc', 'p')",
    "round(1.2345,'2')",
    "round(3.14,'abc')",
    "round(5, -100)",
    "subtime('01:00:00.999999','02:00:00.999998')",
    "timestamp('2020-01-01','01:00:00')",
    "to_base64('')",
    "translate('ABC', 'A', 'B')",
    "translate('abcabc', 'ab', 'xy')",
    "truncate(1234.5678,'-2')",
    "upper(elt(1,'a',x'61'))",
    "weight_string(NULL)"
];

#[test]
fn covered_expressions_stay_in_the_engine() {
    for expression in COVERED {
        assert!(
            engine_owns(expression),
            "the engine stopped running {expression}; either fix the regression or move it to \
             DECLINED with the reason"
        );
    }
}

#[test]
fn declined_expressions_stay_declined() {
    for expression in DECLINED {
        assert!(
            !engine_owns(expression),
            "{expression} now runs in the engine; move it to COVERED and update the count in \
             tikv-expression-corpus-plan.md"
        );
    }
}

#[test]
fn the_gap_count_is_pinned() {
    assert_eq!(COVERED.len(), 15, "the covered list changed size");
    assert_eq!(DECLINED.len(), 59, "the declined list changed size");
}
