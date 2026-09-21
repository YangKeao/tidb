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
use tidb_chunk::chunk::Chunk;
use tidb_datatype::Datum;
use tidb_expr::evaluator::{EvaluatorError, EvaluatorSuite};
use tidb_expr::rewriter::rewrite_expr;
use tidb_expr::tikv::{Context, TikvExpression};
use tidb_expr::{Columns, EvalError};

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
    "coalesce(cast('12:59:59' as time), cast('12:59:59.555' as time(3)))",
    "oct(1.0)",
    "quote('safe text')",
    "substring_index('a.b.c', '.', -2)",
    "if(cast('2020-10-10 12:59:59' as datetime), 1, 2)",
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
    "format_bytes(2048)",
    "format_nano_time(2000)",
    "tidb_decode_binary_plan('malformed')",
    "tidb_decode_plan('malformed')",
    "tidb_encode_sql_digest('select 1')",
    "greatest('2020-01-01','99-1-1')",
    "greatest(-9223372036854775808, cast('9223372036854775809' as unsigned))",
    "greatest(\"a\", \"b\", \"c\")",
    "greatest('a' collate utf8mb4_general_ci, 'B')",
    "hex(weight_string('a'))",
    "hex(weight_string('aAÁàãăâ' collate utf8mb4_general_ci))",
    "if(cast('3' as json), 1, 2)",
    "ifnull(1, 'x' regexp '[')",
    "ifnull(null, cast('[1]' as json))",
    "interval(\"9007199254740991\", \"9007199254740992\")",
    "interval(null, 1, 2)",
    "instr('abc', 'b')",
    "json_schema_valid('{\"required\":[\"a\"]}', '{\"a\":1}')",
    "load_file('')",
    "locate('b', 'abc')",
    "make_set(1, 'a', 'b', 'c')",
    "NULLIF(1, \"1.0\")",
    "oct(b'11111111')",
    "position('b' in 'abc')",
    "quote(x'ff')",
    "regexp_like('abc', 'abc', 'p')",
    "round(1.2345,'2')",
    "round(3.14,'abc')",
    "round(5, -100)",
    "substring_index('a.b.c', '.', '2')",
    "substring_index('a.b.c', '.', -9223372036854775808)",
    "substring_index('a.b.c', '.', 18446744073709551616)",
    "subtime('01:00:00.999999','02:00:00.999998')",
    "timestamp('2020-01-01','01:00:00')",
    "to_base64('')",
    "translate('ABC', 'A', 'B')",
    "translate('abcabc', 'ab', 'xy')",
    "trim('  a  ')",
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
    assert_eq!(COVERED.len(), 19, "the covered list changed size");
    assert_eq!(DECLINED.len(), 72, "the declined list changed size");
}

/// The resolver that milestone E ends up with: an engine context and no native
/// evaluator behind it.
struct EngineOnly;

impl Columns for EngineOnly {
    fn get(&self, _: &[String]) -> Option<Datum> {
        None
    }
    fn tikv_expression_context(&self) -> Option<Context> {
        Some(Context {
            flags: 482,
            ..Context::default()
        })
    }
    fn tikv_expression_required(&self) -> bool {
        true
    }
}

/// Every expression that still falls back to native must fail *cleanly* once
/// there is no native evaluator: a structured engine error, never a value.
///
/// This is what deleting the native evaluator will actually do to the 59, so
/// the removal's error contract is asserted here rather than discovered during
/// the cutover. An expression the rewriter cannot build without a column
/// resolver is skipped for the same reason it is on the declined side: the
/// adapter never sees it.
#[test]
fn every_declined_expression_fails_cleanly_without_the_native_evaluator() {
    let mut skipped: Vec<&str> = Vec::new();
    for expression in DECLINED {
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
            skipped.push(expression);
            continue;
        };
        let Some(ty) = rewritten.static_type().cloned() else {
            skipped.push(expression);
            continue;
        };
        let suite = EvaluatorSuite::new(vec![rewritten], true);
        let mut input = Chunk::new_empty(&[]);
        input.set_num_virtual_rows(1);
        let mut output = Chunk::new_with_capacity(std::slice::from_ref(&ty), 1);
        match suite.run(&EngineOnly, &mut input, &mut output) {
            Err(EvaluatorError::Eval(EvalError::ExternalEngine { code, message })) => {
                assert_eq!(code, 1105, "{expression}");
                assert!(
                    message.contains("declined"),
                    "{expression}: the error must name the refusal: {message}"
                );
            }
            Ok(()) => panic!(
                "{expression} produced a value with no native evaluator; the removal must refuse it"
            ),
            Err(other) => panic!("{expression} failed with {other:?} instead of a refusal"),
        }
    }
    // Both skips are planning-time refusals, not engine declines: the
    // row-value comparison needs a column resolver the rewriter does not have
    // here, and `convert(... using cp866)` names a charset the port does not
    // support, so neither expression ever reaches evaluation.
    assert_eq!(
        skipped,
        ["(1, 2) = (1, 2, 3)", "convert('haha' using cp866)"]
    );
}

/// The declined expressions that survive the planner's construction-time fold.
///
/// A **constant-only** expression does not necessarily reach the adapter at
/// all. `plan_builder.rs` folds the rewritten tree with the live statement
/// context (`fold_constant_in_mode`) before the plan exists. After physical
/// native-kernel deletion, constant-foldable declines become a single
/// `Constant`; the 48 pinned here are the ones whose constant form *does*
/// reach the adapter, which is the surface the removal actually has to answer
/// for.
///
/// The caveat the corpus cannot close: it is constant-only, so a
/// column-bearing shape (`round(col, '2')`) is never generated. The same
/// refusal that keeps `round(5, -100)` here would also keep `round(col, '2')`
/// native -- folding cannot remove a shape that carries a column. This list is
/// therefore the *observed* production surface, not a bound on it.
const SURVIVES_FOLD: &[&str] = &[
    "7 in (7, -9, 9)",
    "benchmark(-3, 1)",
    "case when cast('0' as json) then 1 end",
    "case when false then 1.5 else 0 end",
    "cast('\"123\"' as json) < cast('\"123\"' as json)",
    "cast('2019-11-02 22:00:05' as datetime) in (cast('2019-11-02 22:00:04' as datetime), cast('2019-11-02 22:00:05' as datetime))",
    "char(65, 16740, 67.5 using utf8)",
    "coalesce(1, 'x' regexp '[')",
    "coalesce(cast(1 as json), cast(2 as json))",
    "cot(1)",
    "elt(1.1, '2.1', '3.1', '11.1', '1.1')",
    "elt('2abc','x','y','z')",
    "field(1.10, 0, 11e-1)",
    "field(NULL, 2, 3, 11, 1)",
    "find_in_set('a', 'b,a,c,a')",
    "find_in_set(' ', '  , , ,') collate utf8mb4_general_ci",
    "find_in_set(' ' collate utf8mb4_general_ci, '  , , ,' collate utf8mb4_general_ci)",
    "format(12345.67, 2, 'en_us')",
    "format(1234567.89, 2, 'en_US')",
    "format_bytes(2048)",
    "format_nano_time(2000)",
    "tidb_decode_binary_plan('malformed')",
    "tidb_decode_plan('malformed')",
    "tidb_encode_sql_digest('select 1')",
    "greatest(-9223372036854775808, cast('9223372036854775809' as unsigned))",
    "hex(weight_string('a'))",
    "hex(weight_string('aAÁàãăâ' collate utf8mb4_general_ci))",
    "if(cast('3' as json), 1, 2)",
    "ifnull(1, 'x' regexp '[')",
    "ifnull(null, cast('[1]' as json))",
    "instr('abc', 'b')",
    "locate('b', 'abc')",
    "make_set(1, 'a', 'b', 'c')",
    "oct(b'11111111')",
    "position('b' in 'abc')",
    "quote(x'ff')",
    "regexp_like('abc', 'abc', 'p')",
    "round(1.2345,'2')",
    "round(3.14,'abc')",
    "round(5, -100)",
    "substring_index('a.b.c', '.', '2')",
    "substring_index('a.b.c', '.', -9223372036854775808)",
    "substring_index('a.b.c', '.', 18446744073709551616)",
    "to_base64('')",
    "translate('ABC', 'A', 'B')",
    "translate('abcabc', 'ab', 'xy')",
    "trim('  a  ')",
    "truncate(1234.5678,'-2')",
    "upper(elt(1,'a',x'61'))",
    "weight_string(NULL)",
];

/// Which declined expressions the planner folds away, measured with
/// `NoColumns` (the fold's outcome for these constants does not depend on
/// session state).
#[test]
fn folded_away_expressions_never_reach_the_adapter() {
    let mut survived = Vec::new();
    let mut folded = Vec::new();
    let mut skipped = Vec::new();
    for expression in DECLINED {
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
        let Ok(mut rewritten) = rewrite_expr(expr) else {
            skipped.push(*expression);
            continue;
        };
        tidb_expr::fold_constant_in_mode(
            &mut rewritten,
            &tidb_expr::NoColumns,
            tidb_expr::ConstantFoldMode::Normal,
        );
        if matches!(rewritten, tidb_expr::expression::Expression::Constant(_)) {
            folded.push(*expression);
        } else {
            survived.push(*expression);
        }
    }
    assert_eq!(survived, SURVIVES_FOLD);
    // The same two planning-time refusals the post-deletion test pins never
    // reach the fold either.
    assert_eq!(
        skipped,
        ["(1, 2) = (1, 2, 3)", "convert('haha' using cp866)"]
    );
    assert_eq!(
        folded.len() + survived.len() + skipped.len(),
        DECLINED.len()
    );
    // CHAR_FUNC, contracted FIELD/ELT shapes, and OCT(binary literal) can no
    // longer fold through native kernels; they stay visible above.
    assert_eq!(folded.len(), 20, "the folded count changed");
}
