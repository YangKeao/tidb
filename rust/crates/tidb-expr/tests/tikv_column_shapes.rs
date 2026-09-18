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

//! The fallback surface of a *column-bearing* expression, which the constant
//! corpus cannot measure.
//!
//! `tikv_ratchet.rs` measures the adapter on entirely-constant expressions, and
//! corpus plan section 7.13 shows that 40 of its 59 declines are folded to a
//! `Constant` by the planner before the adapter sees them. Folding cannot
//! remove a shape that carries a column, so `round(col, '2')` stays a real
//! fallback even though `round(5, -100)` is folded away. Nothing measured that
//! direction, so this test does: each expression below names a column and is
//! rewritten through a resolver, exactly as the planner does, and the pinned
//! outcome is asserted in both directions.
//!
//! Column names pick the type: `i*` is BIGINT, `b*` is the BINARY charset,
//! `s*` is `utf8mb4_bin` text, `g*` is `utf8mb4_general_ci` text, `d*` is
//! DATETIME, `c*` is NEWDECIMAL.

use tidb_ast::{QueryStmt, SelectField, Stmt};
use tidb_datatype::{FieldType, FieldTypeCode};
use tidb_expr::rewriter::{rewrite_expr_resolved, ColumnResolver};
use tidb_expr::tikv::{Context, TikvExpression};

fn column_type(name: &str) -> FieldType {
    match name.chars().next() {
        Some('i') => FieldType::new(FieldTypeCode::LongLong),
        Some('s') => FieldType::new(FieldTypeCode::VarString)
            .with_charset_name("utf8mb4")
            .with_collation_name("utf8mb4_bin"),
        Some('g') => FieldType::new(FieldTypeCode::VarString)
            .with_charset_name("utf8mb4")
            .with_collation_name("utf8mb4_general_ci"),
        Some('d') => FieldType::new(FieldTypeCode::Datetime).with_decimal(6),
        Some('b') => FieldType::new(FieldTypeCode::VarString)
            .with_charset_name("binary")
            .with_collation_name("binary"),
        Some('c') => FieldType::new(FieldTypeCode::NewDecimal)
            .with_flen(20)
            .with_decimal(4),
        other => panic!("unknown column prefix {other:?}"),
    }
}

/// Resolves every distinct column name to its own input index and a type from
/// its prefix, so the rewriter mints exactly the shape a real plan would: two
/// different columns never share an index, and a column keeps its index across
/// the tree.
#[derive(Default)]
struct ColumnsByPrefix {
    seen: std::cell::RefCell<std::collections::HashMap<String, usize>>,
}

impl ColumnResolver for ColumnsByPrefix {
    fn resolve(&self, path: &[String]) -> Option<(usize, FieldType, i64)> {
        let name = path.last()?.clone();
        let mut seen = self.seen.borrow_mut();
        let next = seen.len();
        let index = *seen.entry(name.clone()).or_insert(next);
        let unique_id = i64::try_from(index).ok()? + 1;
        Some((index, column_type(&name), unique_id))
    }

    fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
        tidb_datatype::SessionTimeZone::utc()
    }
}

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
    let rewritten = rewrite_expr_resolved(expr, &ColumnsByPrefix::default()).expect("rewrite");
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

/// Shapes a real query can carry over a column. `false` means the adapter
/// declines and the native evaluator answers.
const SHAPES: &[(&str, bool)] = &[
    ("i0 + 1", true),
    ("i0 * 2", true),
    ("i0 > 1", true),
    ("i0 is null", true),
    ("abs(i0)", true),
    ("upper(s0)", true),
    ("length(s0)", true),
    // `max_allowed_packet` policy: the facade has no such setting.
    ("concat(s0, s0)", false),
    ("i0 in (1, 2)", true),
    ("coalesce(i0, 1)", true),
    ("ifnull(i0, 1)", true),
    ("nullif(i0, 1)", true),
    ("if(i0, s0, s0)", true),
    ("case when i0 then s0 else s0 end", true),
    ("least(i0, 1)", true),
    ("interval(i0, 1, 2)", true),
    ("elt(i0, s0, s0)", true),
    // The statement clock is not in the facade context.
    ("d0 > now()", false),
    ("year(d0)", true),
    ("extract(year from d0)", true),
    ("cast(i0 as signed)", true),
    ("cast(s0 as char)", true),
    ("cast(d0 as date)", true),
    // Digits: a literal int runs in the engine, a string one does not.
    ("round(c0, 2)", true),
    ("round(c0, '2')", false),
    ("truncate(c0, -2)", false),
    // Collation-sensitive strings.
    // `utf8mb4_bin` PADS trailing spaces, which a bytewise kernel does not,
    // so only the BINARY charset is admitted.
    ("find_in_set(s0, 'a,b,c')", false),
    ("find_in_set(s0, s1)", false),
    ("find_in_set(b0, b1)", true),
    ("find_in_set(g0, 'a,b,c')", false),
    ("greatest(s0, 'x')", false),
    ("greatest(s0, s1)", false),
    ("greatest(g0, 'x')", false),
    ("greatest(g0, g1)", false),
    ("greatest(b0, b1)", true),
    // Kernels and policies the pinned engine does not have.
    ("weight_string(s0)", false),
    ("cast(i0 as json)", false),
    ("json_schema_valid(s0, '{}')", false),
    ("load_file(s0)", false),
    ("translate(s0, 'a', 'b')", false),
    ("benchmark(1, i0)", false),
    ("i0 regexp '['", false),
    ("regexp_like(s0, '[', 'p')", false),
    ("cot(i0)", false),
];

#[test]
fn column_bearing_shapes_have_the_pinned_outcome() {
    let mut wrong = Vec::new();
    let mut covered = 0;
    for (expression, expected) in SHAPES {
        let actual = engine_owns(expression);
        if actual {
            covered += 1;
        }
        if actual != *expected {
            wrong.push(format!(
                "{expression}: expected {}, adapter says {}",
                if *expected { "engine" } else { "native" },
                if actual { "engine" } else { "native" }
            ));
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
    assert_eq!(covered, 24, "the covered count changed");
    assert_eq!(SHAPES.len(), 44, "the shape list changed size");
}
