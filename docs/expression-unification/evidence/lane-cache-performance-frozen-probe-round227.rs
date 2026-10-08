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

//! Temporary frozen-revision half of the lane-cache performance comparison.

use std::hint::black_box;
use std::time::Instant;

use tidb_ast::{QueryStmt, SelectField, Stmt};

use super::*;

const WARMUPS: usize = 2_000;
const SAMPLES: usize = 9;

const WORKLOADS: [(&str, &str, &str, usize); 6] = [
    ("integer_add", "1 + 2", "INT:3", 200_000),
    ("lazy_if", "if(1, 2, 3)", "INT:2", 100_000),
    ("strcmp", "strcmp('abc', 'abd')", "INT:-1", 50_000),
    (
        "decimal_add",
        "cast(123.45 as decimal(10,2)) + cast(0.55 as decimal(10,2))",
        "DEC:124.00",
        30_000,
    ),
    ("json_type", "json_type('{\"a\":1}')", "STR:OBJECT", 30_000),
    (
        "regexp_like",
        "regexp_like('abcdef', '^abc')",
        "INT:1",
        10_000,
    ),
];

fn parse_ast(sql: &str) -> tidb_ast::Expr {
    let stmt = tidb_parser::parse(&format!("select {sql}")).expect("benchmark expression parses");
    let Stmt::Query(query) = stmt else {
        panic!("not query")
    };
    let QueryStmt::Select(select) = query.into_inner() else {
        panic!("not select")
    };
    let SelectField::Expr { expr, .. } = select.fields.into_iter().next().expect("one field")
    else {
        panic!("not expression")
    };
    expr
}

fn measure(
    tier: &str,
    name: &str,
    expected: &str,
    iterations: usize,
    mut eval: impl FnMut() -> Result<Datum, EvalError>,
) {
    assert_eq!(eval().expect("validation succeeds").label(), expected);
    for _ in 0..WARMUPS {
        black_box(eval().expect("warm evaluation succeeds"));
    }
    for sample in 0..SAMPLES {
        let start = Instant::now();
        let mut last = Datum::Null;
        for _ in 0..iterations {
            last = black_box(eval().expect("timed evaluation succeeds"));
        }
        let elapsed = start.elapsed().as_nanos();
        assert_eq!(last.label(), expected);
        println!(
            "EXPR_LANE_PERF tier={tier} mode=frozen_native name={name} sample={sample} \
             iterations={iterations} elapsed_ns={elapsed} ns_per_eval={:.3} result={expected}",
            elapsed as f64 / iterations as f64,
        );
    }
}

#[test]
#[ignore = "manual release-profile performance probe"]
fn lane_cache_release_performance_probe() {
    println!("EXPR_LANE_PERF_CONFIG warmups={WARMUPS} samples={SAMPLES} revision=frozen");
    for (name, sql, expected, iterations) in WORKLOADS {
        let expr = parse_ast(sql);
        measure("ast", name, expected, iterations, || {
            eval_in(&expr, &NoColumns)
        });
    }
    for (name, sql, expected, iterations) in WORKLOADS {
        let expr = parse_ast(sql);
        let rewritten =
            crate::rewriter::rewrite_expr(&expr).expect("benchmark expression rewrites");
        let mut chunk = tidb_chunk::chunk::Chunk::new_empty(&[]);
        chunk.set_num_virtual_rows(1);
        measure("chunk", name, expected, iterations, || {
            rewritten.eval(&NoColumns, chunk.get_row(0))
        });
    }
}
