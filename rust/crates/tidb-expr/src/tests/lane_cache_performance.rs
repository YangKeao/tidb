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

//! Manual release-profile microbenchmark for the lane-cache-only runtime.
//!
//! This is ignored in ordinary test runs. It deliberately parses and rewrites
//! outside the timed region, then compares an unbound one-shot context with one
//! cache held by the evaluation lane. Run with one test thread and `--nocapture`.
//! The expressions are intentionally unfolded constant-kernel probes, not
//! parser/planner/storage/server or real-row-batch benchmarks.

use std::hint::black_box;
use std::time::Instant;

use tidb_ast::{QueryStmt, SelectField, Stmt};

use super::*;

const WARMUPS: usize = 2_000;
const SAMPLES: usize = 9;

struct Workload {
    name: &'static str,
    sql: &'static str,
    expected: &'static str,
    iterations: usize,
}

const WORKLOADS: [Workload; 6] = [
    Workload {
        name: "integer_add",
        sql: "1 + 2",
        expected: "INT:3",
        iterations: 200_000,
    },
    Workload {
        name: "lazy_if",
        sql: "if(1, 2, 3)",
        expected: "INT:2",
        iterations: 100_000,
    },
    Workload {
        name: "strcmp",
        sql: "strcmp('abc', 'abd')",
        expected: "INT:-1",
        iterations: 50_000,
    },
    Workload {
        name: "decimal_add",
        sql: "cast(123.45 as decimal(10,2)) + cast(0.55 as decimal(10,2))",
        expected: "DEC:124.00",
        iterations: 30_000,
    },
    Workload {
        name: "json_type",
        sql: "json_type('{\"a\":1}')",
        expected: "STR:OBJECT",
        iterations: 30_000,
    },
    Workload {
        name: "regexp_like",
        sql: "regexp_like('abcdef', '^abc')",
        expected: "INT:1",
        iterations: 10_000,
    },
];

fn iteration_divisor() -> usize {
    std::env::var("EXPR_LANE_PERF_ITERATION_DIVISOR")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(1)
}

fn iteration_multiplier() -> usize {
    std::env::var("EXPR_LANE_PERF_ITERATION_MULTIPLIER")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(1)
}

fn cpu_calibration(stage: &str) {
    let start = Instant::now();
    let mut value = 0x9e3779b97f4a7c15_u64;
    for index in 0..20_000_000_u64 {
        value ^= index.wrapping_add(value.rotate_left(13));
        value = value.wrapping_mul(0xbf58476d1ce4e5b9);
        black_box(value);
    }
    let elapsed = start.elapsed().as_nanos();
    println!(
        "EXPR_LANE_CALIBRATION stage={stage} iterations=20000000 elapsed_ns={elapsed} \
         ns_per_iter={:.3} checksum={value}",
        elapsed as f64 / 20_000_000.0
    );
}

fn parse_ast(sql: &str) -> tidb_ast::Expr {
    let stmt = tidb_parser::parse(&format!("select {sql}")).expect("benchmark expression parses");
    let Stmt::Query(query) = stmt else {
        panic!("benchmark statement is not a query")
    };
    let QueryStmt::Select(select) = query.into_inner() else {
        panic!("benchmark query is not a select")
    };
    let SelectField::Expr { expr, .. } = select.fields.into_iter().next().expect("one field")
    else {
        panic!("benchmark select field is not an expression")
    };
    expr
}

fn verify(
    tier: &str,
    mode: &str,
    workload: &Workload,
    eval: &mut impl FnMut() -> Result<Datum, EvalError>,
) {
    let actual = eval()
        .unwrap_or_else(|error| {
            panic!(
                "{tier}/{mode}/{} failed before timing: {error:?}",
                workload.name
            )
        })
        .label();
    assert_eq!(actual, workload.expected, "{tier}/{mode}/{}", workload.name);
}

fn warm(eval: &mut impl FnMut() -> Result<Datum, EvalError>) {
    for _ in 0..WARMUPS {
        black_box(eval().expect("warm benchmark evaluation succeeds"));
    }
}

fn sample(
    tier: &str,
    mode: &str,
    workload: &Workload,
    sample: usize,
    eval: &mut impl FnMut() -> Result<Datum, EvalError>,
) {
    let iterations = workload
        .iterations
        .saturating_mul(iteration_multiplier())
        .checked_div(iteration_divisor())
        .unwrap_or(0)
        .max(1);
    let start = Instant::now();
    let mut last = Datum::Null;
    for _ in 0..iterations {
        last = black_box(eval().expect("timed benchmark evaluation succeeds"));
    }
    let elapsed = start.elapsed().as_nanos();
    assert_eq!(last.label(), workload.expected);
    println!(
        "EXPR_LANE_PERF tier={tier} mode={mode} name={} sample={sample} \
         iterations={iterations} elapsed_ns={elapsed} ns_per_eval={:.3} result={}",
        workload.name,
        elapsed as f64 / iterations as f64,
        workload.expected,
    );
}

fn measure_pair(
    tier: &str,
    workload: &Workload,
    mut one_shot: impl FnMut() -> Result<Datum, EvalError>,
    mut lane_cache: impl FnMut() -> Result<Datum, EvalError>,
) {
    verify(tier, "one_shot", workload, &mut one_shot);
    verify(tier, "lane_cache", workload, &mut lane_cache);
    warm(&mut one_shot);
    warm(&mut lane_cache);

    // Balanced ABBA/BABA sample order prevents a fixed mode/thermal ordering.
    for sample_index in 0..SAMPLES {
        if sample_index % 4 == 0 || sample_index % 4 == 3 {
            sample(tier, "one_shot", workload, sample_index, &mut one_shot);
            sample(tier, "lane_cache", workload, sample_index, &mut lane_cache);
        } else {
            sample(tier, "lane_cache", workload, sample_index, &mut lane_cache);
            sample(tier, "one_shot", workload, sample_index, &mut one_shot);
        }
    }
}

fn run_ast() {
    for workload in &WORKLOADS {
        let expr = parse_ast(workload.sql);
        let cache = ReadyValueCache::new();
        cache.with_columns(&NoColumns, |columns| {
            measure_pair(
                "ast",
                workload,
                || eval_in(&expr, &NoColumns),
                || eval_in(&expr, columns),
            );
        });
        let workers = cache.prepared_worker_count();
        assert!(
            workers > 0,
            "AST workload {} bypassed the cache",
            workload.name
        );
        println!(
            "EXPR_LANE_PERF_WORKERS tier=ast name={} count={workers}",
            workload.name
        );
    }
}

fn run_chunk() {
    for workload in &WORKLOADS {
        let expr = parse_ast(workload.sql);
        let rewritten =
            crate::rewriter::rewrite_expr(&expr).expect("benchmark expression rewrites");
        let mut chunk = tidb_chunk::chunk::Chunk::new_empty(&[]);
        chunk.set_num_virtual_rows(1);
        let cache = ReadyValueCache::new();
        cache.with_columns(&NoColumns, |columns| {
            measure_pair(
                "chunk",
                workload,
                || rewritten.eval(&NoColumns, chunk.get_row(0)),
                || rewritten.eval(columns, chunk.get_row(0)),
            );
        });
        let workers = cache.prepared_worker_count();
        assert!(
            workers > 0,
            "chunk workload {} bypassed the cache",
            workload.name
        );
        println!(
            "EXPR_LANE_PERF_WORKERS tier=chunk name={} count={workers}",
            workload.name
        );
    }
}

#[test]
#[ignore = "manual release-profile performance probe"]
fn lane_cache_release_performance_probe() {
    println!(
        "EXPR_LANE_PERF_CONFIG warmups={WARMUPS} samples={SAMPLES} iteration_divisor={} \
         iteration_multiplier={} order=balanced-abba \
         md5=excluded-known-release-prepare-failure",
        iteration_divisor(),
        iteration_multiplier()
    );
    cpu_calibration("before");
    run_ast();
    run_chunk();
    cpu_calibration("after");
}
