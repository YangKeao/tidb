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

//! Manual release-profile benchmark for real `Chunk` evaluator batches.
//!
//! The benchmark keeps one warm `EvaluatorSuite` per workload/scenario lane,
//! reuses input and output chunks, includes output reset/materialization, and
//! exercises the same
//! `run_with_tikv_numeric` entry used by projection. Unsupported expressions
//! deliberately fall back inside that production entry instead of using a
//! benchmark-only evaluator.

use std::hint::black_box;
use std::time::Instant;

use tidb_ast::{QueryStmt, SelectField, Stmt};
use tidb_chunk::chunk::Chunk;
use tidb_datatype::{BinaryJSON, Decimal, FieldType, FieldTypeCode, SessionTimeZone};

use super::*;
use crate::evaluator::EvaluatorSuite;
use crate::expression::Expression;
use crate::rewriter::{rewrite_expr_resolved, ColumnResolver};

const SAMPLES: usize = 7;
const WARMUP_BATCHES: usize = 32;
const TARGET_SAMPLE_NS: u128 = 100_000_000;
const DENSE_BATCH_SIZES: [usize; 5] = [1, 8, 64, 256, 1024];

fn target_sample_ns() -> u128 {
    std::env::var("EXPR_BATCH_PERF_TARGET_NS")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(TARGET_SAMPLE_NS)
}

fn sample_count() -> usize {
    std::env::var("EXPR_BATCH_PERF_SAMPLES")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(SAMPLES)
}

fn workload_enabled(name: &str) -> bool {
    std::env::var("EXPR_BATCH_PERF_WORKLOAD")
        .map(|filter| filter == name)
        .unwrap_or(true)
}

fn batch_size_enabled(size: usize) -> bool {
    std::env::var("EXPR_BATCH_PERF_BATCH_SIZE")
        .ok()
        .and_then(|value| value.parse().ok())
        .map(|filter: usize| filter == size)
        .unwrap_or(true)
}

struct Workload {
    name: &'static str,
    sql: &'static str,
}

const WORKLOADS: [Workload; 15] = [
    Workload {
        name: "integer_add",
        sql: "i + j",
    },
    Workload {
        name: "integer_add_literal",
        sql: "i + 7",
    },
    Workload {
        name: "integer_add_nested",
        sql: "(i + j) + (i + 11)",
    },
    Workload {
        name: "integer_multiply",
        sql: "i * j",
    },
    Workload {
        name: "abs",
        sql: "abs(i)",
    },
    Workload {
        name: "lazy_if",
        sql: "if(i > 0, i, j)",
    },
    Workload {
        name: "coalesce_nullif",
        sql: "coalesce(nullif(i, 0), j)",
    },
    Workload {
        name: "strcmp",
        sql: "strcmp(s, 'beta')",
    },
    Workload {
        name: "lower",
        sql: "lower(s)",
    },
    Workload {
        name: "upper",
        sql: "upper(s)",
    },
    Workload {
        name: "concat",
        sql: "concat(s, '-x')",
    },
    Workload {
        name: "substring",
        sql: "substring(s, 2, 3)",
    },
    Workload {
        name: "decimal_add",
        sql: "d + cast(0.55 as decimal(10,2))",
    },
    Workload {
        name: "json_type",
        sql: "json_type(js)",
    },
    Workload {
        name: "regexp_like",
        sql: "regexp_like(s, '^a')",
    },
];

struct Resolver {
    schema: Vec<FieldType>,
}

impl ColumnResolver for Resolver {
    fn resolve(&self, path: &[String]) -> Option<(usize, FieldType, i64)> {
        let (index, unique_id) = match path.last()?.as_str() {
            "i" => (0, 1),
            "j" => (1, 2),
            "s" => (2, 3),
            "d" => (3, 4),
            "js" => (4, 5),
            _ => return None,
        };
        Some((index, self.schema[index].clone(), unique_id))
    }

    fn time_zone(&self) -> SessionTimeZone {
        SessionTimeZone::utc()
    }
}

fn schema() -> Vec<FieldType> {
    let mut decimal = FieldType::new(FieldTypeCode::NewDecimal);
    decimal.set_flen(10);
    decimal.set_decimal(2);
    vec![
        FieldType::new(FieldTypeCode::LongLong),
        FieldType::new(FieldTypeCode::LongLong),
        FieldType::new(FieldTypeCode::VarString),
        decimal,
        FieldType::new(FieldTypeCode::Json),
    ]
}

fn rewrite(sql: &str, resolver: &Resolver) -> Expression {
    let stmt = tidb_parser::parse(&format!("select {sql}")).expect("batch expression parses");
    let Stmt::Query(query) = stmt else {
        panic!("benchmark statement is not a query")
    };
    let QueryStmt::Select(select) = query.into_inner() else {
        panic!("benchmark query is not a select")
    };
    let SelectField::Expr { expr, .. } = &select.fields[0] else {
        panic!("benchmark select field is not an expression")
    };
    rewrite_expr_resolved(expr, resolver).expect("batch expression rewrites")
}

fn make_input(
    schema: &[FieldType],
    physical_rows: usize,
    selection: Option<Vec<usize>>,
    null_every_ten: bool,
) -> Chunk {
    const STRINGS: [&[u8]; 6] = [b"Alpha", b"beta", b"abcdef", b"TiDB", b"vectorized", b"z"];
    let decimals = [
        Decimal::from_literal("123.45"),
        Decimal::from_literal("-7.25"),
        Decimal::from_literal("0.00"),
        Decimal::from_literal("999.99"),
    ];
    let json = [
        BinaryJSON::parse(r#"{"a":1}"#).expect("object JSON"),
        BinaryJSON::parse(r#"[1,2,3]"#).expect("array JSON"),
        BinaryJSON::parse(r#""text""#).expect("string JSON"),
        BinaryJSON::parse("7").expect("number JSON"),
    ];
    let mut chunk = Chunk::new_with_capacity(schema, physical_rows);
    for row in 0..physical_rows {
        let is_null = null_every_ten && row % 10 == 0;
        if is_null {
            chunk.append_null(0);
        } else {
            chunk.append_int64(0, row as i64 % 97 - 48);
        }
        chunk.append_int64(1, row as i64 % 13 + 1);
        if is_null {
            chunk.append_null(2);
            chunk.append_null(3);
            chunk.append_null(4);
        } else {
            chunk.append_bytes(2, STRINGS[row % STRINGS.len()]);
            chunk.append_datum(3, &Datum::Decimal(decimals[row % decimals.len()].clone()));
            chunk.append_json(4, &json[row % json.len()]);
        }
    }
    chunk.set_sel(selection);
    chunk
}

fn checksum_datums(values: impl IntoIterator<Item = Datum>) -> u64 {
    values
        .into_iter()
        .fold(0xcbf29ce484222325, |mut hash, value| {
            for byte in value.label().bytes() {
                hash ^= u64::from(byte);
                hash = hash.wrapping_mul(0x100000001b3);
            }
            hash
        })
}

fn expected_checksum(expression: &Expression, input: &Chunk) -> u64 {
    checksum_datums((0..input.num_rows()).map(|row| {
        expression
            .eval(&NoColumns, input.get_row(row))
            .expect("scalar reference succeeds")
    }))
}

fn output_checksum(output: &Chunk, field: &FieldType) -> u64 {
    checksum_datums((0..output.num_rows()).map(|row| output.get_row(row).get_datum(0, field)))
}

fn validate_output(
    workload: &Workload,
    input: &Chunk,
    output: &Chunk,
    field: &FieldType,
    reference_checksum: u64,
) {
    assert_eq!(output.num_rows(), input.num_rows());
    assert_eq!(output_checksum(output, field), reference_checksum);
    if !workload.name.starts_with("integer_add") {
        return;
    }
    for logical_row in 0..input.num_rows() {
        let source = input.get_row(logical_row);
        let result = output.get_row(logical_row);
        if source.is_null(0) {
            assert!(result.is_null(0));
            continue;
        }
        let left = source.get_int64(0);
        let expected = match workload.name {
            "integer_add" => left + source.get_int64(1),
            "integer_add_literal" => left + 7,
            "integer_add_nested" => left + source.get_int64(1) + left + 11,
            _ => unreachable!("checked integer-add workload"),
        };
        assert_eq!(result.get_int64(0), expected);
    }
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
        "EXPR_BATCH_CALIBRATION stage={stage} iterations=20000000 elapsed_ns={elapsed} \
         ns_per_iter={:.3} checksum={value}",
        elapsed as f64 / 20_000_000.0
    );
}

fn run_once(suite: &EvaluatorSuite, input: &mut Chunk, output: &mut Chunk, schema: &[FieldType]) {
    output.reset();
    suite
        .run_with_tikv_numeric(&NoColumns, input, output, schema)
        .expect("production batch evaluation succeeds");
    black_box(output.num_rows());
}

fn bench_scenario(
    workload: &Workload,
    scenario: &str,
    physical_rows: usize,
    selection: Option<Vec<usize>>,
    null_every_ten: bool,
) {
    let schema = schema();
    let resolver = Resolver {
        schema: schema.clone(),
    };
    let expression = rewrite(workload.sql, &resolver);
    let output_field = expression
        .static_type()
        .expect("rewritten expression has a static type")
        .clone();
    let mut input = make_input(&schema, physical_rows, selection, null_every_ten);
    let selected_rows = input.num_rows();
    let expected = expected_checksum(&expression, &input);
    let suite = EvaluatorSuite::new(vec![expression], false);
    let mut output = Chunk::new_with_capacity(std::slice::from_ref(&output_field), selected_rows);

    run_once(&suite, &mut input, &mut output, &schema);
    // The current private TiKV vector admission is intentionally narrow: only
    // signed LongLong PLUS is admitted. All other common workloads measure the
    // real production fallback reached by the same projection entry.
    let tikv_numeric = suite.tikv_numeric_lane_prepared_for_test();
    assert_eq!(tikv_numeric, workload.name.starts_with("integer_add"));
    let route = if tikv_numeric {
        "tikv_numeric"
    } else {
        "production_fallback"
    };
    validate_output(workload, &input, &output, &output_field, expected);
    for _ in 0..WARMUP_BATCHES {
        run_once(&suite, &mut input, &mut output, &schema);
    }

    let calibration_batches = 4usize;
    let start = Instant::now();
    for _ in 0..calibration_batches {
        run_once(&suite, &mut input, &mut output, &schema);
    }
    let calibration_ns = start.elapsed().as_nanos().max(1);
    let iterations = ((target_sample_ns() * calibration_batches as u128) / calibration_ns)
        .clamp(1, 1_000_000) as usize;

    for sample in 0..sample_count() {
        let start = Instant::now();
        for _ in 0..iterations {
            run_once(&suite, &mut input, &mut output, &schema);
        }
        let elapsed = start.elapsed().as_nanos();
        assert_eq!(output.num_rows(), selected_rows);
        assert_eq!(output_checksum(&output, &output_field), expected);
        let batches = iterations as f64;
        let rows = (iterations * selected_rows) as f64;
        println!(
            "EXPR_BATCH_PERF mode=current_production route={route} name={} scenario={scenario} \
             physical_rows={physical_rows} selected_rows={selected_rows} sample={sample} \
             iterations={iterations} elapsed_ns={elapsed} ns_per_batch={:.3} \
             ns_per_row={:.3} rows_per_second={:.3} checksum={expected}",
            workload.name,
            elapsed as f64 / batches,
            elapsed as f64 / rows,
            rows * 1_000_000_000.0 / elapsed as f64,
        );
    }
}

#[test]
#[ignore = "manual stable release-profile batch performance probe"]
fn lane_cache_batch_release_performance_probe() {
    println!(
        "EXPR_BATCH_CONFIG samples={} warmup_batches={WARMUP_BATCHES} \
         target_sample_ns={} dense_sizes=1,8,64,256,1024",
        sample_count(),
        target_sample_ns()
    );
    cpu_calibration("before");
    for workload in &WORKLOADS {
        if !workload_enabled(workload.name) {
            continue;
        }
        for batch_size in DENSE_BATCH_SIZES {
            if !batch_size_enabled(batch_size) {
                continue;
            }
            bench_scenario(workload, "dense", batch_size, None, false);
            if workload.name.starts_with("integer_add") || workload.name == "strcmp" {
                bench_scenario(workload, "dense_null10", batch_size, None, true);
                bench_scenario(
                    workload,
                    "sparse_quarter",
                    batch_size,
                    Some((0..batch_size).step_by(4).collect()),
                    false,
                );
                bench_scenario(
                    workload,
                    "reverse",
                    batch_size,
                    Some((0..batch_size).rev().collect()),
                    false,
                );
                bench_scenario(
                    workload,
                    "duplicate",
                    batch_size,
                    Some((0..batch_size).map(|row| row / 2).collect()),
                    false,
                );
            }
        }
    }
    cpu_calibration("after");
}
