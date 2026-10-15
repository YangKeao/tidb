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
//! `run_with_tikv_batch` entry used by projection. Unsupported expressions
//! deliberately fall back inside that production entry instead of using a
//! benchmark-only evaluator.

use std::hint::black_box;
use std::time::Instant;

use tidb_ast::{QueryStmt, SelectField, Stmt};
use tidb_chunk::chunk::Chunk;
use tidb_datatype::{
    parse_datetime, BinaryJSON, Decimal, FieldType, FieldTypeCode, MySqlDuration, SessionTimeZone,
    TimeType, VectorFloat32,
};

use super::*;
use crate::evaluator::EvaluatorSuite;
use crate::expression::Expression;
use crate::rewriter::{rewrite_expr_resolved, ColumnResolver};

const SAMPLES: usize = 7;
const WARMUP_BATCHES: usize = 32;
const TARGET_SAMPLE_NS: u128 = 100_000_000;
const DENSE_BATCH_SIZES: [usize; 4] = [1, 16, 256, 1024];

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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PerfMode {
    CurrentFallbackAblation,
    CurrentTikvRowAblation,
    BatchTikv,
}

fn perf_mode() -> PerfMode {
    match std::env::var("EXPR_BATCH_PERF_MODE").as_deref() {
        Ok("current_fallback_ablation") => PerfMode::CurrentFallbackAblation,
        Ok("current_tikv_row_ablation") => PerfMode::CurrentTikvRowAblation,
        Ok("batch_tikv") | Err(_) => PerfMode::BatchTikv,
        Ok(mode) => panic!("unknown EXPR_BATCH_PERF_MODE={mode}"),
    }
}

struct Workload {
    name: &'static str,
    sql: &'static str,
}

const WORKLOADS: [Workload; 57] = [
    Workload {
        name: "isnull_int",
        sql: "isnull(i)",
    },
    Workload {
        name: "isnull_real",
        sql: "isnull(r)",
    },
    Workload {
        name: "isnull_decimal",
        sql: "isnull(d)",
    },
    Workload {
        name: "isnull_string",
        sql: "isnull(s)",
    },
    Workload {
        name: "isnull_datetime",
        sql: "isnull(dt)",
    },
    Workload {
        name: "isnull_timestamp",
        sql: "isnull(ts)",
    },
    Workload {
        name: "isnull_duration",
        sql: "isnull(dur)",
    },
    Workload {
        name: "isnull_vector",
        sql: "isnull(v)",
    },
    Workload {
        name: "integer_bit_bitand",
        sql: "i & j",
    },
    Workload {
        name: "integer_bit_bitor",
        sql: "i | j",
    },
    Workload {
        name: "integer_bit_bitxor",
        sql: "i ^ j",
    },
    Workload {
        name: "integer_bit_leftshift",
        sql: "i << j",
    },
    Workload {
        name: "integer_bit_rightshift",
        sql: "i >> j",
    },
    Workload {
        name: "real_compare_lt",
        sql: "r < q",
    },
    Workload {
        name: "real_compare_le",
        sql: "r <= q",
    },
    Workload {
        name: "real_compare_gt",
        sql: "r > q",
    },
    Workload {
        name: "real_compare_ge",
        sql: "r >= q",
    },
    Workload {
        name: "real_compare_eq",
        sql: "r = q",
    },
    Workload {
        name: "real_compare_ne",
        sql: "r != q",
    },
    Workload {
        name: "real_compare_nulleq",
        sql: "r <=> q",
    },
    Workload {
        name: "decimal_compare_lt",
        sql: "d < e",
    },
    Workload {
        name: "decimal_compare_le",
        sql: "d <= e",
    },
    Workload {
        name: "decimal_compare_gt",
        sql: "d > e",
    },
    Workload {
        name: "decimal_compare_ge",
        sql: "d >= e",
    },
    Workload {
        name: "decimal_compare_eq",
        sql: "d = e",
    },
    Workload {
        name: "decimal_compare_ne",
        sql: "d != e",
    },
    Workload {
        name: "decimal_compare_nulleq",
        sql: "d <=> e",
    },
    Workload {
        name: "integer_add",
        sql: "i + j",
    },
    Workload {
        name: "integer_compare_lt",
        sql: "i < j",
    },
    Workload {
        name: "integer_compare_le",
        sql: "i <= j",
    },
    Workload {
        name: "integer_compare_gt",
        sql: "i > j",
    },
    Workload {
        name: "integer_compare_ge",
        sql: "i >= j",
    },
    Workload {
        name: "integer_compare_eq",
        sql: "i = j",
    },
    Workload {
        name: "integer_compare_ne",
        sql: "i != j",
    },
    Workload {
        name: "integer_compare_nulleq",
        sql: "i <=> j",
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
        name: "integer_subtract",
        sql: "i - j",
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
        name: "char_length",
        sql: "char_length(s)",
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
        sql: "d + d",
    },
    Workload {
        name: "real_add",
        sql: "r + q",
    },
    Workload {
        name: "json_type",
        sql: "json_type(js)",
    },
    Workload {
        name: "regexp_like",
        sql: "regexp_like(s, '^a')",
    },
    Workload {
        name: "any_value_datetime",
        sql: "any_value(dt)",
    },
    Workload {
        name: "any_value_timestamp",
        sql: "any_value(ts)",
    },
    Workload {
        name: "any_value_duration",
        sql: "any_value(dur)",
    },
    Workload {
        name: "any_value_json",
        sql: "any_value(js)",
    },
    Workload {
        name: "any_value_vector",
        sql: "any_value(v)",
    },
];

fn tikv_batch_workload(name: &str) -> bool {
    name.starts_with("isnull_")
        || name.starts_with("integer_bit_")
        || name.starts_with("integer_add")
        || name.starts_with("integer_compare_")
        || name.starts_with("real_compare_")
        || name.starts_with("decimal_compare_")
        || matches!(
            name,
            "integer_subtract"
                | "integer_multiply"
                | "abs"
                | "char_length"
                | "lower"
                | "upper"
                | "substring"
                | "decimal_add"
                | "real_add"
        )
        || name.starts_with("any_value_")
}

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
            "r" => (5, 6),
            "q" => (6, 7),
            "dt" => (7, 8),
            "ts" => (8, 9),
            "dur" => (9, 10),
            "v" => (10, 11),
            "e" => (11, 12),
            _ => return None,
        };
        Some((index, self.schema[index].clone(), unique_id))
    }

    fn time_zone(&self) -> SessionTimeZone {
        SessionTimeZone::utc()
    }
}

fn schema() -> Vec<FieldType> {
    let integer = match std::env::var("EXPR_BATCH_PERF_INT_TYPE").as_deref() {
        Ok("tiny") => FieldTypeCode::Tiny,
        Ok("short") => FieldTypeCode::Short,
        Ok("int24") => FieldTypeCode::Int24,
        Ok("long") => FieldTypeCode::Long,
        Ok("longlong") | Err(_) => FieldTypeCode::LongLong,
        Ok(other) => panic!("unsupported benchmark integer type {other}"),
    };
    let mut decimal = FieldType::new(FieldTypeCode::NewDecimal);
    decimal.set_flen(10);
    decimal.set_decimal(2);
    let mut duration = FieldType::new(FieldTypeCode::Duration);
    duration.set_decimal(6);
    vec![
        FieldType::new(integer),
        FieldType::new(integer),
        FieldType::new(FieldTypeCode::VarString),
        decimal.clone(),
        FieldType::new(FieldTypeCode::Json),
        FieldType::new(FieldTypeCode::Double),
        FieldType::new(FieldTypeCode::Double),
        FieldType::new(FieldTypeCode::Datetime),
        FieldType::new(FieldTypeCode::Timestamp),
        duration,
        FieldType::new(FieldTypeCode::VectorFloat32),
        decimal,
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
    let datetime = parse_datetime("2026-10-12 12:34:56.123456", &chrono_tz::UTC, true, false)
        .expect("benchmark datetime")
        .time;
    let mut timestamp = datetime;
    timestamp.set_kind(TimeType::Timestamp);
    let vectors = [
        VectorFloat32::must_create(vec![1.0, 2.0, 3.0]),
        VectorFloat32::must_create(vec![-1.25, 8.5]),
        VectorFloat32::must_create(vec![0.0]),
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
            chunk.append_null(5);
            chunk.append_null(6);
            chunk.append_null(7);
            chunk.append_null(8);
            chunk.append_null(9);
            chunk.append_null(10);
            chunk.append_null(11);
        } else {
            chunk.append_bytes(2, STRINGS[row % STRINGS.len()]);
            chunk.append_datum(3, &Datum::Decimal(decimals[row % decimals.len()].clone()));
            chunk.append_json(4, &json[row % json.len()]);
            chunk.append_float64(5, row as f64 * 0.25 - 7.0);
            chunk.append_float64(6, row as f64 % 11.0 + 0.5);
            chunk.append_time(7, datetime);
            chunk.append_time(8, timestamp);
            chunk.append_duration(
                9,
                MySqlDuration::from_raw_parts((row as i64 % 17 - 8) * 1_234_567_000, 6),
            );
            chunk.append_vector_float32(10, &vectors[row % vectors.len()]);
            chunk.append_datum(
                11,
                &Datum::Decimal(decimals[(row + 1) % decimals.len()].clone()),
            );
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

fn run_once(
    suite: &EvaluatorSuite,
    input: &mut Chunk,
    output: &mut Chunk,
    schema: &[FieldType],
    mode: PerfMode,
) {
    output.reset();
    match mode {
        PerfMode::CurrentFallbackAblation => suite
            .run(&NoColumns, input, output)
            .expect("current fallback ablation succeeds"),
        PerfMode::CurrentTikvRowAblation => suite
            .run_with_tikv_row_ablation(&NoColumns, input, output, schema)
            .expect("current TiKV row-window ablation succeeds"),
        PerfMode::BatchTikv => suite
            .run_with_tikv_batch(&NoColumns, input, output, schema)
            .expect("optimized TiKV batch succeeds"),
    }
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
    let perf_mode = perf_mode();

    run_once(&suite, &mut input, &mut output, &schema, perf_mode);
    let route_after_first = suite.batch_route_snapshot();
    let (mode, route) = if perf_mode == PerfMode::CurrentFallbackAblation {
        assert_eq!(route_after_first, Default::default());
        ("current_fallback_ablation", "current_fallback_ablation")
    } else {
        let tikv_batch = route_after_first.prepared == 1;
        assert_eq!(tikv_batch, tikv_batch_workload(workload.name));
        assert_eq!(route_after_first.fallback, u64::from(!tikv_batch));
        assert_eq!(route_after_first.refusals, u64::from(!tikv_batch));
        assert_eq!(route_after_first.refusal.is_some(), !tikv_batch);
        (
            match perf_mode {
                PerfMode::CurrentTikvRowAblation => "current_tikv_row_ablation",
                PerfMode::BatchTikv => "batch_tikv",
                PerfMode::CurrentFallbackAblation => unreachable!(),
            },
            if tikv_batch {
                match perf_mode {
                    PerfMode::CurrentTikvRowAblation => "current_tikv_row_ablation",
                    PerfMode::BatchTikv => "tikv_batch",
                    PerfMode::CurrentFallbackAblation => unreachable!(),
                }
            } else {
                "production_fallback"
            },
        )
    };
    validate_output(workload, &input, &output, &output_field, expected);
    for _ in 0..WARMUP_BATCHES {
        run_once(&suite, &mut input, &mut output, &schema, perf_mode);
    }
    let calibration_batches = 4usize;
    let start = Instant::now();
    for _ in 0..calibration_batches {
        run_once(&suite, &mut input, &mut output, &schema, perf_mode);
    }
    let calibration_ns = start.elapsed().as_nanos().max(1);
    let iterations = ((target_sample_ns() * calibration_batches as u128) / calibration_ns)
        .clamp(1, 1_000_000) as usize;
    let route_before_samples = suite.batch_route_snapshot();
    let scratch_before_samples = suite.batch_scratch_snapshot().unwrap_or_default();

    for sample in 0..sample_count() {
        let start = Instant::now();
        for _ in 0..iterations {
            run_once(&suite, &mut input, &mut output, &schema, perf_mode);
        }
        let elapsed = start.elapsed().as_nanos();
        assert_eq!(output.num_rows(), selected_rows);
        assert_eq!(output_checksum(&output, &output_field), expected);
        let batches = iterations as f64;
        let rows = (iterations * selected_rows) as f64;
        let live_route = suite.batch_route_snapshot();
        println!(
            "EXPR_BATCH_PERF mode={mode} route={route} name={} scenario={scenario} \
             physical_rows={physical_rows} selected_rows={selected_rows} sample={sample} \
             iterations={iterations} elapsed_ns={elapsed} ns_per_batch={:.3} \
             ns_per_row={:.3} rows_per_second={:.3} checksum={expected} \
             live_prepared={} live_fallback={} live_refusals={} refusal={:?}",
            workload.name,
            elapsed as f64 / batches,
            elapsed as f64 / rows,
            rows * 1_000_000_000.0 / elapsed as f64,
            live_route.prepared,
            live_route.fallback,
            live_route.refusals,
            live_route.refusal,
        );
    }
    let route_after_samples = suite.batch_route_snapshot();
    let scratch_after_samples = suite.batch_scratch_snapshot().unwrap_or_default();
    let measured_calls = route_after_samples
        .prepared
        .saturating_add(route_after_samples.fallback)
        .saturating_sub(
            route_before_samples
                .prepared
                .saturating_add(route_before_samples.fallback),
        );
    if perf_mode == PerfMode::CurrentFallbackAblation {
        assert_eq!(measured_calls, 0);
    } else {
        assert!(measured_calls > 0);
    }
    let vector_allocations = scratch_after_samples
        .vector_allocations
        .saturating_sub(scratch_before_samples.vector_allocations);
    let windows = scratch_after_samples
        .windows
        .saturating_sub(scratch_before_samples.windows);
    println!(
        "EXPR_BATCH_SCRATCH route={route} name={} scenario={scenario} \
         selection_capacity={} measured_calls={measured_calls} \
         vector_allocations={vector_allocations} windows={windows} \
         vector_allocations_per_call={:.3} windows_per_call={:.3}",
        workload.name,
        scratch_after_samples.selection_capacity,
        vector_allocations as f64 / measured_calls.max(1) as f64,
        windows as f64 / measured_calls.max(1) as f64,
    );
}

#[test]
#[ignore = "manual stable release-profile batch performance probe"]
fn lane_cache_batch_release_performance_probe() {
    println!(
        "EXPR_BATCH_CONFIG samples={} warmup_batches={WARMUP_BATCHES} \
         target_sample_ns={} dense_sizes=1,16,256,1024",
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
            if tikv_batch_workload(workload.name) || workload.name == "strcmp" {
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
