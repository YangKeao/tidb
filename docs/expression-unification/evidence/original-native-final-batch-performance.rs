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

//! Disposable benchmark probe for the frozen original-native revision.

use std::hint::black_box;
use std::time::Instant;

use tidb_ast::{QueryStmt, SelectField, Stmt};
use tidb_chunk::chunk::Chunk;
use tidb_datatype::{
    parse_datetime, BinaryJSON, Datum, Decimal, FieldType, FieldTypeCode, MySqlDuration,
    SessionTimeZone, TimeType, VectorFloat32,
};

use super::*;
use crate::evaluator::EvaluatorSuite;
use crate::expression::Expression;
use crate::rewriter::{rewrite_expr_resolved, ColumnResolver};

fn rows() -> usize {
    std::env::var("EXPR_BATCH_PERF_BATCH_SIZE").ok().and_then(|value| value.parse().ok()).filter(|value| *value > 0).unwrap_or(1024)
}
const SAMPLES: usize = 7;

const WORKLOADS: [(&str, &str); 64] = [
    ("pure_pi", "pi()"),
    ("round_int_signed", "round(i)"),
    ("round_int_unsigned", "round(i)"),
    ("any_value_int_unsigned", "any_value(i)"),
    ("integer_not", "not i"),
    ("real_asin", "asin(r)"),
    ("real_acos", "acos(r)"),
    ("isnull_int", "isnull(i)"),
    ("isnull_real", "isnull(r)"),
    ("isnull_decimal", "isnull(d)"),
    ("isnull_string", "isnull(s)"),
    ("isnull_datetime", "isnull(dt)"),
    ("isnull_timestamp", "isnull(ts)"),
    ("isnull_duration", "isnull(dur)"),
    ("isnull_vector", "isnull(v)"),
    ("integer_bit_bitand", "i & j"),
    ("integer_bit_bitor", "i | j"),
    ("integer_bit_bitxor", "i ^ j"),
    ("integer_bit_leftshift", "i << j"),
    ("integer_bit_rightshift", "i >> j"),
    ("real_compare_lt", "r < q"),
    ("real_compare_le", "r <= q"),
    ("real_compare_gt", "r > q"),
    ("real_compare_ge", "r >= q"),
    ("real_compare_eq", "r = q"),
    ("real_compare_ne", "r != q"),
    ("real_compare_nulleq", "r <=> q"),
    ("decimal_compare_lt", "d < e"),
    ("decimal_compare_le", "d <= e"),
    ("decimal_compare_gt", "d > e"),
    ("decimal_compare_ge", "d >= e"),
    ("decimal_compare_eq", "d = e"),
    ("decimal_compare_ne", "d != e"),
    ("decimal_compare_nulleq", "d <=> e"),
    ("integer_add", "i + j"),
    ("integer_compare_lt", "i < j"),
    ("integer_compare_le", "i <= j"),
    ("integer_compare_gt", "i > j"),
    ("integer_compare_ge", "i >= j"),
    ("integer_compare_eq", "i = j"),
    ("integer_compare_ne", "i != j"),
    ("integer_compare_nulleq", "i <=> j"),
    ("integer_add_literal", "i + 7"),
    ("integer_add_nested", "(i + j) + (i + 11)"),
    ("integer_subtract", "i - j"),
    ("integer_multiply", "i * j"),
    ("abs", "abs(i)"),
    ("lazy_if", "if(i > 0, i, j)"),
    ("coalesce_nullif", "coalesce(nullif(i, 0), j)"),
    ("strcmp", "strcmp(s, 'beta')"),
    ("char_length", "char_length(s)"),
    ("lower", "lower(s)"),
    ("upper", "upper(s)"),
    ("concat", "concat(s, '-x')"),
    ("substring", "substring(s, 2, 3)"),
    ("decimal_add", "d + d"),
    ("real_add", "r + q"),
    ("json_type", "json_type(js)"),
    ("regexp_like", "regexp_like(s, '^a')"),
    ("any_value_datetime", "any_value(dt)"),
    ("any_value_timestamp", "any_value(ts)"),
    ("any_value_duration", "any_value(dur)"),
    ("any_value_json", "any_value(js)"),
    ("any_value_vector", "any_value(v)"),
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

// Benchmark data only: identical native/current domain; defaults preserve every old workload.
fn real_input_value(row: usize) -> f64 {
    match std::env::var("EXPR_PERF_REAL_DOMAIN").as_deref() {
        Ok("inverse_trig") => [-1.0,-0.9999,-0.5,-0.0,0.0,0.5,0.9999,1.0,-1.5,1.5][row%10],
        Err(_) => row as f64 * 0.25 - 7.0,
        Ok(other) => panic!("unknown benchmark Real domain {other}"),
    }
}

// Benchmark schema choice only; old controls retain signed LongLong fields.
fn integer_inputs_unsigned() -> bool {
    match std::env::var("EXPR_BATCH_PERF_INT_UNSIGNED").as_deref() {
        Ok("1") => true,
        Ok("0") | Err(_) => false,
        Ok(other) => panic!("unknown benchmark integer signedness {other}"),
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
        FieldType::new(integer).with_unsigned(integer_inputs_unsigned()),
        FieldType::new(integer).with_unsigned(integer_inputs_unsigned()),
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
    let stmt = tidb_parser::parse(&format!("select {sql}")).expect("parse");
    let Stmt::Query(query) = stmt else {
        panic!("not query")
    };
    let QueryStmt::Select(select) = query.into_inner() else {
        panic!("not select")
    };
    let SelectField::Expr { expr, .. } = &select.fields[0] else {
        panic!("not expr")
    };
    rewrite_expr_resolved(expr, resolver).expect("rewrite")
}

fn make_input(schema: &[FieldType]) -> Chunk {
    const STRINGS: [&[u8]; 6] = [b"Alpha", b"beta", b"abcdef", b"TiDB", b"vectorized", b"z"];
    let decimals = [
        Decimal::from_literal("123.45"),
        Decimal::from_literal("-7.25"),
        Decimal::from_literal("0.00"),
        Decimal::from_literal("999.99"),
    ];
    let json = [
        BinaryJSON::parse(r#"{"a":1}"#).unwrap(),
        BinaryJSON::parse(r#"[1,2,3]"#).unwrap(),
        BinaryJSON::parse(r#""text""#).unwrap(),
        BinaryJSON::parse("7").unwrap(),
    ];
    let datetime = parse_datetime("2026-10-12 12:34:56.123456", &chrono_tz::UTC, true, false)
        .unwrap()
        .time;
    let mut timestamp = datetime;
    timestamp.set_kind(TimeType::Timestamp);
    let vectors = [
        VectorFloat32::must_create(vec![1.0, 2.0, 3.0]),
        VectorFloat32::must_create(vec![-1.25, 8.5]),
        VectorFloat32::must_create(vec![0.0]),
    ];
    let mut chunk = Chunk::new_with_capacity(schema, rows());
    for row in 0..rows() {
        chunk.append_int64(0, row as i64 % 97 - 48);
        chunk.append_int64(1, row as i64 % 13 + 1);
        chunk.append_bytes(2, STRINGS[row % STRINGS.len()]);
        chunk.append_datum(3, &Datum::Decimal(decimals[row % decimals.len()].clone()));
        chunk.append_json(4, &json[row % json.len()]);
        chunk.append_float64(5, real_input_value(row));
        chunk.append_float64(6, row as f64 % 11.0 + 0.5);
        chunk.append_time(7, datetime);
        chunk.append_time(8, timestamp);
        chunk.append_duration(
            9,
            MySqlDuration::from_raw_parts((row as i64 % 17 - 8) * 1_234_567_000, 6),
        );
        chunk.append_vector_float32(10, &vectors[row % vectors.len()]);
        chunk.append_datum(11, &Datum::Decimal(decimals[(row + 1) % decimals.len()].clone()));
    }
    chunk
}

fn checksum(values: impl IntoIterator<Item = Datum>) -> u64 {
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

fn run_once(suite: &EvaluatorSuite, input: &mut Chunk, output: &mut Chunk) {
    output.reset();
    suite.run(&NoColumns, input, output).unwrap();
    black_box(output.num_rows());
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
        "ORIGINAL_NATIVE_CALIBRATION stage={stage} ns_per_iter={:.3} checksum={value}",
        elapsed as f64 / 20_000_000.0
    );
}

#[test]
#[ignore = "manual frozen original-native performance probe"]
fn original_native_all_carriers_release_performance_probe() {
    println!("ORIGINAL_NATIVE_INPUT_SIGN unsigned={}",integer_inputs_unsigned());
    println!("ORIGINAL_NATIVE_INPUT_DOMAIN real={}", std::env::var("EXPR_PERF_REAL_DOMAIN").unwrap_or_else(|_| "original".into()));
    println!("ORIGINAL_NATIVE_BATCH_SIZE rows={}", rows());
    cpu_calibration("before");
    let schema = schema();
    let resolver = Resolver {
        schema: schema.clone(),
    };
    for &(name, sql) in &WORKLOADS {
        if std::env::var("EXPR_NATIVE_PERF_WORKLOAD").is_ok_and(|filter| filter != name) {
            continue;
        }
        let expression = rewrite(sql, &resolver);
        let field = expression.static_type().unwrap().clone();
        let mut input = make_input(&schema);
        let expected =
            checksum((0..rows()).map(|row| expression.eval(&NoColumns, input.get_row(row)).unwrap()));
        let suite = EvaluatorSuite::new(vec![expression], false);
        let mut output = Chunk::new_with_capacity(std::slice::from_ref(&field), rows());
        for _ in 0..32 {
            run_once(&suite, &mut input, &mut output);
        }
        let start = Instant::now();
        for _ in 0..4 {
            run_once(&suite, &mut input, &mut output);
        }
        let calibration = start.elapsed().as_nanos().max(1);
        let iterations = ((75_000_000_u128 * 4) / calibration).clamp(1, 1_000_000) as usize;
        for sample in 0..SAMPLES {
            let start = Instant::now();
            for _ in 0..iterations {
                run_once(&suite, &mut input, &mut output);
            }
            let elapsed = start.elapsed().as_nanos();
            assert_eq!(output.num_rows(), rows());
            assert_eq!(
                checksum((0..rows()).map(|row| output.get_row(row).get_datum(0, &field))),
                expected
            );
            println!(
                "ORIGINAL_NATIVE_PERF name={name} sample={sample} iterations={iterations} \
                 elapsed_ns={elapsed} ns_per_row={:.3} checksum={expected}",
                elapsed as f64 / (iterations * rows()) as f64
            );
        }
    }
    cpu_calibration("after");
}
