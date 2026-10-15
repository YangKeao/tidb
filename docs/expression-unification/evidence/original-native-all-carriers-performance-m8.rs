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

const ROWS: usize = 1024;
const SAMPLES: usize = 7;

const WORKLOADS: [(&str, &str); 9] = [
    ("integer_add", "i + j"),
    ("real_add", "r + q"),
    ("decimal_add", "d + d"),
    ("lower", "lower(s)"),
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
    let mut duration = FieldType::new(FieldTypeCode::Duration);
    duration.set_decimal(6);
    vec![
        FieldType::new(FieldTypeCode::LongLong),
        FieldType::new(FieldTypeCode::LongLong),
        FieldType::new(FieldTypeCode::VarString),
        decimal,
        FieldType::new(FieldTypeCode::Json),
        FieldType::new(FieldTypeCode::Double),
        FieldType::new(FieldTypeCode::Double),
        FieldType::new(FieldTypeCode::Datetime),
        FieldType::new(FieldTypeCode::Timestamp),
        duration,
        FieldType::new(FieldTypeCode::VectorFloat32),
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
    let mut chunk = Chunk::new_with_capacity(schema, ROWS);
    for row in 0..ROWS {
        chunk.append_int64(0, row as i64 % 97 - 48);
        chunk.append_int64(1, row as i64 % 13 + 1);
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
    cpu_calibration("before");
    let schema = schema();
    let resolver = Resolver {
        schema: schema.clone(),
    };
    for &(name, sql) in &WORKLOADS {
        let expression = rewrite(sql, &resolver);
        let field = expression.static_type().unwrap().clone();
        let mut input = make_input(&schema);
        let expected =
            checksum((0..ROWS).map(|row| expression.eval(&NoColumns, input.get_row(row)).unwrap()));
        let suite = EvaluatorSuite::new(vec![expression], false);
        let mut output = Chunk::new_with_capacity(std::slice::from_ref(&field), ROWS);
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
            assert_eq!(output.num_rows(), ROWS);
            assert_eq!(
                checksum((0..ROWS).map(|row| output.get_row(row).get_datum(0, &field))),
                expected
            );
            println!(
                "ORIGINAL_NATIVE_PERF name={name} sample={sample} iterations={iterations} \
                 elapsed_ns={elapsed} ns_per_row={:.3} checksum={expected}",
                elapsed as f64 / (iterations * ROWS) as f64
            );
        }
    }
    cpu_calibration("after");
}
