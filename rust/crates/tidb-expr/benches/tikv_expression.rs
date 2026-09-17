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

//! Paired, conversion-inclusive native/TiKV projection microbenchmark.
//! Run: cargo bench -p tidb-expr --features tikv-expr --bench tikv_expression
//! Optional TIKV_BENCH_MS and TIKV_BENCH_PASSES change sample duration/count.
//! Data generation and expression compilation are outside steady-state timing.
//! Timed work includes output reset, input gather/copy, TiKV evaluation, result
//! conversion, and appending the result to a TiDB Chunk. Both engines run in the
//! same binary; samples alternate order. This is not a SQL/cluster benchmark.

use std::cell::Cell;
use std::hint::black_box;
use std::time::{Duration, Instant};

use tidb_ast::CiString;
use tidb_chunk::chunk::Chunk;
use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_expr::column::Column;
use tidb_expr::constant::Constant;
use tidb_expr::evaluator::EvaluatorSuite;
use tidb_expr::expression::{Expression, ScalarFunction};
use tidb_expr::tikv::{Context, TikvExpression};
use tidb_expr::Columns;

struct BenchContext {
    enabled: bool,
    evaluated: Cell<usize>,
}

impl Columns for BenchContext {
    fn get(&self, _: &[String]) -> Option<Datum> {
        None
    }
    fn tikv_expression_context(&self) -> Option<Context> {
        self.enabled.then(|| Context {
            flags: 482,
            max_warning_count: u16::MAX as usize,
            ..Context::default()
        })
    }
    fn record_tikv_expression_rows(&self, rows: usize) {
        self.evaluated.set(self.evaluated.get() + rows);
    }
}

fn column(index: i64, ty: &FieldType) -> Expression {
    let mut column = Column::new(index + 1, ty.clone());
    column.index = index;
    Expression::Column(column)
}

fn call(name: &str, ty: &FieldType, args: Vec<Expression>) -> Expression {
    Expression::ScalarFunction(ScalarFunction::new(CiString::new(name), ty.clone(), args))
}

fn literal(value: i64, ty: &FieldType) -> Expression {
    Expression::Constant(Constant::new(Datum::Int(value), ty.clone()))
}

fn fixture(name: &str, rows: usize, selected: bool) -> (Expression, Chunk, FieldType) {
    let int = FieldType::new(FieldTypeCode::LongLong);
    let double = FieldType::new(FieldTypeCode::Double);
    let bytes = FieldType::new(FieldTypeCode::VarString);
    let types = match name {
        "bytes_length" => vec![bytes.clone()],
        "real_arithmetic" => vec![double.clone(), double.clone()],
        _ => vec![int.clone(), int.clone()],
    };
    let mut input = Chunk::new_with_capacity(&types, rows);
    for row in 0..rows {
        for index in 0..types.len() {
            if selected && row % 11 == 0 {
                input.append_null(index);
            } else if name == "bytes_length" {
                // Includes an embedded NUL and multibyte UTF-8: byte length,
                // not character length and not C-string length.
                let value = format!("row{row}\0中文:{}", "x".repeat(16 + row % 96));
                input.append_bytes(index, value.as_bytes());
            } else if name == "real_arithmetic" {
                input.append_float64(index, row as f64 * 0.25 + index as f64);
            } else {
                input.append_int64(index, row as i64 + index as i64 * 3);
            }
        }
    }
    if selected {
        input.set_sel(Some((0..rows).rev().filter(|row| row % 3 != 0).collect()));
        // A single-row fixture must still have work to measure.
        if input.num_rows() == 0 {
            input.set_sel(Some(vec![0]));
        }
    }
    let (expression, result) = match name {
        "bytes_length" => (call("length", &int, vec![column(0, &bytes)]), int),
        "real_arithmetic" => (
            call(
                "mul",
                &double,
                vec![
                    call(
                        "plus",
                        &double,
                        vec![column(0, &double), column(1, &double)],
                    ),
                    column(1, &double),
                ],
            ),
            double,
        ),
        "int_chain" => (
            call(
                "plus",
                &int,
                vec![
                    call(
                        "mul",
                        &int,
                        vec![
                            call("plus", &int, vec![column(0, &int), column(1, &int)]),
                            literal(3, &int),
                        ],
                    ),
                    literal(7, &int),
                ],
            ),
            int,
        ),
        _ => (
            call("plus", &int, vec![column(0, &int), literal(17, &int)]),
            int,
        ),
    };
    (expression, input, result)
}

fn sample(
    suite: &EvaluatorSuite,
    context: &BenchContext,
    input: &mut Chunk,
    output: &mut Chunk,
    duration: Duration,
) -> f64 {
    let started = Instant::now();
    let mut iterations = 0_u64;
    loop {
        output.reset();
        suite
            .run(black_box(context), black_box(input), black_box(output))
            .unwrap();
        black_box(&*output);
        iterations += 1;
        if iterations.is_multiple_of(16) && started.elapsed() >= duration {
            break;
        }
    }
    started.elapsed().as_nanos() as f64 / iterations as f64
}

fn median(values: &mut [f64]) -> f64 {
    values.sort_by(f64::total_cmp);
    values[values.len() / 2]
}

fn main() {
    let milliseconds: u64 = std::env::var("TIKV_BENCH_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(60);
    let passes: usize = std::env::var("TIKV_BENCH_PASSES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5);
    assert!(
        milliseconds > 0 && passes >= 3,
        "use positive duration and >=3 samples"
    );
    println!(
        "# profile=bench copies=included setup=excluded passes={passes} sample_ms={milliseconds}"
    );
    println!("workload,physical_rows,logical_rows,nullable_selected,native_ns_batch,tikv_ns_batch,tikv_over_native,native_min_ns,native_max_ns,tikv_min_ns,tikv_max_ns");
    for name in ["int_add", "int_chain", "real_arithmetic", "bytes_length"] {
        let (expression, _, _) = fixture(name, 1, false);
        let started = Instant::now();
        for _ in 0..100 {
            black_box(
                TikvExpression::compile(
                    &expression,
                    Context {
                        flags: 482,
                        max_warning_count: u16::MAX as usize,
                        ..Context::default()
                    },
                )
                .unwrap()
                .expect("benchmark must be admitted"),
            );
        }
        eprintln!(
            "{name} tikv_compile_mean_ns={}",
            started.elapsed().as_nanos() / 100
        );
        for rows in [1, 128, 1024, 4096] {
            for selected in [false, true] {
                let (expression, mut input, result_type) = fixture(name, rows, selected);
                let native = EvaluatorSuite::new(vec![expression.clone()], false);
                let tikv = EvaluatorSuite::new(vec![expression], false);
                let native_context = BenchContext {
                    enabled: false,
                    evaluated: Cell::new(0),
                };
                let tikv_context = BenchContext {
                    enabled: true,
                    evaluated: Cell::new(0),
                };
                let mut native_output =
                    Chunk::new_with_capacity(std::slice::from_ref(&result_type), rows);
                let mut tikv_output =
                    Chunk::new_with_capacity(std::slice::from_ref(&result_type), rows);
                native
                    .run(&native_context, &mut input, &mut native_output)
                    .unwrap();
                tikv.run(&tikv_context, &mut input, &mut tikv_output)
                    .unwrap();
                assert_eq!(native_output.num_rows(), input.num_rows());
                assert_eq!(tikv_output.num_rows(), input.num_rows());
                for row in 0..input.num_rows() {
                    assert_eq!(
                        native_output.get_row(row).get_datum(0, &result_type),
                        tikv_output.get_row(row).get_datum(0, &result_type),
                        "{name}, row {row}"
                    );
                }
                assert_eq!(
                    tikv_context.evaluated.get(),
                    input.num_rows(),
                    "benchmark silently fell back"
                );
                let mut native_samples = Vec::new();
                let mut tikv_samples = Vec::new();
                for pass in 0..passes {
                    for enabled in if pass % 2 == 0 {
                        [false, true]
                    } else {
                        [true, false]
                    } {
                        let elapsed = if enabled {
                            sample(
                                &tikv,
                                &tikv_context,
                                &mut input,
                                &mut tikv_output,
                                Duration::from_millis(milliseconds),
                            )
                        } else {
                            sample(
                                &native,
                                &native_context,
                                &mut input,
                                &mut native_output,
                                Duration::from_millis(milliseconds),
                            )
                        };
                        if enabled {
                            tikv_samples.push(elapsed);
                        } else {
                            native_samples.push(elapsed);
                        }
                    }
                }
                let native_ns = median(&mut native_samples);
                let tikv_ns = median(&mut tikv_samples);
                println!("{name},{rows},{},{selected},{native_ns:.1},{tikv_ns:.1},{:.3},{:.1},{:.1},{:.1},{:.1}", input.num_rows(), tikv_ns/native_ns, native_samples[0], native_samples[passes-1], tikv_samples[0], tikv_samples[passes-1]);
            }
        }
    }
}
