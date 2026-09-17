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

//! Same-binary native / copying / borrowed projection benchmark.
//! Includes every per-call validation, guard, copy, scalar load and output write.
//! Excludes fixture creation and compilation. All six execution orders rotate;
//! counters and full output equality prevent misleading silent fallback.
//! TIKV_BENCH_MS sets sample time; TIKV_BENCH_PASSES must be a multiple of six.
//! TIKV_BENCH_FILTER optionally selects workload names by substring.

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
use tidb_expr::tikv::{Backend, Context, TikvExpression};
use tidb_expr::Columns;

struct BenchContext {
    backend: Option<Backend>,
    evaluated: Cell<usize>,
    borrowed: Cell<usize>,
}
fn settings() -> Context {
    Context {
        flags: 482,
        max_warning_count: u16::MAX as usize,
        ..Context::default()
    }
}
impl Columns for BenchContext {
    fn get(&self, _: &[String]) -> Option<Datum> {
        None
    }
    fn tikv_expression_context(&self) -> Option<Context> {
        self.backend.map(|_| settings())
    }
    fn tikv_expression_backend(&self) -> Backend {
        self.backend.unwrap_or_default()
    }
    fn record_tikv_expression_rows(&self, rows: usize) {
        self.evaluated.set(self.evaluated.get() + rows);
    }
    fn record_tikv_borrowed_expression_rows(&self, rows: usize) {
        self.borrowed.set(self.borrowed.get() + rows);
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
fn fixture(
    name: &str,
    rows: usize,
    nullable: bool,
    selected: bool,
) -> (Expression, Chunk, FieldType) {
    let int = FieldType::new(FieldTypeCode::LongLong);
    let double = FieldType::new(FieldTypeCode::Double);
    let bytes = FieldType::new(FieldTypeCode::VarString);
    let is_bytes = name.starts_with("bytes_length");
    let types = if is_bytes {
        vec![bytes.clone()]
    } else if name == "real_arithmetic" {
        vec![double.clone(), double.clone()]
    } else {
        vec![int.clone(), int.clone()]
    };
    let mut input = Chunk::new_with_capacity(&types, rows);
    for row in 0..rows {
        for index in 0..types.len() {
            if nullable && row % 11 == 0 {
                input.append_null(index);
            } else if is_bytes {
                let padding = match name {
                    "bytes_length_4k" => 4096,
                    "bytes_length_64k" => 65536,
                    _ => 16 + row % 96,
                };
                let value = format!("row{row}\0中文:{}", "x".repeat(padding));
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
        if input.num_rows() == 0 {
            input.set_sel(Some(vec![0]));
        }
    }
    let (expression, result) = if is_bytes {
        (call("length", &int, vec![column(0, &bytes)]), int)
    } else {
        match name {
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
        }
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
    let before = (context.evaluated.get(), context.borrowed.get());
    let logical_rows = input.num_rows();
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
    let elapsed = started.elapsed().as_nanos() as f64 / iterations as f64;
    // Outside timing: prove every timed evaluation kept the requested backend.
    let expected = usize::try_from(iterations).unwrap() * logical_rows;
    assert_eq!(
        context.evaluated.get() - before.0,
        if context.backend.is_some() {
            expected
        } else {
            0
        }
    );
    assert_eq!(
        context.borrowed.get() - before.1,
        if context.backend == Some(Backend::Borrowed) {
            expected
        } else {
            0
        }
    );
    elapsed
}
fn median(values: &mut [f64]) -> f64 {
    values.sort_by(f64::total_cmp);
    let mid = values.len() / 2;
    if values.len().is_multiple_of(2) {
        (values[mid - 1] + values[mid]) * 0.5
    } else {
        values[mid]
    }
}
fn main() {
    let ms: u64 = std::env::var("TIKV_BENCH_MS")
        .ok()
        .map(|v| v.parse().expect("integer sample time"))
        .unwrap_or(60);
    let passes: usize = std::env::var("TIKV_BENCH_PASSES")
        .ok()
        .map(|v| v.parse().expect("integer passes"))
        .unwrap_or(6);
    assert!(
        ms > 0 && passes >= 6 && passes.is_multiple_of(6),
        "positive duration; passes multiple of six"
    );
    let filter = std::env::var("TIKV_BENCH_FILTER").unwrap_or_default();
    let orders = [
        [0, 1, 2],
        [0, 2, 1],
        [1, 0, 2],
        [1, 2, 0],
        [2, 0, 1],
        [2, 1, 0],
    ];
    println!("# profile=bench setup=excluded all_per_call_costs=included passes={passes} sample_ms={ms} order=all_six_permutations");
    println!("workload,physical_rows,logical_rows,nullable,selected,physical_payload_bytes,native_ns_batch,copying_ns_batch,borrowed_ns_batch,borrowed_over_copying,borrowed_over_native,native_min_ns,native_max_ns,copying_min_ns,copying_max_ns,borrowed_min_ns,borrowed_max_ns");
    for name in [
        "int_add",
        "int_chain",
        "real_arithmetic",
        "bytes_length",
        "bytes_length_4k",
        "bytes_length_64k",
    ] {
        if !name.contains(&filter) {
            continue;
        }
        let (expression, _, _) = fixture(name, 1, false, false);
        let started = Instant::now();
        for _ in 0..100 {
            black_box(
                TikvExpression::compile(&expression, settings())
                    .unwrap()
                    .expect("must be admitted"),
            );
        }
        eprintln!(
            "{name} shared_tikv_compile_mean_ns={}",
            started.elapsed().as_nanos() / 100
        );
        // Cap even large-payload physical fixtures near 8 MiB (plus prefixes).
        let sizes: &[usize] = match name {
            "bytes_length_4k" => &[128, 1024],
            "bytes_length_64k" => &[128],
            _ => &[1, 128, 1024, 4096],
        };
        for &rows in sizes {
            let mut cases = vec![
                (false, false, false),
                (true, false, false),
                (false, true, false),
                (true, true, false),
            ];
            if rows == 4096 {
                // Expose full-offset validation cost when only one row is used.
                cases.push((false, true, true));
            }
            for (nullable, selected, sparse) in cases {
                let (expression, mut input, ty) = fixture(name, rows, nullable, selected);
                if sparse {
                    input.set_sel(Some(vec![rows - 1]));
                }
                let payload: usize = (0..input.num_cols())
                    .map(|i| input.column(i).read_view().data().len())
                    .sum();
                let suites: Vec<_> = (0..3)
                    .map(|_| EvaluatorSuite::new(vec![expression.clone()], false))
                    .collect();
                let contexts: Vec<_> = [None, Some(Backend::Copying), Some(Backend::Borrowed)]
                    .into_iter()
                    .map(|backend| BenchContext {
                        backend,
                        evaluated: Cell::new(0),
                        borrowed: Cell::new(0),
                    })
                    .collect();
                let mut outputs: Vec<_> = (0..3)
                    .map(|_| Chunk::new_with_capacity(std::slice::from_ref(&ty), rows))
                    .collect();
                for mode in 0..3 {
                    suites[mode]
                        .run(&contexts[mode], &mut input, &mut outputs[mode])
                        .unwrap();
                    assert_eq!(outputs[mode].num_rows(), input.num_rows());
                    assert_eq!(
                        contexts[mode].evaluated.get(),
                        if mode == 0 { 0 } else { input.num_rows() },
                        "{name}: native fallback"
                    );
                    assert_eq!(
                        contexts[mode].borrowed.get(),
                        if mode == 2 { input.num_rows() } else { 0 },
                        "{name}: copying fallback"
                    );
                }
                for row in 0..input.num_rows() {
                    let expected = outputs[0].get_row(row).get_datum(0, &ty);
                    for output in &outputs[1..] {
                        assert_eq!(
                            output.get_row(row).get_datum(0, &ty),
                            expected,
                            "{name}, row {row}"
                        );
                    }
                }
                let mut samples: [Vec<f64>; 3] = std::array::from_fn(|_| Vec::new());
                for pass in 0..passes {
                    for mode in orders[pass % orders.len()] {
                        samples[mode].push(sample(
                            &suites[mode],
                            &contexts[mode],
                            &mut input,
                            &mut outputs[mode],
                            Duration::from_millis(ms),
                        ));
                    }
                }
                let medians = samples.each_mut().map(|v| median(v));
                println!("{name},{rows},{},{nullable},{selected},{payload},{:.1},{:.1},{:.1},{:.4},{:.4},{:.1},{:.1},{:.1},{:.1},{:.1},{:.1}", input.num_rows(), medians[0], medians[1], medians[2], medians[2]/medians[1], medians[2]/medians[0], samples[0][0], samples[0][passes-1], samples[1][0], samples[1][passes-1], samples[2][0], samples[2][passes-1]);
            }
        }
    }
}
