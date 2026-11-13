// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Opt-in Decimal call/allocation comparison; no SQL or throughput claim.
use std::{hint::black_box, time::Instant};
use tidb_ast::CiString;
use tidb_chunk::chunk::Chunk;
use tidb_datatype::{Datum, Decimal, FieldType, FieldTypeCode};
use tidb_expr::{
    column::Column, constant::Constant, evaluator::EvaluatorSuite, expression::Expression,
    scalar_function::ScalarFunction, shared_kernel::SharedProjection, NoColumns,
};
use tidb_query_datatype::expr::EvalContext;
struct ScalarOnly;
impl tidb_expr::Columns for ScalarOnly {
    fn get(&self, _: &[String]) -> Option<Datum> {
        None
    }
    fn enable_vectorized_expression(&self) -> bool {
        false
    }
}

#[cfg(allocation_counting)]
mod allocation {
    use super::black_box;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
    // Count calls to the existing System allocator at link time; do not install
    // a second Rust global allocator (the SDK already owns that registration).
    pub(super) static COUNT: AtomicBool = AtomicBool::new(false);
    pub(super) static CALLS: AtomicU64 = AtomicU64::new(0);
    pub(super) static BYTES: AtomicU64 = AtomicU64::new(0);
    fn record(bytes: usize) {
        if COUNT.load(Relaxed) {
            CALLS.fetch_add(1, Relaxed);
            BYTES.fetch_add(bytes as u64, Relaxed);
        }
    }
    unsafe extern "C" {
        fn __real_malloc(size: usize) -> *mut std::ffi::c_void;
    }
    #[unsafe(no_mangle)]
    unsafe extern "C" fn __wrap_malloc(size: usize) -> *mut std::ffi::c_void {
        record(size);
        // Forward the allocator ABI unchanged; counters do not allocate.
        unsafe { __real_malloc(size) }
    }
    unsafe extern "C" {
        fn __real_calloc(count: usize, size: usize) -> *mut std::ffi::c_void;
    }
    #[unsafe(no_mangle)]
    unsafe extern "C" fn __wrap_calloc(count: usize, size: usize) -> *mut std::ffi::c_void {
        record(count.saturating_mul(size));
        // Forward the allocator ABI unchanged; counters do not allocate.
        unsafe { __real_calloc(count, size) }
    }
    unsafe extern "C" {
        fn __real_realloc(ptr: *mut std::ffi::c_void, size: usize) -> *mut std::ffi::c_void;
    }
    #[unsafe(no_mangle)]
    unsafe extern "C" fn __wrap_realloc(
        ptr: *mut std::ffi::c_void,
        size: usize,
    ) -> *mut std::ffi::c_void {
        record(size);
        // Forward the allocator ABI unchanged; counters do not allocate.
        unsafe { __real_realloc(ptr, size) }
    }
    unsafe extern "C" {
        fn __real_aligned_alloc(align: usize, size: usize) -> *mut std::ffi::c_void;
    }
    #[unsafe(no_mangle)]
    unsafe extern "C" fn __wrap_aligned_alloc(align: usize, size: usize) -> *mut std::ffi::c_void {
        record(size);
        // Forward the allocator ABI unchanged; counters do not allocate.
        unsafe { __real_aligned_alloc(align, size) }
    }
    unsafe extern "C" {
        fn __real_posix_memalign(
            ptr: *mut *mut std::ffi::c_void,
            align: usize,
            size: usize,
        ) -> std::ffi::c_int;
    }
    #[unsafe(no_mangle)]
    unsafe extern "C" fn __wrap_posix_memalign(
        ptr: *mut *mut std::ffi::c_void,
        align: usize,
        size: usize,
    ) -> std::ffi::c_int {
        record(size);
        // Forward the allocator ABI unchanged; counters do not allocate.
        unsafe { __real_posix_memalign(ptr, align, size) }
    }
    pub(super) fn check_counter() {
        use std::alloc::{alloc, alloc_zeroed, dealloc, realloc, Layout};
        let a = Layout::from_size_align(64, 8).unwrap();
        let b = Layout::from_size_align(128, 64).unwrap();
        CALLS.store(0, Relaxed);
        BYTES.store(0, Relaxed);
        COUNT.store(true, Relaxed);
        // Layouts match every allocation/reallocation/deallocation below.
        unsafe {
            let p = black_box(alloc(a));
            assert!(!p.is_null());
            let p = black_box(realloc(p, a, 96));
            assert!(!p.is_null());
            let q = black_box(alloc_zeroed(b));
            assert!(!q.is_null());
            dealloc(p, Layout::from_size_align(96, 8).unwrap());
            dealloc(q, b);
        }
        COUNT.store(false, Relaxed);
        assert_eq!(CALLS.load(Relaxed), 3);
        assert_eq!(BYTES.load(Relaxed), 288);
    }
}

fn literal(text: &str) -> Expression {
    Expression::Constant(Constant::new(
        Datum::Decimal(Decimal::from_literal(text)),
        FieldType::new(FieldTypeCode::NewDecimal),
    ))
}
fn binary(name: &str, left: Expression, right: Expression) -> Expression {
    Expression::ScalarFunction(ScalarFunction::new(
        CiString::new(name),
        FieldType::new(FieldTypeCode::NewDecimal),
        vec![left, right],
    ))
}
fn parts(values: &[Datum]) -> Vec<Option<(bool, String, u32, u32, Option<(i64, i64)>)>> {
    values
        .iter()
        .map(|v| {
            if v == &Datum::Null {
                return None;
            }
            let Datum::Decimal(v) = v else {
                panic!("Decimal expected")
            };
            Some((
                v.is_negative(),
                v.coefficient_digits().into(),
                v.storage_scale(),
                v.scale(),
                v.declared_shape(),
            ))
        })
        .collect()
}
fn measure<T>(
    case: &str,
    width: usize,
    phase: &str,
    route: &str,
    iterations: usize,
    mut run: impl FnMut() -> T,
) {
    if let Ok(only) = std::env::var("DECIMAL_BENCH_ONLY") {
        if only != format!("{case}/{width}/{phase}/{route}") {
            return;
        }
    }
    for _ in 0..32 {
        black_box(run());
    }
    let begin = Instant::now();
    for _ in 0..iterations {
        black_box(run());
    }
    let nanos = begin.elapsed().as_nanos();
    #[cfg(allocation_counting)]
    let (calls, bytes, count) = {
        use allocation::{BYTES, CALLS, COUNT};
        use std::sync::atomic::Ordering::Relaxed;
        CALLS.store(0, Relaxed);
        BYTES.store(0, Relaxed);
        COUNT.store(true, Relaxed);
        for _ in 0..100 {
            black_box(run());
        }
        COUNT.store(false, Relaxed);
        (CALLS.load(Relaxed), BYTES.load(Relaxed), 100)
    };
    #[cfg(not(allocation_counting))]
    let (calls, bytes, count) = (0, 0, 0);
    println!("{case},{width},{phase},{route},{iterations},{nanos},{calls},{bytes},{count}");
}
// One real-column full-entry scenario; keep the constant matrix unchanged.
fn column_bench(iterations: usize) {
    let ty = FieldType::new(FieldTypeCode::NewDecimal)
        .with_flen(65)
        .with_decimal(-1);
    let column = |index| {
        let mut c = Column::new(index + 1, ty.clone());
        c.index = index;
        Expression::Column(c)
    };
    let expression = binary("mul", column(0), column(1));
    let shared = SharedProjection::compile(std::slice::from_ref(&expression)).unwrap();
    let native = EvaluatorSuite::new(vec![expression.clone()], false);
    let width = 64;
    let rows = (0..width).collect::<Vec<_>>();
    let mut input = Chunk::new(&[ty.clone(), ty.clone()], width, width);
    for r in 0..width {
        input.append_datum(
            0,
            &if r % 11 == 0 {
                Datum::Null
            } else {
                Datum::Decimal(Decimal::from_signed_literal(&format!(
                    "{}{}.987654321",
                    if r % 2 == 0 { "-" } else { "" },
                    123456789 + r
                )))
            },
        );
        input.append_datum(1, &Datum::Decimal(Decimal::from_literal("1.000000000000")));
    }
    let mut ctx = EvalContext::default();
    let expected = rows
        .iter()
        .map(|r| expression.eval(&NoColumns, input.physical_row(*r)).unwrap())
        .collect::<Vec<_>>();
    let mut output = Chunk::new(std::slice::from_ref(&ty), width, width);
    native.run(&NoColumns, &mut input, &mut output).unwrap();
    let native_chunk = (0..width)
        .map(|r| output.physical_row(r).get_datum(0, &ty))
        .collect::<Vec<_>>();
    for effects in [&NoColumns as &dyn tidb_expr::Columns, &ScalarOnly] {
        let actual = shared
            .start_execution(effects)
            .evaluate(&input, &rows, &mut ctx)
            .unwrap();
        assert_eq!(parts(&actual[0]), parts(&expected));
        output.reset();
        for v in &actual[0] {
            output.append_datum(0, v);
        }
        let values = (0..width)
            .map(|r| output.physical_row(r).get_datum(0, &ty))
            .collect::<Vec<_>>();
        assert_eq!(parts(&values), parts(&native_chunk));
    }
    output.reset();
    for v in &expected {
        output.append_datum(0, v);
    }
    assert_eq!(
        parts(
            &(0..width)
                .map(|r| output.physical_row(r).get_datum(0, &ty))
                .collect::<Vec<_>>()
        ),
        parts(&native_chunk)
    );
    measure(
        "column_mul",
        width,
        "reuse",
        "native_vector",
        iterations,
        || {
            output.reset();
            native.run(&NoColumns, &mut input, &mut output).unwrap();
            black_box(&output);
        },
    );
    measure(
        "column_mul",
        width,
        "reuse",
        "native_scalar",
        iterations,
        || {
            output.reset();
            for r in &rows {
                output.append_datum(
                    0,
                    &expression.eval(&NoColumns, input.physical_row(*r)).unwrap(),
                );
            }
            black_box(&output);
        },
    );
    for (route, effects) in [
        ("shared", &NoColumns as &dyn tidb_expr::Columns),
        ("shared_scalar", &ScalarOnly),
    ] {
        let mut execution = shared.start_execution(effects);
        measure("column_mul", width, "reuse", route, iterations, || {
            output.reset();
            let values = execution.evaluate(&input, &rows, &mut ctx).unwrap();
            for v in &values[0] {
                output.append_datum(0, v);
            }
            black_box(&output);
        });
    }
    #[cfg(direct_chunk)]
    {
        let mut execution = shared.start_execution(&NoColumns);
        execution
            .evaluate_into_chunk(&input, &rows, &mut ctx, &mut output)
            .unwrap();
        let actual = (0..width)
            .map(|r| output.physical_row(r).get_datum(0, &ty))
            .collect::<Vec<_>>();
        assert_eq!(parts(&actual), parts(&native_chunk));
        measure(
            "column_mul",
            width,
            "reuse",
            "direct_chunk",
            iterations,
            || {
                execution
                    .evaluate_into_chunk(&input, &rows, &mut ctx, &mut output)
                    .unwrap();
                black_box(&output);
            },
        );
    }
    // Diagnostic slices only; their temporary ownership differs, so do not sum
    // or subtract them as an exact decomposition of the complete call.
    if std::env::var_os("DECIMAL_BENCH_COLUMN_PHASES").is_some() {
        let operands = (0..2)
            .map(|c| {
                rows.iter()
                    .map(|r| input.physical_row(*r).get_datum(c, &ty))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let arithmetic = || {
            operands[0]
                .iter()
                .zip(&operands[1])
                .map(|(a, b)| match (a, b) {
                    (Datum::Decimal(a), Datum::Decimal(b)) => {
                        let (value, warning) = a.mul_mysql(b);
                        assert!(warning.is_none());
                        Some(value)
                    }
                    _ => None,
                })
                .collect::<Vec<_>>()
        };
        let values = arithmetic()
            .into_iter()
            .map(|v| v.map_or(Datum::Null, Datum::Decimal))
            .collect::<Vec<_>>();
        assert_eq!(parts(&values), parts(&expected));
        measure(
            "column_mul",
            width,
            "input",
            "native_owner",
            iterations,
            || {
                (0..2)
                    .map(|c| {
                        rows.iter()
                            .map(|r| input.physical_row(*r).get_datum(c, &ty))
                            .collect::<Vec<_>>()
                    })
                    .collect::<Vec<_>>()
            },
        );
        measure(
            "column_mul",
            width,
            "arithmetic",
            "native_owner",
            iterations,
            arithmetic,
        );
        let mut execution = shared.start_execution(&NoColumns);
        measure(
            "column_mul",
            width,
            "evaluate",
            "shared",
            iterations,
            || execution.evaluate(&input, &rows, &mut ctx).unwrap(),
        );
        measure("column_mul", width, "write", "shared", iterations, || {
            output.reset();
            for value in &values {
                output.append_datum(0, value);
            }
            black_box(&output);
        });
    }
}

fn main() {
    if std::env::var_os("DECIMAL_BENCH_LAYOUT").is_some() {
        fn layout<T>(name: &str) {
            println!(
                "{name},{},{}",
                std::mem::size_of::<T>(),
                std::mem::align_of::<T>()
            );
        }
        use tidb_datatype::{CompactDecimal, DecimalArithmeticValue};
        use tidb_query_expr::{
            ExpressionScalar, ExpressionVector, RpnExpressionNode, RpnStackNode,
        };
        println!("type,size,alignment");
        layout::<Decimal>("Decimal");
        layout::<CompactDecimal>("CompactDecimal");
        layout::<DecimalArithmeticValue>("DecimalArithmeticValue");
        layout::<Option<DecimalArithmeticValue>>("Option<DecimalArithmeticValue>");
        layout::<ExpressionScalar>("ExpressionScalar");
        layout::<ExpressionVector>("ExpressionVector");
        layout::<RpnExpressionNode>("RpnExpressionNode");
        layout::<RpnStackNode<'static>>("RpnStackNode");
        return;
    }
    #[cfg(allocation_counting)]
    allocation::check_counter();
    let iterations = std::env::var("DECIMAL_BENCH_ITERATIONS")
        .unwrap_or_else(|_| "2000".into())
        .parse::<usize>()
        .unwrap();
    assert!(iterations > 0 && !cfg!(debug_assertions));
    if std::env::var_os("DECIMAL_BENCH_COLUMNS").is_some() {
        println!(
            "case,width,phase,route,iterations,nanos,alloc_calls,allocated_bytes,alloc_iterations"
        );
        column_bench(iterations);
        return;
    }
    // First operands reuse datatype/benches/decimal.rs rounding and codec inputs.
    let cases = [
        (
            "inline_mul",
            binary("mul", literal("123456789012"), literal("1000000000000")),
        ),
        (
            "spill_mul",
            binary("mul", literal("1234567890123"), literal("1000000000000")),
        ),
        (
            "small_mul",
            binary(
                "mul",
                literal("123456789.987654321"),
                literal("1.000000000000"),
            ),
        ),
        (
            "wide_mul",
            binary(
                "mul",
                literal(&format!("{}.5", "9".repeat(40))),
                literal("3"),
            ),
        ),
        (
            "wide_cancel",
            binary(
                "plus",
                literal(&format!("1{}", "0".repeat(79))),
                literal(&format!("-1{}", "0".repeat(79))),
            ),
        ),
        (
            "hidden_chain",
            binary(
                "div",
                binary(
                    "mul",
                    binary("div", literal("8"), literal("7")),
                    literal("1"),
                ),
                literal("100"),
            ),
        ),
    ];
    println!(
        "case,width,phase,route,iterations,nanos,alloc_calls,allocated_bytes,alloc_iterations"
    );
    for (name, expression) in cases {
        let operands: Vec<Decimal> = match name {
            "inline_mul" => ["123456789012", "1000000000000"]
                .map(Decimal::from_literal)
                .to_vec(),
            "spill_mul" => ["1234567890123", "1000000000000"]
                .map(Decimal::from_literal)
                .to_vec(),
            "small_mul" => vec![
                Decimal::from_literal("123456789.987654321"),
                Decimal::from_literal("1.000000000000"),
            ],
            "wide_mul" => vec![
                Decimal::from_literal(&format!("{}.5", "9".repeat(40))),
                Decimal::from_literal("3"),
            ],
            "wide_cancel" => vec![
                Decimal::from_literal(&format!("1{}", "0".repeat(79))),
                Decimal::from_literal(&format!("-1{}", "0".repeat(79))),
            ],
            _ => ["8", "7", "1", "100"].map(Decimal::from_literal).to_vec(),
        };
        let arithmetic = || {
            let p = black_box(&operands);
            match name {
                "inline_mul" | "spill_mul" | "small_mul" | "wide_mul" => p[0].mul_mysql(&p[1]).0,
                "wide_cancel" => p[0].add_mysql(&p[1]).0,
                _ => {
                    p[0].div_mysql_with_warning(&p[1], 4)
                        .unwrap()
                        .0
                        .mul_mysql(&p[2])
                        .0
                        .div_mysql_with_warning(&p[3], 4)
                        .unwrap()
                        .0
                }
            }
        };
        measure(name, 0, "prepare", "native", iterations, || {
            EvaluatorSuite::new(vec![expression.clone()], false)
        });
        measure(name, 0, "prepare", "shared", iterations, || {
            SharedProjection::compile(std::slice::from_ref(&expression)).unwrap()
        });
        let shared = SharedProjection::compile(std::slice::from_ref(&expression)).unwrap();
        let native = EvaluatorSuite::new(vec![expression.clone()], false);
        let mut vector_execution = shared.start_execution(&NoColumns);
        let mut scalar_execution = shared.start_execution(&ScalarOnly);
        for width in [1, 64] {
            let mut chunk = Chunk::new(&[], width, width);
            chunk.set_num_virtual_rows(width);
            let rows = (0..width).collect::<Vec<_>>();
            let mut context = EvalContext::default();
            let expected = (0..width)
                .map(|r| expression.eval(&NoColumns, chunk.physical_row(r)).unwrap())
                .collect::<Vec<_>>();
            assert_eq!(
                parts(&[Datum::Decimal(arithmetic())]),
                parts(&expected[..1])
            );
            let actual = vector_execution
                .evaluate(&chunk, &rows, &mut context)
                .unwrap();
            assert_eq!(parts(&actual[0]), parts(&expected));
            let scalar_actual = scalar_execution
                .evaluate(&chunk, &rows, &mut context)
                .unwrap();
            assert_eq!(parts(&scalar_actual[0]), parts(&expected));
            if name == "inline_mul" || name == "spill_mul" {
                let Datum::Decimal(v) = &actual[0][0] else {
                    unreachable!()
                };
                assert_eq!(
                    v.coefficient_digits().len(),
                    if name == "inline_mul" { 24 } else { 25 }
                );
            }
            if name == "hidden_chain" {
                let Datum::Decimal(value) = &actual[0][0] else {
                    unreachable!()
                };
                assert_eq!(value.to_string(), "0.01142857");
            }
            let ty = FieldType::new(FieldTypeCode::NewDecimal);
            let mut output = Chunk::new(std::slice::from_ref(&ty), width, width);
            native.run(&NoColumns, &mut chunk, &mut output).unwrap();
            let native_values = (0..width)
                .map(|r| output.physical_row(r).get_datum(0, &ty))
                .collect::<Vec<_>>();
            output.reset();
            for value in &actual[0] {
                output.append_datum(0, value);
            }
            let shared_values = (0..width)
                .map(|r| output.physical_row(r).get_datum(0, &ty))
                .collect::<Vec<_>>();
            assert_eq!(parts(&native_values), parts(&shared_values));
            output.reset();
            for row in 0..width {
                output.append_datum(
                    0,
                    &expression
                        .eval(&NoColumns, chunk.physical_row(row))
                        .unwrap(),
                );
            }
            let scalar_values = (0..width)
                .map(|r| output.physical_row(r).get_datum(0, &ty))
                .collect::<Vec<_>>();
            assert_eq!(parts(&scalar_values), parts(&native_values));
            measure(name, width, "reuse", "native_scalar", iterations, || {
                output.reset();
                for row in 0..width {
                    output.append_datum(
                        0,
                        &expression
                            .eval(&NoColumns, chunk.physical_row(row))
                            .unwrap(),
                    );
                }
                black_box(&output);
            });
            measure(name, width, "evaluate", "native_scalar", iterations, || {
                black_box(
                    (0..width)
                        .map(|row| {
                            expression
                                .eval(&NoColumns, chunk.physical_row(row))
                                .unwrap()
                        })
                        .collect::<Vec<_>>(),
                );
            });
            measure(name, width, "evaluate", "shared", iterations, || {
                black_box(
                    vector_execution
                        .evaluate(&chunk, &rows, &mut context)
                        .unwrap(),
                );
            });
            measure(name, width, "reuse", "shared_scalar", iterations, || {
                output.reset();
                let values = scalar_execution
                    .evaluate(&chunk, &rows, &mut context)
                    .unwrap();
                for value in &values[0] {
                    output.append_datum(0, value);
                }
                black_box(&output);
            });
            measure(name, width, "evaluate", "shared_scalar", iterations, || {
                black_box(
                    scalar_execution
                        .evaluate(&chunk, &rows, &mut context)
                        .unwrap(),
                );
            });
            measure(name, width, "write", "shared", iterations, || {
                output.reset();
                for value in &actual[0] {
                    output.append_datum(0, black_box(value));
                }
                black_box(&output);
            });
            measure(name, width, "arithmetic", "native", iterations, || {
                black_box(
                    (0..width)
                        .map(|_| Datum::Decimal(arithmetic()))
                        .collect::<Vec<_>>(),
                );
            });
            measure(name, width, "reuse", "native_vector", iterations, || {
                output.reset();
                native.run(&NoColumns, &mut chunk, &mut output).unwrap();
                black_box(&output);
            });
            measure(name, width, "reuse", "shared", iterations, || {
                output.reset();
                let values = vector_execution
                    .evaluate(&chunk, &rows, &mut context)
                    .unwrap();
                for value in &values[0] {
                    output.append_datum(0, value);
                }
                black_box(&output);
            });
        }
    }
}
