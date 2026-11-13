// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Opt-in Decimal call/allocation comparison; no SQL or throughput claim.
use std::{
    hint::black_box,
    sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed},
    time::Instant,
};
use tidb_ast::CiString;
use tidb_chunk::chunk::Chunk;
use tidb_datatype::{Datum, Decimal, FieldType, FieldTypeCode};
use tidb_expr::{
    constant::Constant, evaluator::EvaluatorSuite, expression::Expression,
    scalar_function::ScalarFunction, shared_kernel::SharedProjection, NoColumns,
};
use tidb_query_datatype::expr::EvalContext;

// Count calls to the existing System allocator at link time; do not install
// a second Rust global allocator (the SDK already owns that registration).
static COUNT: AtomicBool = AtomicBool::new(false);
static CALLS: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);
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
fn check_counter() {
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
fn parts(values: &[Datum]) -> Vec<(bool, String, u32, u32, Option<(i64, i64)>)> {
    values
        .iter()
        .map(|v| {
            let Datum::Decimal(v) = v else {
                panic!("Decimal expected")
            };
            (
                v.is_negative(),
                v.coefficient_digits().into(),
                v.storage_scale(),
                v.scale(),
                v.declared_shape(),
            )
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
    for _ in 0..32 {
        black_box(run());
    }
    let begin = Instant::now();
    for _ in 0..iterations {
        black_box(run());
    }
    let nanos = begin.elapsed().as_nanos();
    CALLS.store(0, Relaxed);
    BYTES.store(0, Relaxed);
    COUNT.store(true, Relaxed);
    for _ in 0..100 {
        black_box(run());
    }
    COUNT.store(false, Relaxed);
    println!(
        "{case},{width},{phase},{route},{iterations},{nanos},{},{},100",
        CALLS.load(Relaxed),
        BYTES.load(Relaxed)
    );
}
fn main() {
    check_counter();
    let iterations = std::env::var("DECIMAL_BENCH_ITERATIONS")
        .unwrap_or_else(|_| "2000".into())
        .parse::<usize>()
        .unwrap();
    assert!(iterations > 0 && !cfg!(debug_assertions));
    // First operands reuse datatype/benches/decimal.rs rounding and codec inputs.
    let cases = [
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
        measure(name, 0, "prepare", "native", iterations, || {
            EvaluatorSuite::new(vec![expression.clone()], false)
        });
        measure(name, 0, "prepare", "shared", iterations, || {
            SharedProjection::compile(std::slice::from_ref(&expression)).unwrap()
        });
        let shared = SharedProjection::compile(std::slice::from_ref(&expression)).unwrap();
        let native = EvaluatorSuite::new(vec![expression.clone()], false);
        for width in [1, 64] {
            let mut chunk = Chunk::new(&[], width, width);
            chunk.set_num_virtual_rows(width);
            let rows = (0..width).collect::<Vec<_>>();
            let mut context = EvalContext::default();
            let expected = (0..width)
                .map(|r| expression.eval(&NoColumns, chunk.physical_row(r)).unwrap())
                .collect::<Vec<_>>();
            let actual = shared
                .evaluate(&chunk, &rows, &mut context, &NoColumns)
                .unwrap();
            assert_eq!(parts(&actual[0]), parts(&expected));
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
            measure(name, width, "reuse", "native", iterations, || {
                output.reset();
                native.run(&NoColumns, &mut chunk, &mut output).unwrap();
                black_box(&output);
            });
            measure(name, width, "reuse", "shared", iterations, || {
                output.reset();
                let values = shared
                    .evaluate(&chunk, &rows, &mut context, &NoColumns)
                    .unwrap();
                for value in &values[0] {
                    output.append_datum(0, value);
                }
                black_box(&output);
            });
        }
    }
}
