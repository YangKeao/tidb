//! Private experiment executable: NOT a TiKV product test or allocator.
//!
//! Link with GNU --wrap=malloc/calloc/realloc/posix_memalign against the same
//! existing System-backed TiKV rlibs. Do not declare another global allocator.
//! All Decimal inputs are ordinary bounded 1 and 2. No wide imports or MAX scale.
//! Exit 0: stable allocation parity; 1: stable extra native-caller allocation
//! (RED); 2: invalid controls, semantics, or unstable/inconclusive measurement.
//! Source authored only; compilation/execution belongs to the parent.

use std::{
    alloc::{alloc, alloc_zeroed, dealloc, realloc, Layout},
    ffi::{c_int, c_void},
    hint::black_box,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};

use tidb_query_datatype::{
    codec::{
        self,
        mysql::{
            time::interval::{ConvertToIntervalStr, IntervalUnit},
            Decimal, RoundMode,
        },
    },
    expr::EvalContext,
};
use tidb_query_expr::impl_arithmetic::{ArithmeticOpWithCtx, DecimalDivide, DecimalMod};

// Private probe counters, not production instrumentation. These count calls
// (including unsuccessful allocation attempts), not live or peak bytes.
static ENABLED: AtomicBool = AtomicBool::new(false);
static MALLOC: AtomicUsize = AtomicUsize::new(0);
static CALLOC: AtomicUsize = AtomicUsize::new(0);
static REALLOC: AtomicUsize = AtomicUsize::new(0);
static ALIGNED: AtomicUsize = AtomicUsize::new(0);

unsafe extern "C" {
    fn __real_malloc(size: usize) -> *mut c_void;
    fn __real_calloc(count: usize, size: usize) -> *mut c_void;
    fn __real_realloc(ptr: *mut c_void, size: usize) -> *mut c_void;
    fn __real_posix_memalign(out: *mut *mut c_void, align: usize, size: usize) -> c_int;
}

#[no_mangle]
pub unsafe extern "C" fn __wrap_malloc(size: usize) -> *mut c_void {
    if ENABLED.load(Ordering::SeqCst) {
        MALLOC.fetch_add(1, Ordering::SeqCst);
    }
    unsafe { __real_malloc(size) }
}

#[no_mangle]
pub unsafe extern "C" fn __wrap_calloc(count: usize, size: usize) -> *mut c_void {
    if ENABLED.load(Ordering::SeqCst) {
        CALLOC.fetch_add(1, Ordering::SeqCst);
    }
    unsafe { __real_calloc(count, size) }
}

#[no_mangle]
pub unsafe extern "C" fn __wrap_realloc(ptr: *mut c_void, size: usize) -> *mut c_void {
    if ENABLED.load(Ordering::SeqCst) {
        REALLOC.fetch_add(1, Ordering::SeqCst);
    }
    unsafe { __real_realloc(ptr, size) }
}

#[no_mangle]
pub unsafe extern "C" fn __wrap_posix_memalign(
    out: *mut *mut c_void,
    align: usize,
    size: usize,
) -> c_int {
    if ENABLED.load(Ordering::SeqCst) {
        ALIGNED.fetch_add(1, Ordering::SeqCst);
    }
    unsafe { __real_posix_memalign(out, align, size) }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Counts([usize; 4]);

impl Counts {
    fn total(self) -> usize {
        self.0.iter().sum()
    }
}

struct DisableOnDrop;
impl Drop for DisableOnDrop {
    fn drop(&mut self) {
        ENABLED.store(false, Ordering::SeqCst);
    }
}

struct Measurement<T> {
    value: T,
    counts: Counts,
}

// No assertions, result formatting, context construction, or result destruction
// in this function's tracked interval. Guard also closes it during unwinding.
#[inline(never)]
fn measure<T>(f: impl FnOnce() -> T) -> Measurement<T> {
    MALLOC.store(0, Ordering::SeqCst);
    CALLOC.store(0, Ordering::SeqCst);
    REALLOC.store(0, Ordering::SeqCst);
    ALIGNED.store(0, Ordering::SeqCst);
    let guard = DisableOnDrop;
    ENABLED.store(true, Ordering::SeqCst);
    let value = black_box(f());
    drop(guard);
    let counts = Counts([
        MALLOC.load(Ordering::SeqCst),
        CALLOC.load(Ordering::SeqCst),
        REALLOC.load(Ordering::SeqCst),
        ALIGNED.load(Ordering::SeqCst),
    ]);
    Measurement { value, counts }
}

fn measure_pair<A, B>(
    baseline_first: bool,
    actual: impl FnOnce() -> A,
    baseline: impl FnOnce() -> B,
) -> (Measurement<A>, Measurement<B>) {
    if baseline_first {
        let b = measure(baseline);
        let a = measure(actual);
        (a, b)
    } else {
        let a = measure(actual);
        let b = measure(baseline);
        (a, b)
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Pair {
    actual: Counts,
    baseline: Counts,
}

fn quiet(ctx: &EvalContext) -> bool {
    ctx.warnings.warning_cnt == 0 && ctx.warnings.warnings.is_empty()
}

fn same_decimal(a: &Decimal, b: &Decimal) -> bool {
    let a_words = a.words();
    let b_words = b.words();
    a == b
        && a_words.int_digits == b_words.int_digits
        && a_words.storage_frac == b_words.storage_frac
        && a_words.result_frac == b_words.result_frac
        && a_words.negative == b_words.negative
        && a_words.words == b_words.words
}

// This is exactly the existing simple-unit native pipeline, not a second
// arithmetic or formatting algorithm. The final output String allocation is
// intentionally included in both this baseline and the actual DAY operation.
fn day_baseline(value: &Decimal, ctx: &mut EvalContext) -> codec::Result<String> {
    let rounded = value.clone().round(0, RoundMode::HalfEven).into_result(ctx)?;
    let integer = rounded.as_i64().into_result(ctx)?;
    Ok(integer.to_string())
}

fn controls(lhs: &Decimal, rhs: &Decimal) -> Result<(), &'static str> {
    let empty = measure(|| black_box(()));
    println!("control empty {:?}", empty.counts);
    if empty.counts != Counts::default() {
        return Err("empty measurement was nonzero");
    }

    // Fixed small layouts established before tracking. Null checks and
    // deallocation occur after each gate. These use the inherited allocator.
    let small = Layout::from_size_align(64, 8).map_err(|_| "small layout")?;
    let larger = Layout::from_size_align(128, 8).map_err(|_| "larger layout")?;
    let aligned = Layout::from_size_align(128, 64).map_err(|_| "aligned layout")?;

    let malloc = measure(|| unsafe { black_box(alloc(black_box(small))) });
    println!("control malloc {:?}", malloc.counts);
    if malloc.value.is_null() {
        return Err("malloc positive control returned null");
    }
    if malloc.counts.0[0] == 0 {
        unsafe { dealloc(malloc.value, small) };
        return Err("malloc wrapper did not observe Rust allocation");
    }

    let resized = measure(|| unsafe {
        black_box(realloc(black_box(malloc.value), small, black_box(larger.size())))
    });
    println!("control realloc {:?}", resized.counts);
    if resized.value.is_null() {
        unsafe { dealloc(malloc.value, small) };
        return Err("realloc positive control returned null");
    }
    unsafe { dealloc(resized.value, larger) };
    if resized.counts.0[2] == 0 {
        return Err("realloc wrapper did not observe Rust reallocation");
    }

    let zeroed = measure(|| unsafe { black_box(alloc_zeroed(black_box(small))) });
    println!("control calloc {:?}", zeroed.counts);
    if zeroed.value.is_null() {
        return Err("calloc positive control returned null");
    }
    unsafe { dealloc(zeroed.value, small) };
    if zeroed.counts.0[1] == 0 {
        return Err("calloc wrapper did not observe Rust zeroed allocation");
    }

    let over_aligned = measure(|| unsafe { black_box(alloc(black_box(aligned))) });
    println!("control posix_memalign {:?}", over_aligned.counts);
    if over_aligned.value.is_null() {
        return Err("aligned positive control returned null");
    }
    unsafe { dealloc(over_aligned.value, aligned) };
    if over_aligned.counts.0[3] == 0 {
        return Err("posix_memalign wrapper did not observe aligned allocation");
    }

    let formatting = measure(|| {
        codec::Error::overflow("DECIMAL", format!("({} % {})", black_box(lhs), black_box(rhs)))
    });
    println!("control bounded diagnostic {:?}", formatting.counts);
    if formatting.counts.total() == 0 {
        return Err("bounded diagnostic positive control was invisible");
    }
    drop(formatting.value);
    Ok(())
}

fn report(name: &str, pairs: &[Pair; 8]) -> Result<bool, &'static str> {
    // Printing and all interpretation are outside every measurement gate.
    for (index, pair) in pairs.iter().enumerate() {
        println!("{name} sample{index}: actual={:?} baseline={:?}", pair.actual, pair.baseline);
    }
    if pairs.iter().any(|pair| *pair != pairs[0]) {
        return Err("actual/baseline counts were not stable across alternating pairs");
    }
    let pair = pairs[0];
    if pair.actual == pair.baseline {
        println!("{name}: PARITY (not a general allocation bound)");
        Ok(false)
    } else if pair.actual.total() > pair.baseline.total() {
        println!("{name}: RED: {} extra allocation requests", pair.actual.total() - pair.baseline.total());
        Ok(true)
    } else {
        Err("different allocation routes without a positive total excess")
    }
}

fn run() -> Result<bool, &'static str> {
    // All Decimal operands are bounded ordinary 1 and 2. The half value below
    // is only an outside-gate numeric expectation, never an operand.
    let lhs = Decimal::from(1);
    let rhs = Decimal::from(2);
    let half: Decimal = "0.5".parse().map_err(|_| "half expectation")?;
    let mut mod_actual = EvalContext::default();
    let mut mod_base = EvalContext::default();
    let mut div_actual = EvalContext::default();
    let mut div_base = EvalContext::default();
    let mut day_actual = EvalContext::default();
    let mut day_base = EvalContext::default();
    let increment = div_actual.cfg.div_precision_increment;
    if increment != div_base.cfg.div_precision_increment {
        return Err("division configuration mismatch");
    }

    let raw_mod = (&lhs % &rhs).ok_or("raw MOD returned None")?;
    let raw_div = lhs.div(&rhs, increment).ok_or("raw DIV returned None")?;
    if !raw_mod.is_ok() || !raw_div.is_ok() {
        return Err("bounded witness did not produce Res::Ok");
    }
    drop((raw_mod, raw_div));

    // Untracked warm-up for every exact operation, not just the allocator.
    for _ in 0..2 {
        let a = DecimalMod::calc(&mut mod_actual, &lhs, &rhs)
            .map_err(|_| "warm MOD error")?.ok_or("warm MOD NULL")?;
        let b = (&lhs % &rhs).ok_or("warm MOD baseline None")?
            .into_result(&mut mod_base).map_err(|_| "warm MOD baseline error")?;
        if !same_decimal(&a, &b) || a != lhs {
            return Err("warm MOD semantic mismatch");
        }
        let a = DecimalDivide::calc(&mut div_actual, &lhs, &rhs)
            .map_err(|_| "warm DIV error")?.ok_or("warm DIV NULL")?;
        let b = lhs.div(&rhs, increment).ok_or("warm DIV baseline None")?
            .into_result(&mut div_base).map_err(|_| "warm DIV baseline error")?;
        if !same_decimal(&a, &b) || a != half {
            return Err("warm DIV semantic mismatch");
        }
        let a = lhs.to_interval_string(&mut day_actual, IntervalUnit::Day, false, 0)
            .map_err(|_| "warm DAY error")?;
        let b = day_baseline(&lhs, &mut day_base).map_err(|_| "warm DAY baseline error")?;
        if a != b || a != "1" {
            return Err("warm DAY semantic mismatch");
        }
    }
    if [&mod_actual, &mod_base, &div_actual, &div_base, &day_actual, &day_base]
        .iter().any(|ctx| !quiet(ctx)) {
        return Err("warm-up produced warnings");
    }
    controls(&lhs, &rhs)?;

    let mut mod_pairs = [Pair::default(); 8];
    let mut div_pairs = [Pair::default(); 8];
    let mut day_pairs = [Pair::default(); 8];
    for index in 0..8 {
        let reverse = index % 2 != 0;
        let (actual, baseline) = measure_pair(
            reverse,
            || DecimalMod::calc(&mut mod_actual, black_box(&lhs), black_box(&rhs)),
            || (black_box(&lhs) % black_box(&rhs)).map(|r| r.into_result(&mut mod_base)),
        );
        mod_pairs[index] = Pair { actual: actual.counts, baseline: baseline.counts };
        let a = actual.value.map_err(|_| "measured MOD error")?.ok_or("measured MOD NULL")?;
        let b = baseline.value.ok_or("measured MOD baseline None")?
            .map_err(|_| "measured MOD baseline error")?;
        if !same_decimal(&a, &b) || a != lhs {
            return Err("measured MOD semantic mismatch");
        }

        let (actual, baseline) = measure_pair(
            reverse,
            || DecimalDivide::calc(&mut div_actual, black_box(&lhs), black_box(&rhs)),
            || black_box(&lhs).div(black_box(&rhs), increment)
                .map(|r| r.into_result(&mut div_base)),
        );
        div_pairs[index] = Pair { actual: actual.counts, baseline: baseline.counts };
        let a = actual.value.map_err(|_| "measured DIV error")?.ok_or("measured DIV NULL")?;
        let b = baseline.value.ok_or("measured DIV baseline None")?
            .map_err(|_| "measured DIV baseline error")?;
        if !same_decimal(&a, &b) || a != half {
            return Err("measured DIV semantic mismatch");
        }

        let (actual, baseline) = measure_pair(
            reverse,
            || black_box(&lhs).to_interval_string(&mut day_actual, IntervalUnit::Day, false, 0),
            || day_baseline(black_box(&lhs), &mut day_base),
        );
        day_pairs[index] = Pair { actual: actual.counts, baseline: baseline.counts };
        let a = actual.value.map_err(|_| "measured DAY error")?;
        let b = baseline.value.map_err(|_| "measured DAY baseline error")?;
        if a != b || a != "1" {
            return Err("measured DAY semantic mismatch");
        }
        if [&mod_actual, &mod_base, &div_actual, &div_base, &day_actual, &day_base]
            .iter().any(|ctx| !quiet(ctx)) {
            return Err("measurement produced warnings");
        }
    }

    // Inspect every case before propagating an INCONCLUSIVE classification.
    let mod_red = report("MOD", &mod_pairs);
    let div_red = report("DIV", &div_pairs);
    let day_red = report("DAY", &day_pairs);
    Ok(mod_red? | div_red? | day_red?)
}

fn main() {
    println!("Private System/libc request probe; order=[malloc,calloc,realloc,posix_memalign]");
    println!("No wide Decimal inputs, no output-byte/peak-memory guarantee; single-threaded only.");
    let status = match std::panic::catch_unwind(run) {
        Ok(Ok(false)) => { println!("PARITY: all three stable native/baseline pairs agree"); 0 }
        Ok(Ok(true)) => { println!("RED: stable unnecessary caller allocation observed"); 1 }
        Ok(Err(reason)) => { eprintln!("INCONCLUSIVE: {reason}"); 2 }
        Err(_) => {
            ENABLED.store(false, Ordering::SeqCst);
            eprintln!("INCONCLUSIVE: unexpected panic (not an allocation finding)");
            2
        }
    };
    std::process::exit(status);
}
