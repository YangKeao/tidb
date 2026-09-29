//! Private standalone experiment: source authored only, NOT a product test,
//! global allocator, C4 implementation, or a runtime acceptance receipt.
//!
//! Parent must compile/run separately for the pinned Jan TiKV and Aug TiDB
//! datatype cohorts, recording rustc/target/link flags, this source SHA and the
//! actual matched metadata/rlib SHAs (including split .rmeta/.rlib on Aug).
//! Do not infer one cohort's result from the other or from a DEV build.
//!
//! Required GNU linker wraps (through the compiler's linker driver):
//!   -C link-arg=-Wl,--wrap=malloc
//!   -C link-arg=-Wl,--wrap=calloc
//!   -C link-arg=-Wl,--wrap=realloc
//!   -C link-arg=-Wl,--wrap=posix_memalign
//!   -C link-arg=-Wl,--wrap=free
//! Use edition 2021 and the matched tidb_query_datatype dependency set. There
//! is NO #[global_allocator], allocator_api feature, or alternative allocator.
//! This must be an isolated single-threaded executable, not linked into the
//! ordinary test binary or together with the existing decimal probe.
//!
//! Source basis inspected by the author (not compiled or run by the author):
//! - Jan rust-src library/alloc/src/sync.rs:382-392,419-427 has
//!   repr(C, align(2)) ArcInner { strong: Atomic<usize>, weak: Atomic<usize>,
//!   data: T }, and Arc::new actually allocates Box::new(ArcInner { ... }).
//! - Jan library/core/src/sync/atomic.rs:292-321,345 maps Atomic<usize> to
//!   AtomicUsize. Jan library/std/src/sys/alloc/unix.rs:8-56,73-85 forwards the
//!   System routes measured by the positive controls below.
//! - These Jan sources are installed under
//!   expression-reuse/rustup-home/toolchains/
//!   nightly-2026-01-30-x86_64-unknown-linux-gnu/lib/rustlib/src/rust/.
//! - Aug rust-src was NOT installed at the corresponding
//!   nightly-2026-08-22-x86_64-unknown-linux-gnu/lib/rustlib/src path; its Arc
//!   source-layout identity remains unverified by this author. A matching
//!   allocation-size observation is not proof of uninspected field offsets.
//! - TiKV expr/ctx.rs:84-87,114-124,133-135,196-201,239-242 exposes the public
//!   constructors/limit setter used here. codec/mysql/time/tz.rs:46-49 makes
//!   Tz::utc() the named UTC variant, not an Offset or the local timezone.
//!
//! The proxy is NEVER allocated, converted to/from Arc, or used to inspect Arc
//! memory. A real Arc<EvalConfig> request is independently observed through the
//! linked allocation path and only THEN compared with the proxy's layout size.
//! Hook pointers are opaque identities used only for realloc/free matching;
//! no pointer reinterpretation, header access, or usable-size query occurs.
//!
//! Counts include wrapped allocation REQUESTS, including failed attempts.
//! Sizes are the arguments to malloc/calloc/realloc/posix_memalign, NOT allocator
//! usable bytes. calloc bytes use checked count*size; free has no size argument.
//! Event status is posix_memalign's return code (zero placeholder otherwise);
//! malloc/calloc/realloc success is determined by their non-null result pointer.
//! Event storage is fixed-capacity and heap-free; overflow/incomplete records
//! invalidate the measurement. GNU wrapping does not see every call internal
//! to a shared libc or an allocator using different symbols. Real Rust-route
//! positive controls must pass; zero unseen requests must never count as PASS.
//!
//! Retention claim is ONLY one observed config allocation kept alive across
//! Arc clones and zero-detail EvalContext wrapping, followed by the matching
//! free when the sole strong owner (with no external Weak) is dropped. It is
//! not a general live-allocation tracker, Weak-lifetime test, allocator peak,
//! allocator bookkeeping/slack measurement, OOM bound, or C4 worker/pool budget.
//! Other program/context inline fields, driver scratch and output are not in
//! scope. Request-byte sums, especially for realloc controls, are NOT live bytes.
//! Repeated context wrapping is a control, NOT permission to recreate C4's
//! context per row; its first-ready, once-per-runtime lifecycle still applies.
//!
//! Exit 0: controls and all repeated narrow assertions passed for THIS binary.
//! Exit 1: config/Arc/reuse observation differs from the proposed narrow bound.
//! Exit 2: invalid controls, records, or semantic/setup assumptions.

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
compile_error!("this private probe requires the parent's isolated Linux GNU cohort");

use std::{
    alloc::{alloc, alloc_zeroed, dealloc, realloc, Layout},
    ffi::{c_int, c_void},
    hint::black_box,
    mem::{align_of, offset_of, size_of},
    ptr,
    sync::{
        atomic::{AtomicBool, AtomicI32, AtomicPtr, AtomicUsize, Ordering},
        Arc,
    },
};

use tidb_query_datatype::{
    codec::mysql::Tz,
    expr::{EvalConfig, EvalContext},
};

// Accounting-only pinned layout. align(2) mirrors the inspected Jan definition;
// the AtomicUsize fields already require at least that alignment on the pins.
#[repr(C, align(2))]
struct PinnedArcConfigLayout {
    strong: AtomicUsize,
    weak: AtomicUsize,
    cfg: EvalConfig,
}

const MALLOC: usize = 1;
const CALLOC: usize = 2;
const REALLOC: usize = 3;
const ALIGNED: usize = 4;
const FREE: usize = 5;
const MAX_EVENTS: usize = 32;
const SAMPLES: usize = 8;
const REUSES: usize = 64;

#[derive(Clone, Copy)]
struct Event {
    route: usize,
    a: usize,
    b: usize,
    old: *mut c_void,
    new: *mut c_void,
    status: c_int,
}

impl Event {
    fn requested_bytes(self) -> Option<usize> {
        match self.route {
            MALLOC | REALLOC | ALIGNED => Some(self.a),
            CALLOC => self.a.checked_mul(self.b),
            _ => None,
        }
    }

    fn is_new_allocation(self) -> bool {
        matches!(self.route, MALLOC | CALLOC | ALIGNED)
            && self.status == 0
            && self.old.is_null()
            && !self.new.is_null()
    }
}

struct EventSlot {
    route: AtomicUsize,
    a: AtomicUsize,
    b: AtomicUsize,
    old: AtomicPtr<c_void>,
    new: AtomicPtr<c_void>,
    status: AtomicI32,
    done: AtomicBool,
}

impl EventSlot {
    const fn new() -> Self {
        Self {
            route: AtomicUsize::new(0),
            a: AtomicUsize::new(0),
            b: AtomicUsize::new(0),
            old: AtomicPtr::new(ptr::null_mut()),
            new: AtomicPtr::new(ptr::null_mut()),
            status: AtomicI32::new(0),
            done: AtomicBool::new(false),
        }
    }

    fn load(&self) -> Event {
        Event {
            route: self.route.load(Ordering::SeqCst),
            a: self.a.load(Ordering::SeqCst),
            b: self.b.load(Ordering::SeqCst),
            old: self.old.load(Ordering::SeqCst),
            new: self.new.load(Ordering::SeqCst),
            status: self.status.load(Ordering::SeqCst),
        }
    }
}

static ENABLED: AtomicBool = AtomicBool::new(false);
static COUNT: AtomicUsize = AtomicUsize::new(0);
static OVERFLOW: AtomicBool = AtomicBool::new(false);
static EVENTS: [EventSlot; MAX_EVENTS] = [const { EventSlot::new() }; MAX_EVENTS];

// No formatting, heap allocation, locks, assertions, or allocator queries here.
// Capture enablement at hook entry; publish only after the real call returns.
fn record(enabled: bool, event: Event) {
    if !enabled {
        return;
    }
    let index = COUNT.fetch_add(1, Ordering::SeqCst);
    if index >= MAX_EVENTS {
        OVERFLOW.store(true, Ordering::SeqCst);
        return;
    }
    let slot = &EVENTS[index];
    slot.route.store(event.route, Ordering::SeqCst);
    slot.a.store(event.a, Ordering::SeqCst);
    slot.b.store(event.b, Ordering::SeqCst);
    slot.old.store(event.old, Ordering::SeqCst);
    slot.new.store(event.new, Ordering::SeqCst);
    slot.status.store(event.status, Ordering::SeqCst);
    slot.done.store(true, Ordering::SeqCst);
}

unsafe extern "C" {
    fn __real_malloc(size: usize) -> *mut c_void;
    fn __real_calloc(count: usize, size: usize) -> *mut c_void;
    fn __real_realloc(old: *mut c_void, size: usize) -> *mut c_void;
    fn __real_posix_memalign(out: *mut *mut c_void, align: usize, size: usize) -> c_int;
    fn __real_free(old: *mut c_void);
}

#[no_mangle]
pub unsafe extern "C" fn __wrap_malloc(size: usize) -> *mut c_void {
    let enabled = ENABLED.load(Ordering::SeqCst);
    let new = unsafe { __real_malloc(size) };
    record(enabled, Event { route: MALLOC, a: size, b: 0, old: ptr::null_mut(), new, status: 0 });
    new
}

#[no_mangle]
pub unsafe extern "C" fn __wrap_calloc(count: usize, size: usize) -> *mut c_void {
    let enabled = ENABLED.load(Ordering::SeqCst);
    let new = unsafe { __real_calloc(count, size) };
    record(enabled, Event { route: CALLOC, a: count, b: size, old: ptr::null_mut(), new, status: 0 });
    new
}

#[no_mangle]
pub unsafe extern "C" fn __wrap_realloc(old: *mut c_void, size: usize) -> *mut c_void {
    let enabled = ENABLED.load(Ordering::SeqCst);
    let new = unsafe { __real_realloc(old, size) };
    record(enabled, Event { route: REALLOC, a: size, b: 0, old, new, status: 0 });
    new
}

#[no_mangle]
pub unsafe extern "C" fn __wrap_posix_memalign(
    out: *mut *mut c_void,
    align: usize,
    size: usize,
) -> c_int {
    let enabled = ENABLED.load(Ordering::SeqCst);
    let status = unsafe { __real_posix_memalign(out, align, size) };
    // Only read the real function's documented out parameter on success.
    let new = if status == 0 { unsafe { *out } } else { ptr::null_mut() };
    record(enabled, Event { route: ALIGNED, a: size, b: align, old: ptr::null_mut(), new, status });
    status
}

#[no_mangle]
pub unsafe extern "C" fn __wrap_free(old: *mut c_void) {
    let enabled = ENABLED.load(Ordering::SeqCst);
    unsafe { __real_free(old) };
    record(enabled, Event { route: FREE, a: 0, b: 0, old, new: ptr::null_mut(), status: 0 });
}

struct Trace {
    count: usize,
    complete: bool,
    events: [Event; MAX_EVENTS],
}

struct Measurement<T> {
    value: T,
    trace: Trace,
}

struct DisableOnDrop;
impl Drop for DisableOnDrop {
    fn drop(&mut self) {
        ENABLED.store(false, Ordering::SeqCst);
    }
}

// Deliberately keeps the returned owner alive after disabling the gate. Drop
// is measured in its own window. Assertions/printing run outside every window.
#[inline(never)]
fn measure<T>(operation: impl FnOnce() -> T) -> Measurement<T> {
    assert!(!ENABLED.load(Ordering::SeqCst), "nested probe measurement");
    COUNT.store(0, Ordering::SeqCst);
    OVERFLOW.store(false, Ordering::SeqCst);
    for slot in &EVENTS {
        slot.done.store(false, Ordering::SeqCst);
    }
    let guard = DisableOnDrop;
    ENABLED.store(true, Ordering::SeqCst);
    let value = black_box(operation());
    drop(guard);
    let count = COUNT.load(Ordering::SeqCst);
    let complete = !OVERFLOW.load(Ordering::SeqCst)
        && count <= MAX_EVENTS
        && EVENTS[..count.min(MAX_EVENTS)].iter().all(|slot| slot.done.load(Ordering::SeqCst));
    let events = std::array::from_fn(|index| EVENTS[index].load());
    Measurement { value, trace: Trace { count, complete, events } }
}

#[derive(Clone, Copy, Debug)]
enum ProbeError {
    Invalid(&'static str),
    Mismatch(&'static str),
}
type ProbeResult<T> = Result<T, ProbeError>;

fn route_name(route: usize) -> &'static str {
    match route {
        MALLOC => "malloc",
        CALLOC => "calloc",
        REALLOC => "realloc",
        ALIGNED => "posix_memalign",
        FREE => "free",
        _ => "UNKNOWN",
    }
}

fn show(label: &str, sample: usize, trace: &Trace) {
    let mut requests = 0;
    let mut frees = 0;
    let mut requested_sum = Some(0usize);
    for event in &trace.events[..trace.count.min(MAX_EVENTS)] {
        if event.route == FREE {
            frees += 1;
        } else {
            requests += 1;
            requested_sum = requested_sum.and_then(|sum| sum.checked_add(event.requested_bytes()?));
        }
    }
    println!(
        "{label} sample={sample}: events={} requests={requests} frees={frees} requested_sum_not_live={requested_sum:?} complete={}",
        trace.count, trace.complete
    );
    for (index, event) in trace.events[..trace.count.min(MAX_EVENTS)].iter().enumerate() {
        println!(
            "  event={index} route={} request_bytes={:?} raw_a={} raw_b={} old={:p} new={:p} status={}",
            route_name(event.route), event.requested_bytes(), event.a, event.b,
            event.old, event.new, event.status
        );
    }
}

fn valid(trace: &Trace) -> ProbeResult<()> {
    if !trace.complete {
        return Err(ProbeError::Invalid("event capacity exceeded or incomplete publication"));
    }
    Ok(())
}

fn empty(trace: &Trace, failure: ProbeError) -> ProbeResult<()> {
    valid(trace)?;
    if trace.count != 0 {
        return Err(failure);
    }
    Ok(())
}

fn single(trace: &Trace, failure: ProbeError) -> ProbeResult<Event> {
    valid(trace)?;
    if trace.count != 1 {
        return Err(failure);
    }
    Ok(trace.events[0])
}

fn new_request(trace: &Trace, route: usize, bytes: usize) -> ProbeResult<Event> {
    let failure = ProbeError::Invalid("Rust allocation route/size positive control failed");
    let event = single(trace, failure)?;
    if !event.is_new_allocation() || event.route != route || event.requested_bytes() != Some(bytes) {
        return Err(failure);
    }
    Ok(event)
}

fn freed(trace: &Trace, allocation: Event, failure: ProbeError) -> ProbeResult<()> {
    let event = single(trace, failure)?;
    if event.route != FREE || event.old != allocation.new || event.old.is_null() {
        return Err(failure);
    }
    Ok(())
}

// Owns only a positive-control allocation. No alternate allocator implementation.
// Cleanup also happens on a failed control, after measurement has been disabled.
struct RawControl {
    ptr: *mut u8,
    layout: Layout,
}
impl Drop for RawControl {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            unsafe { dealloc(self.ptr, self.layout) };
        }
    }
}

fn controls(sample: usize) -> ProbeResult<()> {
    let measured = measure(|| black_box(()));
    show("control/empty", sample, &measured.trace);
    empty(&measured.trace, ProbeError::Invalid("empty control was nonzero"))?;

    // Non-power-of-two request sizes distinguish the real size arguments from
    // rounded usable-size guesses. Every operation uses the inherited Rust path.
    let small = Layout::from_size_align(73, 8).map_err(|_| ProbeError::Invalid("small layout"))?;
    let larger = Layout::from_size_align(149, 8).map_err(|_| ProbeError::Invalid("larger layout"))?;
    let zeroed = Layout::from_size_align(91, 8).map_err(|_| ProbeError::Invalid("zeroed layout"))?;
    let aligned = Layout::from_size_align(257, 64).map_err(|_| ProbeError::Invalid("aligned layout"))?;

    let measured = measure(|| unsafe { alloc(black_box(small)) });
    let mut block = RawControl { ptr: measured.value, layout: small };
    show("control/malloc", sample, &measured.trace);
    let initial = new_request(&measured.trace, MALLOC, small.size())?;
    if block.ptr.is_null() {
        return Err(ProbeError::Invalid("Rust allocation control returned null"));
    }

    let measured = measure(|| unsafe {
        realloc(black_box(block.ptr), black_box(small), black_box(larger.size()))
    });
    // realloc failure leaves the original allocation live. On success update
    // the RAII owner BEFORE any assertion can fail; never free the stale pointer.
    if !measured.value.is_null() {
        block.ptr = measured.value;
        block.layout = larger;
    }
    show("control/realloc", sample, &measured.trace);
    let resized = single(&measured.trace, ProbeError::Invalid("realloc event count"))?;
    if measured.value.is_null()
        || resized.route != REALLOC
        || resized.old != initial.new
        || resized.new.is_null()
        || resized.requested_bytes() != Some(larger.size())
    {
        return Err(ProbeError::Invalid("Rust realloc route/size positive control failed"));
    }
    let measured = measure(|| drop(block));
    show("control/free-resized", sample, &measured.trace);
    freed(&measured.trace, resized, ProbeError::Invalid("realloc final free control failed"))?;

    let measured = measure(|| unsafe { alloc_zeroed(black_box(zeroed)) });
    let block = RawControl { ptr: measured.value, layout: zeroed };
    show("control/calloc", sample, &measured.trace);
    let allocation = new_request(&measured.trace, CALLOC, zeroed.size())?;
    if block.ptr.is_null() {
        return Err(ProbeError::Invalid("Rust zeroed control returned null"));
    }
    let measured = measure(|| drop(block));
    show("control/free-zeroed", sample, &measured.trace);
    freed(&measured.trace, allocation, ProbeError::Invalid("calloc free control failed"))?;

    let measured = measure(|| unsafe { alloc(black_box(aligned)) });
    let block = RawControl { ptr: measured.value, layout: aligned };
    show("control/posix_memalign", sample, &measured.trace);
    let allocation = new_request(&measured.trace, ALIGNED, aligned.size())?;
    if block.ptr.is_null() || allocation.b != aligned.align() {
        return Err(ProbeError::Invalid("Rust aligned control pointer/alignment mismatch"));
    }
    let measured = measure(|| drop(block));
    show("control/free-aligned", sample, &measured.trace);
    freed(&measured.trace, allocation, ProbeError::Invalid("aligned free control failed"))?;
    Ok(())
}

// Exact public constructor policy requested for this isolated experiment.
// NOT default_for_test(), a session-derived context, or a C4 worker constructor.
#[inline(never)]
fn pure_config() -> EvalConfig {
    let mut cfg = EvalConfig::default();
    cfg.tz = Tz::utc();
    cfg.set_max_warning_cnt(0);
    cfg
}

fn check_config(cfg: &EvalConfig) -> ProbeResult<()> {
    let defaults = EvalConfig::default();
    let utc = Tz::utc().get_chrono_tz();
    if utc.is_none()
        || cfg.tz.get_chrono_tz() != utc
        || !cfg.flag.is_empty()
        || !cfg.sql_mode.is_empty()
        || cfg.max_warning_cnt != 0
        || cfg.paging_size.is_some()
        || cfg.max_keys_read.is_some()
        || cfg.div_precision_increment != defaults.div_precision_increment
        || cfg.is_test
    {
        return Err(ProbeError::Invalid("fixed UTC/default/zero-detail config assumptions differ"));
    }
    Ok(())
}

fn check_context(ctx: &EvalContext) -> ProbeResult<()> {
    check_config(&ctx.cfg)?;
    if ctx.warnings.warning_cnt != 0
        || !ctx.warnings.warnings.is_empty()
        || ctx.warnings.warnings.capacity() != 0
    {
        return Err(ProbeError::Invalid("zero-detail context warning storage was not empty/zero-capacity"));
    }
    if Arc::strong_count(&ctx.cfg) != 1 || Arc::weak_count(&ctx.cfg) != 0 {
        return Err(ProbeError::Invalid("final context is not the sole strong owner with no external Weak"));
    }
    Ok(())
}

fn one_sample(sample: usize, expected_bytes: usize) -> ProbeResult<()> {
    let measured = measure(|| black_box(()));
    show("sample/empty", sample, &measured.trace);
    empty(&measured.trace, ProbeError::Invalid("sample empty control was nonzero"))?;

    // Prove that the chosen config construction itself made no wrapped heap
    // requests. It remains on the stack until the independently measured Arc.
    let Measurement { value: cfg, trace } = measure(pure_config);
    show("config/construct", sample, &trace);
    check_config(&cfg)?;
    empty(&trace, ProbeError::Mismatch("fixed config construction allocated/freed"))?;

    // This is the only tested new config owner: an ACTUAL Arc allocation.
    // No call allocates PinnedArcConfigLayout or feeds its size to an allocator.
    let Measurement { value: owner, trace } = measure(|| Arc::new(black_box(cfg)));
    show("config/Arc-new", sample, &trace);
    let allocation = single(&trace, ProbeError::Mismatch("Arc config was not exactly one allocation request"))?;
    if !allocation.is_new_allocation() || allocation.requested_bytes() != Some(expected_bytes) {
        return Err(ProbeError::Mismatch("real Arc allocation request differs from pinned layout size"));
    }
    check_config(&owner)?;
    if Arc::strong_count(&owner) != 1 || Arc::weak_count(&owner) != 0 {
        return Err(ProbeError::Invalid("new Arc owner count assumptions differ"));
    }

    let measured = measure(|| {
        for _ in 0..REUSES {
            let clone = Arc::clone(black_box(&owner));
            black_box(&clone);
            drop(clone);
        }
    });
    show("config/Arc-clone-drop", sample, &measured.trace);
    empty(&measured.trace, ProbeError::Mismatch("Arc clone/drop with a live owner allocated/freed"))?;

    let measured = measure(|| {
        for _ in 0..REUSES {
            let ctx = EvalContext::new(Arc::clone(black_box(&owner)));
            black_box(&ctx);
            drop(ctx);
        }
    });
    show("context/clone-wrap-drop", sample, &measured.trace);
    empty(&measured.trace, ProbeError::Mismatch("zero-detail context reuse allocated/freed"))?;
    if Arc::strong_count(&owner) != 1 || Arc::weak_count(&owner) != 0 {
        return Err(ProbeError::Invalid("reuse leaked a config Arc/Weak owner"));
    }

    let Measurement { value: ctx, trace } = measure(|| EvalContext::new(black_box(owner)));
    show("context/move-wrap", sample, &trace);
    empty(&trace, ProbeError::Mismatch("moving the sole Arc into context allocated/freed"))?;
    check_context(&ctx)?;

    // That same allocation, not a payload/header address guessed from Arc,
    // must reach the real deallocator after the final owner is destroyed.
    let measured = measure(|| drop(ctx));
    show("context/final-drop", sample, &measured.trace);
    freed(&measured.trace, allocation, ProbeError::Mismatch("final context drop did not free exactly the observed config allocation"))?;
    println!(
        "sample={sample} owner-request-bytes={expected_bytes} retained-one-owner-through-reuse=true matching-final-free=true (not peak/usable-size/general heap)"
    );
    Ok(())
}

fn run() -> ProbeResult<()> {
    let proxy = Layout::new::<PinnedArcConfigLayout>();
    println!("isolated Arc<EvalConfig> request probe: arch={} os={} pointer_bits={}",
        std::env::consts::ARCH, std::env::consts::OS, usize::BITS);
    println!("cohort-label={}", option_env!("ARC_CFG_PROBE_COHORT").unwrap_or("unrecorded-in-binary; parent must log exact cohort"));
    println!("source-pin: Jan installed ArcInner repr(C,align(2)); Aug rust-src not inspected/installed by source author");
    println!("layout: EvalConfig size={} align={}; AtomicUsize size={} align={}; Arc handle size={} (NOT allocation size)",
        size_of::<EvalConfig>(), align_of::<EvalConfig>(), size_of::<AtomicUsize>(),
        align_of::<AtomicUsize>(), size_of::<Arc<EvalConfig>>());
    println!("pinned-proxy: size={} align={} strong_offset={} weak_offset={} cfg_offset={} (proxy is NEVER allocated)",
        proxy.size(), proxy.align(), offset_of!(PinnedArcConfigLayout, strong),
        offset_of!(PinnedArcConfigLayout, weak), offset_of!(PinnedArcConfigLayout, cfg));
    println!("tracking: fixed {MAX_EVENTS} events/window, {SAMPLES} samples, {REUSES} Arc/context reuses/sample; allocation requests and matching frees only");
    if align_of::<AtomicUsize>() < 2 {
        return Err(ProbeError::Invalid("target atomic alignment outside inspected pin assumptions"));
    }
    controls(0)?;
    for sample in 0..SAMPLES {
        one_sample(sample, proxy.size())?;
    }
    controls(1)?;
    println!("PASS: actual Arc request equals pinned size and narrow reuse/final-free controls passed for this linked cohort only");
    Ok(())
}

fn main() {
    let code = match run() {
        Ok(()) => 0,
        Err(ProbeError::Mismatch(message)) => {
            eprintln!("MISMATCH: {message}; not an accepted config-owner allocation bound");
            1
        }
        Err(ProbeError::Invalid(message)) => {
            eprintln!("INVALID/INCONCLUSIVE: {message}; controls/assumptions do not support a result");
            2
        }
    };
    std::process::exit(code);
}
