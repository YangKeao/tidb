//! PRIVATE, SOURCE-AUTHORED ONLY: parent owns every compile/link/run gate.
//!
//! Synthetic ONE-SHOT refusal of ONE verified small allocation in the REAL
//! inherent Datum::to_string on ordinary Decimal12.34. No physical/sustained OOM,
//! huge scale, product hook, allocator replacement, or expression dependency.
//! The existing global allocator MUST route through System/GNU wrap controls.
//!
//! Driver: fresh observer child -> verify its one small malloc -> fresh injected
//! child. Children are single-threaded, warmed OS processes with checked
//! RLIMIT_CORE=0 AND PR_SET_DUMPABLE=0 (including piped-core-handler protection).
//! A fixed raw pipe receipt is written BEFORE refusing the request and survives
//! an old child's allocation abort. No allocation/formatting/logger/unwind in
//! hooks. Injection is disarmed before refusal; error-message allocations work.
//!
//! Driver exits: 0 = one-shot failure returned actual outer InvalidDataType Err;
//! 1 = verified one-shot hit followed by expected SIGABRT (old-path RED);
//! 2 = failed controls, wrong/no hit, unexpected result/signal, or inconclusive.
//! Child exits: 0 = observer/control success; 20 = injected outer codec Err;
//! 21 = injected call returned Ok; 22 = wrong error; 23 = invalid experiment.
//! An allocation abort is NOT caught by catch_unwind. Timeout is inconclusive.
//!
//! Exact parent link template (no Cargo/native-expression build):
//! ROOT=/home/agent/tidb/expression-unification
//! RUSTC=/home/agent/tidb/expression-reuse/rustup-home/toolchains/nightly-2026-01-30-x86_64-unknown-linux-gnu/bin/rustc
//! # Set DATATYPE_RLIB to a verified, MATCHED TEST-cohort artifact, not newest glob.
//! # Set NATIVE_ARGS as a Bash array of only recorded matching -L native paths.
//! # A DEV build does not refresh TEST rlibs or this executable.
//! "$RUSTC" --edition=2021 -C opt-level=0 -C lto=off \
//!   -C linker=/usr/bin/gcc-14 -C link-arg=-fuse-ld=bfd \
//!   -C link-arg=-Wl,--wrap=malloc -C link-arg=-Wl,--wrap=calloc \
//!   -C link-arg=-Wl,--wrap=realloc -C link-arg=-Wl,--wrap=posix_memalign \
//!   -L dependency="$ROOT/target-tikv/debug/deps" "${NATIVE_ARGS[@]}" \
//!   --extern tidb_query_datatype="$DATATYPE_RLIB" \
//!   "$ROOT/tools/decimal-datum-display-fault-probe.rs" \
//!   -o "$ROOT/tools/decimal-datum-display-fault-probe"
//! # Record source/compiler/rlib/dependency/executable hashes and complete command.
//! # Run the executable without child arguments; preserve stdout/stderr/status.
//! # Re-link IDENTICAL source against a later granted cohort and rerun controls.
//! # Link errors or unsupported allocator routes are NOT RED; do not alter Cargo,
//! # the allocator, existing I/Arc probes, or product code to make this link/run.

#[cfg(not(all(target_os = "linux", target_env = "gnu", target_pointer_width = "64")))]
compile_error!("private probe requires the reviewed 64-bit Linux/System/GNU route");

use std::{
    alloc::{alloc, alloc_zeroed, dealloc, realloc, Layout},
    ffi::{c_int, c_ulong, c_void},
    fs::File,
    hint::black_box,
    io::Read,
    os::{fd::FromRawFd, unix::process::ExitStatusExt},
    process::{Command, ExitStatus, Stdio},
    str::FromStr,
    sync::atomic::{AtomicBool, AtomicI32, AtomicUsize, Ordering},
};

use tidb_query_datatype::codec::{
    mysql::{Decimal, DecimalEncoder},
    Datum, Error,
};

type Check<T> = Result<T, &'static str>;

const OFF: usize = 0;
const OBSERVE: usize = 1;
const INJECT: usize = 2;
const MALLOC: u8 = 1;
const CALLOC: u8 = 2;
const REALLOC: u8 = 3;
const ALIGNED: u8 = 4;
const EMPTY_PHASE: u8 = 1;
const MALLOC_PHASE: u8 = 2;
const CALLOC_PHASE: u8 = 3;
const REALLOC_PHASE: u8 = 4;
const ALIGNED_PHASE: u8 = 5;
const DATUM_PHASE: u8 = 6;
const REQUEST: u8 = 1;
const HIT: u8 = 2;
const MISMATCH: u8 = 3;
const FINISH: u8 = 4;
const RETURNED: u8 = 5;
const MAGIC: u32 = 0x4453_4650;
const RECORD_LEN: usize = 32;
const MAX_REQUESTS: usize = 8;
const MAX_SMALL: usize = 256; // Probe eligibility only; NOT a product quota.
const CHILD_CODEC_ERR: i32 = 20;
const CHILD_OK_AFTER_HIT: i32 = 21;
const CHILD_WRONG_ERR: i32 = 22;
const CHILD_INVALID: i32 = 23;
const SIGABRT: i32 = 6;

// Private executable state. It is never added to a product crate/allocator.
static MODE: AtomicUsize = AtomicUsize::new(OFF);
static PHASE: AtomicUsize = AtomicUsize::new(0);
static REQUESTS: AtomicUsize = AtomicUsize::new(0);
static HITS: AtomicUsize = AtomicUsize::new(0);
static EXPECTED_SMALL: AtomicUsize = AtomicUsize::new(0);
static LAST_KIND: AtomicUsize = AtomicUsize::new(0);
static LAST_SIZE: AtomicUsize = AtomicUsize::new(0);
static BAD: AtomicBool = AtomicBool::new(false);
static PIPE_BAD: AtomicBool = AtomicBool::new(false);
static EVENT_FD: AtomicI32 = AtomicI32::new(-1);

#[repr(C)]
struct Rlimit {
    current: u64,
    maximum: u64,
}

unsafe extern "C" {
    fn __real_malloc(size: usize) -> *mut c_void;
    fn __real_calloc(count: usize, size: usize) -> *mut c_void;
    fn __real_realloc(ptr: *mut c_void, size: usize) -> *mut c_void;
    fn __real_posix_memalign(out: *mut *mut c_void, align: usize, size: usize) -> c_int;
    fn pipe(fds: *mut c_int) -> c_int;
    fn close(fd: c_int) -> c_int;
    fn fcntl(fd: c_int, command: c_int, ...) -> c_int;
    fn write(fd: c_int, data: *const c_void, len: usize) -> isize;
    fn setrlimit(resource: c_int, limit: *const Rlimit) -> c_int;
    fn prctl(option: c_int, ...) -> c_int;
}

// One fixed <= PIPE_BUF write, stack-only, no formatted logging or allocation.
// A failed/interrupted receipt fails CLOSED: no request may then be refused.
fn emit(tag: u8, phase: u8, kind: u8, ordinal: usize, size: usize, extra: usize) -> bool {
    let mut record = [0u8; RECORD_LEN];
    record[..4].copy_from_slice(&MAGIC.to_le_bytes());
    record[4] = tag;
    record[5] = phase;
    record[6] = kind;
    record[8..16].copy_from_slice(&(ordinal as u64).to_le_bytes());
    record[16..24].copy_from_slice(&(size as u64).to_le_bytes());
    record[24..32].copy_from_slice(&(extra as u64).to_le_bytes());
    let fd = EVENT_FD.load(Ordering::SeqCst);
    let ok = fd >= 3
        && unsafe { write(fd, record.as_ptr().cast(), record.len()) } == RECORD_LEN as isize;
    if !ok {
        PIPE_BAD.store(true, Ordering::SeqCst);
    }
    ok
}

// Return true ONLY for the independently observed first small malloc and only
// after its persistent receipt succeeded. calloc operands remain separate; no
// unchecked count*size, dereference, or allocation is performed by the hook.
fn observe_request(kind: u8, size: usize, extra: usize) -> bool {
    let mode = MODE.load(Ordering::SeqCst);
    if mode == OFF {
        return false;
    }
    let phase = PHASE.load(Ordering::SeqCst) as u8;
    let ordinal = REQUESTS.fetch_add(1, Ordering::SeqCst) + 1;
    LAST_KIND.store(kind as usize, Ordering::SeqCst);
    LAST_SIZE.store(size, Ordering::SeqCst);
    if ordinal > MAX_REQUESTS {
        BAD.store(true, Ordering::SeqCst);
        MODE.store(OFF, Ordering::SeqCst);
        return false;
    }
    if !emit(REQUEST, phase, kind, ordinal, size, extra) {
        BAD.store(true, Ordering::SeqCst);
        MODE.store(OFF, Ordering::SeqCst);
        return false;
    }
    if mode != INJECT {
        return false;
    }

    // Disarm BEFORE any refusal, including the error formatting that follows it.
    MODE.store(OFF, Ordering::SeqCst);
    let expected = EXPECTED_SMALL.load(Ordering::SeqCst);
    if phase != DATUM_PHASE
        || ordinal != 1
        || kind != MALLOC
        || extra != 0
        || size == 0
        || size > MAX_SMALL
        || size != expected
    {
        BAD.store(true, Ordering::SeqCst);
        emit(MISMATCH, phase, kind, ordinal, size, extra);
        return false;
    }
    if !emit(HIT, phase, kind, ordinal, size, extra) {
        BAD.store(true, Ordering::SeqCst);
        return false;
    }
    HITS.fetch_add(1, Ordering::SeqCst);
    true
}

#[no_mangle]
pub unsafe extern "C" fn __wrap_malloc(size: usize) -> *mut c_void {
    if observe_request(MALLOC, size, 0) {
        std::ptr::null_mut()
    } else {
        unsafe { __real_malloc(size) }
    }
}

#[no_mangle]
pub unsafe extern "C" fn __wrap_calloc(count: usize, size: usize) -> *mut c_void {
    observe_request(CALLOC, count, size);
    unsafe { __real_calloc(count, size) }
}

#[no_mangle]
pub unsafe extern "C" fn __wrap_realloc(ptr: *mut c_void, size: usize) -> *mut c_void {
    observe_request(REALLOC, size, 0);
    unsafe { __real_realloc(ptr, size) }
}

#[no_mangle]
pub unsafe extern "C" fn __wrap_posix_memalign(
    out: *mut *mut c_void,
    align: usize,
    size: usize,
) -> c_int {
    observe_request(ALIGNED, size, align);
    unsafe { __real_posix_memalign(out, align, size) }
}

fn begin(phase: u8, mode: usize, expected: usize) {
    MODE.store(OFF, Ordering::SeqCst);
    PHASE.store(phase as usize, Ordering::SeqCst);
    REQUESTS.store(0, Ordering::SeqCst);
    HITS.store(0, Ordering::SeqCst);
    LAST_KIND.store(0, Ordering::SeqCst);
    LAST_SIZE.store(0, Ordering::SeqCst);
    BAD.store(false, Ordering::SeqCst);
    EXPECTED_SMALL.store(expected, Ordering::SeqCst);
    MODE.store(mode, Ordering::SeqCst);
}

fn stop() -> Check<(usize, usize)> {
    MODE.store(OFF, Ordering::SeqCst);
    if BAD.load(Ordering::SeqCst) || PIPE_BAD.load(Ordering::SeqCst) {
        return Err("bad gate or persistent pipe receipt");
    }
    Ok((REQUESTS.load(Ordering::SeqCst), HITS.load(Ordering::SeqCst)))
}

fn one_control(kind: u8) -> Check<()> {
    if stop()? != (1, 0) || LAST_KIND.load(Ordering::SeqCst) != kind as usize {
        return Err("System allocator control did not observe exactly one expected request");
    }
    Ok(())
}

fn controls() -> Check<()> {
    begin(EMPTY_PHASE, OBSERVE, 0);
    black_box(());
    if stop()? != (0, 0) {
        return Err("empty control allocated");
    }

    let small = Layout::from_size_align(64, 8).map_err(|_| "small layout")?;
    let larger = Layout::from_size_align(128, 8).map_err(|_| "larger layout")?;
    let aligned = Layout::from_size_align(128, 64).map_err(|_| "aligned layout")?;
    begin(MALLOC_PHASE, OBSERVE, 0);
    let ptr = unsafe { black_box(alloc(black_box(small))) };
    let check = one_control(MALLOC);
    if ptr.is_null() {
        return Err("nonarmed malloc control returned null");
    }
    if let Err(err) = check {
        unsafe { dealloc(ptr, small) };
        return Err(err);
    }

    begin(REALLOC_PHASE, OBSERVE, 0);
    let resized = unsafe { black_box(realloc(black_box(ptr), small, larger.size())) };
    let check = one_control(REALLOC);
    if resized.is_null() {
        unsafe { dealloc(ptr, small) };
        return Err("nonarmed realloc control returned null");
    }
    unsafe { dealloc(resized, larger) };
    check?;

    begin(CALLOC_PHASE, OBSERVE, 0);
    let zeroed = unsafe { black_box(alloc_zeroed(black_box(small))) };
    let check = one_control(CALLOC);
    if zeroed.is_null() {
        return Err("nonarmed calloc control returned null");
    }
    unsafe { dealloc(zeroed, small) };
    check?;

    begin(ALIGNED_PHASE, OBSERVE, 0);
    let over_aligned = unsafe { black_box(alloc(black_box(aligned))) };
    let check = one_control(ALIGNED);
    if over_aligned.is_null() {
        return Err("nonarmed aligned control returned null");
    }
    unsafe { dealloc(over_aligned, aligned) };
    check?;
    println!(
        "controls empty=0 malloc/calloc/realloc/aligned=one expected request each; no refusal"
    );
    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
struct Shape {
    precision_and_storage: (usize, u32),
    storage: u32,
    visible: u32,
    negative: bool,
    cell: Vec<u8>,
}

fn snapshot(datum: &Datum) -> Check<Shape> {
    let Datum::Dec(value) = datum else {
        return Err("probe input is not Decimal");
    };
    let mut cell = Vec::new();
    cell.write_decimal_to_chunk(value)
        .map_err(|_| "bounded chunk snapshot failed")?;
    Ok(Shape {
        precision_and_storage: value.prec_and_frac(),
        storage: value.frac_cnt(),
        visible: value.result_frac_cnt(),
        negative: value.is_negative(),
        cell,
    })
}

fn single_thread() -> Check<()> {
    let entries = std::fs::read_dir("/proc/self/task").map_err(|_| "thread count unavailable")?;
    let mut count = 0;
    for entry in entries {
        entry.map_err(|_| "thread entry unavailable")?;
        count += 1;
    }
    if count != 1 {
        return Err("injected/observer child must have exactly one OS thread");
    }
    Ok(())
}

fn child(inject: bool, fd: i32, expected: usize) -> Check<i32> {
    if fd < 3 || (inject && !(1..=MAX_SMALL).contains(&expected)) {
        return Err("invalid child pipe or bounded request");
    }
    EVENT_FD.store(fd, Ordering::SeqCst);
    // Linux RLIMIT_CORE=4; process-local only, both current and hard limits zero.
    let no_core = Rlimit {
        current: 0,
        maximum: 0,
    };
    if unsafe { setrlimit(4, &no_core) } != 0 {
        return Err("cannot set child zero core limit");
    }
    // Piped Linux core handlers can ignore RLIMIT_CORE. Also disable dumping
    // process-locally: linux/prctl.h PR_SET_DUMPABLE=4, PR_GET_DUMPABLE=3.
    // Variadic operands are unsigned long, not narrower integer literals.
    let zero: c_ulong = 0;
    if unsafe { prctl(4, zero, zero, zero, zero) } != 0
        || unsafe { prctl(3, zero, zero, zero, zero) } != 0
    {
        return Err("cannot disable/verify child dumpability");
    }
    single_thread()?;
    let datum = Datum::Dec(Decimal::from_str("12.34").map_err(|_| "bounded fixture parse failed")?);
    let before = snapshot(&datum)?;
    if before.precision_and_storage != (4, 2)
        || before.storage != 2
        || before.visible != 2
        || before.negative
        || before.cell.len() != 40
        || before.cell[..4] != [2, 2, 2, 0]
    {
        return Err("unexpected bounded Decimal12.34 shape");
    }
    for _ in 0..2 {
        if Datum::to_string(black_box(&datum)).map_err(|_| "nonarmed Datum call failed")? != "12.34"
        {
            return Err("nonarmed Datum value changed");
        }
    }
    if snapshot(&datum)? != before {
        return Err("nonarmed Datum call mutated bounded owner");
    }
    println!("child inject={inject} nonarmed=12.34 owner={before:?}");
    println!("context=N/A: inherent Datum::to_string has no EvalContext argument");
    controls()?;
    single_thread()?;

    begin(DATUM_PHASE, if inject { INJECT } else { OBSERVE }, expected);
    let result = black_box(Datum::to_string(black_box(&datum)));
    MODE.store(OFF, Ordering::SeqCst);
    // Persist return BEFORE any post-call snapshot/logging/allocation. An abort
    // later in the harness must not masquerade as a failure inside Datum.
    if !emit(RETURNED, DATUM_PHASE, 0, 0, 0, 0) {
        return Err("missing immediate call-return receipt");
    }
    let (requests, hits) = stop()?;
    if snapshot(&datum)? != before {
        return Err("actual Datum call mutated bounded owner");
    }
    if requests != 1 || LAST_KIND.load(Ordering::SeqCst) != MALLOC as usize {
        return Err("actual Datum call did not have one first malloc");
    }
    let size = LAST_SIZE.load(Ordering::SeqCst);
    if !(1..=MAX_SMALL).contains(&size) {
        return Err("actual Datum request is outside small probe eligibility");
    }
    let code = if !inject {
        if hits != 0 || result.map_err(|_| "observed nonarmed Datum returned Err")? != "12.34" {
            return Err("observed nonarmed result or hit count changed");
        }
        println!("observer actual=12.34 first_malloc={size} requests=1 owner_unchanged=true");
        0
    } else {
        if hits != 1 || size != expected {
            return Err("injected call had wrong/no hit");
        }
        match result {
            Err(err) if matches!(&err, Error::InvalidDataType(_)) && err.code() != 1690 => {
                println!(
                    "injected actual=outer InvalidDataType code={} owner_unchanged=true hits=1",
                    err.code()
                );
                CHILD_CODEC_ERR
            }
            Err(err) => {
                println!("injected WRONG error={err:?} code={}", err.code());
                CHILD_WRONG_ERR
            }
            Ok(text) => {
                println!("injected WRONG Ok={text:?}");
                CHILD_OK_AFTER_HIT
            }
        }
    };
    if !emit(FINISH, DATUM_PHASE, MALLOC, hits, size, code as usize) {
        return Err("missing final child receipt");
    }
    Ok(code)
}

#[derive(Debug, PartialEq, Eq)]
struct Event {
    tag: u8,
    phase: u8,
    kind: u8,
    ordinal: usize,
    size: usize,
    extra: usize,
}

fn decode(bytes: &[u8]) -> Check<Vec<Event>> {
    if bytes.len() % RECORD_LEN != 0 || bytes.len() > 4096 {
        return Err("partial/oversized child receipt");
    }
    let mut events = Vec::new();
    for record in bytes.chunks_exact(RECORD_LEN) {
        if record[..4] != MAGIC.to_le_bytes() || record[7] != 0 {
            return Err("invalid child receipt magic/reserved byte");
        }
        let number = |offset| {
            let mut value = [0u8; 8];
            value.copy_from_slice(&record[offset..offset + 8]);
            u64::from_le_bytes(value) as usize
        };
        events.push(Event {
            tag: record[4],
            phase: record[5],
            kind: record[6],
            ordinal: number(8),
            size: number(16),
            extra: number(24),
        });
    }
    Ok(events)
}

fn run_child(inject: bool, expected: usize) -> Check<(ExitStatus, Vec<Event>)> {
    let mut fds = [-1; 2];
    if unsafe { pipe(fds.as_mut_ptr()) } != 0 {
        return Err("cannot create persistent child pipe");
    }
    // Linux F_SETFD=2, FD_CLOEXEC=1: only writer is inherited across exec.
    if unsafe { fcntl(fds[0], 2, 1) } != 0 {
        unsafe {
            close(fds[0]);
            close(fds[1]);
        }
        return Err("cannot set reader close-on-exec");
    }
    let mut reader = unsafe { File::from_raw_fd(fds[0]) };
    let exe = std::env::current_exe().map_err(|_| "cannot locate probe executable");
    let spawned = match exe {
        Ok(exe) => Command::new(exe)
            .arg(if inject {
                "--child-inject"
            } else {
                "--child-observe"
            })
            .arg(fds[1].to_string())
            .arg(expected.to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|_| "cannot spawn isolated probe child"),
        Err(err) => Err(err),
    };
    // Parent never keeps a pipe writer alive while waiting for child's EOF.
    unsafe { close(fds[1]) };
    let status = spawned?.wait().map_err(|_| "cannot wait for probe child")?;
    let mut bytes = Vec::new();
    reader
        .by_ref()
        .take(4097)
        .read_to_end(&mut bytes)
        .map_err(|_| "cannot read child receipt")?;
    let events = decode(&bytes)?;
    println!("persistent receipt inject={inject} status={status:?} events={events:?}");
    Ok((status, events))
}

fn verify_controls(events: &[Event]) -> Check<()> {
    for (phase, kind) in [
        (MALLOC_PHASE, MALLOC),
        (CALLOC_PHASE, CALLOC),
        (REALLOC_PHASE, REALLOC),
        (ALIGNED_PHASE, ALIGNED),
    ] {
        let rows: Vec<_> = events.iter().filter(|e| e.phase == phase).collect();
        if rows.len() != 1 || rows[0].tag != REQUEST || rows[0].kind != kind || rows[0].ordinal != 1
        {
            return Err("persistent positive control mismatch");
        }
        let event = rows[0];
        let size_matches = match kind {
            MALLOC => event.size == 64 && event.extra == 0,
            CALLOC => event.size.checked_mul(event.extra) == Some(64),
            REALLOC => event.size == 128 && event.extra == 0,
            ALIGNED => event.size == 128 && event.extra == 64,
            _ => false,
        };
        if !size_matches {
            return Err("persistent control request size/alignment mismatch");
        }
    }
    if events.iter().any(|e| {
        e.phase == EMPTY_PHASE
            || !(MALLOC_PHASE..=DATUM_PHASE).contains(&e.phase)
            || e.tag == MISMATCH
    }) {
        return Err("empty/unexpected phase or mismatch in persistent receipt");
    }
    Ok(())
}

fn returned_marker(event: &Event) -> bool {
    event.tag == RETURNED
        && event.phase == DATUM_PHASE
        && event.kind == 0
        && event.ordinal == 0
        && event.size == 0
        && event.extra == 0
}

fn driver() -> Check<i32> {
    println!(
        "BEGIN bounded Datum12.34 synthetic one-shot experiment; physical OOM is NOT exercised"
    );
    let (observed, before) = run_child(false, 0)?;
    if observed.code() != Some(0) {
        return Err("observer child failed; injection NOT run");
    }
    verify_controls(&before)?;
    let datum: Vec<_> = before.iter().filter(|e| e.phase == DATUM_PHASE).collect();
    if datum.len() != 3
        || datum[0].tag != REQUEST
        || datum[0].kind != MALLOC
        || datum[0].ordinal != 1
        || datum[0].extra != 0
        || !(1..=MAX_SMALL).contains(&datum[0].size)
        || !returned_marker(datum[1])
        || datum[2].tag != FINISH
        || datum[2].kind != MALLOC
        || datum[2].ordinal != 0
        || datum[2].size != datum[0].size
        || datum[2].extra != 0
    {
        return Err("observer did not prove the exact first small Datum malloc");
    }
    let expected = datum[0].size;
    println!("verified request malloc({expected}); launching fresh isolated injected child");
    let (injected, after) = run_child(true, expected)?;
    verify_controls(&after)?;
    let datum: Vec<_> = after.iter().filter(|e| e.phase == DATUM_PHASE).collect();
    if datum.len() < 2
        || datum[0].tag != REQUEST
        || datum[1].tag != HIT
        || datum[..2]
            .iter()
            .any(|e| e.kind != MALLOC || e.ordinal != 1 || e.size != expected || e.extra != 0)
    {
        return Err("wrong/no persistent one-shot hit; no RED/GREEN classification");
    }
    if injected.signal() == Some(SIGABRT) && datum.len() == 2 {
        println!("RED verified small refusal + expected allocation-abort SIGABRT; actual Datum outer Err absent");
        return Ok(1);
    }
    if injected.code() == Some(CHILD_CODEC_ERR)
        && datum.len() == 4
        && returned_marker(datum[2])
        && datum[3].tag == FINISH
        && datum[3].kind == MALLOC
        && datum[3].ordinal == 1
        && datum[3].size == expected
        && datum[3].extra == CHILD_CODEC_ERR as usize
    {
        println!("GREEN verified small refusal -> actual outer Codec InvalidDataType Err; owner unchanged");
        return Ok(0);
    }
    Err("unexpected signal/result/receipt; neither expected old RED nor proposed GREEN")
}

fn main() {
    let args: Vec<_> = std::env::args().collect();
    let is_child = args.get(1).is_some_and(|s| s.starts_with("--child-"));
    let result = if args.len() == 1 {
        driver()
    } else if args.len() == 4 && matches!(args[1].as_str(), "--child-observe" | "--child-inject") {
        match (args[2].parse::<i32>(), args[3].parse::<usize>()) {
            (Ok(fd), Ok(expected)) => child(args[1] == "--child-inject", fd, expected),
            _ => Err("invalid private child arguments"),
        }
    } else {
        Err("run the driver without arguments; child modes are internal")
    };
    MODE.store(OFF, Ordering::SeqCst);
    let code = match result {
        Ok(code) => code,
        Err(err) => {
            eprintln!("INCONCLUSIVE: {err}");
            if is_child {
                CHILD_INVALID
            } else {
                2
            }
        }
    };
    std::process::exit(code);
}
