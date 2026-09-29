/* Copyright 2026. Licensed under Apache-2.0.
 *
 * PRIVATE, UNVALIDATED SOURCE: parent must compile/run and retain the exact ELF,
 * dependencies, command, stdout/stderr and exit status. NOT a product allocator.
 *
 * Parent compile command (from expression-unification):
 *   cc -std=gnu11 -O2 -Wall -Wextra -Werror -fPIC -shared -fno-builtin \
 *      -fno-stack-protector -ftls-model=initial-exec \
 *      -U_FORTIFY_SOURCE -D_FORTIFY_SOURCE=0 -Wl,-z,now -Wl,-z,defs \
 *      -o tools/pool-arc-observer.so tools/pool-arc-observer.c
 * Inspect the resulting ELF before running: no malloc-based resolver, libatomic,
 * __tls_get_addr, printf/dlsym imports or competing allocator is allowed.
 * Apply LD_PRELOAD to the FINAL unit-test ELF ONLY, never Cargo/rustc/helpers:
 *   env LD_PRELOAD="$(pwd)/tools/pool-arc-observer.so" "$TEST_ELF" \
 *     tikv::evaluated_ascii::tests::parent_external_observer_actual_pool_owner_arc_new_fixture \
 *     --ignored --exact --nocapture --test-threads=1
 * Parent must verify the test's exact qualified name in its actual artifact.
 * Require BOTH native one-test success/exit0 AND the final observer PASS line.
 * Missing observer output is NOT zero allocations or successful instrumentation.
 *
 * Narrow target: GNU/Linux x86-64 LP64, dynamically linked glibc, inherited Rust
 * tikv_alloc::ALLOC using std::alloc::System. No other preloader/LD_AUDIT,
 * sanitizer/custom allocator, dlopen/dlclose, fork, asynchronous signal handler,
 * pthread cancellation or descriptor/buffer mutation during observation. This
 * is NOT a transparent general-purpose libc interposer: application write and
 * writev use raw Linux syscalls and do NOT supply glibc cancellation points.
 * Raw writes preserve their ordinary result/errno contract but not cancellation.
 * An unobserved thread with no intercepted activity is not enumerated/proven
 * absent. Observed foreign-thread activity and in-flight boundary overlap fail.
 *
 * Read-only target facts, NOT execution evidence: host getconf says glibc2.43.
 * /usr/lib64/libc.so.6 is ELF64 x86-64 and exports __libc_malloc/calloc/realloc/
 * free/memalign@@GLIBC_2.2.5. /usr/lib/libc.so.6 is a WRONG 32-bit compatibility
 * library. Actual target disassembly: posix_memalign[0xa2c50,0xa2ca6) validates
 * alignment and calls 0xa1610; __libc_memalign[0xa2140,0xa2149) jumps to that SAME
 * allocator body. Only successful posix allocation writes its out-pointer;
 * failure returns ENOMEM, invalid alignment EINVAL. The thin wrapper below
 * follows that observed target behavior, including the backend's errno, rather
 * than inventing a __libc_posix_memalign symbol. Re-review on a different libc.
 * Direct __libc_* references plus -z now avoid dlsym/recursive lazy resolution.
 * Before this DSO's constructor, allocation hooks simply forward, without TLS.
 * Afterward initial-exec TLS recursion guards cannot allocate on first access.
 * Nested hook activity during the protocol is INVALID, never silently omitted.
 *
 * Exact protocol: one complete fd2 write OR writev per literal line, newline
 * included; multiple iovec slices in ONE successful writev are allowed. Partial
 * syscalls, split/mixed/multiple-frame writes, wrong ordering/thread, unknown
 * marker, missing phase/control, counter/record overflow => sticky INVALID.
 * All 14 lines start "ASCII_POOL_EXTERNAL_OBSERVER ":
 *   CONTROL_PRE_BEGIN / CONTROL_PRE_END
 *   EMPTY_PRE_BEGIN / EMPTY_PRE_END
 *   OWNER_NEW_BEGIN / OWNER_NEW_END
 *   STACK_CLONES_BEGIN / STACK_CLONES_END
 *   OWNER_DROP_BEGIN / OWNER_DROP_END
 *   EMPTY_POST_BEGIN / EMPTY_POST_END
 *   CONTROL_POST_BEGIN / CONTROL_POST_END
 * Existing DIAGNOSTIC_WARMUP and candidate lines are ignored ONLY before this
 * protocol. Dynamic candidate lines may be split; they are never measurements.
 * Other split ordinary diagnostics cannot supply a marker. Boundaries require
 * zero allocator hooks in flight and only the marker's own write in flight.
 * No allocation/free may occur in the gaps between the 14 markers either.
 *
 * Parent adds SAFE Rust controls in its existing private fixture; root Rust
 * unsafe_code=forbid MUST remain. Do not suggest Rust FFI/global allocators:
 *   Vec<u8>::with_capacity(73), resize(73, nonzero), reserve_exact(149-73), drop;
 *   vec![0u8;91], black_box its slice, drop;
 *   #[repr(align(64))] struct Aligned([u8;257]); Box::new(nonzero Aligned), drop.
 * Black-box operands/results so the real inherited Rust paths execute. Each
 * positive window MUST produce exactly these seven observed public events:
 * malloc73, realloc149, matching free, calloc(product91), matching free,
 * posix_memalign(align64,size320), matching free. 320 includes actual padding;
 * it is NOT the 257-byte payload. Actual calloc factors are retained, not forced
 * to 7x13. A different actual route is INVALID, never silently substituted.
 * Both empty windows and the clone window must have zero allocator/free events.
 * OWNER_NEW must contain exactly one successful nonzero malloc/calloc/posix
 * request, OWNER_DROP exactly its matching free, and no replacement in gaps.
 * No candidate/proxy/Core size is parsed, hardcoded, allocated or compared here.
 *
 * Records are fixed static storage; hook tickets and report buffers are fixed
 * stack objects. There is no heap, stdio, resolver, callback or refusal policy
 * in the observer. All allocation requests are forwarded to real glibc even
 * after INVALID. Reports describe REQUESTED bytes and actual pointer identity,
 * never usable bytes, allocator internals, a peak or whole-process heap. Malloc,
 * calloc and realloc have NO explicit alignment argument: alignment is reported
 * UNSPECIFIED, with separate observed pointer residues (not Rust Layout proof).
 * Event status is the real posix return code; for malloc/calloc/realloc it is
 * only 0=nonnull or 1=NULL (not an invented errno, including zero-size calls).
 * The separately captured backend_errno may be stale after successful calls.
 *
 * The final destructor prints records with raw syscalls. INVALID/report failure
 * ends the process with code86; PASS does not override the inherited exit code.
 */
#define _GNU_SOURCE 1
#include <errno.h>
#include <limits.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/syscall.h>
#include <sys/uio.h>
#include <unistd.h>

#if !defined(__linux__) || !defined(__x86_64__) || !defined(__GLIBC__)
#error "This private observer requires GNU/Linux x86-64 glibc"
#endif
_Static_assert(sizeof(void *) == 8 && sizeof(size_t) == 8 && sizeof(long) == 8,
               "LP64 required");
_Static_assert(ATOMIC_LONG_LOCK_FREE == 2 && ATOMIC_INT_LOCK_FREE == 2,
               "No out-of-line/allocating atomic fallback allowed");

extern void *__libc_malloc(size_t);
extern void *__libc_calloc(size_t, size_t);
extern void *__libc_realloc(void *, size_t);
extern void __libc_free(void *);
extern void *__libc_memalign(size_t, size_t);

#define MAX_EVENTS 64u
#define MAX_LINE 1024u
#define PHASES 7u
#define MARKERS (PHASES * 2u)
#define NONE UINT_MAX
#define PREFIX "ASCII_POOL_EXTERNAL_OBSERVER "

enum route { R_MALLOC, R_CALLOC, R_REALLOC, R_POSIX, R_FREE };
enum fault {
    F_REENTRY = 1ul << 0, F_OVERFLOW = 1ul << 1, F_FOREIGN = 1ul << 2,
    F_BOUNDARY = 1ul << 3, F_GAP = 1ul << 4, F_WRITE = 1ul << 5,
    F_SPLIT = 1ul << 6, F_MARKER = 1ul << 7, F_ORDER = 1ul << 8,
    F_MISSING = 1ul << 9, F_CONTROL = 1ul << 10, F_OWNER = 1ul << 11,
    F_REPORT = 1ul << 12, F_PID = 1ul << 13, F_LATE = 1ul << 14
};
struct event {
    enum route route;
    unsigned phase;
    long tid;
    size_t requested, a, b, alignment;
    uintptr_t old_ptr, new_ptr;
    int status, allocator_errno;
    bool size_known;
};
struct ticket {
    bool entered, outer, counted, writing, inside;
    unsigned phase, marker;
    long tid;
};
struct state {
    bool scope, reported;
    unsigned phase, next_marker, alloc_flight, write_flight;
    long marker_tid, initial_pid;
    unsigned count, begin[PHASES], end[PHASES];
    unsigned gap_events, foreign_observations;
    long first_foreign_tid;
    struct event events[MAX_EVENTS];
    char line[MAX_LINE];
    size_t line_len, line_call_bytes;
    unsigned line_calls;
    long line_tid;
    bool discard_line;
};
static struct state g = { .phase = NONE };
static atomic_flag gate = ATOMIC_FLAG_INIT;
static _Atomic unsigned armed;
static _Atomic unsigned scope_visible;
static _Atomic unsigned bootstrap_flight;
static _Atomic unsigned entry_flight;
static _Atomic unsigned marker_epoch;
static _Atomic unsigned long faults;
static __thread unsigned hook_depth __attribute__((tls_model("initial-exec")));
static const char *const marker_names[MARKERS] = {
    PREFIX "CONTROL_PRE_BEGIN\n", PREFIX "CONTROL_PRE_END\n",
    PREFIX "EMPTY_PRE_BEGIN\n", PREFIX "EMPTY_PRE_END\n",
    PREFIX "OWNER_NEW_BEGIN\n", PREFIX "OWNER_NEW_END\n",
    PREFIX "STACK_CLONES_BEGIN\n", PREFIX "STACK_CLONES_END\n",
    PREFIX "OWNER_DROP_BEGIN\n", PREFIX "OWNER_DROP_END\n",
    PREFIX "EMPTY_POST_BEGIN\n", PREFIX "EMPTY_POST_END\n",
    PREFIX "CONTROL_POST_BEGIN\n", PREFIX "CONTROL_POST_END\n"
};
static const char *const phase_names[PHASES] = {
    "CONTROL_PRE", "EMPTY_PRE", "OWNER_NEW", "STACK_CLONES",
    "OWNER_DROP", "EMPTY_POST", "CONTROL_POST"
};
static const char *const route_names[] = {
    "malloc", "calloc", "realloc", "posix_memalign", "free"
};

static long raw3(long nr, long a, long b, long c) {
    long result;
    __asm__ volatile("syscall" : "=a"(result)
                     : "a"(nr), "D"(a), "S"(b), "d"(c)
                     : "rcx", "r11", "memory");
    return result;
}
static void bad(unsigned long bits) {
    atomic_fetch_or_explicit(&faults, bits, memory_order_relaxed);
}
/* Caller holds gate; counts observations, not distinct threads or hooks. */
static void note_foreign(long tid) {
    bad(F_FOREIGN);
    if (g.foreign_observations == UINT_MAX) bad(F_OVERFLOW);
    else ++g.foreign_observations;
    if (g.first_foreign_tid == 0) g.first_foreign_tid = tid;
}
static void lock_gate(void) {
    while (atomic_flag_test_and_set_explicit(&gate, memory_order_acquire))
        __asm__ volatile("pause");
}
static void unlock_gate(void) {
    atomic_flag_clear_explicit(&gate, memory_order_release);
}
static size_t text_len(const char *s) {
    size_t n = 0;
    while (s[n] != '\0') ++n;
    return n;
}
static bool starts(const char *s, size_t n, const char *prefix) {
    size_t m = text_len(prefix);
    if (n < m) return false;
    for (size_t i = 0; i < m; ++i) if (s[i] != prefix[i]) return false;
    return true;
}
static bool equal_text(const char *s, size_t n, const char *text) {
    return n == text_len(text) && starts(s, n, text);
}
static bool contains_root(const char *s, size_t n) {
    for (size_t i = 0; i < n; ++i)
        if (starts(s + i, n - i, "ASCII_POOL_")) return true;
    return false;
}
static bool unfinished_marker(const char *s, size_t n) {
    static const char root[] = "ASCII_POOL_";
    if (contains_root(s, n)) return true;
    if (n == 0 || n >= sizeof(root)-1) return false;
    for (size_t i = 0; i < n; ++i) if (s[i] != root[i]) return false;
    return true;
}
static bool raw_output(const char *s, size_t n) {
    while (n != 0) {
        long r = raw3(SYS_write, 2, (long)(uintptr_t)s, (long)n);
        if (r == -EINTR) continue;
        if (r <= 0 || (unsigned long)r > n) return false;
        s += (size_t)r;
        n -= (size_t)r;
    }
    return true;
}
static void exit_invalid(void) __attribute__((noreturn));
static void exit_invalid(void) {
    (void)raw3(SYS_exit_group, 86, 0, 0);
    __builtin_unreachable();
}

/* Pre-constructor accounting uses only lock-free atomics, never TLS/errno.
 * A bootstrap request straddling any marker invalidates, even if it completes
 * after the whole protocol. Marker boundaries also inspect bootstrap_flight.
 */
struct bootstrap_ticket { unsigned epoch; bool counted, inside; };
static struct bootstrap_ticket bootstrap_enter(void) {
    struct bootstrap_ticket t = {0};
    unsigned n = atomic_load_explicit(&bootstrap_flight, memory_order_relaxed);
    for (;;) {
        if (n == UINT_MAX) { bad(F_OVERFLOW); break; }
        if (atomic_compare_exchange_weak_explicit(&bootstrap_flight, &n, n+1,
                memory_order_acq_rel, memory_order_relaxed)) {
            t.counted = true;
            break;
        }
    }
    t.epoch = atomic_load_explicit(&marker_epoch, memory_order_acquire);
    t.inside = atomic_load_explicit(&scope_visible, memory_order_acquire) != 0;
    return t;
}
static void bootstrap_leave(struct bootstrap_ticket t) {
    if (t.inside || atomic_load_explicit(&scope_visible, memory_order_acquire) ||
        t.epoch != atomic_load_explicit(&marker_epoch, memory_order_acquire))
        bad(F_BOUNDARY);
    if (t.counted)
        atomic_fetch_sub_explicit(&bootstrap_flight, 1, memory_order_release);
}

/* No lock is held across a real allocator or an application write syscall. */
static struct ticket enter_hook(bool writing) {
    struct ticket t = {0};
    if (!atomic_load_explicit(&armed, memory_order_acquire)) return t;
    t.entered = true;
    if (hook_depth++ != 0) {
        if (atomic_load_explicit(&scope_visible, memory_order_acquire))
            bad(F_REENTRY);
        return t;
    }
    t.outer = true;
    t.writing = writing;
    /* Observe admission BEFORE waiting for gate. A foreign hook queued behind
     * the final marker cannot be relabeled as an outside-protocol operation.
     */
    unsigned waiting = atomic_load_explicit(&entry_flight, memory_order_relaxed);
    bool queued = false;
    for (;;) {
        if (waiting == UINT_MAX) { bad(F_OVERFLOW); break; }
        if (atomic_compare_exchange_weak_explicit(&entry_flight, &waiting, waiting+1,
                memory_order_acq_rel, memory_order_relaxed)) {
            queued = true;
            break;
        }
    }
    unsigned entry_epoch = atomic_load_explicit(&marker_epoch, memory_order_acquire);
    bool entry_inside = atomic_load_explicit(&scope_visible, memory_order_acquire) != 0;
    t.tid = raw3(SYS_gettid, 0, 0, 0);
    lock_gate();
    if (entry_epoch != g.next_marker || entry_inside != g.scope) bad(F_BOUNDARY);
    t.inside = entry_inside || g.scope;
    t.phase = g.phase;
    t.marker = g.next_marker;
    unsigned *flight = writing ? &g.write_flight : &g.alloc_flight;
    if (*flight == UINT_MAX) bad(F_OVERFLOW);
    else { ++*flight; t.counted = true; }
    if (t.inside && t.tid != g.marker_tid) note_foreign(t.tid);
    if (queued) atomic_fetch_sub_explicit(&entry_flight, 1, memory_order_release);
    unlock_gate();
    return t;
}
static void leave_alloc(struct ticket t, struct event e) {
    if (!t.entered) return;
    if (t.outer) {
        lock_gate();
        if (t.counted) --g.alloc_flight;
        if (t.inside || g.scope) {
            if (!t.inside || !g.scope || t.phase != g.phase ||
                t.marker != g.next_marker) bad(F_BOUNDARY);
            if (t.tid != g.marker_tid) note_foreign(t.tid);
            else {
                if (g.phase == NONE) {
                    bad(F_GAP);
                    if (g.gap_events == UINT_MAX) bad(F_OVERFLOW);
                    else ++g.gap_events;
                }
                e.phase = g.phase;
                e.tid = t.tid;
                if (!e.size_known) bad(F_OVERFLOW);
                if (g.count == MAX_EVENTS) bad(F_OVERFLOW);
                else g.events[g.count++] = e;
            }
        }
        unlock_gate();
    }
    --hook_depth;
}

void *malloc(size_t n) {
    if (!atomic_load_explicit(&armed, memory_order_acquire)) {
        struct bootstrap_ticket t = bootstrap_enter();
        void *p = __libc_malloc(n);
        bootstrap_leave(t);
        return p;
    }
    struct ticket t = enter_hook(false);
    void *p = __libc_malloc(n);
    int saved = errno;
    struct event e = { .route=R_MALLOC, .requested=n, .a=n,
        .new_ptr=(uintptr_t)p, .status=p ? 0 : 1,
        .allocator_errno=saved, .size_known=true };
    leave_alloc(t, e);
    errno = saved;
    return p;
}
void *calloc(size_t a, size_t b) {
    if (!atomic_load_explicit(&armed, memory_order_acquire)) {
        struct bootstrap_ticket t = bootstrap_enter();
        void *p = __libc_calloc(a, b);
        bootstrap_leave(t);
        return p;
    }
    struct ticket t = enter_hook(false);
    void *p = __libc_calloc(a, b);
    int saved = errno;
    bool known = b == 0 || a <= SIZE_MAX / b;
    struct event e = { .route=R_CALLOC, .requested=known ? a*b : 0,
        .a=a, .b=b, .new_ptr=(uintptr_t)p, .status=p ? 0 : 1,
        .allocator_errno=saved, .size_known=known };
    leave_alloc(t, e);
    errno = saved;
    return p;
}
void *realloc(void *old, size_t n) {
    if (!atomic_load_explicit(&armed, memory_order_acquire)) {
        struct bootstrap_ticket t = bootstrap_enter();
        void *p = __libc_realloc(old, n);
        bootstrap_leave(t);
        return p;
    }
    uintptr_t previous = (uintptr_t)old;
    struct ticket t = enter_hook(false);
    void *p = __libc_realloc(old, n);
    int saved = errno;
    struct event e = { .route=R_REALLOC, .requested=n, .a=n,
        .old_ptr=previous, .new_ptr=(uintptr_t)p, .status=p ? 0 : 1,
        .allocator_errno=saved, .size_known=true };
    leave_alloc(t, e);
    errno = saved;
    return p;
}
void free(void *p) {
    if (!atomic_load_explicit(&armed, memory_order_acquire)) {
        struct bootstrap_ticket t = bootstrap_enter();
        __libc_free(p);
        bootstrap_leave(t);
        return;
    }
    uintptr_t previous = (uintptr_t)p;
    struct ticket t = enter_hook(false);
    __libc_free(p);
    int saved = errno;
    struct event e = { .route=R_FREE, .old_ptr=previous,
        .allocator_errno=saved, .size_known=true };
    leave_alloc(t, e);
    errno = saved;
}
static int forward_posix(void **out, size_t alignment, size_t n) {
    if (alignment == 0 || alignment % sizeof(void *) != 0 ||
        (alignment & (alignment - 1)) != 0) return EINVAL;
    void *p = __libc_memalign(alignment, n);
    if (p == NULL) return ENOMEM;
    *out = p;
    return 0;
}
int posix_memalign(void **out, size_t alignment, size_t n) {
    if (!atomic_load_explicit(&armed, memory_order_acquire)) {
        struct bootstrap_ticket t = bootstrap_enter();
        int result = forward_posix(out, alignment, n);
        bootstrap_leave(t);
        return result;
    }
    struct ticket t = enter_hook(false);
    int result = forward_posix(out, alignment, n);
    int saved = errno;
    void *p = result == 0 ? *out : NULL;
    struct event e = { .route=R_POSIX, .requested=n, .a=alignment, .b=n,
        .alignment=alignment, .new_ptr=(uintptr_t)p, .status=result,
        .allocator_errno=saved, .size_known=true };
    leave_alloc(t, e);
    errno = saved;
    return result;
}

/* Called under gate, after the application syscall actually wrote the line. */
static void complete_line(void) {
    bool diagnostic = starts(g.line, g.line_len, PREFIX "DIAGNOSTIC_WARMUP ") ||
                      starts(g.line, g.line_len, PREFIX "candidate ");
    if (diagnostic && g.next_marker == 0 && !g.scope) return;
    if (!contains_root(g.line, g.line_len)) return;
    if (g.reported) {
        bad(F_LATE);
        static const char message[] = "ASCII_POOL_OBSERVER INVALID late-marker\n";
        (void)raw_output(message, sizeof(message)-1);
        exit_invalid();
    }
    if (g.line_calls != 1 || g.line_call_bytes != g.line_len) bad(F_SPLIT);
    unsigned id;
    for (id = 0; id < MARKERS; ++id)
        if (equal_text(g.line, g.line_len, marker_names[id])) break;
    if (id == MARKERS) { bad(F_MARKER); return; }
    if (id != g.next_marker) { bad(F_ORDER); return; }
    if (raw3(SYS_getpid, 0, 0, 0) != g.initial_pid) bad(F_PID);
    if (g.alloc_flight != 0 || g.write_flight != 1 ||
        atomic_load_explicit(&bootstrap_flight, memory_order_acquire) != 0 ||
        atomic_load_explicit(&entry_flight, memory_order_acquire) != 0)
        bad(F_BOUNDARY);
    if (id == 0) g.marker_tid = g.line_tid;
    if (g.line_tid != g.marker_tid || g.line_tid <= 0) note_foreign(g.line_tid);
    unsigned phase = id / 2;
    if ((id & 1u) == 0) {
        if (g.phase != NONE) bad(F_ORDER);
        g.scope = true;
        g.phase = phase;
        g.begin[phase] = g.count;
        atomic_store_explicit(&scope_visible, 1, memory_order_release);
    } else {
        if (!g.scope || g.phase != phase) bad(F_ORDER);
        g.end[phase] = g.count;
        g.phase = NONE;
        if (id == MARKERS-1) {
            g.scope = false;
            atomic_store_explicit(&scope_visible, 0, memory_order_release);
        }
    }
    ++g.next_marker;
    atomic_store_explicit(&marker_epoch, g.next_marker, memory_order_release);
}
static void feed_bytes(const char *p, size_t n, size_t call_bytes, long tid) {
    for (size_t i = 0; i < n; ++i) {
        char c = p[i];
        if (g.discard_line) {
            if (c == '\n') g.discard_line = false;
            continue;
        }
        if (g.line_len == 0) {
            g.line_tid = tid;
            g.line_call_bytes = call_bytes;
            g.line_calls = 1;
        } else if (g.line_tid != tid) bad(F_FOREIGN | F_SPLIT);
        if (g.line_len == MAX_LINE) {
            bad(F_OVERFLOW);
            g.line_len = 0;
            g.discard_line = c != '\n';
            continue;
        }
        g.line[g.line_len++] = c;
        if (c == '\n') {
            complete_line();
            g.line_len = 0;
            g.line_calls = 0;
        }
    }
}
static void finish_write(struct ticket t, int fd, const struct iovec *iov,
                         int count, long result) {
    if (!t.entered) return;
    if (t.outer) {
        lock_gate();
        if ((t.inside || g.scope) && t.tid != g.marker_tid) note_foreign(t.tid);
        if (fd == 2) {
            /* Never inspect buffers after a failed syscall (e.g. EFAULT). */
            if (result < 0) bad(F_WRITE);
            else {
                size_t total = 0;
                bool valid = count >= 0 && count <= 1024;
                for (int i = 0; valid && i < count; ++i) {
                    if (iov[i].iov_len > SIZE_MAX - total) valid = false;
                    else total += iov[i].iov_len;
                }
                if (!valid) bad(F_OVERFLOW | F_WRITE);
                else {
                    if ((unsigned long)result != total) bad(F_WRITE);
                    if (g.line_len != 0) {
                        if (g.line_calls == UINT_MAX) bad(F_OVERFLOW);
                        else ++g.line_calls;
                    }
                    size_t remaining = (size_t)result;
                    for (int i = 0; i < count && remaining != 0; ++i) {
                        size_t n = iov[i].iov_len < remaining ? iov[i].iov_len : remaining;
                        feed_bytes(iov[i].iov_base, n, total, t.tid);
                        remaining -= n;
                    }
                    if (remaining != 0) bad(F_WRITE);
                }
            }
        }
        /* No second destructor will inspect a late incomplete reserved frame.
         * Reject it at this syscall boundary, without waiting for a newline.
         */
        if (g.reported && fd == 2 && unfinished_marker(g.line, g.line_len))
            bad(F_LATE | F_SPLIT);
        if (g.reported && atomic_load_explicit(&faults, memory_order_relaxed) != 0) {
            static const char message[] = "ASCII_POOL_OBSERVER INVALID late-write\n";
            (void)raw_output(message, sizeof(message)-1);
            exit_invalid();
        }
        if (t.counted) --g.write_flight;
        unlock_gate();
    }
    --hook_depth;
}
ssize_t write(int fd, const void *p, size_t n) {
    if (!atomic_load_explicit(&armed, memory_order_acquire)) {
        struct bootstrap_ticket t = bootstrap_enter();
        long r = raw3(SYS_write, fd, (long)(uintptr_t)p, (long)n);
        bootstrap_leave(t);
        if (r < 0) { errno = (int)-r; return -1; }
        return (ssize_t)r;
    }
    struct ticket t = enter_hook(true);
    long r = raw3(SYS_write, fd, (long)(uintptr_t)p, (long)n);
    int saved = r < 0 ? (int)-r : errno;
    struct iovec iov = { .iov_base=(void *)p, .iov_len=n };
    finish_write(t, fd, &iov, 1, r);
    errno = saved;
    return r < 0 ? -1 : (ssize_t)r;
}
ssize_t writev(int fd, const struct iovec *iov, int count) {
    if (!atomic_load_explicit(&armed, memory_order_acquire)) {
        struct bootstrap_ticket t = bootstrap_enter();
        long r = raw3(SYS_writev, fd, (long)(uintptr_t)iov, count);
        bootstrap_leave(t);
        if (r < 0) { errno = (int)-r; return -1; }
        return (ssize_t)r;
    }
    struct ticket t = enter_hook(true);
    long r = raw3(SYS_writev, fd, (long)(uintptr_t)iov, count);
    int saved = r < 0 ? (int)-r : errno;
    finish_write(t, fd, iov, count, r);
    errno = saved;
    return r < 0 ? -1 : (ssize_t)r;
}

static bool allocation(const struct event *e, enum route route, size_t n) {
    return e->route == route && e->size_known && e->requested == n &&
           e->new_ptr != 0 && e->status == 0;
}
static bool released(const struct event *e, uintptr_t p) {
    return e->route == R_FREE && p != 0 && e->old_ptr == p;
}
static bool positive_control(unsigned phase) {
    unsigned b = g.begin[phase], e = g.end[phase];
    if (e < b || e-b != 7 || e > g.count) return false;
    const struct event *v = &g.events[b];
    return allocation(&v[0], R_MALLOC, 73) &&
           allocation(&v[1], R_REALLOC, 149) && v[1].old_ptr == v[0].new_ptr &&
           released(&v[2], v[1].new_ptr) &&
           allocation(&v[3], R_CALLOC, 91) &&
           released(&v[4], v[3].new_ptr) &&
           allocation(&v[5], R_POSIX, 320) && v[5].alignment == 64 &&
           (v[5].new_ptr & 63u) == 0 && released(&v[6], v[5].new_ptr);
}
static bool empty_phase(unsigned phase) {
    return g.begin[phase] == g.end[phase];
}
static bool owner_pair(struct event *owner) {
    if (g.end[2] < g.begin[2] || g.end[2]-g.begin[2] != 1 ||
        g.end[4] < g.begin[4] || g.end[4]-g.begin[4] != 1 ||
        g.end[2] > g.count || g.end[4] > g.count) return false;
    const struct event *e = &g.events[g.begin[2]];
    if ((e->route != R_MALLOC && e->route != R_CALLOC && e->route != R_POSIX) ||
        !e->size_known || e->requested == 0 || e->new_ptr == 0 || e->status != 0)
        return false;
    if (e->route == R_POSIX &&
        (e->alignment == 0 || e->new_ptr % e->alignment != 0)) return false;
    if (!released(&g.events[g.begin[4]], e->new_ptr)) return false;
    *owner = *e;
    return true;
}

/* Small stack-only formatter. No snprintf, printf, streams or locale. */
struct output { char bytes[768]; size_t n; bool okay; };
static void put_char(struct output *o, char c) {
    if (o->n == sizeof(o->bytes)) o->okay = false;
    else o->bytes[o->n++] = c;
}
static void put_text(struct output *o, const char *s) {
    while (*s) put_char(o, *s++);
}
static void put_number(struct output *o, uint64_t n, unsigned base) {
    char reversed[32];
    size_t count = 0;
    do {
        unsigned digit = (unsigned)(n % base);
        reversed[count++] = "0123456789abcdef"[digit];
        n /= base;
    } while (n != 0);
    while (count != 0) put_char(o, reversed[--count]);
}
static void decimal_field(struct output *o, const char *name, uint64_t n) {
    put_text(o, name); put_number(o, n, 10);
}
static void pointer_field(struct output *o, const char *name, uintptr_t p) {
    put_text(o, name); put_text(o, "0x"); put_number(o, p, 16);
}
static void flush(struct output *o) {
    put_char(o, '\n');
    if (!o->okay || !raw_output(o->bytes, o->n)) bad(F_REPORT);
}

__attribute__((constructor)) static void observer_start(void) {
    g.initial_pid = raw3(SYS_getpid, 0, 0, 0);
    static const char banner[] =
        "ASCII_POOL_OBSERVER loaded=v1 target=linux-x86_64-glibc request-only controls=SAFE_RUST\n";
    if (!raw_output(banner, sizeof(banner)-1)) bad(F_REPORT);
    atomic_store_explicit(&armed, 1, memory_order_release);
}
__attribute__((destructor(101))) static void observer_finish(void) {
    ++hook_depth;
    struct event owner = {0};
    struct event records[MAX_EVENTS];
    unsigned count, marker_count, phase_counts[PHASES], gaps, foreign;
    long marker_tid, foreign_tid;
    lock_gate();
    if (g.next_marker != MARKERS || g.scope || g.phase != NONE) bad(F_MISSING);
    if (g.alloc_flight != 0 || g.write_flight != 0 ||
        atomic_load_explicit(&bootstrap_flight, memory_order_acquire) != 0 ||
        atomic_load_explicit(&entry_flight, memory_order_acquire) != 0)
        bad(F_BOUNDARY);
    if (g.discard_line || unfinished_marker(g.line, g.line_len)) bad(F_SPLIT);
    if (!positive_control(0) || !positive_control(6) ||
        !empty_phase(1) || !empty_phase(5)) bad(F_CONTROL);
    if (!empty_phase(3) || !owner_pair(&owner)) bad(F_OWNER);
    count = g.count;
    marker_count = g.next_marker;
    marker_tid = g.marker_tid;
    gaps = g.gap_events;
    foreign = g.foreign_observations;
    foreign_tid = g.first_foreign_tid;
    for (unsigned i = 0; i < PHASES; ++i) {
        if (g.end[i] < g.begin[i]) {
            bad(F_ORDER);
            phase_counts[i] = 0;
        } else phase_counts[i] = g.end[i] - g.begin[i];
    }
    for (unsigned i = 0; i < count; ++i) records[i] = g.events[i];
    g.reported = true;
    g.scope = false;
    atomic_store_explicit(&scope_visible, 0, memory_order_release);
    unlock_gate();
    for (unsigned i = 0; i < PHASES; ++i) {
        struct output o = { .okay=true };
        put_text(&o, "ASCII_POOL_OBSERVER window phase=");
        put_text(&o, phase_names[i]);
        decimal_field(&o, " events=", phase_counts[i]);
        decimal_field(&o, " complete=", marker_count > i*2+1);
        flush(&o);
    }
    for (unsigned i = 0; i < count; ++i) {
        const struct event *e = &records[i];
        struct output o = { .okay=true };
        put_text(&o, "ASCII_POOL_OBSERVER event");
        decimal_field(&o, " index=", i);
        put_text(&o, " phase=");
        put_text(&o, e->phase < PHASES ? phase_names[e->phase] : "GAP_INVALID");
        put_text(&o, " route="); put_text(&o, route_names[e->route]);
        decimal_field(&o, " tid=", (uint64_t)e->tid);
        decimal_field(&o, " requested_bytes=", e->requested);
        decimal_field(&o, " size_known=", e->size_known);
        decimal_field(&o, " arg_a=", e->a); decimal_field(&o, " arg_b=", e->b);
        put_text(&o, " alignment_arg=");
        if (e->route == R_POSIX) put_number(&o, e->alignment, 10);
        else put_text(&o, "UNSPECIFIED");
        pointer_field(&o, " old=", e->old_ptr); pointer_field(&o, " new=", e->new_ptr);
        decimal_field(&o, " ptr_mod8=", e->new_ptr & 7u);
        decimal_field(&o, " ptr_mod16=", e->new_ptr & 15u);
        decimal_field(&o, " status=", (uint64_t)(unsigned)e->status);
        decimal_field(&o, " backend_errno=", (uint64_t)(unsigned)e->allocator_errno);
        flush(&o);
    }
    struct output o = { .okay=true };
    unsigned long errors = atomic_load_explicit(&faults, memory_order_relaxed);
    put_text(&o, "ASCII_POOL_OBSERVER "); put_text(&o, errors ? "INVALID" : "PASS");
    decimal_field(&o, " faults=", errors);
    decimal_field(&o, " markers=", marker_count);
    decimal_field(&o, " events=", count);
    decimal_field(&o, " marker_tid=", (uint64_t)marker_tid);
    decimal_field(&o, " gap_events=", gaps);
    decimal_field(&o, " foreign_observations=", foreign);
    decimal_field(&o, " first_foreign_tid=", (uint64_t)foreign_tid);
    decimal_field(&o, " actual_owner_requested_bytes=", owner.requested);
    put_text(&o, " owner_alignment_arg=");
    if (owner.route == R_POSIX) put_number(&o, owner.alignment, 10);
    else put_text(&o, "UNSPECIFIED");
    pointer_field(&o, " owner_ptr=", owner.new_ptr);
    put_text(&o, " matching_final_free="); put_text(&o, errors ? "UNPROVEN" : "true");
    put_text(&o, " extent=request-not-usable-not-peak-not-general-heap");
    flush(&o);
    if (atomic_load_explicit(&faults, memory_order_relaxed) != 0) exit_invalid();
    --hook_depth;
}
