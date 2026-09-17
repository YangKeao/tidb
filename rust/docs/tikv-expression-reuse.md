# TiDB Rust → existing TiKV expressions: copying PoC

## What this branch implements

TiDB Rust projection can opt into **in-process execution by TiKV's existing RPN
engine**. No SQL builtin was copied/reimplemented, and no server or RPC is
involved. The boundary copies selected TiDB `Chunk` values into nullable owned
columns, constructs TiKV decoded vectors, runs `RpnExpression::eval_decoded`, and
copies results back into a TiDB output chunk.

- TiDB baseline: `ceaaa790da06562dbaa0aff48a7fd6914b6375f7` (`hparser-integration`).
- TiKV baseline: `51b411a728f7c5b12f919fd4dac00a664145b751`.
- TiKV implementation: [`ff315c7d001573166595702af6a36b6f69d2f93e`](https://github.com/YangKeao/tikv/commit/ff315c7d001573166595702af6a36b6f69d2f93e).
- TiDB branch: [`YangKeao/tidb:feat/tikv-expression-reuse-poc`](https://github.com/YangKeao/tidb/tree/feat/tikv-expression-reuse-poc).
- Only YangKeao forks are publication targets. Upstream push URLs were disabled
  in the isolated development clones.

### Enabling it

Both build-time and execution-time opt-in are required. `tikv-expr` is an
optional Cargo feature on `tidb-expr` and `tidb-executor`; default builds do not
compile the engine, and feature-enabled builds still execute natively unless
the caller opts in:

```rust,ignore
use tidb_executor::{run_select_on, StmtContext};

let context = StmtContext::for_query().with_tikv_expression(true);
let result = run_select_on("SELECT a+b, LENGTH(s) FROM t", &catalog, &context)?;
assert!(context.tikv_expression_rows() > 0); // actual successful engine rows
```

This exposes an executor API, **not a new SQL session variable or server CLI
flag**. The counter counts expression-rows, not distinct SQL rows; cloned
statement contexts share it, independent contexts do not.

### Bounded admission and semantics

The initial automatic subset is deliberately small:

- Signed integer and DOUBLE homogeneous `+`, `-`, `*`, `ABS`.
- Homogeneous signed integer / DOUBLE `=`, `!=`, `<`, `<=`, `>`, `>=`, `<=>`.
- `LENGTH` / `OCTET_LENGTH` for byte-preserving string columns.
- Typed static literals, NULLs, and referenced input columns.

Unsupported expressions remain native **before evaluation**. CASE/IF and other
lazy control flow, session/side-effect functions, parameters, correlated or
deferred values, unsigned arithmetic, mixed-domain casts, division, temporal,
JSON, vector and Decimal expressions are not automatically admitted. Nonfinite
DOUBLE inputs also remain native before executing the engine. Runtime errors
are propagated with TiKV's code/message; they are never retried natively.
Diagnostic text is not normalized to TiDB's native formatter: for example TiKV
may print operand values instead of symbolic column expressions, and `REAL`
instead of `DOUBLE` for floating overflow. Tests preserve MySQL code/SQLSTATE
but do not claim byte-for-byte error-message parity.

Decimal conversion exists and is tested in TiKV's standalone API, but arbitrary
TiDB intermediate decimals can retain hidden fractional digits independently of
presentation scale. A string bridge cannot preserve that whole state, so the
TiDB dispatcher explicitly leaves Decimal native. This is not full SQL parity.

Only expression/schema metadata is protobuf-serialized, once at compilation;
row batches are not serialized. The local signature mapper preserves each
node's inferred FieldType without expanding distributed pushdown policy. It
bridges TiDB's prost representation and TiKV's rust-protobuf representation.

Only referenced columns are converted. Selection order, duplicates, and NULLs
are preserved. Large inputs split at TiKV's internal 1024-row batch boundary;
empty inputs do not invoke the evaluator. Tests include invalid UTF-8 and NUL
bytes, so byte strings are not inadvertently converted to C strings or UTF-8.

The context includes full SQL-mode bits, pushdown flags, session timezone,
division precision and warning limit. Warning details are forwarded; the TiKV
facade separately reports total occurrences across its internal batches.
Automatic division/cast admission is deliberately deferred; comprehensive
warning-count/session-diagnostic parity is not claimed by this PoC.

Mutable RPN state is cached under the execution-local `EvaluatorSuite`, not the
shared `Arc<EvaluatorProgram>`. Settings changes invalidate the cache. The
existing parallel projection dispatcher creates a new suite per task/chunk, so
it can still pay compilation per dispatched chunk; the steady-state benchmark
below does not hide that limitation by claiming end-to-end SQL speedups.

## Build and reproduce

Run TiDB commands from this checkout's `rust/` directory. The manifest pins the
published YangKeao/TiKV commit; no sibling checkout or local path patch is needed.

```sh
rustup toolchain install nightly-2026-08-22 --profile minimal --component rustfmt,clippy
export RUSTUP_TOOLCHAIN=nightly-2026-08-22
# Avoid multiplying TiDB's configured frontend threads by Cargo parallel jobs.
export RUSTFLAGS=''
# Needed on this host's CMake 4 / GCC 16 for legacy gRPC/abseil dependencies:
export CMAKE_POLICY_VERSION_MINIMUM=3.5
export CXXFLAGS='-include cstdint -std=c++17'

cargo test -p tidb-expr --features tikv-expr --lib --test all -j 12 --locked -- --test-threads=2
cargo test -p tidb-executor --features tikv-expr --test all -j 12 --locked -- --test-threads=2
cargo test -p tidb-expr --lib --test all -j 12 --locked -- --test-threads=2
cargo test -p tidb-executor --lib external_expression_error_preserves_code_state_and_message -j 12 --locked
cargo check -p tidb-exec --features tidb-executor/tikv-expr --lib -j 12 --locked --message-format short
cargo clippy -p tidb-expr -p tidb-executor --features tikv-expr --lib --tests --benches --no-deps -j 12 --locked --message-format short
cargo bench -p tidb-expr --features tikv-expr --bench tikv_expression -j 12 --locked
```

The measured runs built the benchmark separately with `--no-run`, then pinned
its printed executable to CPU 2 after other builds had completed:

```sh
cargo bench -p tidb-expr --features tikv-expr --bench tikv_expression -j 12 --locked --no-run --message-format short
taskset -c 2 <printed-benchmark-binary> > benchmark.csv 2> setup.log
TIKV_BENCH_MS=120 TIKV_BENCH_PASSES=7 taskset -c 2 <printed-benchmark-binary> > benchmark-repeat.csv 2> setup-repeat.log
```

The first feature-enabled test command also refreshed Cargo.lock after the
explicit revision change; all subsequent final build/check commands used
`--locked`. The host used isolated Cargo/Rustup homes and a shared TiDB target
directory outside the checkout. These paths are not required by the code.

For just the new tests, add the filter `tikv_adapter` to the expression `--test
all` command, or `tikv_expression` to the executor command. TiKV validation is
run from its own checkout at the pinned commit, with the same environment:

```sh
cargo test -p tidb_query_expr --lib -j 2 --message-format short -- --test-threads=2
```

Native C/C++ prerequisites (CMake, compiler, OpenSSL development files, etc.)
remain necessary. This PoC embeds the existing dependency graph rather than
extracting a slim engine: it still builds gRPC/native utility dependencies even
though evaluation itself does not perform network IO. Cargo dependency
workspaces do not export their lockfiles or root patches; protocol revisions
and compatible protobuf/raft patches are pinned explicitly. The combined lock
currently follows TiKV's existing exact flate2 1.0.11 dependency, including for
feature-disabled resolution; this is a compatibility cost of the first PoC.

## Validation evidence

Validation uses the published TiKV git revision, not only a development path.

| Scope | Result |
| --- | --- |
| TiKV complete expression library | 420 passed; includes 12 standalone suites |
| TiDB expression library, feature on | 1180 passed, 99 existing ignored |
| TiDB expression integration, feature on | 25 passed; includes 7 adapter suites |
| TiDB executor integration, feature on | 340 passed, 184 existing ignored; includes 11 SQL/adapter suites |
| TiDB feature-disabled regressions | 1180 library + 18 integration passed, 99 existing ignored |
| MySQL external-error mapper | 1 passed; preserves code, SQLSTATE and message |
| `tidb-exec` cluster/session library with feature enabled | `cargo check` passed |
| `make lint` from TiDB repository root | Passed |
| Affected Rust libraries/tests/benches, Clippy | Passed with existing baseline warnings; no warnings in new adapter/tests/benchmark |
| Owned Rust formatting / `git diff --check` | Passed |

A final API review found that `Real::new` rejects NaN but accepts infinity;
`Inf * 0` could then panic in TiKV's `NotNan` arithmetic. A failing regression
reproduced this through both public embedding layers before the fix. The final
TiKV revision rejects all selected nonfinite inputs and serialized constants
before entering kernels, while allowing nonfinite values in unselected rows.
No panic-catching wrapper or kernel change hides the problem.

The SQL tests exercise parser → planner → projection via `run_select_on` and
`run_select_meta_on`, assert actual TiKV counters, compare inferred result types,
and cover BIGINT/DOUBLE arithmetic, comparisons, VARCHAR byte length, NULLs,
multiple chunks, lazy CASE fallback, Decimal fallback, selected/duplicate rows,
concurrent context isolation and preserved overflow error identity.

The fresh-clone `make bazel_prepare` gate was attempted and could not run because
`bazel` is not installed. No Go/Bazel source was changed. Full TiKV server tests,
full TiDB workspace/server tests, live-cluster/TPC-H tests, race sanitizers and
TiFlash integration were not run. Existing ignored tests remain ignored; no
pre-existing test was removed or weakened to make this PoC pass.

## Conversion-inclusive benchmark

The dependency-free `tidb-expr/benches/tikv_expression.rs` harness evaluates the
**same expression and input** with native and TiKV `EvaluatorSuite`s in the same
optimized binary. It verifies every warmup result and requires a positive TiKV
counter, so silent native fallback cannot produce a misleading benchmark.

Timed work includes output reset, selection gather, input conversion/copies,
actual evaluation, output conversion and output chunk appends. Input generation
and compilation are excluded from steady-state timing. TiKV compilation is
reported separately. No RPC, scan, planner or SQL result formatting is timed.

- Host: AMD Ryzen 9 9900X, 12 cores / 24 threads, x86-64, Linux.
- Rust: nightly-2026-08-22 (rustc 1.100.0-nightly).
- Profile: existing `bench` profile, opt-level 3, no LTO, 16 codegen units.
- Workloads: integer add, nested integer arithmetic, nested DOUBLE arithmetic,
  variable byte length; 1 / 128 / 1024 / 4096 physical rows.
- Dense non-NULL and nullable/reverse-selected inputs; selected cases include
  NULL every 11 rows and select roughly two thirds of physical rows.
- Five paired samples, 60 ms minimum per engine/sample; execution order alternates.
- Report median ns/batch plus min/max, not a claimed confidence interval.
- Warning cap 65535, matching `StmtContext`, including current eager TiKV warning
  reservation cost. This is intentionally not tuned away for the benchmark.
- Optional environment: `TIKV_BENCH_MS`, `TIKV_BENCH_PASSES` (minimum 3).

### Results (2026-09-17)

Both final runs use the pinned `ff315c7` dependency, including its finite-input
checks. CPU affinity is fixed to logical CPU 2; frequency boost and ASLR remain
enabled, and the host is not otherwise isolated. These are exploratory local
microbenchmarks, not end-to-end SQL, Go TiDB, TiKV-server, or TiFlash results.

For **1024 dense, non-NULL rows**, default-run median time including copies:

| Workload | Native Rust µs/batch | TiKV bridge µs/batch | Native / TiKV | Repeat TiKV / native |
| --- | ---: | ---: | ---: | ---: |
| Integer add | 78.47 | 18.07 | 4.34× | 0.237 |
| Nested integer arithmetic | 192.99 | 25.86 | 7.46× | 0.136 |
| Nested DOUBLE arithmetic | 143.66 | 30.88 | 4.65× | 0.226 |
| Byte length | 64.92 | 40.65 | 1.60× | 0.589 |

The longer repeat uses seven 120-ms paired samples. Both runs also show the
important downside: **single-row dense evaluation is about 2–4.7× slower**
through the bridge, even before compilation. NULL/selected and 4096-row cases
are included in the raw data; do not extrapolate these four rows to all SQL.
Sample spreads show host noise, especially for some large/string batches.

- [Default run: 5 × 60 ms samples](tikv-expression-benchmark.csv)
- [Longer repeat: 7 × 120 ms samples](tikv-expression-benchmark-repeat.csv)

Separate mean compile cost over 100 preparations in the repeat run was 3.762 µs
(integer add), 4.934 µs (integer chain), 3.732 µs (DOUBLE arithmetic), and 1.671 µs
(byte length). It is **not included** in the warm timings above. Actual parallel
projection can incur this per dispatched chunk, so these numbers do not justify
an automatic/default engine switch.

The measured result supports continuing the experiment: copying does not erase
all benefits for these batch workloads, but buffer/view reuse and compilation
lifetime should be measured before broadening the integration.

## Next changes worth measuring separately

1. Reuse typed input/output buffers and avoid owned `Datum` materialization.
2. Share decoded column views rather than immediately replacing both batch types.
3. Avoid recompiling RPN per parallel projection chunk.
4. Separate the evaluator from server-oriented utility/native dependencies.
5. Expand admission only with differential tests for casts, Decimal hidden
   precision, unsigned/context-sensitive arithmetic and diagnostics.

This is a functioning integration/measurement experiment, not a replacement of
TiDB's complete expression system or a claim of a completed Go-package port.
