# Borrowed TiKV expression adapter: follow-up experiment

This experiment builds on `tikv-expression-reuse.md`; it is not a complete Go-package transcreation, a production-default change, or an end-to-end zero-copy SQL engine. The original copying benchmark and its CSV files remain untouched.

## Scope and implementation

TiDB branch: `feat/tikv-expression-borrowed-poc`, based on `38870eba83fcf6b4939063e01c42c383f5d512b2`.
TiKV branch: `feat/borrowed-expression-poc`, based on `ff315c7d001573166595702af6a36b6f69d2f93e`.
Measured TiDB source/benchmark revision: `bdc7597cf916a259061c2785e7a2f5b002b4c39e` (the following documentation commit adds this report and CSVs).
Pinned TiKV revision: `521ac7330208b7f74f2b182bc373882687967280`, published only to `YangKeao/tikv`.
The TiDB lockfile changes only the TiKV source revision; no dependency-version churn or local path dependency remains.

The new path eliminates these **adapter payload materializations**:

1. TiDB packed Chunk -> owned nullable facade input columns.
2. Owned facade inputs -> TiKV owned decoded input vectors.
3. TiKV result -> owned facade result column -> `Vec<Datum>`.

Instead, `Column::read_view()` keeps the existing storage guarded and exposes borrowed payload, validity and offsets. TiKV accepts native-endian packed Int/Real bytes (unaligned storage is valid) or byte payloads with borrowed i64 offsets. Selection maps stay borrowed, including reordered and duplicate rows. Numeric arguments are decoded into stack-local scalar holders; strings remain slices. Generated argument loaders invoke the **original arithmetic/comparison/ABS/LENGTH kernel bodies**, not new arithmetic implementations. A scoped sink writes each result directly into its TiDB destination column.

This still performs numeric scalar loads, validation and metadata allocations. RPN-generated intermediates/results still allocate original `VectorValue` storage. Writing final output to the TiDB representation remains necessary; returning a string column still writes its bytes to the output. `LENGTH` is especially favorable because it can inspect slice lengths without reading string payload contents. Its large-string speedup must not be generalized to arbitrary string kernels.

The borrowed path uses a separate, small RPN stack traversal over the same compiled nodes and an opt-in argument loader. The copying path retains its original `eval_decoded` and typed `ArgConstructor` machinery. Thus measurements include the new loader's dispatch/validation costs; this is not an isolated memcpy-only ablation. The per-row enum loader can lose some of the old typed-vector specialization benefit, so copying may remain faster in some cases.

## Runtime choice and safety contract

With the executor's `tikv-expr` feature enabled:

```rust
let ctx = StmtContext::for_query()
    .with_tikv_expression_backend(tidb_expr::tikv::Backend::Borrowed);
```

`Backend::Copying` selects the previous adapter. `with_tikv_expression(true)` continues to select copying unless a different backend was explicitly configured earlier. `with_tikv_expression(false)` disables either backend. Feature-disabled builds and the default statement configuration remain native.

`tikv_expression_rows()` counts successful rows for either TiKV adapter; `tikv_borrowed_expression_rows()` counts only actual borrowed execution. They count expression-row evaluations, not unique SQL rows, and statement clones share these effect counters while configuration remains copy-on-write.

The original bounded SQL admission contract is unchanged: homogeneous signed integer/DOUBLE arithmetic and comparisons, ABS, byte-preserving LENGTH/OCTET_LENGTH, and eligible static constants/columns. No new automatic admission for unsigned values, Decimal, implicit cross-domain casts, lazy CASE, parameters, temporal/JSON/vector operations or division. Unsupported SQL stays native. Unsupported borrowed loaders can use copying before execution; runtime errors are never replayed.

The standalone facade validates column shape, validity, offsets, selection bounds and all selected REAL values before the first kernel/sink. Full offset validation is O(physical rows), even for sparse selections. Whole-input validation can also report a later invalid REAL before an earlier kernel overflow, whereas the copying facade can fail earlier while processing an internal batch. This is an explicit public-boundary error-precedence difference for invalid inputs, not a change to the admitted automatic SQL path (which preflights nonfinite inputs before either adapter). Nonfinite constants remain rejected at compile time. TiDB automatic evaluation leaves nonfinite inputs native before execution. The borrowed engine path checks packed input layouts; TiDB's direct-output API requires an initially empty, layout-compatible destination. Copying fallbacks retain the existing copying API's input contract. The existing `EvaluatorSuite` calculated-expression path also supports appending after a prefix: it now chooses copying before execution for a nonempty destination, preserving that behavior and the prefix on errors. A RED/GREEN regression covers this distinction; it does not relax the new direct API's contract. A later internal batch error resets any partial destination output and does not increment successful-row counters.

Column owners can alias between chunks even when the Chunk objects differ. The adapter decides copying fallback before acquiring input guards for aliased/shared output. It deduplicates locks for repeated source owners. Borrowed guards remain local to one call and cannot escape into the cached program or asynchronous work. Pointer-identity tests cover guarded Chunk payload/metadata and actual engine byte-column output; frozen unaligned buffers and shared storage are covered separately.

## Benchmark protocol

Use the new `tikv_expression_borrowed` benchmark, not the preserved historical two-backend benchmark. One optimized binary compares native, copying and borrowed on identical fixtures and settings. Every case verifies all output values/NULLs and backend counters before timing. Each timed sample additionally checks its exact counter deltas after stopping the timer, proving no timed calls silently fell back. Compilation/fixture creation are excluded; per-call preflight, guards, allocation, evaluation, final output writes and output reset are included.

The six possible engine orders rotate over a multiple of six samples. Default settings are six samples of at least 60 ms per backend/case. Medians average the middle two values for an even sample count; CSV also records minima/maxima. Both borrowed/copying and borrowed/native ratios are reported (less than one is faster). Compilation time is reported separately as a mean over 100 identical compiles; the compiled program is shared in design, not a separately optimized compiler per backend.

Original workloads and sizes remain: int_add (`a + 17`), int_chain (`(a + b) * 3 + 7`), real_arithmetic (`(a + b) * b`) and bytes_length; physical rows 1/128/1024/4096. NULLs (every 11th row) and reverse selection (roughly two-thirds of rows) are now independent fixture dimensions. A selected one-row case retains row zero. Each original workload also includes a sparse case selecting only the last row from 4096 physical rows; the CSV identifies this by `physical_rows=4096,logical_rows=1,selected=true`. This exposes full-offset validation cost instead of hiding it. There are 80 cases in total. Extra LENGTH cases use 4 KiB padding at 128/1024 rows and 64 KiB padding at 128 rows, bounding individual physical fixtures near 8 MiB. `physical_payload_bytes` describes the physical fixture, not an instrumented number of bytes copied.

Run from `rust/`, after configuring the same nightly toolchain and build prerequisites as the original report:

```sh
cargo bench -p tidb-expr --features tikv-expr \
  --bench tikv_expression_borrowed -j 1 --locked --no-run
# Run the executable printed by Cargo, with builds stopped:
TIKV_BENCH_PASSES=6 TIKV_BENCH_MS=60 taskset -c 2 <benchmark-executable>
# Independent longer repeat:
TIKV_BENCH_PASSES=12 TIKV_BENCH_MS=80 taskset -c 2 <benchmark-executable>
```

The actual session wraps these commands in the memory guard described below. This is a projection microbenchmark, not a whole-SQL/cluster throughput measurement. CPU boost remains enabled and the host is not otherwise isolated.

## Memory limits

After the user reported a possible OOM/SSH interruption, heavy commands were serialized across both repositories. Cargo/native compilation and tests use one worker initially (`-j 1`, `CARGO_BUILD_JOBS=1`, `CMAKE_BUILD_PARALLEL_LEVEL=1`, `NUM_JOBS=1`, `RUST_TEST_THREADS=1`); `RUSTFLAGS=''` avoids the checkout's extra frontend compiler threads.

The workspace helper `expression-reuse/tools/limited-run.py` enforces an 8192 MiB per-process virtual-address-space limit, samples inherited process-group RSS every 0.5 seconds and aborts above 6144 MiB, and stops its own command if host available memory drops below 8192 MiB. RSS sums conservatively double-count shared pages and are sampled, not a cgroup hard RSS cap. Benchmarks use a smaller 1024 MiB RSS budget and bounded fixtures. The helper never kills unrelated processes. A limit failure must be investigated/reduced, not silently retried unbounded.

OOM was not confirmed: after the interruption the host had about 40 GiB available, kernel-log access was denied, and neither implementation agent had started a build at the interruption. The subsequently inspected SSH-session cgroup reported zero OOM events; that does not rule out an event elsewhere on the host.

An existing chunk allocator test creates 100 worker threads internally even with a single test-harness thread. It initially failed to create a thread under the 8 GiB address-space limit, with the workspace's 32 MiB default thread stacks. The limits were **not raised**: rerunning this crate with `RUST_MIN_STACK=2097152 MALLOC_ARENA_MAX=2` reduced stack/allocator virtual reservations and passed all 275 tests. This override is only for the chunk stress tests, not benchmarks or the vendored client tests that require larger stacks.

The guard's success, small-budget enforcement and cancellation cleanup tests passed (3 tests). Its enforcement test intentionally allocates only 32 MiB against a 16 MiB RSS budget; it is not an OOM stress test. Real builds/benchmarks did not trip the sampled RSS/host-reserve guard. Observed command-group peaks are recorded below; sampling can miss shorter peaks.

## Validation receipts

The validation scope covers the changed storage view, engine/codegen boundary, adapter and SQL projection integration, plus feature-disabled and downstream consumers. It is not a full database-package transcreation or cluster qualification.

- TiKV: **429 expression-library tests** and **21 codegen tests** passed, including actual borrowed byte pointer identity, unaligned numerics, invalid inputs, warning limits, unsupported loaders, eager NULL/overflow ordering and sink failures.
- TiDB chunk: **275 passed / 4 existing ignored**, plus **2 compile-fail lifetime doctests**.
- TiDB expression, feature enabled: **1180 unit + 35 integration passed / 99 existing ignored**, including **10 borrowed-adapter suites** and the original copying regressions.
- TiDB executor, feature enabled: **345 passed / 184 existing ignored**, including **5 new borrowed SQL suites** and the original copying SQL suites.
- TiDB expression, feature disabled: **1180 unit + 18 integration passed / 99 existing ignored**.
- Downstream `tidb-exec` library check with the executor feature passed.
- Scoped Clippy and repository `make lint` passed; existing unrelated warnings remain. Formatting and diff checks passed.
- Independent cross-repository read-only reviews found no blockers. Their sparse-offset-validation and public invalid-input error-precedence caveats are recorded above, not hidden.

The nonempty-output compatibility regression was verified RED before fixing routing: native/copying appended successfully while borrowed returned error 1105. After the guard, all three preserve `[99, -6, NULL, 5, -6]`, and the borrowed-configured append case records copying rather than falsely claiming borrowed execution. The direct API's nonempty-output rejection remains tested.

Commands below are scoped to TiDB `rust/`, unless noted, using the toolchain/prerequisite environment from the original report. The session ran heavy commands through `python3 /home/agent/tidb/expression-reuse/tools/limited-run.py -- <command>` with the memory/concurrency settings above. `--message-format short` was used for the expression, executor, Clippy and benchmark build logs.

```sh
RUST_MIN_STACK=2097152 MALLOC_ARENA_MAX=2 \
  cargo test -p tidb-chunk --lib -j 1 --locked -- --test-threads=1
cargo test -p tidb-chunk --doc ColumnReadView -j 1 --locked -- --test-threads=1
cargo test -p tidb-expr --features tikv-expr --lib --test all \
  -j 1 --locked -- --test-threads=1
cargo test -p tidb-executor --features tikv-expr --test all \
  -j 1 --locked -- --test-threads=1
cargo clippy -p tidb-chunk -p tidb-expr -p tidb-executor \
  --features tidb-expr/tikv-expr,tidb-executor/tikv-expr \
  --lib --tests --benches --no-deps -j 1 --locked
cargo bench -p tidb-expr --features tikv-expr \
  --bench tikv_expression_borrowed -j 1 --locked --no-run
# TiDB checkout root (same environment, RUST_MIN_STACK=33554432):
cargo test --manifest-path rust/Cargo.toml -p tidb-expr --lib --test all \
  -j 1 --locked --message-format short -- --test-threads=1
cargo check --manifest-path rust/Cargo.toml -p tidb-exec \
  --features tidb-executor/tikv-expr --lib -j 1 --locked --message-format short
# GOPATH/GOCACHE/GOTMPDIR configured by /home/agent/tidb/goenv.sh:
GOFLAGS='-mod=mod -p=1' GOMAXPROCS=1 GOMEMLIMIT=3GiB make -j 1 lint
# TiKV checkout root (no --locked/--offline/--no-default-features flags):
cargo test -p tidb_query_expr --lib -j 1 --message-format short -- --test-threads=1
cargo test -p tidb_query_codegen --lib -j 1 --message-format short -- --test-threads=1
```

Logs are in the session workspace `expression-reuse/logs/`: `borrowed-expr-final-tests.log`, `borrowed-executor-final-tests.log`, `borrowed-chunk-bounded-tests.log`, `borrowed-chunk-final-doc-tests.log`, `borrowed-clippy-final.log`, and `tikv-borrowed-final-{expr,codegen}.log`. The initial chunk thread-reservation failure remains in `borrowed-chunk-final-tests.log`; it is not the successful retry receipt.

Not verified: full TiKV server/workspace tests, complete SQL type/function parity, real-cluster behavior, production memory accounting or full-query/cluster throughput. No new Go/Bazel files changed; the earlier baseline's fresh-clone Bazel preparation limitation (missing bazel) remains, and no new Bazel sweep was needed. Existing ignored/transcreation-gap tests were not relabeled or enabled.

## Measured results

Run date: **2026-09-17**. Host: AMD Ryzen 9 9900X (12 cores / 24 threads), 46 GiB RAM, Linux 7.1.8, nightly-2026-08-22; bench opt-level 3, LTO off, 16 codegen units. Both runs used CPU 2, boost enabled, ASLR unchanged, no competing builds from this session. The host was not otherwise isolated.

Both complete **80-case** runs passed all value and routing assertions. Data:

- [6 × 60 ms run](tikv-expression-borrowed-benchmark.csv)
- [12 × 80 ms independent repeat](tikv-expression-borrowed-benchmark-repeat.csv)

The table uses the longer repeat's medians, **microseconds per 1024-row dense non-NULL batch**. Speedup means copying / borrowed; time reduction means 1 − borrowed / copying.

| Workload | Native | Copying | Borrowed | Speedup vs copying | Time reduction |
|---|---:|---:|---:|---:|---:|
| `a + 17` | 77.2139 | 17.5048 | 12.4210 | 1.409× | 29.0% |
| `(a + b) * 3 + 7` | 187.3715 | 24.9027 | 21.9794 | 1.133× | 11.7% |
| `(a + b) * b` (DOUBLE) | 142.1513 | 30.9117 | 28.3186 | 1.092× | 8.4% |
| `LENGTH`, short bytes | 64.7909 | 41.5748 | 10.7244 | 3.877× | 74.2% |

The first run's time reductions for these four cases were 29.7%, 12.1%, 11.3% and 74.8%. This supports the direction of the gains, not a claim that every small percentage is invariant. The final borrowed path is 5.02–8.52× faster than this binary's native path for these four cases; historical CSVs were not used as denominators.

### Large strings: favorable, but deliberately not the headline

For 1024 dense rows with 4 KiB padding, copying took **792.5013 µs** and borrowed **10.6236 µs** (74.60×). For 128 dense rows with 64 KiB padding, copying took **1805.0710 µs** and borrowed **1.4679 µs** (1229.70×). `LENGTH` can derive its result from offsets alone, so borrowed runtime barely changes with payload size. These factors do **not** predict speedups for kernels that scan or transform string contents.

The copying baseline is very sensitive to this large-buffer fixture: the corresponding nullable cases took 180.1227 µs and 304.5175 µs, respectively, versus borrowed 11.3834 µs and 1.5326 µs. This pronounced dense/nullable difference warrants allocator/cache profiling before assigning a precise cause. No allocation profiler was run; neither the large factors nor `physical_payload_bytes` are presented as a measured count of memcpy operations.

### Regressions and remaining work

- **Sparse byte selection regresses:** one selected row out of 4096 physical rows takes **1.1846 µs borrowed vs 0.2912 µs copying**, i.e. **4.07× slower**; native takes 0.0696 µs. The first run also regressed (4.00×). The borrowed boundary validates all 4097 offsets instead of gathering just the selected value. This is the sole copying-relative regression among these 80 cases in both runs; it was not hidden behind a performance fallback.
- The tested short-data single-row cases still favor native execution: removing payload staging does not remove dispatch, guard, validation and allocation startup costs.
- Numeric chains retain per-node argument loading and owned RPN intermediates, so their incremental gain is smaller than byte LENGTH. No SIMD or full-query speedup is inferred.
- A future iteration can reuse validated views or safely validate only selected offset pairs, and specialize typed borrowed loaders further. This experiment does not introduce such heuristics or widen admission.

Compilation remains the same prepared-program pipeline for copying and borrowed. The repeat's mean compilation times over 100 compiles were 4.216 µs (add), 4.241 µs (chain), 3.538 µs (DOUBLE), and 1.533–1.536 µs (LENGTH fixtures); they are excluded from warm timings.

### Observed command memory

Largest sampled process-group RSS among completed development/validation builds: **2263.7 MiB**, below the 6144 MiB guard threshold. The final release rebuild sampled **1011.6 MiB**; final Clippy **1268.3 MiB**. The two complete benchmark processes sampled **42.5 / 43.5 MiB**, below their 1024 MiB thresholds. These benchmark peaks aggregate all three backends and are **not per-backend memory comparisons**. Final native tests, downstream check and `make lint` also exited zero; their receipts are `borrowed-native-final-tests.log`, `borrowed-exec-final-check.log` and `borrowed-make-lint-final.log`.

## Changed surfaces and delivery

- `tidb-chunk`: guarded views, shared-storage detection, lifetime/pointer tests.
- `tidb-expr`: explicit backend choice, borrowed/direct-output adapter, projection routing, regression tests and the separate three-backend benchmark.
- `tidb-executor`: statement-local configuration, clone-shared effect counters and SQL integration tests.
- TiKV: opt-in codegen argument loaders, borrowed RPN boundary/facade, kernel annotations (original bodies unchanged), tests and API documentation.
- Dependency lock/manifests, this report, living ExecPlan and the two raw CSVs. The original branches, copying evaluator and historical benchmark/report files remain preserved.

Only the YangKeao fork branches are delivery targets: [TiDB](https://github.com/YangKeao/tidb/tree/feat/tikv-expression-borrowed-poc) and [TiKV](https://github.com/YangKeao/tikv/tree/feat/borrowed-expression-poc). No upstream organization branch was pushed.
