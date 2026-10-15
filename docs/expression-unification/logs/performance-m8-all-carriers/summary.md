# All-native-EvalType batch benchmark — corrected cohort

Date: 2026-10-12 (Asia/Shanghai)

## Correction

The original version of this summary incorrectly described two same-binary ablations as historical implementations:

- raw-log `native_reference` is the current migrated branch's `EvaluatorSuite::run`, not the frozen original-native evaluator;
- raw-log `original_tikv` forces the current optimized code to one-row windows, not a historical pre-optimization TiKV binary.

Those labels must not be used for cross-version speedup claims. They have been renamed in source to `current_fallback_ablation` and `current_tikv_row_ablation`. The raw log remains immutable evidence for the old binary and is interpreted only as this current-code ablation.

The corrected historical cohort uses:

- original native: TiDB `364aef2bab5cc633ecb76a775ae8f36f86a6687d` with paired TiKV `548812e1ef57aef077a2062a9cc356640a6347f5`;
- historical pre-optimization TiKV: TiDB `f388b337dfad156233904d5407230f8439e0150e` with TiKV `608bbfc9c63e8b50fa2be757533eba572ef9b874`;
- optimized TiKV batch: the dirty task patch on those latter base revisions, binary SHA-256 `4b002dc20e48ce86730437971a9dfb5d3d9b4f53e4184807648bf9e26273a173`.

The frozen-native probe binary SHA-256 is `6854cdace78b049247d1d1e165704750ce3ca90fd9fc289c03d98f6cd8b38613`; the historical-TiKV binary SHA-256 is `401b5aa8d62617028d87e8efb1983be27dc6cf444f95baff089d34c9f15a203e`.

All entries use the same SQL workload, deterministic 1,024-row input generation, output reset/materialization, CPU2, release profile, and correctness checksum. The frozen-native probe used seven 75 ms samples. Historical TiKV only admitted signed Int PLUS to its batch route; the other carriers are N/A rather than a current-code simulation.

## Corrected dense batch=1,024 medians

All times are ns/selected row.

| TiDB EvalType / workload | original native | historical TiKV batch | optimized TiKV batch | optimized vs original native |
|---|---:|---:|---:|---:|
| Int / `integer_add` | 21.324 | 253.812 | 55.491 | 2.60x slower |
| Real / `real_add` | 111.344 | N/A | 56.781 | 1.96x faster |
| Decimal / `decimal_add` | 73.998 | N/A | 548.599 | 7.41x slower |
| String / `lower` | 150.011 | N/A | 113.659 | 1.32x faster |
| Datetime / `any_value_datetime` | 119.524 | N/A | 43.268 | 2.76x faster |
| Timestamp / `any_value_timestamp` | 119.799 | N/A | 42.469 | 2.82x faster |
| Duration / `any_value_duration` | 121.732 | N/A | 49.928 | 2.44x faster |
| JSON / `any_value_json` | 140.874 | N/A | 220.669 | 1.57x slower |
| VectorFloat32 / `any_value_vector` | 155.959 | N/A | 181.934 | 1.17x slower |

For the only true three-way historical cell, optimized Int batch is 4.58x faster than historical TiKV batch but remains 2.60x slower than original native.

## Int gap profile

The optimized Int route was sampled again with Linux `perf` on CPU2 using the
release binary, batch 1,024, the `integer_add` workload, five 100 ms samples per
selection scenario, and then seven 200 ms samples for the sampled profile. The
warm dense samples remained 54.800–55.809 ns/row. `perf` collected about 6,000
cycle samples with zero lost samples.

The profile shows that the remaining gap is control/materialization work, not
integer addition itself:

- the generated TiKV `ArgConstructor`/`Evaluator` wrapper is 32.43% self;
- `OrdinaryFrame::finish_batch` is 9.99% self;
- `NativeDatumSource::read_batch` is 6.19% self;
- `malloc`, `free`, and raw-vector growth account for substantial additional
  self time (9.27%, 4.28%, and multiple growth symbols);
- output append helpers (`append_int64` and `finish_append_fixed`) are 4.14%
  self in total;
- the actual `IntIntPlus::calc` arithmetic is only 0.88% self.

This matches the source shape: `finish_batch` still loops over every lane,
constructs singleton argument views, calls `eval_prepared_kernel(..., 1, ...)`,
validates the singleton result, and pushes it into a new output vector. At the
outer boundary, both input columns are copied from the TiDB Chunk into TiKV
vectors and the result is appended row-by-row back into the destination Chunk.
The structural counter reports three top-level typed-vector allocations per
1,024-row call; allocator samples also include the singleton-kernel machinery.

The next optimization should therefore invoke the already vector-capable TiKV
kernel once with the full row count and whole operand vectors, then reuse or
borrow typed input/output storage. The former removes per-lane RPN wrapper and
validation work; the latter removes the two input imports, intermediate result
allocation, and row-wise output append. The stat run had a 0.4% L1 data-cache
miss rate and 0.1% branch-miss rate, so the profile does not indicate a cache or
branch-prediction bottleneck.

### Historical raw samples used for the medians

The frozen-native ns/row samples were:

- Int: 26.801, 21.238, 21.324, 21.338, 21.342, 21.324, 21.313
- Real: 111.180, 111.277, 111.263, 111.365, 111.344, 111.460, 111.456
- Decimal: 74.036, 74.147, 73.964, 73.854, 73.849, 74.002, 73.998
- String: 150.011, 149.760, 149.990, 150.195, 150.027, 150.598, 149.794
- Datetime: 119.633, 119.444, 119.480, 119.633, 119.318, 119.606, 119.524
- Timestamp: 119.653, 119.759, 119.744, 119.813, 119.799, 119.857, 120.319
- Duration: 121.732, 122.011, 121.847, 121.632, 121.627, 121.641, 121.879
- JSON: 140.951, 140.547, 140.874, 140.796, 141.058, 141.484, 140.832
- VectorFloat32: 155.797, 156.670, 155.912, 155.959, 155.974, 156.208, 155.232

Historical TiKV Int ns/row samples were 323.170, 253.199, 253.246, 252.763, 254.022, 253.812, and 253.848. The first sample includes the observed CPU warm-up transition; the seven-sample median is 253.812. Calibration ended at 1.093 ns/iteration for frozen native and 1.087 ns/iteration for historical TiKV.

## Current-code ablation retained in `raw.log`

The raw run compares three modes through the same final release-profile harness:

- `native_reference` (legacy label): the current migrated fallback.
- `original_tikv` (legacy label): the current TiKV code with one selected occurrence per invocation window.
- `batch_tikv`: the optimized TiKV path with one 1,024-row window.

The test used five timing samples per mode/workload/scenario, a 20 ms target per sample, batch size 1,024, and dense, NULL-every-10, sparse-quarter, reverse, and duplicate selections. All 675 timing samples completed; the 45 workload/scenario checksum groups agreed across all three modes. Every TiKV sample used its prepared route with zero fallback and zero refusal.

### Same-binary ablation medians

All times are ns/selected row. Speedups divide the reference median by the optimized batch median.

| TiDB EvalType / workload | current fallback | current TiKV row-window | TiKV batch | vs fallback | vs row-window |
|---|---:|---:|---:|---:|---:|
| Int / `integer_add` | 937.293 | 734.561 | 55.491 | 16.89x | 13.24x |
| Real / `real_add` | 1169.102 | 727.577 | 56.781 | 20.59x | 12.81x |
| Decimal / `decimal_add` | 1472.598 | 1141.958 | 548.599 | 2.68x | 2.08x |
| String / `lower` | 1152.233 | 732.305 | 113.659 | 10.14x | 6.44x |
| Datetime / `any_value_datetime` | 1079.345 | 532.610 | 43.268 | 24.95x | 12.31x |
| Timestamp / `any_value_timestamp` | 1080.525 | 530.788 | 42.469 | 25.44x | 12.50x |
| Duration / `any_value_duration` | 1069.459 | 545.756 | 49.928 | 21.42x | 10.93x |
| JSON / `any_value_json` | 1151.959 | 913.845 | 220.669 | 5.22x | 4.14x |
| VectorFloat32 / `any_value_vector` | 1120.784 | 825.052 | 181.934 | 6.16x | 4.53x |

The structural counters count typed vectors constructed by the evaluator, not allocator-hook events. Binary arithmetic creates three typed vectors per optimized call versus 3,072 in the row reference; unary/identity workloads create two versus 2,048. Optimized execution uses one window per 1,024-row call; the row reference uses 1,024.

## Cohort

- CPU: AMD Ryzen 9 9900X, 12 cores / 24 threads.
- TiDB base commit: `f388b337dfad156233904d5407230f8439e0150e` plus the dirty task patch.
- TiKV base commit: `608bbfc9c63e8b50fa2be757533eba572ef9b874` plus the dirty task patch.
- Benchmark binary SHA-256: `4b002dc20e48ce86730437971a9dfb5d3d9b4f53e4184807648bf9e26273a173`.
- Calibration ns/iteration across 54 before/after observations: min 1.077, median 1.090, max 1.387.
- Raw output: `raw.log` in this directory.

## Reproducing the cohorts

The current-code harness is `rust/crates/tidb-expr/src/tests/lane_cache_batch_performance.rs`.
Build the TiDB Rust library test in release mode against the paired TiKV checkout;
run its ignored `tests::lane_cache_batch_performance::lane_cache_batch_release_performance_probe`
with `--exact --nocapture --test-threads=1`, pinned to CPU2. Set
`EXPR_BATCH_PERF_BATCH_SIZE=1024`, `EXPR_BATCH_PERF_SAMPLES=5`, and
`EXPR_BATCH_PERF_TARGET_NS=20000000`. Repeat with `EXPR_BATCH_PERF_MODE` set to
`current_fallback_ablation`, `current_tikv_row_ablation`, and `batch_tikv`.
Set `EXPR_BATCH_PERF_WORKLOAD` to a workload name above to restrict the run.
The source emits all selection shapes; the historical comparison uses dense only.

For the original-native cohort, check out the frozen pair above, copy the retained
[probe](../../evidence/original-native-all-carriers-performance-m8.rs) into
`rust/crates/tidb-expr/src/tests/original_native_all_carriers_performance.rs`,
and register it from the tests module. Build a separate release test binary and
run the probe's ignored test. The probe fixes seven samples and 1,024 rows.
For the historical TiKV cohort, check out its frozen pair, replace the batch
performance test with the retained [historical probe](../../evidence/historical-tikv-batch-performance-m8.rs),
and run only `integer_add` with seven 75 ms samples. Build targets must be separate
from the current tree. These probes change tests only; do not transplant current
production evaluators into historical trees. Record fresh toolchain, environment
and binary hashes when rerunning; this publication retains the earlier measured
hashes and does not claim a fresh release benchmark.

## Publication validation (2026-10-15)

Paired TiKV publication commit: `0fdd7ebcc2df2d01a362abe374f77e9302e6de81`. The TiDB commit containing this report publishes the matching consumer patch. Check out both `expression-unification-demo` trees as sibling directories; TiDB Rust path dependencies use `../../tikv/components/…`. The benchmark binary hashes above identify the pre-commit measured builds, not new binaries built from the publication commits.

Fresh checks: `cargo test --locked -p tidb-expr evaluator::batch::tests --lib` (25 passed), `cargo test --locked -p tidb_query_expr --lib local::profile_tests` (48 passed), TiDB `make lint`, scoped Rust formatting and both Git diff checks passed. These checks do not rerun release timing, real-cluster, FIPS or server load tests.

The earlier complete TiDB library run had 1,374 passed / 4 failed (`ifnull_string_column_literal_uses_go_signature_and_column_collation`, `exp`, `vectorized_builtin_op_func`, `str_to_date_partial_formats_follow_no_zero_date`). The TiKV library run had 1,023 passed / 6 failed (the STR_TO_DATE worker test and five evaluated-argument recipe/identity guards). These are retained baseline failures, not a claim that the full suites pass. The earlier full TiKV workspace clippy attempt was blocked by a compiler ICE in unchanged `components/file_system`; it was not repeated for publication.
