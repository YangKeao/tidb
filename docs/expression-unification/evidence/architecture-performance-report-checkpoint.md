# Architecture, migration, performance, and compatibility report

Checkpoint `architecture-performance-report-217` adds the standalone Chinese report `../ARCHITECTURE_MIGRATION_AND_EVALUATION_REPORT.md`. It describes the ownership model, local RPN and ready-value interfaces, TiDB staged glue/effect/lifecycle boundaries, module relationships, interleaved M0–M6 and per-family replacement process, encountered problems, and compatibility limits.

## New measurements

The frozen TiDB baseline and current TiDB were built with the same nightly-2026-08-22 release toolchain into separate targets. A temporary probe parsed each SQL expression outside the timing loop, used 2,000 warmups, and collected 9 correlated samples in each of 3 independent process-level replicates. CPU-2-pinned processes were interleaved before/after/after/before/before/after. Temporary sources and the detached baseline worktree were removed after measurement.

The primary current measurement uses the public AST/value default ownerless context. Session policy defaults to `None`, so this path constructs one-shot ownership per call. Current/frozen median ratios are: integer add 66.33x, lazy IF 45.69x, STRCMP 17.32x, Decimal add 7.30x, JSON_TYPE 9.16x, and REGEXP_LIKE 1.35x. Labels match for these six cases. These are explicit regressions for the measured path, not database-QPS results or attribution to one kernel.

A secondary explicit-pooled best-case/design measurement supplied a reusable `ReadyValueExecution`; it is not the current production/default path. Paired ratios remain 37.50x, 35.53x, 10.13x, 5.17x, 5.92x and 1.22x respectively. This shows lifecycle reuse helps but does not restore native-path performance. CPU boost/scaling remained enabled; only three process replicates exist, ranges are not confidence intervals, and no statistical significance was calculated.

The MD5 current release probe returned `ExpressionRuntimeFailure { class: InvalidSpecification, phase: Prepare }`; the same named frozen release test over the same MD5 row table passed and the current one failed, while the same named current debug test passed. Test helpers and binaries are not byte-identical. Therefore MD5 has no current performance ratio, the MD5 release path is not claimed compatible, and this receipt does not independently decide other crypto families. `pr_ready` remains false.

A cold isolated release `tidb-expr` libtest build recorded 2m30s / 151.62s wall / 2,679,980 KiB max RSS before and 5m30s / 341.13s wall / 3,252,576 KiB current. Cargo `Finished` is the cleaner compile marker; wall/RSS also include test execution. The revision range contains other source, lock, and dependency-graph changes, so growth cannot be attributed solely to direct TiKV dependencies.

## Receipts and exclusions

Distilled data and raw SHA-256 values are in `../logs/performance-before-after-summary.txt`. A final invariant checks report sections and references, primary and secondary medians, the release MD5 0/101 split, and temporary-probe deletion (`report-performance-invariant.log`, SHA-256 `e9e1bc60c98ebc0e9dc0affa5d599ad51b62df1861b3668ba945d127bd934d19`).

Excluded attempts are recorded: initial baseline execution omitted explicit `RUSTC`; first current one-shot and first pooled probes aborted at MD5 after four workloads; an existing Decimal `cargo bench` emitted no timings because of its `cfg(test)` guard; and one mistaken second-manifest command was killed before use. No failed, partial, or zero-output attempt contributes to final ratios.

Independent reviewer `329776b3-06e3-45ca-a67d-e72a33e34a7b` initially rejected seven categories of overstatement plus the conclusion. After the report and measurements were corrected, the reviewer returned APPROVED; the correction list is in `../logs/architecture-performance-report-review.txt`.

No production source changed in this checkpoint. Coverage remains 240/245, strict final audit remains 0, and accelerated M0–M6 Demo completion remains scoped exactly as before. Targeted suites cover different compatibility dimensions; they do not establish a complete family-by-entry cross-product.
