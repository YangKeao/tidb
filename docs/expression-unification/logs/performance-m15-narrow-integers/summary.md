# Narrow integer input types (2026-10-16)

Tiny/Short/Int24/Long join LongLong on the same eight-byte Int carrier. The
single eager capability registry, arithmetic/comparison kernels, driver and
signed-result contracts are unchanged. Unsigned operands remain admitted only
for the seven comparisons; unsigned arithmetic is refused. The older row-only
profile remains signed LongLong. Year stays excluded because its native Datum
read has a different signedness rule; BIT/ENUM/SET/arrays remain excluded.

This changes source and runtime type admission, not arithmetic precision:
signed TINYINT127 plus1 produces LongLong128. Tests cover each input width's
valid extrema and signed/unsigned comparisons, rather than storing artificial
out-of-range i64 values in narrow-typed fields. No new executor, function-named
route or temporary migration implementation is added.

## Release results

Dense1024 selected rows, ns/selected-row. Four raw SQL types each run PLUS and LT.
Inputs i/j use the exact type under test; deterministic values fit all four
signed widths. Comparison unsigned correctness is tested separately; the timing
cohort does not measure every signedness or operator specialization.

Three cohorts alternate fallback→batch / batch→fallback / fallback→batch.
Each current cell has seven adaptive approximately75ms samples across all five
shapes. Latencies are median process medians, ablation speedup is median paired
ratio. The fallback and batch columns are the SAME current binary's
EvaluatorSuite::run versus run_with_tikv_batch, not historical implementations.
A separate true original-native production pair runs with the same benchmark
schema after each pair. This is a distinct version comparison.

| input type | workload | current fallback ablation | batch | ablation speedup | frozen native | native / batch |
|---|---|---:|---:|---:|---:|---:|
| int24 | integer_add | 973.244 | 12.840 | 75.80x | 21.822 | 1.70x |
| int24 | integer_compare_lt | 1097.262 | 11.303 | 97.05x | 72.284 | 6.40x |
| long | integer_add | 976.054 | 12.840 | 76.13x | 21.862 | 1.70x |
| long | integer_compare_lt | 1085.028 | 11.301 | 96.24x | 72.246 | 6.39x |
| short | integer_add | 977.411 | 12.879 | 75.88x | 21.772 | 1.69x |
| short | integer_compare_lt | 1085.797 | 11.368 | 95.19x | 72.254 | 6.36x |
| tiny | integer_add | 978.241 | 12.873 | 75.84x | 21.782 | 1.69x |
| tiny | integer_compare_lt | 1097.769 | 11.293 | 97.33x | 72.018 | 6.38x |

All1680 current timing records /120 type-workload-shape-pair checksum groups
agree, including24 true-native/current dense groups. Every measured batch uses
prepared only with zero fallback/refusal. Dense, NULL-every10, sparse-quarter,
reverse and duplicate samples and process medians are all retained. This is
local expression throughput, not server QPS or all-integer-function coverage.
CPU2 pinning does not isolate SMT, thermal, boost or other host activity. No
compilation ran during final timing. Include flat/slower cells when comparing
future checkpoints; native/batch ratios below1 would mean batch is slower.

## Source and validation

- Current TiDB: commit containing this report; paired TiKV8b210d8330eb7afecf973f6ccee0df059e76eb03. Current binary SHA256025aa8b47be1fb0a540662e576c589cb800ed0d2178a873f3e4c28f3c461399d.
- True frozen production pair: TiDB364aef2bab5cc633ecb76a775ae8f36f86a6687d / TiKV548812e1ef57aef077a2062a9cc356640a6347f5. Only the test-only probe and tests/mod.rs registration differ. Frozen binary SHA25691f9c56c4a52ba68fd51976ceb287559ed2929f056a12ba570c5764720dcebe9.
- Exact source ../../evidence/original-native-narrow-integer-performance-m15.rs, SHA256325197d64b4059be2981a1c1e08b51114c0da4f143c4edfb01d64fe952abc93d. EXPR_BATCH_PERF_INT_TYPE selects raw input metadata identically in current/frozen probes; default LongLong remains unchanged.
- Ryzen9 9900X, Rust1.97.1/bootstrap, GCC/G++14, empty RUSTFLAGS, release current/frozen performance binaries. machine.txt and toolchain.txt preserve host facts.
- TiDB batch28 passed, including112 type/operator/signedness cases and four signed PLUS extrema cases crossing2050 selected occurrences. Full TiDB1379 pass/same4 baseline failures. TiKV final debug profile52 passed, full1028 pass/same6 baseline failures. The prior release test log predates updating obsolete Tiny/Long refusal expectations; final debug checks the accepted contract. Exact commands and failure names in validation.md/full-library logs.
- TiDB make lint, scoped rustfmt and both diff checks pass. No fresh server, PD/TiKV, FIPS or full workspace clippy success is claimed.

## Reproduce

Build matching current/frozen release libtests as current-bin/frozen-bin beside
collect.py (binaries are not committed). Register the archived test-only source
in the frozen checkout without changing production. Run collect.py,
summarize.py and make-report.py; record new hashes. validation.md records the
build/test commands. Schema type is controlled by EXPR_BATCH_PERF_INT_TYPE;
performance definitions and sample selection are preserved in the scripts.

## Process repeatability

Three process-median ranges, not confidence intervals:

| type | workload | batch range | fallback range | native range |
|---|---|---:|---:|---:|
| int24 | integer_add | 12.839–12.840 | 973.056–980.594 | 21.750–21.878 |
| int24 | integer_compare_lt | 11.296–11.306 | 1084.699–1097.288 | 72.251–72.357 |
| long | integer_add | 12.819–12.866 | 971.537–979.472 | 21.825–23.013 |
| long | integer_compare_lt | 11.263–11.348 | 1083.273–1087.648 | 72.208–72.375 |
| short | integer_add | 12.827–12.905 | 977.202–980.775 | 21.719–21.945 |
| short | integer_compare_lt | 11.311–11.561 | 1082.131–1095.343 | 72.062–72.298 |
| tiny | integer_add | 12.821–12.898 | 973.453–979.429 | 21.764–21.856 |
| tiny | integer_compare_lt | 11.252–11.294 | 1082.067–1099.093 | 71.946–72.143 |
