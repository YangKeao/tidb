# Same-domain Double and Decimal comparisons (2026-10-16)

Fourteen new eager admissions: LT, LE, GT, GE, EQ, NE and NULL-safe equality
for Double and signed Decimal. The existing generated comparer kernels, one
capability registry, shared vector driver and typed PB decoder are reused.
No function-named executor, alternate operation implementation or migration
switch is added. Go builtin_compare.go generateCmpSigs selects these exact
same-domain signatures with signed Int boolean results. This receipt excludes
mixed-domain coercion, raw FLOAT, NaN, unsigned Decimal and string collation.
Finite Double, signed zero and infinities remain valid; Decimal preserves
coefficient precision rather than comparing via f64.

NULL-safe equality completes both child phases and returns a non-NULL boolean.
The six strict PB row comparisons retain their left-NULL stop. The batch domain
is pure/eager: it evaluates complete child columns before the parent and does
not claim ordered effects or lazy short-circuit semantics.

## Release results

Dense 1024 selected rows, ns/selected-row. Three pairs alternate current
fallback→batch / batch→fallback / fallback→batch. Each workload/shape/mode has
seven adaptive approximately 75ms samples. Latencies below are medians of three
process medians; ablation speedup is median within-pair ratio. The first two
columns use the SAME current binary with EvaluatorSuite::run versus
run_with_tikv_batch: this is a route ablation, NOT historical native.

A separate true frozen production pair, with only the archived test probe
added, runs after each pair. That column is a distinct version comparison.
The added Decimal e column differs from d, so comparison is not self-equality.
Both probes use identical deterministic inputs, SQL, output materialization
and FNV checksum. Setup is outside timing; reusable input/output chunks and
output reset are inside the normal warm evaluator contract.

| comparison | current fallback ablation | batch | ablation speedup | frozen native | native / batch |
|---|---:|---:|---:|---:|---:|
| `decimal_compare_eq` | 1634.582 | 147.140 | 11.14x | 175.654 | 1.19x |
| `decimal_compare_ge` | 1633.398 | 147.200 | 11.12x | 176.259 | 1.20x |
| `decimal_compare_gt` | 1643.559 | 147.545 | 11.13x | 178.813 | 1.21x |
| `decimal_compare_le` | 1655.036 | 147.408 | 11.24x | 177.132 | 1.20x |
| `decimal_compare_lt` | 1633.466 | 147.324 | 11.07x | 175.773 | 1.19x |
| `decimal_compare_ne` | 1644.747 | 147.446 | 11.13x | 175.229 | 1.19x |
| `decimal_compare_nulleq` | 1603.675 | 147.328 | 10.89x | 166.678 | 1.13x |
| `real_compare_eq` | 1217.180 | 11.719 | 103.77x | 80.790 | 6.89x |
| `real_compare_ge` | 1222.839 | 11.809 | 102.96x | 80.429 | 6.81x |
| `real_compare_gt` | 1225.585 | 11.760 | 103.98x | 80.689 | 6.86x |
| `real_compare_le` | 1219.966 | 11.638 | 104.39x | 80.967 | 6.96x |
| `real_compare_lt` | 1223.032 | 11.618 | 105.14x | 80.729 | 6.95x |
| `real_compare_ne` | 1209.555 | 11.724 | 103.26x | 80.609 | 6.88x |
| `real_compare_nulleq` | 1217.628 | 11.846 | 102.81x | 80.363 | 6.78x |

Double dense batch medians are 11.618–11.846ns/row, versus true native 80.363–80.967 (6.78–6.96x). Decimal batch is 147.140–147.545, versus native 166.678–178.813 (1.13–1.21x). This is a smaller Decimal gain than the roughly 11x current-fallback ablation. It does not remove the previously measured Decimal PLUS gap.

All 2940 current timing records / 210 workload-shape-pair checksum groups agree,
including the 42 true-native/current dense groups. All measured batch invocations
are prepared-only, zero fallback/refusal. All five shapes (dense, NULL-every10,
sparse-quarter, reverse and duplicates), every sample and process medians remain
in raw logs and summary.json. This is local expression throughput, not SQL-server
QPS or application workload coverage. CPU2 pinning is not isolation from host,
SMT, boost or thermal effects. No compilation ran during measurement.

## Cohort and validation

- Current TiDB: commit containing this report; paired TiKV d6de538b5d97fdda755918f7ea1520843da02632. Current binary SHA256 aecfed619e5d559454a4aba589e3076883c1b74106f7807a1d11ef12e85a5244.
- True frozen production: TiDB364aef2bab5cc633ecb76a775ae8f36f86a6687d and TiKV548812e1ef57aef077a2062a9cc356640a6347f5. TiKV remains clean; TiDB changes only test registration/probe. New expanded binary SHA256 8b044e1ed7a025c89d6c307820a8e9f2b895ad83022909397bac557bf490fce5, not the earlier M13 binary.
- Exact frozen build source: ../../evidence/original-native-numeric-comparisons-performance-m14.rs, SHA256 4b805d4f72a6aee451482b73d296282f8839b50f712b47157f45cd84ce481e75. Thirty definitions include prior carriers/Int comparisons, only the fourteen numeric comparisons collected here.
- Ryzen9 9900X, CPU2, release, Rust1.97.1/bootstrap, GCC/G++14, empty RUSTFLAGS. machine.txt and toolchain.txt preserve host facts.
- TiDB batch27/27; independent hand-ordered numerical oracle covers all fourteen operators, precision beyond f64, scale-equivalent decimals, signed zero, finite extremes/infinities, both/left/right NULL and 2050 reverse/duplicate selected occurrences crossing windows. PB encode/decode/evaluate covers fourteen signatures and all four NULL combinations.
- Full TiDB1378 pass / same4 baseline failures; TiKV profile51 pass, full1027 pass / same6 baseline failures. TiKV guards unsigned inputs/results for all fourteen signatures. make lint, scoped Rust formatting and both git diff checks pass. Exact commands/failure names are in validation.md and full-library logs.
- No fresh real-cluster/server/FIPS or full workspace clippy pass claimed. The previous unrelated compiler ICE remains outside this receipt.

## Reproduce

Build the matching current release libtest and the frozen production libtest
with this archived test-only probe as current-bin/frozen-bin beside collect.py.
The binary files are intentionally not committed. Run collect.py then
summarize.py and make-report.py; record new hashes. validation.md preserves
build environment and test commands. Include every cell, including any flat or slower
results relative to true original native. Registry admission alone
is not proof of all functions, SQL modes or metadata combinations.

## Process repeatability

Ranges below span the three process medians, not confidence intervals.

| workload | batch median range | current fallback median range | frozen native median range |
|---|---:|---:|---:|
| `decimal_compare_eq` | 146.774–147.177 | 1626.674–1668.117 | 175.634–175.718 |
| `decimal_compare_ge` | 146.838–148.148 | 1630.972–1640.171 | 176.102–176.469 |
| `decimal_compare_gt` | 146.961–147.633 | 1633.327–1644.973 | 177.204–179.029 |
| `decimal_compare_le` | 147.254–147.896 | 1627.934–1680.671 | 176.075–177.186 |
| `decimal_compare_lt` | 147.285–147.496 | 1626.236–1652.298 | 175.585–175.864 |
| `decimal_compare_ne` | 147.114–147.955 | 1637.262–1674.478 | 175.003–177.173 |
| `decimal_compare_nulleq` | 147.021–147.436 | 1600.747–1641.595 | 166.122–168.465 |
| `real_compare_eq` | 11.617–12.111 | 1216.056–1223.195 | 80.625–80.882 |
| `real_compare_ge` | 11.720–11.950 | 1211.691–1230.393 | 80.390–80.592 |
| `real_compare_gt` | 11.698–11.787 | 1210.757–1236.542 | 80.632–80.693 |
| `real_compare_le` | 11.596–11.687 | 1210.167–1225.534 | 80.932–81.114 |
| `real_compare_lt` | 11.613–11.979 | 1221.508–1223.265 | 80.349–81.238 |
| `real_compare_ne` | 11.711–11.766 | 1209.296–1215.928 | 80.513–80.942 |
| `real_compare_nulleq` | 11.843–11.854 | 1216.721–1218.907 | 79.674–80.658 |
