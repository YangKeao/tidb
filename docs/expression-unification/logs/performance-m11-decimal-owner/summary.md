# Direct Decimal word owner (2026-10-16)

The shared `Decimal::try_from_native_digits` constructor fills its final initialized
SmallVec owner directly. ASCII/count/scale/resource checks remain; bounded parsing
itself guarantees word ranges and partial-word padding. This removes a temporary
heap word Vec, a second validation scan and a word copy. Wide values still use the
same checked owner reservation. The general borrowed-word importer is now test-only.
This does not substitute the broader physical Chunk-codec domain for logical values.

## Results

Dense 1,024 rows, ns/selected-row, three alternating before/after process pairs,
seven 75 ms samples per cell, CPU2, Ryzen 9 9900X. Latencies are medians of process
medians; speedup is the median of within-pair ratios.

| workload | M10 | direct word owner | paired speedup |
|---|---:|---:|---:|
| `any_value_datetime` | 10.567 | 10.777 | 0.98x |
| `any_value_duration` | 14.863 | 14.923 | 1.00x |
| `any_value_json` | 135.419 | 135.900 | 1.00x |
| `any_value_timestamp` | 10.572 | 10.799 | 0.98x |
| `any_value_vector` | 81.217 | 83.039 | 0.98x |
| `decimal_add` | 250.597 | 229.120 | 1.09x |
| `integer_add` | 13.072 | 13.139 | 0.99x |
| `lower` | 35.957 | 36.050 | 1.00x |
| `real_add` | 14.374 | 14.373 | 1.00x |

Decimal improves about 9% (250.597→229.120 ns/row). The other workloads do not
benefit: some are flat and Time/Vector are about 2% slower in this cohort. Their
code paths were not intentionally optimized; this measurement does not isolate
binary-layout versus machine effects, so those changes are retained rather than
reported as improvements. Future workload-level assessment must include this
small tradeoff. Against the separately measured M9 frozen-native Decimal 73.172,
the current Decimal workload still costs about 3.13x as much.

All 1,890 timing records complete; all 135 paired workload/selection checksum
groups agree. Prepared-only, zero fallback/refusal. Other shapes and per-process
medians are in `summary.json`; raw samples are retained. This is a microbenchmark,
not server performance or all-function coverage.

Before: TiDB `7c4e26dc1f884e27a5ec0cd215ee3ca537b093ab`, paired TiKV
`992e28c009b433408a4ed90027af5a0e60f30866`, binary SHA-256
`ffa486169878efc8265d8cd1190843c019cf4b09e79ca83c611ad516751e6e5e`.
After: paired TiKV `73640d2eb43a45511c6470bd22dbc263c9bf7b2e`, matching TiDB
report commit; binary SHA-256
`faa041dd2002d36cd9321fef315c8767c53aca106dd08354b56b9f0700c731db`.
Rust 1.97.1, empty RUSTFLAGS, GCC/G++14 and bootstrap settings match M9/M10.
No compilation ran during timing. CPU2 pinning is not full host/SMT/boost isolation.

Fresh validation: complete TiKV datatype library 564/564; Decimal targeted 83/83;
TiDB batch25/25 and full release expression1374 passed / the same4 baseline failures;
full TiKV expression1024 passed / the same6 baseline failures. Scoped formatting and
Git diff checks pass. TiDB consumer code is unchanged; prior M10 lint applies.
No fresh server, FIPS, real-cluster or full workspace clippy pass is claimed.

Rebuild separate release binaries and place them as `before-bin` and `after-bin`
beside `collect.py`; run it and `summarize.py` to reproduce the settings/checks.
Binaries are not committed. Frozen native was not remeasured for this slice.
