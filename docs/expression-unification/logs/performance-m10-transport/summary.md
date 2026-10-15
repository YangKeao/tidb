# Bulk output transport checkpoint (2026-10-16)

Resolve the destination ColumnWrite once per batch window. A shared nullable
fixed-cell builder reserves once and appends through one owned buffer borrow;
shared/frozen buffers preserve the scalar copy-on-write behavior. Int/Real/Time/
Duration use that builder. Bytes reuse the existing raw-cell batch builder.
Decimal validates/converts each cell once into bounded, accounted scratch before
publication, replacing its previous double conversion. JSON/Vector still use
scalar cell append through the one resolved output column.

The new builder preserves NULL bitmap, hidden NULL backing, scratch element,
append-to-nonempty, reset/reuse and frozen alias behavior against scalar appends.
It does not borrow TiKV result storage or eliminate input imports. There is no
second execution route or per-function fast path.

## Release results

Dense 1,024 rows, ns/selected-row. Three independent process pairs alternate
before→after / after→before / before→after, seven 75 ms samples per cell, CPU2,
AMD Ryzen 9 9900X. Latencies are medians of process medians; speedup is the median
of within-pair ratios. Other machine/toolchain settings match M9.

| workload | M9 vector kernel | M10 bulk output | paired speedup |
|---|---:|---:|---:|
| `any_value_datetime` | 15.768 | 10.484 | 1.50x |
| `any_value_duration` | 19.681 | 14.731 | 1.33x |
| `any_value_json` | 142.699 | 135.732 | 1.05x |
| `any_value_timestamp` | 15.788 | 10.563 | 1.49x |
| `any_value_vector` | 83.575 | 82.299 | 1.02x |
| `decimal_add` | 289.160 | 251.091 | 1.15x |
| `integer_add` | 17.322 | 13.039 | 1.33x |
| `lower` | 37.692 | 35.952 | 1.05x |
| `real_add` | 18.472 | 14.236 | 1.30x |

All 1,890 timing records complete; 135 paired workload/selection checksum groups
agree, all routes prepared, zero fallback/refusal. Dense, NULL-every-10, sparse,
reverse and duplicate selections are retained in `summary.json` and raw logs.
Vector changes are small (dense ~2%, sparse essentially unchanged) and should
not be interpreted as a robust improvement. This is an evaluator benchmark,
not SQL-server throughput or universal function performance.

Before: TiDB `fed88c82379097940d8ee784de0032a3d6387ee9`, TiKV
`992e28c009b433408a4ed90027af5a0e60f30866`, binary SHA-256
`ddbd2d777e9e4be54e216b456a756635a85a85c4984989400754a39188866408`.
After: the TiDB commit containing this report, same paired TiKV; binary SHA-256
`ffa486169878efc8265d8cd1190843c019cf4b09e79ca83c611ad516751e6e5e`.
No compilation ran during timing. CPU2 pinning leaves host/SMT/boost effects;
three pairs characterize local repeatability, not all deployments.

Fresh checks: Chunk 271 passed / 4 ignored, including the new nullable/frozen
storage regression; expression batch 25/25; full release expression library
1,374 passed / the same four recorded baseline failures; TiDB make lint and
scoped formatting/diff checks pass. TiKV code is unchanged at this checkpoint.
No fresh FIPS/server/real-cluster run is claimed.

Rebuild separate release libtests and provide `before-bin` / `after-bin` beside
`collect.py` to rerun the three pairs; `summarize.py` checks route/checksum counts
and reproduces `summary.json`. Binaries are not committed. Frozen-native values
remain the separately refreshed M9 cohort; they were not remeasured for M10.
