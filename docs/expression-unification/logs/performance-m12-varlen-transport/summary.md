# Varlen serialization and input views (2026-10-16)

Ordinary owned Chunk output buffers now receive JSON type/payload and vector
serialization directly, without per-cell encoded temporary vectors. Shared/frozen
storage keeps its copy-on-write behavior. Int/Real/Bytes/JSON input import holds
one raw cell view per batch branch. JSON import no longer copies through an
intermediate native BinaryJSON owner; type checks and the later output validation
remain. Typed TiKV vectors and result ownership remain: this is not zero-copy
cross-library evaluation or kernel-direct-to-Chunk output.

## Results

Dense 1,024 rows, ns/selected-row, three alternating old/new process pairs with
seven 75 ms samples per cell. Latencies are medians of process medians; speedup
is the median within-pair ratio. CPU2, Ryzen 9 9900X. Frozen native is a separately
refreshed three-process cohort run afterward, not part of that paired speedup.

| workload | M11 | M12 | paired speedup | frozen native (fresh) |
|---|---:|---:|---:|---:|
| `any_value_datetime` | 10.826 | 10.091 | 1.07x | 117.316 |
| `any_value_duration` | 14.965 | 14.448 | 1.04x | 119.242 |
| `any_value_json` | 136.204 | 122.171 | 1.11x | 137.705 |
| `any_value_timestamp` | 10.767 | 10.086 | 1.07x | 116.551 |
| `any_value_vector` | 83.312 | 72.932 | 1.14x | 158.377 |
| `decimal_add` | 230.038 | 228.664 | 1.01x | 73.061 |
| `integer_add` | 13.170 | 12.836 | 1.02x | 21.111 |
| `lower` | 36.084 | 32.570 | 1.11x | 148.327 |
| `real_add` | 14.317 | 13.863 | 1.03x | 109.633 |

All 1,890 old/new timing records complete, 135 paired workload/selection checksum
groups agree, all prepared, zero fallback/refusal. Frozen-native/current dense
checksums also match for all nine workloads. NULL-every-10, sparse-quarter,
reverse and duplicate scenarios are retained in summary/raw files. Decimal's
~1% change is effectively flat; this slice targets Bytes/JSON/Vector storage.
The observed Time change also shares the same rebuilt import function; it is not
evidence that the Time kernel was optimized.

The current nine-workload microbenchmark is faster than refreshed original native
for eight workloads; Decimal remains ~3.13x slower (228.664 vs 73.061). This does
not generalize to all functions in these carriers or to SQL-server throughput.

Before: TiDB `d2395c39f4634ed36c6219e14f579a518d03969a`, paired TiKV
`73640d2eb43a45511c6470bd22dbc263c9bf7b2e`; binary SHA-256
`faa041dd2002d36cd9321fef315c8767c53aca106dd08354b56b9f0700c731db`.
After: TiDB commit containing this report, same paired TiKV; binary SHA-256
`68f11b4b4e27507f564c67b952c4e68d7d2ff324da3bf757e9a2ff062b0f6a30`.
Frozen native: TiDB `364aef2bab5cc633ecb76a775ae8f36f86a6687d` / TiKV
`548812e1ef57aef077a2062a9cc356640a6347f5`, SHA-256
`6854cdace78b049247d1d1e165704750ce3ca90fd9fc289c03d98f6cd8b38613`.
Rust 1.97.1/bootstrap, empty RUSTFLAGS and GCC/G++14 match previous checkpoints.
No compilation ran during timing; pinning does not isolate all SMT/host/boost
activity. These are local repeatability measurements.

Fresh validation: full Chunk272 passed / 4 ignored including encoded JSON/vector
bytes, NULL/reset and frozen alias regression; batch25/25; full release expression
1374 passed / same4 baseline failures; make lint, scoped formatting and diff checks
pass. TiKV code is unchanged. No fresh FIPS/server/real-cluster run is claimed.

Rebuild independent release libtests as `before-bin` / `after-bin` beside
`collect.py`; run it and `summarize.py` for the paired settings/checks. Use the
retained M8 original-native probe for the separately refreshed native cohort.
Binaries are not committed; record new hashes and environment on rebuild.
