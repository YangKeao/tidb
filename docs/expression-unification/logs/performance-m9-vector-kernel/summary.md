# Full-vector kernel checkpoint

Measured 2026-10-15/16, Asia/Shanghai, home-gentoo Ryzen 9 9900X, CPU2.

The ordinary frame now invokes the generated evaluator once with all operands.
The old per-lane singleton argument construction, result vectors and concatenation
are deleted, together with the unused SmallVec dependency. Work is charged for
all lanes before dispatch; actual retained output is checked before publication
or the next expression effect. This remains a retained-owner budget, not a hard
allocator-peak guarantee. Typed input import and output append still remain.

## Results

Dense, 1,024 selected rows, ns/selected-row. Before/after entries are medians of
three process medians; each process has seven 75 ms samples. Speedup is the median
of within-pair ratios. Three pairs alternate before→after / after→before /
before→after. Frozen native was separately remeasured in three subsequent
processes, seven 75 ms samples each, and is not part of the paired ratio.

| workload | before | vector kernel | speedup (paired) | frozen native (fresh) |
|---|---:|---:|---:|---:|
| `any_value_datetime` | 42.812 | 15.755 | 2.72x | 118.505 |
| `any_value_duration` | 49.070 | 19.711 | 2.49x | 120.386 |
| `any_value_json` | 214.989 | 143.059 | 1.50x | 140.665 |
| `any_value_timestamp` | 42.820 | 15.687 | 2.73x | 118.747 |
| `any_value_vector` | 176.592 | 83.071 | 2.13x | 155.379 |
| `decimal_add` | 536.500 | 288.082 | 1.86x | 73.172 |
| `integer_add` | 52.792 | 17.370 | 3.05x | 21.196 |
| `lower` | 104.976 | 37.813 | 2.78x | 148.969 |
| `real_add` | 55.483 | 18.447 | 3.00x | 109.856 |

All 1,890 before/after timing records completed. All 135 paired workload/selection
checksum groups agree. Every TiKV sample reports prepared only, zero fallback
and refusal. Dense, NULL-every-10, sparse-quarter, reverse and duplicate selections
are included; per-process medians and all 45 scenario summaries are in
`summary.json`. Frozen-native samples and process medians are retained separately.
This is a warm evaluator microbenchmark, not SQL/server QPS or proof for all
functions in a carrier.

Int now measures 17.370 ns/row versus the separately refreshed original-native
21.196. Decimal remains about 3.94x slower than native and JSON about 1.02x;
those remaining gaps require their own transport/kernel profiles. These fresh
results supersede the earlier M8 performance state, not its historical evidence.

## Cohort and validation

- Before: TiDB `4f548ce68aa1f155a73c63f65e57370caf1bdf45`, TiKV `0fdd7ebcc2df2d01a362abe374f77e9302e6de81`; retained binary SHA-256 `4b002dc20e48ce86730437971a9dfb5d3d9b4f53e4184807648bf9e26273a173`.
- After: paired TiKV `992e28c009b433408a4ed90027af5a0e60f30866`, matching TiDB lock/doc commit containing this report; measured binary SHA-256 `ddbd2d777e9e4be54e216b456a756635a85a85c4984989400754a39188866408`.
- Frozen original-native: TiDB `364aef2bab5cc633ecb76a775ae8f36f86a6687d` / TiKV `548812e1ef57aef077a2062a9cc356640a6347f5`, binary SHA-256 `6854cdace78b049247d1d1e165704750ce3ca90fd9fc289c03d98f6cd8b38613`.
- The current release build used local Rust 1.97.1, `RUSTC_BOOTSTRAP=1`, empty `RUSTFLAGS`, GCC/G++14 and `CXXFLAGS=-include cstdint`. No compilation ran during the final timing collection. CPU2 pinning does not isolate all host/SMT/boost effects; three pairs show local repeatability, not a statistical guarantee across machines.
- TiDB batch 25/25; full release library 1,374 passed / the same four baseline failures. TiKV profile 48/48 and runtime budget/storage 6/6; full library 1,024 passed / the same six baseline failures. Scoped formatting and diff checks pass. Prior TiDB lint applies to the unchanged consumer code; this checkpoint changes its lockfile and docs only.
- No fresh SQL-server, FIPS or real-cluster run is claimed. Full TiKV workspace clippy remains a previously documented compiler-ICE limitation, not a passing gate.

## Reproduction

Build independent release libtest binaries for the before and after pairs, and
provide them as `before-bin` and `after-bin` beside `collect.py` (binaries are not
committed). Run `python3 collect.py`, then `python3 summarize.py` in this directory.
The scripts preserve the workload/shape/sample settings and assert route/checksum
agreement. Use the retained original-native M8 probe and frozen pair for native
repeats. Record new hashes and environment for any rebuild.
