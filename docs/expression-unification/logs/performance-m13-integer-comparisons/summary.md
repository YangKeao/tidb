# LongLong integer comparison family (2026-10-16)

New eager admissions: LT, LE, GE, NE and NULL-safe equality. GT/EQ already had
signed batch admission before this slice. All seven now support signed/unsigned
LongLong operands using their full 64 bits and each operand's FieldType flags.
The single capability registry separates physical Int transport from permission
to execute unsigned operands. Arithmetic and other registered families retain
signed-only input/results; the older row contract is unchanged. Ordinary results
remain signed Int booleans. Tiny/Long/other raw integer SQL type codes remain
outside this LongLong closure; this is not all integer/coercion overload coverage.

NULL-safe equality is eager but not NULL-strict: both complete child phases are
required, then the official kernel returns a non-NULL boolean. Other comparisons
keep their NULL behavior. The protobuf catalog and typed decoder are updated
together; PB row evaluation retains left-NULL stop for the six strict comparisons,
while NULL-safe equality demands its right operand. No function-named executor
or parallel migration implementation was added.

## Release results

Dense 1,024 selected rows, ns/selected-row. The first two timing columns use the
same CURRENT binary, explicitly calling `EvaluatorSuite::run` for fallback
ablation versus `run_with_tikv_batch`. These are NOT historical native/TiKV
implementations. Three mode pairs alternate fallback→batch / batch→fallback /
fallback→batch, seven 75 ms samples per cell. Latencies are medians of process
medians; ablation speedup is the median within-pair ratio.

The frozen-native column comes from a genuinely separate frozen production pair,
with a test-only comparison probe added. A native process runs after each mode
pair; its median is a version comparison, not the same-binary ablation ratio.

| comparison | current fallback ablation | batch | ablation speedup | frozen native | native / batch |
|---|---:|---:|---:|---:|---:|
| `eq` | 1090.482 | 11.164 | 97.71x | 71.327 | 6.39x |
| `ge` | 1088.026 | 11.160 | 97.49x | 72.624 | 6.51x |
| `gt` | 1095.315 | 11.229 | 97.57x | 71.732 | 6.39x |
| `le` | 1085.500 | 11.197 | 96.88x | 72.077 | 6.44x |
| `lt` | 1088.006 | 11.192 | 97.20x | 71.763 | 6.41x |
| `ne` | 1096.537 | 11.183 | 98.19x | 71.978 | 6.44x |
| `nulleq` | 1137.923 | 11.283 | 100.85x | 78.756 | 6.98x |

All 1,470 current timing records complete, all 105 workload/shape/pair checksum
groups agree. Batch is prepared-only with zero fallback/refusal. The 21 frozen
native/current dense groups also agree. Dense, NULL-every-10, sparse-quarter,
reverse and duplicate selections are retained in raw/summary JSON. Timing uses
signed input columns; unsigned coverage below is correctness evidence, not a
measurement of every signedness specialization. This is not SQL-server QPS.

A short additional M12/M13 carrier control has 450 timing records / 45 checksum
groups with no mismatches. It is a single ordered control, not three paired
replicates; the first Int run has a cold/warm transition. Do not count its Int
latency difference as a claimed optimization. The full raw data and dense
medians are retained in the carrier-control files.

## Source cohort and checks

- Current TiDB: commit containing this report; paired TiKV `6d18f1407b9bb45906bd0b5e67f24c798ec3d6db`. Current libtest SHA-256 `983cd916c640edcc17553874c9d590be9b4ed441dde46ac0a07fa7834bc5e1b0`.
- Original-native: TiDB `364aef2bab5cc633ecb76a775ae8f36f86a6687d`, TiKV `548812e1ef57aef077a2062a9cc356640a6347f5`. TiKV is clean; only the probe/test registration changes in TiDB. Expanded probe SHA-256 binary `ddc2c246cf6458dc2bb3dbddbf59d76de3d2481a0efa8ad8790fa5f3ce795ce1`. This is a NEW 16-workload probe binary, not the older nine-workload M8/M12 hash.
- Retained expanded probe source: `../../evidence/original-native-integer-comparisons-performance-m13.rs`; copy/register it in the frozen test module without changing production evaluators. It includes the original nine carrier workloads plus these seven comparisons; only the seven are summarized here.
- CPU2, Ryzen9 9900X, Rust1.97.1/bootstrap, empty RUSTFLAGS, GCC/G++14, release profile. No compilation during timing. Pinning does not fully isolate SMT/host/boost effects; three cohorts describe local repeatability.
- Fresh TiDB batch26/26, including 56 operator/signedness/column-or-literal combinations over 2,050 reverse/duplicate selected rows against independent i128 ordering and NULL truth; protobuf encode/decode/evaluate signedness and NULL cases; full library1,376 passed / the same4 baseline failures.
- TiKV profile50/50, including NULL-safe complete operand demand and unsigned-policy guards; full expression1,026 passed / the same6 baseline failures. Scoped formatting, diff checks and TiDB lint pass. The new encoder/decoder mismatch and NULL-safe PB early-stop failure were caught and fixed before publication.
- No fresh real-cluster/FIPS/server load run or full workspace clippy pass is claimed.

## Reproduction

Build the matching current release libtest as `current-bin` and the test-only
frozen comparison libtest as `frozen-bin` beside `collect.py`; run that script
and `summarize.py`. Binaries are not committed. The retained probes define the
same deterministic inputs and checksum; record new build hashes/environment.
GT/EQ's pre-existing batch path and the newly admitted five signatures must not
be conflated with the forced fallback ablation. No historical-TiKV comparison
for previously unavailable routes is fabricated.
