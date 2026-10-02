# Expression unification experiment

Checkpoint-ID: `grouping-between-54` (previous `compare-six-53`).
**174/245 functional families; target221; strict final acceptance0.** Added GROUPING and BETWEEN;47 more needed. Incomplete, not PR-ready.
Paired branches: [TiDB](https://github.com/YangKeao/tidb/tree/expression-unification-demo) · [TiKV](https://github.com/YangKeao/tikv/tree/expression-unification-demo). Matching Plan snapshots accompany both commits; no force-push or automatic PR.

## This checkpoint
GROUPING's unique bit/set algorithm and public metadata/function types now live in TiKV. Native helpers alias them; real scalar execution submits actual gid/mark-set bytes to three fixed mode kernels, or a genuine NULL witness before metadata lookup. Checked packing preserves raw unsigned bits, empty sets and more-than64-mark wrapping. No new driver, carrier, binding or wire admission.

BETWEEN already composes shared comparison/logical workers after the preceding migration. New evidence closes its remaining family without another kernel: AST selector-once/eager bounds and rewritten lazy bounds retain their existing negated/NaN and collation differences. SQL tests include existing GROUPING rollup admission and isolated direct-column zero-slot failures.

## Validation
| Gate | Result |
|---|---|
| TiKV GROUPING / local | 4 / 297 passed; local1 ignored |
| Native GROUPING / BETWEEN | 6 / 3 passed |
| SQL BETWEEN and GROUPING | 2 passed |
| Full expression | **1502 passed,4 old failures,94 ignored; exit101** |

Nine attempts: eight actual runs plus one compile failure. Five final focused gates pass. Two new-test REDs were corrected from source: a typed error-decoration expectation and undeclared temporal fixture FSP. Missing timezone/type annotations caused the compile failure. No original expected values or fixtures changed. Full failure details equal the preceding checkpoint after thread IDs only.14 Rust sources,10 new tests; no dependency/lock changes.

[Exact commands and hashes](logs/grouping-between-summary.txt) · [review map, corrections and exclusions](evidence/grouping-between-checkpoint.md) · `checkpoint.json` · root Plan.

## Remaining work and risks
IntDIV was investigated, not implemented or counted. Its mutable precision/warning/legacy exact-division behavior needs a separate bounded change. IN/INTERVAL/NullEq still have native computation; shared comparison leaves alone earn no credit.71 eligible families remain.

The prior row-comparison context activation remains an explicit compatibility change, not old-NoColumns equivalence. Broader request-root closure, whole workspace/lint/dev/bazel_prepare, release/performance/zero-copy, physical heap/peak/OOM/M6 and exhaustive compatibility/differential gates are unverified. Full unistore was not rerun here because legacy production was untouched. Existing parser/GB/ignored-vector/extreme Decimal exceptions remain. No whole Go-package/type-domain or FIPS claim.
