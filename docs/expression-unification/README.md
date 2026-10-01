# Expression unification experiment

Checkpoint-ID: `div-one-50` (previous `mod-one-49`).
**164/245 functional families; target221; strict final acceptance0.** New family: true division `/`, not integer DIV. Incomplete, not PR-ready.
Paired branches: [TiDB](https://github.com/YangKeao/tidb/tree/expression-unification-demo) · [TiKV](https://github.com/YangKeao/tikv/tree/expression-unification-demo). Each checkpoint includes the same Plan; no force-push or automatic PR.

## Shared implementation
Four profiles cover native/legacy real and Decimal division. Full-u32 precision is a transient typed binding over the existing three physical Decimal/Decimal/budget slots. The actual wrapper records one disposition, while the official Decimal column remains the only value owner. Guarded materialization validates the call witness/status/presence and clears state before reuse.

Shared Grow division preserves native hidden scales, signed overflow saturation, visible-floor truncation and source precision policies. Native `div_mysql_with_warning` is a thin bridge; duplicate quotient/bounding code is removed. Frontends retain original casts, child/batch demand and post-result warning/error handling, not arithmetic. Native effective precision and legacy raw increments remain distinct; actual NULL/missing use existing recipes. Wire behavior, IntDIV flow and coefficient-fast tiers remain unchanged.

## Validation
| Gate | Result |
|---|---|
| Shared / native Decimal | 88 / 25 passed |
| TiKV division / local | 13 / 291 passed; local1 ignored |
| Native division | 20 passed |
| Legacy / SQL | 1 / 2 passed |
| Full expression | **1492 passed,4 old failures,94 ignored; exit101** |
| Full unistore | **201 passed,1 old failure,13 ignored; exit101** |

Ten Cargo attempts: nine actual runs and one E0004 compile failure, fixed by an exact new-result rejection arm. Seven final focused gates pass. Full failure sections equal the previous checkpoint after thread IDs only. No new test RED or original fixture changes; two newly authored self-oracles were replaced with independent pins before tests ran. Eleven new tests,19 Rust sources (TiKV8/native11), no dependency/manifest/lock changes; scoped formatter/diff checks pass.

Exact commands and all log hashes: [summary](logs/div-one-summary.txt); ownership/protocol/policies: [evidence](evidence/div-one-checkpoint.md), `checkpoint.json` and the root Plan.

## Remaining work
81 eligible families remain; target needs57. IntDIV warning-before-conversion, typed Time/parser closure, broader request-root integration, physical heap/peak/OOM, differential/M6/TiFlash, release/performance, whole workspace and lint remain unverified. Existing parser/full-suite failures and extreme Decimal release-shape exceptions remain unresolved. No complete Go-package/type-domain equivalence claim. AES two-family reuse is a read-only candidate requiring a shared public crypto/dependency step, not completed work.
