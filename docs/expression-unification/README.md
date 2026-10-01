# Expression unification experiment

Checkpoint-ID: `compare-substrate-52` (previous `aes-two-51`).
**166/245 functional families; target221; strict final acceptance0.** This datatype substrate adds **zero** evaluator families. Incomplete, not PR-ready.
Paired branches: [TiDB](https://github.com/YangKeao/tidb/tree/expression-unification-demo) · [TiKV](https://github.com/YangKeao/tikv/tree/expression-unification-demo). Each checkpoint includes the same Plan; no force-push or automatic PR.

## Shared comparison substrate
- Native raw JSON comparison and its lossless/serde decoder closure now live in TiKV. Native BinaryJSON delegates, with a shared data-only node and structural scalar adaptation. Duplicate-key/count/key-order behavior, exact versus epsilon numeric comparison, opaque/temporal ranks, malformed fallback and distinct decoder depth/slice behavior remain unchanged; wire JSON comparison is not substituted.
- Native calendar comparison calls `Time::native_core_compare`, reusing existing wire raw ordering while ignoring the low four metadata bits. Numeric datetime conversion remains unchanged.
- Native Decimal Ord calls allocation-free borrowed `native_decimal_cmp`. Hidden storage scale and original coefficient policy remain intact; no fallible owned bridge or wire Decimal change is introduced.

No runtime predicate recipes, carriers, bindings, driver, PB or legacy admission are added. Full six-family comparison routing—including numeric batch and context-aware row paths—remains the next integration step. JSON encoders/renderers/parsers and complete type/package migration are not claimed.

## Validation
| Gate | Result |
|---|---|
| Shared JSON / Decimal / Time | 37 / 89 / 55 passed |
| Native binary JSON / Decimal / core time | 32 / 25 / 15 passed |
| Existing expression comparisons / SQL | 67 / 19 passed; expression5 ignored |
| Full expression | **1494 passed,4 old failures,94 ignored; exit101** |
| Full unistore | **201 passed,1 old failure,13 ignored; exit101** |

Ten test commands: eight focused gates pass first attempt; two full failure sections equal the previous checkpoint after thread IDs only. No Cargo/compile/new-test failure, retry or lock change. Four additive shared tests; all original test bodies and expectations preserved. Eight Rust sources pass pinned formatter checks. Three non-Cargo lookup/verification mistakes were corrected without source changes and are recorded, not counted as test failures.

Exact commands, incidents and hashes: [summary](logs/compare-substrate-summary.txt). Ownership and preserved contracts: [evidence](evidence/compare-substrate-checkpoint.md), `checkpoint.json`, and the root Plan. Previous AES family evidence remains [here](evidence/aes-two-checkpoint.md).

## Remaining work
79 eligible families remain; target needs55. Next: all six comparisons, now that the raw JSON prerequisite is shared; IntDIV remains separate. Native IEEE versus legacy total float order, actual inputs and demand, typed/batch/PB/legacy/row paths must all be preserved before family credit. Broader request-root integration, physical heap/peak/OOM, differential/M6/TiFlash, release/performance, whole workspace and lint remain unverified. Existing parser/full-suite failures and extreme Decimal release-shape exceptions remain unresolved. No complete Go-package/type-domain or FIPS claim.
