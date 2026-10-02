# Expression unification experiment

Checkpoint `json-values-56` (previous `json-nullsafe-55`). **184/245 functional families; target221; strict final acceptance0.** Added JSON_ARRAY, JSON_OBJECT, JSON_KEYS and JSON_PRETTY;37 more needed. Incomplete, not PR-ready.

Paired branches: [TiDB](https://github.com/YangKeao/tidb/tree/expression-unification-demo) · [TiKV](https://github.com/YangKeao/tikv/tree/expression-unification-demo). Matching Plan snapshots accompany both commits; no force-push or automatic PR.

## This checkpoint
- Workers construct arrays/objects from actual ordered operands, including empty lists; duplicate-key resolution is no longer native.
- Both KEYS arities use shared workers. The public raw SDK algorithm also delegates, retaining duplicate keys and its different nonobject-empty-array policy.
- Exact native JSON/pretty formatting moves to shared code. Computed JSON retains the old BinaryJSON parse/encoding boundary; PRETTY remains text. Generic codecs are not falsely claimed migrated.
- Actual context now reaches typed constructor/pretty dispatch. Resource admission activation is intentional and verified, including genuine zero-argument calls and NULL outcomes.
- No new driver/carrier/kind/binding or PB/legacy admission. EXTRACT and UNQUOTE remain uncredited pending their distinct raw policies.

## Validation
| Gates | Result |
|---|---|
| CPP JSON / local | 6 / 301 passed; local1 ignored |
| Native raw SDK / result SDK / context | 20 / 2 / 2 passed |
| SQL | 2 passed;22 direct zero-slot cases |
| Full expression | **1511 passed,4 old failures,94 ignored; exit101** |

Seven actual runs: six focused green, no compile failures/new REDs/retries. Complete expression failure details match the previous checkpoint. Unistore full was not rerun; its prior failure remains unresolved.22 Rust sources,12 additive tests, no manifest/dependency/lock changes. One parent static-check invocation used a nonexistent context path; the corrected proof passed without source changes.

[Review map and semantic boundaries](evidence/json-values-checkpoint.md) · [commands and hashes](logs/json-values-summary.txt) · `checkpoint.json` · root Plan. Historical manifests remain in Git; cumulative records remain in `migration-progress.json` with the prior180 objects unchanged.

## Remaining work
61 eligible families remain. Shared leaves alone earn no credit. EXTRACT/UNQUOTE, IntDIV and IN/INTERVAL native answers are still open. Whole workspace/lint/dev/bazel_prepare, strictM6/release/performance/zero-copy/physical heap/peak/OOM and exhaustive compatibility gates remain deferred. Prior request-root/context, float feature, parser/GB/vector/extreme Decimal exceptions stay explicit. No whole-package transcreation or PR-readiness claim.
