# Expression unification experiment

Checkpoint `json-nullsafe-55` (previous `grouping-between-54`). **180/245 functional families; target221; strict final acceptance0.** Added NullEq and five JSON families;41 more needed. Incomplete, not PR-ready.

Paired branches: [TiDB](https://github.com/YangKeao/tidb/tree/expression-unification-demo) · [TiKV](https://github.com/YangKeao/tikv/tree/expression-unification-demo). Matching Plan snapshots accompany both commits; no force-push or automatic PR.

## This checkpoint
- NullEq composes existing presence/Eq/IsTrue workers. Duration warning+false, Time NULL and row behavior remain distinct.
- JSON_CONTAINS, OVERLAPS, MEMBER_OF, CONTAINS_PATH and LENGTH use fixed shared workers. Native serde equality, raw SDK policies and legacy membership remain separate. Native public raw containment/overlap algorithms are also deleted.
- The shared path parser/walker replaces native copies. Contains-path preserves lazy path coercion and projects actual per-path worker results; JSON_EXTRACT receives foundation reuse but no family credit.
- No new driver/result kind/carrier/binding or wire admission. One existing serde_json feature is enabled explicitly for bit-exact float transport; wider standalone CPP rounding effects are disclosed, not claimed equivalent.

## Validation
| Final focused gates | Result |
|---|---|
| CPP raw / new JSON / local / ordinary JSON | 1 / 3 / 299 / 21 passed; local1 ignored |
| Native profiles / NullEq / raw SDK | 3 / 2 / 19 passed |
| Legacy / SQL | 2 / 2 passed |
| Full expression | **1507 passed,4 old failures,94 ignored; exit101** |
| Full unistore | **205 passed,1 old failure,13 ignored; exit101** |

15 attempts:14 actual runs plus1 new-fixture compile failure. Nine final focused gates pass. One new SQL fixture used planning3143 instead of the unchanged execution-tier1105; corrected from source, not recorded output. No old expected values/fixtures changed. Complete full-suite failure details match prior checkpoints.24 Rust sources,15 new tests, one manifest feature change; no new dependencies or lock changes.

[Review map and semantic boundaries](evidence/json-nullsafe-checkpoint.md) · [commands and hashes](logs/json-nullsafe-summary.txt) · `checkpoint.json` · root Plan. The current manifest is compacted; historical manifests remain in Git, cumulative records in `migration-progress.json`.

## Remaining work
65 eligible families remain. IntDIV is still unimplemented; IN/INTERVAL retain native answers. Shared leaves alone do not earn credit. Whole workspace/lint/dev/bazel_prepare, strictM6/release/performance/zero-copy/physical heap/peak/OOM and exhaustive compatibility gates remain deferred. Prior request-root/context, parser/GB/vector/extreme Decimal exceptions remain explicit. No whole-package transcreation or PR-readiness claim.
