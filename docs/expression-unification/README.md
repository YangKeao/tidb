# Expression unification experiment

Checkpoint-ID: `shared-text-four-08` (previous: `shared-bytes-five-07`)

**Ten families now delegate to TiKV and have their native algorithms removed:** ASCII, LENGTH/OCTET_LENGTH, BIT_LENGTH, LTRIM, RTRIM, UNHEX, CRC32, REVERSE, CHAR_LENGTH/CHARACTER_LENGTH and QUOTE. Functional progress:10/245, target221. Final audit/performance/workspace acceptance remains open; this is not overall completion or PR readiness.

## Paired repositories

- [YangKeao/tidb, expression-unification-demo](https://github.com/YangKeao/tidb/tree/expression-unification-demo)
- [YangKeao/tikv, expression-unification-demo](https://github.com/YangKeao/tikv/tree/expression-unification-demo)

Use sibling checkouts named `tidb` and `tikv` for Rust path dependencies. `checkpoint.json` records the paired TiKV commit. Root Plans mirror `/home/agent/tidb/EXPRESSION_UNIFICATION_PLAN.md`. Validated checkpoints push both branches, without force-push or automatic PRs.

## Implementation

All migrated operations share one synchronous evaluator/driver and operation-keyed pool, not threads or per-function pools. Incompatible cached workers retire before reservations are released. Explicit capabilities are borrowed; contextless callers use a closed TiKV one-shot evaluator, never native fallback. Cold preparation/cache switching and complete operation-scope reuse remain performance follow-ups.

TiDB retains argument demand/coercion/normalization and SQL metadata. CHAR_LENGTH includes typed PB, unistore Shared and legacy SimpleSig/public-helper routes. Main/PB uses Go invalid-byte normalization; legacy preserves its pre-existing Rust grouping. QUOTE preserves Rust normalization and delegates NULL→"NULL" plus escaping. CRC32 raw UInt packing is preserved; pre-existing SQL inference remains signed LongLong.

## Validation

TiKV180 passed/1 ignored plus1 dispatch-identity guard. New native/PB dispatcher2 passed; Session/SQL19 passed; legacy unistore1 passed. Full expression1376 passed/4 unchanged baseline failures/94 ignored; complete failure blocks match07 after only thread-ID normalization. A new SQL fixture initially assumed unsigned CRC32 column metadata; corrected against unchanged inference, not by changing production behavior or payload expectations.

Exact commands/results: `evidence/shared-text-four-checkpoint.md` and `logs/shared-text-four-summary.txt`. `migration-progress.json` separates functional progress from final acceptance. The user requested faster functional migration: comprehensive audits, allocator remeasurement, release performance and whole-workspace/make lint checks remain explicit follow-ups. No physical heap/peak/OOM guarantee or current-artifact coverage by old allocation receipts is claimed.

## Reproduction and next batch

Evidence scopes commands to `tikv/` (Jan2026 compiler) or `tidb/rust/` (Aug2026). Do not mix profiles/artifacts or count zero matched tests as passing. Binaries/caches are not published. Historical baselines remain in earlier evidence documents. This is expression-kernel reuse, not a complete Go-package transcreation claim.

Next: HEX/BIN/LEFT/RIGHT/REPLACE with closed Int/Bytes fixed-arity arguments through the same driver; next-batch workspace changes are excluded from this publication.
