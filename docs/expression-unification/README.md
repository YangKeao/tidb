# Expression unification experiment

Checkpoint-ID: `shared-bytes-five-07` (previous: `ascii-sql-activation-06`)

**Six SQL families now use TiKV; their native algorithms are deleted:** ASCII, LENGTH/OCTET_LENGTH, BIT_LENGTH, LTRIM, RTRIM and UNHEX. Functional delegation/deletion progress is6/245 families (target221). Comprehensive final-audit/performance gates remain separate and open; this is not overall completion or PR readiness.

## Paired repositories

- [YangKeao/tidb, expression-unification-demo](https://github.com/YangKeao/tidb/tree/expression-unification-demo)
- [YangKeao/tikv, expression-unification-demo](https://github.com/YangKeao/tikv/tree/expression-unification-demo)

Keep sibling checkouts named `tidb` and `tikv` for Rust path dependencies. `checkpoint.json` records the paired TiKV commit. Both root Plans are publication mirrors of `/home/agent/tidb/EXPRESSION_UNIFICATION_PLAN.md`. Core-validated steps push both branches without force-push or automatic PR creation.

## This checkpoint

- ASCII uses an existing operation scope, a borrowed execution, or a TiKV one-shot evaluator if no capability exists. No native fallback, including NULL. Evaluators are ordinary synchronous objects, not threads.
- Session's explicit pool policy, stable root, context COW and captured result closer remain intact. Real zero-slot SQL now fails through the native PoolResource adapter rather than computing natively.
- All six unary families now share one TiKV evaluator and one operation-keyed pool. Incompatible cached/idle workers retire before their reservations are released. No per-op pool, new-root bypass or native replay. Public LENGTH helper/typed row calls also delegate; original coercion and result packing remain.

## Actual validation

New shared-pool dispatch4 passed (including2MiB byte results); Session runtime/SQL17 passed. Mixed one-slot SQL covers all five new families and aliases; zero-slot SQL rejects12 NULL/non-NULL calls instead of replay/bypass. Full expression1374 passed/4 unchanged baseline failures/94 ignored; complete failure blocks match06 after only thread-ID normalization. The new mixed SQL fixture initially used the wrong tagged Datum representation; corrected to unchanged chunk String+Binary materialization without changing payload expectations or production code.

Exact commands and limits: `evidence/shared-bytes-five-checkpoint.md`. `migration-progress.json` separates functional migration from final acceptance. The six-operation TiKV product is unchanged from06 in this checkpoint. Full local raw logs are under expression-unification/logs/; the published evidence summarizes actual runs without copying repeated compiler warnings.

The user requested faster functional migration. Broad audits, allocation remeasurement and release performance are follow-ups, not per-cut blockers. One-shot cold-start cost, full operation-scope reuse, broader wrapper propagation, network end-to-end, whole workspace and make lint remain unverified. The last192-byte independent allocation-request observation belongs to checkpoint04, not this modified backend/new binary. No physical heap/peak/OOM guarantee is claimed.

## Reproduction

The experiment uses separate Jan2026 TiKV and Aug2026 TiDB compiler/target profiles; evidence states each command's working directory. Binaries/caches are not published. Do not count zero matched tests as passing or link arbitrary artifacts. Historical baselines and receipts remain in `evidence/validation-baseline.md` and earlier checkpoint documents. This is expression-kernel reuse, not a complete upstream Go-package transcreation claim.
