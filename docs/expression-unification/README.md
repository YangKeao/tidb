# Expression unification experiment

Checkpoint-ID: `ascii-sql-activation-06` (previous: `session-runtime-lifetime-05`)

**SQL ASCII now uses TiKV; its native algorithm is deleted.** Functional delegation/deletion progress is1/245 families (target221). Comprehensive final-audit/performance gates remain separate and open; this is not overall completion or PR readiness.

## Paired repositories

- [YangKeao/tidb, expression-unification-demo](https://github.com/YangKeao/tidb/tree/expression-unification-demo)
- [YangKeao/tikv, expression-unification-demo](https://github.com/YangKeao/tikv/tree/expression-unification-demo)

Keep sibling checkouts named `tidb` and `tikv` for Rust path dependencies. `checkpoint.json` records the paired TiKV commit. Both root Plans are publication mirrors of `/home/agent/tidb/EXPRESSION_UNIFICATION_PLAN.md`. Core-validated steps push both branches without force-push or automatic PR creation.

## This checkpoint

- ASCII uses an existing operation scope, a borrowed execution, or a TiKV one-shot evaluator if no capability exists. No native fallback, including NULL. Evaluators are ordinary synchronous objects, not threads.
- Session's explicit pool policy, stable root, context COW and captured result closer remain intact. Real zero-slot SQL now fails through the native PoolResource adapter rather than computing natively.
- Six unary kernels share one TiKV evaluator: ASCII, LENGTH/OCTET_LENGTH, BIT_LENGTH, LTRIM, RTRIM and UNHEX. The last five are backend-ready only; TiDB bindings come next. Old ASCII API is a thin wrapper, not another implementation.

## Actual validation

TiKV local176 passed/1 ignored plus1 closed-operation guard test; ASCII dispatcher6 passed (including2MiB input); Session runtime/SQL15 passed. Real column SQL covers NULL/empty/binary/UTF-8 under one-slot and zero-slot policies. Full expression1370 passed/4 unchanged baseline failures/94 ignored; all four complete failure blocks match05 after only thread-ID normalization.

Exact commands and limits: `evidence/ascii-sql-activation-checkpoint.md`. `migration-progress.json` separates functional migration from backend-only work and final acceptance. Full local raw logs are under expression-unification/logs/; the published evidence summarizes actual runs without copying repeated compiler warnings.

The user requested faster functional migration. Broad audits, allocation remeasurement and release performance are follow-ups, not per-cut blockers. One-shot cold-start cost, full operation-scope reuse, broader wrapper propagation, network end-to-end, whole workspace and make lint remain unverified. The last192-byte independent allocation-request observation belongs to checkpoint04, not this modified backend/new binary. No physical heap/peak/OOM guarantee is claimed.

## Reproduction

The experiment uses separate Jan2026 TiKV and Aug2026 TiDB compiler/target profiles; evidence states each command's working directory. Binaries/caches are not published. Do not count zero matched tests as passing or link arbitrary artifacts. Historical baselines and receipts remain in `evidence/validation-baseline.md` and earlier checkpoint documents. This is expression-kernel reuse, not a complete upstream Go-package transcreation claim.
