# Expression unification experiment

Checkpoint-ID: `integer-seven-10` (previous: `fixed-args-five-09`)

**22/245 families now delegate to TiKV with native algorithms removed; target221.** This checkpoint adds BIT_COUNT, ~, &, |, ^, << and >> to the fifteen string/checksum families. Final audit/performance/workspace acceptance is still open; this is not overall completion or PR readiness.

## Paired repositories

- [YangKeao/tidb, expression-unification-demo](https://github.com/YangKeao/tidb/tree/expression-unification-demo)
- [YangKeao/tikv, expression-unification-demo](https://github.com/YangKeao/tikv/tree/expression-unification-demo)

Use sibling `tidb` and `tikv` checkouts for path dependencies. `checkpoint.json` pins the paired TiKV commit. Root Plans mirror `/home/agent/tidb/EXPRESSION_UNIFICATION_PLAN.md`. Core-validated steps push both branches without force-push or automatic PRs.

## Shared execution

One synchronous worker/driver and operation-keyed pool handle nullable Bytes, Int bit patterns, Int2, Bytes+Int and Bytes×3, yielding owned Int/Bytes. Each operation fixes canonical slots and its official FnCall. Existing ASCII/Bytes APIs are thin entries; contextless helpers use TiKV one-shot evaluation, never native fallback. Workers are evaluator instances, not threads.

Bitwise algorithms are removed from unary, integer, real and decimal routes. Native coercion, diagnostic order and original NULL-demand points remain; six bitwise results keep UInt, BIT_COUNT signed Int. Existing arithmetic-only fast gates are unchanged. AST/typed/row-vector fallback and public helpers reach the shared implementation. PB/unistore had no admitted signatures for these seven families and were not expanded for credit.

## Actual validation

TiKV190 passed/1 ignored plus1 identity/type guard. New native dispatch3 passed; Session/SQL23 passed. Full expression1382 passed/4 unchanged baseline failures/94 ignored,1480 discovered; complete failure blocks equal09 after only thread-ID normalization. No new-test failure or expected-value modification. Stored-column SQL checks UInt/signed metadata and bit/shift boundaries;16 direct zero-slot calls reject instead of bypassing.

Commands/results and scope: `evidence/integer-seven-checkpoint.md`, `logs/integer-seven-summary.txt`. Functional versus final acceptance is separated in `migration-progress.json`. The maintenance guide documents bit-pattern transport, not a replacement SQL FieldType descriptor. RPC/read-pool/wire behavior is unchanged.

## Deferred and next

Still open: broad frontend operation-scope/wrapper guards, complete scope reuse, release performance, new allocator observations/physical peak or OOM safety, network end-to-end, full workspace and make lint. Old allocation receipts do not certify current artifacts. Evidence uses scoped Jan2026 TiKV and Aug2026 TiDB commands; zero-match tests are not passing evidence. This is kernel reuse, not a complete Go-package transcreation claim.

Next: five boolean families using explicit original truth/presence normalization and closed single/two-function recipes; no finite-only Real/NaN subdomain credit. Next-batch changes are excluded from this publication.
