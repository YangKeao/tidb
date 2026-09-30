# Expression unification experiment

Checkpoint-ID: `logical-three-13` (previous: `hash-two-12`)

**32/245 families delegate to TiKV with their native evaluator algorithms removed; target221.** This checkpoint adds AND, OR and XOR. Final audit, performance and whole-workspace acceptance remain open.

## Paired repositories

- [YangKeao/tidb, expression-unification-demo](https://github.com/YangKeao/tidb/tree/expression-unification-demo)
- [YangKeao/tikv, expression-unification-demo](https://github.com/YangKeao/tikv/tree/expression-unification-demo)

Use sibling `tidb` and `tikv` checkouts. `checkpoint.json` pins the paired TiKV commit. Root Plans mirror `/home/agent/tidb/EXPRESSION_UNIFICATION_PLAN.md`. Validated steps push both branches without force-push or automatic PRs.

## Logical execution

One synchronous worker/driver and operation-keyed pool remain. LogicalFunction/LogicalArgs preserve the frontend's original child demand. Only AND(false, undemanded) and OR(true, undemanded) permit an explicit irrelevant RHS representative. Invalid markers, including NULL-left and XOR, fail ScopeContract before any factory or dispatch. The representative is not evaluated RHS data, and even short-circuit answers come from the real official kernel.

Closed-ready AND/OR lowering checks the exact signature/control tag and complete ordered arguments, then emits the official eager FnCall. Ordinary Row/wire control stays lazy. No driver, carrier or limit expansion: compile depth3/nodes4 remains.

AST/helper and legacy unistore retain eager evaluation; typed/PB AND/OR retain lazy demand and evaluate RHS for NULL-left. Original ignored warning-probe errors versus numeric_arg? diagnostics remain distinct. BETWEEN also uses the shared AND. Existing vector/selection consumers are untouched; XOR gains no PB/unistore admission. Public contextless helpers still use TiKV, never native fallback.

## Actual validation

TiKV194 passed/1 ignored plus1 identity guard; new caller dispatcher3 passed; SQL/lifecycle29 passed; legacy unistore1 passed. Full expression1390 passed/4 unchanged baseline failures/94 ignored,1488 discovered. Complete failure blocks match12 after only thread-ID normalization.

New SQL checks all27 three-valued table cells and9 direct zero-slot refusals, including absorbing-left cases. SQL/Go expected values are unchanged. One previous NOT BETWEEN instrumentation expectation changes from1 to2 facade invocations because AND and NOT now both delegate; different workers' counters are not subtracted as a total.

Exact commands and scope: `evidence/logical-three-checkpoint.md`, `logs/logical-three-summary.txt`.

## Open work

Next: four INET families with original conversion and UInt/binary/text metadata. Four IS_IP predicates, SHA2, compression and ORD retain explicit compatibility gaps and receive no partial-domain credit. Raw floating-point helper sharing is being evaluated without weakening Real/NotNan invariants. Broad operation-scope guards, release performance, allocator remeasurement, physical peak/OOM safety, network end-to-end, full workspace and make lint remain unverified. This is kernel reuse, not a complete Go-package transcreation or PR-readiness claim.
