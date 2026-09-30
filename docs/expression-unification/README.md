# Expression unification experiment

Checkpoint-ID: `boolean-five-11` (previous: `integer-seven-10`)

**27/245 families delegate to TiKV with native algorithms removed; target221.** This checkpoint adds NOT, ISNULL, ISTRUE, ISFALSE and ISTRUE_WITH_NULL, including their supported aliases/negated forms. Final audit, performance and whole-workspace acceptance remain open.

## Paired repositories

- [YangKeao/tidb, expression-unification-demo](https://github.com/YangKeao/tidb/tree/expression-unification-demo)
- [YangKeao/tikv, expression-unification-demo](https://github.com/YangKeao/tikv/tree/expression-unification-demo)

Use sibling `tidb` and `tikv` checkouts. `checkpoint.json` pins the paired TiKV commit. Root Plans mirror `/home/agent/tidb/EXPRESSION_UNIFICATION_PLAN.md`. Core-validated steps push both branches without force-push or automatic PRs.

## Shared execution

One synchronous worker/driver and operation-keyed pool handle closed ready-value recipes. Boolean callers retain their original truth/presence conversion across all supported native types, then pass nullable Int0/1. This is not Int-only SQL admission. Three negated tests use a fixed base+UnaryNot pair with exact ordered signature/name/function-pointer validation; other operations remain single-call. Compile depth3/nodes4 accommodates these recipes without opening arbitrary programs. Workers are not threads; contextless callers still use TiKV one-shot evaluation, never native replay.

The closed public BooleanFunction/eval_boolean_ready_in helper also connects legacy unistore. Ordinary versus PB warning behavior, typed UNKNOWN validation versus AST presence, and legacy integer-versus-datum channels stay distinct. Existing NOT/ISNULL vector answer-producing shortcuts are removed; they decline before evaluating children and use the existing context-aware row route. Syntactic NOT IN/BETWEEN/LIKE/REGEXP only moves the negation, not the underlying predicate or child order. No PB/SQL admission was expanded.

## Actual validation

TiKV192 passed/1 ignored plus1 exact-chain guard. New dispatcher3 passed; SQL/lifecycle25 passed; legacy unistore1 passed. Full expression1385 passed/4 unchanged baseline failures/94 ignored,1483 discovered. Complete failure blocks match10 after only thread-ID normalization. No old expected values changed.

SQL exercises13 existing spellings over NULL/zero/nonzero values, with28 direct zero-slot projection/filter refusals. ISTRUE_WITH_NULL has no ordinary SQL return-type admission and is verified through existing native/PB routes instead. Exact commands and scope: `evidence/boolean-five-checkpoint.md`, `logs/boolean-five-summary.txt`.

## Open work

Broad frontend operation-scope guards, complete scope reuse, release performance, allocator remeasurement, physical peak/OOM safety, network end-to-end and full workspace/make lint remain unverified. Old allocation receipts do not certify current artifacts. This is kernel reuse, not a complete Go-package transcreation claim. The next hash/compression/string candidates require compatibility checks before receiving migration credit.
