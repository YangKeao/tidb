# Expression unification experiment

Checkpoint-ID: `fixed-args-five-09` (previous: `shared-text-four-08`)

**Fifteen families delegate to TiKV with their native algorithms removed:** ASCII, LENGTH/OCTET_LENGTH, BIT_LENGTH, LTRIM, RTRIM, UNHEX, CRC32, REVERSE, CHAR_LENGTH/CHARACTER_LENGTH, QUOTE, HEX, BIN, LEFT, RIGHT and REPLACE. Functional progress15/245; target221. Final audit/performance/workspace acceptance is still open, not overall completion or PR readiness.

## Paired repositories

- [YangKeao/tidb, expression-unification-demo](https://github.com/YangKeao/tidb/tree/expression-unification-demo)
- [YangKeao/tikv, expression-unification-demo](https://github.com/YangKeao/tikv/tree/expression-unification-demo)

Keep sibling checkouts named `tidb` and `tikv` for path dependencies. `checkpoint.json` records the paired TiKV commit. Root Plans mirror `/home/agent/tidb/EXPRESSION_UNIFICATION_PLAN.md`. Core-validated steps push both branches without force-push or automatic PR creation.

## One evaluator, fixed argument shapes

The same synchronous worker/driver and operation-keyed pool now support nullable Bytes, Int bit patterns, Bytes+Int and three Bytes inputs, yielding owned Int/Bytes. Each operation fixes canonical slots and one official FnCall; no arbitrary program or SQL schema API. ASCII/Bytes methods are thin compatibility entries. Contextless calls use TiKV one-shot evaluation, never native fallback. These workers are not threads.

TiDB retains original coercion, demand order, normalization and result metadata. HEX includes typed Int/BIT/UInt and Bytes branches; LEFT/RIGHT cover binary/text and count-first NULL behavior; REPLACE preserves conversion of later tuple arguments even after NULL. Caller compile limit4 admits the fixed three-input recipe; root/epoch/retirement accounting is unchanged.

## Actual validation

TiKV186 passed/1 ignored plus1 identity guard. New dispatcher3 passed; Session/SQL21 passed. Full expression1379 passed/4 unchanged baseline failures/94 ignored; complete failure blocks equal08 after only thread-ID normalization. No new-test failure or expected-value change this checkpoint. Real mixed-column SQL exercises high-bit integers and multibyte strings;20 direct zero-slot SQL calls reject instead of bypass/replay.

Exact commands and limits: `evidence/fixed-args-five-checkpoint.md`, `logs/fixed-args-five-summary.txt`. `migration-progress.json` separates functional progress from final acceptance. TiKV's coprocessor maintenance guide now records the closed in-process argument/ownership contract; RPC wire/read-pool behavior is unchanged.

## Deferred and next

Not verified: release performance, complete operation-scope reuse/wrapper propagation, new allocation measurements/physical peak or OOM safety, network end-to-end, full workspace or make lint. Old allocation receipts do not certify current artifacts. Historical compatibility remains: main/PB versus legacy CHAR_LENGTH normalization, QUOTE's Rust normalization, and raw CRC32 UInt versus existing signed SQL inference.

Evidence scopes Jan2026 TiKV and Aug2026 TiDB commands to their respective directories. Do not mix compiler artifacts or count zero matched tests as passing. This is kernel reuse, not a complete Go-package transcreation claim. Next: seven integer-bitwise families; their workspace changes are excluded from this publication.
