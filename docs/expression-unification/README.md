# Expression unification experiment

Checkpoint-ID: `hash-two-12` (previous: `boolean-five-11`)

**29/245 families delegate to TiKV with their native evaluator algorithms removed; target221.** This checkpoint adds MD5 and SHA/SHA1. SHA is an alias, not a third family. Final audit, performance and whole-workspace acceptance remain open.

## Paired repositories

- [YangKeao/tidb, expression-unification-demo](https://github.com/YangKeao/tidb/tree/expression-unification-demo)
- [YangKeao/tikv, expression-unification-demo](https://github.com/YangKeao/tikv/tree/expression-unification-demo)

Use sibling `tidb` and `tikv` checkouts. `checkpoint.json` pins the paired TiKV commit. Root Plans mirror `/home/agent/tidb/EXPRESSION_UNIFICATION_PLAN.md`. Validated steps push both branches without force-push or automatic PRs.

## Shared execution and deletion scope

One synchronous worker/driver and operation-keyed pool execute closed ready-value recipes. These two hashes use the existing nullable Bytes-to-Bytes single-call route; no new driver, pool, type carrier or limits. Native `hash_input`, charset conversion and original text packing remain. The official kernels own hashing and lowercase hex generation, including NULL wrappers. OpenSSL errors propagate as runtime failures, never native retries.

The old generic MD5/SHA1 digest calculation and imports are removed from the expression implementation. Other SHA2/SM3 consumers retain shared input/hex helpers. PASSWORD's separate parser/auth double-SHA1 implementation remains an unmigrated family: this is not a claim that all SHA1 code across the repository is gone. No PB/unistore admission was added.

The preceding boolean batch preserves frontend truth/presence and demand semantics, uses fixed base+UnaryNot recipes for negated predicates, and replaces native vector answer shortcuts with existing context-aware row evaluation. Compile limits remain depth3/nodes4. Workers are evaluator instances, not threads; contextless calls still use TiKV, never native fallback.

## Actual validation

TiKV193 passed/1 ignored plus1 identity guard; new native dispatcher2 passed; SQL/lifecycle27 passed. Full expression1387 passed/4 unchanged baseline failures/94 ignored,1485 discovered. Complete four failure blocks match11 after only thread-ID normalization. No existing expected values changed.

New SQL covers binary/text columns, NULL, empty strings, embedded NUL, invalid UTF-8 bytes and all three spellings. Twelve direct zero-slot queries reject instead of bypassing. Fixed SQL digests were computed with independent Python hashlib, not recorded from TiKV. OpenSSL-error runtime injection was not tested.

Exact commands and scope: `evidence/hash-two-checkpoint.md`, `logs/hash-two-summary.txt`.

## Open work

AND/OR/XOR require preserving each original eager/lazy demand and diagnostic path. SHA2, compression functions and ORD still have explicit warning/format/NULL/charset compatibility gaps; no partial-domain credit is given. Broad operation-scope guards, release performance, allocator remeasurement, physical peak/OOM safety, network end-to-end, full workspace and make lint remain unverified. Old allocation receipts do not certify current artifacts. This is kernel reuse, not a complete Go-package transcreation claim or PR-readiness claim.
