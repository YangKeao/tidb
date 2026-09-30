# Expression unification experiment

Checkpoint-ID: `inet-four-14` (previous: `logical-three-13`)

**36/245 families delegate to TiKV with their native evaluator algorithms removed; target221.** This checkpoint adds INET_ATON, INET_NTOA, INET6_ATON and INET6_NTOA. Final audit, performance and whole-workspace acceptance remain open.

## Paired repositories

- [YangKeao/tidb, expression-unification-demo](https://github.com/YangKeao/tidb/tree/expression-unification-demo)
- [YangKeao/tikv, expression-unification-demo](https://github.com/YangKeao/tikv/tree/expression-unification-demo)

Use sibling `tidb` and `tikv` checkouts. `checkpoint.json` pins the paired TiKV commit. Root Plans mirror `/home/agent/tidb/EXPRESSION_UNIFICATION_PLAN.md`. Validated steps push both branches without force-push or automatic PRs.

## INET execution and deletion scope

One synchronous worker/driver and operation-keyed pool remain. All four functions reuse existing nullable Bytes/Int shapes and official single-call kernels, including NULL inputs. No new driver, carrier, limits or admission surfaces.

ATON retains checked text conversion and UInt packing. NTOA retains Int/UInt bit transport and the original other-type truncation warning/error policy before signed conversion; the kernel decides u32 range and formatting. INET6 preserves raw String/Bytes payloads, including malformed UTF-8, for the kernel to parse or reject. Its ATON result remains binary and NTOA remains text.

Native parsing, shifts, range decisions and formatting for these four routes are deleted, including the now-unused inet6_aton_text helper. Shared conversion helpers and four IS_IP predicate algorithms remain: their NULL behavior differs, and IS_IPV4 also differs on leading-zero parsing. This is not a claim that all IP-related code is gone. No PB/unistore admission is added.

Previous logical operators preserve original eager/lazy demand through explicit validated undemanded-RHS markers; their actual results also come from TiKV. Ordinary wire/control semantics are unchanged. Workers are evaluator instances, not threads; contextless helpers still delegate, without native fallback.

## Actual validation

TiKV197 passed/1 ignored plus1 identity guard; new caller dispatcher2 passed; SQL/lifecycle31 passed. Full expression1392 passed/4 unchanged baseline failures/94 ignored,1490 discovered. Complete failure blocks match13 after only thread-ID normalization.

New SQL covers seven rows of fixed network-order constants, nullable text/integer/binary columns and eight direct zero-slot refusals. No expected values were recorded from the engine or changed in this checkpoint. The previous NOT BETWEEN instrumentation change belongs to13, not this diff.

Exact commands and scope: `evidence/inet-four-checkpoint.md`, `logs/inet-four-summary.txt`.

## Open work

Next: six raw floating-point families, with explicit IEEE-bit identity and shared TiKV math primitives, without weakening Real/NotNan. Legacy ASIN/ACOS can expose NaN through casts/comparisons, unlike ordinary native NULL policy; error propagation and Decimal SIGN normalization require preservation. Four IS_IP predicates, SHA2, compression and ORD retain documented compatibility gaps and receive no partial-domain credit.

Broad operation-scope guards, release performance, allocator remeasurement, physical peak/OOM safety, network end-to-end, full workspace and make lint remain unverified. This is kernel reuse, not complete Go-package transcreation or PR readiness.
