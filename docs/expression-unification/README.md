# Expression unification experiment

Checkpoint-ID: `case-sha2-ord-four-18` (previous: `packet-string-four-17`)

**55/245 families delegate to TiKV with their native evaluator algorithms removed; target 221.** This checkpoint adds LOWER, UPPER, SHA2 and ORD. Strict final-audited acceptance remains 0; this is not PR-ready.

## Paired repositories

- [YangKeao/tidb, expression-unification-demo](https://github.com/YangKeao/tidb/tree/expression-unification-demo)
- [YangKeao/tikv, expression-unification-demo](https://github.com/YangKeao/tikv/tree/expression-unification-demo)

Use sibling checkouts. `checkpoint.json` pins TiKV and the published Plan hash. Root Plans mirror `/home/agent/tidb/EXPRESSION_UNIFICATION_PLAN.md` at publication. Validated steps push both branches without force-push or automatic PRs.

## Casing, SHA2 and ORD ownership

LOWER/UPPER binary signatures execute the actual no-op kernels. UTF8 signatures privately bind existing EncodingUtf8Mb4 getters and their Go simple Unicode tables; no new table or casing implementation is added. This avoids the charset-dependent wire selector while preserving canonical empty-charset/zero-heap metadata. Native code retains only per-malformed-byte RuneError normalization and packing. Aliases and existing PB binary/UTF8/NULL routes delegate too.

SHA2 has one TiKV selector/digest/hex core. Invalid wire selectors still return NULL and warning1583; the native private recipe returns quiet NULL without clearing warnings. Original byte/integer coercion and NULL-left demand remain. ReadyIntArg replaces the packet-specific name; the separate BytesIntReady role validates Undemanded length before using an irrelevant Some(0), never an evaluated SQL NULL.

ORD keeps native argument-charset/first-character preparation, including typed ETString order, then delegates its base256 fold. The facade and both ready matchers validate the proven four-byte prepared domain, without truncation or new budgets. Original wire return-collation decoding/NULL0 stays separate from native NULL. Existing latin1 is byte-preserving: ORD of stored UTF8 'é' remains195, not a silently corrected233.

No new SHA2/ORD PB/unistore admission, driver, pool, limits or native fallback is introduced.

## Actual validation

- TiKV local: 211 passed/1 ignored; original string: 63 passed; original encryption: 8 passed.
- Native dispatcher retry: 3 passed; SQL/lifecycle: 39 passed.
- Full expression: 1403 passed/4 unchanged failures/94 ignored, 1501 discovered. Complete failure blocks match17 after only thread-ID normalization.

The first native compile failed before tests because three new test references used a nonexistent crate-root Expression path. Only those paths were corrected to the existing expression module. No runtime or old expected value was changed; the initial chain did not reach SQL/full tests.

SQL checks five stored rows and12 results including aliases, byte/charset prechecks, fixed published SHA256 values and invalid-selector quiet NULL, plus eight direct zero-slot refusals. Full unistore was not rerun. Exact commands: `logs/case-sha2-ord-four-summary.txt`; boundaries: `evidence/case-sha2-ord-four-checkpoint.md`.

## Next work and exclusions

TRIM, SUBSTRING_INDEX, LPAD and RPAD are the next parallel batch, not yet credited. Explicit demand markers and native/wire policy differences remain necessary; pad may narrowly require four fixed operands/five nodes rather than fake typed inputs or partial-arity credit.

Variadic/lazy CONCAT, INSERT offsets, GB collation residuals and other documented compatibility gaps remain. Broad operation-scope guards, physical peak/OOM safety, allocator remeasurement, paired differential reruns, full workspace, make lint and release performance remain unverified. Kernel reuse is not complete Go-package transcreation.
