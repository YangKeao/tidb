# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **temporal-foundation-74**, following **json-search-73**.

## Progress

Functional migration stays **215/245**; strict final-audited count **0**. This datatype prerequisite adds **no family credit**. Target221 needs6 more, with30 eligible families remaining; all215 prior family objects are unchanged.

TiKV datatype now owns raw-calendar validation, generic civil-to-instant/DST conversion and the original +500ns-to-raw packing. Native conversion methods are thin facades and conversion errors are aliases. Actual caller timezone types and timezone databases remain intact; wire Tz is not substituted for native SessionTimeZone. Wide/raw calendar, leap-second, repeated-time, bounded gap search and panic/cast behavior remain unchanged.

A second shared module owns lexical timezone suffixes, fraction index/source-length FSP and loose date splitting/classification. Native suffix DTOs are aliases with their original Debug label. Full parser/numeric/flag/kind policy remains native; no new evaluator profile, admission, carrier or driver is introduced. This is the foundation for subsequent TIMESTAMP/literal work, not completion of those families.

**Retained compatibility limitation:** prior INTDIV raw-empty coefficient lhs divided by1 used to yield exact zero; shared math rejects it (infallible SDK panic, evaluated infrastructure failure). No native fallback conceals it; arbitrary invalid-raw mathematical parity is unclaimed.

## Validation and evidence

[Evidence](evidence/temporal-foundation-checkpoint.md), [exact commands/hashes](logs/temporal-foundation-summary.txt), [manifest](checkpoint.json), [cumulative ledger](migration-progress.json).

Five exclusive write owners and two bounded read-only reviewers;6 Rust files,2 new modules,2 new CPP tests. Seven Cargo launches:5 nonzero green and2 unchanged old full-suite failures. No new RED, compile failure, interruption, zero-match or retry.

CPP time61 and native full datatype453 pass. Three unchanged session tests pass for LA/London repeated-time choices, timezone-suffix/fractional carry and strict/non-strict DST-gap insertion. Both new SDK tests pass on the first gate. Full expression **1568/4old/94ignored** and unistore **211/1old/13ignored** retain identical failure sections/lists after numeric thread-ID normalization.

CPP51/native74 original test bodies are byte-identical; existing SQL test files are unmodified. Pinned formatting and both diff checks pass. No dependency/lock/tzdata/generated/Go/Bazel changes or fixture recording.

Full temporal parsing/types and remaining evaluator closure, RAND state capability, password Unicode/lazy policy, lexer/digest and plan codec/proto migration, JSON_SUM_CRC32 ARRAY admission, M6/default-NoColumns and earlier compatibility gaps remain open. Whole workspace/lint/dev/bazel_prepare, release/performance/zero-copy/physical heap/peak/OOM and exhaustive differential/TiFlash/FIPS are deferred. No whole-package transcreation or PR-readiness claim.

Three Plans agree; manifest pins their hash and paired TiKV commit. TiKV publishes first, then TiDB, without force push or PR. Old untracked client-differential BUILD.bazel stays excluded. Overall goal continues.
