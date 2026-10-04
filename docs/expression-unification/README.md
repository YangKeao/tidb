# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **timestamp-78**, following **convert-tz-77**.

## Progress

Functional migration is **219/245**, strict final-audited count **0**. New family: **timestamp**. All218 prior family objects are unchanged. Target221 needs2 more, with26 eligible families remaining; the overall goal continues.

TiKV `native_timestamp.rs` owns ordinary TIMESTAMP parsing, warning text, formatting, duration grammar/arithmetic/range/FSP. Native retains original coercion/context demand and computed projection only. Four fixed profiles handle actual NULL, one-argument head, two-argument base and duration addition. A successful two-argument base is the actual SDK Time identity frame, moved unchanged into the second worker after RHS coercion—even for yearzero. No native parser/formatter/arithmetic or host-built base remains.

The new scoped callback shares the existing three-way router and selected scope after lease finish. It does not rediscover authority, retain worker borrows or create a second pool. NoColumns keeps its old preparation-before-allocation order, with its one-shot owner alive through both stages. Actual text/source-kind/zone uses a distinct carrier, not literal DateModes. Zone-only binding is shared while literal policies remain separate. A small datatype-owned seven-field view avoids duplicate bit masks.

Original typed post-wrapping and eager SQL child evaluation remain unchanged. No PB/legacy admission is added. FROM_UNIXTIME and UNIX_TIMESTAMP still need their actual conditional getters/clock and independent legacy policies; the staged helper alone earns neither family credit.

## Validation and evidence

[Evidence](evidence/timestamp-checkpoint.md), [exact commands/hashes](logs/timestamp-summary.txt), [manifest](checkpoint.json), [cumulative ledger](migration-progress.json).

Nine locked launches: datatype1, CPPcore2/local330+1ignored, native timestamp30/gateway196+1ignored and SQL1 pass. SQL has42real-column probes:20normal,20direct target-root zero-slot refusals and2filters. Numeric-vs-string parsing, actual/declared FSP differences and warning demand are explicit. New gateway tests cover three authority routes and success/error/panic/close; real TIMESTAMP stages also run in one slot.

An initial compile attempt found two wrong String constructors in the new gateway test; static str fixed them without production/oracle changes. All9newtests pass their first executed matching gate. Full expression **1574/4old/94ignored** and unistore **211/1old/13ignored** retain exact normalized failure sections and remain RED. No interrupted or zero-match run.

CPP252/native361 original test bodies are byte-identical.15Rust files, one new module, nine new tests; pinned format/diff checks pass. Dependencies/locks, Go/Bazel/generated/fixtures unchanged.

M6/default-NoColumns propagation, remaining evaluator closure, old planner mode forwarding/CAST warning/INTDIV raw-empty and other compatibility gaps remain open. Whole workspace/lint/dev/bazel_prepare/release, exhaustive differential/TiFlash/FIPS, performance/allocator/physical heap/peak/OOM/zero-copy/dual-timezone footprint are deferred. No whole-package transcreation or PR-readiness claim.

Three Plans agree; manifest pins their hash and paired TiKV commit. Publish TiKV then TiDB without force push or PR. Old untracked client-differential BUILD.bazel stays excluded.
