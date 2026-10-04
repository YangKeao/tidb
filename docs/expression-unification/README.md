# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **temporal-literals-76**, following **temporal-parser-75**.

## Progress

Functional migration is **217/245**, strict final-audited count **0**. Two new families: **date_literal** and **timestamp_literal**. All215 prior family objects are unchanged. Target221 needs4 more, with28 eligible families remaining; overall goal continues.

TiKV workers now own the complete literal regex, parser, date-mode and hard-error policies. Native `time_literal.rs` only restores raw Time, declared FieldType and errors. Actual raw text/mode bits plus the shared owned SessionTimeZone enter a distinct TemporalText carrier. Zone binding is invocation-local and RAII-cleared, without wire-Tz conversion/name reparse. Fixed name capacity participates in logical resource checks; no physical heap/performance claim.

These literals remain **rewrite-time constant folds**, including ODBC syntax. Direct AST/nonconstant ODBC refusals remain; no runtime/PB/unistore admission is added. Internal mangled registry arity records are not executable dispatch. TIMESTAMP literal returns DateTime with its computed FSP. Ordinary TIMESTAMP() remains open because its second coercion is conditional on the first parse. Default NoColumns one-shot ownership remains; explicit `_in` zero-slot tests do not close resolver-owned M6 propagation.

**Retained limitations:** table-projection PlanScopeResolver omits date_modes forwarding and therefore uses original strict defaults despite SET sql_mode. This was confirmed from unchanged prior production source, not a baseline execution replay; it is not repaired or advertised as Go mode parity. The prior CAST zero-date warning and INTDIV raw-empty-lhs/nonzero mismatch also remain.

## Validation and evidence

[Evidence](evidence/temporal-literals-checkpoint.md), [exact commands/hashes](logs/temporal-literals-summary.txt), [manifest](checkpoint.json), [cumulative ledger](migration-progress.json).

Seven exclusive writers, bounded caller review;13 Rust files, one new module, seven new tests. CPP core2/local327+1ignored, native root5/gateway1, corrected SQL1 and original timezone-literal SQL1 pass. Final SQL matrix covers30SELECT probes under both vector flags,20successful typed Time cells and explicit old mode-forwarding refusal boundaries—not per-row literal workers.

Ten locked test commands:6green,2failed new-test assumptions later corrected from prior source,2unchanged old full-suite failures. New tests initially assumed padded DATE parse diagnostics and table-route custom-mode forwarding; only those new expectations were corrected, not production/old tests/fixtures. Full expression **1570/4old/94ignored** and unistore **211/1old/13ignored** retain exact normalized failure sections. All7newtests pass finally;5first gate. No compile failure/interruption/zero-match. One auxiliary source-audit path typo was corrected and disclosed.

CPP198/native358 original test bodies are byte-identical. Pinned format/diff checks pass. No dependency/lock, Go/Bazel/generated/fixture changes. M6, remaining evaluator closure, YEAR/INTERVAL, whole-workspace/lint/dev/bazel_prepare/release, exhaustive differential/TiFlash/FIPS, dual-timezone footprint, allocator/heap/peak/OOM/zero-copy/performance remain deferred. No whole-package transcreation or PR-readiness claim.

Three Plans agree; manifest pins their hash and paired TiKV commit. Publish TiKV then TiDB without force push or PR. Old untracked client-differential BUILD.bazel stays excluded.
