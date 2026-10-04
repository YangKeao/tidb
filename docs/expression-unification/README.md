# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **request-scope-82**, following **from-unixtime-81**.

Functional coverage remains **221/245 (90.20%)**, strict final-audited count **0**. All221 family objects are unchanged; this integration fix adds no family credit. **The overall goal remains active.**

## Existing-owner propagation

- DATE/TIMESTAMP/ODBC literal rewriting now supplies PlanScopeResolver's existing context to the scoped worker. Resolver timezone/modes and ordinary fold-warning bookkeeping remain authoritative. The old table-projection mode-default gap is not repaired.
- Legacy Shared children borrow available parent scope/execution while keeping their original row/settings/warning context. Active child scope retains priority; no-parent callers keep the old path. This does not create a live DAG request owner or close every NoColumns/default root.
- TiKV Rust, kernels, SDK, dependencies and physical formats are unchanged. Native changes are limited to two forwarding seams, a narrow resolver hook and test-only old literal convenience wrappers.

## Validation

[Evidence](evidence/request-scope-checkpoint.md), [exact commands/hashes](logs/request-scope-summary.txt), [manifest](checkpoint.json), [cumulative ledger](migration-progress.json).

All3new regression tests have genuine **fail-before / pass-after** evidence. Planner1, legacy1 and the SQL temporal-literal group5 pass after the fix. The new SQL test executes18SELECT probes, including six zero-slot refusals and ordinary-column controls. Two initial new-test fixture errors were corrected from existing source, without changing expected SQL results or production to accommodate them.

Full expression **1582/4old/94ignored** and unistore **214/1old/13ignored** remain RED with byte-identical normalized failure sections. Ten locked launches total: two fixture failures, three reproduced defects, three green post-fix runs and two unchanged old full-suite failures. No compile failure, zero-match, interrupted test or fixture recording. Five native Rust files pass pinned formatting; original test bodies remain unchanged. No TiKV Rust, Cargo/lock, Go/Bazel or generated changes.

## Remaining acceptance

[Remaining acceptance review](evidence/remaining-acceptance.md) distinguishes **10 core families**, **8 ordinary pending families**, and **6 complex exception candidates**. They are not24approved exceptions. Next: IF/IFNULL and the remaining conditional core, CAST/M2, IN/extrema/INTERVAL, real request-owner lifecycle and final cross-entry/deletion evidence. Keep the accepted synchronous/scoped design; do not restart a universal compiler rewrite.

Known raw INTDIV, mode forwarding, CAST diagnostic and JSON/metadata gaps remain disclosed. Whole workspace/lint/dev/bazel_prepare/release, exhaustive differential/TiFlash/FIPS and performance/allocator/physical heap/peak/OOM/zero-copy/dual-timezone footprint are not verified. No whole-package transcreation or PR-readiness claim.

Three Plans agree; the manifest pins their hash and paired TiKV commit. Publish TiKV then TiDB without force push or PR. The old untracked client-differential BUILD.bazel stays excluded.
