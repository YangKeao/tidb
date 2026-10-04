# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **if-84**, following **ifnull-83**.

Functional coverage: **223/245 (91.02%)**; strict final-audited count: **0**. The222 prior family objects are byte-identical; only IF is added. **The overall goal remains active.**

## IF branch selection belongs to TiKV

- Head consumes the actual normalized nullable condition and computes Then/Else. Only that report drives one branch callback; Finish consumes the original report and actual selected identity, including NULL. No host-preselected answer followed by IDENTITY.
- AST/typed/PB selectors delegate. Ordinary truth conversion and PB's warning-aware lossy numeric conversion remain distinct and precede head admission. Missing/extra arguments, outer casts, unsigned reinterpretation and temporal FSP retain their original policies.
- Three wire IF workers retain the full Int domain and share the SDK selector with the head and two pure optimizer/proof sites. Fold truth errors still mean Else; proof truth errors still mean unknown.
- Full raw identity domains remain admitted. No eager IF helper, IfString PB signature, legacy SimpleSig, carrier or general driver is added. First-condition preparation retains original context/NoColumns precedence; head → chosen branch → Finish use selected columns.

## Validation

[Evidence](evidence/if-checkpoint.md), [exact commands/hashes](logs/if-summary.txt), [manifest](checkpoint.json), [cumulative ledger](migration-progress.json).

All8new tests pass on first matching execution. SQL54SELECT probes comprise48stored-column root queries (24zero-slot refusals),4lazy-error checks and2filters. Nine locked launches:7green and2unchanged old full-suite RED. TiKV core1/wire4/local334, native root9/gateway196, legacy1 and SQL1 pass; local/root/gateway each retain one ignored test.

Full expression **1588/4old/94ignored** and unistore **216/1old/13ignored** remain RED with byte-identical normalized failure sections. No compile failure, new execution failure, zero-match, interrupted test or fixture recording. The existing vector predicate treats only empty vectors as zero; tests preserve nonempty zero-lane vectors as true rather than changing production to match an older comment.

Pinned formatting and diff checks cover11native/8TiKV Rust files. The123CPP/519native original test bodies are byte-identical; two new modules and eight appended tests. No Cargo/lock, Go/Bazel or generated changes.

## Remaining acceptance

[Remaining review](evidence/remaining-acceptance.md): **8core**, **8ordinary pending**, **6complex exception candidates**—not22approved exceptions. Next: CASE/COALESCE/NULLIF, CAST/M2, IN/extrema/INTERVAL, real request-owner lifecycle and final cross-entry/deletion evidence. Keep the accepted scoped design, not a universal compiler rewrite.

Known full-suite, raw INTDIV, mode forwarding, CAST diagnostic, JSON/metadata, vector-truth and older Values precharge gaps remain. Workspace/lint/dev/bazel_prepare/release, exhaustive differential/TiFlash/FIPS and performance/allocator/physical heap/peak/OOM/zero-copy/dual-timezone footprint are not verified. No whole-package transcreation or PR-readiness claim.

Three Plans agree; the manifest pins their hash and paired TiKV commit. Publish TiKV then TiDB without force push or PR. The old untracked client-differential BUILD.bazel remains excluded.
