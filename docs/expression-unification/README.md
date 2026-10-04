# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **ifnull-83**, following **request-scope-82**.

Functional coverage: **222/245 (90.61%)**; strict final-audited count: **0**. The221 prior family objects are byte-identical; only IFNULL is added. **The overall goal remains active.**

## IFNULL selection belongs to TiKV

- Two fixed workers consume actual nullable identities. Head computes Done or NeedSecond; only NeedSecond evaluates the second child and forwards the original report to Finish. Native code projects computed frames, not a retained preselected answer.
- AST, typed, PB and eager-value branches delegate. Three wire workers and the two pure optimizer/proof selectors share one nullable SDK chooser. Original demand, clone timing, missing/extra PB arguments, metadata and return casts remain intact.
- Full native raw identity domains remain admitted. No new carrier, general compiler, SimpleSig or PB signature is added. Existing seven PB signatures reach legacy via Shared.
- First-child preparation keeps the original context and no-owner-before-pool precedence. Head → demanded second → Finish share the selected scope. This does not close broader M6 request-root/live-DAG ownership.

## Validation

[Evidence](evidence/ifnull-checkpoint.md), [exact commands/hashes](logs/ifnull-summary.txt), [manifest](checkpoint.json), [cumulative ledger](migration-progress.json).

All8new tests pass on first matching execution. The SQL test has54SELECT probes:48direct stored-column queries (24zero-slot refusals),4lazy-error checks and2filters. TiKV core1/wire4/local332, native bridge1/gateway196, legacy1 and SQL1 pass; local/gateway each retain one ignored test.

The focused native `ifnull` filter is **6pass/1old catalog failure**, including both new frontend tests passing. Full expression **1585/4old/94ignored** and unistore **215/1old/13ignored** remain RED with unchanged normalized failure sections. Eleven locked launches total:8green (including final narrow-export verification),1known focused RED and2known full RED. No compile failure, new failure, expectation correction, zero-match or interrupted test.

Pinned formatting and diff checks cover11native/8TiKV Rust files. The121CPP/515native original test bodies are byte-identical; no fixtures were recorded. Two new private implementation modules; no Cargo/lock, Go/Bazel or generated changes.

## Remaining acceptance

[Remaining review](evidence/remaining-acceptance.md): **9core**, **8ordinary pending**, **6complex exception candidates**—not23approved exceptions. Next: IF, preserving distinct ordinary/PB truth and warning policies; then CASE/COALESCE/NULLIF, CAST/M2, IN/extrema/INTERVAL, request-owner lifecycle and final cross-entry/deletion evidence. Keep the accepted scoped design, not a universal compiler rewrite.

Known catalog, raw INTDIV, mode forwarding, CAST diagnostic, JSON/metadata and older Values precharge gaps remain. Whole workspace/lint/dev/bazel_prepare/release, exhaustive differential/TiFlash/FIPS and performance/allocator/physical heap/peak/OOM/zero-copy/dual-timezone footprint are not verified. No whole-package transcreation or PR-readiness claim.

Three Plans agree; the manifest pins their hash and paired TiKV commit. Publish TiKV then TiDB without force push or PR. The old untracked client-differential BUILD.bazel remains excluded.
