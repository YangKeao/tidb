# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **case-86**, following **coalesce-85**.

Functional coverage: **225/245 (91.84%)**; strict final-audited count: **0**. All224 previous family objects are byte-identical; only CASE is added. **The overall goal remains active.**

## CASE introduces no SDK profile

Actual condition chains reuse IF Head/Finish. Only computed Then/Else reports select a value or request a later condition; selected NULL stops. Genuine no-ELSE exhaustion uses CoalesceEnd. A statically sole ELSE is evaluated in original preparation, then passed to AnyValue—no fabricated condition/report or admission seed.

AST/typed/PB selectors and pure-fold choice delegate. Three wire CASE functions share the existing IF chooser while retaining full Int and original ownership policies. Simple AST base-once (even zero WHENs/NULL base), rewritten typed per-WHEN evaluation, ordinary/PB truth conversion, fold/proof policies and original SQL branch casts remain distinct. Private IF decoding/finish helpers are reused; its existing regression passes.

## Validation

[Evidence](evidence/case-checkpoint.md), [exact commands/hashes](logs/case-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

All7new tests pass on first matching execution. SQL54SELECT probes:48searched column-root cases (24zero-slot refusals),4lazy-error checks and2positive-only simple-CASE filters. Existing SQL branch casts explain Decimal1.500/Datetime.000; simple-CASE filters are not claimed as CASE zero-slot roots or AST base-once evidence.

Ten locked launches:8green,2unchanged old full RED. CPP core1/wire2/local337+1ignored; native root18, IF regression1, gateway196+1ignored, six-signature legacy1 and SQL1 pass. Full expression **1594/4old/94ignored** and unistore **218/1old/13ignored** retain byte-identical normalized failure sections. No compile failure, new execution failure, zero-match, interrupted test or fixture recording.

Pinned formatting/diff checks cover9native/2TiKV Rust files. One new native module and seven new tests;19CPP/318native original test bodies are byte-identical. No new profile/carrier/codec/driver/budget/PB admission, Cargo/lock, Go/Bazel or generated changes.

## Remaining acceptance

[Remaining review](evidence/remaining-acceptance.md): **6core**, **8ordinary pending**, **6complex exception candidates**—not20approved exceptions. Next NULLIF with original eager operand demand, CAST/M2, IN/extrema/INTERVAL, true request-owner lifecycle and final cross-entry evidence. Continue the accepted scoped design, not a universal compiler rewrite.

Known baseline failures and documented INTDIV/CAST/mode/JSON/vector/older Values gaps remain. Workspace/lint/dev/bazel_prepare/release, exhaustive differential/TiFlash/FIPS, performance/physical heap or stack peak/OOM/allocator/zero-copy/dual-timezone footprint and complete Go-package transcreation are unverified. No PR-readiness claim.

Three Plans agree; the manifest pins their hash and paired TiKV commit. Publish TiKV then TiDB without force push or PR. Unrelated untracked client-differential BUILD.bazel remains excluded.
