# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **decimal-policy-89**, following **cast-real-uint-88**.

**M2 type deduplication, not new evaluator-family credit.** Functional coverage remains **226/245 (92.24%)**, strict final-audited count **0**. All226 family objects and the19-family remainder are unchanged. The overall goal is active.

## Two native algorithm bodies removed

- `mydecimal::format_float_g_shortest` delegates to TiKV `Decimal::native_format_float_g_shortest`. Its LowerExp digits, signed zero and nonfinite spellings remain intact. The older Ryu finite formatter retains its different zero policy; both use one Go-g layout renderer. Consumers include actual MyDecimal/Datum conversion, not just diagnostic text.
- `Decimal::cast_to_precision` delegates rounding, canonical coefficient precision checks and all-nine clamping to TiKV `try_native_cast_to_precision`. Raw scales, shape clearing, wide values and existing degenerate-target behavior remain intact.

No new C4 profile, transport or admission. CAST warning classification and source preparation remain with the caller; no whole-CAST or whole-Decimal-target claim. R91's statement that this formatter was still native is historical, not current status.

## Validation

[Evidence](evidence/decimal-policy-checkpoint.md), [exact commands/hashes](logs/decimal-policy-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Five new tests pass on first matching execution. New SQL20SELECT covers decimal values/warnings/metadata, FLOAT conversion and floating diagnostics in both vector modes. Prior R91 SQL36SELECT also passes again, separately—not counted as new or as a new worker proof.

Nine locked launches:7green,2unchanged old full RED. CPP Decimal97, native Decimal103, cast21, bridge1, legacy1, newSQL1 and priorSQL1 pass. Full expression **1598/4old/94ignored**, unistore **219/1old/13ignored** retain identical normalized failure sections. No compile failure, new failure, oracle correction, zero-match, interruption or fixture recording.

Pinned formatting/diff checks cover3native/1TiKV Rust files, no new modules.95CPP/203native original test bodies are byte-identical;2CPP/3native new tests. No Cargo/lock, Go/Bazel or generated changes.

## Remaining acceptance

[Remaining review](evidence/remaining-acceptance.md): **5core**, **8ordinary pending**, **6complex exception candidates**, not19approved exceptions. Continue other CAST/M2 domains, complete extrema/IN/INTERVAL, actual request-owner lifecycle and final cross-entry evidence.

Known baseline failures and extreme Decimal/CAST/INTDIV/mode/JSON/vector/older Values gaps remain. Workspace/lint/dev/bazel_prepare/release, exhaustive differential/TiFlash/FIPS, performance/physical memory/OOM/allocator/zero-copy/dual-timezone footprint and complete Go-package transcreation are unverified. No PR-readiness claim.

Three Plans agree; the manifest pins their hash and paired TiKV commit. Publish TiKV then TiDB without force push or PR. Unrelated untracked client-differential BUILD remains excluded.
