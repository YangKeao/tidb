# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **cast-real-uint-88**, following **nullif-87**.

**Partial CAST step, not a new completed family.** Functional coverage stays **226/245 (92.24%)**, strict final-audited count **0**. All226 family objects and the19-family remainder are unchanged. The overall goal remains active.

## Real/Float32→UNSIGNED

TiKV now owns the deleted native rounding, negative wrapping, range/nonfinite handling and overflow decision. `native_cast.rs` produces a computed u64 plus optional rounded overflow bits; `tikv/cast_real_unsigned.rs` strictly decodes that report and presents the original1690 warning. It does not calculate a native answer or swallow infrastructure failures.

The diagnostic `format_float_g_shortest` remains a native adapter. Signed/other casts, outer NULL and UNION's negative early-zero branch stay unchanged. Legacy TiKV wire casting has different rounding/clipping/boundary/NaN policy and is not substituted for the frozen native policy. No new carrier, general driver or PB admission.

## Validation

[Evidence](evidence/cast-real-uint-checkpoint.md), [exact commands/hashes](logs/cast-real-uint-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Seven new tests pass on first matching execution. SQL36SELECT probes:32stored DOUBLE/FLOAT cases (16slice-root zero-slot refusals),2strict-mode warning checks,2filters. Existing unsigned CastRealAsInt PBShared is covered; its signed/NULL cases remain outside this slice. Zero slots produce no1690 because there is no computed SDK event.

Nine locked launches:7green,2unchanged old full RED. CPP core1/local341+1ignored; native cast21, bridge1, gateway196+1ignored; legacy1 and SQL1 pass. Full expression **1598/4old/94ignored**, unistore **219/1old/13ignored** retain identical normalized failure sections. No compile failure, new failure, oracle correction, zero-match, interruption or fixture recording.

Pinned formatting/diff checks cover7native/8TiKV Rust files and2new modules.205CPP/493native original test bodies are byte-identical;3CPP/4native new tests. No Cargo/lock, Go/Bazel or generated changes; `compile.rs` unchanged.

## Remaining acceptance

[Remaining review](evidence/remaining-acceptance.md): **5core**, **8ordinary pending**, **6complex exception candidates**, not19approved exceptions. Continue other CAST/M2 domains and the diagnostic formatter, complete extrema/IN/INTERVAL, actual request-owner lifecycle and final cross-entry evidence.

Known baseline failures and CAST/INTDIV/mode/JSON/vector/older Values gaps remain. Workspace/lint/dev/bazel_prepare/release, exhaustive differential/TiFlash/FIPS, performance/physical heap or stack/OOM/allocator/zero-copy/dual-timezone footprint and complete Go-package transcreation are unverified. No PR-readiness claim.

Three Plans agree; the manifest pins their hash and paired TiKV commit. Publish TiKV then TiDB without force push or PR. Unrelated untracked client-differential BUILD remains excluded.
