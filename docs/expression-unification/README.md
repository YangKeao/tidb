# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **nullif-87**, following **case-86**.

Functional coverage: **226/245 (92.24%)**; strict final-audited count: **0**. All225 prior family objects are byte-identical; only NULLIF is added. **The overall goal remains active.**

## NULLIF result selection belongs to TiKV

Original eager operands, clones and equality once are retained. `tikv/null_if.rs` completes comparison before encoding the actual left, even when equal. `NullIfNative` uses existing BytesInt transport. One TiKV borrowed selector supplies both actual kernel execution and exact reply-length preflight; the dispatcher still runs and NULL output does not erase left input capacity.

No synthetic operand, cached native answer, new carrier, driver or PB admission. SQL already supports NULLIF through direct rewriter construction; a separate registry-based FunctionBuilder still rejects it. Every successful equality domain already uses a Compare worker, so **SQL zero-slot refusal is comparison-stage evidence, not proof of the new NULLIF selector**. Direct bridge/dispatch tests isolate that selector.

## Validation

[Evidence](evidence/nullif-checkpoint.md), [exact commands/hashes](logs/nullif-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Six new tests finally pass. One new SQL expectation initially failed: VARCHAR arg0 metadata is8/0, not the derived-string8/-1 used by CASE/IF. Corrected only from DDL default/type-clone source; production and old tests unchanged, initial RED retained.

SQL38SELECT probes:32stored-column cases (16positive,16comparison-stage zero-slot),4eager RHS errors including NULLlhs,2filters. No new selector-root SQL claim.

Nine locked launches:6green,1new-test-oracle RED,2unchanged old full RED. CPP core1/local339+1ignored; native root2 including old NULLIF rows, bridge1, gateway196+1ignored; corrected SQL1 pass. Full expression **1596/4old/94ignored**, unistore **218/1old/13ignored** retain identical normalized failure sections. No compile failure, zero-match, interrupted test or fixture recording.

Pinned formatting/diff checks cover6native/8TiKV Rust files; one new native module.130CPP/378native original test bodies are byte-identical, with3CPP/3native new tests. No Cargo/lock, Go/Bazel or generated changes; `compile.rs` and PB/legacy admission are unchanged.

## Remaining acceptance

[Remaining review](evidence/remaining-acceptance.md): **5core**, **8ordinary pending**, **6complex exception candidates**, not19approved exceptions. Continue ordinary CAST/M2, IN/extrema/INTERVAL, actual request-owner lifecycle and final cross-entry evidence.

Known baseline failures and documented INTDIV/CAST/mode/JSON/vector/older Values gaps remain. Workspace/lint/dev/bazel_prepare/release, exhaustive differential/TiFlash/FIPS, performance/physical heap or stack/OOM/allocator/zero-copy/dual-timezone footprint and complete Go-package transcreation are unverified. No PR-readiness claim.

Three Plans agree; the manifest pins their hash and paired TiKV commit. Publish TiKV then TiDB, without force push or PR. Unrelated untracked client-differential BUILD remains excluded.
