# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **convert-charset-93**, after **charset-codec-92**.

Functional coverage **229/245 (93.47%)**, strict final-audited count **0**. All228 previous family objects remain byte-identical; only `convert_charset` is added. Overall goal stays active.

## Charset evaluator now shared

`tikv/convert_charset.rs` delegates `to_binary`, `from_binary` and `CONVERT USING` to three actual SDK workers. AST, typed, public-helper and implicit binary-aware routes are connected; native encode/decode/validation/replacement/result-domain bodies are removed. Encoding name lookup is shared too.

ConvertUsing requires four actual inputs: nullable bytes, exact source spelling, effective field charset and target. A closed Bytes4 carrier/profile whitelist reuses the existing four-slot ready storage; generic arity and wire/native PB admission stay unchanged.

Direct-helper NULL→empty, caller early NULL, unknown-target-before-coercion, AST lowercase versus typed lossy spelling, and metadata passthrough remain distinct. SDK selects Bytes/retag/NULL/error; retag default collation is projected afterward using the original global GB mode. Reply precharge is a conservative `8*n+1`, not a physical heap/OOM guarantee.

## Validation

[Evidence](evidence/convert-charset-checkpoint.md), [commands/counts/hashes](logs/convert-charset-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Six new tests pass on first actual execution. New SQL34SELECT=32direct+2filters:14new Convert-root zero-slot refusals and2old NULL-witness refusals are separated. Seven original charset SQL tests also pass. Metadata, replacement/decode, exact/effective source names and implicit GBK→HEX integration are pinned; FromBinary has direct helper evidence, not new SQL admission.

Thirteen Cargo launches: four compile failures from missing macro trait imports, fixed without algorithm/oracle changes; nine executed gates give seven green and two unchanged old full REDs. Full expression **1604/4old/94ignored**, unistore **220/1old/13ignored**, with identical normalized failure sections. A formatter second pass occurred before Cargo, separately recorded.

Pinned formatting/diff checks pass. Scope:9TiKV/10native Rust files,2new modules;145CPP/425native old test bodies unchanged. Independent source review found no blocker.

## Remaining acceptance

[Review](evidence/remaining-acceptance.md):5core,5ordinary pending,6complex exception candidates—not16approved exceptions. Request-root/default-NoColumns/live-DAG ownership, final cross-entry work and documented known gaps remain.

Workspace/lint/dev/bazel_prepare/release, exhaustive differential/TiFlash/FIPS, performance/physical memory/OOM/allocator/zero-copy/dual-timezone footprint and complete Go-package transcreation are unverified. No goal-completion or PR-readiness claim.

Three Plans agree; manifest pins their hash and paired TiKV commit. Publish TiKV then TiDB without force push or PR. Unrelated untracked client-differential BUILD stays excluded.
