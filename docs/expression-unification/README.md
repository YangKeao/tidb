# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **field-cast-render-155**, following **field-name-storage-154**.

Functional **238/245 (97.14%)**, strict **0**, remaining **7**—unchanged. No whole CAST/M2 or Go-package credit; prior evaluator CAST ledger unchanged.

Existing SDK owner `native_type_name.rs` now owns the complete restricted CAST type grammar renderer. Native `FieldType` projects array-element identity and metadata only; the local renderer body is deleted.

## Verification

[Evidence](evidence/field-cast-render-checkpoint.md), [commands/counts/hashes](logs/field-cast-render-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Five Cargo gates GREEN: SDK1, full native datatype499, native-new1, source surface1 and vector CAST session SQL1. Two new tests;5 SDK/28 native old touched-file test bodies unchanged. No new Rust/SQL files or fixture/probe credit.

## Remaining

[Remaining acceptance](evidence/remaining-acceptance.md): compact/source/full restore and expression rendering; typed/write/SQL lowering; broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance. Full expression/unistore/workspace/lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness remain unverified. Historical R100 expression4/unistore1 failures and prior RED receipts retained. Goal remains active.
