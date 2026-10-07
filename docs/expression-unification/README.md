# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **field-compact-render-156**, following **field-cast-render-155**.

Functional **238/245 (97.14%)**, strict **0**, remaining **7**—unchanged. No whole CAST/M2 or Go-package credit; prior evaluator CAST ledger unchanged.

Existing SDK owner `native_type_name.rs` now owns SQL display rune escaping and the complete compact FieldType grammar/width policy. Native code projects element lossy text and metadata only; the local match/assembly and char loop are deleted.

## Verification

[Evidence](evidence/field-compact-render-checkpoint.md), [commands/counts/hashes](logs/field-compact-render-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Five Cargo gates GREEN: SDK1, full native datatype501, native-new1, source runtime1 and SHOW COLUMNS session SQL1. Three new tests;6 SDK/29 native old touched-file test bodies unchanged. No new Rust/SQL files or fixture/probe credit.

## Remaining

[Remaining acceptance](evidence/remaining-acceptance.md): source/full restore and expression rendering; typed/write/SQL lowering; broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance. Full expression/unistore/workspace/lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness remain unverified. Historical R100 expression4/unistore1 failures and prior RED receipts retained. Goal remains active.
