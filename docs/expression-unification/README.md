# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **field-byte-render-158**, following **field-source-render-157**.

Functional **238/245 (97.14%)**, strict **0**, remaining **7**—unchanged. No whole CAST/M2 or Go-package credit; prior evaluator CAST ledger unchanged.

Existing SDK owner `native_type_name.rs` now owns the complete lossless raw-byte FieldType restore grammar. Native code projects effective identity, metadata and raw element slices only; its final renderer body is deleted. All FieldType renderer policy is now SDK-owned.

## Verification

[Evidence](evidence/field-byte-render-checkpoint.md), [commands/counts/hashes](logs/field-byte-render-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Five final Cargo gates GREEN: SDK1, full native datatype503, native-new1, existing raw-byte1 and exact SHOW CREATE session SQL1. Two new tests;8 SDK/31 native old touched-file test bodies unchanged. No new Rust/SQL files or fixture/probe credit. A broad SHOW CREATE launch retained RED because an unrelated global catalog test expected49 entries but saw32; exact target passed.

## Remaining

[Remaining acceptance](evidence/remaining-acceptance.md): the separate expression text renderer; typed/write/SQL lowering; broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance. Full expression/unistore/workspace/lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness remain unverified. Historical R100 expression4/unistore1 failures and prior RED receipts retained. Goal remains active.
