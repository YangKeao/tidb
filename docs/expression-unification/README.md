# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **field-merge-table-150**, following **field-decimal-meta-149**.

Functional **238/245 (97.14%)**, strict **0**, remaining **7**—unchanged. No whole CAST/M2 or Go-package credit.

Existing SDK owner `native_type_name.rs` now owns the exact 29×29 FieldType merge table and Go map-zero index policy. Native aggregate code projects complete identity and maps the shared result byte only; its 841-byte table/index are deleted.

## Verification

[Evidence](evidence/field-merge-table-checkpoint.md), [commands/counts/hashes](logs/field-merge-table-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Five Cargo gates GREEN: SDK1, full native datatype493, new/source merge tests2 and set-operation session SQL1. Two new tests;3 SDK/0 native old touched-file test bodies unchanged. No new Rust/SQL files or fixture/probe credit. A mechanical verifier compared all 841 bytes before deletion.

## Remaining

[Remaining acceptance](evidence/remaining-acceptance.md): aggregate eval composition, expression rendering, typed/write/SQL lowering; broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance. Full expression/unistore/workspace/lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness remain unverified. Historical R100 expression4/unistore1 failures remain unrepaired; prior RED receipts retained. Goal remains active.
