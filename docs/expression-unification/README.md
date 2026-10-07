# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **field-aggregate-controller-152**, following **field-aggregate-policy-151**.

Functional **238/245 (97.14%)**, strict **0**, remaining **7**—unchanged. No whole CAST/M2 or Go-package credit.

Existing SDK owner `native_eval_type.rs` now owns complete allocation-free iterator controllers for `AggFieldType` and `AggregateEvalType`. Native aggregate code projects concrete metadata descriptors, preserves the empty native shape and applies shared results only.

## Verification

[Evidence](evidence/field-aggregate-controller-checkpoint.md), [commands/counts/hashes](logs/field-aggregate-controller-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Five Cargo gates GREEN: SDK1, full native datatype495, new/source aggregate tests4 and set-operation session SQL1. Two new tests;5 SDK/2 native old touched-file test bodies unchanged. No new Rust/SQL files or fixture/probe credit. Tests cover empty/panic, all-null, full Unknown identity, first metadata, mixed sign and binary output.

## Remaining

[Remaining acceptance](evidence/remaining-acceptance.md): expression rendering, typed/write/SQL lowering; broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance. Full expression/unistore/workspace/lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness remain unverified. Historical R100 expression4/unistore1 failures remain unrepaired; prior RED receipts retained. Goal remains active.
