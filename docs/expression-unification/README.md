# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **field-aggregate-policy-151**, following **field-merge-table-150**.

Functional **238/245 (97.14%)**, strict **0**, remaining **7**—unchanged. No whole CAST/M2 or Go-package credit.

Existing SDK owner `native_eval_type.rs` now owns aggregate flag merge/set, mixed-sign integer bump, Unspecified-aware eval merge and binary-output policy. Native aggregate code scans concrete FieldType metadata and assigns shared decisions only.

## Verification

[Evidence](evidence/field-aggregate-policy-checkpoint.md), [commands/counts/hashes](logs/field-aggregate-policy-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Five Cargo gates GREEN: SDK1, full native datatype494, new/source aggregate tests4 and set-operation session SQL1. Two new tests;4 SDK/1 native old touched-file test bodies unchanged. No new Rust/SQL files or fixture/probe credit. Complete identity distinguishes Unknown(0) from Unspecified Known(0).

## Remaining

[Remaining acceptance](evidence/remaining-acceptance.md): binary-string metadata scanning, expression rendering, typed/write/SQL lowering; broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance. Full expression/unistore/workspace/lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness remain unverified. Historical R100 expression4/unistore1 failures remain unrepaired; prior RED receipts retained. Goal remains active.
