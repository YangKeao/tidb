# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **field-equality-policy-147**, following **field-string-policy-146**.

Functional **238/245 (97.14%)**, strict **0**, remaining **7**—unchanged. No whole CAST/M2 or Go-package credit.

Existing SDK owner `native_string_type.rs` now owns FieldType Equal/PartialEqual composition over an identity-preserving facts carrier. Native projects actual code/eval/length/scale and metadata equality facts only; Known and Unknown numeric aliases remain distinct.

## Verification

[Evidence](evidence/field-equality-policy-checkpoint.md), [commands/counts/hashes](logs/field-equality-policy-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Five final Cargo gates GREEN: SDK1, full native datatype490, new/source FieldType2 and set-operation session SQL1. Two new tests;3 SDK/25 native old touched-file test bodies unchanged. No new Rust/SQL files or fixture/probe credit. The summary retains one invalid Cargo target invocation.

## Remaining

[Remaining acceptance](evidence/remaining-acceptance.md): other datatype controllers, expression rendering, typed/write/SQL lowering; broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance. Full expression/unistore/workspace/lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness remain unverified. Historical R100 expression4/unistore1 failures remain unrepaired; prior RED receipts retained. Goal remains active.
