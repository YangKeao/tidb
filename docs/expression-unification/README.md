# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **datatype-bound-138**, following **string-target-137**.

Functional **238/245 (97.14%)**, strict **0**, remaining **7**—unchanged. No whole CAST/M2 or Go-package credit.

SDK `native_eval_type.rs` now owns GetMaxValue/GetMinValue target dispatch, reusing integer/float owners, exact Decimal boundary text and one canonical duration limit. Native materializes actual Datum/collation/Decimal/Duration/Time storage only. Known/Unknown identity, ARRAY effective type, metadata casts, Float32 narrowing, legacy Decimal text and temporal endpoints are retained.

## Verification

[Evidence](evidence/datatype-bound-checkpoint.md), [commands/counts/hashes](logs/datatype-bound-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Five Cargo gates GREEN: SDK dispatch1/Decimal1, full native datatype481, existing reverse1 and executor SQL consumer1. Three new tests;7 SDK/25 native old touched-file test bodies unchanged. No new Rust/SQL files or fixture/probe credit. Parent adopted/formatted/validated the complete native diff after requesting a late freeze interrupt; chronology is recorded in the summary.

## Remaining

[Remaining acceptance](evidence/remaining-acceptance.md): source conversion/event merging, other datatype controllers, expression rendering, typed/write/SQL lowering; broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance. Full expression/unistore/workspace/lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness remain unverified. Historical R100 expression4/unistore1 failures remain unrepaired; prior RED receipts retained. Goal remains active.
