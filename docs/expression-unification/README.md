# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **reverse-bound-139**, following **datatype-bound-138**.

Functional **238/245 (97.14%)**, strict **0**, remaining **7**—unchanged. No whole CAST/M2 or Go-package credit.

New SDK owner `native_reverse_bound.rs` shares ChangeReverseResultByUpperLowerBound's overflow early return, source-kind boundaries, equal replacement, ceiling/floor action and bounded increments. Existing integer and Decimal owners are reused. Native retains actual context conversion, comparison/collation, Datum projection and Decimal addition.

## Verification

[Evidence](evidence/reverse-bound-checkpoint.md), [commands/counts/hashes](logs/reverse-bound-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Five Cargo gates GREEN: SDK2, full native datatype482, new/existing reverse2 and executor SQL consumer1. Three new tests;0 SDK/26 native old touched-file test bodies unchanged. One new SDK file, no new native/SQL files or fixture/probe credit. The summary records the SDK lease transfer and pre-Cargo inverted-prepare correction.

## Remaining

[Remaining acceptance](evidence/remaining-acceptance.md): numeric event merging, other datatype controllers, expression rendering, typed/write/SQL lowering; broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance. Full expression/unistore/workspace/lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness remain unverified. Historical R100 expression4/unistore1 failures remain unrepaired; prior RED receipts retained. Goal remains active.
