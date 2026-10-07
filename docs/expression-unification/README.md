# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **year-text-142**, following **decimal-uint-141**.

Functional **238/245 (97.14%)**, strict **0**, remaining **7**—unchanged. No whole CAST/M2 or Go-package credit.

Existing SDK owner `native_temporal_convert.rs` now owns YEAR trimmed parse source, overflow-side zero and original-length/leading-zero adjustment. Native invokes the shared integer parser and retains actual typed-event projection.

## Verification

[Evidence](evidence/year-text-checkpoint.md), [commands/counts/hashes](logs/year-text-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Five Cargo gates GREEN: SDK1, full native datatype485, new/existing YEAR2 and session SQL consumer1. Two new tests;2 SDK/28 native old touched-file test bodies unchanged. No new Rust/SQL files or fixture/probe credit. The summary records the SDK lease stop/parent transfer before Cargo.

## Remaining

[Remaining acceptance](evidence/remaining-acceptance.md): other datatype controllers, expression rendering, typed/write/SQL lowering; broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance. Full expression/unistore/workspace/lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness remain unverified. Historical R100 expression4/unistore1 failures remain unrepaired; prior RED receipts retained. Goal remains active.
