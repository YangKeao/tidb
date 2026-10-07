# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **decimal-uint-141**, following **numeric-event-140**.

Functional **238/245 (97.14%)**, strict **0**, remaining **7**—unchanged. No whole CAST/M2 or Go-package credit.

Existing SDK owner `native_integer_convert.rs` now owns scientific-notation expansion and exact DECIMAL-text-to-UNSIGNED conversion. Native retains public APIs, typed-error mapping and actual Decimal text rendering. No float bridge is introduced.

## Verification

[Evidence](evidence/decimal-uint-checkpoint.md), [commands/counts/hashes](logs/decimal-uint-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Five final Cargo gates GREEN: SDK3, full native datatype484, new/existing conversion2 and session SQL consumer1. Three new tests;3 SDK/24 native old touched-file test bodies unchanged. No new Rust/SQL files or fixture/probe credit. The summary retains one test-oracle RED and records parent adoption after both agents completed production changes but delayed tests/final responses.

## Remaining

[Remaining acceptance](evidence/remaining-acceptance.md): other datatype controllers, expression rendering, typed/write/SQL lowering; broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance. Full expression/unistore/workspace/lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness remain unverified. Historical R100 expression4/unistore1 failures remain unrepaired; prior RED receipts retained. Goal remains active.
