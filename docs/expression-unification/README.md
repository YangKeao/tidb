# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **datum-bit-target-163**, following **datum-decimal-target-162**.

Functional **238/245 (97.14%)**, strict **0**, remaining **7**—unchanged. No complete write-lowering, whole CAST/M2 or Go-package credit.

Existing SDK owner `native_eval_type.rs` now owns BIT target shape and input route selection. Native conversion keeps literal parsing, actual unsigned conversion, concrete errors/events and BinaryLiteral construction; local flen validation/shift/width and source-kind selectors are deleted.

## Verification

[Evidence](evidence/datum-bit-target-checkpoint.md), [commands/counts/hashes](logs/datum-bit-target-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Five Cargo gates GREEN: SDK1, full native datatype508, native-new1, existing mixed conversion1 and exact BIT write session SQL1. Two new tests;10 SDK/34 native old touched-file test bodies unchanged. No new Rust/SQL files or fixture/probe credit.

## Remaining

[Remaining acceptance](evidence/remaining-acceptance.md): remaining typed/write SQL lowering, expression text rendering; broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance. Full expression/unistore/workspace/lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness remain unverified. Historical R100 expression4/unistore1 failures and prior RED receipts retained. Goal remains active.
