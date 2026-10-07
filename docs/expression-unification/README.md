# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **datum-time-route-165**, following **datum-enum-set-route-164**.

Functional **238/245 (97.14%)**, strict **0**, remaining **7**—unchanged. No complete write-lowering, whole CAST/M2 or Go-package credit.

Existing SDK owner `native_eval_type.rs` now owns DATE/DATETIME/TIMESTAMP target kind, FSP sentinel and input route selection. Native conversion keeps actual parsing/rounding, SQL date flags, timezone/DST operations and typed zero fallback; local target-code/FSP/source selectors are deleted.

## Verification

[Evidence](evidence/datum-time-route-checkpoint.md), [commands/counts/hashes](logs/datum-time-route-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Five Cargo gates GREEN: SDK1, full native datatype510, native-new1, existing Decimal-temporal1 and numeric-temporal session SQL1. Two new tests;12 SDK/36 native old touched-file test bodies unchanged. No new Rust/SQL files or fixture/probe credit.

## Remaining

[Remaining acceptance](evidence/remaining-acceptance.md): remaining typed/write SQL lowering, expression text rendering; broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance. Full expression/unistore/workspace/lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness remain unverified. Historical R100 expression4/unistore1 failures and prior RED receipts retained. Goal remains active.
