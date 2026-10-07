# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **datum-decimal-target-162**, following **datum-integer-diagnostic-161**.

Functional **238/245 (97.14%)**, strict **0**, remaining **7**—unchanged. No complete write-lowering, whole CAST/M2 or Go-package credit.

Existing SDK owner `native_eval_type.rs` now owns Decimal target metadata shape and input diagnostic-action selection. Native conversion keeps actual Decimal parse/round/fit, concrete typed errors and caller context writes; local flen/decimal controller and source/event diagnostic match are deleted.

## Verification

[Evidence](evidence/datum-decimal-target-checkpoint.md), [commands/counts/hashes](logs/datum-decimal-target-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Five final Cargo gates GREEN: SDK1, full native datatype507, native-new1, existing Decimal row1 and numeric session SQL1. Initial SDK and native compiles both exposed the same const `i64::max` incompatibility; both RED receipts are retained, and exact conditional clamping passed both reruns. Two new tests;9 SDK/33 native old touched-file test bodies unchanged. No new Rust/SQL files or fixture/probe credit.

## Remaining

[Remaining acceptance](evidence/remaining-acceptance.md): remaining typed/write SQL lowering, expression text rendering; broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance. Full expression/unistore/workspace/lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness remain unverified. Historical R100 expression4/unistore1 failures and prior RED receipts retained. Goal remains active.
