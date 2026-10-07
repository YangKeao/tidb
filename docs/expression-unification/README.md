# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **datum-year-route-166**, following **datum-time-route-165**.

Functional **238/245 (97.14%)**, strict **0**, remaining **7**—unchanged. No complete write-lowering, whole CAST/M2 or Go-package credit.

Existing SDK owner `native_eval_type.rs` now owns YEAR input routing across text, time, direct duration, JSON and signed fallback. Native conversion keeps text rules, statement-time/session-zone duration handling, actual numeric conversion, adjustment and concrete events; its source-kind selector is deleted.

## Verification

[Evidence](evidence/datum-year-route-checkpoint.md), [commands/counts/hashes](logs/datum-year-route-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Five Cargo gates GREEN: SDK1, full native datatype511, native-new1, existing duration-YEAR1 and YEAR-controller session SQL1. Two new tests;13 SDK/37 native old touched-file test bodies unchanged. No new Rust/SQL files or fixture/probe credit.

## Remaining

[Remaining acceptance](evidence/remaining-acceptance.md): remaining typed/write SQL lowering, expression text rendering; broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance. Full expression/unistore/workspace/lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness remain unverified. Historical R100 expression4/unistore1 failures and prior RED receipts retained. Goal remains active.
