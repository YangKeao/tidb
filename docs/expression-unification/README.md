# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **datum-enum-set-route-164**, following **datum-bit-target-163**.

Functional **238/245 (97.14%)**, strict **0**, remaining **7**—unchanged. No complete write-lowering, whole CAST/M2 or Go-package credit.

Existing SDK owner `native_eval_type.rs` now owns unified ENUM/SET input classification and route selection. Native conversion keeps element parsing, collators, actual unsigned conversion and concrete values/events; local source matches and redundant SET numeric-failure state are deleted.

## Verification

[Evidence](evidence/datum-enum-set-route-checkpoint.md), [commands/counts/hashes](logs/datum-enum-set-route-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Five final Cargo gates GREEN: SDK1, full native datatype509, native-new1, existing mixed conversion1 and exact hex-literal ENUM/SET default session SQL1. A first native compile RED from a new-test enum/type name shadow is retained; only the new test path was qualified before 509/509 GREEN. Two new tests;11 SDK/35 native old touched-file test bodies unchanged. No new Rust/SQL files or fixture/probe credit.

## Remaining

[Remaining acceptance](evidence/remaining-acceptance.md): remaining typed/write SQL lowering, expression text rendering; broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance. Full expression/unistore/workspace/lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness remain unverified. Historical R100 expression4/unistore1 failures and prior RED receipts retained. Goal remains active.
