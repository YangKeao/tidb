# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **datum-integer-diagnostic-161**, following **datum-string-route-160**.

Functional **238/245 (97.14%)**, strict **0**, remaining **7**—unchanged. No complete write-lowering, whole CAST/M2 or Go-package credit.

Existing SDK owner `native_eval_type.rs` now owns signed/unsigned integer post-conversion diagnostic-action selection. Native conversion keeps source projection, concrete typed errors, caller context writes and actual numeric conversion; its two source-kind diagnostic matches are deleted.

## Verification

[Evidence](evidence/datum-integer-diagnostic-checkpoint.md), [commands/counts/hashes](logs/datum-integer-diagnostic-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Five final Cargo gates GREEN: SDK1, full native datatype506, native-new1, existing parser-precedence1 and numeric session SQL1. A first native compile RED from a new-test enum/type name shadow was retained; only the new test path was qualified before 506/506 GREEN. Two new tests;8 SDK/32 native old touched-file test bodies unchanged. No new Rust/SQL files or fixture/probe credit.

## Remaining

[Remaining acceptance](evidence/remaining-acceptance.md): remaining typed/write SQL lowering, expression text rendering; broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance. Full expression/unistore/workspace/lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness remain unverified. Historical R100 expression4/unistore1 failures and prior RED receipts retained. Goal remains active.
