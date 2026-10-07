# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **datum-target-select-159**, following **field-byte-render-158**.

Functional **238/245 (97.14%)**, strict **0**, remaining **7**—unchanged. No complete write-lowering, whole CAST/M2 or Go-package credit.

Existing SDK owner `native_eval_type.rs` now owns the complete `Datum::ConvertTo` target-domain selector. Native conversion keeps actual values, typed diagnostics/event order, session context effects and storage adaptation; its FieldTypeCode family match is deleted.

## Verification

[Evidence](evidence/datum-target-select-checkpoint.md), [commands/counts/hashes](logs/datum-target-select-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Five Cargo gates GREEN: SDK1, full native datatype504, native-new1, existing conversion1 and numeric/temporal session SQL1. Two new tests;6 SDK/30 native old touched-file test bodies unchanged. No new Rust/SQL files or fixture/probe credit.

## Remaining

[Remaining acceptance](evidence/remaining-acceptance.md): remaining typed/write SQL lowering, expression text rendering; broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance. Full expression/unistore/workspace/lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness remain unverified. Historical R100 expression4/unistore1 failures and prior RED receipts retained. Goal remains active.
