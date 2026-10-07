# Shared FieldType aggregate controllers

**field-aggregate-controller-152 / R158**, after [FieldType aggregate policies](field-aggregate-policy-checkpoint.md). Functional238/245, strict0 and remaining7 unchanged; no whole CAST/M2/package credit.

Existing SDK owner `native_eval_type.rs` now owns complete allocation-free iterator controllers for `AggFieldType` code/flags/mixed-sign aggregation and `AggregateEvalType` null/string/unsigned/binary aggregation. The descriptor boundary preserves complete named versus unknown identity, empty native shape versus eval panic, all-null behavior, Null last-value exclusion, exact named unsigned tracking and first-field metadata.

Native `field_type/aggregate.rs` projects concrete FieldType descriptors and applies shared results only.

## Validation

Five matched Cargo gates GREEN: SDK1, full native datatype495, new/source aggregate tests4 and set-operation session SQL1. [Exact commands/counts/hashes](../logs/field-aggregate-controller-summary.txt). Two new tests;5 SDK/2 native old touched-file test bodies unchanged. No new Rust file. Session/SQL files unchanged; no new SQL fixture/probe/cell credit. Expression rendering and broader M2/root/liveDAG/final acceptance remain. Full expression/unistore/workspace/lint/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/package/PR readiness unverified. Historical R100 expression4/unistore1 failures and prior RED receipts retained.
