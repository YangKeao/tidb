# Shared FieldType Decimal metadata controllers

**field-decimal-meta-149 / R155**, after [FieldTypeCode policy tables](field-code-policy-checkpoint.md). Functional238/245, strict0 and remaining7 unchanged; no whole CAST/M2/package credit.

Existing SDK owners `native_type_name.rs`, `native_string_type.rs` and `native_eval_type.rs` now own MySQL integer classification, CHAR↔VARCHAR conversion, DECIMAL metadata validity and flen/scale delta updates. The boundary preserves complete named versus unknown identity, Year exclusion, the i64→i32 default metadata wrapper, DECIMAL 30/65 limits, negative-old metadata compensation and non-DECIMAL no-op behavior.

Native `field_type/mod.rs` projects complete identity/metadata and assigns returned values only.

## Validation

Five matched Cargo gates GREEN: SDK3, full native datatype492, new/source FieldType2 and numeric/temporal metadata session SQL1. [Exact commands/counts/hashes](../logs/field-decimal-meta-summary.txt). Four new tests;10 SDK/27 native old touched-file test bodies unchanged. No new Rust file. Session/SQL files unchanged; no new SQL fixture/probe/cell credit. A pre-integration review caught tuple-first computation drifting overflow partial-mutation order; split scale/flen APIs preserve it and a caught-panic test verifies the resulting state. Expression rendering and broader M2/root/liveDAG/final acceptance remain. Full expression/unistore/workspace/lint/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/package/PR readiness unverified. Historical R100 expression4/unistore1 failures and prior RED receipts retained.
