# Shared FieldType name and storage policy

**field-name-storage-154 / R160**, after [FieldType value metadata policy](field-value-policy-checkpoint.md). Functional238/245, strict0 and remaining7 unchanged; no whole CAST/M2/package credit.

Existing SDK owner `native_type_name.rs` now owns type-name parsing aliases and fixed/DECIMAL storage-width policy. The boundary preserves first-only blob/binary replacement, fallback Known(0), complete named versus unknown identity, variable-width fallback, fixed eight-byte types, DECIMAL digit packing and invalid-metadata panic behavior.

Native `field_type/names.rs` and `memory.rs` project complete identity and metadata only.

## Validation

Five final Cargo gates GREEN: SDK1, full native datatype498, native shared-policy11, scoped source parser9 and numeric/temporal session SQL1. [Exact commands/counts/hashes and retained broad-filter RED](../logs/field-name-storage-summary.txt). Three new tests;4 SDK/8 native old touched-file test bodies unchanged. No new Rust file. Session/SQL files unchanged; no new SQL fixture/probe/cell credit. An overbroad `parser_` filter included unrelated charset global-registry tests:26 passed/4 failed, then exact `field_type_source::parser_` was 9/9 GREEN; no unrelated code was changed. Expression rendering and broader M2/root/liveDAG/final acceptance remain. Full expression/unistore/workspace/lint/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/package/PR readiness unverified. Historical R100 expression4/unistore1 failures and prior RED receipts retained.
