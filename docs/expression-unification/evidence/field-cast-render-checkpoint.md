# Shared FieldType CAST renderer

**field-cast-render-155 / R161**, after [FieldType name and storage policy](field-name-storage-checkpoint.md). Functional238/245, strict0 and remaining7 unchanged; no whole CAST/M2/package credit and no evaluator CAST-slice change.

Existing SDK owner `native_type_name.rs` now owns the complete restricted CAST type grammar renderer. The boundary preserves full array-element named versus unknown identity, CHAR/BINARY and explicit charset behavior, DECIMAL comma-space formatting, temporal precision, integer signedness, unknown empty rendering and ARRAY suffix order.

Native `field_type/mod.rs` projects array-element identity and metadata only.

## Validation

Five Cargo gates GREEN: SDK1, full native datatype499, native-new1, source surface1 and vector CAST session SQL1. [Exact commands/counts/hashes](../logs/field-cast-render-summary.txt). Two new tests;5 SDK/28 native old touched-file test bodies unchanged. No new Rust file. Session/SQL files unchanged; no new SQL fixture/probe/cell credit. Compact/source/full restore and expression rendering plus broader M2/root/liveDAG/final acceptance remain. Full expression/unistore/workspace/lint/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/package/PR readiness unverified. Historical R100 expression4/unistore1 failures and prior RED receipts retained.
