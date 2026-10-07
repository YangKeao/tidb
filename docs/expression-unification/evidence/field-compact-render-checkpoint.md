# Shared compact FieldType renderer

**field-compact-render-156 / R162**, after [restricted CAST type renderer](field-cast-render-checkpoint.md). Functional238/245, strict0 and remaining7 unchanged; no whole CAST/M2/package credit.

Existing SDK owner `native_type_name.rs` now owns SQL display rune escaping and the complete compact FieldType grammar/width policy. The boundary preserves invalid UTF-8 per-byte replacement before SDK escaping, enum/set quoting, temporal and numeric precision, strict integer display widths, zerofill, type aliases and complete named versus unknown identity.

Native `field_type/mod.rs` projects element lossy text and metadata only; `format.rs` is a facade.

## Validation

Five Cargo gates GREEN: SDK1, full native datatype501, native-new1, source runtime1 and SHOW COLUMNS session SQL1. [Exact commands/counts/hashes](../logs/field-compact-render-summary.txt). Three new tests;6 SDK/29 native old touched-file test bodies unchanged. No new Rust file. Session/SQL files unchanged; no new SQL fixture/probe/cell credit. Source/full restore and expression rendering plus broader M2/root/liveDAG/final acceptance remain. Full expression/unistore/workspace/lint/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/package/PR readiness unverified. Historical R100 expression4/unistore1 failures and prior RED receipts retained.
