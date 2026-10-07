# Shared lossless FieldType byte renderer

**field-byte-render-158 / R164**, after [FieldType source suffix renderer](field-source-render-checkpoint.md). Functional238/245, strict0 and remaining7 unchanged; no whole CAST/M2/package credit.

Existing SDK owner `native_type_name.rs` now owns the complete lossless raw-byte FieldType restore grammar: raw ENUM/SET bytes and quote doubling, precision/scale sentinels, flag order, charset/collation suffixes and named versus unknown identity. Array behavior remains exact because native projects the effective code (JSON for arrays).

Native `field_type/mod.rs` projects effective identity, metadata and raw element slices only. Its byte renderer body is deleted; all FieldType renderer policy now belongs to the SDK owner.

## Validation

Five final Cargo gates GREEN: SDK1, full native datatype503, native-new1, existing raw-byte1 and exact SHOW CREATE session SQL1. [Exact commands/counts/hashes and retained broad-filter SQL RED](../logs/field-byte-render-summary.txt). Two new tests;8 SDK/31 native old touched-file test bodies unchanged. No new Rust file. Session/SQL files unchanged; no new SQL fixture/probe/cell credit. A broad `show_create_table` filter included an unrelated global catalog count test (12/1; expected49 saw32); the exact target then passed and no unrelated state/code was changed. The separate expression text renderer plus broader M2/root/liveDAG/final acceptance remain. Full expression/unistore/workspace/lint/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/package/PR readiness unverified. Historical R100 expression4/unistore1 failures and prior RED receipts retained.
