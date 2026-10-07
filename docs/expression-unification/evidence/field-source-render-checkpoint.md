# Shared FieldType source suffix renderer

**field-source-render-157 / R163**, after [compact FieldType renderer](field-compact-render-checkpoint.md). Functional238/245, strict0 and remaining7 unchanged; no whole CAST/M2/package credit.

Existing SDK owner `native_type_name.rs` now owns information-schema, type-description and source-string flags/charset/collation suffix policy. The boundary preserves Bit/Year unsigned exclusion, Year zerofill exclusion, String binary exclusion, named char/blob classification, complete named versus unknown identity and suffix order/case.

Native `field_type/mod.rs` projects compact text, complete identity and metadata only.

## Validation

Five final Cargo gates GREEN: SDK1, full native datatype502, native-new1, source runtime1 and SHOW COLUMNS session SQL1. [Exact commands/counts/hashes and retained new-oracle RED](../logs/field-source-render-summary.txt). Two new tests;7 SDK/30 native old touched-file test bodies unchanged. No new Rust file. Session/SQL files unchanged; no new SQL fixture/probe/cell credit. The first new native oracle omitted the width forced by ZEROFILL and failed 501/1; only that new expectation was corrected, production stayed unchanged, and the full rerun passed 502/502. Full byte restore and expression rendering plus broader M2/root/liveDAG/final acceptance remain. Full expression/unistore/workspace/lint/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/package/PR readiness unverified. Historical R100 expression4/unistore1 failures and prior RED receipts retained.
