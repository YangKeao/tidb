# Shared FieldType JSON tag policy

**field-json-tag-167 / R173**, after [Datum YEAR routing](datum-year-route-checkpoint.md). Functional238/245, strict0 and remaining7 unchanged; no whole JSON shape/type-dedup, complete write-lowering, whole CAST/M2/package credit.

Existing SDK owner `native_field_value.rs` now owns Go `bytes.EqualFold`-compatible classification for the nine FieldType JSON tags and unknown keys, including ASCII folding and long-s/Kelvin special fold classes. Native `field_type/json.rs` keeps serde scalar/slice decoding, map order, duplicate/null handling, GoSharedSlice storage and concrete FieldType projection.

## Validation

Five Cargo gates GREEN: SDK1, full native datatype512, native-new1, existing slice-JSON1 and enum-metadata session SQL1. [Exact commands/counts/hashes](../logs/field-json-tag-summary.txt). Two new tests;1 SDK/8 native old touched-file test bodies unchanged. No new Rust file. Session/SQL files unchanged; no new SQL fixture/probe/cell credit. Remaining full JSON shape policy, typed/write SQL lowering, expression text rendering and broader M2/root/liveDAG/final acceptance remain. Full expression/unistore/workspace/lint/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/package/PR readiness unverified. Historical R100 expression4/unistore1 failures and prior RED receipts retained.
