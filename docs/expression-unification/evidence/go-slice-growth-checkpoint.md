# Shared Go slice growth policy

**go-slice-growth-168 / R174**, after [FieldType JSON tags](field-json-tag-checkpoint.md). Functional238/245, strict0 and remaining7 unchanged; no whole JSON/type-dedup, complete write-lowering, whole CAST/M2/package credit.

Existing SDK owner `native_field_value.rs` now owns Go 1.25 64-bit slice growslice capacity, allocator size-class rounding and incremental JSON array decode capacity for scanned/noscan element layouts. Native `go_runtime.rs` keeps GoSharedSlice headers/backing storage, public compatibility signatures and layout projection; its size-class table and growth algorithm are removed.

## Validation

Five Cargo gates GREEN: SDK1, full native datatype513, native-new1, existing slice-JSON1 and enum-metadata session SQL1. [Exact commands/counts/hashes](../logs/go-slice-growth-summary.txt). Two new tests;2 SDK/0 native old touched-file test bodies unchanged. No new Rust file. Session/SQL files unchanged; no new SQL fixture/probe/cell credit. Remaining full JSON/type policy, typed/write SQL lowering, expression text rendering and broader M2/root/liveDAG/final acceptance remain. Full expression/unistore/workspace/lint/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/package/PR readiness unverified. Historical R100 expression4/unistore1 failures and prior RED receipts retained.
