# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **go-slice-growth-168**, following **field-json-tag-167**.

Functional **238/245 (97.14%)**, strict **0**, remaining **7**—unchanged. No whole JSON/type-dedup, complete write-lowering, whole CAST/M2 or Go-package credit.

Existing SDK owner `native_field_value.rs` now owns Go 1.25 64-bit slice growth, allocator size-class rounding and incremental decode capacity for scanned/noscan layouts. Native runtime keeps GoSharedSlice headers/backing and public compatibility wrappers; its size-class table and growth algorithm are deleted.

## Verification

[Evidence](evidence/go-slice-growth-checkpoint.md), [commands/counts/hashes](logs/go-slice-growth-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Five Cargo gates GREEN: SDK1, full native datatype513, native-new1, existing slice-JSON1 and enum-metadata session SQL1. Two new tests;2 SDK/0 native old touched-file test bodies unchanged. No new Rust/SQL files or fixture/probe credit.

## Remaining

[Remaining acceptance](evidence/remaining-acceptance.md): full FieldType JSON/type policy, remaining typed/write SQL lowering, expression text rendering; broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance. Full expression/unistore/workspace/lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness remain unverified. Historical R100 expression4/unistore1 failures and prior RED receipts retained. Goal remains active.
