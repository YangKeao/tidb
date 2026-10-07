# Shared FieldType merge table

**field-merge-table-150 / R156**, after [FieldType Decimal metadata controllers](field-decimal-meta-checkpoint.md). Functional238/245, strict0 and remaining7 unchanged; no whole CAST/M2/package credit.

Existing SDK owner `native_type_name.rs` now owns the exact 29×29 `fieldTypeMergeRules` table and Go map-zero indexing policy. The boundary preserves full named versus unknown identity, the zero index for unknown or unregistered inputs, asymmetry and known result bytes.

Native `field_type/aggregate.rs` projects complete type identity and maps the shared result byte to `FieldTypeCode` only; its 841-byte table and index selector are removed.

## Validation

Five matched Cargo gates GREEN: SDK1, full native datatype493, new/source merge tests2 and set-operation session SQL1. A mechanical verifier compared all 841 table bytes to native HEAD before deletion. [Exact commands/counts/hashes](../logs/field-merge-table-summary.txt). Two new tests;3 SDK/0 native old touched-file test bodies unchanged. No new Rust file. Session/SQL files unchanged; no new SQL fixture/probe/cell credit. Aggregate eval composition, expression rendering and broader M2/root/liveDAG/final acceptance remain. Full expression/unistore/workspace/lint/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/package/PR readiness unverified. Historical R100 expression4/unistore1 failures and prior RED receipts retained.
