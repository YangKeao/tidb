# Shared FieldTypeCode policy tables

**field-code-policy-148 / R154**, after [FieldType equality policy](field-equality-policy-checkpoint.md). Functional238/245, strict0 and remaining7 unchanged; no whole CAST/M2/package credit.

Existing SDK owners `native_type_name.rs` and `native_string_type.rs` now own both default field length/decimal tables and the remaining blob/char/vector/varchar/unspecified/prefixable/fractionable/time/float/integer/stored/numeric/temporal classifiers. The boundary preserves complete named versus unknown identity, NewDate and Year distinctions, default versus CAST tables and JSON CAST metadata.

Native `field_type/mod.rs` projects complete type identity only.

## Validation

Five matched Cargo gates GREEN: SDK2, full native datatype491, new/source FieldType2 and numeric/temporal metadata session SQL1. [Exact commands/counts/hashes](../logs/field-code-policy-summary.txt). Three new tests;5 SDK/26 native old touched-file test bodies unchanged. No new Rust file. Session/SQL files unchanged; no new SQL fixture/probe/cell credit. Other datatype controllers, expression rendering and broader M2/root/liveDAG/final acceptance remain. Full expression/unistore/workspace/lint/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/package/PR readiness unverified. Historical R100 expression4/unistore1 failures and prior RED receipts retained.
