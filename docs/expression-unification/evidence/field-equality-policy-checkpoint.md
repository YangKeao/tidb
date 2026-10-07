# Shared FieldType equality policy

**field-equality-policy-147 / R153**, after [FieldType string policy](field-string-policy-checkpoint.md). Functional238/245, strict0 and remaining7 unchanged; no whole CAST/M2/package credit.

Existing SDK owner `native_string_type.rs` now owns FieldType Equal/PartialEqual composition over a compact equality-facts carrier. It preserves actual named/unknown identity, VARCHAR↔VARSTRING equivalence, the asymmetric left-eval flen/decimal rules, REAL unspecified and JSON flen exceptions, Int/String decimal ignoring, NOT NULL handling and the unsafe-string branch.

Native `field_type/mod.rs` projects code/eval/flen/decimal values and charset/collation/unsigned/elements/NOT-NULL equality facts only.

## Validation

Five final matched Cargo gates GREEN: SDK1, full native datatype490, new/source FieldType2 and set-operation session SQL1. One earlier invocation is retained RED because `field_type_source` is not a Cargo test target; it was corrected to the generated `all` target before compilation/test execution. [Exact commands/counts/hashes](../logs/field-equality-policy-summary.txt). Two new tests;3 SDK/25 native old touched-file test bodies unchanged. No new Rust file. Session/SQL files unchanged; no new SQL fixture/probe/cell credit. Other datatype controllers, expression rendering and broader M2/root/liveDAG/final acceptance remain. Full expression/unistore/workspace/lint/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/package/PR readiness unverified. Historical R100 expression4/unistore1 failures and prior RED receipts retained.
