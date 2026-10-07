# Shared datatype minimum and maximum bounds

**datatype-bound-138 / R144**, after [string target production](string-target-checkpoint.md). Functional238/245, strict0 and remaining7 unchanged; no whole CAST/M2/package credit.

SDK `native_eval_type.rs` owns GetMaxValue/GetMinValue target dispatch. It reuses existing integer and float bound owners, Decimal boundary text from `native_decimal_convert.rs`, and a single canonical duration limit in `native_duration_convert.rs`. Known type identity is matched directly; unknown payloads never become known types.

The controller preserves unsigned integer bounds, signed Float/Double maxima, raw i64-to-i32 metadata casts, string boundary bytes, clamped nonnegative Decimal shape, duration endpoints and Date/Datetime/Timestamp calendar endpoints. Native only materializes actual Datum, collation-string, Decimal, Duration and Time storage. Reverse-conversion source-bound and increment controllers remain explicit unless they reuse the boundary text leaf.

## Validation

Five matched Cargo gates GREEN: SDK dispatch1/Decimal1, full native datatype481, existing reverse1 and executor SQL consumer1. [Exact commands/counts/hashes and agent-stop chronology](../logs/datatype-bound-summary.txt). Three new tests;7 SDK/25 native old touched-file test bodies unchanged. No new Rust file. Executor/SQL files unchanged; no new SQL fixture/probe/cell credit. Other datatype source conversion/event merging/controllers, expression rendering and broader M2/root/liveDAG/final acceptance remain. Full expression/unistore/workspace/lint/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/package/PR readiness unverified. Historical R100 expression4/unistore1 failures and prior RED receipts retained.
