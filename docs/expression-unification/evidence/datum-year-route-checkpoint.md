# Shared Datum YEAR conversion route

**datum-year-route-166 / R172**, after [Datum temporal routes](datum-time-route-checkpoint.md). Functional238/245, strict0 and remaining7 unchanged; no complete write-lowering, whole CAST/M2/package credit.

Existing SDK owner `native_eval_type.rs` now owns YEAR input routing across text, time, direct duration, JSON and signed fallback sources. Native `datum_convert.rs` keeps text year parsing, statement-time/session-zone duration conversion, actual JSON/signed conversion, zero adjustment and concrete events.

## Validation

Five Cargo gates GREEN: SDK1, full native datatype511, native-new1, existing duration-YEAR1 and YEAR-controller session SQL1. [Exact commands/counts/hashes](../logs/datum-year-route-summary.txt). Two new tests;13 SDK/37 native old touched-file test bodies unchanged. No new Rust file. Session/SQL files unchanged; no new SQL fixture/probe/cell credit. Remaining typed/write SQL lowering, expression text rendering and broader M2/root/liveDAG/final acceptance remain. Full expression/unistore/workspace/lint/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/package/PR readiness unverified. Historical R100 expression4/unistore1 failures and prior RED receipts retained.
