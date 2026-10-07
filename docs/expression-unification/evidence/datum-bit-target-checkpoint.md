# Shared Datum BIT target controller

**datum-bit-target-163 / R169**, after [Datum Decimal target](datum-decimal-target-checkpoint.md). Functional238/245, strict0 and remaining7 unchanged; no complete write-lowering, whole CAST/M2/package credit.

Existing SDK owner `native_eval_type.rs` now owns BIT target shape—invalid, full-width or bounded upper/byte-width—and input route selection for bytes, signed integer and unsigned fallback. The boundary preserves 1..64 limits, negative i64 casting, string/bytes precedence, truncating clamps and encoded width.

Native `datum_convert.rs` keeps literal parsing, actual unsigned conversion, concrete errors/events and BinaryLiteral construction.

## Validation

Five Cargo gates GREEN: SDK1, full native datatype508, native-new1, existing mixed conversion1 and exact BIT write session SQL1. [Exact commands/counts/hashes](../logs/datum-bit-target-summary.txt). Two new tests;10 SDK/34 native old touched-file test bodies unchanged. No new Rust file. Session/SQL files unchanged; no new SQL fixture/probe/cell credit. Remaining typed/write SQL lowering, expression text rendering and broader M2/root/liveDAG/final acceptance remain. Full expression/unistore/workspace/lint/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/package/PR readiness unverified. Historical R100 expression4/unistore1 failures and prior RED receipts retained.
