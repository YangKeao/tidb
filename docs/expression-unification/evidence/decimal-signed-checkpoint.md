# Shared target-aware Decimal-to-signed conversion

**decimal-signed-target-143 / R149**, after [YEAR text preparation](year-text-checkpoint.md). Functional238/245, strict0 and remaining7 unchanged; no whole CAST/M2/package credit.

Existing SDK owner `native_integer_convert.rs` now consumes the shared Decimal ref and owns half-up integer rounding, lazy saturating fallback, target signed bounds, exact visible overflow subject and source-overflow precedence over a bounded event.

Native `datum_convert.rs` retains only projection from the shared converted carrier to its existing typed value/event representation. Decimal storage and typed errors remain canonical rather than copied.

## Validation

Five final matched Cargo gates GREEN: SDK1, full native datatype486, new/existing conversion2 and session SQL consumer1. Two earlier attempts retained RED because the same new parent-authored test expected `126.5` to round ties-even to `126`; canonical shared Decimal behavior is half-up `127`. Only the two new expectations and evidence wording were corrected. [Exact commands/counts/hashes](../logs/decimal-signed-target-summary.txt). Two new tests;5 SDK/29 native old touched-file test bodies unchanged. No new Rust file. Session/SQL files unchanged; no new SQL fixture/probe/cell credit. Other datatype controllers, expression rendering and broader M2/root/liveDAG/final acceptance remain. Full expression/unistore/workspace/lint/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/package/PR readiness unverified. Historical R100 expression4/unistore1 failures and prior RED receipts retained.
