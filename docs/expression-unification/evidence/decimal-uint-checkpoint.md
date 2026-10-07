# Shared scientific and Decimal-to-unsigned conversion

**decimal-uint-141 / R147**, after [numeric event precedence](numeric-event-checkpoint.md). Functional238/245, strict0 and remaining7 unchanged; no whole CAST/M2/package credit.

Existing SDK owner `native_integer_convert.rs` now owns scientific-notation expansion and the exact DECIMAL-text-to-UNSIGNED algorithm. It preserves the first `e`/`E`, i64 exponent parsing, i128 decimal-point placement, zero extension, expanded overflow subjects, lexical upper-bound comparison, first-fraction-byte rounding, integer parse failure and original arithmetic/panic order.

Native `convert.rs` retains public APIs, maps shared typed errors, and supplies actual Decimal text. No float bridge is introduced; the path remains precision-preserving and target-aware.

## Validation

Five final matched Cargo gates GREEN: SDK3, full native datatype484, new/existing conversion2 and session SQL consumer1. One earlier SDK attempt retained RED because a new parent-authored test expected `1.25e-2` as `0.00125` instead of unchanged source behavior `0.0125`; only that new oracle was corrected before rerun. [Exact commands/counts/hashes and agent-stop chronology](../logs/decimal-uint-summary.txt). Three new tests;3 SDK/24 native old touched-file test bodies unchanged. No new Rust file. Session/SQL files unchanged; no new SQL fixture/probe/cell credit. Other datatype controllers, expression rendering and broader M2/root/liveDAG/final acceptance remain. Full expression/unistore/workspace/lint/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/package/PR readiness unverified. Historical R100 expression4/unistore1 failures and prior RED receipts retained.
