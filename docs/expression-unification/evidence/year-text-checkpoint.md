# Shared YEAR text preparation

**year-text-142 / R148**, after [Decimal-to-unsigned conversion](decimal-uint-checkpoint.md). Functional238/245, strict0 and remaining7 unchanged; no whole CAST/M2/package credit.

Existing SDK owner `native_temporal_convert.rs` now selects the trimmed YEAR parse source and owns parse-overflow side value zero plus original-length/leading-zero adjustment. This preserves the distinction between original and trimmed length, four-byte `0000`, non-four-byte leading zero and overflow saturation.

Native invokes the already shared integer parser and maps its actual typed event; SDK does not construct or deliver native diagnostics.

## Validation

Five matched Cargo gates GREEN: SDK1, full native datatype485, new/existing YEAR2 and session SQL consumer1. [Exact commands/counts/hashes and lease chronology](../logs/year-text-summary.txt). Two new tests;2 SDK/28 native old touched-file test bodies unchanged. No new Rust file. Session/SQL files unchanged; no new SQL fixture/probe/cell credit. Other datatype controllers, expression rendering and broader M2/root/liveDAG/final acceptance remain. Full expression/unistore/workspace/lint/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/package/PR readiness unverified. Historical R100 expression4/unistore1 failures and prior RED receipts retained.
