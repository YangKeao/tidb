# Shared-ref Decimal-to-unsigned conversion

**decimal-unsigned-ref-144 / R150**, after [target-aware Decimal-to-signed conversion](decimal-signed-checkpoint.md). Functional238/245, strict0 and remaining7 unchanged; no whole CAST/M2/package credit.

Existing SDK owner `native_integer_convert.rs` now consumes the shared Decimal ref for DECIMAL-to-UNSIGNED, renders its canonical visible sign/scale/storage metadata, and reuses the exact scientific/text/round/bound algorithm shared in R147. No float bridge or duplicate native rendering remains in this path.

Native `convert.rs` retains its public API and maps the shared typed error only.

## Validation

Five matched Cargo gates GREEN: SDK1, full native datatype487, new/existing conversion2 and session SQL consumer1. [Exact commands/counts/hashes](../logs/decimal-unsigned-ref-summary.txt). Two new tests;6 SDK/25 native old touched-file test bodies unchanged. No new Rust file. Session/SQL files unchanged; no new SQL fixture/probe/cell credit. Other datatype controllers, expression rendering and broader M2/root/liveDAG/final acceptance remain. Full expression/unistore/workspace/lint/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/package/PR readiness unverified. Historical R100 expression4/unistore1 failures and prior RED receipts retained.
