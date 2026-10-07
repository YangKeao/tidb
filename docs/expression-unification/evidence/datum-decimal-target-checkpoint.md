# Shared Datum Decimal target controller

**datum-decimal-target-162 / R168**, after [Datum integer diagnostics](datum-integer-diagnostic-checkpoint.md). Functional238/245, strict0 and remaining7 unchanged; no complete write-lowering, whole CAST/M2/package credit.

Existing SDK owner `native_eval_type.rs` now owns Decimal target metadata shape selection—unspecified, invalid or bounded precision/scale—and source/event input diagnostic action selection. The boundary preserves the UNSPECIFIED sentinel, negative metadata clamping, M<D rejection, text truncation precedence and overflow event payload.

Native `datum_convert.rs` keeps actual Decimal parsing/rounding/fitting, concrete typed errors and caller-owned context writes.

## Validation

Five final Cargo gates GREEN: SDK1, full native datatype507, native-new1, existing Decimal row1 and numeric session SQL1. [Exact commands/counts/hashes and retained dual compile RED](../logs/datum-decimal-target-summary.txt). Two new tests;9 SDK/33 native old touched-file test bodies unchanged. No new Rust file. Session/SQL files unchanged; no new SQL fixture/probe/cell credit. Remaining typed/write SQL lowering, expression text rendering and broader M2/root/liveDAG/final acceptance remain. Full expression/unistore/workspace/lint/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/package/PR readiness unverified. Historical R100 expression4/unistore1 failures and prior RED receipts retained.
