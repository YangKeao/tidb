# Shared Datum integer diagnostic-action policy

**datum-integer-diagnostic-161 / R167**, after [Datum string route](datum-string-route-checkpoint.md). Functional238/245, strict0 and remaining7 unchanged; no complete write-lowering, whole CAST/M2/package credit.

Existing SDK owner `native_eval_type.rs` now owns signed/unsigned integer post-conversion diagnostic action selection: skip, numeric overflow, constant overflow, Decimal overflow and unhandled. The boundary preserves text parser precedence, float/ENUM/SET constant wording, Decimal round-fit routing and fallback event order.

Native `datum_convert.rs` keeps source projection, actual numeric values/conversion, concrete typed error construction and caller-owned context writes.

## Validation

Five final Cargo gates GREEN: SDK1, full native datatype506, native-new1, existing parser-precedence1 and numeric session SQL1. [Exact commands/counts/hashes and retained new-test compile RED](../logs/datum-integer-diagnostic-summary.txt). Two new tests;8 SDK/32 native old touched-file test bodies unchanged. No new Rust file. Session/SQL files unchanged; no new SQL fixture/probe/cell credit. Remaining typed/write SQL lowering, expression text rendering and broader M2/root/liveDAG/final acceptance remain. Full expression/unistore/workspace/lint/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/package/PR readiness unverified. Historical R100 expression4/unistore1 failures and prior RED receipts retained.
