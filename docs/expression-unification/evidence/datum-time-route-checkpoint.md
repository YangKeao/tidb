# Shared Datum temporal conversion routes

**datum-time-route-165 / R171**, after [Datum ENUM/SET routes](datum-enum-set-route-checkpoint.md). Functional238/245, strict0 and remaining7 unchanged; no complete write-lowering, whole CAST/M2/package credit.

Existing SDK owner `native_eval_type.rs` now owns DATE/DATETIME/TIMESTAMP target-kind selection, target FSP sentinel policy and input routing for time, duration, text, signed/bounded-unsigned integer, Decimal, JSON and unsupported sources. The boundary preserves UInt values beyond i64 as unsupported and complete named/unknown target identity.

Native `datum_convert.rs` keeps actual parsing/rounding, SQL zero/invalid-date flags, timezone/DST operations, typed zero fallback and concrete values/events.

## Validation

Five Cargo gates GREEN: SDK1, full native datatype510, native-new1, existing Decimal-temporal1 and numeric-temporal session SQL1. [Exact commands/counts/hashes](../logs/datum-time-route-summary.txt). Two new tests;12 SDK/36 native old touched-file test bodies unchanged. No new Rust file. Session/SQL files unchanged; no new SQL fixture/probe/cell credit. Remaining typed/write SQL lowering, expression text rendering and broader M2/root/liveDAG/final acceptance remain. Full expression/unistore/workspace/lint/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/package/PR readiness unverified. Historical R100 expression4/unistore1 failures and prior RED receipts retained.
