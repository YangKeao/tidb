# Shared Datum ENUM/SET input routes

**datum-enum-set-route-164 / R170**, after [Datum BIT target](datum-bit-target-checkpoint.md). Functional238/245, strict0 and remaining7 unchanged; no complete write-lowering, whole CAST/M2/package credit.

Existing SDK owner `native_eval_type.rs` now owns unified ENUM/SET input classification and each target's route selection across text, binary literal, zero ENUM, named ENUM/SET, unsigned fallback and vector. The boundary preserves ENUM vector fallback as default-plus-truncation, SET vector hard Unsupported, zero ENUM identity and collation/event behavior.

Native `datum_convert.rs` keeps actual element parsing, collators, unsigned conversion, concrete values/errors and events. SET's redundant numeric-failure state is removed because both previous failed paths produced the same default SET and truncation event.

## Validation

Five final Cargo gates GREEN: SDK1, full native datatype509, native-new1, existing mixed conversion1 and exact hex-literal ENUM/SET default session SQL1. [Exact commands/counts/hashes and retained new-test compile RED](../logs/datum-enum-set-route-summary.txt). Two new tests;11 SDK/35 native old touched-file test bodies unchanged. No new Rust file. Session/SQL files unchanged; no new SQL fixture/probe/cell credit. Remaining typed/write SQL lowering, expression text rendering and broader M2/root/liveDAG/final acceptance remain. Full expression/unistore/workspace/lint/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/package/PR readiness unverified. Historical R100 expression4/unistore1 failures and prior RED receipts retained.
