# Shared Datum string conversion route selector

**datum-string-route-160 / R166**, after [Datum target selector](datum-target-select-checkpoint.md). Functional238/245, strict0 and remaining7 unchanged; no complete write-lowering, whole CAST/M2/package credit.

Existing SDK owner `native_eval_type.rs` now owns string-target conversion route policy: raw binary pass-through, binary decode, binary encode, checked text, binary-literal decode and generic stringify. The boundary preserves BinaryLiteral's route independent of source/target binary booleans, BIT's generic stringify route and source/target collation identity.

Native `datum_convert.rs` keeps actual bytes, charset transforms, typed errors, flags and context effects; duplicate binary-literal transform error mapping is removed.

## Validation

Five Cargo gates GREEN: SDK1, full native datatype505, native-new1, existing binary-string conversion1 and GBK write session SQL1. [Exact commands/counts/hashes](../logs/datum-string-route-summary.txt). Two new tests;7 SDK/31 native old touched-file test bodies unchanged. No new Rust file. Session/SQL files unchanged; no new SQL fixture/probe/cell credit. Remaining typed/write SQL lowering, expression text rendering and broader M2/root/liveDAG/final acceptance remain. Full expression/unistore/workspace/lint/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/package/PR readiness unverified. Historical R100 expression4/unistore1 failures and prior RED receipts retained.
