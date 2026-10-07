# Shared Datum conversion target selector

**datum-target-select-159 / R165**, after [lossless FieldType byte renderer](field-byte-render-checkpoint.md). Functional238/245, strict0 and remaining7 unchanged; no complete write-lowering, whole CAST/M2 or package credit.

Existing SDK owner `native_eval_type.rs` now owns the complete `Datum::ConvertTo` target-domain selector: NULL, signed/unsigned integers, Float32/Float64, strings/blobs, Decimal, temporal targets, ENUM/SET/BIT, JSON, vector and unsupported. The boundary preserves complete named versus unknown identity and effective JSON array behavior.

Native `datum_convert.rs` keeps actual datum conversion, typed error construction, diagnostic/event order, session-zone/context effects and storage adaptation.

## Validation

Five Cargo gates GREEN: SDK1, full native datatype504, native-new1, existing conversion1 and numeric/temporal session SQL1. [Exact commands/counts/hashes](../logs/datum-target-select-summary.txt). Two new tests;6 SDK/30 native old touched-file test bodies unchanged. No new Rust file. Session/SQL files unchanged; no new SQL fixture/probe/cell credit. Remaining typed/write SQL lowering, expression text rendering and broader M2/root/liveDAG/final acceptance remain. Full expression/unistore/workspace/lint/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/package/PR readiness unverified. Historical R100 expression4/unistore1 failures and prior RED receipts retained.
