# Shared FieldType value metadata policy

**field-value-policy-153 / R159**, after [FieldType aggregate controllers](field-aggregate-controller-checkpoint.md). Functional238/245, strict0 and remaining7 unchanged; no whole CAST/M2/package credit.

New SDK owner `native_field_value.rs` owns the complete runtime and parser `DefaultTypeForValue` metadata matches, signed/unsigned digit widths, Go special-float widths and code/length/decimal/flag/charset policy. The boundary preserves mode-specific literal differences, arithmetic overflow points, NaN/±Inf, DECIMAL 65/30 caps, runtime Unsupported metadata and parser empty metadata.

Native `field_type/value.rs` projects public value shapes and applies returned metadata specs; datum payload extraction remains native.

## Validation

Five matched Cargo gates GREEN: SDK1, full native datatype496, new/source runtime+parser value tests3 and numeric/temporal session SQL1. [Exact commands/counts/hashes](../logs/field-value-policy-summary.txt). Two new tests; no old touched-file test bodies. One new SDK Rust owner. Session/SQL files unchanged; no new SQL fixture/probe/cell credit. Pre-Cargo review corrected the unit Bool descriptor construction before any launch; no RED receipt. Expression rendering and broader M2/root/liveDAG/final acceptance remain. Full expression/unistore/workspace/lint/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/package/PR readiness unverified. Historical R100 expression4/unistore1 failures and prior RED receipts retained.
