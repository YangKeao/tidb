# Closed Decimal composition and shared numeric coercion

**decimal-coerce-129 / R134**, after [float composition](float-composition-checkpoint.md). Functional238/245, strict0 and remaining7 unchanged; no whole CAST/M2/package credit.

`native_cast_decimal.rs` composes actual numeric projection and shared default Decimal conversion inside SDK. Native ordinary/input-warning bridges pass data and warning effects, not conversion callbacks. Existing generic interfaces remain compatible. Warning order, two text parses, Float32/default conversion, event discard/error folding and precision behavior are unchanged.

New `native_coerce_numeric.rs` owns general integer classification, mixed-signed comparison, bits/Decimal/f64 projection and nullable truth. Native Integer aliases its SDK carrier. Literal outcome values and actual hybrid ordinals are retained; non-integer kinds stay None and range sentinels retain their error. Truth preserves NULL unknown, shared boolean semantics, suppressed conversion events and original error projection. Literal value helpers use the existing shared outcome projection; constructors and UTF-8 access adapters remain native.

## Validation

Seven matched gates GREEN without failure/retry: SDK Decimal3/numeric2, native new1/old Decimal1/coerce1 and existing SQL Decimal1/comparison1. [Exact commands/counts/hashes](../logs/decimal-coerce-summary.txt). Four new tests;2 SDK/1 native old touched-file test bodies unchanged. Existing coerce and SQL test files are unchanged. No new SQL fixture or probe/cell credit. Other typed/write numeric controllers, broader M2/root/liveDAG/final acceptance remain. Full suites/lint/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/package/PR readiness unverified. Historical R100 expression4/unistore1 failures and prior RED receipts retained.
