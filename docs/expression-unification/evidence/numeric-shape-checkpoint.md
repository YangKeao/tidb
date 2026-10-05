# Numeric target shape and JSON integer composition

**numeric-shape-131 / R136**, after [numeric argument preparation](numeric-argument-checkpoint.md). Functional238/245, strict0 and remaining7 unchanged; no whole CAST/M2/package credit.

Existing `native_numeric_argument.rs` now owns Decimal target shape and JSON Display→integer warning→signed value composition. Native passes actual metadata/JSON storage and truncate effects. Existing JSON/integer implementations are reused, including the original fixed UTC rather than a session-zone read. Warning veto precedes value production.

`codec/native_eval_type.rs` owns the two FieldType limit primitives; both ordinary setters and the shared target-shape controller reuse them. Integer widths, non-integer unspecified width, negative scale/width, unknown codes and actual array code views are preserved. Native still constructs the target FieldType and projects resulting storage.

## Validation

Seven matched gates GREEN without failure/retry: SDK datatype2/argument5, full native datatype477, numeric consumers2/old Real1, existing integer/division SQL2. [Exact commands/counts/hashes](../logs/numeric-shape-summary.txt). Four new tests;102 SDK/56 native old touched-file test bodies unchanged. No new Rust files or SQL fixture/probe credit. Existing MAX_FRACTION changes visibility only to pub(crate), letting the new limit primitive reuse its canonical value rather than add another constant. Final target fitting, Real-to-Decimal and other typed/write controllers remain. Full expression/unistore/workspace/lint/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/package/PR readiness unverified. Broader M2/root/liveDAG/final acceptance remain. Historical R100 expression4/unistore1 failures and prior RED receipts retained.
