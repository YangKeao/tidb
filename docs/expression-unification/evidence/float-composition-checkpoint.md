# Closed float controller composition

**float-composition-128 / R133**, after [scalar Datum foundations](scalar-datum-checkpoint.md). Functional238/245, strict0 and remaining7 unchanged; no whole CAST/M2/package credit.

Existing SDK `native_cast_float.rs` now composes actual numeric input projection, canonical JSON Display and shared datatype float conversion internally. Native ordinary/value-only bridges no longer supply JSON rendering or conversion callbacks; they pass actual data and the original truncate effect, then project errors. Existing generic SDK interfaces/tests remain compatible and algorithm ownership is not duplicated.

Ordinary JSON still parses document Display, including quotes; value-only JSON uses numeric conversion. Ordinary lossy text differs from strict value-only UTF-8/Decimal-prefix conversion. FLOAT narrowing, non-Real overflow, veto-before-overflow, signed zero, NaN and original unsupported/null/sentinel behavior remain distinct. No new profile/admission bypass or SQL lowering change.

## Validation

Five matched gates GREEN without failures/retries: SDK4, new native1/old float1/scalar1 and existing SQL1. [Exact commands/counts/hashes](../logs/float-composition-summary.txt). Three new tests;2 SDK/1 native old touched-file test bodies unchanged. The scalar regression and existing comprehensive float SQL test are unchanged and passed. No new SQL fixture or probe/cell credit. The former native input helper survives only under cfg(test) for old tests. Other typed/write numeric controllers, broader M2/root/liveDAG/final acceptance remain. Full suites/lint/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/package/PR readiness unverified. Historical R100 expression4/unistore1 failures and prior RED receipts retained.
