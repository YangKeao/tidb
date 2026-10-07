# Shared reverse-conversion bound controller

**reverse-bound-139 / R145**, after [datatype bounds](datatype-bound-checkpoint.md). Functional238/245, strict0 and remaining7 unchanged; no whole CAST/M2/package credit.

SDK `native_reverse_bound.rs` owns ChangeReverseResultByUpperLowerBound's post-conversion controller: overflow early return, source-kind boundary selection, equal-source replacement, ceiling/floor action and bounded increments for integer, Float32, Real and Decimal. It reuses existing integer bounds and Decimal boundary text.

Native retains the actual context conversion, comparison/collation, Datum projection and Decimal add operation. This preserves overflow demand order, equal-before-increment priority, floor no-increment, integer checked limits, Float32/Real maxima and NaN behavior, Decimal target-maximum comparison, nonnumeric passthrough and invalid integer-target panic categories.

## Validation

Five matched Cargo gates GREEN: SDK2, full native datatype482, new/existing reverse2 and executor SQL consumer1. [Exact commands/counts/hashes and two lease-stop chronology](../logs/reverse-bound-summary.txt). Three new tests;0 SDK/26 native old touched-file test bodies unchanged. One new SDK file, no new native file. Executor/SQL files unchanged; no new SQL fixture/probe/cell credit. Other datatype conversion/event merging/controllers, expression rendering and broader M2/root/liveDAG/final acceptance remain. Full expression/unistore/workspace/lint/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/package/PR readiness unverified. Historical R100 expression4/unistore1 failures and prior RED receipts retained.
