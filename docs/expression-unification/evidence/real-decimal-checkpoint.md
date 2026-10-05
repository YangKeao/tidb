# Shared Real-to-Decimal preparation

**real-decimal-132 / R137**, after [numeric shape](numeric-shape-checkpoint.md). Functional238/245, strict0 and remaining7 unchanged; no whole CAST/M2/package credit.

Existing SDK `native_numeric_argument.rs` owns exact-integral selection, canonical Real parsing, error disposition, lazy truncate-level/subject demand and warning-before-value-projection finish. A private-field preparation state keeps parser storage until effects accept it. Strict overflow returns before expression rendering; Warn/Ignore retain the original subject demand and truncate call. SDK owns the fallback shortest-float subject and diagnostic formatting.

Native calls the existing `numeric_expression_text` only when requested and passes its optional result as data, not a formatting callback. That renderer remains an explicit native dependency; this is not complete numeric-controller ownership. Unspecified-scale Int/UInt Decimal construction also moves to the SDK; other actual kinds remain unchanged. Existing target shape/fitting order is retained.

## Validation

Six matched gates GREEN without failure/retry: SDK7, native consumers3/old Real1/division1, existing SQL2. [Exact commands/counts/hashes](../logs/real-decimal-summary.txt). Three new tests;5 SDK/34 native old touched-file test bodies unchanged. Existing expression renderer is byte-identical. No new Rust files or SQL fixture/probe credit. Native tests exercise strict suppression of ParamMarker subject reads, Warn/Ignore demand order, fallback/veto, metadata fitting and zone→flags order. Final fitting, expression rendering, other typed/write selectors and SQL lowering remain. Full suites/lint/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/package/PR readiness unverified. Broader M2/root/liveDAG/final acceptance remain. Historical R100 expression4/unistore1 failures and prior RED receipts retained.
