# Shared context-aware Decimal conversion

**decimal-context-126 / R131**, after [integer-argument composition](arg-integer-checkpoint.md). Functional238/245, strict0 and remaining7 remain unchanged. No whole CAST/M2 or Go-package credit.

`codec/native_decimal_context.rs` owns the distinct context-aware Datum selector, MyDecimal parsing, diagnostic trimming/rendering and effect ordering. String uses original raw bytes, not plain conversion's lossy UTF-8; Bytes remains unsupported. String, BIT/Literal and JSON invoke the generic truncation handler even for no error. Real/Float32 return direct parser errors without invoking it; other supported kinds retain plain value-only behavior. JSON retains the original getter chain, MyDecimal float conversion and empty-literal panic, rather than substituting plain JSON-to-decimal policy.

Native provides only actual numeric storage, typed Terror construction and the existing generic context handler. The binary Terror factory is shared with the existing BinaryLiteral methods. No native parser, value selector or diagnostic-formatting policy callback remains in this entry.

To avoid duplicate implementations, `mysql/native_decimal_parse.rs` owns MyDecimal-to-Decimal projection and scale padding, using existing canonical normalization. `mysql/binary_literal.rs` owns native literal Display, reused by native Display and SDK diagnostics. Native MyDecimal exposes its existing raw-value adapter only crate-locally; transport shape is unchanged.

## Validation

Five final gates GREEN: SDK2, full native datatype477, new expression1/old constants16, SQL1. Eight matched launches include three retained RED gates from new-test assumptions: an invalid scale setter setup/overbroad malformed-JSON panic expectation,1265-versus1292 identity, and omitted DECIMAL narrowing warning. Only new setup/expectations were corrected from original source; production/old tests unchanged. [Exact source explanations, commands/counts/hashes](../logs/decimal-context-summary.txt).

Four new tests;14 SDK/248 native old touched-file test bodies unchanged. Final SQL executes two SELECTs/6 Decimal cells over both vector modes, temporal millisecond arguments and fractional projection/narrowing, with result codes and one exact1292 warning per query. JSON context behavior is tested in units, not claimed fully exercised by SQL. Original tests/oracles remain unchanged. Other typed/write numeric wrappers, including scalar_function composition, remain open. Broader M2/root/liveDAG/final acceptance, full suites/lint/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness remain unverified. Historical R100 expression4/unistore1 failures remain unrepaired; prior RED receipts retained.
