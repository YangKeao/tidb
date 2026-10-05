# Shared plain scalar Datum conversion

**scalar-datum-127 / R132**, after [context Decimal](decimal-context-checkpoint.md). Functional238/245, strict0, remaining7 unchanged; no whole CAST/M2/package credit.

SDK `codec/native_scalar_convert.rs` owns plain Datum-to-bool/to-f64 and JSON-to-float. Native facades project values, truncation and typed errors. Existing float parser, Decimal/temporal number conversion, literal integer outcome, vector zero check and JSON parse/compare remain sole owners. There are no host parser/value/comparator callbacks.

Float32 boolean tests the stored f64 without narrowing, whereas float conversion narrows first. JSON boolean compares against actual parsed JSON zero, not JSON float truth. Datum text requires valid UTF-8; JSON invalid UTF-8 becomes empty text. Hybrid ordinals and literal truncation values remain intact. Vector boolean retains the existing empty-storage test, not an all-lanes-zero predicate. `NativeDecimalParseRef::is_zero` is also shared by native Decimal, retaining its UTF-8 validation and empty/all-zero coefficient behavior.

## Validation

Six final gates GREEN: SDK2, full native datatype477, new expression1/old coerce1/float1, SQL1. Eight launches include two new SQL probe failures: a JSON string was incorrectly expected to behave like numeric JSON through ordinary CAST, and an arithmetic probe also did not reach the assumed typed helper. Final SQL uses a numeric JSON document under the existing Display-parser policy; production and old tests were not changed. Both REDs retained; no JSON-string lowering fix or exhaustive SQL coverage claimed. [Exact commands/counts/hashes and scope correction](../logs/scalar-datum-summary.txt).

Four new tests;5 SDK/251 native old touched-file test bodies unchanged. Final SQL runs two SELECTs/8 Real cells over both vector modes: numeric JSON, Enum/Set ordinals and BIT, Double metadata and no warnings. Direct units separately lock JSON-string conversion and boolean/float distinctions. Other typed/write numeric controllers, broader M2/root/liveDAG/final acceptance remain. Full suites/lint/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/package/PR readiness remain unverified. Historical R100 expression4/unistore1 failures and prior RED receipts retained.
