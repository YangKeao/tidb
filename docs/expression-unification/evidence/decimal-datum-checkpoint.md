# Shared decimal-datum dependency chain

**decimal-datum-124 / R129**, after [string arguments](arg-string-checkpoint.md). Functional238/245, strict0 and remaining7 are unchanged; no whole CAST/M2 or Go-package credit.

SDK `codec/native_decimal_convert.rs` owns plain Datum-to-decimal selection, decimal text conversion and JSON-to-decimal policy. It consumes actual numeric storage, including Enum/Set ordinals, and returns existing owned Decimal transport plus typed truncation/overflow events. Native datatype facades only project values, events and errors.

Existing Decimal parsers/math, temporal numeric rendering and literal width/fold outcomes remain their sole implementations. The existing canonical Decimal-to-transport projector gains crate-local visibility for JSON float conversion; there is no new coefficient renderer or duplicate arithmetic.

Preserved distinctions: String/Bytes use UTF-8 replacement before parsing, while invalid UTF-8 JSON strings become empty text; plain floats use bounded decimal text parsing while JSON floats use the original float constructor. Float32 narrows before shortest formatting. Decimal shape is preserved, Enum/Set use true ordinals, and BIT/BinaryLiteral share unsigned decoding and truncation outcomes. Temporal values use their original number rendering without introducing controller rounding.

`to_decimal_with_context`, other typed/write selectors and integer-controller callback interfaces remain separate. Existing unsigned CAST now reaches the shared plain Decimal selector through its unchanged adapter; the integer-argument controller is not claimed migrated.

## Validation

Five final gates GREEN: SDK2, full native datatype477, new expression1/old integer1 and SQL1. Six matched launches include a new SDK assertion failure: it expected zero for wide Literal/Bit, confusing signed conversion with the original literal outcome. Source BinaryLiteral.to_int/shared outcome and old Datum.to_decimal require u64MAX+Truncated; only that new expectation was corrected. Production/old tests unchanged, RED retained. [Exact commands/counts/hashes](../logs/decimal-datum-summary.txt).

Four new tests;5 SDK/234 native old touched-file test bodies unchanged. New SQL executes two SELECTs/8 UInt cells over both vector modes: actual Enum/Set ordinals and full-width BIT/JSON unsigned values, LongLong20/0 unsigned metadata and no warnings. Context-aware conversion remains covered by the unchanged datatype suite, not claimed migrated. Old tests/oracles are immutable. Full suites/lint/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness remain unverified. Historical R100 expression4/unistore1 failures remain unrepaired.
