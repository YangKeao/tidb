# Shared signed-datum conversion dependency chain

**signed-datum-120 / R123–124**, following [temporal foundations](temporal-calendar-checkpoint.md). Functional238/245, strict0 and remaining7 are unchanged. This is partial CAST/M2 ownership, not whole YEAR/DATE/CAST or Go-package acceptance.

## Ownership

`codec/native_numeric.rs` owns signed-datum selection over19 actual input variants. The native projector borrows raw values and carries real Enum ordinals and Set masks, rather than deriving numbers from names or constructing descriptors. Float32 retains its stored f64 for this operation. Decimal retains all parse/storage fields; Time and Duration are neither rounded nor rendered before SDK dispatch.

`native_integer_convert.rs` owns integer bounds, six bounded conversion helpers, integer prefix/rounding/text policy and JSON integer policy. Its diagnostic callback accepts only borrowed typed effects, emitted synchronously at the original algorithm points. The native adapter applies existing generic diagnostic operations, including lazy error construction; it does not parse or choose values. Deferring a batch of diagnostics until computation ended would change effects before an error or panic, so no deferred event list is used.

`native_temporal_number.rs` owns temporal numeric rendering. Duration negation uses existing shared Decimal math and canonical transport, not a new sign-flip approximation. The pure BinaryLiteral integer outcome lives in `mysql/binary_literal.rs`; wire-context conversion is not substituted for that source policy.

Native conversion facades map values, typed errors and events. Existing integer-expression callback interfaces remain unchanged, but their signed-datum business implementation is now in SDK. YEAR/DATE expression controllers and other typed conversion selectors are separate remaining work.

## Preserved distinctions

- UInt overflow saturates with Truncated; Enum/Set saturate without an event. Real/Float32 use raw f64 ties-even rounding without Float32 narrowing in this selector.
- BinaryLiteral wider than eight significant bytes returns signed zero plus Truncated; BIT reinterprets the original unsigned outcome, including wide-input MAX becoming -1 with Truncated. An eight-byte MAX literal saturates with Overflow, while BIT becomes -1 without an event.
- Time rounds the temporal value to FSP0 in the actual zone before rendering a number; Duration rounds before sexagesimal rendering without zone demand. Decimal signed overflow retains its original saturation/Truncated behavior.
- Time numeric formatting returns raw-zero and DATE before considering fractional metadata. Raw microseconds can render seven digits; slicing remains exactly the old operation, including its panic domain. Negative submicrosecond Duration canonicalizes decimal zero through shared math, preserving source scale behavior.
- JSON string integer routing examines the raw string before trimming. Leading negative text uses signed parsing only when its original length exceeds one; other strings use unsigned parsing and reinterpretation even for signed requests. This branch ignores requested target/flags; malformed UTF-8 becomes empty text. Ordinary String/Bytes instead return UTF-8 errors.
- Numeric JSON branches use actual Known/Unknown target identity and flags. Malformed scalar accessors keep their original panics. Signed float NaN yields zero without overflow; unsigned float NaN keeps its explicit panic. No unified finite-value screen is introduced.

## Validation

Initial native datatype compilation failed because parent import cleanup removed `BinaryLiteral`, which existing file-level tests still use. Restored a test-only import; no old test body, oracle or production policy changed. The initial RED log is retained. All eight final gates passed: SDK native101 and binary8, native datatype477, three expression gates and two SQL gates. Nine launches total include the retained initial compile failure; no test-execution failure or zero-match occurred. Eleven new tests (SDK6/native5);12 old SDK and264 old native test bodies are byte-identical. Exact commands, timings and hashes are in [the receipt](../logs/signed-datum-summary.txt).

The new SQL test runs the same four projections in both vector modes: two SELECTs/eight cells. Actual stored ENUM/SET names reach YEAR's ordinal fallback (2/5), a stored DATETIME rounds across the LA DST boundary to20110313030000, and Duration carries from11:59:59.999999 to120000. Signed/Year result metadata and absence of warnings are checked. Existing temporal SQL and integer-controller context-demand tests also pass. This is consumer evidence, not YEAR-controller migration. New tests use literal expectations derived from source; old tests and fixtures remain immutable. No performance, physical-memory or whole-controller acceptance claim follows from source sharing.

## Remaining

YEAR/DATE and other CAST controllers, broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance remain. Full expression/unistore/workspace, lint/dev/bazel/release, exhaustive differential, performance/physical memory/OOM/allocator, TiFlash/FIPS/dual-tzdata and whole-Go-package/PR readiness remain unverified. Historical R100 expression4/unistore1 failures remain unrepaired. Goal stays active.
