# Digit-string Decimal parse and shift ownership

**decimal-parse-107 / R110**, after [IN ownership](in-control-checkpoint.md).
Functional **238/245**, strict **0**, remaining **7** are unchanged. This is a type-layer prerequisite, not whole CAST/M2 or Go-package completion.

## Single algorithm owner, original native storage

SDK `mysql/native_decimal_parse.rs` owns literal construction, unsigned coefficient generation, normalization, MySQL text parsing and exponent/error policy, maximum magnitude construction and bounded shift. Native `decimal/mod.rs` deletes those bodies and keeps thin adapters.

Native `Decimal` and its `DecimalDigits` retain their private fields, SmallVec24, Debug/Clone behavior and public API. Owned SDK parts move the actual SmallVec, not a serialized Vec or rendered decimal. Borrowed shift input carries actual sign, coefficient bytes, visible/storage scale and declared shape. Zero-shift and overflow copy the original representation; fresh results clear declared shape as before.

The existing SDK rounder and canonical coefficient extractor are reused. Only the extractor's visibility becomes `pub(super)` inside mysql; its algorithm is unchanged. No second rounder, native fallback, new C4 profile, admission gate, transport carrier or dependency is introduced.

## Preserved compatibility boundaries

- General parsing trims leading space/tab only; trailing and exponent handling retain their original Unicode trimming. Integer overflow retains the original suffix digits before subsequent exponent policy.
- A bad exponent first clears the value, but later clamped exponent bounds can replace that disposition with Overflow or Truncated. Caller-supplied reduced word limits remain supported.
- Shift uses stored, not merely visible, scale. Fractional exhaustion must not let rounding resurrect discarded digits. Integer overflow returns the original value and metadata.
- Normalization retains original storage-floor padding, sign-preserving option and UTF-8 dereference order. Raw zero-shift does not validate malformed bytes. No new raw-input admission or extreme-shift/OOM repair is claimed.
- The fixed-word MyDecimal, wire Decimal and stricter canonical parser are distinct compatibility policies, not interchangeable implementations. R107's fixed-word facade remains unchanged.

## Actual SQL and remaining CAST policy

String/Bytes ordinary DECIMAL CAST calls this general parser. Its native warning path parses Unicode-trimmed input, while actual conversion parses the original text and discards that second disposition. Thus leading LF followed by1.25 can remain quiet yet convert to0; leading TAB converts to1.25. The new SQL regression pins this existing behavior rather than reusing the first parse's value.

CAST's source selection, warning dispatch/order, Real-specific decimal prefix, Float32/Other conversions, unspecified scale, UNION negative bypass and other typed/vector routes remain explicit follow-up work. Existing SDK precision casting is not newly reimplemented here.

## Validation

[Exact commands/counts/hashes](../logs/decimal-parse-summary.txt): five nonzero green receipts—SDK2, full native datatype463, focused expression CAST7 (one preexisting ignored vectorized string-to-DECIMAL UNION gap), new SQL1 and original precision/float SQL1. No failed, zero-match or interrupted run. The ignored test is not a pass.

Four new tests (SDK2/native2);98 SDK and198 native old test bodies in changed files remain byte-identical. Native CAST orchestration, binary reader and original datatype test/fixture files are unchanged. The new facade test verifies a90-byte owned coefficient allocation moves through the bridge and checks inline integer storage; it is not a physical-memory, zero-copy or performance guarantee. The new SQL test uses10 SELECTs across scalar/vector modes, covering signs, zeros, bounded exponents, parse/precision warnings, stored Decimal shape/arithmetic and the LF/TAB distinction. No new resource refusal or whole-CAST claim follows.

## Deferred

CAST/M2 completion, six complex candidates and broader request-root/default-NoColumns/liveDAG/final acceptance remain. Full expression/unistore/workspace/lint/dev/bazel/release/exhaustive/performance/physical/transient memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness are unverified. Historical R100 expression4/unistore1 failures remain unrepaired. Overall goal remains active.
