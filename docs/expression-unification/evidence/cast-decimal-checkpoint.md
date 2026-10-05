# Ordinary DECIMAL CAST control ownership

**cast-decimal-108 / R111**, following [shared digit parsing](decimal-parse-checkpoint.md). Functional **238/245**, strict **0**, remaining **7** stay unchanged. This is a partial CAST slice, not whole CAST/M2 or Go-package completion.

## Ownership

Native `cast.rs` delegates its ordinary DECIMAL arm and existing input-warning helper through `tikv/cast_decimal.rs` to SDK `native_cast_decimal.rs`. Native source selection, warning classification, Real prefix/exponent parsing and production-diagnostic bodies are deleted. Existing `to_f64_for_cast` String/Bytes consumers retain a thin `decimal_prefix` adapter over the same exported SDK prefix implementation; no duplicate parser or whole-DOUBLE claim is introduced.

SDK sees actual Decimal parts, Int, UInt, Real, String bytes, Bytes, Float32 or Other. Input warnings are synchronous and precede source conversion. SDK owns source choice, discarded conversion events/error-to-zero policy, unspecified-scale bypass, precision and production diagnostics. Native code only projects actual fields, executes requested default `Datum::to_decimal()` conversions and appends SDK-specified warnings.

Other conversion returns the actual `(value, event)` or error; SDK—not the adapter—discards the event or folds the error. These existing datatype conversion actuators remain outside this slice's algorithm-deletion claim.

Narrow raw transport moves/borrows/clones the existing SmallVec and five metadata fields. It does not normalize or validate representation. DTO cast/round methods reuse existing shared math and coefficient extraction; native private storage and existing ordinary math adapters remain intact. No new C4 profile, carrier, dependency, admission gate or wire/PB signature is added.

## Contracts kept distinct

- Warning parsing uses Unicode-trimmed text. Actual String/Bytes conversion parses original text independently. Invalid UTF-8 remains quiet zero. The first parse's value is never reused.
- Real retains Rust Display and its prefix/exponent policy. Its canonical negative magnitude uses the existing normalizer, retaining canonical zero. Float32/Other request the original default-context conversion—not caller timezone/type flags/truncation policy.
- A Decimal source clones actual raw metadata; unspecified scale returns it unchanged, including hidden storage and declared shape. Fresh arithmetic results clear shape as before.
- Production still casts first, skips diagnostics for zero precision, otherwise rounds for the original integer-width check and ordered overflow/truncation decision. Input overflow keeps its raw1690 template; production overflow uses the formatted target.
- The warning-only service is shared by existing UNION callers, preserving its infallible signature. Their negative bypass and other business logic remain native and unchanged.
- Outer NULL/range/vector guards, numeric-argument/vector fast paths, other CAST targets and the distinct legacy wire policies remain outside this slice.

## Validation

[Exact commands/counts/hashes](../logs/cast-decimal-summary.txt): seven nonzero green receipts—SDK2, native bridge1, original CAST7 (one preexisting ignored vectorized UNION gap), original UNION4, full native datatype463, new SQL1 and R110 SQL1. No compile/test failures, zero-match runs, interruptions or retries. Four new tests;2 SDK and221 native old test bodies in changed files remain byte-identical. Original `func.rs`, `scalar_function.rs`, rewriter and datatype fixture files are unchanged. The bridge test's zero-slot witness retains the old pure path, not new admission coverage. The new SQL test uses eight SELECTs across scalar/vector modes: stored Int/UInt/Real/Float32/Decimal, JSON event discard and SQL NULL, ordered double-overflow diagnostics, and build-time unspecified-scale comparison preparation. Existing vectorized string-to-DECIMAL UNION remains ignored/unmodeled, not newly passed or repaired. No fixtures/provider output are used as newly recorded oracles.

## Deferred and incidents

The native-consumer agent's first implementation turn failed at runtime without a closing message. The same file lease was resumed after inspecting actual file state; this is not a Cargo test failure or evidence that no edits occurred. Static consumer review also found previously missed `.map(decimal_prefix)` function-value references. The same SDK parser was exported and the original callers kept a thin adapter before compilation; no native fallback or fabricated fail-before receipt was introduced.

Whole CAST/M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance remain. Full expression/unistore/workspace/lint/dev/bazel/release/exhaustive/performance/physical-transient memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness are unverified. Historical R100 expression4/unistore1 failures remain unrepaired. Overall goal stays active.
