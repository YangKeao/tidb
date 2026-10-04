# Partial CAST: Real/Float32→UNSIGNED

Checkpoint **cast-real-uint-88**, after **nullif-87**. This is an M2 conversion slice, **not whole-CAST completion or a new family**. Functional226/245, strict0, remaining19 and all226 family objects are unchanged. Latest whole-family functional checkpoint remains nullif-87.

## Actual algorithm deletion

The production `real_to_u64_saturating` body in native `cast.rs` is removed. TiKV `native_cast.rs` now solely owns its original policy:

1. Round the actual f64 storage value with ties-even, including a Float32 datum's existing f64 bits without narrowing.
2. If rounded value is negative, return its saturating signed conversion reinterpreted as u64 and report the rounded overflow value.
3. Otherwise, nonfinite or at/beyond2^64 returns u64MAX plus that event.
4. Remaining in-range values return u64 without an event.

This preserves negative zero/no-event, upper-half unsigned values, negative huge wrapping, ±Inf and NaN behavior. It is not interchangeable with the pre-existing TiKV wire cast, whose rounding, clipping, upper-bound warning and NaN policies differ. That wire implementation is unchanged, not used as an oracle for this native domain.

`to_u64_unsigned_in` and both unsigned `eval_cast` branches propagate `Result`. The old infallible helper name survives only as a `cfg(test)` thin expect wrapper for immutable existing tests; it contains no conversion algorithm or production fallback.

## Closed worker and event boundary

`CastRealUnsignedNative`: Values/unary Bytes/1call/OwnBytes, with actual native Real or Float32 identity as input. Raw IEEE values are admitted; None, malformed frames and other identity kinds are rejected. Existing identity transport is unchanged.

The single producer returns `NativeCastRealUnsignedResult { value, overflow_bits }`. Canonical reports are9bytes (`flag0 + u64LE`) or17bytes (`flag1 + u64LE + rounded overflow IEEEbits`). The decoder checks structure without repeating numerical policy. NULL output is invalid.

Common ready-value preflight, including direct-ready, uses the same producer only for the9/17byte reply bound; the real dispatcher still executes afterward. Actual input capacity and the existing Bytes row-metadata floor/postflight remain charged. Pure producer computation happens for planning and execution; no performance/zero-copy claim is made.

`tikv/cast_real_unsigned.rs` only encodes actual input, decodes the computed report, presents optional1690 and returns computed u64. There is no native rounding, finite/range check, overflow reclassification or cached answer. Warning presentation still uses native `format_float_g_shortest` (`mydecimal.rs172–209`), which is **not migrated or already shared merely because a separate CPP Decimal formatter exists**.

The existing warning remains `append_warning`, not strict-mode error promotion. With zero slots no producer result exists and no1690 is appended; old native warning-before-new-worker timing is not claimed. Warning-sink panic is covered by the active scope guard and poisons that scope.

## Explicit exclusions

- Signed targets and all other source/target conversion domains.
- Outer NULL fast paths.
- `UnsignedInUnion` negative early-zero bypass.
- Diagnostic formatter migration and broader request-root/default-NoColumns ownership closure.
- Legacy TiKV wire conversion policy changes.

These remain visible in source and tests. A shared signature name does not mean all CAST paths are migrated.

Existing unsigned `CastRealAsInt` PB reaches Shared and this slice without new admission. Signed and NULL controls under the same signature still succeed on their old paths even with zero slots. The PB test uses six real Float64 literals, including a negative NaN that roundtrips through the actual float codec; it does not fabricate a protocol or claim that legacy positive-NaN encoding behavior was fixed. Direct native Float32 storage is covered separately.

## Core evidence

[Commands, counts, times and hashes](../logs/cast-real-uint-summary.txt); [manifest](../checkpoint.json).

Seven new tests all pass on first matching execution: one producer test, two local admission/budget tests, one bridge/lifecycle test, one cast-policy test, one existing PBShared test and one SQL test. Cases include ties-even, raw Float32 storage, full unsigned range, nonfinite values, strict-warning policy, malformed reports, actual input capacity, warning panic and no-owner operation. Existing cast test bodies remain untouched.

SQL36SELECT=8stored DOUBLE/FLOAT columns × vector0/1 × slots1/0=32direct probes (16positive,16slice-root refusals), plus2strict overflow-warning checks and2positive filters. No earlier function, condition or filter substitutes for the direct CAST worker. Results retain unsignedLongLong20/0/binary metadata and actual UInt values. Expected warnings come from the original source/fixtures, not provider output.

Nine locked nonzero single-threaded launches:7green,2only-old full RED. CPP core1/local341+1ignored; native cast21/bridge1/gateway196+1ignored; legacy1 and SQL1 pass. Full expression1598/4old/94ignored and unistore219/1old/13ignored remain RED. No compile failure, new execution failure, oracle correction, zero-match, interruption or fixture recording.

Entire full-suite failure sections are byte-identical to R90 after normalizing only numeric panic-heading thread IDs: expression `411274feba11e202735df5a8056c72babbc8cf9df2eab3258c670e9289a1ee95`, unistore `2f19c9ad5338a86c48b895e3923c3e39454117e3410d47df5aa3fb4254ed7ce1`.

Source audit:8CPP/7native Rust files,2new modules;205CPP/493native original test bodies byte-identical,3CPP/4native new tests. Pinned formatting/diff checks pass. E independently reviewed the producer, report/domain, budget/dispatch, Result propagation and exclusions without a blocker. Agent-doc review is descriptive with no new policy. No Cargo/lock, Go/Bazel/generated or `compile.rs` changes; unrelated untracked BUILD excluded.

## Next work and nonclaims

[Remaining acceptance](remaining-acceptance.md):5core,8ordinary pending,6complex candidates. Continue other CAST/M2 domains and actual diagnostic sharing, then complete extrema/IN/INTERVAL and cross-entry owner/deletion evidence. Extrema cannot be closed by a small wire alias: native tie-first, collation, numeric scale/promotion, NoColumns comparison and temporal/vector policies differ from existing wire paths.

Known full-suite failures and CAST/INTDIV/mode/JSON/vector/older Values gaps remain. Workspace/lint/dev/bazel_prepare/release, exhaustive differential, TiFlash/FIPS, performance/physical heap/stack/OOM/allocator/zero-copy/dual-tzdata and complete Go-package transcreation are unverified. No whole-goal completion or PR-readiness claim.
