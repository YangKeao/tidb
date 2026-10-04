# UNIX_TIMESTAMP evaluator checkpoint

Checkpoint **unix-timestamp-79**, following **timestamp-78**: **220/245 functional, strict0**,25eligible remain and1needed for221. R81 bounded source review established the contract; R82 implements it. Frozen family `unix_timestamp` includes the ordinary clock/contextual forms, native PB UnixTimestampInt/Dec and both legacy unistore roots. All219 prior family objects are unchanged; FROM_UNIXTIME is unchanged and unclaimed.

## Three policies, all implemented in TiKV

`native_unix_timestamp.rs` owns ordinary parsing/epoch/result shaping and the two strict-Time legacy compatibility kernels. Existing wire kernels remain separate because their timezone, gap and Decimal-return policies differ. Their identical microsecond range predicate is shared, not their different semantics.

Six profiles use actual operands:

| Profile | Input | Output |
| --- | --- | --- |
| UnixTimestampNowNative | Existing Int2: actual seconds and full-u32 nanos | Int identity |
| UnixTimestampNullNative | Actual nullable input absence, Bytes(None) | NULL |
| UnixTimestampParseNative | Existing TemporalParseText: text, runtime numeric-kind flag, zone1 | NULL, numeric identity, Time continuation or warning |
| UnixTimestampValueNative | New unary TemporalValue: actual SDK Time frame and fresh zone2 | Int/Decimal identity |
| UnixTimestampIntLegacy | TemporalValue: actual typed Time and borrowed request zone | Int identity |
| UnixTimestampDecLegacy | Same actual legacy inputs | Decimal identity |

Native final materialization uses identity decoding only: no epoch math, Decimal parsing, calendar projection or formatting. Standard Time/Int/Decimal frames are reused; computed warnings use tag17+LE1292+UTF8. No host-built parsed base or ready result is injected.

## Ordinary demand and arithmetic

Arity/left coercion retain frontend precedence. SQL NULL reads no zone. Nonnull text preserves Int/UInt/Decimal/Real/Float32 source classification and uses TIME get_fsp, not TIMESTAMP's duration-first-dot helper. Zone1 is read once for the shared parser. Parsing failure produces the original warning and NULL; all-zero YMD is NULL even with nonzero clock fields. Partial-zero or invalid civil/clock input returns numeric zero preserving parsed FSP. These branches never read zone2.

Only a valid civil base produces a continuation. The existing scoped callback passes its original SDK frame unchanged and reads zone2 freshly; the two getter results may differ. Complete yearzero, pre-epoch or above-range dates still demand zone2 before range clipping. Local/Named use the exact R79 Naive transition helper; Fixed subtracts the original raw i32 seconds without a FixedOffset validity clamp.

Zero arguments read actual statement clock seconds/full-u32 nanos once and ignore the original offset. The original i64 multiply/add is kept, not replaced with checked/saturating/i128 or chrono validation; values at or above one billion nanos remain accepted. Extreme arithmetic/physical-OOM/release behavior is not newly certified by this migration.

Range clipping remains inclusive 1_000_000..=32_536_771_199_999_999 microseconds. FSP0 yields Int; positive FSP truncates fraction exactly and emits Decimal with scale/storage=FSP, including exactly FSP zero coefficient digits. No second rounding or host Decimal::from_literal remains.

## Legacy and PB closure

Legacy roots retain first-child-only eval_time, missing/wrong-type/NULL behavior, existing child SQL-error folding and infrastructure propagation. A typed None reaches the actual nullable worker; invalid nonnull cores are never precomputed as host zero. `tikv/unix_timestamp.rs` takes typed Time, the separately borrowed request zone and Columns capability. It never calls Columns.time_zone or reconstructs a zone from name/offset. Date/DateTime/Timestamp kinds and all raw FSP bytes survive framing; Date's hidden clock is not cleared.

Legacy uses existing native_core_to_datetime(raw,zone,false). Zero/conversion failure/gap/out-of-range gives Int0 or Decimal coefficient0 with scale/storage0. Valid Decimal always uses all six fractional digits and coefficient=micros, regardless of source FSP or return metadata. Int seconds are only widened from SDK i64 to native i128. Native PB signatures keep their existing shared ordinary root, not these legacy policies; the unchanged outer ScalarFunction::coerce_to_ret_type still converts the result to the declared family. Thus Decimal1.123 becomes Int1 for a LongLong result, while an integral ordinary result widens to Decimal1 for NewDecimal; Decimal-to-Decimal is not forced to legacy's scale6. The observed-NULL branch now reaches the NULL worker without coercing prior values, reading suffix children or changing old arity short-circuit behavior.

Existing TiKV wire kernels retain earliest-overlap/transition-gap behavior and return-field Decimal rounding. They are not falsely claimed equivalent to either native policy.

## Carrier, ownership and evidence

TemporalValue is a closed unary Bytes role with the same move-bound zone holder/RAII and prebind/output-overlap capacity accounting. Native continuation requires DateTime/FSP<=6; legacy accepts all Time kinds/raw FSP without calendar preflight. Numeric reply bound64 is explicit. No pool, EvalConfig, resource cause, factory budget, wire signature or ordinary admission is widened.

Eight exclusive subagents plus parent integration; eighteen Rust files, two new modules and ten new tests. Ten locked launches completed nonzero tests: seven green, one new-test expectation failure followed by its green retry, and two unchanged old full-suite failures. CPPcore2/wire2/local332+1ignored, nativeUnix8/gateway196+1ignored, legacy2 and SQL1 pass. SQL44 SELECTs cover20normal+20real-column zero-slot refusals+2filters+2controlled clocks. Full expression1578/4old/94ignored and unistore212/1old/13ignored retain exactly normalized failure sections and stay RED.

The NEW PB test initially forgot unchanged outer declared-family conversion. Only its two expected cases/comment were corrected from existing scalar_function.rs and datatype conversion source, all byte-identical to the previous commit; no original oracle or production coercer changed. Other nine new tests passed their first matching gate. CPP203/native471 original test bodies were byte-compared. [Exact receipts](../logs/unix-timestamp-summary.txt) retain every attempt, hash and source-backed correction; no source result is treated as runtime evidence.

FROM_UNIXTIME, remaining evaluator closure, M6/default-NoColumns propagation and previous planner mode/CAST warning/INTDIV raw-empty/other compatibility gaps remain open. Whole workspace/lint/dev/bazel_prepare/release, exhaustive differential/TiFlash/FIPS, performance/allocator/physical heap/peak/OOM/zero-copy/dual-timezone footprint remain deferred. No whole-package transcreation or PR-readiness claim.
