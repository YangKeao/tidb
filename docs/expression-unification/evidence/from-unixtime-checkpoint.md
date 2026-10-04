# FROM_UNIXTIME evaluator checkpoint

Checkpoint **from-unixtime-81**, following **decimal-presentation-80**. Functional coverage is **221/245 (90.20%)**, strict0, with24eligible families remaining. Only FROM_UNIXTIME is added; all prior220 family objects are unchanged. Reaching the functional threshold does not complete M0–M6. The R83 Decimal/float/diagnostic SDK is used unchanged; no new dependency or general-parser policy is introduced.

## Ordinary stages

Five closed profiles: FromUnixTimeNumericNative, FromUnixTimeTextNative, FromUnixTimeLocalNative, FromUnixTimeLegacy and FromUnixTimeNullNative. Numeric takes actual Int/UInt/Decimal/Real/Float32 identity bytes, including original raw Decimal representation; other ordinary values use their original frontend string coercion and the Text profile. Actual absence uses the Null profile. No host finite check, Decimal presentation, fraction selection, epoch math or local formatting remains.

Head output is actual NULL or an epoch report: tag0+secondsLE8+microsLE4+FSP (14bytes), or tag1 with the same actual epoch fields followed by computed truncate text. Native replays handle_truncate before reading the zone. Error stops there; Warn/Ignore proceed according to the original Columns policy. The complete original report, including a warning when present, moves unchanged into TemporalValue with the actual zone. The local worker reads the epoch fields without replaying the warning. It returns nullable actual UTF8, not a host-built base.

Only valid local output reaches original layout coercion and the existing DateFormatTextNative bridge. Nested scoped callbacks keep these family stages under the original selected owner, including one-shot use; no alternate pool or re-discovered authority. Eager upstream argument evaluation is unchanged: only leaf coercion is delayed.

Preserved source details: native Decimal visible rather than storage text; Float32's original f64 bits without narrowing; text integer-parse failure warns and continues with computed epoch0/FSP0, while numeric parse failure silently returns NULL. -0.x retains its original positive fraction behavior. Only the first nine fraction characters are validated; later suffixes remain ignored. The maximum integral value is checked before HalfUp rounding, allowing carry to MAX+1. Fixed zone uses the original raw offset, not a validity clamp. The old native instant_to_local test name is only a cfg(test) alias to the moved helper.

## PB and legacy

PB NULL short-circuit forwards only the observed NULL, retaining uncoerced prefixes, unread suffixes and original arity priority. The one-argument PB return-time cast and outer declared-family conversion remain unchanged; their mode/zone reads are not confused with the ordinary leaf's single zone read.

Legacy's first child remains typed Decimal; absent/wrong-type/SQL-folded values reach the actual nullable worker. Nonnull input carries raw Decimal identity and the separately borrowed request zone. Columns is scope capability, not a replacement timezone. TiKV retains original visible-text-to-f64 conversion, f64 range, truncation, rounded nanos and ordinary u32 nanos-times1000 operation. The existing overflow/panic/release behavior is not silently fixed. The resulting DateTime has FSP0 but may retain raw microseconds.

Two-argument legacy retains missing-first wire-shape panic and only evaluates layout after a computed Time. A short-lived LegacyEvaluator forwards selected raw_columns without replacing row/request settings or SimpleExpr::Shared's own context. Lossy UTF8 layout conversion stays at the original frontend boundary, followed by existing DateFormatCoreNative using the actual computed core and optional layout. Base and formatter share scope; broader Shared-child/M6 propagation is explicitly not claimed.

Existing TiKV wire FROM_UNIXTIME kernels retain their independent exact Decimal, precision, timezone and UTF8/error policies. They are not replaced with a native compatibility alias.

## Validation and limits

Eight exclusive writers plus parent integration;18Rust files, two new modules and10new tests. Eleven locked nonzero launches: seven green, two new-test failures followed by successful retries, and two unchanged old full-suite failures. Final gates: CPPcore2/wire2/local330+1ignored, native root7/gateway196+1ignored, legacy1 andSQL1. SQL covers42SELECTs:20normal,20zero-slot and2filters. Full expression1582/4old/94ignored and unistore213/1old/13ignored keep exactly normalized failure sections and remain RED. CPP205/native476 original test bodies, three wire FROM bodies and two other native production bodies are byte-identical. Pinned formatting/diff checks pass; dependencies/locks/generated/Go/Bazel/fixtures are unchanged.

Two new input setups—not production or expected results—were corrected: raw1e-9 needs a nine-byte coefficient, and malformed identity must truncate its fixed header rather than pop an allowed coefficient byte; legacy's original f64 upper literal rounds to32536771200, so the outside probe is32536771201. Both are established from original source and binary64 reasoning, not provider output. All10new tests finally pass; eight passed first matching gate. [Exact commands, attempts, hashes and proofs](../logs/from-unixtime-summary.txt) retain both red runs.

R84 Numeric14/Textmax(14,inputlen+64)/Null0 reply preflight is in the common ready path, including direct-ready callers; Local/Legacy retain64-byte plus zone-capacity checks. The older generic Values minimum-payload precharge with postflight, including R82 clock, is not repaired or newly claimed bounded.

Reaching the functional221 threshold does not complete M0–M6 or strict auditing. General type/parser work, default-NoColumns/M6, earlier mode forwarding/CAST warning/INTDIV raw-empty/JSON/metadata gaps remain. Whole workspace/lint/dev/bazel_prepare/release, exhaustive differential/TiFlash/FIPS and performance/allocator/physical heap/peak/OOM/zero-copy/dual-timezone footprint are unverified. No package-transcreation or PR-readiness claim.
