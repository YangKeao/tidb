# Shared duration conversion and CAST controls

**duration-control-118 / R121**, following [JSON source policies](json-source-checkpoint.md). Functional238/245, strict0 and remaining7 are unchanged. This is a duration slice, not whole temporal/CAST/M2 or Go-package acceptance.

## Ownership

SDK `codec/native_duration_convert.rs` owns six conversion entries: native duration rounding, NumberToDuration, StrToDatetime, StrToDuration, text-to-duration and datatype duration-target selection. Native public functions project values/events/errors; the private duration text selector is removed. DurationRoundError aliases the shared enum, while native value carriers keep their existing representation. Review also removed a duplicated JSON-unquote selector: `mysql/json/native_text.rs::native_unquote_binary_json` is now the sole owner, used by both the duration target and native BinaryJSON's public adapter. Its raw writer/string primitives and their old tests are unchanged.

SDK `native_cast_duration.rs` owns ordinary, argument and computed-duration control. A source descriptor carries actual effective code, raw flags and decimal precision. The native bridge supplies only a lazy session-zone getter, applies the returned generic truncate effect, and wraps raw result storage. No host parser, formatter, conversion or source-selection callback remains in these controllers.

## Preserved policies

- Ordinary CAST renders first, including the original JSON Display panic boundary. Non-temporal/non-string JSON tags warn and return NULL without reading the session zone.
- Integer-source selection uses actual metadata or the no-metadata Int/UInt kinds; UInt uses low64-bit signed reinterpretation. A mismatched actual kind reports the original integer-datum error without a zone read.
- Other sources read the zone before conversion, including NULL and unsupported kinds. The private datatype leaf rejects NULL; the expression controller preserves the original generic converter's outer NULL bypass.
- Event-free values return directly. Truncation/overflow produces one SDK-selected message; numeric sources become NULL, nonnumeric sources retain the converted value. Native applies `handle_truncate` before exposing that result, so strict/error behavior remains contextual. No RoundedToScale event is produced by this closed duration cluster.
- Argument conversion passes actual Duration/NULL through before context access, retaining raw nanoseconds/FSP. Only effective known Date/Datetime/Timestamp metadata supplies declared FSP; others use6.
- Computed conversion derives FSP from the byte length after the last dot, capped at6, then performs the original second SQL rendering. It does not substitute a temporal FSP parser.
- Number conversion checks ±8385959 before absolute value. Large positive values try numeric datetime in UTC; invalid MM/SS yields zero with FSP0 and truncation without first validating requested FSP.
- Rounding normalizes target FSP before equal-FSP early return, uses i128 arithmetic, rounds negative exact halves toward zero, checks i64 overflow and does not apply SQL TIME range clamping.
- Text conversion preserves the12-digit datetime-first alternative and original event subjects. JSON unquote retains its second unescape and distinct invalid-binary/invalid-text errors; nonstring display is unchanged.

YEAR/DATE remain outside this slice. Their `coerce_str` behavior is not SQL rendering, hybrid fallback needs numeric fields absent from the19-kind string view, and calendar/year/clock-dependent leaves are still native. Wire Duration, date-mode propagation and historical diagnostic quirks are not repaired here.

## Validation

Final ten filters passed: SDK foundation2/unquote1/controller2; native datatype472, new wrapper1, original source-domain1, typed-signature1 and argument-wrapper1; new SQL1 and original duration-helper SQL1. Twenty launches total:19 matched GREEN and1 matched RED, no compile failure, zero-match or interruption. Nine new tests;6 old SDK and276 old native test bodies remain byte-identical. Exact commands, counts and raw-log hashes are in [the receipt](../logs/duration-control-summary.txt).

The real RED was a new-test oracle mistake, not a production regression: unknown JSON tag255 was incorrectly expected to panic. Source tracing shows display ignores its decode failure and yields empty text, then duration parsing returns `Comparison("invalid duration format")`. Only that new test was corrected; an explicit root +Inf fixture separately checks the actual formatting panic. The full datatype library then passed472. Original RED and corrected GREEN remain. Subsequent review removed the duplicated unquote selector, appended its SDK test and native public-adapter assertions, and reran all final filters against that closure. Prior-stage passes are not presented as final-code runs.

The new SQL test has8 SELECTs (4 projections ×2 vector modes),36 cells:30 raw Duration values and6 NULLs, plus2 fixture INSERTs. It pins signed/unsigned/real/decimal/text sources, numeric-event NULL versus string clamp/prefix values, ordered1292 warnings, negative stored-duration half rounding versus text parsing, numeric/text datetime fallbacks, and actual TIME(3) storage with two1264 warnings. All TIME headers and payload FSP3 are literal expectations. This proves ordinary CAST and datatype DML conversion; argument/computed, byte-cap and generic strict effects are direct-unit/old-gate scope, not new SQL claims. No provider output was used as an oracle and no old fixture was changed. Direct wrapper tests cover context demand and generic effects, not physical resource accounting.

## Remaining

Other CAST/YEAR/DATE, generic typed child evaluation, broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance remain. Full suites/lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness remain unverified. Historical R100 expression4/unistore1 failures stay unrepaired; goal remains active.
