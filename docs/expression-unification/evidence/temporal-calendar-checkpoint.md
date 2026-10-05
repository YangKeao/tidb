# Shared temporal foundations and expression string coercion

**temporal-calendar-119 / R122**, following [duration controls](duration-control-checkpoint.md). Functional238/245, strict0 and remaining7 are unchanged. This is partial CAST/M2 evidence, not whole YEAR/DATE/CAST, temporal-family or Go-package acceptance.

## Ownership

SDK `codec/native_temporal_convert.rs` owns six entries: temporal kind conversion, temporal fractional rounding, duration-to-calendar conversion, duration-to-year conversion with event, year adjustment and parsing a raw YEAR into temporal storage. `NativeYearConverted::into_result` is the sole strict event fold; native methods only project raw values and Year overflow events.

SDK `native_coerce_string.rs` owns the complete19-kind expression string selector. Native `coerce_str` delegates. Real uses raw f64 Rust Display and Float32 narrows to f32 before Rust Display; neither substitutes Go SQL float rendering. Original kind-specific UTF-8 errors, NULL, sentinels, hybrid names and JSON display boundaries remain. No host parser, formatter or conversion callback is supplied.

## Preserved distinctions

- Kind conversion sets the requested kind before same-kind/raw-zero early return. DATE forces FSP0 without clearing clock fields. Only timestamp-gap failure takes the adjustment route, returns DateTime/FSP0 and revalidates.
- Rounding returns DATE or exact raw-zero storage before target-FSP validation. Other values normalize target FSP before same-FSP return. Valid-calendar rounding reprojects the rounded instant through the zone; any calendar conversion error uses the original clock-only fallback, with cross-day error. Original chrono addition panic behavior is retained.
- Duration calendar conversion uses the timestamp's zone and its civil midnight, rejects ambiguous/nonexistent midnight, adds raw elapsed nanoseconds with checked arithmetic and preserves original cached-offset behavior. It does not borrow the extra reprojection from temporal rounding.
- Concat YEAR uses shared duration rounding and raw HHMMSS, without reading timezone/date fields. Other YEAR conversion uses the original calendar path. Overflow subjects retain the original year; strict conversion folds an event to `OutOfRange("year")`.
- Raw YEAR parsing treats zero as DATE/raw0/FSP0 and otherwise checks only u16 admission before original bit packing. It does not add a9999 or MySQL YEAR range check.

## Boundaries still native

YEAR/DATE expression controllers and signed-datum fallback are not closed. The existing integer cast SDK calls native `Datum::to_i64_in` for remaining domains. Its actual Enum/Set ordinal cannot be inferred from the19-kind string view's names. Temporal numeric formatting, integer text conversion, JSON integer policy and binary-literal integer conversion require a subsequent coherent M2 slice. This checkpoint does not disguise that gap with host callbacks or synthetic metadata.

## Validation

All11 launches passed with nonzero matches: SDK foundation2/coercion2, native datatype474, new coercion1, existing YEAR2/round1/gap1/string-consumer1, and new SQL1 plus two old SQL gates. No compile/test failure, zero-match, retry or interruption. Eight new tests (SDK4/native4);256 old native test bodies are byte-identical, and no existing SDK test body is in the changed files. Exact commands, counts, timings and raw-log hashes are in [the receipt](../logs/temporal-calendar-summary.txt).

The new SQL test fixes the statement clock through real SET timestamp. Eight standalone SELECTs contain22 cells (8 YEAR integers,14 temporal values): four clock/zone queries, two YEAR/DST queries and two empty gap-table queries. Two additional strict typed INSERT…SELECT probes assert1292/22007, full error text and one error warning, then verify no row was stored. Fixed headers, actual Time kind/FSP and literal displays are checked in both vector modes. This covers duration/calendar year boundaries, raw YEAR field injection, DST rounding and typed timestamp-gap refusal through existing controllers.

Preflight explicitly rejected using string-gap storage output as a typed-gap oracle: kind conversion resets to DateTime/FSP0, and downstream kind propagation remains untouched. Non-strict typed-gap storage is not verified here. Normal SQL has no supported concat=true switch; concat mode is covered by direct SDK/native units and existing expression tests, not invented SQL evidence. New tests use source-derived literal expectations, not provider-output recording. Direct context-demand tests and caught original JSON formatting panics are not physical-resource proof or failed-test evidence.

## Remaining

Other CAST/YEAR/DATE, broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance remain. Full suites/lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness remain unverified. Historical R100 expression4/unistore1 failures stay unrepaired; goal remains active.
