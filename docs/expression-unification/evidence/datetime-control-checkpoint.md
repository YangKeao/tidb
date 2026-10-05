# Shared DATE/DATETIME controller

**datetime-control-122 / R127**, after [YEAR control](year-control-checkpoint.md). Functional238/245, strict0 and remaining7 stay unchanged: this is a CAST slice, not whole CAST/M2 or Go-package completion.

SDK `native_cast_time.rs` owns source-aware temporal conversion, parser choice, DATE clock clearing, zero-date decisions and warning classification. The same owner serves explicit CAST, computed temporal values and argument conversion. Existing datatype parsers/rounding/calendar primitives remain their sole implementations. Native bridges supply actual text/numeric views, physical source code, raw target kind/FSP, lazy modes/clock/zone getters and a warning effect.

## Preserved distinctions

- YEAR source returns its injected raw calendar fields before normal target finishing. Duration uses the statement timestamp's fixed offset, not a substituted session-zone calendar. Only Some(FSP) performs its later zone-demanding round. Neither early path is routed through ordinary DATE clock clearing.
- Argument Time/NULL returns immediately; other arguments use DateTime with unspecified FSP. Actual kind/FSP/storage pass through unchanged on early returns.
- Other values first coerce text, then read modes and the zone. Numeric, Decimal, float, temporal and text source parsers remain distinct. Decimal formatting uses the same shared visible formatter as before.
- Truncation and DST warnings retain order. DST formatting reads the zone again; it is not cached. String/Bytes alone receive the post-parse NO_ZERO_DATE check. Ordinary DATE clears the clock only at its original final step.
- Parse-failure diagnostics use UInt reinterpretation or shared datatype-to-i64 in UTC, retaining failure-to-original-text fallback. This is not signed CAST's error-to-zero policy. Existing warning-classifier arithmetic panic behavior is preserved.
- The known caller-side date_modes forwarding gap and zero-date diagnostic quirks are not repaired here. No hidden origin tags, fabricated descriptors or host parser callbacks are introduced.

## Validation

Six final gates GREEN: SDK2, native bridge1, existing CAST20/argument8 and two SQL gates. Seven matched launches include one initial SDK test failure: its new invalid-FSP assertion wrongly used7, which the original native check_fsp and SDK native_normalize_fsp clamp to6. Replaced only that new input with-2, the source-defined invalid value; production and old tests unchanged, RED retained. [Exact commands/counts/hashes](../logs/datetime-control-summary.txt).

Four new tests (SDK2/native2),221 old native test bodies unchanged; no old SDK test bodies occur in touched files. New SQL executes two SELECTs/10 cells over both vector modes: DATE clock clearing, Decimal numeric parsing, DST carry, UInt wrap and malformed-text diagnostics, exact warning order and temporal code/FSP metadata. Existing calendar SQL validates clock/YEAR/DST consumers too. Old tests and fixtures remain immutable; new expectations derive from source. Other implicit/typed/write conversion selectors, broader M2/root/liveDAG/final acceptance remain. Full suites/lint/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness remain unverified. Historical R100 expression4/unistore1 failures remain unrepaired.
