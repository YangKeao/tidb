# Shared numeric conversion event precedence

**numeric-event-140 / R146**, after [reverse-bound control](reverse-bound-checkpoint.md). Functional238/245, strict0 and remaining7 unchanged; no whole CAST/M2/package credit.

SDK `native_conversion_event.rs` owns generic Result outcome extraction and first/second event selection. It preserves the distinct policies: generic `prefer_event` gives the second event priority; signed/float composition gives a parsed truncation priority while fatal, but gives a subsequent bounded overflow priority when truncation is warning/ignored.

Native retains actual `ScalarConversionEvent` and `ScalarConversionError` construction, conversion values and all Diagnostics warning/error calls. SDK returns only ownership-preserving outcome/event-source selections, without callbacks or cloned typed errors.

## Validation

Five matched Cargo gates GREEN: SDK2, full native datatype483, new/existing precedence2 and session SQL consumer1. [Exact commands/counts/hashes and native-agent stop chronology](../logs/numeric-event-summary.txt). Three new tests;0 SDK/27 native old touched-file test bodies unchanged. One new SDK file, no new native file. Session/SQL files unchanged; no new SQL fixture/probe/cell credit. Other datatype controllers, expression rendering and broader M2/root/liveDAG/final acceptance remain. Full expression/unistore/workspace/lint/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/package/PR readiness unverified. Historical R100 expression4/unistore1 failures and prior RED receipts retained.
