# Shared string target production

**string-target-137 / R143**, after [numeric text helpers](numeric-text-checkpoint.md). Functional238/245, strict0 and remaining7 unchanged; no whole CAST/M2/package credit.

New SDK datatype owner `native_string_convert.rs` holds ProduceStr byte/rune limiting, complete valid UTF-8 prefix selection, whitespace-tail classification and binary fixed-string zero padding. Named string/blob/char/varchar classification extends existing `native_string_type.rs`; raw unknown identities are never reinterpreted.

SDK reuses canonical UTF-8 decoding and rune counting. Character limits retain Go RuneError width-one behavior for invalid input; non-binary blobs stop before an incomplete/invalid terminal rune while preceding bytes remain byte-preserving. Logical rune length is computed only when diagnostics are enabled. Whitespace-only VARCHAR tails request warning plus truncation event, fixed CHAR tails truncate quietly, and all other overflows request Data Too Long. Native constructs typed errors from lazy descriptor messages and projects actual FieldType/charset/storage.

## Validation

Five matched Cargo gates GREEN: SDK target2/named type1, full native datatype480 and existing SQL2. [Exact commands/counts/hashes and subagent incident](../logs/string-target-summary.txt). Three new tests;1 SDK/24 native old touched-file test bodies unchanged. One new SDK file, no new native files. Expression/SQL files unchanged; no new SQL fixture/probe credit. The native subagent failed after its final successful edit; parent adopted, formatted and validated the complete adapter/test, and the subagent later confirmed no known unfinished concern. Other datatype source conversion/event merging/controllers, expression rendering and broader M2/root/liveDAG/final acceptance remain. Full expression/unistore/workspace/lint/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/package/PR readiness unverified. Historical R100 expression4/unistore1 failures and prior RED receipts retained.
