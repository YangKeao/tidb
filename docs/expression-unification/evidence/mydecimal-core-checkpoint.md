# Fixed-word MyDecimal sharing

**mydecimal-core-104 / R107**, after [legacy DATE_ADD/SUB](legacy-date-arithmetic-checkpoint.md).
This is a type-layer step: functional237/245, strict0 and eight remaining families are unchanged. Existing family objects and historical slices are preserved.

## One algorithm owner, original storage facade

TiKV datatype `mysql/native_mydecimal.rs` owns the existing fixed-word MyDecimal parsing, rounding, shifting, comparison, numeric conversion, rendering, raw representation, parts conversion and hash construction. It reuses the existing shared float formatter. This is not an alias of wire Decimal: the layouts, parser/status rules and rounding domains differ.

Native `mydecimal.rs` retains the real `repr(C)` 40-byte facade with its original private fields, Clone/Copy/Debug/Default and fieldwise equality. Thin safe adapters copy three signed count bytes, the sign and all nine signed words. Inactive words, hidden fractions and distinct stored/result fraction counts survive; no string round-trip, validation, normalization or unsafe cast is inserted. Original tests still inspect the actual native storage type, not a substitute test-only layout.

Mutable adapters retain the original destination contents and write back through a private guard on normal return, soft error and unwind. Separate `round` and `round_in_place` paths retain their original aliasing policy. Checked and unchecked raw decoding remain distinct; the latter is not strengthened into a canonical decoder. Extreme shift/padding and existing panic domains are not repaired incidentally.

## Required binary writer

`mysql/native_decimal_codec.rs` owns binary-size calculation, checked size, leading-zero scanning, binary writing and the original error/warning enums. Native `decimal/codec.rs` delegates these leaves; binary reading and value/word adaptation remain separate. MyDecimal hashing uses this same shared writer rather than copying it into the new module.

The original checked-size domain does not require fraction<=precision. The writer retains its existing debug assertion, partial writes and empty-output panic, rather than introducing new error returns. Later fraction truncation may override an earlier overflow warning. Hash construction still strips significant zeros, ignores the original soft writer warning and appends the fraction byte.

## Evaluator integration

R106's legacy arithmetic protocol is unchanged. Its generic MyDecimal preparation now reaches the shared implementation through the same native facade: original parse status is discarded, SDK-directed scale-zero rounding uses HalfUp, and rendering remains the original operation. No new evaluator profile, carrier, PB/legacy admission or function-family credit is added.

Visible storage rendering remains distinct from result-fraction rendering. The public general digit-string Decimal parser, broader CAST/M2, binary reader, IN and broader execution-context propagation are not declared complete. This is sharing of existing Rust implementation, not a completed Go-package transcreation.

## Validation

[Commands/counts/hashes](../logs/mydecimal-core-summary.txt): ten locked single-threaded launches, all nonzero and passing on the first attempt. No compile failure, test failure, zero-match, retry or interrupted run.

Two new SDK tests cover raw/status/render/round paths and source-derived binary bytes; one appended native test covers actual storage and unwind writeback. All21 original tests in changed native files and the complete original MyDecimal test/support suffix remain byte-identical. No old fixture or oracle is changed; the new hash expectation is derived from precision3/fraction2 binary encoding, not recorded provider output.

The full native datatype suite passes462 tests. Targeted chunk10, codec decimal6 and codec hash7 tests pass, covering real raw40 storage, hidden/result fraction handling, binary encoding and hash consumers. The existing bridge test,56-mapping legacy test, three original legacy tests and R105's52SELECT SQL fixture also pass. Zero new SQL probes or evaluator profiles are claimed.

## Deferred

Whole expression/unistore suites, workspace/lint/dev/bazel/release, exhaustive differential, performance, physical/transient memory, allocator/OOM, TiFlash/FIPS, dual-tzdata and PR readiness remain unverified. Historical R100 expression4/unistore1 failures remain unrepaired. The overall goal remains active.
