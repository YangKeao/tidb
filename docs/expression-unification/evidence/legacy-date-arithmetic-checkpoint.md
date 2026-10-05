# Legacy DATE_ADD/SUB closure

**legacy-date-arithmetic-103 / R106**, following [ordinary arithmetic](date-arithmetic-checkpoint.md).
Functional **237/245 (96.73%)**, strict **0**, remaining **8**. The235 previous family objects and historical partial/type records remain intact; `date_add`/`date_sub` are appended over the frozen implemented domain, not newly enabled signatures.

## Ownership and boundaries

`native_legacy_date_arithmetic.rs` owns legacy getter demand, loss-tolerant text handling, reformatting, arithmetic, rendering and explicit presence. `tikv/legacy_date_arithmetic.rs` is the native bridge. `cophandler.rs` retains static admission and requested generic child evaluation; its private interval/date/arithmetic algorithms are deleted.

Datatype `time/native_core_arithmetic.rs` owns raw CoreTime add_date/add_duration, normalization, day-number conversion and month-end adjustment. Native `core_time.rs` retains thin public delegates, its original DateAddError type and a test-only helper delegate. Native day-number cutoff3,652,500 deliberately differs from wire3,652,425. Raw clock fields, integer projections, reserved-bit repacking and duration-before-calendar ordering remain original; no new chrono validation is inserted.

The SDK may request original generic `MyDecimal::from_string`, optional `round_in_place(0, HalfUp)` and `to_string_bytes`; original statuses remain discarded. SDK—not the adapter—chooses whether this preparation is needed. These existing generic primitives **remain native**; there is no claim that MyDecimal or all M2 is shared, and no substitution with the different wire parser.

## Five closed profiles

| Profile | Carrier | Output subset |
|---|---|---|
| LegacyDateArithmeticTextHeadNative | Values Bytes, actual signature metadata | Request index2/Bytes |
| LegacyDateArithmeticTimeHeadNative | Values Bytes, actual signature metadata | Request index0/Time |
| LegacyDateArithmeticDurationHeadNative | Values Bytes, actual signature metadata | Request index0/Duration |
| LegacyDateArithmeticStepNative | Values Bytes2, whole SDK report plus actual nullable response | Valid union |
| LegacyDateArithmeticParseNative | Existing TemporalText, whole report, original fixed flags0, captured legacy zone | Null or Request index1 |

All successful outer results are present. Metadata contains actual date/interval kinds and subtraction, not a packed fake integer. Folded integer responses retain full i128 in16 little-endian bytes; reals retain their8 bytes; Decimal/Time/Duration use actual identity frames; byte responses remain raw. Whole computed reports are forwarded without native cursor reconstruction.

Terminal reports contain SDK-computed presence0 for NULL or1 for an actual value. Request reports have no presence. The three native APIs return value plus presence; bare predicates do not replace this with numeric truth conversion or host `is_some`.

Checked retained bound: `3*(first.len+second.len)+512+replyVisibleScale+savedDateVisibleScale`. Decimal scale is inspected only for a requested Decimal channel, or a saved Decimal date at Parse. Arbitrary bytes resembling a Decimal frame are not charged as one. Actual owned capacities and the Parse zone name have separate pre-invocation checks. This is not a physical/transient-memory or OOM guarantee.

## Original semantics retained

Text reads unit2 → date0 → parse with original zone → zero-date stop → interval1. Typed Time/Duration read date0 → unit2 → interval1; typed zero Time does not skip those later reads. Missing children retain channel-specific NULL behavior rather than a new fixed-arity rejection. Byte conversion remains lossy UTF8.

Only date integers narrow i128 to i64. Interval integers render the full i128. Decimal composite formatting retains the original internal negative-sign quirks, distinct from ordinary arithmetic. Date arithmetic applies nanoseconds first, then years/months/days, ignores original parsed truncation, and quietly maps original failures to NULL—no new1292/1441 or statement-policy getter. Text chooses FSP0/6 from microseconds; typed Time retains original kind/FSP; typed Duration retains checked arithmetic's maximum FSP.

Eight baseline-unimplemented Duration→Datetime signatures remain child-free NULL in value readers and zero in bare presence, even with no slots. Wrong readers also remain static refusals. No Shared PB kernel or specialized vector admission is added.

Head resource admission now precedes its requested original children. After admission, their original demand/error order remains. Callback errors preserve their original generic type. The outer scoped pack guard covers callbacks and late panics; requested child evaluators inherit selected capability, including one-shot NoColumns execution, without replacing their request context. This is scoped progress, not closure of all request-root/M6 obligations.

## Validation

[Commands/counts/hashes](../logs/legacy-date-arithmetic-summary.txt):12 actual locked single-threaded launches, **11 nonzero passing runs and one retained compile failure**. Initial TiKV compile emitted E0599/E0282: the new integer date path called `into_result()` on an already-Result parser return. Removing only that extra call fixed compilation; retry and all subsequent gates passed. No test-execution failure, zero-match run, skipped retry or oracle rewrite is counted as a pass.

Five new tests: shared datatype, SDK controller, direct worker lifecycle, native bridge, and real legacy consumer. Across changed files,205 TiKV and327 native old test bodies are unchanged.

The consumer test asserts all56 actual PB mappings become the expected legacy expression, not merely that decode succeeds. It exercises48 calculating signatures over value/presence/wrong-reader routes, normal/unit-NULL input and live/zero-slot scopes; the eight original refusals remain child-free. It checks request order, Timestamp/FSP preservation, zero Duration presence, full i128, negative Decimal formatting and missing children. Three unchanged legacy tests also pass.

The bridge/direct tests cover all five roots, actual-capacity and instruction refusal/reuse, selected scope, original callback errors, callback-panic poisoning and Parse-only zone demand/binding. The17 original CoreTime tests pass. Three unchanged SQL tests—including R105's52SELECT fixture—are rerun; **zero new SQL probes** are claimed this round.

## Still open

[Remaining acceptance](remaining-acceptance.md): CAST/M2, IN and six complex candidates, plus broader request-root/default-NoColumns/live-DAG/final acceptance. Generic MyDecimal primitives and baseline-unimplemented domains are explicit follow-ups. Full expression/unistore suites were not rerun; historical R1004+1 failures remain unrepaired. Workspace/lint/dev/bazel/release, exhaustive differential, performance, physical memory, allocator/OOM, TiFlash/FIPS, dual-tzdata, whole-Go-package transcreation and PR readiness are not verified. Overall goal remains active.
