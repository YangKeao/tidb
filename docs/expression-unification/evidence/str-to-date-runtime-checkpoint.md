# Ordinary STR_TO_DATE through staged SDK workers

**str-to-date-runtime-95**, after **str-to-date-types-94**. Functional230/245, strict0, remaining15. All229 prior family objects and previous partial CAST/type records are byte-identical; only `str_to_date` is added.

## Ownership and demand

SDK query_expr `native_str_to_date.rs` owns ordinary token scanning, meridiem fixing, validation, rendering, warning classification/messages and typed DATETIME's late zero-prefix decision. Native `calendar.rs` no longer contains ParsedDateTime, inner parser, month-zero sentinel or six private parsing helpers. Scalar native contains/prefix logic is removed. Public datatype parsing/classification/punctuation was shared in R97; its broader grammar is not substituted for this ordinary policy, nor is wire parsing changed.

Three closed Values/OwnBytes profiles, one call each:

| Stage | Actual inputs | Outcome |
|---|---|---|
| Head | BytesBytesInt: coerced input, format, nullable declared-type enum | Value, warning, NeedDateModes or NeedTypedDateMode |
| Date Finish | New BytesIntInt: whole state report, no_zero_date, allow_invalid_dates | Value or warning |
| Typed Finish | BytesInt: whole typed report, no_zero_date | Prefixed value or silent NULL |

Head supports genuine nullable SDK inputs; native observed caller NULL instead uses the existing real witness before coercing either ready operand. Both children have already been evaluated. No placeholder is synthesized for an uncoerced operand.

State carries year i64, day u32 and other original fields plus original input text; day999 must not truncate to a packed date. Getters occur only on corresponding Need reports: original date-mode read after successful scanning even for month0, and a separate late typed-mode read only for time-only DATETIME results. SDK renders1292/1411 messages; native calls original append_warning without strict truncation escalation.

Declared-type metadata uses a lossless enum codec: named variants' mysql_type0..255, Unknown(byte) as -1-byte, None as undeclared. `FieldType::new(Unknown(12)).code()` really preserves that Unknown variant, so bare mysql_type would incorrectly treat it as Datetime(+12). This is actual metadata, not an origin or computed-result flag.

Generic temporal CAST finishing remains unchanged, not claimed as migrated by this family. Duration(NULL) still reads timezone; Date/Datetime(NULL) does not add a getter or worker. Public/wire grammar, native PB and legacy admission remain unchanged.

## Resource and lifecycle boundary

Whole tag3/tag4 reports move into continuations without stripping/repacking. Strict result-domain checks reject DateFinish NULL/continuations and TypedFinish warnings/continuations. Uniform checked first.len+64 precharges retained output before producers; actual input and report capacities remain checked. Fresh report reservation does not claim parser Vec<char>/format temporary, physical heap/peak/OOM or allocator guarantees.

Generic1..3 arity and default native factory4nodes suffice; no compile widening. Direct CPP tests isolate each stage with max_steps0, distinct from frontend pool0, and128Ki actual spare capacity versus64Ki caps with preinvoke refusal and healthy reuse. Native tests cover late-mode/warning panic poisoning, original context reads, coercion/child/NULL order and Unknown metadata.

## Receipts and correction

[Exact commands/hashes](../logs/str-to-date-runtime-summary.txt); [manifest](../checkpoint.json).

Ten locked single-threaded test launches:7green,2only-old full REDs and1new-test RED subsequently corrected. The new intermediate-state assertion mistakenly expected Head's NeedTypedDateMode to already contain `0000-00-00 `. The frozen contract and original scalar_function2995 add that prefix only after the late getter. Only this new expectation was changed to `20:23:10`; the separate TypedFinish prefix assertion remains. No production or original test/oracle changed, no provider output recorded. Six new tests finally pass, but not all first-run green.

Latest gates: CPPcore1/local349+1ignored; native entry1/bridge1/gateway196+1ignored/newSQL1/oldSQL1 pass. Full expression1606/4old/94ignored and unistore220/1old/13ignored remain RED. Whole failure sections match R96 after only numeric panic-heading thread IDs become THREAD (`411274feba11e202735df5a8056c72babbc8cf9df2eab3258c670e9289a1ee95` / `2f19c9ad5338a86c48b895e3923c3e39454117e3410d47df5aa3fb4254ed7ce1`). Known relaxed `%d` input01 month0 still yields NULL/1411 rather than the old test's expected date; no silent repair.

New SQL34SELECT=8storedcases×2vector×pool1/0 plus2positive Datefilters. Fourteen new nonNULLHead refusals include parser/typed-mode computed NULL; two old actualNULL-witness refusals are separate. Stored VARCHAR/static format handling has no earlier worker. DATE10/0, Duration17/6, DATETIME26/6, complete Unicode-punctuation date,1292/1411 warnings and dynamic-format default NULL/relaxed prefix are pinned. Positive filter/generic CAST refusals are not borrowed as isolated Finish evidence. The original classifier test adds4SELECTs.

Seven CPP/seven native Rust files, two new modules, three new tests per repository.134CPP/411native old test bodies unchanged. Pinned formatting/diff and receipt checks pass; independent E source review found no blocker. No Cargo/lock/Go/Bazel changes; guides descriptive and unrelated BUILD excluded.

## Remaining acceptance

[Review](remaining-acceptance.md):5core,4ordinary and6complex candidates remain, together with real request-root/default-NoColumns/liveDAG/final acceptance. Generic CAST/static metadata boundaries and known gaps stay explicit.

Workspace/lint/dev/bazel/release/exhaustive/TiFlash/FIPS/performance/physicalheap/OOM/allocator/dual-tzdata/complete Go-package/PR-readiness are not claimed. The overall goal remains active.
