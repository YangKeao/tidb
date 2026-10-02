# Four statement-clock families — clock-four-60

Previous `json-unquote-59`. **196/245 functional (80%)**, strict final acceptance0; target221,25 more needed,49 eligible remain. Whole additions: **curtime/current_time, utc_time, utc_date, utc_timestamp**. NOW/CURDATE/SYSDATE remain uncredited. [Commands and hashes](../logs/clock-four-summary.txt).

## Ownership and implementation

Six exclusive writers;13 Rust files (CPP7/native6),1 new source,11 additive tests. A owns new CPP `native_clock.rs` and root exports; G fixed workers in `impl_time.rs`; C local batch/registry and function/official-expression boundaries; D native mappings/checked bridge/SDK tests; E `time_fn/{mod,tests}.rs`; F session lifecycle tests. Parent owns formatting, serialized gates, docs, Plan and publication. No dependencies, manifests, locks, Go or Bazel changes.

The unique pure core moves all six native epoch-formatting bodies and reuses `Time::native_civil_from_days`. It does not convert to chrono or bitpacked Time, which would narrow the wide signed-epoch/string domain. Six fixed entrypoints consume actual `NativeClockInput { utc_secs:i64, nanos:u32, tz_offset:i32 }`. Only two general formatter exports remain as native aliases for unmigrated NOW/CURDATE/SYSDATE consumers; those entry bodies/public helpers are not claimed complete.

Seven unit OwnBytes profiles reuse existing roles:

- UtcDateNative, CurrentTimeWithoutFspNative, UtcTimeWithoutFspNative: Bytes1.
- UtcTimestampNative, CurrentTimeWithFspNative, UtcTimeWithFspNative: BytesInt, two actual columns.
- UtcTimeNullNative: genuine NullWitness(None), one nullable Int column, **not NoArgs**.

The byte operand is exactly16 LE bytes: UTC seconds8, raw nanoseconds4, session offset4. Precision is a separate actual parsed Int0..6. It is not an action/rounding mode or an encoded answer. The checked native packer only allocates/copies representation; the frontend does no offset arithmetic, truncation, carry, date calculation or formatting. Computed bytes become String, preserving existing typed Time/Duration parsing and metadata.

Both closed boundaries reuse G's width/FSP validators. C required one explicitly granted additional `types/expr_eval.rs` lease for the official guard and NULL whitelist; no new driver/kind/carrier/binding or compile/NoArgs change. Raw seconds/nanos/offset retain their full integer domains: unused fields do not acquire validation errors. Ordinary wire and native PB/legacy admission remain unchanged.

## Exact policies

Arity/FSP validation remains before the single original `Columns::now()` call, now inside the existing preparation guard. Error names, UInt-to-i64 diagnostic wrap, unsupported arguments and missing-clock text remain. Native value-entry UTC_TIME(NULL) skips the clock but executes its own NULL worker.

CURTIME applies the captured offset; UTC variants ignore it. Zero-argument time truncates. Explicit time precision, including zero or CURTIME's coerced NULL precision, first truncates submicroseconds and then rounds half-up. UTC_TIMESTAMP instead rounds original nanoseconds directly; its default/NULL precision is zero. Euclidean negative-epoch/day handling, midnight carry, raw noncanonical fraction behavior and ordinary arithmetic overflow behavior remain unchanged. No new production panic catcher; resource refusal may preempt reaching a kernel panic. New SDK tests focus on representation/precision/refusal, not a new scoped overflow-panic experiment. The unchanged-driver prior panic test runs again in the full expression suite.

## Verification and corrections

Ten actual Cargo runs: **7 green,1 initial new SQL RED,2 known full-suite RED**. Zero compile failures or zero-match attempts. Final eleven added tests all pass; all thirteen sources pass pinned rustfmt checks. Full suites ran after the final production edit and before a SQL-test-only correction; production code did not change afterward.

Green: CPP pure/fixed clocks4, local307+1ignored; native clock14, original submicrosecond rounding1; corrected SQL2, original typed-clock SQL1 and original SYSDATE-is-NOW consumer SQL1. Final SQL probes: ten initial fixed outputs plus five after a day/offset change, five metadata checks, **11 direct zero-slot statements covering six SQL value profiles**. A seventh NULL profile is independently verified at native value-entry/SDK/C4, not by unadmitted SQL.

**Initial SQL RED0/2 was a new test admission mistake:** `UTC_TIME(NULL)` is rejected by existing precision grammar before evaluation. `tidb-parser/src/expr/func.rs:872-898` explicitly requires an IntLit; the running parser reported the same rejection. F removed this unadmitted spelling from positive/worker probes, adjusted only its associated output/indexes, and added an explicit1064 non-evaluation assertion. All other fixed literals and old tests remain unchanged. The parser was not expanded, production was not repaired, and the initial failure log is retained. The corrected SQL run passes2/0. The discarded five-profile optional-frame design was never implemented; final protocol is the seven-profile contract above.

Full expression **1526/4old/94ignored** and unistore **206/1old/13ignored**, exit101. Complete failure sections/lists match `json-unquote-59` after only numeric panic-thread normalization. The previous checkpoint's extra aggregate JSON_KEYS type mismatch remains unresolved and **was not rerun or relabeled passing**.

Static audit confirms old native clock tests/inline block and CPP wire test block unchanged; NOW/CURDATE/FSP parser bodies exact; SYSDATE/public helpers/PB/legacy/typed/vector/admission sources unchanged; all six formatter bodies deleted. One parent proof script assumed `mod tests` instead of actual `mod clock_source_tests`, failed before that assertion and passed after selector correction. Four source lookups used nonexistent paths (parent1, read-only scouts3); glob/actual source corrected them. F's unavailable skill-catalog lookup was resolved by reading the checked-in guideline. These are disclosed tooling incidents, not compile failures.

## Limits and next candidates

StrictM6, broader default-NoColumns request-root integration, workspace/lint/dev/bazel_prepare, release/performance/zero-copy/physical heap/peak/OOM and exhaustive context/domain/wire/differential/TiFlash/FIPS remain deferred, along with prior parser/GB/vector/extreme Decimal exceptions and JSON_KEYS integration mismatch. Writer/transport allocation costs are not measured. No whole Go-package transcreation, PR readiness or overall completion claim.

Actual frozen-baseline subtraction confirms49 eligible remaining; an earlier narrative omitted six candidates, not the authoritative ledger. Read-only next choices: MERGE/PATCH must close distinct serde nullable-document, raw SDK and lazy legacy policies; ANY_VALUE/NAME_CONST need exact19-kind identity using returned payload, not a discarded worker receipt; CRC32 must preserve number spelling and unadmitted SQL ARRAY syntax. JSON_SEARCH has distinct SDK/SQL wildcard and eager-walk behavior; SCHEMA includes external refs/cache and cannot earn local-only credit. None is advanced by this checkpoint.
