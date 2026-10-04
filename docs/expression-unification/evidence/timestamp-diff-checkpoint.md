# TIMESTAMPDIFF: text and raw policies with shared temporal types

**timestamp-diff-91**, after **bounded-staleness-90**. Functional228/245, strict0, remaining17. All227 previous family objects are byte-identical; only `timestampdiff` is added. Type-helper sharing itself earns no extra family credit.

## Deleted native algorithms

- `time_fn/calendar.rs`: dedicated parsed struct/parser adapter and complete TIMESTAMPDIFF algorithm replaced by the scoped bridge; test-only NoColumns wrapper remains.
- `core_time.rs`: raw time-difference and timestamp-difference bodies delegate to TiKV. Interval/difference types are aliases of single shared definitions; original TimeDifference Debug name is preserved.
- `time_parse.rs`: named-helper unit lookup delegates to the shared nine-unit lookup, retaining ASCIIuppercase and original InvalidUnit error.
- `cophandler.rs`: manual legacy zero/unit/result selection removed; only original readers and result projection remain.

TiKV `time/native_timestamp_diff.rs` owns type algorithms and lookup. `impl_time.rs` provides two closed Values/Bytes3/1call/nullable-signed-Int profiles. Existing driver/carriers, fixed-Int budget/postflight and actual capacity accounting are reused. No existing identity/wire encoding, compiler or wire policy changes.

## Deliberately different semantics

| Domain | Retained source policy |
|---|---|
| Ordinary/typed | Evaluate all children, perform both original datetime casts, then fully coerce unit/left/right in order. NULL cannot suppress later coercion errors. Time is displayed before strict parsing, so visible FSP—not hidden raw microseconds—governs the value. |
| Shared PB | Existing TimestampDiff admission has no ordinary datetime wrappers. First actual NULL returns through the existing NULL witness before coercing the prefix, demanding the suffix or checking nonNULL arity. NonNULL inputs use Text with original context. |
| Text worker | Actual nullable UTF8 inputs; all present UTF8 checked even with another NULL. Strict parse, wide-u32 written years, civil-day/i64-month arithmetic, ASCIIuppercase without trim. Unknown-unit0 occurs after parsing and delta computation. |
| Manual legacy/Core | Missing/NULL unit stops before endpoints; otherwise both typed readers run even with left NULL. Actual optional8LE cores, full64bit-zero NULL, exact uppercase byte units (no UTF8 gate). Unknown-unit0 precedes arithmetic. |
| Datatype raw math | Original i32 daynr/sign expression, i64 microseconds, u32 years/months and u8 subtraction remain, including unchecked/panic behavior. Named helper still errors on unknown units. |
| Existing TiKV wire | Constant unit metadata, its invalid-time/error rules and existing packed-time arithmetic are unchanged; not an alias of either native profile. |

Concrete differences are pinned: year0 Jan1→Mar1 is60days Text versus59 Core; hidden fractional raw bits can produce a Core difference while formatted Text produces0; raw lowbits-only cores are not full-zero NULL. No chrono conversion, year9999 narrowing, calendar repair or i128 overflow fix is introduced.

Real native PB conversion is catalog-first Shared. Residual manual `SimpleSig::TimestampDiff` is tested as its own existing domain, not misrepresented as that wire route. Ordinary cast warnings/modes/timezone remain upstream; Core typed-reader SQL-error folding and infrastructure propagation remain unchanged.

## Validation and evidence boundaries

[Exact commands and hashes](../logs/timestamp-diff-summary.txt); [manifest](../checkpoint.json).

Nine new tests pass on first matching execution:2CPP datatype/kernel,2local shape/budget,2native datatype/entry,1bridge,1legacy and1SQL. Original source tests also run. Capacity tests refuse oversized actual unit/core storage before invocation and then reuse the healthy worker; no physical allocation/peak claim.

SQL34SELECT=8cases×2vector×2pool=32direct+2positive filters.16zero-slot probes isolate the new Text root, including4NULL cases—not the separate PB NULL witness. Stored Time/NULL datetime casts are no-ops, so no prior parser/Compare is substituted for root evidence. SQL pins signedLongLong20/0/binary headers, DAY±2, a full-month boundary short by one microsecond, negative subsecond SECOND→0, MICROSECOND530865, leap2000 and NULLs. Dedicated SQL grammar rejects unknown units; only direct tests exercise that existing internal domain. DATETIME6 exposes its microseconds; hidden-FSP differences are direct-test evidence, not invented storage behavior.

Ten locked single-threaded nonzero launches:8green,2only-old full RED. CPP datatype1/time79/local345+1ignored; native datatype91/root4/gateway196+1ignored/legacy2/SQL1 pass. Full expression1602/4old/94ignored and unistore220/1old/13ignored remain RED. No new failure, compile failure, oracle correction, zero-match, interruption or fixture recording. Counts overlap.

Whole failure sections match R93 after only numeric panic-heading thread IDs are normalized: expression `411274feba11e202735df5a8056c72babbc8cf9df2eab3258c670e9289a1ee95`, unistore `2f19c9ad5338a86c48b895e3923c3e39454117e3410d47df5aa3fb4254ed7ce1`.

Source audit:9CPP/12native Rust files,2new modules;260CPP/542native original test bodies byte-identical,4CPP/5native new tests. Pinned formatting/diff checks pass. Independent H review found no blocker in arithmetic widths/order, aliases/Debug, Text/raw distinctions or caller demand. Agent-doc changes are descriptive, not policy; no Go/Bazel/Cargo/lock/generated or `compile.rs` changes. Unrelated untracked BUILD excluded.

## Remaining acceptance

[Remaining review](remaining-acceptance.md):5core,6ordinary pending,6complex candidates. Actual request-root/default-NoColumns/live-DAG owner and final cross-entry closure remain. EXTRACT's mixed selector, INTERVAL lazy cursor/NaN differences and GREATEST/LEAST five domains were scoped, not claimed migrated. IN observation-count authority remains unresolved without hiding calls or changing old tests.

Known CAST/Decimal/mode/INTDIV/JSON/vector/older Values gaps and old failures remain. Workspace/lint/dev/bazel_prepare/release, exhaustive differential/TiFlash/FIPS, performance/physical memory/OOM/allocator/zero-copy/dual-tzdata and complete Go-package transcreation are unverified. Overall goal active; no PR-readiness claim.
