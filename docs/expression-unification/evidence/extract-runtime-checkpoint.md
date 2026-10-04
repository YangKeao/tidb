# EXTRACT runtime closure

**extract-runtime-98 / R101**, after **extract-types-97**. Functional **232/245 (94.69%)**, strict0, remaining13. Only `extract` is added; all231 previous family objects and all prior partial/type records are unchanged.

## Shared ownership

The prerequisite [duration/extraction datatype services](extract-types-checkpoint.md) now support six runtime profiles in SDK `native_extract.rs`:

| Profile | Actual inputs / role |
|---|---|
| Select | unit, optional lossless source FieldTypeCode, actual DatumKind / Values |
| Datetime | raw core and unit / existing TimeCoreBitsBytes |
| Duration | unit and raw nanoseconds / Values |
| MixedDuration | unit, coerced text, first actual allow-invalid flag / Values |
| MixedFinish | whole SDK continuation report, second actual flag / Values |
| Composite | unit and nullable coerced text / Values |

All return owned byte reports through one-call recipes. No new carrier, compile/factory limit, PB/legacy admission or specialized vector kernel is added. Datetime retains its original dedicated carrier role rather than weakening role validation.

`time_fn/extract.rs` is a thin bridge; its native selector, mixed parsing and clock comparison are deleted. Calendar composite dispatch is also thin and its three exclusive parser/formula helpers are deleted. Its clock helper remains for four other production callers. Native `tikv/extract.rs` executes SDK-requested casts with the **complete original FieldType**, preserves original context getters/warnings, and projects SDK-computed reports. It does not reconstruct mixed state or calculate extraction results.

## Source semantics retained

Selection uses actual type metadata, not host-computed domain booleans. Unknown(12) is distinct from named Datetime. Time with a clock-only unit still uses the original duration cast. Unknown units cast first: NULL or failed casts can suppress the later1105 error.

Typed children stay eager; original unit NULL and lossy UTF8 handling remain. Actual cast/unit NULL uses the existing real NULL witness. Mixed parsing reads the first mode only after string preparation. Parse errors, truncation or overflow return original hard1292 before any second getter. Only an SDK continuation requests the second fresh mode value. Fixed UTC, positive-year and H/M/S equality determine the datetime choice; microseconds are not compared. Whole reports carry SDK-produced duration state plus original unit/text.

Calendar compatibility remains separate: broad u32 hours and civil years, right-aligned one/two/three groups, full fraction validation then truncation, bad clock suffix to zero, unknown unit to0, and original fixed warning on bad date. Both original formula tables use ASCII uppercase. Neither the narrow duration parser nor packed Time substitutes for this domain; original ordinary i64 overflow behavior remains.

The checked reply bound is actual byte lengths plus64, charged before producers along with all actual input capacities, then checked against fresh output capacity. Composite NULL may be conservatively precharged but returns actual None. Parser temporaries, physical heap/peak/OOM and allocator behavior are not certified.

## Validation

[Exact commands/counts/hashes](../logs/extract-runtime-summary.txt), [manifest](../checkpoint.json).

Six locked single-threaded launches pass: CPP core1/local351+1ignored; native entry13/gateway196+1ignored; new SQL1/original SQL1. Five new tests pass first execution. The entry gate also runs11 original tests. All137CPP/415native original test bodies in touched files are byte-identical; no fixture or provider-oracle recording.

Direct tests isolate all six workers and their actual output/error/NULL policies. Wrong roles, malformed shapes/flags, work-budget zero and128Ki actual capacity against64Ki budget refuse before invocation; healthy reuse follows. Mixed finish receives a real worker-produced state. Native tests preserve differing mode values, original metadata and getter demand, real NULL witnesses, second-mode/warning panic quarantine and one-slot continuation.

The new SQL test has **28 SELECTs** across both vector settings and pool1/0. Twelve positive-pool successful rows include two NULLs; two positive-pool malformed inputs retain hard1292/22007 and one session error-row warning. Fourteen zero-slot probes isolate the new Select root using stored columns without nested CAST, including NULL and bad text. These do not claim isolated SQL refusal proof for later finish profiles. Materialized values, signed LongLong20/0 and binary metadata are pinned. The original32-SELECT interval/EXTRACT/TIMESTAMPDIFF test also passes.

Full expression/unistore were not rerun; R100's4+1 failures remain historical evidence, not current receipts. Static contract corrections occurred before builds: Datetime kept its dedicated role, and a tentative parent case-sensitive reminder was rejected after verifying both original tables uppercase. No failed Cargo launch or source behavior change resulted.

Seven CPP/eight native Rust files, two new modules. Formatting/diff/receipt checks pass; focused production-source review found no blocker. Guides remain descriptive. No Cargo/lock/Go/Bazel change; unrelated BUILD excluded.

## Remaining work

[Acceptance](remaining-acceptance.md): five core, two ordinary and six complex candidates—not blanket exceptions. Request-root/default-NoColumns/liveDAG/final acceptance and earlier compatibility gaps remain. Workspace/lint/dev/bazel/release/exhaustive/TiFlash/FIPS/performance/physical memory/OOM/allocator/dual-tzdata/whole-Go-package/PR-readiness are not certified. Overall goal remains active.
