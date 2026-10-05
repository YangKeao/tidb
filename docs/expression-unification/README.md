# Expression unification experiment

Paired branch: `expression-unification-demo` in YangKeao/tidb and YangKeao/tikv.
Current checkpoint: **duration-control-118**, after **json-source-117**.

Functional **238/245 (97.14%)**, strict **0**, remaining **7**—unchanged. This is partial CAST/M2 ownership, not whole temporal/CAST/M2, Go-package or new-family acceptance. Goal remains active.

## Shared ownership

- `native_duration_convert.rs` owns rounding, NumberToDuration, StrToDatetime/StrToDuration, text-to-duration and datatype duration-target selection.
- `native_cast_duration.rs` owns ordinary, argument and computed-duration controllers. Native supplies lazy zone data, applies the returned generic truncate effect and wraps raw storage.
- `native_text.rs::native_unquote_binary_json` is the single unquote selector for both the duration target and native BinaryJSON adapter; no duplicate selector remains.

Integer UTC fallback, negative-half rounding versus text parsing, JSON error/display boundaries, numeric NULL versus nonnumeric event values and computed double rendering are preserved. YEAR/DATE calendar/coercion controllers remain native; wire Duration policy is unchanged.

## Verification and incident

[Evidence](evidence/duration-control-checkpoint.md), [exact commands/counts/hashes](logs/duration-control-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

**20 launches:19 matched GREEN,1 matched RED.** Final ten filters passed: SDK2/1/2, native datatype472 plus four expression filters, and two SQL gates. Nine new tests;6 old SDK and276 old native test bodies unchanged.

The RED was a new test incorrectly expecting unknown JSON tag255 to panic. Source-derived expectations were corrected to empty display followed by a duration-format error; root +Inf separately pins the real panic. Production and old tests were unchanged. The RED, corrected pass, and final passes after unquote deduplication are retained separately.

New SQL covers8 SELECTs/36 cells (30 Duration,6 NULL), source modes, rounding, warnings, datetime fallbacks and real TIME(3) storage. Argument/computed and strict-effect cases remain direct-unit scope. No output-recorded oracle or performance/physical-memory claim.

## Remaining

[Remaining acceptance](evidence/remaining-acceptance.md): other CAST/YEAR/DATE, broader metadata/M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance.

Full expression/unistore were not rerun; historical4+1 failures remain. Workspace/lint/dev/bazel/release/exhaustive/TiFlash/FIPS/performance/physical memory/OOM/allocator/dual-tzdata/whole-Go-package/PR readiness remain unverified. Paired TiKV and three identical Plans are pinned; unrelated BUILD excluded. No force push or PR.
