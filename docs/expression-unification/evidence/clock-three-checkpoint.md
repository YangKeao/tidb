# NOW / CURDATE / SYSDATE checkpoint

`clock-three-62`, following `json-merge-pair-61`. Three whole frozen families add functional delegation/deletion credit: **201/245**, strict final-audited acceptance **0**. There are 44 eligible families left and 20 further migrations needed for 221. This is not overall completion, package transcreation or PR readiness.

## Ownership and execution

Eight exclusive owners changed 15 Rust files (TiKV7/native8), including one new `native_typed_clock.rs`, and added14 permanent tests. H owns clock leaves/exports; B typed SDK; G workers; C closed registration; D native bridge; E SQL preparation; A public-helper adapters; F SQL lifecycle tests. Parent owns contracts, Plan, formatting, serialized gates, guides and publication. No dependency/lock/Go/Bazel/physical transport changes. C's existing shared clock selector makes an official-guard edit unnecessary.

NOW and its CURRENT_TIMESTAMP/LOCALTIME/LOCALTIMESTAMP aliases, CURDATE/CURRENT_DATE, and SYSDATE use three fixed unit profiles: NowNative(BytesInt), CurrentDateNative(Bytes), SysdateNative(BytesInt), all OwnBytes. They reuse the existing16-byte UTC-seconds/raw-nanoseconds/offset frame, separate actual precision operand and shared width/FSP validators. No new driver, carrier, result kind, binding, cause, NoArgs profile, ordinary-wire/PB/legacy/parser admission or fabricated result input.

NOW truncates; ordinary live SYSDATE rounds original nanoseconds. The latter captures actual SystemTime after the original flag/arity/FSP/statement-clock demand, retaining the frozen statement offset. `sysdate_is_now=true` selects NOW through a nonexecuting preparation helper inside one guard, not nested drivers. CURDATE retains arity-before-clock demand. Frontends no longer add offsets, truncate, carry or format these answers. Existing formatter bodies are reused rather than copied; only an old test facade remains native.

## Distinct typed public SDK closure

Public `get_time_value` exposes raw CURRENT_TIMESTAMP/CURRENT_DATE helpers with different semantics from the SQL string workers. Its prelude, function-marker strings, non-sentinel branches and predicate body are byte-exact. `native_typed_clock_utc` validates UTC after FSP/one clock read and before the second session-zone getter. `native_typed_clock_fields<TZ>` uses the actual generic session zone, not the explicit parse zone or tuple offset, and owns local calendar projection and truncation. `native_typed_date_fields(raw_core)` reuses shared core projections and clears clock fields only after complete time construction succeeds.

Native Time/TimeType/SessionTimeZone are **not** wire aliases. The unchanged native `Time::from_date_checked` remains the final representation/bit-width codec, preserving raw year/microsecond domains, requested kind and original date-helper FSP. It is not replaced by a wire calendar validator. Date-kind hidden clock bits, chrono leap admission and microsecond overflow before date clearing are retained. The native graph has one chrono0.4.45; standalone TiKV uses0.4.43. Generic TimeZone avoids mixing the different chrono-tz versions.

This previously pure SDK stays pure; its real zero-slot success is not a worker-refusal proof. Actual SQL value paths independently prove the three guarded profiles.

## Validation receipts

[Exact commands, summaries and raw-log SHA256](../logs/clock-three-summary.txt); [manifest](../checkpoint.json).

Ten Cargo attempts/runs: seven green clock gates, two known full-suite RED runs, and one rejected exploratory CRC32 SQL candidate. No compile failure, interruption, zero-match or retry. Final permanent tests all pass; no clock production/test repair was needed after the first gate.

- TiKV clock13; local311 passed/1ignored.
- Native clock19; original typed-helper bank5; sysdate7.
- SQL new2 and original sysdate statement-clock1. Fourteen fixed value pins (eleven also pin metadata); thirteen direct zero-slot expressions, including three genuine live-SYSDATE OFF-mode calls. No live wall-clock equality oracle; fixed pure/SDK tests pin rounding and day carry.
- Full expression1536/4old/94ignored; unistore207/1old/13ignored. Entire failure sections and final lists equal the previous checkpoint after only numeric panic-thread-ID normalization, hashes `27654f2c0ad2971242c00f50182467598cdfd9c2c65e5bf5a32e683708448637` and `b64dcced405c699c11b72ee31888c58e491b4e5b4e74fa2d445454e1d1e146e9`.

All15 sources pass pinned rustfmt checks and both repository diff checks. Original helper/SQL tests are byte-exact after removing only new function blocks; old time tests are an exact prefix, old add_sub and CPP wire test sections unchanged. Constructors, timezone representation, function catalog, new_function, PB/unistore, bridge packer, compile/official guard and locks are unchanged. No expected values or fixtures were rerecorded.

## Rejected candidate and audit incidents

Before clock implementation, one temporary SQL test tried the existing quoted CRC32 spelling against five source-pinned column cases. It failed on the first SELECT with `Exec(Eval(FunctionNotExists("test.json_sum_crc32")))`: quoted lookup follows the UDF route. The temporary test alone was removed; its RED receipt remains. No CRC32 algorithm, registry or ARRAY admission changed, and no CRC32 credit is claimed.

Source review corrected the parent's initial SYSDATE-truncation assumption before implementation; its original live branch rounds. Native Time was verified not to be a CPP alias. Crossed typed-date ABI proposals were settled on actual raw-core input before compilation. Parent static checks initially guessed the wrong new_function path, assumed an inserted helper test was appended, then over-stripped the following existing doc comment; corrected path/function-end selectors prove original sources unchanged. These were audit-selector errors, not source repairs or test passes. Next-candidate RO also corrected guessed paths without changing files.

## Limits and next candidates

Prior JSON_KEYS aggregate Json-vs-String failure remains unresolved and was not rerun. Deep raw JSON decode precedence/performance, tree conversion/key-search cost, M6, broader default-NoColumns roots, whole workspace/lint/dev/bazel_prepare, release/performance/zero-copy/physical heap/peak/OOM, exhaustive differential/TiFlash/FIPS and prior parser/GB/ignored-vector/extreme-Decimal exceptions remain deferred. No measured performance-neutrality claim.

Read-only next map: DATE is smallest but needs policy-aware native/PB NULL handling and its distinct legacy raw-core predicate. TIME+MICROSECOND need the full original duration parser, compact-datetime fallback, formatting and raw microsecond domain, not merely existing fraction leaves. DATE_LITERAL/TIMESTAMP_LITERAL are distinct rewrite-time families requiring exact text-parser/FSP/error/DST-carry closure, not DATE/CAST aliases. CRC32 quoted SQL is now disproven, not a runnable candidate. ANY_VALUE/NAME_CONST still require all19 Datum representations. None receives advance credit.

Architecture-index and coprocessor guides describe these boundaries; no new repository policy is introduced. Publication is TiKV first, then TiDB with its exact paired SHA and common Plan hash. No force push/PR; the preexisting untracked client-differential BUILD.bazel stays excluded.
