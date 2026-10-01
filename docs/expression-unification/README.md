# Expression unification experiment

Checkpoint-ID: `format-time-three-40` (previous: `construct-time-four-39`)
**131/245 frozen families delegate with native evaluator algorithms removed; target 221.** Added: DATE_FORMAT, TIME_FORMAT and LAST_DAY. Strict final-audited acceptance remains **0**; incomplete and not PR-ready. DATE and MICROSECOND are not credited.
Paired branches: [TiDB](https://github.com/YangKeao/tidb/tree/expression-unification-demo) · [TiKV](https://github.com/YangKeao/tikv/tree/expression-unification-demo). Parent owns publication pins, Plan mirrors and paired pushes; no force-push or automatic PR.

## This checkpoint
- **Scope:** 22 Rust files (TiKV 9/native 13), including parent's two-line session_tz import/call correction. No manifest/lockfile changes or metadata commands. FROM_UNIXTIME shares the formatting helper with the real context but its other algorithms are not migrated and it gains no family credit.
- **Shared formatting:** one scanner/specifier match with five distinct views (Text/Core/Wire/Clock/RawDuration), not one substituted policy. Native evaluator and public Time/Duration loops are deleted; weekday/ordinal/microsecond getters thin-delegate without MICROSECOND credit. Seven new public methods comprise six Time methods and one const Duration method. Necessary wire changes preserve wire policy; some write! sites now allocate temporary format! strings, an unmeasured cost, not a zero-cost claim.
- **Demand:** DATE_FORMAT retains both ordered text coercions, bad-clock-to-midnight text policy and valid empty masks; LAST_DAY retains strict suffix validation. Original SQL casts can still produce 8034/1292. TIME_FORMAT uses two complete sequential calls: successful duration parsing returns actual owned source text, then mask coercion occurs before the formatting worker. No host parser or recursive callback; first admission can precede mask coercion, and double parsing/leases/owned-text transport/two one-shot workers without capability are explicit costs.
- **Closed protocol:** seven operations, six ordinary OwnBytes results and one existing Int result. Sole new role TimeCoreBitsBytes carries NONNULL LE8 raw core and the actual nullable layout. Existing NullWitness carries an observed NULL without demanding layout; genuine missing-first legacy bool uses closed NoArgs and the worker's zero result. Existing JsonOther/Pi NoArgs cases remain intact, not a general whitelist opening. Existing bool-with-first uses only IsNotNull's documented presence marker and never reads the mask. No new result kind, report, cause, driver or native PB/legacy signature admission.
- **Closure:** native SQL/typed/PB NULL and arity boundaries, legacy bytes and distinct presence-bool paths, public formatting helpers and FROM_UNIXTIME's formatting consumer are included. DATE's mode/projection rules and MICROSECOND's strict tidb_datatype::parse_time dependency remain deferred; the broad duration parser is not a substitute.

## Actual validation
| Receipt (`format-time-` prefix) | Result | Compile / run seconds |
|---|---|---|
| TiKV Time | 54 passed, 361 filtered | 3.32 / 0.01 |
| TiKV Duration | 21 passed, 394 filtered | 0.12 / 0.00 |
| Native CoreTime | 15 passed, 423 filtered | 3.32 / 0.00 |
| Native Time | 33 passed, 405 filtered | 0.09 / 0.00 |
| native-duration.log | **0 matched, 438 filtered; exit 0; not green coverage** | 0.09 / 0.00 |
| native-duration-tests.log | 16 passed, 422 filtered | 0.09 / 0.00 |
| TiKV local evaluator | 269 passed, 1 existing ignored, 502 filtered | 12.00 / 0.18 |
| TiKV time kernels | 68 passed, 704 filtered | 0.12 / 0.01 |
| dispatch.log | **Compilation failure E0271/E0308; no tests; exit 101** | — |
| dispatch-retry.log | **2 passed, 1 new test RED, 1565 filtered; exit 101** | 7.50 / 0.00 |
| dispatch-corrected.log | 3 passed, 1565 filtered | 3.24 / 0.00 |
| sql.log | **Compilation failure E0432; no tests; exit 101** | — |
| sql-retry.log | 86 passed, 2078 filtered | 20.32 / 1.32 |
| Legacy | 2 passed, 207 filtered | 8.56 / 0.00 |
| Full expression | **1470 passed, 4 old failures, 94 ignored; 1568 total; exit 101** | 2.95 / 10.42 |
| Full unistore | **195 passed, 1 old failure, 13 ignored; 209 total; exit 101** | 0.13 / 3.00 |
**16 Cargo attempts; 13 runs actually executed tests = 10 green + 1 new test RED + 2 current old full-suite failures.** The other attempts are two compilation failures and one zero-match run. Three failed-target retry commands (dispatch twice, SQL once); launch failures 0. Overall checks did **not** all pass first try.
Parent fixed TIME_FORMAT's use of the Datum-only evaluate_bytes_in packer by using evaluate_args_in with Args::Bytes, without changing demand. The SQL compile failure exposed session_tz's stale cfg(test) date_format import; its two-line correction imports date_format_in and passes the real cols. Neither failure is hidden as a launch issue.
The new dispatch test wrongly treated `not-a-time` as invalid: the unchanged broad no-colon/no-leading-digit parser returns Some(0, empty), so mask coercion is demanded. Parent changed that new invalid-input case to `900:00:00` and retained `not-a-time` as a UTF-8-mask-error case. Parser body is byte-identical to 2a8e; original fixtures/expected values were not changed and new expectations were not generated from the provider.
**Failure mapping is required for expression:** current thread-ID-only SHA `25f964887541f45602363f00dc60df4f0af21c74edccc16c38d0804094fb9bff` is NOT baseline `80be9bda05e5bf630e9c246ca9523d23f82c122eaaeec6e220b1b00cec436615`. Only mapping the panic header duration.rs:164:55 → :212:55 makes the complete section equal to construct-time; parent proved to_number's body unchanged and formatter deletion explains the shift. Unistore needs no mapping: thread-only SHA `e285bfdba646f2d01f85b485ae317cc07c51b78cf0ae6d664c0fb2bc39259759`, source line 194, matches construct-time.
All 22 pinned formatter checks and both diff checks passed; formatter/static-proof check failures 0. Non-test incidents: parent guide-read offset 760 exceeded 136 lines; A's guessed datatype/time.rs path failed once; D found two unit NullWitness constructors and B corrected them to NullWitness(None) before compilation (one source-interface correction, not another compile failure). Two oversized presentation requests were rejected then successfully split; no file/source/test impact. The prior inverse-civil indentation proof correction is historical only.
Three new SQL tests include 16 actual zero-slot Resource cases with original warnings preserved; two legacy tests cover bytes/presence and real/time consumer conversions with Resource/PoolClosed. Three dispatch tests cover typed/AST/PB NULL, arity and resources. Hand-derived raw year 10000/hour 24/microsecond 1048575, negative raw 24-hour versus SQL text, core-zero %M NULL/literal trailing-percent behavior and missing-first zero are new policy checks, not old fixtures or provider oracles.
Historical parser-all three-E0061 failure remains unresolved and was not rerun; isolated auth_shared's 20-pass receipt is historical. Exact commands: [summary](logs/format-time-summary.txt), [evidence](evidence/format-time-checkpoint.md), `checkpoint.json`.
## Remaining work
DATE, strict MICROSECOND, JSON_LENGTH and later TIMEDIFF/TIMESTAMPDIFF are uncredited candidates. Legacy capability propagation, operation scopes, allocation/peak, paired differential reruns, release/profile gates, old duration-panic adaptation, parser all, whole workspace, make lint, M6 and TiFlash remain unfinished; no whole-package/type, performance or OOM-safety completion. JSON_PRETTY retains its separate locks.
