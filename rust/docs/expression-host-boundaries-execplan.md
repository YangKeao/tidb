# Share native host boundaries: completed experiment, candidate withdrawn

This ExecPlan follows PLANS.md. Keep its Progress, Surprises & Discoveries, Decision Log and Outcomes & Retrospective current. Human message6129cd47 in #tidb-expression-reuse:5841a0b2 requests simpler row and batch integration without performance loss. Task14 tested this bounded simplification; the no-loss condition was not established, so all nine source files were restored.

## Purpose / Big Picture


Make repeated native host declaration and coordinate rules easier to review without adding an execution engine or changing SQL demand order. The experiment shared signature checks and window preparation, then removed eight consecutive unused authenticated reads already performed by the following shared read. It compiled and passed scoped behavior checks, but did not meet the performance condition. The observable final result is an unchanged retained implementation, a reproducible rejected patch, and complete performance evidence.

## Progress


- [x] Preserve Main93ae4ceb, SDKf7672bf, all9521 source/config inputs, original files and protected BUILD hash.
- [x] Identify semantic distinctions between row NULL short-circuit, repeated child demands and bulk source/cast phases; keep those distinctions.
- [x] Share complete signature matching at six entries and window validation/copy/install at five entries; boundary-only net29 production lines removed, no tests changed.
- [x] Remove eight adjacent unused get calls immediately before same-argument clone_for_kernel; combined production92 added/153 deleted, net61; tests unchanged.
- [x] Complete combined release unit/service builds, same2440 test names, all973 selected consumers972pass/1 exact inherited failure, root lint, pinned Jan30/Aug22 formatting and diff checks.
- [x] Receive independent read-only review of the eight deletions and whole common-entry/functionality evidence; preserve the functional snapshot separately.
- [x] Resume November9 after re-reading latest directives and checking actual load; preserve two failed quiet checks, then launch on third passing observation.
- [x] Run strict45 three-version preflight and full180-cell balanced SQL measurement with171672 successful complete queries; recompute/audit all data and clean only owned services/databases.
- [x] Reject combined candidate, preserve patch and actual binaries, restore all nine files and verify all9521 source/config inputs equal the pretrial retained version.
- [x] Prepare final performance report and final retained code counts. Deliver before documentation-only local DCO commit; record its receipt separately. No push.

## Surprises & Discoveries


The code reduction did not imply lower runtime cost. Deep16 expression CPU increased3.61% by median paired ratio, with all four rounds increasing2.91%–5.19%. Text CPU increased1.75%, also all four rounds. Decimal CPU fell1.68% in all four rounds. Scan control CPU increased1.62%, so the observations do not prove that the source change caused every difference. Four observed ratios are not a confidence interval. The no-loss condition was not established, which is sufficient to reject conservatively.

Initial strict SQL preflight differed only in12 estimated-row values2730.67 versus3333.33. Before any timing, all45 observations and a column-by-column proof were archived. Only owned storage was restarted, and the exact same strict preflight passed45/45; no EXPLAIN normalization or comparison relaxation. The transient's exact cause is unproven.

The first extraction script mistook a test-only retained_bytes method for the typed-window entry boundary. Its partial script/diff was saved, affected owned files restored and the script corrected before any build. The first final restoration assertion compared entire git status to the pretrial snapshot and failed solely because this new untracked ExecPlan existed. All9521 source/config inputs already matched; the corrected proof explicitly permits only that new document in addition to the protected preexisting BUILD file. Neither incident changes test or measurement provenance.

The checked-Decimal cast installs the original task arity2 even when its invocation has one argument. The candidate preserved this behavior. Diagnostic row/window progression was already shared by begin_math_diagnostic_scope/resume_math_diagnostic_value; it was not a new simplification. Normal warning-free SQL supplies no evidence of overflow diagnostic execution and cannot be explained by that path.

## Decision Log


- Decision: Prototype private NativeBatchHosts helpers using a statically specialized FnOnce demand builder.
  Rationale: Share complete signature checks and existing coordinate copy/install sequence without fields, a vtable, new per-row state or new evaluator.
  Date/Author:2026-11-06 / ExpressionReuseEngineer.

- Decision: Preserve family metadata checks, native_values checks, row NULL behavior, child demand replay and full bulk source/cast phases in their original callers.
  Rationale: Combining them could change error precedence, short-circuiting and warnings. Share only rules proven independent of those differences.
  Date/Author:2026-11-06 / ExpressionReuseEngineer.

- Decision: Remove only eight precisely adjacent unused get calls, not all repeated-looking authentication.
  Rationale: Existing clone_for_kernel first performs the same entry(handle,role,origin) check and StoredValue::Sql requirement before copying. Identical bare arguments and no intervening callbacks, allocations or other operations make these checks redundant. Whole-window, preallocation and postcallback checks remain.
  Date/Author:2026-11-06 / ExpressionReuseEngineer.

- Decision: Complete code/functional work during the declared shared-load period; defer reliable performance acceptance only.
  Rationale: Human51565ebb at2026-11-06 03:17:13UTC warned of daytime development until evening. Reminder6ab2b189 was anchored to that message. On November9 this interval had elapsed; latest directives and actual machine load were checked again rather than preserving an expired clock-based hold. Two actual16-second observations failed; the same reminder was snoozed15min. Third observation passed before launch.
  Date/Author:2026-11-06 and2026-11-09 / ExpressionReuseEngineer; propagated to TiDBEngineer.

- Decision: Reject the whole combined candidate and restore all nine original-HEAD backups.
  Rationale: Deep/text CPU increases in all four rounds prevent no-loss acceptance. Decimal improvement does not offset that requirement. Scan changes and limited external-activity monitoring prevent a causal claim about every difference. The boundary-only intermediate was never accepted and must not be left behind as an invented baseline.
  Date/Author:2026-11-09 / ExpressionReuseEngineer.

## Outcomes & Retrospective


Final retained Rust changes for this task are0 added/0 deleted production and tests. The trial61-line reduction is not retained. Cumulative actual retained reductions since Demo-policy removal remain production2077 added/7143 deleted, net5066; tests5492 added/5684 deleted, net192. These gross figures sum sequential retained changes, so later edits to the same line may contribute again; net deltas telescope. Documentation, config, JSON/TSV observations, earlier test-evaluator retirement and rejected trials are excluded. The earlier hypothetical5127 production reduction was not achieved.

The experiment completed all180 cells and171672 successful full-fetch queries; independent numerical audit passes. Known build/test/perf process watch recorded no interference, which is not a guarantee against unknown external activity. All own services stopped and both temporary database directories were removed after ownership checks. Actual original-native performance goal remains unmet: same-cohort retained/original frontend CPU ratios for integer, nested integer, JSON and deep16 are in the final report, with no causal kernel/framework cost split claimed.

Functional evidence was delivered separately in8ef19c3c, with immutable attachment4694f18b. Read-only reviews e0a7f728 and83d0f913 verified code/functionality without rebuilding or claiming performance. The final performance report and archive use different filenames; never rewrite the functional ZIP. Report delivery precedes the authorized local documentation-only DCO commit. No Rust candidate is staged or committed, no push/PR/deployment is authorized, and human acceptance is still required after task in_review.

## Context and Orientation


Main is /home/agent/tidb/expression-unification/tidb, restored source HEAD93ae4ceb132bec289080d7f4fbeea991015acf09. SDK sibling ../tikv remainsf7672bf980143f2938fd0f900650a4c29091e70f. The nine trial Main files under rust/crates/tidb-expr/src/evaluator are batch_hosts.rs, batch_arithmetic_window.rs, batch_unary_window.rs, batch_decimal_fast_window.rs, batch_typed_window.rs, batch_numeric_window.rs, batch_in.rs, batch_interval.rs and batch_prefix_values.rs. They are now byte-identical to their retained originals.

NativeBatchHosts adapts authenticated native values to the existing SDK protocol. HostInvocation represents one logical row occurrence; HostWindowInvocation borrows a nonempty row selection with an occurrence origin. Repeated physical rows remain distinct logical occurrences. Existing SqlDemand states and task layout were never changed.

The unrelated untracked rust/third_party/tikv-client-rs/tests/client_go_differential/BUILD.bazel remains untouched and must never be staged/deleted. SHA256 iscc9fb078591d7c94366d1e6f504ff2bade23ccb57a9313f9c7235807ff658d52. Frozen native worktree /home/agent/tidb/bench-original-native/tidb is read-only, including its preexisting test changes.

## Plan of Work


Implementation and evaluation are complete. Preserve the final evidence, deliver the report, then commit only this execution record locally using authorized YangKeao / yangkeao@chunibyo.icu DCO identity. No future implementation or build is necessary to finish this experiment. Any new source candidate would need a distinct scope, fixed inputs, binaries and evidence; it must not overwrite this result or reuse it as proof of no regression.

For reconstructing the rejected prototype, use the saved main-candidate.diff in a separate checkout with the pinned original HEAD. It defines private invocation_signature_matches, window_scope_is_valid and install_window_task helpers on NativeBatchHosts, retaining identical error/allocation/demand order. The eight get removals are separately described in duplicate-read-proof.json. Do not turn every row invocation into a one-row window, restore a withdrawn ownership experiment, or relax capability authentication.

## Concrete Steps


All paths below are relative to the agent workspace unless stated. Actual candidate functional build used Main cwd, empty RUSTFLAGS, CARGO_BUILD_JOBS=1 and:

    ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j1 -p tidb-expr --lib --no-run --message-format=json

The exact subsequent consumer, service and lint commands with real child exits are in front-runtime1.json, server-build1.json, lint1.json and their receipts under evidence/path-unification/native-read-dedup. Fixed Jan30/Aug22 rustfmt covers the nine files. Root make lint passed; Rust-only changes required no Bazel prepare.

The actual performance controller was launched exactly once:

    python3 evidence/remove-demo-policy/resume-nov05/job_runner.py evidence/path-unification/native-read-dedup/night-experiment-nov09.job.json

It exited0 after483.95 seconds. Do not rerun it over these completed artifacts. Recompute final effects/counts safely with:

    python3 evidence/path-unification/native-read-dedup/final_analysis.py

The recomputation does not launch services or modify code. Inspect final-decision.json, restoration-nov09.json, audit-final.json and cleanup.json. Expected final decision isREJECT; all9521 inputs match retained source and no candidate Rust diff remains.

## Validation and Acceptance


Candidate actual unit binarySHA256 isee9f6df30be9ddda987228a3ec215625a635a29e36a0bf29543e203817c6706d. Complete2440 test names match;973 selected consumers yield972pass/1 inherited common failure with exact body after PID/time-only normalization. No golden updates, missing terminal events or timeouts. SDK unchanged: its prior1159pass/1 STR_TO_DATE failure/1ignore and171clippy messages are inherited, not new executions. No new full2440 sweep or real allocation-failure injection is claimed.

SQL measurements use own8192-row storage fixture, three actual frontend binaries on the same backend,15 scenarios and four balanced rounds. Each cell runs at least2seconds and30 successful complete-fetch queries. Results, fields and warnings are checked before/after timing; preflight checks complete EXPLAIN. Scenarios include4096-row families, three one-row controls, deep16, width8, JSON batch32 and vectorization disabled. This is whole-SQL/whole-frontend CPU, not pure-kernel nanoseconds or stored-JSON-column coverage.

Acceptance required retained semantics and no demonstrated performance loss. Functionality passed scoped checks; no-loss acceptance failed, so the correct delivered implementation is the original retained source. Rebuilding byte-identical restored source is unnecessary; restoration proof verifies all pinned source/config inputs, unchanged SDK/native and protected file, and records the expected status difference for this new document.

## Idempotence and Recovery


The completed evidence directory must not be overwritten. All candidate files, patch, actual binaries, initial preflight mismatch and original-HEAD backups remain available. Whole-candidate rollback used all nine *.retained.baseline.rs files; *.phase1.baseline would leave unaccepted intermediate changes and is not a retained restoration.

Immutable retained server is evidence/remove-demo-policy/performance/tidb-server-after-round6 SHAe17bfb3553535fc478b7cd9576f3658498611463fcfcf7561e159aba278124ee. Candidate round10 is7bebf5d50216d097d53901b09f51049c635ea8c4039cd03b182c2ec07d44cb3e and remains rejected. Mutable target-tidb/release/tidb-server still holds round10, so never use it as the restored baseline. Round9e6142453431d7a28fbe90ef0690c8bf962b6def29d5e224cd7d9f9daa682b609 is an unaccepted intermediate. Original native is99a4c3e566774ba9733bc9d54366cf1e9b85446f63fa9a7418bfef5d3daeddd2; shared backend is355be0acec593a24bb43ac0f5e6b8e30aae354b7e63fdd3530f3f2895c41e029.

Owned service cleanup verifies PID/birth/command data-dir and free ports; do not stop another person's jobs to obtain measurements. This experiment's services and data are already cleaned. No staging/cleanup command should use a broad wildcard that captures the protected BUILD file.

## Artifacts and Notes


Primary evidence directory is /home/agent/.slock/agents/02fef0a9-76ce-4afe-9bce-73eebe82ff7d/evidence/path-unification/native-read-dedup. It contains REPORT.final.zh.md, summary.json, primary.jsonl, strict preflight history, final-decision.json, source/binary pins, all functional receipts, restoration-nov09.json and cleanup.json. Phase1 evidence lives in sibling protocol-boundaries and is explicitly intermediate. The final archive task14-unification-final-rejected.zip is distinct from the immutable functional ZIP. Postcommit verification belongs in a separate receipt.

## Interfaces and Dependencies


The retained HostInvocation, HostWindowInvocation, HostCatalog, NativeBatchHosts, SqlDemand, native value authentication and SDK execution loop remain unchanged. No new public interface, dependency, runtime field or tests survive this experiment. This record documents a rejected internal simplification rather than introducing another framework layer.

2026-11-09 revision: completed actual measurement after fresh load checks, rejected the candidate, verified full restoration and recorded final cumulative counts. Earlier functional-only pending statements are superseded by this complete outcome.

Final delivery/review: report delivered in353ba57c before any local documentation commit. TiDBEngineer independent read-only review164d96db recomputed180cells/171672queries, checked all9521 restored inputs, binary separation, preflight and cleanup, and passed the conservative rejection. It did not rebuild or rerun tests. Path-unification without performance loss remains unachieved; a future isolated experiment could test the two steps separately, but is not represented as completed here.
