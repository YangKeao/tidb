# Remove Demo resource policy and validate inputs once

This living ExecPlan follows repository PLANS.md. Update Progress, discoveries, decisions and outcomes as work continues.

## Purpose / Big Picture


YangKeao requested cancellation of Demo resource policies in message d286c74f in #tidb-expression-reuse:5841a0b2. Queries must retain native SQL values, demand order, warnings, side effects and errors, while no longer failing because of the experimental compilation, execution, materialization or value-work allowances. Keep the common SDK execution graph, host start/resume/cancel protocol, independent experimental entrypoints and observation counters. Validate immutable program facts during preparation and each actual input batch at its entry, not once per output column. A callback's new result still requires its own shape/type validation.

Task #7 tracks this work. TiDB starts at 4ab480e118b9a4e3cd2b1329bd7894577a99e109, SDK sibling ../tikv at 8f67591496dbcddbb422f5b6049eb07498bd006e. Original native reference /home/agent/tidb/bench-original-native/tidb is read-only. Do not touch unrelated untracked rust/third_party/tikv-client-rs/tests/client_go_differential/BUILD.bazel (SHA cc9fb078591d7c94366d1e6f504ff2bade23ccb57a9313f9c7235807ff658d52). No push, PR, release or deployment.

## Progress


- [x] Read current directive, claim task #7, inspect source and repository guidance; confirm no owned builds running.
- [x] Record failing cancellation regression: cancelled_demo_allowances_do_not_reject_native_projection compiled and failed on old implementation with metadata ResourceLimit (regression-before.log). Deep/wide values and same-session recovery passed in the first-round boundary matrix.
- [x] Remove Demo policy checks and their runtime accounting from SDK and TiDB; retain required actual-size overflow/allocation failure handling.
- [x] Move immutable checks to preparation and consolidate actual batch validation; preserve changing callback result checks.
- [x] Update quota-only tests to the new contract and run relevant existing semantic tests.
- [x] Run SDK/front/executor/Unistore checks appropriate to changed interfaces, formatting and root lint; preserve and report known baseline failures.
- [x] Verify SQL deep/wide boundary recovery and representative performance, document exact results and remaining limits, commit local changes and deliver evidence.

- [x] 2026-11-05: Resolve all 442 initial frontend test interface errors; `cargo check --tests` passed before subsequent cleanup. Preserve malformed token lengths by converting only test wire boundaries to Vec.
- [x] 2026-11-05: Replace two intentionally infinite quota-dependent fixture providers with finite HostContract failures; keep actual cancellation/cleanup proofs. The complete SDK test runner now finishes in less than a second.
- [x] SDK fullsixth: 1158 passed, the single independently confirmed pre-existing STR_TO_DATE failure, 1 ignored. Final first-round rebuild and full runtime completed with the same baseline.
- [x] 2026-11-05: Remove native host storage rescans, repeated warning-size ledgers, per-window value byte ledgers and duplicate retained schema snapshots from production. Explicit private observations compute sizes only when requested.
- [x] 2026-11-05: Remove private Decimal/vector materialization allowance arguments and reply-size precomputations while retaining actual role/frame guards. Move completed window values into authenticated slots.
- [x] Finish deliberate migration of cancelled-policy assertions, preserve actual value/event/ID references, and complete runtime/performance gates below.

- [x] Independently preserve original gold references and check explicit policy deltas: AST 191763, reserved raw 184760, reserved dates 715200, membership 1144320, wildcard 42912. All originally admitted membership records are identical. New JSON fixtures assert actual after records before folding archived before records into the unchanged original digests.
- [x] Resolve all eight failures from the 87-pass focused build8 diagnostic; frontend test compile check27 and executable build9 passed.
- [x] Full build9 diagnostic completed all2440:2297passed/47failed/95ignored/1timeout. This is not a final-source pass. Final runner allows1800seconds and empirical4GiB AS for exactly two large native corpora,768MiB otherwise; no name-based skips.
- [x] Delete final unobserved import/copy ledgers found during review; preserve actual store poison checks, allocation checks and per-token authentication.
- [x] Final report uses the previous three SQL tables and reports both immediate before/after gains and the remaining gap to the original native frontend. User 6ce6e4da requests local DCO commits after this report, followed by autonomous further performance attempts. No push or deployment is authorized.

- [x] Follow-on frame variant: SDK1158/1known/1ignored, Main973=972passed/1samecommon failure; clippy, lint, dual format and45SQL prechecks passed.
- [x] Four-round180-cell/170737-query frame experiment audited. JSON/text/control CPU improves6.7%/6.2%/9.8%, but deep16CPU regresses4.3%; this variant is preserved as evidence, not committed as the final optimization.
- [ ] Refine loop return to small LocalResult<()> using its existing completed-value slot, and inline the small private push_frame helper; rerun SDK and consumer gates.
- [x] Independently compare refined actual release ELF against round-one af24 and original99a:180cells/172004queries audited,45strict equivalent preflight observations. Retain with explicit heavy-expression gains and small repeated nested/deep/wide regressions; final report generated before local commit. Local commit receipts are recorded in the handoff.

## Surprises & Discoveries


The SDK currently couples execution-domain admission to EvalBudget accounting mode even though EvalExecution already records the domain. Remove the policy coupling without widening admitted types or enabling a fallback. Existing tests deliberately configure tiny resource caps; they must be updated to cancellation semantics, not called inherited failures. Existing tests involving true impossible allocation sizes must not be converted into attempted enormous allocations.

The old tests often relied on quota failure before reaching later assertions; removing the policy exposes some previously unexecuted fixture assumptions (for example, an intentionally impossible floating-point formatting precision). Retire policy-only fixture arms rather than introducing replacement execution caps. Keep malformed transport, genuine allocation/overflow, callback and primary-error coverage. A source-text assertion counting EvalBudget calls is retired along with EvalBudget.

## Decision Log


- Decision: The user's cancellation supersedes earlier instructions to preserve or not raise Demo quotas. Remove the policy, rather than increasing finite defaults or adding a runtime opt-out switch. Keep real overflow and allocation errors.
  Rationale: This is an explicit authorized behavior change and the requested Ponytail simplification.
  Date/Author: 2026-10-30 / ExpressionReuseEngineer.
- Decision: Keep independent experimental entrypoints and observers. Where their existing signatures expose obsolete allowance arguments, preserve compatibility at the public boundary and clearly document that Demo allowance parameters no longer impose policy; remove the internal enforcement/accounting and do not introduce replacement knobs.
  Rationale: User explicitly asked to retain these interfaces and observations while cancelling the policies.
  Date/Author: 2026-10-30 / ExpressionReuseEngineer.

## Outcomes & Retrospective


The Demo policy removal is complete, validated, reported and locally committed as c57e66f1ff/ccca28c. The first-round report preserves all disclosed inherited failures and records production/test Rust net reductions5097/284. Representative4096-row integer/nested/JSON CPU costs fell40.4%/47.4%/38.0%, while remaining1.714x/3.382x/5.270x original. The follow-on trial is complete and the refined in-place-frame variant is retained:4096-row control/JSON/text frontend CPU falls12.6%/9.4%/7.7% relative to the round-one final implementation, while nested/deep16/width8 CPU rises2.8%/1.3%/2.6% across all four rounds. This is a documented tradeoff, not a universal speedup. Cumulative production/test Rust net reductions are5078/281lines. The original-performance goal remains unmet: integer/nested/JSON CPU is still1.666x/3.235x/4.797x original. Further profiling of value transport and authentication is a possible future direction, not an unmeasured gain or part of this commit. No new evaluator, fallback, dependency or public API is introduced.

## Context and Orientation


TiDB rust/crates/tidb-expr/src/evaluator.rs caches PreparedBatchSuite. evaluator/batch.rs lowers source expressions, prepares one SDK program per calculation, validates batches, provides bindings and publishes results. evaluator/batch_hosts.rs and adjacent batch_* value modules implement host services, using NativeValueStore in batch_values.rs. SDK ../tikv/components/tidb_query_expr/src/types/expr_eval.rs is the sole common iterative expression driver. Its local/runtime.rs defines Demo ExecutionLimits/EvalBudget and retained-storage helpers; local/spec.rs defines CompileLimits; local/value_work.rs provides a thread-local conversion-work policy. local/batch.rs exposes both production and independent experimental entrypoints. A suspended host task means synchronous saved argument-demand state, not an asynchronous thread.

The completed read-only review at the agent workspace evidence/ponytail-framework-review/REVIEW.zh.md lists eleven source-located findings. YangKeao expanded scope in ed90825f: all six reviewed classes of extra work are authorized for removal, including duplicate metadata validation, selection copies, per-value heap handles/round trips, single-element vector intermediates, fresh/exact-growing temporary containers and full storage rescans. Preserve host task protocol, independent lineage APIs and telemetry, and do not revive an old evaluator.

## Plan of Work


First add a focused test proving valid expressions execute despite formerly restrictive Demo allowance inputs. Capture its pre-change failure. Keep allocations bounded by test data. Then remove checks and dedicated statistics used solely to enforce Demo policy from compilation, frame/task scheduling, native materialization and lexical value-work. Preserve actual carrier admission, source identity, required callback result shape, integer overflow and allocation failure handling. Keep observation-only data requested by the user.

Consolidate schema, input column layout, selected row bounds and output layout validation at the suite/batch boundary. Prove immutable worker/profile and host metadata relations once during preparation. Per-row ordered paths must preserve row/select-list order and new externally supplied results must be validated when received. Reuse existing test helpers rather than adding a parallel evaluator.

Update tests whose explicit purpose was cancelled cap rejection. Preserve all value/event reference fixtures and strict SQL correctness checks. Exercise deep/wide expressions previously refused by Demo policy, NULL/short circuit, actual malformed input, callbacks, recovery and rebind/reuse.

## Concrete Steps


From /home/agent/tidb/expression-unification/tidb use the existing wrapper ../tools/cargo-tidb with RUSTFLAGS='' and CARGO_BUILD_JOBS=1. For frontend tests run:

    ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j1 -p tidb-expr --lib <focused-test>

Use the corresponding tidb-executor and Unistore package commands from existing project receipts for affected consumers. SDK uses its pinned Jan30 toolchain and existing offline target/cache; recover exact command from the prior evidence and do not mix frontend/compiler versions. Run root make lint after source work, with no simultaneous build mutating the same generated sources. Check changed files with both configured Rust formatters. Store logs and receipts in agent evidence/remove-demo-policy/.

## Validation and Acceptance


Formerly tiny Demo allowances do not reject otherwise valid bounded expressions. Deep and wide test queries return exact expected values, without fallback or raising a finite cap. Changing input shape and invalid callback output still fail correctly; subsequent valid executions recover. Existing native SQL value/events/read-count contracts remain valid except explicit old resource-policy expectations superseded by this request. Review diffs to ensure host task and observer interfaces remain present. A useful performance check must use rebuilt binaries on the same representative workload and report limits; tests alone are not performance evidence.

## Idempotence and Recovery


Save pre-change tracked source snapshots in agent evidence before scripted edits. Never replay old migration scripts or restore historical working-source backups. Work only on the two current owned trees. Finish edits before starting builds/tests; record terminal sessions before subsequent edits to their source. Recover partial transformations using exact saved files, never a blanket checkout deleting unrelated work.

## Artifacts and Notes


Record failing regression, compiler/test results, final source and binary hashes, preserved protected-file hash and focused source audits. The task remains in progress until required behavior and validation are complete; send final results to the original #tidb-expression-reuse:5841a0b2 thread and move task #7 to in_review.

## Interfaces and Dependencies


No new package or version. Use the already resolved house SmallVec crate (1.13.2, smallvec="1.4") through a direct SDK dependency edge for short-lived frame and argument buffers; geometric spill preserves arbitrary sizes. This avoids unsafe reuse across borrowed lifetimes. Both lockfiles change only that dependency edge. Keep one SDK graph and existing host task protocol, experimental entrypoint functions, result metadata APIs and observers. Resource-policy DTO/signature compatibility is permitted only at retained external interfaces; it must not keep active enforcement or repeated runtime storage walks. No Go/Bazel changes are planned. Any newly discovered scope change must be recorded here with its concrete reason.

## Nov 5 continuation

Recovered runtime interruptions and switched to one foreground build at a time, durable terminal receipts, and test-only address-space/core limits for old stress fixtures. Production receives no replacement resource cap. Two inherited STR_TO_DATE failure logs establish the original SDK failure. The latest whole SDK run before the final test migration completed with 1136 passing, 23 failing, one ignored; it is diagnostic, not acceptance. Migrated 21 policy-only tails to actual value, demand trace and cleanup assertions, keeping true coordinate batch bounds and invalid input checks. Historical ready-operation digest changes require independent record capture from the exact pre-change SDK commit before changing any expectation. A detached reference checkout contains only optional test record export, no revived evaluator or product fallback.

Private adapter copy/probe allowances and unused reply preplanning are being removed. Five quota-only huge-format tests and two quota-only mixed-test segments have original bodies, hashes and removal reasons archived; valid coefficients with billions of digits must not be formatted merely to test a policy that no longer exists. Other demand, malformed capability and recovery checks remain. The compression finite large-input case now compares actual results to native dispatch. Full frontend, executor, Unistore, lint, dual format and real before/after SQL measurements remain outstanding.

### 2026-11-05 19:55 UTC continuation

SDK final build12 completed:1158passed, the exact previously confirmed STR_TO_DATE failure,1ignored. Project-configured targeted clippy completed with170warnings (95 expression warnings); no warning-equality claim across changed invocations. Both Jan30/Aug22 format checks passed for197 changed Rust files.

Independent PB eager full comparison completed2,317,248 records:2,313,984 identical;3,264 old ResourceLimit outcomes now expose the same original malformed-Decimal UTF8 panic. Indexed before/after fixture asserts actual outcomes before folding independently archived before records into unchanged529CC9B973792E9D. Current temporary exporter removed. The common657792/D19F5082E4AF34C5 versus historicalA94C463994D2D98D is independently reproduced on unmodified before source and remains disclosed, never rebased.

Final scope audit found independent experimental graph entrypoints still imposing the same CompileLimits and source/materialization Demo policy. Removed those checks and size ledgers from ordinary/lower/lineage, keeping entry signatures, graph/ID/type/schema validation, observations and actual allocation errors. New tests execute at zero policy settings and retain mismatched-input refusal. Explicit native bounded primitive APIs and StructuralOnly preparation grammar are distinct contracts; SQL runtime passes unbounded native work settings. Maincheck34 passed after these changes; build12 is running. Complete runtime, executor/Unistore, lint, SQL performance and commits are still owed. No speedup claimed.

### Final-source candidate before full validation

Build12 focused68 finished66passed/2failed; both remaining tests were corrected for existing Ready cache lifetime contracts and passed on build13. The caller-owned scope remains poisoned after the original native BIT presentation panic; a new scope reuses the prepared program successfully. Cross-call worker observers remain asserted rather than erased. Both PB eager full2317248 tests pass with independently checked policy fixtures and unchanged historical digest. Experimental ordinary/lineage/seed zero-policy cases also pass.

The last unobserved pending-output payload ledger and dead typed-carrier size helper are now removed. Conservative source metadata observations remain because the independent SourceFragment interface exposes them; they impose no Demo cap. Final rootlint/check/build and a complete2440-test run are next; no timing result or completion is claimed.

### Complete Main verification and reverse-consumer policy migration

Main final binary14 completed all2440 tests:2341passed,4independently known failures,95ignored; no timeout or abort. The4failures are the original catalog/EXP/vectorized-op cases plus the independently before-reproduced common digest. Projection52 and Expand6 pass. SDK full1158passed/1old STR_TO_DATE failure/1ignored remains applicable; no SDK edits after that run.

Unistore's first full run found an additional66048-record legacy-scope golden change. Independent before4ab/8f with the identical1100-ABS input passed3F895141D852AA15. Strict comparison found64256 identical records and1792 changes, all formerly ResourceLimit/Prepare for the named deep resource-refusal input. Independently running OLD native kernels with one ABS (idempotent for the fixture's5/NULL/native-error/panic inputs) matched CURRENT1100ABS across ALL66048 records, including typed views, callbacks and reuse: raw SHA5c66676b3a3c37822a5d7782f541eec5000bd8a243f447cba1f64430b54b4bde. The old reference source was restored after this counterfactual capture.

Unistore test digest now verifies exact after records from1792 indexed TSV observations before folding independently archived before records into the unchanged3F89golden; every observation must be visited. The existing export continues writing actual after data. No runtime algorithm or old evaluator was added. TSV uses standard borrowed line parsing, without adding a dependency. Rebuilding/rerunning the full reverse-consumer gate is in progress; only its cfg(test) source/fixture changed, so all Main unit and production inputs remain unchanged. This scope proof and all before/after records are archived. Final lint and both Rust formatters passed after this migration.

Performance measurement and commits remain pending. Do not describe the known failure baselines as an all-green suite.

### Round-one measured outcome (2026-11-05)

All required gates finished:tidb-expr2341passed/4known failures/95ignored,SDK1158/1known/1ignored,Projection52passed,Expand6passed,Unistore207/3known/13ignored;rootlint and both formatters pass. Independent review79da679d confirmed the1792Unistore changes and exactold-one/new1100-ABS equivalence, limited to that fixture.

Final release ELF af24b10211b8ba79fd22b66fa80e10af6f206d67831a4a8c446e7868b8a035dd was measured against immutable before4cb4d8ce and original99a4c3e5 with the same releaseTiKV355be0ac. All99independent formulas and60three-mode preflight queries passed exact results/metadata/warnings/plans. The720primary cells contain357008 timed full-fetch queries;36concurrency windows contain65257 queries. All360boundary observations were retained:before68success/44resource errors/8connection losses,after120success withsame-session recovery,original120success. Audit passed and all owned services were stopped.

At4096rows, integer/nestedinteger/JSON SQL latency fell4.9%/14.7%/24.6%; frontendCPU/query fell40.4%/47.4%/38.0%. CPU remains1.714x/3.382x/5.270x original respectively, so overall original performance has NOT been reached. Full results include scalar/multirow SQL,18families,NULL densities,batch32/1024,vectorization-off,depth/width,memory and1/8-client load. SQL ms/query includes planning,RPC,encoding and fullfetch; it is not pure function ns/eval.

ProductionRust net reduction5097lines;testRust284lines. Exact Git additions/deletions include comments/blank lines;23548lines of independent JSON/TSV policy observations are counted separately. Detailed report and compact evidence are in the agent's evidence/remove-demo-policy/performance/measurement/REPORT.zh.md and companion CSV/JSON files. Per user6ce6e4da, deliver the report first, then localDCO commits, then autonomously try further optimization against original native. No push,PR ordeployment.

### Follow-on: retain suspended frames in place


After the delivered report and local commits c57e66f1ff/ccca28c, an actual user-mode cycles profile of the immutable round-one server found26.86% self samples in memmove. Disassembly shows584-byte EvalFrame moves through every pop/re-push, including suspended parents. The follow-on prototype advances frames through last_mut, pushing only a newly demanded child and popping only completed/failed frames. Control results are moved from their accumulator; host finish borrows the frame. The TaskGuard still outlives frame values. A callback error cannot be caught by the same active host's Result boundary, whereas failure to allocate a requested child can be handled by its waiting parent.

The existing exact-cause/cleanup/recovery test is extended from one relay to1/8/32 relay depths, exercising inline and spilled stacks. Run the complete SDK expression suite and relevant frontend frame consumers, preserving all prior golden observations and disclosed baseline failures; run formatting, project clippy and root lint. Then compare the rebuilt candidate against the immutable round-one af24 and original99a binaries on the same six representative4096-row SQL families with balanced rounds, actual frontend CPU and full-fetch latency. Profiles are outside comparison timing. Keep the optimization only if semantics pass and measured gains justify it; otherwise revert this isolated follow-on diff, retaining the failed experiment evidence. No additional API, evaluator, native fallback or dependency is introduced.

### Frame experiment and refinement


The preserved frame-only release ELF862baf5bb88a46ab073dc7206195905df5373b6264592dc3be578536fb5c92b8 completed180cells/170737queries with45equivalent preflight observations. At4096rows, JSON/text/control frontend CPU falls6.7%/6.2%/9.8%; deep16CPU increases4.3% across all four rounds, integer2.7%, width8about1.7%. Full records and an explicit provisional report are archived outside the repository under performance/followon. Do not overwrite them or confuse the regression with success.

The next variant retains parents in place but restores a small LocalResult<()> step return. Completion already has an owned value in the existing returned slot; after a successful step its presence causes exactly one frame pop. Errors still exclude the active host unless allocation of a requested child failed. The private push_frame helper receives an inline hint to let code generation optimize initial frame copies. Existing nested demand, exact error identity, cancellation, all SDK tests and973frontend consumers must still match. A fresh15case/three-version/four-round experiment compares directly to af24 and99a, so the original-performance gap and any remaining regressions are explicit rather than inferred from separate historical runs.

## Final refinement result (2026-11-05 23:05Z)

The preserved actual after-round3 ELF ebb0117d1925d8fea67cd7df8bf8c490614197caee3c8ab11bf8a106481f191d completed180cells/172004queries against af24 and99a. Control/Decimal/float/JSON/text CPU falls12.6%/7.8%/8.6%/9.4%/7.7% at4096rows; all four rounds agree on those directions. Nested integer/deep16/width8 CPU rises2.8%/1.3%/2.6%, also all four rounds. Integer CPU changes-0.8% with a range crossing zero. Retain because the larger heavy-expression benefits justify these explicit small regressions; do not claim the original native target is achieved.

The first preflight attempt stopped before timing because12batch cases had different estimated row counts2730.67/3333.33; every result, field, warning and all other EXPLAIN columns matched. That attempt is preserved. Fresh owned storage and frontend restarts converged all estimates to3333.33, and the repeated45observations passed exact equality of every EXPLAIN column without normalization. The precise transient origin is unproven. A diagnostic-only metadata subquery failed identically in current/original; it was not a timed workload.

Final SDK runtime:1158passed/1exact inherited STR_TO_DATE failure/1ignored. Final frontend consumer runtime:972passed/1exact inherited common failure over973selected tests, including complete PB eager actual phases and lazy actual physical cases. Both failure bodies match the round-one baseline after only PID/time normalization. Project clippy, root make lint, both pinned rustfmt toolchains, and git diff --check pass. No real allocation-failure injection or new full2440frontend sweep is claimed. Input snapshots and protected BUILD hash are unchanged throughout build/timing. The final report, raw cell observations, gates and failed/retried preflight records are under agent workspace evidence/remove-demo-policy/performance/followon-return; first-round and provisional reports remain immutable. All owned benchmark processes and data are cleaned only after PID/birth and port checks.

Revision note: closed the previously pending refinement with measured gains, repeated regressions, unchanged failure baselines and a retain decision. No new evaluator, fallback, API or dependency was added.
