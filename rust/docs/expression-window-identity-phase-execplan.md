# Skip identity numeric conversion at the existing whole-source boundary

This living ExecPlan follows root PLANS.md. Update Progress, Surprises & Discoveries, Decision Log and Outcomes & Retrospective. Task20 continues the human shared-path/no-loss goal and TiDBEngineer message adf55170, from retained service14 rather than rejected mutable service15.

## Purpose / Big Picture


Reduce redundant per-row value movement without adding another evaluator, conversion implementation or input path. The existing native whole-source arithmetic adapter already owns all converted argument values before its numeric cast phase. When source and target domains match, its common cast function returns the value unchanged. Move this existing condition to the complete argument-loop entry so matching batches keep their existing vector intact. Observe preserved SQL behavior and a supported measured affected-case improvement before retention; the original overall goal is not claimed complete.

## Progress


- [x] Claim task20, read existing six diagnostic samples and inspect actual retained service14 plus current source.
- [x] Declare the distinct invariant-domain loop hypothesis before editing production source.
- [x] Apply only the loop condition; exact complete-file reversal and actual saved service16 branch bypass verified, with independent artifact review fac6cb75.
- [x] Original973 scoped consumers ended972pass/1 exact inherited common failure, original2440 names unchanged; actual root lint, both pinned format checks and fixed9521 inputs passed.
- [x] Complete primary8-scene128-cell window and separate4-scene72-cell uncertainty followup; preserve every result and adverse/control observation.
- [x] Decide conservative withdrawal and restore all9521 inputs; report/statistics delivered in bdf6d1f8 before submission. Raw evidence is being sealed with this document; subsequent raw-delivery and local documentation-only DCO completion receipts are recorded separately outside the immutable snapshot. No push.

## Surprises & Discoveries


The six existing current/original diagnostic samples are leaf self cycles, not full call chains, allocation ownership or recoverable gains. Current deep16 self samples show resume_arithmetic_window3.32percent and native_numeric_batch_cast0.78percent. Actual retained service14 calls native_numeric_batch_cast per row at0x429a060; its callee at0x4517480 compares source/target and copies the80-byte value unchanged when equal. The outer loop still moves the value out, writes aNULL placeholder and moves its result back. This is a verified redundant identity phase, distinct from rejected inlining and duplicate-authentication read deletion. The original inline candidate15 remains withdrawn; never use the mutable Cargo target as baseline.

## Decision Log


Decision (2026-11-10 / ExpressionReuseEngineer): keep every authentication/source-normalization operation, warning order and genuine numeric kernel, but hoist the existing source-equals-target cast rule to the argument loop boundary. Rationale: source/target are fixed per batch and the source phase has already completed; the existing callee's matching-domain branch is exactly Ok(value), with no context callback, conversion or error. Different-domain arguments execute the exact original loop. This is not loop fusion: all source rows still finish before any actual cast.

Decision: reuse the already sampled retained14/original99a evidence; do not rerun diagnostic captures, completed23-scene matrices, rejected read deletions or inline attributes. Actual new artifact and this distinct focused candidate comparison are required.

## Outcomes & Retrospective


Actual implementation, behavior and both performance windows completed. The guard mechanism works and the scoped behavior/quality gates pass, but no-loss performance acceptance was not established: withdraw the complete2-line source change and restore all9521 source/config inputs. Newly retained production/test changes are0; cumulative reductions remain5159 production/192 test lines. Measured deep16 candidate gains are retained as rejected-candidate evidence, not erased or counted as a retained improvement. Task19 was not retried.

## Context and Orientation


Main /home/agent/tidb/expression-unification/tidb HEAD6fc710429a8c9372a2d6e463fb1c26f9c522adae is documentation-only beyond retained c4fc. SDK sibling tikv HEADf7672bf980143f2938fd0f900650a4c29091e70f is unchanged. Edit only rust/crates/tidb-expr/src/evaluator/batch_arithmetic_window.rs, resume_arithmetic_window's second phase. Preserve collect_window_source in batch_window_values.rs. The callee rule is finish_numeric_batch_cast in rust/crates/tidb-expr/src/scalar_function.rs, reached by native_numeric_batch_cast; do not edit either. Domain means the existing NativeEvalType, not value content or SQL field spelling.

Retained service14 SHA2562d435bce2ba0ea2c153df91b5e6dbd82a5200d78c20bb09ee3dac8c993d75fea, original99a4c3e566774ba9733bc9d54366cf1e9b85446f63fa9a7418bfef5d3daeddd2 and backend355be0acec593a24bb43ac0f5e6b8e30aae354b7e63fdd3530f3f2895c41e029 are immutable actual binaries. Candidate uses new saved service16/unit16, never overwrite old services/archives.

## Plan of Work


Surround the existing per-row numeric-cast loop with if source != batch.domain, leaving all enclosed statements byte-for-byte identical except indentation. Store original/candidate whole files and assert exact reversal. Assert the unedited callee's equal-domain branch is Ok(value). No tests, metadata, protocol, kernel, conversion, warning, callback or source collector is changed. Same-domainNULL, signed/unsigned bits and invalid values receive precisely the already completed source normalization and prior strict integer carrier check; the removed identity cast could not itself validate or fail. Mixed domains retain the original phase and every error branch.

First build and pin actual service16 with fixed inputs, then inspect actual resume_arithmetic_window control flow and its cast call: same-domain batches must branch around the entire loop, with mismatching-domain calls still present. Symbol disappearance alone is not an acceptance criterion. Record code/text growth and limitations. If the intended loop remains active, withdraw before unnecessary runtime or timing. A supported artifact proceeds to original existing behavior tests, quality and focused performance.

## Concrete Steps


From Main, empty RUSTFLAGS and CARGO_BUILD_JOBS=1, run ../tools/cargo-tidb build --manifest-path rust/Cargo.toml --release --locked --offline -j1 -p tidb-server --bin tidb-server --no-default-features. Pin new output as evidence/remove-demo-policy/performance/tidb-server-after-round16 after actual child exits0 and all9521 inputs stay fixed. Then run ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j1 -p tidb-expr --lib --no-run --message-format=json, preserve actual unit16, enumerate original2440 names and run existing973 selected consumers individually through the durable runner. Expect972pass/1 unchanged inherited common failure after onlyPID/time normalization. Run root make lint, Jan30/Aug22 pinned rustfmt checks for changed Rust, git diff --check and source guards. SDK remains inherited validation only; do not claim new full2440, SDK or allocation-failure execution.

Agent evidence/path-unification/window-identity-phase contains job specs, actual PID/birth/exit receipts, source snapshots, exact file proof, saved assembly, scoped test logs, configuration/results/timing, decision and restoration. Its functional controller reuses the successful real service build receipt rather than rebuilding it.

## Validation and Acceptance


Original native-window differential tests cover integer signedness/NULL, literal and computed sources, Real/Decimal and mixed domains, source/cast warning phases, quotient/errors and same-program recovery. The original973 scoped list is unchanged and is execution evidence, not a substitute claim for full2440 tests. No new runtime API or algorithm requires a duplicate test implementation.

The fixed8 scenes are scan, integer, decimal, float, text, deep16, integer-plus-literal andABS(nullable+j),4096 rows with legal initial32/max1024/vectorization1. A/B are independent launches of the same retained service14; C is new16; F original99a. Four balanced orders A-C-F-B,C-B-A-F,B-F-C-A,F-A-B-C yield32 strict observations and128 timed cells, each at least4seconds AND30 full fetches with no maximum query cap. Before timing compare complete results, field metadata, warnings, fullEXPLAIN and actual effective configuration. Each timed cell saves actual settings before and read-only settings after, exact warm/final outputs, POSIX target-process CPU and latency/query counts. No build/test/profiler during timing; fresh16second actual quiet/core2-5 checks plus latest directives are required, not clock alone. Preserve any initial estimated-row-only preflight failure and all observations, allow only exact owned-storage restart/retry without normalizing plans. Report all eight absolute CPU/p50/p95/QPS values, paired medians/ranges vsA/B/F and same-ELF B/A control. No control subtraction, confidence interval or universal zero-loss claim.

Retain only with preserved behavior, supported mechanism and adequate actual affected-case evidence, including honest adverse/control assessment. Otherwise restore only the guarded file and count no gain. A new kept two-line branch adds2 production lines and tests0: cumulative production5157 fewer, tests192 fewer. Whitespace/indentation churn is separately counted; documents/config/rejected changes excluded. Report numerical measurements rather than promise benefit from3.32/0.78percent samples.

## Idempotence and Recovery


Do not change production source during any build/test/timing. Keep all actual failures, do not rebase golden results or alter source during comparison. On rejection restore original batch_arithmetic_window.rs only after checking it still matches saved candidate; verify all9521 inputs, HEAD/status and protected files. Stop only command/PID/birth-matched own services and remove only their data directories. Unrelated untracked rust/third_party/tikv-client-rs/tests/client_go_differential/BUILD.bazel SHA256cc9fb078591d7c94366d1e6f504ff2bade23ccb57a9313f9c7235807ff658d52 must never be edited/staged/deleted. Prior task15/16/17/18/19 archives are immutable. Authorized local DCO YangKeao/yangkeao@chunibyo.icu follows report/raw delivery; no push/PR/deploy.

## Artifacts and Notes


The task19 six captures remain immutable evidence/path-unification/major-overhead-probe and task19-native-store-inline-final.zip. New baseline caller/cast disassembly is captured directly from saved service14 before editing. No new profiling has run. Source/input proofs and actual build/test receipts establish the candidate's new provenance; old validation remains explicitly inherited.

## Interfaces and Dependencies


No signature, dependency, SDK source, field layout or evaluator changes. NativeBatchHosts::resume_arithmetic_window keeps its HostTaskId/reply parameters and HostStep result. Vec<Datum> from collect_window_source is untouched for matching domains; differing domains use the exact existing native_numeric_batch_cast loop. Required task/error/state/host authentication and downstream native arithmetic remain shared.

Independent source review6ac6519c passed before new artifact/runtime validation. It confirms the diagnostic closure is not invoked for equal domains and the exact full-file reversal. The original973 scope excludes the5,581,440-comparison numeric_window_context_sources_keep_types_phases_diagnostics_and_reuse matrix; it is not scheduled or claimed as a new run. Existing selected differential cases still cover same/mixed domains, source ordering and recovery. Keep these scopes separate in the final report.

Before any SQL timing/preflight: the bounded estimated-row-only retry proof accepts only the already observed2730.67/3333.33 pair, recorded verbatim in either direction for any of the fixed8 cases. Every other plan column/result/field/warning/config must match. This does not alter strict equality or accepted timing data; all32 initial observations are archived and only a single exact owned-storage restart/retry is allowed. Broader differences fail rather than normalize.

Actual gates: service build557.421seconds exited0, unit build281.055seconds exited0, scoped runtime416.173seconds controller exited0 with972pass/1 exact known test failure, lint2.372seconds exited0; functional controller702.306seconds exited0. New unit16 SHA256b019e7f93560d847c3c325dffb90b3efe8e9421beea7b8945e30d2d3e9d58cc2; service16 d5790aed8d45ab6adb7f9f1eb824ed5e5a866b73c65a5037734642276df4051e. Initial verdict writer failed an invalid old-address-equality assertion and shell continued unit compilation before the corrected formal JSON; preserve this chronology. Actual source-register/branch dataflow and unedited callee bytes were manually inspected before that launch, corrected before runtime/timing, and independently confirmed by fac6cb75. No source changed during any actual job.

## Separate validation of unresolved adverse observations


The primary8-scene32-strict128-cell window has actual exit0/655.009seconds,42064 timed queries, audit/owned cleanup passed. Deep16 CPU fell5.270percent vsA and4.852percent vsB, all four rounds; latency p50 also fell all four. This is supported affected-case evidence, but literal p50 vsB rose2.429percent all four, text p95 rose against both references, and Decimal CPU vsB rose0.458percent all four. Same-ELF A/B variation is also present. Do not discard those data or decide retention from the positive case alone.


Before retention, declare a separate focused followup for literal,text,Decimal and scancontrol only. Use the exact same candidate16 and retained14 A/B, no new build or tests and no originalF or positive-deep remeasurement. All six permutations of A/B/C balance each process into each start position twice and every pair before/after three times. Four cases times three labels times six rounds yield12 strict observations and72 timed cells; each at least8seconds AND100 complete fetches, no query cap, actual configuration32/1024/1 before and after. Exact results/fields/warnings/fullEXPLAIN still required, with only the predeclared bounded estimated-row-only archival/restart rule. Obtain fresh directive/load observation after primary read-only review IO ends. No source changes or profiling during timing.


This followup selects uncertainty from the complete primary results; report that selection and keep the two windows and their references separate. Do not pool them, subtract A/B, invent confidence bounds or treat a self-control as original native. Report every CPU/p50/p95/QPS observation in both windows. If adverse observations cannot be resolved adequately, withdraw; no gain is promised from the new guard. Agent evidence window-identity-adverse-check keeps actual inherited validation provenance explicitly separate from new SQL observations and real child receipts.

## Final two-window outcome and restoration


Primary actual655.009387seconds exited0:8scenes,32 strict retries passed,128timedcells,42064queries,4rounds. Initial32 observations failed only on estimated rows2730.67/3333.33 in69plan cells across8cases, every other field identical; preserve the full initial observations and single exact owned-storage restart/retry. Supplement actual711.169857seconds exited0:4scenes,12 strict retries passed,72timedcells,44545queries,6permutations and3labels with no originalF. Initial12 observations failed only on the same estimated-row pair in22plan cells, all4cases, and were preserved before a single exact restart/retry. Both real child terminals, all configuration/readback checks, source/ELF guards, zero known build/test/perf interference and owned service/data cleanup passed. Total200cells/86609timedqueries is only a work-count sum, never a pooled statistical cohort.


Deep16 primary CPU C/A fell5.270percent (range-6.624..-4.556), C/B fell4.852percent (-6.437..-4.034), all four rounds. Its p50 also fell against both references all four. Primary literal p50 vsB rose2.429percent all four and textp95 rose against both references all four; these did not remain unidirectional in the six-round followup. However DecimalCPU vsB rose0.458percent all four primary rounds and0.808percent all six followup rounds (followup range+0.041..+1.256). Followup DecimalCPU vsA median+0.768percent has range-0.556..+1.164. Keep both windows, all absolute values, paired ratios/ranges, QPS/tails and same-ELF A/B variation. Do not deduct controls, dismiss changes as noise, attribute them all to source, or assert zero-loss from the positive deep result.


Decision (2026-11-10 / ExpressionReuseEngineer): conservative withdrawal because the declared no-performance-loss condition cannot be accepted with persistent DecimalCPU uncertainty. No third blind rerun. The source mechanism and scoped tests did succeed; withdrawal is not a claim that all local optimization failed, that source definitely caused a slowdown, or that candidate performance was unmeasured.


Restore only saved batch_arithmetic_window.rs after verifying it equals the exact candidate. Actual restoration.json confirms all9521 source/config files, HEAD and pretrial git status match initial retained inputs; SDK and protectedBUILD unchanged. Prior15/16/17/18/19 archives retain their exact hashes. No build or test repeated after byte-for-byte restoration. Retained runtime reference remains immutable saved service14/unit14; actual candidate service16 d579 and unit16 b019 remain preserved rejected artifacts. Mutable Cargo target still16 and must not be treated as retained.


Attempted production17add/15delete, net+2, tests0/0; all withdrawn. Newly retained production/test0/0. Cumulative retained production2134add/7293delete, net5159fewer; tests5492add/5684delete, net192fewer. Documentation, evidence/config and rejected candidate lines are excluded. This completes the scoped investigation, not the original broader shared-path/no-loss goal. Deliver full report/statistics and both windows' raw evidence before already-authorized local documentation-only DCO; do not push.

Independent final review834e84ca passed: all72 supplemental cells/44545 queries, every three-label paired metric/range,12 initial observations with22 estimated-row-only differences, exact retry, actual source/binary/config/exit/cleanup and all9521 restored inputs independently verified. It supports conservative withdrawal, not a causal source-regression claim or no-loss proof. Raw scope, codegen, unit/quality, primary and supplemental review receipts are archived unchanged. No further tests/builds/timing were run by the reviewer. The result report keeps both windows separate.
