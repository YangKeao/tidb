# Transfer owned typed-forward results into their existing arena

This living ExecPlan follows PLANS.md. Authorization is YangKeao's continuing simplification/performance instruction and supervisor message6f1f387e in #tidb-expression-reuse:5841a0b2. Task13 permits local experiments and DCO commits, without push or deployment.

## Purpose / Big Picture


Determine whether removing one redundant native value copy from the existing deferred typed-window forwarding path helps actual SQL workloads. The path first authenticates and independently clones an input that must remain live, then currently clones that independent result a second time into its output slot. Use the already existing ownership-taking insertion for that final step. Preserve both authentication passes, the independent input copy, original integer conversion and all protocol checks.

## Progress


- [x] Inspect all insertion callers and existing ownership-taking implementation; scope the trial to the one production insertion in batch_typed_window.rs.
- [x] Save initial input hashes and the original file; make the two-line substitution without a new API, dependency, evaluator or test-only instrumentation.
- [x] Run existing typed-window large-value/authentication/recovery checks, scoped frontend consumers and quality gates; build an actual release service.
- [x] Compare retained round6, candidate round8 and original-native actual services in the same strict balanced cohort. Decide retention from all results.
- [x] Preserve final report/evidence and restore the one trial Rust file; clean owned services/data. Only this document is prepared for the authorized local DCO commit after report delivery; the separate postcommit verification records completion. No push.

## Surprises & Discoveries


The value-store insert_clone and insert_owned methods already share insert_with and identical generation/role/provenance and reservation gates. The latter accepts an owned Datum. No new storage API is necessary. Other insert_clone callers mostly borrow constants or test fixtures and are deliberately outside scope.

The first copied gate controller retained a974-case constant from the previous experiment. Its already compiled code asserts after the actual973-case run completes. Preserve that initial controller and terminal record; the continuation checks the same973 cases and proceeds only with remaining gates, without changing tests or repeating completed work.

Initial strict45 preflight again differs only in12 cases' estimated-row columns2730.67 versus3333.33. Archive every observation and column difference before any timing; restart only owned storage and repeat the unchanged strict check. All45 then match before180 timed cells. Exact transient cause remains unknown. No EXPLAIN normalization is introduced.

## Decision Log


- Decision: Change only the final insertion in resume_typed_window from insert_clone(&value,...) to insert_owned(value,...).
  Rationale: This local value is already independently owned, is not used afterward and crosses no callback between the two insertions. Its original source capability remains intact. Existing exact-value, mixed-type, large-string, malformed-capability, occurrence, generation, cancellation and recovery tests cover the public behavior. No new branch or implementation-mirroring test is needed for this existing API substitution.
  Date/Author:2026-11-06 / ExpressionReuseEngineer.

- Decision: Assess this as a small ownership simplification, with performance as a constraint; do not require a broad SQL speedup to justify an existing-API substitution with no added state.
  Rationale: Human message7edb6fd7 asks whether to shift focus to simplification; reply44ce279d adopts that priority. Retention still requires all scoped behavior/quality gates and no material consistent measured regression. The extra paired timings run existing real deferred-forward test corpora, including startup, fixtures, assertions and cleanup; these are neither pure-kernel measurements nor SQL results. Ordinary SQL matrix differences alone cannot be attributed to a path it may not exercise. All data, including unfavorable changes, must be reported.
  Date/Author:2026-11-06 / ExpressionReuseEngineer, before any candidate performance timing.

- Decision: Reject and restore the two-line trial, preserving all source, actual binaries, raw data and proof.
  Rationale: Small whole-functional-corpus changes do not outweigh consistent regressions in the actual SQL candidate. Simplification cannot justify retaining a regressing binary. Exact performance causes remain unproven.
  Date/Author:2026-11-06 / ExpressionReuseEngineer, after all180 SQL cells and16 corpus cells completed.

## Outcomes & Retrospective


The experiment is complete and rejected. All973 selected frontend cases ended:972 pass and1 inherited common failure exactly matching after PID/time normalization only. No new test names or bodies were added. Both pinned format checks, root lint, diff check and the actual release server build pass. SDK remains unchanged; its complete suite/clippy are inherited provenance only.

Existing real deferred-forward corpora complete16 cells and160 actual subprocesses, all original assertions passing. Whole-corpus CPU medians improve approximately0.6%; the actual_profiles range crosses1. These include startup, fixtures, original-oracle checks and cleanup, and are neither pure-kernel nor SQL improvements.

Strict SQL comparison completes180 cells and169100 queries. Candidate CPU regresses consistently in deep16(+6.07%), width8(+3.81%), text(+2.48%) and scan(+10.84%); width8 latency rises3.72% in all four rounds. Other CPU/latency ranges, including favorable observations, appear in the complete report. Ordinary SQL path coverage and causal attribution to removed cloning are not established. The measured candidate binary nevertheless fails the no-regression condition.

Archive the exact candidate and diff, verify the initial backup equals its original HEAD blob, and restore only batch_typed_window.rs byte for byte. All9521 current source/config inputs match retained round6, not rejected round8. The candidate adds2/deletes2 production lines and changes no tests; actual retained Rust change is0/0, leaving cumulative production5066/test192 net lines deleted. Only this experiment document is submitted locally after report delivery. No push.

Original-native performance target remains unmet. In this cohort retained round6 CPU ratios are1.625/2.834/4.187x for integer/nested/JSON, and5.303x for deep16. Never use the rejected candidate as the final retained version or compound ratios across different cohorts. Human message6129cd47 now asks to unify batch/row/diagnostic integration without performance loss; that is the next independently tracked work, not a completed result of this two-line trial.

## Context and Orientation


Main checkout /home/agent/tidb/expression-unification/tidb begins at b581e2e96dce2f00a24c789cc8d97d6fce1fc069. SDK sibling ../tikv remains f7672bf980143f2938fd0f900650a4c29091e70f. Production file is rust/crates/tidb-expr/src/evaluator/batch_typed_window.rs, method NativeBatchHosts::resume_typed_window. Existing tests are in that file. Ownership and authenticated handles live in evaluator/batch_values.rs. Handles name producer, role, generation and logical/physical occurrence; they are capabilities, not serialized SQL data.

Actual retained service is evidence/remove-demo-policy/performance/tidb-server-after-round6, SHA e17bfb3553535fc478b7cd9576f3658498611463fcfcf7561e159aba278124ee. Round7 was rejected, and the mutable build output may still contain it; never use that as baseline. Candidate uses a new immutable round8 file. Original service SHA99a4c3e566774ba9733bc9d54366cf1e9b85446f63fa9a7418bfef5d3daeddd2 and shared backend SHA355be0acec593a24bb43ac0f5e6b8e30aae354b7e63fdd3530f3f2895c41e029 stay fixed.

## Plan of Work


First preserve a source/config snapshot and the one original Rust file. Replace only the final typed-window insertion with the existing owned entry. Do not remove the input clone, source authentication, complete-window prevalidation or integer bit normalization. Run the existing973 selected frontend consumers, including all typed-window tests and full PB eager actual phases/lazy physical corpora. SDK source stays unchanged, so previous SDK tests/clippy are inherited provenance rather than new executions.

Then build the actual Main release server, complete root lint, both pinned format checks and diff checks. Freeze all inputs and ELF hashes before measurement. Use the existing15-case,3-version,4-balanced-round harness with minimum2 seconds AND30 full-fetch queries per timed cell. Strict preflight checks results, fields, warnings and complete EXPLAIN. Archive any failed preflight before retry; never normalize plans. Keep all180 cells and all paired-round ranges, including regressions, scan controls and single-row scenarios.

## Concrete Steps


Workspace /home/agent/.slock/agents/02fef0a9-76ce-4afe-9bce-73eebe82ff7d holds evidence/remove-demo-policy/performance/followon-typed-forward-owned. JSON job specs record exact cwd, argv, environment and real child exit. From workspace run:

    python3 evidence/remove-demo-policy/resume-nov05/job_runner.py evidence/remove-demo-policy/performance/followon-typed-forward-owned/front-build1.json
    python3 evidence/remove-demo-policy/performance/followon-typed-forward-owned/gates.py
    python3 evidence/remove-demo-policy/performance/followon-typed-forward-owned/run_comparison.py

Main builds use ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j1 -p tidb-expr --lib --no-run --message-format=json with empty RUSTFLAGS. Server build uses the same wrapper/profile and -p tidb-server --bin tidb-server --no-default-features. One compiler runs at a time. Never overlap compiler/test/perf with timing. A wrapper exit0 is not a child test success; read the receipt.

## Validation and Acceptance


Require972 passing selected consumers and only the independently inherited common-summary failure. Its assertion must match the old body after PID/time normalization only; no test was added or removed, and no golden digest may change. Existing typed-window tests exercise1/3/2050 rows, duplicate physical rows, the existing mixed native Datum families, wide strings, malformed provenance and provider recovery. Run root make lint, fixed Jan30/Aug22 rustfmt with skip_children=true and git diff --check. No new full2440 frontend sweep or real allocation-failure injection is claimed. SDK complete tests/clippy are not rerun because its source is unchanged.

Measurement uses whole-frontend CPU ms/query and full SQL fetch latency, not pure-kernel ns. Report medians of paired ratios and observed four-round ranges, not confidence intervals. Make a retain/reject decision after all data. If the selected SQL matrix does not exercise this specialized deferred-forward path, disclose that and do not attribute unrelated changes to the removed copy.

## Idempotence and Recovery


Never overwrite completed evidence, old binaries or golds. Restore only the owned Rust file from its verified initial backup if the candidate is rejected; compare final source/config hashes with retained round6. The unrelated untracked rust/third_party/tikv-client-rs/tests/client_go_differential/BUILD.bazel must remain untouched, unstaged and undeleted, SHA cc9fb078591d7c94366d1e6f504ff2bade23ccb57a9313f9c7235807ff658d52. Stop only recorded owned processes after PID/birth verification. Delete only recorded owned PD/TiKV data directories after shutdown/command data-dir/port proofs. Preserve raw measurements and actual ELFs locally.

## Artifacts and Notes


Deliver the final report before authorized local DCO commits. Stage only the owned Rust file if retained and this document; never the protected BUILD. Seal detailed evidence, with a separate later postcommit verification for source binding, DCO identity and cleanup. Update task13 to in_review for human validation. No push, PR or deployment.

## Interfaces and Dependencies


Use only existing NativeValueStore::insert_owned and NativeBatchHosts protocol. No public API, kernel, interpretation route, quota or dependency changes.
