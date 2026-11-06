# Test checked host-argument buffer reservation

This living ExecPlan follows PLANS.md. Authorization is YangKeao's overnight optimization instruction and supervisor e8caca84 in #tidb-expression-reuse:5841a0b2. It permits local experiments and DCO commits, with no push or release.

## Purpose / Big Picture


Test whether reserving the already checked remaining argument count reduces repeated handle-buffer allocation in the existing host protocol. Preserve all argument demand, NULL stopping, source/role/generation checks and callback ordering. Decide using actual SQL measurements, rather than assuming fewer reallocations produce a material performance gain.

This experiment is complete and rejected. The tested change is functionally valid but provides no reliable text/JSON CPU gain, while conditional CPU consistently regresses. All trial Rust changes and their new test are restored to the previously validated source. Only this document is submitted; complete trial source, binaries, test proofs and measurements remain available.

## Progress


- [x] Inspect actual per-kind argument storage, exclude the legacy byte fallback and avoid adding a new container or dependency.
- [x] Add one regression covering native/PB two- and three-argument calls, result/order/capacity, and PB leading NULL. Prove original code actually fails its capacity assertion with exit101, then pass all five cases after the two production-line substitutions.
- [x] Complete974 selected candidate frontend consumers, exact inherited-assertion comparison with independently proven test-framework count adjustment, root lint, both fixed format checks, diff check and actual release server build.
- [x] Finish independent strict45 preflight and180 timed cells, audit172618 queries, reject the candidate and restore exactly the three owned Rust files.
- [x] Stop owned services, check ports, remove only recorded owned databases, and prepare final report/evidence before the authorized local document-only DCO commit. No push.

## Surprises & Discoveries


Main tidb-expr has no direct small-vector dependency. Modern paths already carry inline40-byte capability identities. The selected issue is Vec growth by exactly one slot per callback, rather than per-handle heap payloads. Legacy NativeTask.values is a separate fallback. SQL owners often must remain available across callbacks, so cloning cannot safely be replaced with a lease without checking each contract.

The first test build fails on three incorrect import paths in the new test. Its receipt, log and initial draft are preserved in before-build-import-error. Fix only those test imports, rebuild, and then observe the actual original capacity assertion fail. The compile failure is not the regression's before-failing proof.

Adding exactly one test changes libtest's filtered-out summary2439 to2440. A checker that previously allowed only PID/time differences initially rejects this structural difference. Independently list both actual unit binaries: old2440 tests, new2441, old names are a subset and the only added name is the new capacity test. Validate filtered counts against those totals before normalizing only that summary field. The assertion body then matches exactly; no old golden digest is changed. Raw bodies and known-failure-test-count-proof.json preserve the reasoning.

Initial strict preflight differs only in12 estimated-row columns2730.67 versus3333.33. Results, fields, warnings and every other EXPLAIN column match. Stop before timing and archive all45 initial observations and their column proof. Restart owned storage and repeat the same strict comparison; all45 match before measurement. No plan normalization or relaxed rule is used. The precise transient cause remains unknown.

## Decision Log


- Decision: Try checked remaining-arity reservation with the existing fallible Vec API.
  Rationale: The demanded arity and next index are already checked. PB leading NULL still retains only its one observed token and stops; reserving metadata must not evaluate another child.
  Date/Author:2026-11-06 / ExpressionReuseEngineer.

- Decision: Reject and restore the candidate, including its implementation-specific capacity test.
  Rationale: Text/JSON CPU medians change−0.7%/+0.3% with ranges crossing1. Conditional CPU rises1.5% in every round; single-row JSON CPU rises0.9% in every round. JSON latency median rises2.5%. No reliable target-workload gain supports keeping the change or its additional test burden.
  Date/Author:2026-11-06 / ExpressionReuseEngineer.

## Outcomes & Retrospective


The candidate passed its new functional/capacity checks and preserved existing semantics. Nevertheless,180 cells and172618 queries do not support retention. Most CPU ranges cross1. Text latency median falls1.2%, but JSON rises2.5%; do not select only favorable metrics. The experiment does not establish the underlying cause of the unfavorable CPU changes.

Restore batch_hosts.rs, batch_eager_values.rs and batch_pb_eager_values.rs exactly to their initial c23d56b contents. Check every current Rust/config file against the retained round6 build snapshot. SDK stays f7672bf. The actual retained frontend remains round6 e17; round7 1ffd is archived and rejected. New retained production/test Rust changes are0/0. Cumulative net reductions remain5066 production and192 test lines, including comments and blanks. The withdrawn candidate had2 added/2 deleted production lines and183 added test lines; those are trial evidence, not retained code.

The original-native goal remains unmet. In this experiment's own balanced cohort, the retained round6 CPU is1.624/2.939/4.313 times original for4096-row integer/nested/JSON, and5.585 for depth16. These ratios compare the actually retained baseline with original, not the withdrawn candidate. Do not compound previous report percentages or infer a historical regression from different cohorts.

## Context and Orientation


Main checkout /home/agent/tidb/expression-unification/tidb starts at c23d56b0fa1030f6b548803a87cbc61a044b8e66. SDK sibling ../tikv stays at f7672bf980143f2938fd0f900650a4c29091e70f. The production trial is in rust/crates/tidb-expr/src/evaluator/batch_eager_values.rs::resume_eager_values and batch_pb_eager_values.rs::resume_pb_eager_values. The new candidate test is inside batch_hosts.rs::tests. None of these Rust changes is submitted.

NativeRuntime handles are opaque capabilities naming an authenticated producer, role, generation and occurrence. They carry native SQL owners indirectly through NativeValueStore. Eager host tasks collect these handles before invoking the original value kernel; changing buffer capacity must not change which children run or when the real context receives callbacks.

The unrelated untracked rust/third_party/tikv-client-rs/tests/client_go_differential/BUILD.bazel must never be edited, staged or deleted. Its SHA remains cc9fb078591d7c94366d1e6f504ff2bade23ccb57a9313f9c7235807ff658d52.

## Plan of Work


The archived native eager trial replaces try_reserve_exact(1) with try_reserve_exact(arity-next), using the already checked demanded arity. PB eager uses try_reserve_exact(if is_null {1} else {arity-next}); its existing NULL and metadata checks run first. All other validation, kernels, ownership and callbacks remain in place.

The archived test drives actual start/resume with authentic stored values. Native and PB calls use atan2 with two arguments and substring with three. After the first non-stopping callback, require enough capacity for the complete demand; subsequent prefixes must not change capacity. Check requested ordinal order, actual result, PB leading NULL stopping after one read, and cancellation. The original implementation fails the first capacity assertion; the candidate completes all five scenarios.

After the measured rejection, copy only the three initial file backups back, first proving each backup equals its initial HEAD blob. Preserve main-trial.diff, all three tested trial source files, both immutable unit binaries and both test receipts. Compare final source hashes with the previously validated round6 inputs. The final candidate-specific test must not remain as a failing test against the restored implementation.

## Concrete Steps


Agent workspace is /home/agent/.slock/agents/02fef0a9-76ce-4afe-9bce-73eebe82ff7d. Evidence root is evidence/remove-demo-policy/performance/followon-argument-capacity. Exact commands, cwd, environment, logs and actual child exits are recorded in job JSON and receipt JSON.

Completed orchestration used:

    python3 evidence/remove-demo-policy/resume-nov05/job_runner.py evidence/remove-demo-policy/performance/followon-argument-capacity/front-before-build-2.json
    python3 evidence/remove-demo-policy/performance/followon-argument-capacity/before_probe.py
    python3 evidence/remove-demo-policy/performance/followon-argument-capacity/gates.py
    python3 evidence/remove-demo-policy/performance/followon-argument-capacity/run_comparison.py

Main builds use ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j1 -p tidb-expr --lib --no-run --message-format=json with empty RUSTFLAGS. The actual server build uses the same wrapper/profile with -p tidb-server --bin tidb-server --no-default-features. Unit binaries are preserved before execution. Do not mistake an outer wrapper0 for a successful child test101.

Do not rerun completed controllers over immutable outputs. Reproduction needs a new evidence directory, input snapshots and owned loopback services. The tested source is available in main-trial.diff and the archived .trial.rs files, rather than the restored final checkout.

## Validation and Acceptance


Candidate Main974 cases comprise the original973-name selection plus the one new capacity/order/NULL test:973 pass,1 inherited common-summary failure, no timeout or missing terminal event. Full PB eager actual phases and lazy actual physical corpora remain included. The exact comparison allows only PID/time and the narrowly proven libtest filtered-count increase; the assertion payload stays identical. The full assertion and actual old/new name sets support that exception. No golden digest is rebased.

SDK source/HEAD/status match the previously retained f7672bf inputs. Its earlier1159pass/1 inherited STR_TO_DATE failure/1ignore and171 clippy messages are retained provenance, not newly executed SDK tests or clippy in this Main-only experiment. The new candidate passes root make lint, fixed Jan30/Aug22 rustfmt on the three files with skip_children=true, and git diff --check. No new complete2441-case frontend sweep or true allocation-failure injection is claimed.

Use the same fifteen-scenario matrix and four balanced orders for actual round6 baseline, round7 candidate and original99a, sharing backend355. Every timed cell runs at least2 seconds and30 full-fetch SQL queries. Strict45 preflight observations compare results, fields, warnings and complete EXPLAIN. Each cell verifies results/fields/warnings before and after timing. All180 cells and172618 queries audit successfully, with9521 source/config files fixed and no compiler/test/perf overlapping timing.

CPU is whole-frontend CPU ms/query, excluding backend and client. Latency includes planning, RPC, encoding and full client fetch. Report paired-round medians and observed ranges, not confidence intervals. The rejected ELF is1ffd1d5752982a5b4caa597e5e7ad595cefb205c377cec8b0323bacd7b996395. The retained ELF is e17bfb3553535fc478b7cd9576f3658498611463fcfcf7561e159aba278124ee.

## Idempotence and Recovery


Only stop recorded experiment-owned processes after PID/birth checks. Preserve all previous binaries, reports and golds. Keep failed preflight observations before strict retry; never normalize plans or start timing on a failed preflight. All owned services are stopped, ports free, and only recorded PD/TiKV data directories are deleted after command data-dir and dead-process proof.

Restore only the three trial Rust files, not the protected BUILD or another person's changes. SDK stays untouched. Final verification binds restored source to the retained round6 build, rather than claiming it matches the rejected round7 source. Only the experiment document is locally DCO committed after the final report. No push or deployment.

## Artifacts and Notes


The evidence includes before/after test proofs, the initial import-error build, trial source/diff and input hashes, every selected consumer result, precise framework-count normalization proof, all actual binary pins, raw timed queries and four-round summaries, failed/retried preflight, restore proof and cleanup proof. The report is generated before the local commit. The compact archive excludes large actual ELF and deleted databases; actual binaries remain local. Postcommit-verification.json records the final document commit, unchanged SDK, protected-file hash, restored-source match to retained round6, binary pins and service cleanup.

## Interfaces and Dependencies


The trial uses the existing NativeBatchHosts, SqlDemand Vec and LocalHostServices protocol. There is no new public API, evaluator, kernel, dependency, quota or authentication change. No trial Rust implementation is retained.
