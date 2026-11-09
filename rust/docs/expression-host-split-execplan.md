# Independently measure shared host boundaries and authenticated read removal

This ExecPlan follows repository-root PLANS.md. It records completed independent trials, their rejected implementations, final independent review, restoration and report delivery. Keep Progress, Surprises & Discoveries, Decision Log, and Outcomes & Retrospective current.

## Purpose / Big Picture


The requested outcome was simpler expression host code without making SQL slower. NativeBatchHosts in rust/crates/tidb-expr/src/evaluator connects TiDB values and function-specific operators to the existing SDK execution loop. This work tested two small simplifications separately after their combined predecessor failed performance acceptance. Neither independent candidate established the required no-regression result, so neither remains in production source. This trial adds no retained performance improvement or source-line reduction. The broader path-unification and original-native performance goals remain unmet.

## Progress


- [x] Preserve retained source, SDK, native source, protected unrelated file, actual binaries, and complete input manifests.
- [x] Isolate the six-file common-boundary candidate: 92 production lines added, 121 removed, net29; tests unchanged.
- [x] Prove all9521 source/configuration inputs match its historical actual build; explicitly inherit behavior and quality evidence instead of claiming new execution.
- [x] Independently build and validate the five-file authenticated-read candidate: eight adjacent unused get statements, 0 added and32 removed; tests unchanged.
- [x] Finish that candidate's57 strict observations and228 balanced cells,181127 full queries; reject, restore and independently review it.
- [x] Activate only the29-line candidate and finish45 strict observations and180 cells,167714 full queries; audit and clean owned services/data.
- [x] Honor YangKeao's November9 instruction15fe96e3: supplemental timing only after November10 midnight Asia/Shanghai.
- [x] At November10 00:51 local, recheck actual clock, latest directive, exact inputs/ELFs and actual quiet; separately finish15 strict observations and60 cells,10519 queries, for CAST/IN/INTERVAL/GREATEST plus scan.
- [x] Reject the29-line candidate after both windows; restore only its six owned source files and verify full inputs/HEAD/status/protected file equal the pretrial retained manifest.
- [x] Preserve all favorable and unfavorable results and prepare the joint detailed report with180+60 separate windows and the independent32-line trial.
- [x] Read-only final review1f8c3d67 recomputed all240 A-window cells/178233 queries, strict comparisons, actual after-midnight runner start, ELF/source bindings, cleanup and9521-input restoration; incorporated it and corrected the stale reception clock field.
- [x] Deliver the final joint report and statistics in message82b11b18 before any documentation commit; seal the immutable new archivef86cb707, SHA256d8ae059666eec07ddefd4c08b1c1701c3dee802a80ec8eb7476c6969392782b0.

The authorized local documentation commit is recorded by a separate postcommit receipt after this document is committed. Its SHA is deliberately outside the sealed precommit archive. No push or deployment.

## Surprises & Discoveries


The common-boundary candidate's supplementary GREATEST CPU/query fell3.45% in paired median (all four ratios below1), and INTERVAL fell0.76% (all four below1). These benefits did not establish no regression across required cases: the base JSON vectorization-off p50 rose2.64%, all four rounds higher. Most base CPU ranges crossed1. Base scan CPU ratios were1.2700,1.0714,0.9589,0.9727; supplemental scan median rose5.85%, range-1.18% to+8.05%. Keep this control variability, not just favorable cases. Neither control noise nor unchanged SQL results proves a causal source-code explanation.

The independent32-line candidate raised deep16/width8/text CPU by3.91%/3.95%/2.27%, all four rounds higher; text p50 rose3.05%, also all four. It was restored before the29-line source was activated.

Initial strict preflight differed only in estRows2730.67 versus3333.33, affecting12 base cases,5 supplemental cases and16 read-removal cases. Each complete failing observation set and column-by-column proof was archived before any timing. Restarting only owned storage and repeating the exact original comparison passed. The transient cause is unproven; comparison was never relaxed or normalized.

Shared desktop activity repeatedly failed actual quiet checks. Neither the old November6 daytime announcement nor midnight alone was considered proof of machine quiet. The final supplemental run began after the explicit time boundary and a fresh16-second successful observation. Known build/test/perf-name sampling during all timing windows found no events, but cannot exclude every external activity.

## Decision Log


- Decision: Test the29-line boundary helper and32-line read removal separately from retained round6, never assume an individually passing part validates their rejected combination.
  Rationale: The earlier combined61-line trial had adverse performance evidence.
  Date/Author: 2026-11-09 / ExpressionReuseEngineer, supervisor77e1dfb1.
- Decision: Reject the independent32-line trial and restore five files before activating the29-line trial.
  Rationale: Repeatable unfavorable deep/wide/text CPU and text latency; strict SQL equivalence alone is insufficient.
  Date/Author: 2026-11-09 / ExpressionReuseEngineer; read-only reviewedf73ca5.
- Decision: Add a separate five-case supplemental window without altering the running180-cell scripts.
  Rationale: Supervisor3f3607bb requested direct CAST, IN, INTERVAL and GREATEST coverage. Scan is repeated as a control. Separate windows must not be pooled into a fictitious19-case228-cell cohort.
  Date/Author: 2026-11-09 / ExpressionReuseEngineer; ACK4d97e27e.
- Decision: Reschedule the same persistent reminder to November10 00:01 Asia/Shanghai and enforce a not-before timestamp at the supplemental entry.
  Rationale: Explicit human directive15fe96e3 required supplementary performance after midnight. Later actual quiet remains necessary.
  Date/Author: 2026-11-09 / YangKeao; propagated4f834e7f and received5aaeb279.
- Decision: Reject the29-line candidate after completing both windows; keep its observed GREATEST/INTERVAL benefits in the report.
  Rationale: No-loss acceptance was not established; vectorization-off JSON p50 increased in all four base rounds and control noise prevents broad causal claims. There is no basis to count the provisional29 lines as retained.
  Date/Author: 2026-11-10 / ExpressionReuseEngineer; decision.json and restoration.json.

## Outcomes & Retrospective


Both independent production trials were restored. No test was added, removed or changed by these candidates. Cumulative retained changes from the earlier Demo-policy removal and accepted follow-ups remain production2077 added/7143 removed, net5066 fewer lines; tests5492 added/5684 removed, net192 fewer. Gross counts sum retained change churn and can count a subsequently edited line again; net changes telescope. Exclude docs, configuration, JSON/TSV, rejected experiments and earlier test-evaluator retirement.

This trial's retained source and tests are both0. Do not claim the original-native performance target or path-unification goal has been achieved. Fewer lines and strict correctness checks did not prove the required performance behavior. Source snapshots and actual ELF identity made restoration and independent review possible. Detailed joint evidence reports favorable and unfavorable effects, without attributing full SQL cost to a specific kernel or erased check.

## Context and Orientation


Main repository: /home/agent/tidb/expression-unification/tidb. Pretrial HEAD1cd4092753fd0a8042a86ea5cd79623eace003c0. SDK sibling tikv HEADf7672bf980143f2938fd0f900650a4c29091e70f remains clean. Frozen original /home/agent/tidb/bench-original-native/tidb is read-only, including its preexisting benchmark test changes. All repositories use their existing expression-unification branches.

The29-line candidate touched evaluator/batch_hosts.rs, batch_arithmetic_window.rs, batch_unary_window.rs, batch_decimal_fast_window.rs, batch_typed_window.rs and batch_numeric_window.rs under rust/crates/tidb-expr/src. It shared complete signature comparisons, nonempty and overflow-safe window coordinates, and the same fallible row copy/task installation using a statically dispatched FnOnce. CheckedDecimal retained its original task arity2. Function-specific checks, allocation order, NULL stopping, child-demand replay, warning/error and callback phases stayed intact.

The32-line candidate touched batch_hosts.rs, batch_typed_window.rs, batch_in.rs, batch_interval.rs and batch_prefix_values.rs. Exactly eight unused get statements immediately preceded the same-store/token/role/origin clone_for_kernel. That existing method independently authenticates the entry and requires StoredValue::Sql before copying; no intervening callback or allocation existed. All window, preallocation, NULL/kind and postcallback checks remained. The authentication contract itself was unchanged.

Neither experiment changed SDK task protocols, observability, interpreter, dependencies, limits or diagnostics. Diagnostic core was already shared. Warning-free SQL is not evidence of executing overflow diagnostics; no cost attribution to that path is claimed.

## Plan of Work


The implementation and measurement phases are complete. Preserve candidates and raw data rather than reopening them. The only repository change to retain is this result document after actual report delivery. Read-only final review1f8c3d67 passed and was incorporated. Joint report5d73fe0e, statisticsd841dc46 and immutable archivef86cb707 were delivered in82b11b18 before any commit. This document is the sole repository deliverable; record its authorized local DCO commit separately without changing the archive. Task15 can enter review for this trial's concrete result; broader performance goals remain unmet. Human approval is needed to mark the task done, not to make already authorized reversible local progress.

## Concrete Steps


Evidence workspace: /home/agent/.slock/agents/02fef0a9-76ce-4afe-9bce-73eebe82ff7d. Under evidence/path-unification, common-boundaries-isolated contains the base180, common-boundaries-supplement contains60, and read-dedup-isolated contains228. Actual commands, all already completed:

    python3 evidence/remove-demo-policy/resume-nov05/job_runner.py evidence/path-unification/common-boundaries-isolated/night-isolated-boundaries.job.json
    python3 evidence/remove-demo-policy/resume-nov05/job_runner.py evidence/path-unification/common-boundaries-supplement/night-boundaries-supplement.job.json
    python3 evidence/remove-demo-policy/resume-nov05/job_runner.py evidence/path-unification/read-dedup-isolated/night-isolated-reads.job.json

The actual child receipts are night-isolated-boundaries.receipt.json, night-boundaries-supplement.receipt.json and night-isolated-reads.receipt.json, with returncode0 and485.26/237.34/581.69 seconds. The outer job runner's exit alone is insufficient. Audit-final, preflight-history, cleanup and timing-interference-watch files record actual evidence. Do not rerun completed controllers or overwrite primary.jsonl/preflight.json. Candidate-worktree guards will correctly fail after source restoration; review saved manifests and original measurements instead.

## Validation and Acceptance


Base45, supplement15 and read-removal57 final strict observations compare full results, fields, warnings and EXPLAIN. Every timed cell has at least2 seconds and30 full-fetch queries, no maximum query cap; four balanced rounds, three actual frontends. Each cell warms and rechecks the exact result/field/warning reference. Frontend cores2-5, TiKV6-7, PD8-9, client10. CPU is whole SQL frontend process milliseconds per full query; latency includes execution/full fetch/client decode. Four observed paired ranges are not confidence intervals. The fixture has8192 rows; ordinary queries return4096, with explicit one-row controls. JSON uses actual computed expressions, not unsupported stored-JSON reads.

Boundary-only behavior is inherited, explicitly not newly executed: all9521 inputs match historical phase1 build; actual unit80d02f666bc83d214e50e0ccfb4cf6cc8e0d9bf021455ed9ce4b836a846fadab;973 consumers972 passed/1 identical inherited common failure after only PID/time normalization; same2440 names, root make lint and Jan30/Aug22 fmt/diff checks passed historically. Saved full old receipts/logs and reused-functional-provenance.json explain inheritance.

Read-removal behavior was newly executed: actual unit1f822cdc98df785434b8e886c9e6274c2a665455e89ece8a608f83cceb22cffd, same2440 names,973 consumers972 passed/1 same inherited failure, no timeout/missing terminal, new actual service/root lint/dual fmt/diff checks. Unchanged SDK's1159 passed/1 STR_TO_DATE failure/1 ignored and171 clippy messages are inherited, not newly run. Neither candidate has a new full2440 execution or real allocation-failure injection. No golden rebasing.

After restoration the source/configuration bytes equal already validated retained inputs. No new build or behavior rerun is claimed. Documentation-only completion uses diff review and git diff --check; no new production source remains to justify another expensive sweep.

## Idempotence and Recovery


Both restorations verified current owned candidate bytes before writing the saved retained bytes and then checked full input/HEAD/status manifests. Main now has no modified Rust source. The untracked unrelated rust/third_party/tikv-client-rs/tests/client_go_differential/BUILD.bazel is protected, SHA256cc9fb078591d7c94366d1e6f504ff2bade23ccb57a9313f9c7235807ff658d52. Never edit, delete or stage it. Before the documentation commit, only this plan and that unrelated file are untracked; index is empty. Stage only this plan.

All owned services were stopped after PID/birth verification. Owned temporary pd-data/tikv-data were removed only after matching recorded command data-dir and confirming processes dead/ports free. Do not stop other processes or free ports by taking over their owners. No new services or reminders are needed when final delivery is complete; cancel the completed trial's own reminder rather than leaving a stale wake.

## Artifacts and Notes


Joint report: evidence/path-unification/task15-isolated-final/REPORT.zh.md; all statistics and decisions in statistics-and-decisions.json. Base/supplement180+60 are separate windows,19 distinct cases with repeated scan, not one228-cell cohort. Independent32 has its own19 cases/228 cells. Keep original source manifests, candidate files, actual receipt logs and every failed initial preflight. Actual large binaries remain pinned at their saved paths, not substituted with the mutable build target.

Retained round6 service SHA256e17bfb3553535fc478b7cd9576f3658498611463fcfcf7561e159aba278124ee;29-line round9e6142453431d7a28fbe90ef0690c8bf962b6def29d5e224cd7d9f9daa682b609; rejected32-line round112b1ccd9c3104f9b8046b7eb64f0bfd050829acb6aacaa516fe2ddbb86d90ff07; original native99a4c3e566774ba9733bc9d54366cf1e9b85446f63fa9a7418bfef5d3daeddd2; shared TiKV355be0acec593a24bb43ac0f5e6b8e30aae354b7e63fdd3530f3f2895c41e029. Mutable target-tidb/release/tidb-server still contains rejected round11 and is not an accepted binary.

TiDBEngineer reviewedf73ca5 checked32-line results/restoration; b342457b checked all base180 statistics/inputs/ELFs/cleanup and supplement design; final review1f8c3d67 recomputed both A windows (240 cells/178233 queries), supplemental strict comparisons, actual midnight compliance, ELF/source bindings, cleanup and9521-input restoration, and supports conservative rejection. All are read-only, no new test or performance executions. Reception retained an old pre-midnight clock string, corrected without altering runner receipts; actual supplemental start1794243111.803 was Nov10 00:51:51.803 Asia/Shanghai.

## Interfaces and Dependencies


HostInvocation, HostWindowInvocation, HostCatalog, NativeBatchHosts, SqlDemand and the SDK execution loop remain the existing retained implementations. No production API, field, dependency or validation contract changed in the final tree. Local DCO author YangKeao / yangkeao@chunibyo.icu is already authorized; stage only this document after report delivery. No push, PR or deployment is authorized.

2026-11-10: rewrote the living plan around completed independent trials, rejected candidates, full restoration and explicit remaining review/report/archive/documentation steps. The previous working checkpoints remain in the immutable evidence workspace and original delivered artifacts.

Final report and raw evidence delivered82b11b18 before the documentation commit. Archive contains the dated precommit plan snapshot; this plan now records delivery. Keep the archive immutable and write any postcommit receipt separately.
