# Reuse checked single-call expression frames

This living ExecPlan follows PLANS.md at the repository root. Keep Progress, Surprises & Discoveries, Decision Log, and Outcomes & Retrospective current. Authorization is YangKeao's overnight optimization instruction6ce6e4da and supervisor8159d2bb in #tidb-expression-reuse:5841a0b2. It covers local experiments and DCO commits, with no push or release.

## Purpose / Big Picture


Reduce expression execution overhead while preserving SQL behavior and the existing host protocol. A program containing one unannotated structured call only forwards that call's result. Reuse its frame for the call after the existing validation, rather than retaining an otherwise empty parent. Here, unannotated means the program has no CheckedResultFlow identity to forward.

The retained direct-branch implementation improves frontend CPU on several substantial SQL workloads. It does not reach the original-native performance target. Report both its gains and small latency regressions, rather than assuming fewer frames imply a universal improvement.

## Progress


- [x] Inspect singleton input normalization and exclude it as a current SQL optimization: the batch and lineage SQL paths already bypass it.
- [x] Extend the existing overflow test; verify the old implementation actually fails its new depth assertion at3 versus2, then preserve overflow and exactly two reads while passing at2.
- [x] Validate and measure the first Option-frame variant, retain its complete evidence, and leave it uncommitted because nested/deep/wide CPU consistently regresses.
- [x] Validate the direct-branch variant through the complete SDK, all973 selected frontend consumers, exact inherited-failure comparison, lint, clippy, both formatters, and real release build.
- [x] Complete independent strict preflight and180 timed cells for each variant; retain the direct form, record tradeoffs and code counts, and clean owned services and database directories.
- [x] Prepare the final report, complete reviewable changes and evidence for authorized local DCO commits. Commit identifiers are recorded afterward in the evidence's postcommit-verification.json. No push.

## Surprises & Discoveries


The remaining singleton Int reconstruction belongs to the legacy row interface. Removing it would not improve the SQL paths under measurement, so it was left unchanged.

The first implementation used Option<EvalFrame> and a common post-dispatch branch. Its4096-row conditional/JSON/text CPU improves8.2%/5.1%/4.4%, but nested integer/depth16/width8 CPU regresses4.4%/5.8%/4.0%. All six CPU directions agree across four rounds. This variant remains uncommitted.

Both experiments initially encounter only12 differences in the pseudo estimated-row plan column:2730.67 versus3333.33. Results, fields, warnings and every other plan column match. No timing starts on these failures. Each complete failed45-observation preflight and its column-by-column proof is preserved. Extra non-timed catalog priming in the second experiment does not prevent the transient. Restarting owned storage and repeating the unchanged strict preflight yields full equality before timing. The precise cause of the transient remains unproven.

## Decision Log


- Decision: Reuse only a single-node program whose flow is None.
  Rationale: Multi-instruction programs need their operand stack; annotated programs need their existing result-identity forwarding. Existing execution checks and host catalog validation run before substitution.
  Date/Author:2026-11-06 / ExpressionReuseEngineer.

- Decision: Keep direct branches for Control, Host and Ordinary scheduling, rather than the first variant's large optional-frame local and common branch.
  Rationale: The first measured tradeoff is unfavorable for deep and wide integer expressions. The direct variant has consistent heavy-workload CPU gains without a consistent nested/deep/wide CPU direction. Large-temporary or code-generation cost is a hypothesis, not a proven causal attribution. Separate cohorts cannot establish a direct percentage delta between variants.
  Date/Author:2026-11-06 / ExpressionReuseEngineer.

## Outcomes & Retrospective


Keep the direct implementation. Against its own current-efa cohort,4096-row conditional/JSON/text frontend CPU falls9.8%/4.7%/2.2%, with SQL latency down4.4%/3.3%/1.2%. Decimal and float CPU falls4.7%/6.4%. These CPU directions agree in all four rounds.

Integer/nested/depth16/width8 CPU medians change by−0.9%/−0.9%/−0.4%/−0.2%, but every range crosses1; no stable gain is claimed. Decimal latency rises1.1%, depth16/width8 latency0.8%/0.5%. Scan CPU rises0.7% with a range crossing1 and latency0.1%; it is an explicit control. Single-row differences are also small with CPU ranges crossing1.

The original-native goal remains unmet: integer/nested/JSON CPU is1.616/2.999/4.341 times original, and depth16 is5.615 times original in this cohort. Do not compound percentages from previous reports. Final production Rust adds19 and deletes6, net+13; tests add9, including3 cfg(test) lines outside the inline test module. Cumulative reductions from Demo-policy removal are5066 production and192 test lines, counting comments and blanks. Prior23548 JSON/TSV data lines remain separate.

## Context and Orientation


Main checkout is /home/agent/tidb/expression-unification/tidb at9d6fc86053b2b262ab08a2955a45030a248d5171 before this experiment. SDK sibling ../tikv starts at317b6a50c6f7d107729b99104166af820f5fc417. Only SDK components/tidb_query_expr/src/types/expr_eval.rs changes production code.

ProgramFrame sequences RPN instructions. HostFrame, OrdinaryFrame and ControlFrame implement existing structured calls. In eval_frames, check_execution runs before the instruction is scheduled. For one unannotated structured call, the program's stack starts empty and its only remaining action would be forwarding the child's unannotated result. Replacing that active frame preserves the call's own validation, host identity, requested arguments, cleanup, and error handling. Annotated or multi-instruction programs still push a child and forward normally.

Main's unrelated untracked rust/third_party/tikv-client-rs/tests/client_go_differential/BUILD.bazel must never be edited, staged or deleted. Its SHA iscc9fb078591d7c94366d1e6f504ff2bade23ccb57a9313f9c7235807ff658d52.

## Plan of Work


The retained implementation computes reuse_program from frame.nodes.len()==1 and frame.flow.is_none(). Each existing structured-call branch constructs its original call frame after its original checks. If reusable, replace the active Program frame; otherwise set pushing_child and invoke the existing push_frame path. Ordinary non-structured RPN instructions keep their original dispatch path, without the first variant's extra common optional-frame branch.

Preserve the distinction between a host's own error and a child-allocation error caught by its waiting parent. Do not change native algorithms, public APIs, provider callbacks, producer/role/generation checks, field types, result provenance, or experimental observations. The added maximum-depth observation exists only in test builds.

## Concrete Steps


From the agent workspace /home/agent/.slock/agents/02fef0a9-76ce-4afe-9bce-73eebe82ff7d, first evidence is evidence/remove-demo-policy/performance/followon-forwarding and final evidence is its sibling followon-forwarding-direct. Both hold exact job JSON, commands, environments, logs, receipts, SQL, data and immutable binary pins. The original before-failing regression proof and binary are preserved in the first directory; the identical assertion and instrumentation are reused by the second.

The completed orchestration used:

    python3 evidence/remove-demo-policy/resume-nov05/job_runner.py evidence/remove-demo-policy/performance/followon-forwarding-direct/sdk-build1.json
    python3 evidence/remove-demo-policy/performance/followon-forwarding-direct/gates.py
    python3 evidence/remove-demo-policy/performance/followon-forwarding-direct/run_comparison.py

The SDK build uses ../tools/cargo-tikv test --release --locked --offline -j1 -p tidb_query_expr --lib --no-run --message-format=json from the SDK checkout. Main uses ../tools/cargo-tidb with --manifest-path rust/Cargo.toml, the same release/locked/offline/-j1 flags and the tidb-expr library or tidb-server binary target. RUSTFLAGS is empty. Unit binaries are copied before execution. Actual child exits are in receipt JSON; a wrapper returning0 never turns the inherited test failure101 into success.

Do not rerun these completed controllers over their immutable outputs. A reproduction needs a new evidence directory, fresh input snapshots and new owned services. The report lists every full command, including project clippy flags and root make lint.

## Validation and Acceptance


The final SDK runs1161 cases:1159 pass,1 inherited STR_TO_DATE failure,1 ignored. The frontend consumer list runs973 cases:972 pass,1 inherited common-summary failure, no timeout or missing terminal event. Both failure bodies match the earlier baseline after PID/time normalization only. Never alter historical golds to claim success. Complete PB eager actual phases and lazy actual physical corpora remain included, along with borrowed selection, stale/foreign identity, callbacks, exact errors, cleanup and recovery cases.

Project clippy, root make lint, Jan30 and Aug22 fixed rustfmt checks, and git diff --check pass. Clippy reports171 warnings, the same171 messages as the prior retained version. No new2440-case full frontend sweep or true allocation-failure injection is claimed.

Each experiment independently compares actual current-efa, candidate and original99a release endpoints, sharing the unchanged355 TiKV. Fifteen SQL scenarios span4096-row scan, integer/nested, decimal/float, text/JSON/control; single-row integer/JSON/text; depth16/width8; and JSON batch32/vectorization-off. Four balanced rounds produce180 cells, each at least2 seconds and30 full-fetch queries. First variant172861 queries; final173050. Strict45 preflight observations match full results, metadata, warnings and EXPLAIN; every timed cell checks results/fields/warnings before and afterward. No compiler, test or perf overlaps timing. Source and configuration hashes match9521 inputs throughout.

Frontend CPU is the whole frontend process in CPU ms/query, not pure function ns/eval and not backend/client CPU. SQL latency includes planning, RPC, encoding and full client fetch. Report paired-round medians and observed ranges; these ranges are not confidence intervals. The actual final ELF is e17bfb3553535fc478b7cd9576f3658498611463fcfcf7561e159aba278124ee; first candidate ead33b4f141ee36605ea23447011e8795209886b64d3f7cd1b5fd4cc0ab58c91 remains provisional.

## Idempotence and Recovery


Only stop recorded experiment-owned loopback services after checking PID and birth identity. Preserve previous binaries, reports and goldens. Failed preflights are archived rather than overwritten or normalized. No timing begins until the unchanged full strict comparison succeeds. Both experiments' owned services are stopped and ports free. Only their recorded PD/TiKV database directories are removed, after checking command data-dir ownership and dead process identity.

If the retained tradeoff is later rejected, revert only this experiment's SDK commit; its complete report and actual binaries remain available. No remote write, push or deployment is authorized here.

## Artifacts and Notes


The final report is generated before the local DCO commits. The final compact package includes all final raw timing, exact tests and gates, code counts, source diffs, initial preflight diagnostics, the first provisional report and its entire evidence ZIP. Large actual ELF files remain local; database directories were intentionally cleaned. Postcommit-verification.json records signed commit identities, clean expected statuses, unchanged compiled-input hashes, report-before-commit chronology, binary pins and service cleanup.

## Interfaces and Dependencies


Use only the existing EvalFrame variants and SmallVec. There is no new evaluator, native fallback, kernel, dependency, public API or quota. Keep catalog, type, shape, namespace, generation, producer role, callback and result-identity checks in their existing owners.
