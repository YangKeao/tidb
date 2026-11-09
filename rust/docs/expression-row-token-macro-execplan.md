# Share native token validation statements without adding runtime instructions

This ExecPlan follows root PLANS.md and records task18, continuing the authorized expression-reuse simplification. It is an independent candidate after the rejected closure and match experiments; their evidence and source restorations remain unchanged.

## Purpose / Big Picture

Fifty branches of NativeBatchHosts::resume_sql repeat the same native-byte let-else validation with different error literals. A function-local native_token! statement macro shares that source while expanding the original statement into each original branch. It preserves the three task reads, branch order, calls and arguments, error returns, special routes, NULL fallback, origin/generation authentication and callback behavior. It introduces no runtime closure, public interface or dependency.

The result is source simplification, not an established new speedup. Production Rust changes are57 additions/150 deletions, net93 fewer lines; original tests are unchanged. Retained cumulative production additions2134/deletions7293 yield5159 fewer lines; tests remain5492/5684, net192 fewer. Documents, configuration and rejected trials are excluded. Gross sequential churn may count a line changed again later; net reduction telescopes. The broader original-native performance and complete path-unification goal remains unmet.

## Progress

- [x] Claim task18 and announce independent scope; read existing function-local next! idiom in evaluator/batch_ast_value.rs.
- [x] Preserve retained source and9521 input hashes before mutation; replace only50 exact checks; reverse all invocations and remove macro to restore the entire original file, including unchanged original tests.
- [x] Build new actual unit14; enumerate exactly the original2440 test names; run original973 selected consumers:972 pass,1 identical known failure after PID/time-only normalization. No golden or footer rebase.
- [x] Build new actual service14, root make lint, Jan30/Aug22 formatting, diff checks and fixed source snapshots; all actual exits0.
- [x] Compare actual linked code and allocated sections, including relocation-backed source locations; preserve precise equality boundaries.
- [x] Fresh directive and16-second quiet check after builds and peer large-file reads ended; strict92 first-pass observations, independent four-label23-scene/368-cell window and owned cleanup complete.
- [x] Prepare complete report, statistics and raw evidence for delivery before the already-authorized local DCO submission. Exact delivery/commit identities are recorded separately; no push, PR or deployment.

## Surprises & Discoveries

A macro's invocation-supplied token ident must bind in its caller's branch; the transcriber emits let-else directly without an extra block. Its reply name resolves to the existing definition-site function parameter. Source reversal and successful actual compilation establish different parts of this argument; neither is called a compiler-expanded AST dump.

The new service's whole ELF hash differs from retained e17. Its7435-byte resume_sql at0x42c4690 and entire78,390,112-byte .text section are byte-identical. All SHF_ALLOC sections also have equal addresses, sizes and flags. Their only file-byte differences are the build ID and12 bytes holding six source line numbers. Actual .rela.dyn RELATIVE relocations resolve the same batch_hosts.rs file; column numbers and corresponding source statements match, with each line number decreasing by93. Nonallocated .symtab and.strtab differ too. This comparison does not cover every ELF-header byte or unassigned segment padding, nor ASLR, heap/cache state, scheduling or all other runtime conditions.

The first data-location diagnostic assumed an on-disk direct file pointer and failed before producing a proof. The actual field is relocated; the corrected diagnostic resolves its real addend. Both attempts are preserved. No performance run or production source changed during this diagnostic correction.

Legal tidb_init_chunk_size is at most32. All observations request and assert init32, max1024 or32, and vectorization1 or0; every preflight and timed cell stores actual GET values, with a read-only GET after timing. A batch1024 label describes maximum chunk size, not initial or fixed kernel row count. Prior trials' legacy configuration replays and results are not reused as this window's observations.

## Decision Log

Decision: keep the local statement macro as verified source simplification. Exact source reversal, unchanged behavior/inventory and independently checked compiled code/data support this decision. Do not book observed CPU reductions as a macro speedup or claim a measured change of exactly0%. Date/author:2026-11-10 / ExpressionReuseEngineer.

Decision: use original973 scoped tests and the50-site reversal check instead of importing131 lines from an uncommitted rejected trial. There is no new runtime condition; real compile/hygiene, error/recovery corpus and source equality still required verification. Original2440 enumeration is not2440 executions. SDK and allocation-failure injection were not newly run.

Decision: report every favorable and adverse SQL observation with both retained controls and the original reference. No control subtraction, post-hoc tolerance threshold, confidence-interval claim or single-cause attribution is used. The exact artifact comparison supports source sharing without new execution instructions; whole-SQL timing remains empirical evidence with its own limitations.

## Outcomes & Retrospective

Actual functional controller exited0/1259.30seconds: unit compilation280.93s, original973 runtime415.62s, service557.52s and root lint2.42s. All required formatting/diff/input checks passed. Selected behavior is972 pass/1 inherited common failure; SDK1159 pass/1 failure/1 ignore and171 clippy messages are inherited unchanged-input evidence only. No new full2440, SDK suite or allocation-failure run is claimed.

Performance controller exited0/1599.96seconds with23 scenes,92 first-pass strict comparisons,368 cells and501709 complete-fetch queries. No initial strict retry was necessary. Known build/test/perf watch events were empty. All own PID/birth-checked services stopped, ports were free and only recorded own data directories were removed.

Control-expression CPU C/A and C/B medians decrease2.59%/3.18%, each four rounds in the same direction. CONCAT C/A p50 increases0.64% and QPS decreases0.54%, also four rounds; C/B medians p50+0.57%/QPS-0.63% have ranges crossing zero. Scan CPU C/B+0.53% is four-round adverse, whereas C/A+0.025% crosses zero. STRCMP CPU medians C/A+3.03%/C/B+2.06% are not four-round uniform. All scenes, metrics, ranges and A/B self-controls are retained. Their cause is not established by the macro or environment alone, and they are not omitted because linked instructions match.

The kept outcome is93 fewer production lines with unchanged original tests and linked execution instructions; no newly attributable performance gain is established. Prior rejected candidates remain rejected. Independent source, unit, service/codegen, complete368-cell and31-allocated-section factual reviews passed; none rebuilt or reran tests/performance. Complete report, statistics, immutable raw archive and those independent reviews accompany the local change; final submission receipts are separate from the sealed archive.

## Context and Orientation

Main parent63087338793cd358fc4dcfd0f5276aa277d67033 is task17's document-only result; SDK sibling f7672bf980143f2938fd0f900650a4c29091e70f is clean and unchanged. Only rust/crates/tidb-expr/src/evaluator/batch_hosts.rs changes production code. The new result document is this file. Protected unrelated untracked rust/third_party/tikv-client-rs/tests/client_go_differential/BUILD.bazel remains untouched, SHA256cc9fb078591d7c94366d1e6f504ff2bade23ccb57a9313f9c7235807ff658d52.

validate_row_reply still authenticates task, argument shape and ordinal before dispatch. Native Bytes(Some(token)) is a value capability; wire None differs from SQL NULL within a capability. Existing family methods authenticate origin/generation and preserve callback checks. Raw integer CAST, window rejection and optional CONCAT/REGEXP byte fallback are unchanged.

## Plan of Work

Use the retained file snapshot, define the function-local macro after validation, and replace only exact original let-else blocks. Caller ident and old static error literal are explicit parameters. Save all50 mappings and exact whole-file reversal. Keep source fixed through actual unit, selected runtime, service and quality gates. Pin saved actual binaries, compare real symbol bytes/sections, then execute the declared independent performance window. Final reports precede local DCO submission; no source rebuild or previous trial rerun is needed after these unchanged inputs pass.

## Concrete Steps

From Main, use empty RUSTFLAGS and CARGO_BUILD_JOBS=1:

    ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j1 -p tidb-expr --lib --no-run --message-format=json
    ../tools/cargo-tidb build --manifest-path rust/Cargo.toml --release --locked --offline -j1 -p tidb-server --bin tidb-server --no-default-features
    make lint

Agent evidence is evidence/path-unification/row-token-macro. functional_gates.py and night_macro.py run through job_runner.py with actual child exit/PID/birth receipts. Explicit tests, scripts, source reversal, ELF bytes/disassembly, complete strict/timing data, configuration readbacks, independent reviews and cleanup receipts are preserved. Do not treat a stale running receipt or wrapper exit alone as completion.

## Validation and Acceptance

A=retained e17, B=the same exact retained ELF independently launched, C=actual macro service14, F=original99a; shared TiKV remains355b. Four Williams orders A-C-F-B, C-B-A-F, B-F-C-A, F-A-B-C place every label in each position once, each pair before/after twice, each directed adjacency once. Each of368 cells is at least4seconds and30 full fetches, with no query cap. Process CPU uses POSIX clock_getcpuclockid; API nanosecond resolution is not measurement accuracy. These are whole-SQL execute/fetch observations, not kernel ns/eval; QPS is single-client throughput, not capacity.

Preflight compares exact result encoding, fields, warnings, complete EXPLAIN and effective configuration. Four-round median paired ratios and observed ranges are reported against A/B/F; ranges are not confidence intervals. A/B controls observe startup/time variation, not isolation from every external activity. Fresh quiet and known-process watches are not guarantees against future unknown interference. Loaded code/data equality has the explicit boundaries above; whole ELF/environment identity and universally zero measured loss are not claimed.

## Idempotence and Recovery

Do not modify running source, completed receipts or old archives. Any rejection would restore only the owned source after a candidate-byte guard, then compare full9521 inputs and protected state; this candidate is kept. Keep actual unit14/service14 and retained baseline separately pinned. Stage only this owned Rust file and result plan for already-authorized local DCO YangKeao/yangkeao@chunibyo.icu. Do not stage the protected BUILD, change SDK or push. Keep postcommit receipts separate from the immutable trial archive.

## Artifacts and Notes

Actual unit14 SHA256960326c001495f29a8af8bc2f5e8ceefbef4a56fdf4e5caf22de7e433afaa26a; service14 SHA2562d435bce2ba0ea2c153df91b5e6dbd82a5200d78c20bb09ee3dac8c993d75fea. Retained service e17bfb3553535fc478b7cd9576f3658498611463fcfcf7561e159aba278124ee; original99a4c3e566774ba9733bc9d54366cf1e9b85446f63fa9a7418bfef5d3daeddd2; backend355be0acec593a24bb43ac0f5e6b8e30aae354b7e63fdd3530f3f2895c41e029.

Main text SHA256f1d758df27b29e23d9018b2e381dac10247ae5a440b602da91c6c2e34e013aee; resume_sql bytes SHA256df2ccaa0db56861c617652b3c4cb9e5b4ba51ccaca8eb9577208a2d83fa7bfb6. Original task15/16/17 ZIPs remain immutable; this trial is sealed separately as task18-row-token-macro-final.zip after its final factual review.

## Interfaces and Dependencies

No public signature, dependency, SDK, protocol field, native value store, callback or kernel changes. native_token! exists only inside resume_sql and emits the original let-else into the original branch. All original source/error semantics and tests beyond this exact expansion are unchanged.
