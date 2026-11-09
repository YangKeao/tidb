# Unify row host reply dispatch without changing capability or SQL semantics

This living ExecPlan follows repository-root PLANS.md. Keep Progress, Surprises & Discoveries, Decision Log and Outcomes & Retrospective current.

## Purpose / Big Picture


YangKeao6129cd47 requests simpler shared expression paths without performance loss. Supervisor3159f388 requires continuing from retained code after the independently rejected window-boundary and adjacent-read trials. This candidate changes a different boundary: NativeBatchHosts::resume_sql in rust/crates/tidb-expr/src/evaluator/batch_hosts.rs. Replace the long sequence of repeated task matches and native-token extraction with one task match and one local checked extraction implementation. Preserve exact per-family errors, typed numeric and window rejection routes, raw integer CAST, optional wire-NULL CONCAT/REGEXP handling and every existing operator callback/authentication operation. No SDK, native value store, task field or kernel changes.

## Progress


- [x] Read original human6129cd47 and supervisor3159f388, claimed new task16a5121028; task15 remains reviewed completed experiment with immutable archive.
- [x] Identify common row reply dispatcher, callers, demand variants and exceptional argument representations; announce scope7a7dd2e1.
- [x] Preserve retained inputs and actual baseline pins; implement only row reply routing and one12-family invalid-reply/recovery test, production net100 fewer/test131 more.
- [x] Build actual unit, verify original2440 test inventory plus exactly the new test, run selected consumer974 tests and preserve inherited failure unchanged; build a new actual service and required quality checks.
- [x] After real build/test terminal and fresh directive/actual quiet check, run strict SQL results/fields/warnings/full-EXPLAIN then four balanced rounds against retained round6 and original native. Include common controls and direct CAST/IN/INTERVAL/GREATEST/LOWER/UPPER/CONCAT/STRCMP/REGEXP dispatch cases.
- [x] Review all276 cells/189975queries, reject and restore the one candidate file; all9521inputs, heads/status/protected file match retained inputs. Own services/data cleaned.
- [x] Deliver full report/statistics (db5a06b4) before local commit, independent full statistics/restoration review9dbf7ebd PASS; seal raw archive and record only this result document in the already authorized local DCO step, no push. Commit receipt is preserved separately after the immutable archive.

## Surprises & Discoveries


The retained dispatcher repeats identical Bytes(Some(token)) extraction and per-family error construction across dozens of arms. Integer character CAST receives Int instead, while CONCAT and original REGEXP accept nullable byte values. Numeric parent receives the original reply and window-only tasks reject row replies. These exceptions must not be coerced through the common native-token path.

## Decision Log


- Decision: One enum match and local checked token extraction, only inside resume_sql, instead of moving window preparation helpers or deleting authenticated value reads.
  Rationale: A distinct repeated entry path; no indirect function pointer dispatch, new dependency or interpreter. Shared extraction preserves invalid-reply diagnostics and each operator's validation.
  Date/Author: 2026-11-10 / ExpressionReuseEngineer.
- Decision: Keep task15 and earlier rejected source trials untouched, and use retained round6 actual binary for comparison.
  Rationale: Rejected candidates and mutable target binaries are not accepted baselines.
  Date/Author: 2026-11-10 / ExpressionReuseEngineer.

## Outcomes & Retrospective


The distinct row reply dispatcher was implemented and independently behavior-checked, then rejected after23scene/69strict/276balancedcells/189975completequeries. LOWER/UPPER CPU/p50 improved3.20%/2.78% in all four rounds, but STRCMP and REGEXP_LIKE p50 increased5.78%/4.26%, also all four; scanCPU increased8.99% with3.70–15.97%range. These do not establish causation for every change or no regression. The one source file, including131newtestlines, is restored. This trial adds zero retained code reduction or speedup; cumulative retained production5066/test192 net fewer lines. Broader goal remains unmet. No repeat of rejected helper/read candidates or report-wide completion claim.

## Context and Orientation


Main /home/agent/tidb/expression-unification/tidb HEAD5506e0c1a386fd77b2c7fb2a75da34fac0cb5bcc, SDK sibling tikv HEADf7672bf980143f2938fd0f900650a4c29091e70f clean, frozen original /home/agent/tidb/bench-original-native/tidb read-only with preexisting test changes. A row reply carries a borrowed VectorValue plus argument ordinal/type; validate_row_reply authenticates demanded argument shape and task identity before dispatch. A native capability is a Bytes(Some(token)) value; SQL NULL stays inside that capability and is different from wire-level None. Per-family methods retain origin/generation/callback checks.

## Plan of Work


Preserve original batch_hosts.rs and complete input manifest after this plan path exists. Mechanically map each existing branch to a match arm without changing its callee or error. A local closure extracts one native token only when a native arm calls it. Keep task validation before any extraction. Existing CONCAT/REGEXP fallback remains byte-for-byte apart from its preceding dispatch. Add a table-driven invalid-wire-NULL test over representative simple task demands with exact family errors, wrong-ordinal precedence, task cancellation/generation and provider reuse; keep existing valid-data/callback corpus unchanged. Then build/check actual artifacts and measure in a new evidence directory.

## Concrete Steps


From Main root, empty RUSTFLAGS/CARGO_BUILD_JOBS=1:

    ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j1 -p tidb-expr --lib --no-run --message-format=json
    ../tools/cargo-tidb build --manifest-path rust/Cargo.toml --release --locked --offline -j1 -p tidb-server --bin tidb-server --no-default-features
    make lint

Use the existing durable job_runner.py from agent workspace evidence/remove-demo-policy/resume-nov05 with real child receipts, PID/birth and logs; carry build/test terminals through, never stop merely because a job launched. Actual selected consumer baseline973=972pass/1 exact known common failure; add exactly one new contract test to974. Changed test inventory must equal original names plus new name, not pretend identical. SDK unchanged means prior SDK1159/1/1 and171clippy messages remain inherited, not new executions.

## Validation and Acceptance


Exact error texts and classification for malformed replies, stale tasks, callback failures, NULL/short-circuit and recovery must remain. No golden updates. Use existing shared entry consumer corpus plus the new contract test. Root lint, pinnedJan30/Aug22 rustfmt on changed Rust and git diff --check are required. New actual service must hash-match saved build provenance.

Performance uses retained round6 e17bfb35, candidate new preserved ELF, and original99a4c3e5, same backend355be0ac. At least2seconds AND30 full fetches per cell, four balanced rounds, no cap. Strict result digest, complete field metadata, warnings and full EXPLAIN must match before timing. Initial known estRows-only failure may only be retried after fully preserving evidence and restarting owned storage, never normalizing or relaxing. Report all scenarios, scan/core controls, paired medians and four observed ranges, not confidence intervals. Whole SQL process CPU and full-fetch latency are not kernel ns/eval. Do not infer quiet from midnight; latest directive and fresh actual machine observations are required. Any retained change needs no established performance regression; otherwise restore only owned candidate bytes and verify full inputs.

## Idempotence and Recovery


New evidence directory evidence/path-unification/row-reply-dispatch in agent workspace, separate from all immutable prior trials. Never overwrite existing preflight/primary or completed controllers. Record actual candidate names before builds. The protected unrelated untracked rust/third_party/tikv-client-rs/tests/client_go_differential/BUILD.bazel SHA256cc9fb078591d7c94366d1e6f504ff2bade23ccb57a9313f9c7235807ff658d52 must never be edited/staged/deleted. Stage only owned code/test/plan after report. Stop/clean only recorded PID/birth/command data-dir services; do not touch other work. Time/load future holds need a persistent Raft reminder and fresh evidence, not long polling or obsolete notices.

## Artifacts and Notes


Retained actual service evidence/remove-demo-policy/performance/tidb-server-after-round6 SHA256e17bfb3553535fc478b7cd9576f3658498611463fcfcf7561e159aba278124ee. Mutable target-tidb/release/tidb-server still holds rejected round11; never use it as baseline. Task15 final archive task15-isolated-final-rejected.zip SHA256d8ae059666eec07ddefd4c08b1c1701c3dee802a80ec8eb7476c6969392782b0 is immutable. Root result plan expression-host-split-execplan.md is committed and is not this new plan. Report before any already authorized local DCO commit, seal a new archive and preserve postcommit receipt separately. No push/PR/deployment.

## Interfaces and Dependencies


Only private resume_sql routing changes; HostArgReply, HostTaskId, SqlDemand, existing operator methods and SDK LocalHostServices contract remain. No public API or dependency change. Shared extraction must return borrowed token bytes without owning, cloning or authenticating the underlying native value; existing operator methods still do real value authentication at their required points.

2026-11-10 01:15local checkpoint: mechanical49-arm mapping verified exact errors/callees/arguments; nonnative fallback and validate_row_reply byte-identical. Candidate production214 added314 deleted, net100; one new12-family contract test adds129 testlines. Actual unit build started through persistent functional-dispatch controller, toolsession75154; expected2440originalnames+1 and974scopedconsumers. Peerstaticreviewrequested083c13a4. Sourcefixeduntiltruebuild/test/service/qualityterminals; no performance yet.

The separate performance harness is prepared, not executed:23cases/69strict/276balancedcells. Matrix contains original15 controls,4direct CAST/IN/INTERVAL/GREATEST probes and4row dispatch queries LOWER/UPPER,CONCAT,STRCMP,REGEXP_LIKE. Initial known estRows-only retry expects20 non-single-row cases, preserves all observations and never relaxes comparison. Functional build inputs remain fixed; peer83a0cc19 accepted read-only static review and is pending.

2026-11-10 01:23local correction: binding() returns BindingContract, while ordinal validation returns HostContract. Initial newtest asserted HostContract for both, a test defect; production dispatch was correct. First actualunit build0, directedtest101, consumer143 and controller1 preserved; second unchanged cachedbuild0/consumer143/controller1 resulted from a failed formatted source replacement followed by an unintended shell continuation, not a pass. After allownedtests exited, corrected exactenumassertions and verifiedproductionbyteidentity. Started functional-dispatch3 in a separatecall, toolsession73893, candidate-input3/newunitround12c; originalnames/receiptsretained. Currenttest131added, source100netfewer unchanged. Peerinitialtestdesignapprovalwithdrawnb7de2c7c; production49comparisonremainsvalid, fresh newunit/runtime verificationrequired.

Peer3d90c529 independently reviewed corrected candidate-input3, exact error enums/messages and all9521bindings; production unchanged/net100fewer, test131more. Saved independent-static-review3.json, no compilation/testexecutionbyreviewer. Actualnewunit/runtime/service/quality/performance gates remain required.

Corrected actual unitfront-unit-round12c built withSHA4330e555beb786f0eb5d2986ea599ca65990bf9e169d2a834d101bdfa74a7b91; exactoriginal2440names+onecontracttest verified. The new test actually exited0, runtime3 currently353/974allpass. Continue remaining consumers and actualservice/quality, no performance acceptance yet.

Runtime3 actual common failure has the same assertion body; only the runner footer changed2439->2440 filtered tests due to the verified one new test. Original comparison3 will stop on this expected metadata difference and is preserved. Separate strict checker4 verifies actual2440names+exactnew1, exactfooterdelta and every other output byte after PID/time normalization. Prepared postruntime4 continuation requires full974actualterminal/sourcepins and then executes only service/quality stages; no redundant unit/runtime rerun or golden change.

Actual runtime3 completed974=973pass/1knowncommon in418.12sec, newcontracttest0. Old comparator3 stopped because runner filteredcount+1, actualfailurebodyunchanged; strict checker4 verifiedactual2440+1inventory/exactfooterdelta andallothertextafterPID/time. ResumedONLYservice/quality via functional-postruntime4 (tool74230), reusing samepinnedactualunit/runtime, no repeat974. Actualserver-build1 nowrunning; quality/sourcepins/performance stillpending. Candidatecombinedsource adds31lines because production100fewer/tests131more; reportseparatecounts, notjustproductiondeletion.

2026-11-10 01:45local: actual functional-postruntime4 exited0. Actual service round12 SHA256acd49c7818353c3e1e498bd40ed2dcab9abb87eb6a3daf37ba925c049d83ef64 built558.49sec; rootlint, pinnedJan30/Aug22format, diff and all9521fixedinputs passed. Independent reviewer7065f4e2 confirmed all974 actualterminal records, exactold2440names+1, samefailurebody and provenfooterincrement; no newfull2441 orallocationfailureinjection. Fresh16secactualquiet passed, started night-row-dispatch ONCE with23cases/69strict/276cells. Performance and retention stillpending; sourcekeptfixedduringmeasurement.

2026-11-10 02:00local result: actual resume2 controller0/638.49sec; finalstrict69PASS afterinitial20estRows-onlyfailure. Initial controller1 archivefilename error preserved; resume2 recheckedall69observations/allcolumns, completedarchive with comparison4 filename, freshquietandownedstoragerestart, exactsamepreflight, no relaxation or initialtiming. Actual276cells complete,189975queries, no knownwatch events; ownPD/TiKV/frontends stoppedandowneddata removed. Candidateproduction214add314delNET100/tests131added=combined31more source lines. Candidate rejected, full9521source/config/HEAD/status/protectedBUILD restored exactly to initial-input.json. Mutable service target still rejectedround12, never an accepted baseline. Actual974new behavior973pass/1known, SDKinherited, no full2441/newallocationfailureexecution. Finalreport all23CPU/latencytables andJSONranges prepared; pending independentstatisticalreview/report-firstdoccommit.

Independent final review9dbf7ebd re-read and independently recomputed all276cells/189975queries/23cases, verifiedinitial69/20estRows-onlyarchiveerrorandstrictretry, actualELFs andownedcleanup, restored9521inputs/heads/status/protectedfile. Supportsconservativerejection; no newbuild/test/performanceexecutionsbyreviewer. Fullreport41885806 andstatistics50b623db deliveredvia db5a06b4 BEFORE localdocumentationDCO. Onlythisresultplanisowned/staged; protectedBUILD andSDKremainuntouched. Newsourceandperformancegainsretained0/0, broadergoalnotachieved.
