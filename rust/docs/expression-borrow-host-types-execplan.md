# Borrow host argument types during signature preparation

This living ExecPlan follows root PLANS.md. Update Progress, Surprises & Discoveries, Decision Log, and Outcomes & Retrospective as the experiment proceeds.

## Purpose / Big Picture


SQL expression preparation currently copies every argument FieldType into a temporary vector solely to compare it with the immutable host signature. This experiment removes that temporary deep copy while preserving catalog identity, slot bounds, argument count, complete argument descriptors and result descriptor checks. Observable acceptance requires the same SQL results/errors and measured benefit without evidence of regression; compilation or smaller code alone is insufficient.

## Progress


- [x] Inspect all private preparation callers and the relevant recovered allocation/copy stacks; save initial9521 inputs before edits.
- [x] Distinguish required independent source snapshots from the disposable signature-comparison vector.
- [x] Adapt the existing private constructor to borrowed exact-size iteration while preserving old callers; extend existing signature checks.
- [x] Build actual service19 and prove the disposable clone/vector is absent from the real host compilation branch.
- [x] Complete SDK1161/Main973 behavior and required lint, clippy, format and source checks.
- [x] Run the fixed paired SQL experiment, decide withdraw, report exact production/test counts and restore the two owned files.
- [x] Complete measured withdrawal and prepare report/archive delivery and the authorized local result-document DCO; actual transport/commit receipts are separate.

## Surprises & Discoveries


The high-weight FieldType clone belongs to tipb::FieldType. BatchFacts owns an independent ClosedSourceSnapshot and later compares current source/schema against it; those clones are required for authentication and remain unchanged. Another captured stack contains Vec<FieldType> construction from a LocalExpr iterator in local::compile; the sole matching production source is the HostCall argument-type vector.

PreparedHostCall::prepare is pub(crate), not an external experimental API. Its only production caller is local/compile.rs. Other references in local/host.rs and types/{expr,expr_eval}.rs are tests. All public catalog/program accessors and runtime protocols remain unchanged.

Slice equality checks length before fields. Plain iterator equality alone changes that ordering; ExactSizeIterator plus an explicit length-first check preserves it. A borrowed iterator has a known number of remaining elements and owns no FieldType strings.

## Decision Log


Decision: change the existing crate-private constructor, without another wrapper, cache, source snapshot representation or execution path. Rationale: only the disposable comparison vector is redundant. Date/Author: 2026-11-10 / ExpressionReuseEngineer.

Decision: retain explicit length-first rejection, complete FieldType equality, existing errors and catalog/slot/result ordering. Rationale: arity errors should not traverse descriptors, and metadata is a trust boundary. Date/Author: 2026-11-10 / ExpressionReuseEngineer.

## Outcomes & Retrospective

Candidate19 is WITHDRAWN. The exact temporary type vector disappears from the actual successful host-preparation branch, but the new six-scene measurements do not demonstrate stable worthwhile benefit and adequate no-regression evidence. No new performance gain or source reduction is retained. Cumulative retained production remains2134 added/7293 deleted/net5159 fewer lines; tests5492 added/5684 deleted/net192 fewer. The original unified-path/no-performance-loss objective remains unmet.

The candidate was7 production lines added/8 deleted (one fewer), with6 added test lines. All candidate changes were restored byte-for-byte. All9521 source/config inputs, three repository HEAD/status surfaces and the protected BUILD match the pre-experiment snapshot before this result document was recreated. SDK is clean. No build/test was repeated after restoration; the actual19 artifacts remain saved as rejected, and saved14 is still the retained baseline. Task24's sealed evidence and document are unchanged.

Actual measurement2 completed0/468.989seconds,96 cells and111222 queries. Deep4096 CPU C/A is−2.413% (all four lower), but C/B is−0.358% with mixed directions. Integer p50 C/B is+0.540% (all four higher). Scan CPU C/A is+5.225% (all four higher), whereas C/B is−0.063%; the identical A/B control varies. Text QPS C/A and C/B improve0.633%/0.290% (all four), while CPU is mixed. Neither subtract controls nor attribute every difference to code/noise. Full absolute values, all favorable/adverse paired ratios and observed ranges are preserved in REPORT.zh.md, statistics-and-decisions.json and primary-summary.csv.

## Context and Orientation


Main is /home/agent/tidb/expression-unification/tidb at bb58396b1aa505b9da4ae41383013c01eec29cb2; sibling ../tikv is SDKf7672bf980143f2938fd0f900650a4c29091e70f. Evidence directory is /home/agent/.slock/agents/02fef0a9-76ce-4afe-9bce-73eebe82ff7d/evidence/path-unification/type-metadata-clone. initial-input.json records all9521 source/config inputs, three HEAD/status values and the protected untracked BUILD digest.

SDK components/tidb_query_expr/src/local/compile.rs builds the temporary argument vector around line910. local/host.rs checks it around line638, then returns PreparedHostCall containing only an owned immutable catalog and slot. Borrowed input descriptors never escape. local/profile.rs owns source snapshots and is outside the edit scope.

## Milestones


First, implement the private borrowed comparison and all caller adaptations. Extend the existing preparation_checks_catalog_and_complete_signature test to accept a borrowed non-slice iterator and reject extra arguments, preserving existing missing/changed descriptor and foreign catalog tests. The SDK inventory should remain1161 because the existing test is extended.

Second, build and inspect actual service19 before timing. Its HostCall compilation branch must no longer allocate, clone or drop a disposable Vec<FieldType>; a renamed/disappearing closure symbol alone is insufficient. Required source snapshot copies must remain. Any new compile or behavior failure is preserved and fixed before acceptance; a failed mechanism is withdrawn.

Third, run scoped behavior/quality gates and a new actual SQL window from fixed artifacts. Preserve all favorable/adverse data and exact failure provenance, then decide whether the candidate earns retention. A withdrawal restores only owned files and verifies the complete initial source set before the result document is recreated.

## Plan of Work


Change PreparedHostCall::prepare to accept impl IntoIterator<Item=&FieldType, IntoIter: ExactSizeIterator> with a named reference lifetime. Convert the iterator only after catalog and slot checks; compare lengths first, then signature.arg_types.iter().eq(arg_types). Leave catalog/slot/result validation and errors unchanged. In compile.rs replace the cloning collect with args.iter().map(LocalExpr::field_type), passing it by value. The associated exact-size bound preserves existing slice/Vec/Box callers. Only host.rs and compile.rs remain changed; direct-iterator prototype caller edits were removed before any build.

## Concrete Steps


Use one compiler stream, fixed parent-directory tools wrappers, RUSTFLAGS empty and CARGO_BUILD_JOBS=1. From Main:

    ../tools/cargo-tidb build --manifest-path rust/Cargo.toml --release --locked --offline -j1 -p tidb-server --bin tidb-server --no-default-features
    ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j1 -p tidb-expr --lib --no-run --message-format=json
    make lint

From SDK:

    ../tools/cargo-tikv test --release --locked --offline -j1 -p tidb_query_expr --lib --no-run --message-format=json

Save compiler-reported actual executables as round19 with SHA256, then run the explicit Main973 list and SDK1161 suite. Expected known failures remain one Main optional-common assertion and one SDK STR_TO_DATE assertion, plus one SDK ignored test; compare complete original outputs with only PID/time normalization. Main inventory2440 and SDK1161 names should be unchanged. New failures, timeouts and missing terminal events block acceptance.

Use SDK make run with the existing project clippy lint set scoped to expr test targets, TIKV_FRAME_POINTER=0 to preserve emptyRUSTFLAGS, then both pinned Jan30/Aug22 rustfmt checks on both touched Rust files and git diff --check. Complete commands/environments and actual child exits are recorded in JSON jobs/receipts. Rust-only edits require no Bazel preparation. No new datatype test sweep is claimed because that crate is unchanged.

## Validation and Acceptance


New service19 is the only candidate. Retained A/B are the exact same saved service14 SHA2d435bce2ba0ea2c153df91b5e6dbd82a5200d78c20bb09ee3dac8c993d75fea. Mutable Cargo output and saved18fcbb are rejected. F is original99a4c3e566774ba9733bc9d54366cf1e9b85446f63fa9a7418bfef5d3daeddd2; backend remains saved355be0acec593a24bb43ac0f5e6b8e30aae354b7e63fdd3530f3f2895c41e029, not rebuilt with current SDK.

After actual gates complete and relevant reads finish, acquire a fresh16-second quiet observation BEFORE starting owned storage: test cores2-5 <=5%, no process consuming>.5CPU seconds and no known build/test/profiler. The old midnight condition has elapsed; clock alone is not idle evidence. Use new owned8192-row fixture and the existing documented post-DDL preparation restart, distinct from at most ONE estimate-only retry restart before timing. Preserve initial failed24 full observations and receipts; never normalize accepted plans.

Six scenes: deep16 one-row/4096-row, integer4096,Decimal4096,text4096 and scan4096. Four labels A/B/C/F, four Williams orders,96 cells, each >=4seconds AND30 complete fetches. Exact configuration reads32/1024/vectorization1 before/after,24 full preflight plans/results/metadata/warnings. Save all192 before/after records BEFORE assertions, containing two warm and one final complete observation per cell. Each timed query fetches all rows and checks row count. No builds/tests/new sampling during timing; interference watch and owned PID/birth cleanup are required.

Report whole-SQL CPU ms/query and p50/p95/QPS, paired medians and observed ranges versus both A/B and F separately; no control subtraction, confidence-interval claim or kernel ns/eval inference. Retain only with actual mechanism, behavior, worthwhile benefit and adequate no-regression evidence. Otherwise restore and preserve all data. Scope does not claim full Main2440, allocation-failure injection, new callchain capture or full workspace make dev/clippy.

## Idempotence and Recovery


Save byte backups before edits. Never modify sealed task24 directory or any prior archive. All new jobs use distinct names and durable actual child receipts; wrapper exit alone is not success. On withdrawal restore only the two owned SDK files, remove the owned live plan, and compare all9521 inputs/HEAD/status/protected with initial; recreate final result document afterward. Do not delete the protected BUILD, other agents' files/processes, or old actual artifacts. Clean only recorded owned endpoints and command-bound data directories.

## Artifacts and Notes


Record source differences, real codegen, test names/terminal events, exact failures, per-window observations/statistics, gross production/test additions/deletions and cumulative counts. Report/raw data are delivered before already authorized local DCO as YangKeao / yangkeao@chunibyo.icu. Retention commits SDK source then Main document; rejection commits result document only. No push, PR, deploy or release. Task moves to in_review, not done without approval.

## Interfaces and Dependencies


No new dependency or public ABI. Existing pub(crate) PreparedHostCall::prepare accepts borrowed exact-size metadata and still returns catalog-owned PreparedHostCall. Full FieldType equality and independent source snapshot validation remain authoritative. Runtime callbacks, values and scheduling stay unchanged.

2026-11-10: initialized from concrete caller/source analysis before implementation, with explicit arity-first validation and separate required snapshot ownership.

2026-11-10 refinement before any build: a direct ExactSizeIterator argument required test caller churn in two extra files. The standard IntoIterator associated bound keeps old callers intact while preserving length-first checks, so the final candidate touches only two SDK files. The discarded prototype diff is preserved as source exploration.

2026-11-10 functional completion: functional-resume1 exited0/453.223s after the initial inventory-classifier exit1. Both actual unit builds, complete scoped terminal results, original failure texts, clippy/lint/four format checks and fixed inputs pass. Codegen peer independently confirmed the temporary clone vector disappeared from this branch; whole .text increases2752 bytes. Two preliminary quiet probes failed (browser activity and transient Go compilation); no owned workload service or timing started. Wait for current independent functional reading to end, then observe fresh load again.

## Final validation and experiment record

Service19 built once, actual0/702.256seconds. Its real compile branch passes original LocalExpr pointers directly into the constructor. The constructor checks key, slot, argument count, full argument equality and full return equality before retaining the original catalog. The temporary Vec allocation, element clones and success/error cleanup are removed from this branch. Required independent source snapshots, other descriptors, child buffers and error strings remain. Whole .text grows2752bytes; compiler size decreases304bytes and constructor grows72bytes. Independent source and actual-codegen reviews pass; no allocation-count or speed conclusion follows from instruction sizes.

Main actual unit19 SHA5628d5ffc5ccbc4d357c98a8b1df6bf8e143579835469f91cde7b71e11019afb built0/413.111seconds. Its2440 names are unchanged. Selected973 tests are972pass/one original optional-common assertion failure; full original failure output is equal after only PID/time normalization, including1713/657792/D19F5082E4AF34C5. Full2440 was not run.

SDK actual unit19 SHAef32feae920e440972c679c38e7259335e5115489c0011bf399435e450b29445 built0/237.792seconds. Runtime is1159pass/one original STR_TO_DATE failure/one ignore. Full original failure is identical after PID/time normalization. New borrowed-iterator/excess-arity assertions extend the existing test and pass. Inventory is1155 ordinary test definitions plus6 benchmark definitions,1161 names/kinds unchanged; benchmarks run in default test mode, not performance measurement.

The first functional controller actually exited1/651.615seconds because its inventory classifier counted only ordinary tests while expecting1161. Its receipt/output are retained. After proving all actual names and kinds equal, a separate continuation reused completed builds and only ran pending stages, actual0/453.223seconds. No already-completed runtime/build was repeated to hide the classifier failure.

Root make lint, scoped SDK expr-test clippy, four pinned Jan30/Aug22 rustfmt checks, both git diff checks and fixed9521 source inputs pass. Clippy171 warning diagnostics and source snippets match the prior scope. Independent functional/quality review81830b95 passes, including all973 individual events, SDK terminal output, inventories, actual artifacts and receipts. Reviewer only enumerated names, not tests. No new datatype suite, allocation-failure injection, full-workspace make dev or callchain capture was performed.

Four preliminary16second load observations were needed: attempts1–3 fail and remain saved, attempt4 passes after independent reading completes. Only then did owned storage/bootstrap begin. It created8192 rows and the derived type table, then performed the documented post-DDL preparation restart; storage1 actual0/20.949seconds. This setup restart is separate from the sole allowed failed-preflight retry.

Measurement1 actual1/25.254seconds before any timing:42 estimated-row cells in15 comparisons differ (A2730.67, others3333.33), including identical retained B. All remaining full-plan cells, results, metadata, warnings and effective configuration match exactly. All24 full original objects and the failure are retained. A new actual quiet observation attempt5 passes before restarting the same owned storage once, restart1 actual0/5.075seconds. No dataset rewrite or accepted-plan normalization occurs.

The second preflight passes all24 strict observations. Measurement2 completes96cells/111222queries in four balanced Williams orders, each at least4seconds AND30 full fetches. Configuration32/1024/vector1 is saved before/after every cell. All192 persisted before/after records contain288 full result observations and match strict references. Each timed query fetches all rows/checks row count; full EXPLAIN is compared at preflight only. The one-second known build/test/perf watch records zero events. Raw latencies, process CPU, full observations, configuration, exact ELF provenance and all failures are preserved; ranges are observed min/max, not confidence intervals.

All owned service PID/birth identities are stopped, ports free and both command-owned data directories removed. Restoration.json confirms9521 initial inputs/HEAD/status/protected before document recreation. Restoring identical bytes does not trigger another build/test. Report and raw archive are delivered before the authorized result-document-only DCO; no source candidate is committed and no push/PR/deploy is authorized. Separate postcommit receipts verify the final document, sealed contents and source state without modifying the sealed experiment.

2026-11-10 final experiment decision: withdraw candidate19 after actual paired SQL evidence; preserve all mechanism/functional evidence and mixed results. New retained performance gain/source reduction are zero, cumulative5159 production/192 test fewer. This concludes this distinct trial, not the broader performance objective.

Independent performance/restoration review fa139f56 passes: all96cells/111222queries, absolute and four ratio/range groups, strict24 and192records/288observations, unique retry,9521 restored inputs and cleanup independently verified. No reviewer rebuild/test. All favorable text QPS and adverse integer/scan/control observations remain in the report.

Report and full statistics delivered as b98d08b1 before any local commit. Raw archive delivery and document-only DCO are the remaining recorded handoff steps; no further implementation or measurement is pending in this trial.
