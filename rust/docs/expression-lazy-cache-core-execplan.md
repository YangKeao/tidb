# Trial result: allocate native cache synchronization only when used

This completed ExecPlan is governed by root PLANS.md; it records the measured withdrawal and the separate delivery/commit receipt boundaries.

## Purpose / Big Picture


Every SQL ScalarFunction currently creates seven empty native cache owners, including ordinary arithmetic which never uses those caches. The saved retained service really performs seven heap allocations in its default constructor. This experiment delays allocation of each existing cache's synchronization state until an actual cache operation or invocation share needs it. SQL results, warning/callback order, cache sharing, constructor exclusion, cancellation and context replacement must remain unchanged. Real service instructions, behavior and paired SQL measurements decide retention; a successful build is insufficient.

## Progress


- [x] Save all9521 initial source/config inputs and distinguish this mechanism from withdrawn19.
- [x] Inspect recovered samples83/84, source ownership and actual retained default/cache constructors.
- [x] Use a standard OnceLock inside the existing cache owner and extend the existing concurrency test for pre-initialization sharing.
- [x] Build service20 and prove the seven empty-cache allocations are removed from actual ScalarFunction construction.
- [x] Complete SDK cache/suite and Main973 plus28 relevant cache callers, quality and source checks.
- [x] Acquire fresh actual quiet, complete the fixed ordinary/cache SQL comparison, audit and clean owned processes/data.
- [x] Restore from actual evidence and prepare counts, report and raw package; actual delivery and local DCO are recorded by separate receipts without pushing.

## Surprises & Discoveries


The existing NativeContextCache is already the shared cache used by Main's BuiltinFuncCache facade. Ordinary Clone deliberately creates an independent empty owner; only share_for_invocation/share_state share live state. Therefore no new cache, wrapper or evaluation path is needed. The existing owned initialization reservation retains an Arc and must remain unchanged.

Recovered old memory samples83/84 contain ScalarFunction::default → ScalarFunction::new → binary_expression → rewrite_expr_resolved_inner. Their sample weights are not allocation counts or estimated speedups. Saved14 default at0x452cf60 has seven actual malloc sites; the common cache-new function at0x45c5730 allocates its synchronization core. Retained assembly is preserved in the new evidence directory; no new capture or old archive mutation occurs.

OnceLock adds an initialization state to each owner and may enlarge enclosing expression/invocation types or add a hot access check. Those are real performance risks. Used-cache scenes must be measured alongside deep arithmetic and scan; do not accept only the allocation win. The previous task25 temporary signature vector is unrelated and remains withdrawn.

## Decision Log


Decision: change only the existing SDK cache's private core field from Arc<CacheCore<T>> to OnceLock<Arc<CacheCore<T>>>. Resolve it on any actual cache access or explicit share. Keep CacheCore, Mutex/Condvar exclusion, NativeCacheInitialization and its commit/drop exactly as before. Rationale: standard library lazy publication removes unused cores without another evaluator or cache facade. Date/Author:2026-11-10 / ExpressionReuseEngineer.

Decision: invocation sharing initializes only synchronization storage, never constructs or reads a cached value. Owner cloning remains independent and empty. Rationale: shared handles created before the first value must still meet at the same state. Date/Author:2026-11-10 / ExpressionReuseEngineer.

Decision: preserve full original failures, source/actual-ELF provenance and fixed behavior/performance gates. Withdraw if synchronization semantics change or stable useful performance/no-regression evidence is insufficient. Date/Author:2026-11-10 / ExpressionReuseEngineer.

## Outcomes & Retrospective


The candidate is withdrawn. Actual20 removes seven eager56-byte core allocations, but retains a new hot publication check,+56bytes observed Expression stride and+76992bytes text. One-row deep-expression SQL CPU improves6.172%/6.782% relative to A/B;4096-row deep CPU rises2.982%/1.999%, REGEXP_LIKE rises2.170%/2.238%, each four rounds in the stated direction. Same-version controls also vary: the data do not attribute every change to source and do not establish no regression.

The single SDK file and owned untracked trial plan were restored before recreating this result document. All9521source/config inputs, three HEAD/status surfaces and protected BUILD matched initial exactly; no rebuild followed restoration. New runs below belong to rejected20; validation of the byte-identical restored source is inherited. Newly retained production/test/performance gains are0; cumulative retained source remains5159production/192test lines fewer. Pre-document Main54d9839f29899e2bba0fdae2d04a4e4d3c5aad40 and SDKf7672bf980143f2938fd0f900650a4c29091e70f remain the original parents. The broad goal remains unmet. Mutable actual20 is rejected; next retained baseline is separately saved14.

## Context and Orientation


Main is /home/agent/tidb/expression-unification/tidb; SDK is ../tikv. Evidence belongs to /home/agent/.slock/agents/02fef0a9-76ce-4afe-9bce-73eebe82ff7d/evidence/path-unification/next-callchain-cost. Its initial-input.json was saved before any new repository edit. All earlier sealed directories and archives, including task25, are immutable.

SDK components/tidb_query_expr/src/native_regexp.rs defines NativeContextCache, its core lock/condition variable, an owned initialization reservation, native regexp invocation and existing tests. native_cache_lease_tests.rs independently compares the original cache over exhaustive sequential and concurrent cases. Main rust/crates/tidb-expr/src/builtin_ext/cache.rs aliases that owner; scalar_function.rs embeds seven owners used for schema/path/find-in-set/regexp/replacement/like/ilike values. Those Main production files are not edited.

An owner is an independent cache instance. A shared invocation handle explicitly refers to the same live instance. A reservation excludes concurrent constructors while the caller demands children; dropping it restores availability and leaves any previous value intact. The core's allocation is not a cached value's construction. Allocator/OOM equivalence is already outside this cache's documented promise; no weaker SQL failure or ordering rule is introduced.

## Milestones


First, implement one SDK file, retain source backups and pin candidate inputs. Extend the existing concurrency test so a pre-initialization share and concurrent shares all observe one successful value and the owner clone stays independent. No test name or public method changes.

Second, build actual service20 and both unit artifacts sequentially. Actual ScalarFunction construction must no longer eagerly allocate the seven cores; first access/share must still publish one correct core. Inspect actual branches, not merely missing symbols. Complete existing SDK exhaustive cache tests/suite and scoped Main consumers/cache tests before timing.

Third, measure ordinary and real cache users from fixed artifacts and a new isolated SQL fixture. Preserve favorable/adverse observations, exact failures and same-version controls. A rejected change is restored byte-for-byte and verified against all9521 initial inputs before recreating a result document. A retained change is committed only after concrete review and report/raw delivery under existing local DCO authorization.

## Plan of Work


Import std::sync::OnceLock. NativeContextCache::new returns an empty OnceLock without allocating. Add one private core accessor which uses get_or_init to create the same Arc<CacheCore> with the original empty Mutex state and Condvar. share_state resolves the owner's core and returns an initialized OnceLock containing an Arc clone. available locks the resolved core and uses its condition variable. begin_initialization clones that same Arc into the unchanged reservation. Do not alter reservation commit/drop, failed-constructor handling, contexts, value construction or owner Clone semantics.

Document that synchronization storage is lazy while explicit sharing still does not observe/construct a value. Existing modules cover native cache lifecycle; the repository maintenance guide index has no dedicated query-expression guide, and no server/Raft/transaction ownership or storage contract changes.

## Concrete Steps


One compiler stream, pinned existing wrappers, empty RUSTFLAGS and CARGO_BUILD_JOBS=1. From Main:

    ../tools/cargo-tidb build --manifest-path rust/Cargo.toml --release --locked --offline -j1 -p tidb-server --bin tidb-server --no-default-features
    ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j1 -p tidb-expr --lib --no-run --message-format=json
    make lint

From SDK:

    ../tools/cargo-tikv test --release --locked --offline -j1 -p tidb_query_expr --lib --no-run --message-format=json

Save compiler-reported actual service/unit executables as20 with SHA256. SDK inventory remains1155 ordinary tests plus6 benchmark definitions; run in ordinary test mode, not as performance benchmarks. Runtime expected1159 pass/one original STR_TO_DATE failure/one ignore; compare full failure with only PID/time normalization. Main inventory2440 must remain unchanged; run the existing973 consumer list and disjoint relevant cache callers, preserving known original failures rather than rebasing them.

Use SDK make run with the fixed project clippy flags and only the expression test target, TIKV_FRAME_POINTER=0/empty RUSTFLAGS. Run pinned Jan30 and Aug22 rustfmt checks on the one touched Rust file and both git diff checks. No datatype suite or Bazel preparation is needed for this Rust-only cache edit. Complete exact commands/environments/actual child exits in durable JSON jobs and receipts.

## Validation and Acceptance


First validate pre-initialization sharing and concurrent construction, owner clone independence, context replacement, constructor errors/panic, dropped reservation, blocked readers/followers and retained old values. Existing original-vs-candidate cache sequences and barriers are required, not a new weaker oracle. Relevant Main schema/path/find/regexp/like consumers must pass. Existing original Main/SDK failures retain complete output; a new assertion difference blocks until understood and resolved.

After all builds/tests/reviewer reads finish, fresh16second observations BEFORE owned SQL storage must show cores2–5 at most5%, no process consuming over0.5CPU seconds and no known build/test/perf process. Fresh current directive and actual machine state decide timing; old quiet or midnight evidence cannot authorize a new run. No compiler/test/profiler during timing.

Use A/B as the exact same saved retained14 service SHA2d435bce2ba0ea2c153df91b5e6dbd82a5200d78c20bb09ee3dac8c993d75fea, C as actual20, F original99a4c3e566774ba9733bc9d54366cf1e9b85446f63fa9a7418bfef5d3daeddd2. Backend remains old saved355be0acec593a24bb43ac0f5e6b8e30aae354b7e63fdd3530f3f2895c41e029, not rebuilt. Source snapshots and actual ELF pins must match at entry/exit.

Fixed12 scenes: scan, integer, Decimal, text, deep16 one-row and4096-row plus regexp_like, regexp_replace, like, find_in_set, json_schema_valid and json_modify, all cache-user scenes4096 rows. Record exact SQL/specs before any fixture or timing. Four balanced Williams orders,192cells, each at least4seconds AND30 full fetches. Actual configuration32/1024/vector1 before/after; strict48 complete preflight plans/results/metadata/warnings;384 persisted before/after records containing576 full observations saved before assertions. Each timed query fetches all rows/checks count.

Use new owned8192-row fixture. The documented post-DDL preparation restart is separate from at most ONE failed-preflight estimate-only restart of the same owned storage; retain all first objects/receipts, require every other field exact and never normalize accepted plans. No repeated retries or missing after-observation evidence. Stop/clean only recorded own PID/birth/command data paths.

Report CPU ms/query, p50/p95 and QPS absolute statistics, C/A/C/B/C/F and B/A paired medians with actual ranges for all scenes. No control subtraction, confidence intervals, universal no-regression, kernel ns/eval or allocation-count speed extrapolation. Keep only if the mechanism/behavior and useful measured benefit with adequate no-loss evidence justify it; otherwise restore. Report candidate and retained gross additions/deletions/net counts separately from documents/evidence.

## Idempotence and Recovery


Back up the one SDK source file before editing and save candidate bytes/input manifests before building. Every job gets distinct durable actual exit records; a wrapper0 or vanished PID is not success. Preserve failed attempts and correct the cause before continuing. Never mutate sealed old evidence or reuse saved19 as retained baseline.

On withdrawal, assert current candidate bytes, restore only native_regexp.rs, remove only this owned live plan, verify all9521 inputs/three HEAD/status surfaces/protected BUILD against initial, then recreate result documentation. Do not rebuild identical restored sources. Preserve the unrelated protected BUILD and all other agents' files/processes. On retention, commit reviewed SDK source then Main documentation after report/raw delivery; local authorized DCO is YangKeao / yangkeao@chunibyo.icu. No push/PR/deploy/release. Task becomes in_review, not done without human approval.

## Artifacts and Notes


Retained default and cache-new assembly, recovered frame references, source backups/diff, exact source/ELF provenance, runtime inventories and complete terminal events, strict observations, actual latencies/process CPU, all control/adverse data, cleanup and final restoration/retention belong in the new evidence directory. Reports/raw archive precede local commits; postcommit receipts stay separate so sealed evidence remains immutable.

## Interfaces and Dependencies


No dependency, public method or SQL protocol change. Private cache storage becomes OnceLock<Arc<CacheCore<T>>>. The private accessor returns &Arc<CacheCore<T>>. NativeCacheInitialization still owns Arc<CacheCore<T>>, and existing caller/cache-value types and synchronization remain authoritative. Expected owner layout growth and extra hot OnceLock check must be measured, not hidden.




2026-11-10 functional checkpoint: Main973=972pass/one original failure with only PID/time normalization; additional28disjoint cache callers allpass. SDK1159pass/one original STR_TO_DATE failure/oneignore,1155test+6benchmark definitions unchanged. Rootlint,expr-test clippy171exactbaseline diagnostics,two pinned formatchecks,9521inputs allpass. No fullMain2440/newdatatype/OOM injection. Independent functional review requested before fresh load/timing.

## Completed actual validation and measurement


Service20/Mainunit20/SDKunit20 builds actually exited0 in658.136/369.208/232.999seconds. Main2440names unchanged; scoped973=972pass/one full-output-identical existing failure, plus28disjoint cache callers allpass. SDK1155ordinary+6benchmark-definition inventory unchanged, ordinary run1159pass/one original STR_TO_DATE failure/oneignore; failure normalization only PID/time. Extended first-share concurrency and original error/panic/cancel/old-value tests ran. FullMain2440/newdatatype/OOM injection not claimed.

Functional controller actual0/1040.365seconds; rootlint, scopedexpr-testclippy171baseline-identical messages/codes/source snippets, two pinned formats/diffs/inputs passed. Independent source/codegen/behavior records preserved. Independent performance/restoration review e7a36256 passed: raw192cells143397latencies, all metrics/ranges,20tables864numbers,48strict observations,384records576full observations, first33/96estRows-only differences and unique retry, actual artifacts/cleanup/all9521restoration. Reviewer did not rebuild, rerun tests or performance; report/archive/future local commit were not yet in that review scope.

Twelve16second quiet attempts failed and are preserved; fresh13/14 passed before setup/unique retry respectively. Owned8192fixture, derived real table and postDDL preparation restart actual0/19.441seconds. Firstpreflight48objects actual1/26.052seconds BEFOREtiming:33comparisons96estRows-only differences inclsameB, all other plan cells/results/fields/warnings/configuration exact. Originals/receipts preserved. Newquiet14, then ONE same-data restart actual0/4.220seconds, then48strict objects fully equal without normalization.

Measurement2 actual0/861.000seconds,192cells143397full-fetch queries,384before/after records containing576full observations. Every cell checked effective32/1024/vector1 before/after; each timed query checked complete row count. Full digests/fields/warnings were compared in before/after observations; exact plans at preflight, not per timed query. AuditPASS/knownprocesswatch0, not a guarantee against all external bursts. Owned PID/birth/command data cleanup passed before restoration.

Four Williams orders balance label positions/pairs; SQL2-5/backend6-7/PD8-9/client10. A/B exactsame saved14, Cactual20, Foriginal99a/backendold355notrebuilt. Paired four-round medians/ranges are not confidence intervals or ratios of absolute medians. No control subtraction/kernelns-eval/allocation-count/single-source or universal no-loss claim is made.

## Observed SQL CPU changes for all fixed scenes


Negative CPU change is favorable; all other metrics, adverse cases and same-version controls remain in the full report/raw data.

| Scene | CPU C/A | CPU C/B | Four-round C/B observed range |
| --- | --- | --- | --- |
| FIND_IN_SET | +1.157% | +0.951% | +0.380% … +2.415% |
| JSON_SET | +8.071% | +7.066% | +0.370% … +29.573% |
| JSON_SCHEMA_VALID | +1.094% | +0.346% | -0.250% … +2.140% |
| LIKE | +2.219% | +1.988% | +0.733% … +3.247% |
| REGEXP_LIKE | +2.170% | +2.238% | +1.490% … +3.945% |
| REGEXP_REPLACE | +2.407% | +2.008% | +0.699% … +2.856% |
| Decimal4096行 | +0.616% | -0.294% | -1.068% … +0.056% |
| 整数4096行 | +0.858% | -2.409% | -3.713% … +1.545% |
| 扫描4096行 | +0.027% | -0.069% | -2.366% … +0.689% |
| 文本4096行 | +1.431% | +2.118% | +0.487% … +2.590% |
| 深16层／1行 | -6.172% | -6.782% | -8.661% … -5.783% |
| 深16层／4096行 | +2.982% | +1.999% | +0.352% … +2.584% |

## Final decision and delivery boundaries


Decision: withdraw the entire17added/12deleted production and8added/1deleted test candidate, preserving its measured single-row benefit and adverse/control data. The intended no-loss result was not demonstrated despite correct lazy publication and construction allocation elimination. No supplementary result selection or repetition of rejected candidates follows. Date/Author:2026-11-10 / ExpressionReuseEngineer.

New retained additions/deletions are0/0 in both source classes; historical retained production2134added/7293deleted and tests5492added/5684deleted, net5159/192fewer. Documents, scripts, artifacts and rejected changes excluded. Full report records CPU,p50,p95,QPS absolute medians and paired C/A,C/B,C/F,B/A medians/ranges for all12scenes.

Report/statistics and complete immutable raw archive precede already-authorized local documentation-only DCO YangKeao/yangkeao@chunibyo.icu. Actual delivery/command/commit receipts remain separate from sealed evidence. No push/PR/deploy, source rebuild after restoration, or claim of broad-goal completion is made. This result is in review for human validation.
