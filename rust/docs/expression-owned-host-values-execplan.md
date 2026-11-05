# Move owned host vectors through the existing expression driver

This living ExecPlan follows the repository root PLANS.md. Update Progress, discoveries, decisions and outcomes as work continues.

## Purpose / Big Picture

The shared driver currently copies a generated, already owned host argument into a new vector, and copies an owned Ready result again before returning it. These copies came from the retired Demo retained-capacity policy. Transfer these owners through the same HostFrame instead. Borrowed vectors and scalar broadcasts still need the existing logical-row copy. Source identity, type and row validation, task protocol, callback ordering and error cleanup remain unchanged.

## Progress

- [x] Create and self-claim task #10; keep completed task #7 and its reports unchanged in review.
- [x] Preserve actual baseline server ebb0117d1925d8fea67cd7df8bf8c490614197caee3c8ab11bf8a106481f191d and original99a. Build an independent deterministic8192row real-TiKV fixture.
- [x] Complete8matched current/original CPU profiles without overlapping builds; exact result/fields/warnings/full EXPLAIN equality. Actual call-chain expansion is largely unavailable in these release binaries (median known depth0), so only sampled instruction symbols are reliable.
- [x] Verify ownership test fails on old implementation (pointer differs, exit101), then implement the move-only change at argument caching and Ready completion.
- [x] SDK1159passed/1exact inherited STR_TO_DATE failure/1ignored; frontend consumer973=972passed/1exact inherited common failure, no timeout/abort. Project clippy, root lint, both fixed rustfmt toolchains and diff checks pass.
- [x] Build actual candidate efa6343c56e56d4af205c2ee7e1674cfff311c5192c6c6799f55b3321ca65dfb;45strict preflight observations and180paired cells/172529queries audited. Retain based on measured gains, report controls and original gap, generate report before local DCO commit. No push.

## Surprises & Discoveries

Initial profiles showed identical actual results, fields, warnings and plan operators but different pseudo estimated rows8192/3 versus10000/3 after fixture bootstrap. Those captures are preserved as diagnosis only. Restart owned storage after bootstrap and redo all profiles with strict plan equality, rather than relaxing the comparison rule.

## Decision Log

- Decision: Try ownership transfer before changing authentication.
  Rationale: Initial visible self samples are dominated by copying/allocation; the isolated store-entry authentication symbol is below about1%, while inlined authentication cannot be separated from its caller by that number. No trust-boundary check will be removed.
  Date/Author: 2026-11-05 / ExpressionReuseEngineer, within YangKeao6ce6e4da and supervisor150af307.

## Outcomes & Retrospective

Retain the move-only change. Relative to ebb,4096row nested/deep16/width8 frontend CPU falls13.7%/17.5%/14.8%, with SQL latency down3.6%/6.4%/3.5%. Control/JSON/text CPU falls11.1%/6.5%/8.7%. Those CPU directions agree in all four rounds. Integer CPU median falls4.5% but its range crosses1 and SQL latency rises0.4%; scan CPU rises1.4% and latency0.9%. No universal-speedup claim is made. Original CPU ratios remain1.600x/2.881x/4.478x for integer/nested/JSON and5.219x fordeep16: the original-performance goal is still unmet. Production Rust changes+12/-13, net-1; tests+80, cumulative reductions5079production/201test lines.

## Context and Orientation

Main repository is /home/agent/tidb/expression-unification/tidb, baseline7848c98ad439387dcba85af7495cd89119ec889a. SDK sibling ../tikv is c343ed40c70d3921ab8dad9efcefe538d4f5e329. Modify only components/tidb_query_expr/src/types/expr_eval.rs and its focused test. RpnStackNodeVectorValue::Generated owns its VectorValue in logical row order; Ref borrows a physical vector with a potentially reordered/repeated logical_rows map. HostCatalog admits a closed set of Int/Bytes/DateTime/Duration carriers, and HostFrame validates exact complete FieldType and row shape before retention or output. Preserve those gates. The unrelated untracked Main rust/third_party/tikv-client-rs/tests/client_go_differential/BUILD.bazel must remain untouched (SHA cc9fb078591d7c94366d1e6f504ff2bade23ccb57a9313f9c7235807ff658d52).

## Plan of Work

HostFrame::accept should retain a generated physical_value by move after the existing validation. Other nodes continue through copy_host_rows, preserving borrowed selection and scalar broadcasting. HostFrame::finish should return its owned vector directly after existing shape/type checks; the closed catalog supplies the carrier domain. Neither branch keeps a callback borrow, invokes an evaluator recursively, adds a kernel, changes values, or changes source authentication. Extra retained capacity is now the actual owner's capacity, with no Demo quota or normalization contract to recreate.

## Concrete Steps

From the agent workspace, evidence/remove-demo-policy/performance/followon-transport holds the independent harness. Run cluster.py storage/bootstrap and derive-fixture.py only for new owned data directories, restart owned storage after bootstrap, then profile_pair.py. Each profile records the actual ELF, exact query and query count, user-mode cycles and full final result checks. Profiling is separate from timing. SDK build and runtime jobs use ../tools/cargo-tikv --release --locked --offline -j1 -p tidb_query_expr; Main uses the pinned ../tools/cargo-tidb wrapper. Exact commands, environment, working directories and actual exit codes are recorded in job JSON and receipt JSON files. Do not rerun a completed job over immutable artifacts; preserve failed attempts and resume only its necessary next phase.

## Validation and Acceptance

The focused test passes an owned byte vector with spare capacity through argument caching and Ready completion at1and3rows, asserting the same payload allocation and NULL preservation. It must fail on the old copying path and pass after ownership transfer. Existing borrowed/scalar/selection, callback reset, foreign/stale handles, exact owned cause, nested cleanup and recovery tests must remain valid. Run the complete SDK suite and the previously specified973frontend consumers, including full PB eager actual phases and lazy actual physical corpora. Existing known SDK STR_TO_DATE and Main common failures must match after PID/timing normalization only; never rebase old golden values to declare success. Run root make lint, the project's clippy configuration, both pinned Jan30/Aug22 rustfmt checks, and git diff --check. Before timing, require strict result/fields/warnings/EXPLAIN equality for all selected queries. Use at least four balanced rounds and report frontend CPU and full-fetch latency separately, including small regressions and the original-native gap.

## Idempotence and Recovery

Only task-owned loopback services may be stopped; prove PID and birth time before signaling. Keep immutable baseline binaries and all prior reports. At completion stop owned services, prove ports free, and remove only owned database directories after matching their recorded command data-dir. Preserve all raw observations, profile data and diagnostics. If the small change does not justify its tradeoffs, revert only this experiment's SDK diff and preserve its report. No push, release, new dependency, public API, evaluator or native fallback is authorized here.

## Artifacts and Notes

Agent workspace evidence/remove-demo-policy/performance/followon-transport is the evidence root. profile-initial-estimates contains the initial diagnostic-only attempt. Task7 final reports and c343ed4/7848c98 commits remain the completed previous baseline.

## Interfaces and Dependencies

Use the existing VectorValue owner and RpnStackNode variants; no new public interface or dependency is required. Runtime trust-boundary validation and native value generation/role/producer/occurrence authentication remain in their existing owners.

Profile limitation: perf script independently confirms median known call-chain depth0 across all8captures; only1–14samples per capture have at least2known frames. Do not call these complete call-stack profiles, attribute total value-transport or authentication time from standalone symbols, or interpret missing stack frames as absent cost. Leaf-symbol samples and the source-located unconditional vector copies justify testing ownership transfer; only the independent SQL experiment can quantify its actual effect.

## Final validation and evidence

Actual candidate release ELF efa6343c56e56d4af205c2ee7e1674cfff311c5192c6c6799f55b3321ca65dfb was compared directly with baseline ebb and original99a, sharing backend355. Fifteen scenarios, four balanced rounds,180cells and172529queries pass independent reconciliation; every cell has at least2seconds and30queries. All45preflight result/field/warning/full-EXPLAIN observations match, and warm/final checks hold in every timed cell. No build, unit test or perf overlaps timing. Build input snapshots match exactly. All owned services are stopped and both temporary database directories removed only after PID/birth, recorded data-dir and port checks; observations and immutable binaries are retained.

The focused ownership test fails on the old copying implementation with exit101 and passes in the new full SDK suite. SDK1159pass/1exact inherited STR_TO_DATE failure/1ignore; scoped frontend972pass/1exact inherited common failure. Only PID/time normalization is allowed in failure comparison. No historical gold changes, new full2440frontend run or real allocation-failure injection is claimed. Project clippy exits0 with171warnings versus170previously; the added advisory is cloned_ref_to_slice_refs in the new test and is not denied by project configuration. The tested source is preserved byte-for-byte.

The first frontend runtime attempt stopped before0tests because the copied973name manifest was omitted from the new evidence directory. Preserve this failed receipt; copy the exact previous manifest and scope, then run front-runtime2 on the same immutable frontend binary172f2a3aad68b2d5d359266434c21d5d7c7d6b2b333470aab7476e9579e476a6. No build or failed case was hidden by that recovery. Exact runtime2 terminates over all973selected consumers.

All raw timing, queries, receipts, before/after ownership proofs, source/binary hashes, failed/retried profile and harness attempts, code counts and report are in agent workspace evidence/remove-demo-policy/performance/followon-transport. The report is generated before the local commit; the compact archive excludes ELF, raw perf files and databases, which remain local or are deliberately cleaned as described. Previous task7 commits and reports remain immutable.

Revision note: close the experiment with actual ownership proof, matched sampling limitations, terminal validation and measured retain decision; the larger original-performance target remains explicit.
