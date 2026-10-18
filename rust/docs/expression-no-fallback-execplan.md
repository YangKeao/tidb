# Execute expressions without an alternate evaluator

This ExecPlan is a living document. Maintain Progress, Surprises & Discoveries,
Decision Log, and Outcomes & Retrospective according to root PLANS.md.

## Purpose / Big Picture


The user now requires complete removal of expression execution fallback, not
another performance-only extension of the eager whitelist. An expression which
previously worked must retain its SQL result, demanded arguments, side effects,
and diagnostic contents while executing through one shared execution model.
Scalar and vector kernels inside that model are acceptable; calling the old
expression evaluator after preparation fails is not the final architecture.
Resource and SQL failures must remain failures, not retries through another
interpreter. Warning/error relative ordering remains explicitly relaxed by the
previous user contract; demand and side-effect ordering do not.

## Progress

- [x] N7l: retire statement/live clock Ready callers and integrate the entire clock family and aliases as authenticated sources, preserving precision, original clock tuple, live SYSDATE sampling, callbacks and result boundaries.

- [x] N7k: retire physical/logical TSO Ready calls and integrate transaction timestamp canonical sources with independent old observations, finite resource/provenance and original return-family validation.

- [x] N7j: replace native/legacy UNIX_TIMESTAMP and FROM_UNIXTIME Ready handoffs with actual SDK values and canonical sources, including DATE_FORMAT handoff, preserving distinct native/legacy conversion, dynamic context demand, raw numeric identity, finite resources and exact original comparison before retirement.
- [x] N7i: retire EXTRACT selector/cast/mixed/composite/NULL Ready paths and integrate canonical scalar EXTRACT plus named composite sources, preserving original source metadata, both eager operands, casts, separately demanded mode reads, warning/error distinctions and broad composite arithmetic. Freeze independent old observations and actual Ready/source REDs before edits; pair old/new before wire/controller retirement; validate finite work, provenance, late errors, dynamic modes, duplicates and mixed Projection before signed local commits.

- [x] N7h: retire legacy DATE_ADD/SUB text/time/duration Ready head/step/parse controllers in favor of actual SDK state, keeping exact requested source channels, folded i128, original explicit zone, generic MyDecimal parse/round/render and separate presence. Capture independent old observations and forbidden Ready REDs before edits, pair old/new before retiring old controller bodies, bound demanded work and validate public/unistore consumers with signed local commits. The broader unistore SimpleExpr walker and its other unsupported source families remain explicit global work; this checkpoint must not claim their canonical source migration.

- [x] N7g: remove ordinary DATE_ADD/SUB calendar, AST-default and typed-duration Ready controllers; preserve composite-amount-first versus duration-date-first demand, exact coercion/rounding, unconditional warnings and late overflow disposition. Capture original observations and source/Ready REDs before production edits, then shared actual-value SDK controller, generic date_add_/date_sub_ source integration, finite work/ownership and full validation with signed local commits. Legacy PB date arithmetic is separate and remains explicit pending work.

- [x] N7f:468099 original58044483da932cf4;467856actual old/new paired+243hash arity boundaries before bounded-staleness Ready caller retirement/wire delegation. Canonical generic sources, both original casts, statement SafeTS, capability/resource/callback/selection closure integrated. Fullfront1644same3/all222batch, SDK1096same1, Projection26/extra mixed group, focus8, clippy89expr/171allEXACTN7e/lint/fmt12/diffs/unchangedSTRfixtureSHA pass. Receipt evidence/no-fallback-n7f.md. Local signed checkpoint only; no push; global retirement remains open.

- [x] N7e: five actual inherited identity REDs fixed with generated construction snapshots, preserving same-instance pointer identity across codegen units. Focus25pass/0fail/1ignored, codegen20, fullSDK1096remaining1, frontend1637same3/all218batch, Projection26, isolatedbodyorigin1, clippy89expression/171all exacttitles, lint/pinnedformats/diff/fixturehashes pass. Receipt evidence/no-fallback-n7e.md. Local signed checkpoint only, no push; calendar/scanner and global retirement remain open.

- [x] N7d: remove unconditional zero-month/fullzero rejection under relaxed NO_ZERO_DATE; original Go calendar/probe authority and ten parser fixtures. Original844344 d7dd9f9ee5b55431 captured BEFORE fix;826200 unchanged+18144 explicit Go-calendar deltas verified. Real value/source/SDK REDs green. Front1637remaining3/all218batch, SDK1091same6, Projection26, clippy89exacttitles/lint/pinnedfmt5/diff pass. Receipt evidence/no-fallback-n7d.md. Local signed checkpoint, no push; scanner/global retirement remain open.

- [x] N7c: immutable392556 b4faecb69db19986;392535 actual paired comparisons+21 hash-guarded boundaries before native Ready retirement. Canonical generic/exact-PB sources, original endpoint casts, full metadata, resources and callback ownership integrated. Final frontend1634same4/all217batch, SDK1090same6, datatype564same2, Projection25/eightgroups, clippy89exacttitles/lint/pinnedfmt19/diff pass. Receipt evidence/no-fallback-n7c.md. Local signed checkpoint; no push; global retirement remains open.

- [x] N6q0: explicit six MOD/div/DIV row/window catalog identities share the existing SDK frame; two protocol tests retain2050 occurrence Fresh/Reuse, primary errors, cancellation and reuse. Core1068same6, existing-consumer frontend1515same4/95ignored with exactly three pending admission REDs filtered, clippy89 inherited warnings, lint/pinnedformats/diff pass. Three root REDs rerun and still fail the same route assertions; actual source integration remains open and these new tests stay uncommitted until fixed. Receipt evidence/no-fallback-n6q0.md.

- [x] N6p: native frontend MOD/division and integer/Decimal DIV ready-argument adapters retired, with one plain value layer and direct shared SDK calls.51388 exact old-adapter comparisons pass before retirement; independent captured witnesses retained afterward. Full frontend1515same4/95ignored(all151batch), Projection25, five new value/resource/cause checks, lint, pinned format and both diff gates pass. SDK production code unchanged; its guide updated. Native quotient root admission and global fallback retirement remain open. Receipt evidence/no-fallback-n6p.md.

- [x] N6n: canonical native unary signs share11 old RPN value kernels, constant promotion and typed overflow. Exact public result coercion and ENUM/SET integer views preserved; CHAR native qualification stays independent of compiled closure.2176 root comparisons+16 valid SET flag comparisons+63 provider cases,151 batch/full frontend1510same4, SDK1062same6,18 protocol/catalog, Projection25/22roots, lint/clippy89/pinnedformats/diff pass. Signed local TiDB66b3323e/TiKVd6f07d4; receipt evidence/no-fallback-n6n.md. Global retirement remains open.
- [x] N6o: shared quotient value kernels and borrowed Decimal DIV computed outcomes. Core1066same6/frontend1510same4, four new direct expected tests and original wrapper tables, scoped clippy89 inherited warnings, lint, pinned format and both diff checks pass. Source integration remains next.

- [x] N6k: typed final-output ownership now shares actual native batch eligibility, preserves output position/prefix/aliases, and checks retained/copy/conversion ownership before further demand. CONCAT late-error publication RED fixed. Eight-carrier tests exposed Duration rounding; a dedicated native transport constructor preserves full nanoseconds at all three frontend import seams while SQL constructors remain unchanged.305 native comparisons, resource/empty/reuse guards, full frontend1496same4/95ignored(137batch), core1058same6/1ignored(14window), datatype564same2(parent563same2 proven in isolated checkout), ProductionProjection25 extended eight-root matrix, lint/scopedclippy89warnings/pinnedfmt/diff pass. SDK local20f6608; receipt rust/docs/evidence/no-fallback-n6k.md. Operand/source coverage and global retirement remain open.

- [x] N6j: every native-value-closed control/comparison root selects its genuine row program, independent of PLUS/MINUS/MUL descendants. Original EQ(ABS(...),ABS(...)) error-order/identity regression was RED before the fix.648 new root and96 SQL comparisons pass; full frontend1491same4/95ignored including132batch, ProductionProjection25 extended seven-root control matrix, lint/pinnedfmt/diff pass. SDK unchangedcff06c7. Receipt rust/docs/evidence/no-fallback-n6j.md. Typed outputs, other source domains and global retirement remain open.

- [x] N6i: shared native static candidate fact and native-value row-root publication independent of function spelling/view label. Original RealABS and non-call budget regressions reproduced before final fixes. Full frontend1489same4/95ignored,130batch,122root+48SQL comparisons, ProductionProjection25 (extended five-root matrix), lint/pinnedfmt/diff pass. SDK unchangedcff06c7. Receipt rust/docs/evidence/no-fallback-n6i.md. Typed output/operand domains and global retirement remain open.

- [x] N6h: control/comparison arithmetic row demand, bounded native batch publication staging and scoped typed controls below window roots. Both native demand/publication defects and the SDK restriction were reproduced before implementation.448 root comparisons,72 SQL additions,126 batch/full1485same4/95ignored, core14window/full1058same6/1ignored, Projection25, lint/scopedclippy89warnings/pinnedfmt/diff pass. SDK signed local cff06c7; receipt rust/docs/evidence/no-fallback-n6h.md. Other batch publication owners and global alternate-driver retirement remain open.

- [x] N6g whole-child unary integration: three explicit unary-window descriptors (integer ABS, integer-source Decimal CAST, source-typed JSON CAST) use the same SDK frame. Core11window/full1055same6/1ignored and scopedclippy89warnings pass. Native providers share the extracted complete source-vector normalization owner with arithmetic;71 actual provider/input-failure comparisons plus108 resource cases and early unsupported-source/large-Decimal formatting guards pass. New root/edge lowering selects these actual phases instead of the former three exclusions. Standalone Decimal CAST priority now has a genuine single-argument window phase over the same strict column-padding import, before the original flag point. Existing122batch tests pass after source integration; new336root/nested native comparisons pass. Full frontend1483same4/95ignored, ProductionProjection24, core1055same6/1ignored, scopedclippy/lint/pinnedfmt/diff pass. Receipt rust/docs/evidence/no-fallback-n6g.md.

- [x] N6f closed arithmetic root integration: closed arithmetic roots select authentic Row/Bulk programs before values, plus a typed Decimal priority stage in the SAME SDK driver before the original per-output flag getter. Pure integer-column CAST layout is shared with the old owner. Native Unsupported alone selects the next semantic phase; errors remain terminal. Root673/fast-source150/window-provider99 comparisons pass, plus exact resource/purpose guards. Complete frontend1478same4/95ignored, Projection23, lint/pinnedfmt/diff passed. General bulk ABS/DecimalCAST/JSONCAST and control wrapping remain next work.

- [x] N6f final review: initial source patch is retained as evidence only, already applied and superseded. Exact context tests now compare the native per-output getter point rather than the old once-per-suite convenience assertion. Late invalid-purpose regression was RED before complete-vector validation and now passes. Initial provider RefCell borrow RED fixed before branch publication.
- [x] N6f0 preparation: native value storage has a separate, typed Decimal-fast outcome domain and purpose-bound capability. Unsupported, SQL NULL and an actual fast coefficient remain distinct. Ordinary SQL insertion, borrowing, kernel cloning and output roles cannot manufacture/consume a fast outcome. The existing generation/provenance and pre-insertion resource checks apply. No production root coverage changes in this step. Five store tests/complete frontend1469same4/95ignored/Projection22/lint/pinnedfmt/diff pass. Receipt rust/docs/evidence/no-fallback-n6f0.md.


- [x] (2026-10-17) Recover paired baseline: TiDB bf4723283aadba5fbde116a7572c2aac18a887ba,
  TiKV d32cebb3ebc353748cb225fe254d6893e62c8bdc; existing unrelated untracked
  tikv-client-rs BUILD.bazel must not be staged.
- [x] Read both AGENTS.md files, root PLANS.md, and existing implementation.
- [x] (2026-10-17) N1: Terminal preparation errors are cached and propagated; regression fails before fix, batch35 pass, full1387 pass/same4 known failures/95ignored, lint and scoped fmt pass.
- [x] (2026-10-17) N1b: all ordinary String/JSON/Vector/Decimal literal payloads gated in borrowed form before any snapshot/temporary conversion; four-family regression red before fix,59 focused/full1411same4 pass; lint/fmt pass. Receipt rust/docs/evidence/no-fallback-n1b.md.
- [x] (2026-10-17) N2: multi-output/leaf/direct-column scheduling, selected raw-column alias-safe copy, suite metadata/node caps. Batch40/Chunk275/pinned-nightly Projection9 pass; full1392 retains same4 inherited failures; lint/fmt pass.
- [x] (2026-10-17) N3a: STRCMP uses its existing native collation kernel in composed shared graphs; batch41/full1393 retain same4 failures, TiKV1033 retains same6, actual rewritten SQL25 scenarios all prepared without fallback/refusal; scoped project clippy/lint/fmt pass.
- [x] (2026-10-17) N3b: JSON_TYPE SQL kernels, explicit native failure kinds and raw native JSON leaf view. Frontbatch47/full1399same4, coreprofile60/full1036same6 pass;25 SQL routes pass without fallback/refusal; lint/scoped project clippy/fmt pass.
- [x] (2026-10-17) N3c: CONCAT/CONCAT_WS use a closed SQL host catalog and the existing shared HostFrame. One shared demanded-prefix policy and join algorithm; exact native packet observations/warnings/errors, generation-safe cleanup and bounded opaque heaps. Front52/full1404same4, core1039same6, production Projection10 including serial/parallel packet policy, concat25 SQL routes pass; lint/scoped clippy/fmt pass.
- [x] (2026-10-17) N3d: REGEXP_LIKE uses the same HostFrame and original native context/cache handles, plain shared LIKE primitive and exact typed error mapping. Front57/full1409same4, core1041same6, SQL25 scenarios prepared/no fallback/refusal; lint/scoped clippy/fmt pass. Receipt rust/docs/evidence/no-fallback-n3d.md.
- [x] (2026-10-17) N4a: shared frame controls IF/IFNULL/COALESCE/CASE and AND/OR, exact native carriers, signed Int NULLIF, compiled singleton window. Batch43/core profile59/Projection9 pass; full1395 and1035 retain exactly the inherited4/6 failures. Both25-scenario SQL cohorts pass with no fallback/refusal; lint/scoped project clippy/fmt pass.
- [x] (2026-10-17) N4b1: native integer cast_char through the same HostFrame/shared string-cast primitive, exact unsigned bits/length/charset/warning contents and source-only metadata. Front63/full1415same4, core1042same6,24 actual rewritten SQL executions without fallback/refusal; lint/scoped clippy/fmt pass. Receipt rust/docs/evidence/no-fallback-n4b1.md.
- [ ] N4b: Complete coercions, temporal result transforms, full argument demand policies and effectful multi-output ordering.
- [x] (2026-10-17) N5a1: TEST-ONLY native value store prototype; all19 exact representations, namespace/generation/role/source/row validation, logical resource gates. Three tests/full1418same4/lint/fmt pass. No new production SQL coverage. Receipt rust/docs/evidence/no-fallback-n5a1-prototype.md.
- [x] (2026-10-18) N5a2: production native leaf handles and value-only CHAR consumer; all19 actual kinds, hybrid columns, exact diagnostics and real SQL/Projection validated. Receipt rust/docs/evidence/no-fallback-n5a2.md. General computed native values remain N5 work.
- [x] N5a: lossless invocation-scoped native value handles; validate namespace/generation/role/source/row provenance and bounded value retention before extending CHAR beyond wire carriers. Prototype first, then integrate demanded native leaves and validate all19 kinds/representation edges.
- [x] (2026-10-18) N5b1: direct native column transport no longer depends on computation wire type admission; raw-byte tests cover six storage cases, NULLs, aliases, selection and vectorization off. 74focused/full1426same4/lint/fmt pass. Receipt rust/docs/evidence/no-fallback-n5b1.md.
- [x] (2026-10-18) N5b2: native leaf root output ABI with separate Output role, complete prepublication handle validation and native Chunk publication. 17 output kinds/216 literal scenarios, disabled-direct-column path,76focused/full1428same4/Projection12/lint/fmt pass. Receipt rust/docs/evidence/no-fallback-n5b2.md.
- [x] (2026-10-18) N5c1: native computed CAST islands through one HostFrame, plain shared result coercion and direct real→unsigned primitive (no nested evaluated-args program). 79focused/full1431same4/core1043same6/24SQL/Projection13/lint/clippy/fmt pass. Receipt rust/docs/evidence/no-fallback-n5c1.md.
- [x] (2026-10-18) N5c2: JSON CAST joins native computed islands with pre-argument type admission and shared literal/nonliteral value conversion. 82focused/full1434same4/156inputcases/24SQL/Projection14/lint/fmt pass; poisoned vector-NULL input proves zero argument demand. Receipt rust/docs/evidence/no-fallback-n5c2.md.
- [x] (2026-10-18) N5c3: correct native-value producer capability gaps to admission rather than terminal binding errors; RED valid CAST(ABS/PLUS/IF) SQL regression,83focused/full1435same4/lint/fmt pass. Receipt rust/docs/evidence/no-fallback-n5c3.md.
- [x] (2026-10-18) N5d1: native COALESCE/IFNULL share the existing staged driver and pure nullable selector, preserve actual NULL/value kind/FSP and mint own producer handles. 87focused/full1439same4/core1043same6/30SQL/Projection15/lint/clippy/fmt pass. Receipt rust/docs/evidence/no-fallback-n5d1.md.
- [x] (2026-10-18) N5d2: native IF/CASE use the same staged driver, original pure truth conversion/branch selector and checked native result publication. 90focused/full1442same4/core1043same6/30SQL/Projection16/lint/clippy/fmt pass; CAST(IF) migration gap retired. Receipt rust/docs/evidence/no-fallback-n5d2.md.
- [x] (2026-10-18) N5d3: native AND/OR and rewriter IsTrueWithNull share checked frames and direct RPN value kernels, preserving nullable demand and warning prefixes. 93focused/full1445same4/core1043same6/36SQL/Projection17/lint/clippy/fmt pass. Receipt rust/docs/evidence/no-fallback-n5d3.md.
- [x] (2026-10-18) N5e1: native NOT/IS NULL/TRUE/FALSE and UNKNOWN predicates compose through checked native values and direct existing kernels. 95focused/full1447same4/core1043same6/72SQL/Projection18/lint/clippy/fmt pass; JSON IS NULL migration gap retired. Receipt rust/docs/evidence/no-fallback-n5e1.md.
- [x] (2026-10-18) N5f1: shared plain comparison preparation and direct existing core value entries, with immutable metadata facts and finite vector-copy budget. 23,328 comparison cases/new2tests/95batch/full1449same4/core1043same6/Projection18/lint/clippy/fmt pass. Native comparison HostFrame is next. Receipt rust/docs/evidence/no-fallback-n5f1.md.
- [x] (2026-10-18) N5f2: six native comparison hosts preserve raw operand demand, immutable type/constant/column facts and original typed integer priority. 98focused/full1452same4/core1043same6/90SQL/Projection19/lint/clippy/fmt pass. Receipt rust/docs/evidence/no-fallback-n5f2.md.
- [x] (2026-10-18) N5f3: native NULL-safe equality shares comparison frames and pure generic preparation, preserving presence, sentinel priority and duration-versus-datetime conversion disposition. 98focused/full1453same4/core1043same6/132SQL/Projection19/lint/clippy/fmt pass. Receipt rust/docs/evidence/no-fallback-n5f3.md.
- [x] (2026-10-18) N5f4: native NULLIF shares the binary frame, original literal comparison/normalization policy and first-value identity. Context traces preserve the original comparison configuration read. 101focused/full1456same4/core1043same6/192SQL/Projection20/lint/clippy/fmt pass. Receipt rust/docs/evidence/no-fallback-n5f4.md.
- [x] (2026-10-18) N5g1: native ABS shares pure signed/unsigned/IEEE/Decimal primitives, actual-value dispatch and preformatted source diagnostics after a complete byte gate. 103focused/full1458same4/core1044same6/234SQL/Projection21/lint/clippy/fmt pass. Receipt rust/docs/evidence/no-fallback-n5g1.md.
- [x] (2026-10-18) N5g2: shared plain add/subtract/multiply value preparation and direct integer/IEEE/Decimal/vector kernels, with old callers sharing a temporary adapter. 24576 value/error/warning/observation comparisons,3newtests/full1461same4/core1044same6/Projection21/lint/clippy/fmt pass. Native arithmetic source demand is next. Receipt rust/docs/evidence/no-fallback-n5g2.md.
- [x] (2026-10-18) N5g3: native PLUS/MINUS/MUL producers beneath row-demand boundaries, with plain numeric conversion, exact fast-probe re-demands and demand-sensitive source diagnostics. Existing bulk phases preserved. 106batch/504manual/342SQL/full1464same4/core1044same6/Projection22/lint/clippy/fmt pass. Receipt rust/docs/evidence/no-fallback-n5g3.md.
- [ ] N5: Cover remaining calls, source metadata, host context and non-projection callers.
- [ ] N6: Remove alternate-evaluator dispatch and migration state; validate full SQL,
  shared-library and release-server cohorts and report remaining performance costs.

## Surprises & Discoveries

N7e fresh RED enters a substituted ASCII panic kernel despite the official name. Fresh-factory pointer comparison cannot fix this safely across codegen units; a saved pointer from the same construction instance avoids that ambiguity.


The system compiler is Rust1.97.1 while the full repository pins
nightly-2026-08-22 (Rust1.100). Expression/Chunk-only tests also run on the system
compiler with RUSTC_BOOTSTRAP, but jemalloc mem-profiling requires the pinned
intrinsic signature. For caller/server validation use the existing
/home/agent/tidb/expression-unification/tools/cargo-tidb wrapper, not an allocator
source workaround. The initial offline Projection test also needed the locked
paste crate downloaded; --locked preserved dependency versions.

The existing public run_with_tikv_window catches every preparation error,
including ResourceLimit and contract errors, stores only a bounded refusal code,
and then calls self.run. This loses the error and can bypass the intended
resource policy. Execution failures after successful preparation already
propagate without replay. suite_shape also rejects all multi-output suites,
column mappings and owner swaps, even if their functions are individually
supported. The static 68/131 signature count is not all SQL call coverage. N1 exposed a
previously hidden malformed specification: catalog IS NULL permits JSON-to-String
coercion, but this batch source did not insert a cast. Check the shared signature's
actual argument/result carriers before compilation so this known capability gap
is classified as admission, rather than swallowing arbitrary InvalidSpec errors.

## Decision Log

- Decision: Snapshot generated RpnFnMeta name and all three pointers once in a private field, and check unchanged dispatch only at closed evaluated-recipe admission. Preserve checked FunctionRef selection, generic const macro construction, canonical shape checks and original raw/wire execution.
  Rationale: Reject mutated or renamed-other-factory calls before witness/kernel effects without executing a validator as a probe, adding a second dispatcher, or comparing independently emitted function addresses. Actual metadata sizes remain included in existing resource budgets.
  Date/Author: 2026-10-18 / TiDBEngineer.

- Decision: Keep native row operand demand separate from native batch output publication. A control/comparison root can require both. Stage final owned results only for the eligible promoted batch owner; global row mode retains its native prefix behavior. SDK/resource/SQL errors stay terminal and discard pending batch output.
  Rationale: The real native comparison demonstrated successful NULL short-circuiting and a different late-error output prefix; neither changing only the arithmetic kernel nor removing the structural guard preserves both contracts.
  Date/Author: 2026-10-18 / TiDBEngineer.


- Decision: Start by making resource/contract/evaluation preparation failures
  observable typed errors, cached before any value or context effects.
  Rationale: A no-fallback executor must not trade correctness/resource gates
  for coverage. Ordinary unsupported shapes remain explicitly unfinished until
  the subsequent capability milestones cover them.
  Date/Author: 2026-10-17 / TiDBEngineer.
- Decision: Reuse TiKV-owned algorithms and local execution; never add a second
  TiDB-specific function implementation to improve a route counter.
  Rationale: The goal is execution unification with compatibility, not renaming
  the current fallback or replacing valid SQL with unsupported errors.
  Date/Author: 2026-10-17 / TiDBEngineer.

## Outcomes & Retrospective

N7f retires another native head/finish Ready program pair and adds canonical generic bounded-staleness source execution. Fixed old behavior, callback authority and original argument-cast order are verified independently. Fullfront1644same3/all222batch, SDK1096same1, Projection26 and exact inherited lint diagnostics pass; global migration and remaining source/PB/oldcaller work remain unfinished.

N7e removes five real SDK dispatch-integrity failures without comparing independent factory pointers. All existing positive worker, resource, source and Projection checks retain their limits and behavior. SDK has1096pass/one inherited STR_TO_DATE worker failure; frontend1637pass/same3fail, datatype baseline564pass/same2fail. Full migration is unfinished, and parser differences remain explicit.


N1 regression reproduced the bug before implementation: the oversized metadata gate returned successful generic evaluation instead of a resource error. Cached typed errors now pass batch35 and full1387 with exactly the same4 inherited failures. The first candidate exposed the existing JSON-to-String catalog conversion gap; explicit carrier admission fixed its classification without swallowing InvalidSpec. TiKV is unchanged. N2 now also passes batch40, Chunk275 and the pinned-nightly production Projection caller tests9. Multiple pure outputs, literals, direct selected columns and vector-off one-row execution no longer trigger an alternate evaluator. Ordinary unsupported-function/context routing is still present; next is N3/N4, and the goal remains incomplete. STRCMP is now prepared in all25 benchmark size/selection combinations, with matching native checksums. The other five workload families still require migration. Native-only STRCMP is outside the old131-wire-signature denominator; do not inflate that count. Each self-contained code milestone must have targeted regression
coverage, required lint, and a Signed-off-by commit; temporary migration routing
must be reported explicitly until removed. Do not use the earlier performance
pass's cost boundary as authorization to stop this new goal.

## Context and Orientation


Worktrees are /home/agent/tidb/expression-unification/tidb and its sibling tikv.
TiDB rust/crates/tidb-expr/src/evaluator.rs owns projection scheduling and the
BatchLane state. evaluator/batch.rs authenticates program/schema/column bindings
and calls TiKV local execution. scalar_function.rs and pushdown_catalog.rs own
SQL argument demand and native-to-shared call selection. TiKV
components/tidb_query_expr/src/local/profile.rs contains the eager capability
contract; local program/frame execution and types/expr_eval.rs implement shared
execution. Existing generic evaluation also calls shared kernels but retains a
separate expression driver; merely relabeling it is not elimination.

## Plan of Work


N1 adds a cached terminal preparation-failure lane and preserves its typed
EvaluatorError across repeated calls, with a real oversized metadata regression
which fails before the change. N2 replaces whole-suite single-root assumptions
with per-output scheduling while delaying column-owner moves until all computed
outputs succeed. N3 connects existing shared call implementations, preserving
collation, packet limits, NULL and JSON representation/error behavior. N4 adds
row demand for IF/CASE/COALESCE/NULLIF/AND/OR without eager evaluation of skipped
branches. N5 inventories and closes every other entry and semantic domain,
including temporal/numeric policies, casts, host operations, filters and scalar
entrypoints. N6 deletes the now-unreachable legacy expression driver and records
source/route evidence plus SQL and performance verification.

## Concrete Steps


Run commands from the TiDB worktree unless noted. Use the established environment:

    export RUSTC_BOOTSTRAP=1 RUSTFLAGS='' CC=gcc-14 CXX=g++-14
    export CXXFLAGS='-include cstdint' CMAKE_POLICY_VERSION_MINIMUM=3.5
    export CARGO_HOME=/home/agent/tidb/expression-unification/cargo-home
    export CARGO_TARGET_DIR=/home/agent/tidb/expression-unification/target-tidb
    cargo test --manifest-path rust/Cargo.toml --release --locked --offline -j2 -p tidb-expr --lib evaluator::batch::tests
    make lint

For full callers, from the rust/ directory use the repository-pinned toolchain:

    RUSTFLAGS='' CARGO_BUILD_JOBS=12 /home/agent/tidb/expression-unification/tools/cargo-tidb test --release --locked --offline -j12 -p tidb-executor --lib projection::tests

Check the target's existing artifacts and toolchain before building. Save output
under /home/agent/.slock/agents/1555103c-9f3e-46ca-ac2e-b964c650c2f1/evidence/no-fallback/.
Inspect every changed file, run scoped rustfmt and git diff --check, then commit
only task-owned files with git commit -s. No broad git add and no destructive
cleanup of the unrelated BUILD file.

## Validation and Acceptance


N1 must return the same structured resource error on the first and second call,
with zero fallback, no eligibility/value reads and untouched input/output. Existing
supported workloads remain prepared and unsupported cases remain transparently
reported as migration gaps. Later milestones must compare against independent
results and frozen-native oracles, not only against the migrated implementation.
Include NULLs, reverse/duplicate/empty selections, over-1024 windows, all result
carriers, multi-output aliasing, short-circuit errors/effects and resource limits.
Final acceptance includes no production path replaying another expression
interpreter, supported SQL retaining compatibility, and published route evidence.
Full existing suites have four known TiDB and six known TiKV failures; compare
exact identities, never silently ignore new failures. Run release SQL/real-cluster
checks when changes reach their caller paths and stop owned test services afterward.

## Idempotence and Recovery


Tests and builds are repeatable. Each code checkpoint must be independently
runnable. Preserve rejected experiments as patches/evidence and reverse only
owned changes; never reset the workspace or rewrite the paired baseline. Update
this plan and workspace memory before each context transition.

## Artifacts and Notes


Baseline performance evidence is in docs/expression-unification/PERFORMANCE_EVOLUTION_REPORT.md.
The 64 workload benchmark includes six generic fallback examples; it does not
cover every SQL function or multi-output SELECT. New evidence must state exactly
what route and caller it measures and must preserve slow results.

## Interfaces and Dependencies


Keep EvaluatorSuite public caller signatures stable while migrating internals.
Retain typed EvaluatorError/ExpressionRuntimeFailure propagation. BatchLane may
cache an EvaluatorError (which is Clone) to avoid recomputing or losing a terminal
preparation failure. A future shared execution extension must continue owning
resource accounting and must not expose arbitrary Rust callbacks as an unchecked
escape into the old interpreter.

N3a discovery: a native collation literal adds a lowered graph node without adding a SQL call. Reindex lowered preorder ordinals while preserving original SQL source IDs; nested sibling calls test both identities. STRCMP is explicitly admitted only with a known constant policy. Existing opaque ready-value and row contracts remain closed.

N4a findings: existing ControlFrame already schedules lazy child programs
iteratively, but its unannotated selection was signed-Int-only, and its null
helper supported only lineaged Int/Bytes. Reusing it required typed selected
payloads, typed generated NULL and complete null discrimination, rather than
calling a native expression callback. BatchCallSignature describes actual
variable-arity controls while the historical batch_signature API remains the
fixed eager registry. BatchFacts snapshots determine the allowed occurrence
window without another source traversal/allocation. The frontend tiles all
control-containing graphs at width1; this is a deliberate initial performance
cost and is not an alternate expression route.

Native COALESCE changes temporal FSP on the selected value. Until that transform
is in the shared program, frontend admission requires matching temporal
kind/FSP. Conditions needing truth coercion, unsigned branches and effectful
calls remain explicitly unfinished. Eager arithmetic's row/vector argument
policy and source-shaped SQL error adaptation require separate validation;
these changes do not establish complete SQL diagnostic parity.

N3b: Native JSON_TYPE has representation-sensitive behavior: unknown tags and
opaque framing failures are SQL document errors, while malformed Literal tags
still report BOOLEAN and temporal tags report their native type without decoding
an instant. The existing report worker already uses the correct shared primitive;
the new SQL graph kernel returns its actual String/error instead of the opaque
report envelope. An explicit failure-kind enum bridges native JSON errors and
invalid UTF-8; a forged numeric code/message is not a disposition.

JSON_TYPE's direct JSON columns/literals now have an explicit raw-byte projection.
The original SQL declaration remains in NodeRecord and row-schema validation;
only its lowered checked carrier is Blob. This preserves every type-code byte
and payload without passing through wire JsonType. NativeChunk performs the
projection after reading under the original native schema; selected raw cell
copies preserve duplicates and NULL. Raw JSON literal source bytes are gated
before cloning. Computed JSON subtrees, non-String/Bytes text datum kinds and
other implicit coercions are still unfinished, not silently reclassified as
supported. No source or failure path calls the legacy expression evaluator.

N3c architectural decision: reuse the existing staged HostFrame instead of
adding another expression node scheduler. An explicit SQL-only host catalog
contains closed operation IDs and immutable signatures, never native expression
objects or evaluation callbacks. The production provider holds values/types,
ConcatDemand, task generations and the original native Columns interface. The
same shared driver evaluates every requested argument. Context errors are kept
as their original owned EvalError in a private failure slot, consumed immediately
after the frame failure; no code/message inference or re-evaluation occurs.

The original native ConcatPreparation now shares its demanded-prefix state with
the new provider. The generated RPN wrapper and direct PreparedConcatArgs entry
share one fallible join helper. Provider heaps have their own bounded ledger;
frame caches and results remain in the shared exact ledger. This is a separate
policy from statement/allocator peak accounting. Legacy row host catalogs remain
signed-Int-only and cannot authorize SQL batch hosts; every SQL host source and
catalog identity is snapshotted/revalidated, including dead branches and empty
invocations. Context hosts currently use singleton windows.

Accuracy correction: the historical scratch vector_allocations field sums
static input/call nodes across successful windows; it does not observe allocator
calls. Dynamic controls may skip nodes and hosts own additional caches/buffers.
The field is now documented as an estimate and the probe labels it
estimated_carrier_nodes. Historical reports/binaries remain unchanged.


N3d completed implementation: REGEXP_LIKE is a closed SQL
host operation with exact two/three Bytes arguments and signed LongLong result.
Its task requests arguments in the same frame driver, checking each demanded
UTF-8 value before requesting the next; NULL does not inspect the context or
pattern cache. The source captures only original native invocation handles and
constant-argument cache policy. After all demanded non-NULL arguments the host
binds the actual native context ID and uses PreparedNativeRegexpLike, the same
plain value primitive now used by the original native RPN kernel. Collation's
initial i flag precedes explicit match_type, preserving rightmost precedence.
Native errors retain typed causes and the frontend's existing error mapping.
The known cache-byte observer excludes opaque regex engine/TLS/allocator cost,
just as the previous native implementation; this is not full memory accounting.


N3d outcome: all six original benchmark fallback families are now prepared for
those exact SQL shapes. This does not retire the global alternate evaluator.
Next audit the source-literal preclone byte gate (String/JSON/vector/Decimal),
then extend native coercions and argument/result transport beyond ready wire
carriers while retaining one driver. Complete N4b/N5 before deleting migration
routing; replacing supported SQL by an unsupported error is not completion.


N1b outcome: the source-literal audit reproduced an actual gate bypass for all
four heap-bearing literal families. Borrowed logical payload charging now
precedes every temporary conversion and detached copy in this preflight.
Decimal source bytes are deliberately conservative, not physical heap numbers.
This closes a resource-policy gap; it does not expand SQL type/function coverage.


N4b1 outcome: the initial native integer CHAR cast no longer routes to another
expression evaluator, including explicit parser/rewriter cast nodes nested in
CONCAT/REGEXP. Metadata snapshots carry only original value-kernel parameters.
Do not generalize this to all string coercions: raw native Float32/NaN, hybrid
and other storage kinds need lossless argument transport before wider admission.
Continue broader native value/context coverage before retiring global routing.


N5a plan: add evaluator/batch_values.rs with an invocation-local store of actual
Datum owners. Shared frames transport only private Bytes handles, checked by
namespace, window generation, ABI role, source-node ordinal and selected-row
occurrence. Handles are minted only by native source bindings and unwrapped only
by the matching closed native operation. SQL bytes are stored as values rather
than interpreted as handles. Keep original native SQL declarations in source and
row-schema validation. Reset after frame/task completion and root consumption,
and drop on failure. Native payload charges are a conservative logical policy
separate from frame byte caches and external source/SDK transient allocations;
do not call this a full allocator tracker. Validate the store in isolation
before integrating native CHAR inputs. Use existing native value-only cast
helpers, never an Expression or ScalarFunction evaluation callback. Preserve
NaN bits, Float32 kind, Enum/Set names+ordinals, raw bytes/JSON tags, Decimal
coefficient/scale/declaration and temporal raw/FSP/kind. Add stale/cross-store,
wrong-role/source/row and resource-denial tests, then actual SQL/caller coverage.


N5a1 outcome: test-only ownership/handle protocol is runnable and verified for
all19 native storage kinds and adversarial provenance/lifecycle cases. Keep the
prototype behind cfg(test) until actual native input bindings and the closed
consumer are integrated. This checkpoint adds no production SQL coverage.


N5a2 outcome: native leaf inputs now retain actual Datum representations in an
invocation-local store. Original schema and source provenance remain checked;
private handles pass through the existing frame driver to a closed CHAR host.
It calls the existing value-only native cast primitive, captures exact warnings
under a logical budget, and publishes them after result/resource checks. No
Expression evaluation callback or alternate local program is introduced.

The N4b1 assumption that integer declaration flags identify the actual native
value kind was wrong: an Int(-1) declared UNSIGNED rendered UInt64::MAX. A RED
comparison reproduced this and the existing core result-kind registry now
propagates actual signedness through supported wrappers. This correction is
included here, along with UInt literals that cannot use the signed wire lane.

Native source/SDK decoding and formatting temporaries remain outside the
logical observer. This is not a hard allocator or statement memory bound.
General computed native inputs, native root outputs, remaining function/context
coverage and other callers still require work; global fallback remains active.


N5b2 outcome: rich leaf roots can now publish native values through the same
frame driver. Output handles use a distinct role and exact root-node/selected
occurrence provenance. All handles and retained vector/selection bytes are
checked before a window writes its first result. Publication borrows the owned
Datum and reuses Chunk::append_datum, matching native layout/normalization,
without a kernel clone or alternate expression call. Ordinary representable
Int/UInt/finite Real/String/Bytes literals retain the existing broadcast path.
Root columns with direct-column optimization disabled also use this ABI.
Computed native values and full lazy/effect/context coverage remain incomplete.


N5c1 outcome: native value transport now includes computed CAST producers. A
closed 1-Bytes-to-Bytes host receives actual native values and immutable cast
parameters/source/result types and mints a new handle with its own producer
ordinal and requested output role. CAST islands compose with each other and
with the existing CHAR bridge into CONCAT/REGEXP. Source metadata/payload gates
precede snapshots, and both native CAST hosts check borrowed input allowance
before the kernel clone. They never invoke an Expression or ScalarFunction.

Result coercion is the unchanged logic extracted from ScalarFunction into a
plain name/type/value function shared by both callers. Transitive audit caught
the old real-to-unsigned helper using evaluate_args_in; that route is removed
from the native helper. Both it and the framed core ABI now use the same direct
f64 primitive, retaining ties-even, Float32 f64 bits, nonfinite results and
exact overflow warnings. Core boundary/NaN assertions and frontend parity pass.

Generic CAST warnings go directly to the actual context, whose allocation
policy remains external. A later resource failure is terminal and never replays
the warning-producing kernel. The earlier CHAR-specific buffered warning policy
is retained. JSON CAST still needs its distinct typed argument admission and
conversion order; general typed parents/controls and other functions remain
outside these native islands. N4b/N5/N6 remain incomplete.


N5c2 outcome: JSON CAST validates its original source type at host start before
requesting the value, including a NULL vector source. After demand its metadata
selects the same literal typed conversion or nonconstant hybrid/carrier
normalization as the native caller, followed by the existing prepared JSON
conversion with original source and target flags. Constant and numeric-row
owners now delegate to these extracted plain value helpers; their type/value
fetch order and correlated/parameter/deferred behavior are retained. The host
owns only a literal-kind bit and immutable types, never those expression nodes.
This completes JSON inside the current CAST islands, not general parent/control
or source/caller coverage. The global alternate evaluator remains.


N5c3 correction: widening native argument recursion made a formerly unreachable
source-view guard reachable for valid SQL such as CAST(ABS(column) AS SIGNED).
That guard still reported a binding violation, causing a terminal prepare error
instead of preserving SQL that this migration has not yet connected. Unknown
native-value producers now report an admission capability gap. Actual binding,
resource and native execution failures retain their terminal behavior. The
compatibility regression test documents this temporary state and must be
updated as those producers acquire unified recipes; it is not a requirement
to keep fallback in the completed architecture.


N5d1 outcome: native nullable controls request one original argument at a time
in the same HostFrame, classify actual Datum NULL using the existing pure SDK
selector, and stop at the first non-NULL. Selected data passes through the
shared native result boundary and acquires the control node's own provenance;
all-NULL also mints an owned control result. No old evaluated-args/control
wrapper or expression evaluation callback is used.

Preflight now computes subtree ends in-place in linear reverse order, allowing
variable-arity descriptors to join exact child producer ordinals without an
extra traversal/array. Metadata/kind classification is bounded and stops its
sibling iterator when the node/depth budget ends. Source arity/node and pending
metadata policy checks precede potentially large argument declaration scans.

An intermediate preference moved already prepared COALESCE(IF(...)) trees into
a native island that cannot yet produce IF handles. Before committing, the
selector was restricted to complete native producer trees when choosing a
nullable root, retaining those existing typed fast paths. Missing native
producers still use the explicit admission migration state, never a terminal
protocol error. General native IF/CASE/logical/nullif and other producers remain
outstanding; this checkpoint does not remove global fallback.


N5d2 outcome: native IF and flattened CASE request only the original predicate
and selected branch. Truth conversion uses the same plain native_truthy surface
as ScalarFunction's original truthy_of, followed by native_if_choose_branch.
It does not substitute WHERE's warning policy or the presence of a handle.
CASE with no selected branch owns a new NULL result. The common selection
argument/result helpers retain provenance, budgets, native value kinds and
original return conversion. Source classification also recognizes non-Int
predicate domains while retaining complete-tree and bounded inspection checks.

The prior CAST(IF(...)) capability regression case now requires preparation
without fallback; ABS/PLUS producer cases remain explicit migration gaps.
Native logical AND/OR, NULLIF/equality and further producer/caller coverage
remain outstanding before the alternate evaluator can be removed.


N5d3 outcome: AND/OR use the shared scalar demand rule and actual context's
plain logical conversion before requesting the next argument. Only absorbing
false AND / true OR skip the right operand; SQL NULL still demands it. Direct
RPN logical kernels finalize nullable values without an evaluated-args program.
Borrowed payload gates precede copies and store borrows end before warning
callbacks. Each result owns the logical producer's identity.

Real parser/rewriter tests exposed its istrue_with_null(CAST(...)) wrapper. A
closed native truth host now calls the original warning-free truth conversion
and direct int_is_true<KeepNullOn> kernel. The preceding CAST retains diagnostic
ownership, avoiding duplicate warnings. Native NULLIF/comparison, arithmetic
producers, effects, source forms and other callers remain outstanding; global
fallback has not been removed.


N5e1 outcome: unary boolean predicates share the existing Truth frame with
IsTrueWithNull. Exact operation/name joins and argument provenance precede
conversion; each result is published with its own producer. Raw IS NULL skips
truth validation, UNKNOWN aliases retain it, and NOT keeps its distinct
range-sentinel diagnostic. Pure native_truthy and direct existing RPN value
kernels preserve their original NULL/error/warning policies. Negated predicates
compose the same positive and unary NOT kernels as their existing programs.

The old JSON IS NULL fallback assertion is retired: this route now prepares
and returns identical values without an intermediate JSON-to-text conversion.
This extends native producer closure but does not complete comparison, NULLIF,
arithmetic, effectful operations, source forms or caller integration.


N5f1 outcome: comparison domain/coercion preparation is now a shared pure value
module, and core profile tables expose direct calls to the same helpers as
their generated kernels. Owned metadata facts let a future compiled host keep
column/constant/type distinctions without expression owners. Existing metadata
queries remain lazy at their original branches. Native vector representation
copies require a finite source-plus-copy allowance before allocation.

Current comparison callers still adapt the prepared values back into their
existing argument program; this checkpoint deliberately records that remaining
work. N5f2 must connect the checked native comparison frame and preserve both
raw operand demands before any comparison coercion. It must then retire those
legacy callers as source/caller coverage expands; global fallback remains.


N5f2 outcome: fixed native comparisons now stage both original raw operands
through the shared HostFrame, retain only a checked fixed-size left capability,
gate both borrowed payloads before copies, and release store borrows before
context conversion. Pure typed integer input preparation is shared with the
original caller; its NULL priority and integer-specific errors remain distinct
from generic comparison preparation. Direct kernels finalize the result under
the original result coercion and own producer identity.

Constant classification is computed in the existing reverse source pass using
the original unfoldable-call predicate. It preserves duration-column/constant
and Decimal-column/text distinctions without an expression owner in the host.
Null-safe equality, NULLIF, arithmetic producers, effects, additional source
forms/callers and migration dispatch removal remain outstanding.


N5f3 outcome: NULL-safe equality uses the same native comparison demand and
publication protocol while retaining its distinct generic preparation policy.
Actual NULL presence and duration-only normalization call the original pure
core kernels directly. Datetime conversion NULL and sentinel-before-NULL
behavior remain unchanged. Existing callers share the extracted preparation;
their legacy adapter remains a migration dependency. NULLIF, arithmetic,
effects, source forms, callers and global dispatch removal remain outstanding.


N5f4 outcome: native NULLIF uses the same comparison demand/provenance/budget
protocol but retains its original literal operand policy and fixed collation.
It skips binary-operator signed-literal normalization, validates the actual
computed nullable Eq result and calls the original pure branch selector.
Selected first values come from their original native owner and gain the
NULLIF node's own output identity after shared return coercion.

The context trace audit restored the original configuration observation in
the preceding NULL-safe direct boundary and preserves it in NULLIF: both raw
arguments precede that read and comparison coercion follows it. NULLIF now
composes with the existing native nullable/conditional/comparison producers;
arithmetic, other functions, effects, sources/callers and dispatch removal remain.


N5g1 outcome: ABS now demands native values through the shared unary frame,
selects computation by actual Datum kind and publishes its own coerced result.
Pure primitives also serve the original registered RPN wrappers. Source-shaped
overflow diagnostics are borrowed, bounded and charged before formatting or
literal conversion; an unrenderable tree skips formatting entirely. The host
owns immutable diagnostic text, with no expression callback or evaluated-args
interpreter. Existing typed ABS trees retain their migration coverage when a
child producer is not yet native.

Next arithmetic producers must preserve their own operand-conversion and
short-circuit policy. In particular, integer PLUS/MINUS/MUL convert the left
operand and stop on NULL before demanding the right; the comparison frame's
unconditional raw right demand cannot be reused blindly. Numeric argument
conversion, source-shaped overflow rendering and configuration reads must be
extracted as plain policies before connecting their shared value kernels.
Remaining functions, effects, sources/callers and global dispatch still require
migration; the no-fallback goal remains active.


N5g2 outcome: add/subtract/multiply now have an owned prepared-value boundary
independent of expressions or argument programs. The original generic ladder
and typed integer callers share that preparation; the temporary adapter is
explicit. Pure compute invokes the original core value methods directly and
retains actual structured arithmetic failures. A24576-case comparison checks
values, native error types, warning contents and subtraction-mode reads.
Actual SDK Decimal budget refusal retains its typed resource cause and outer
Evaluation class, while local observer refusals retain ResourceLimit.

Next connect these values to the common HostFrame after extracting plain
numeric operand conversion and immutable source diagnostics. Preserve the
integer fast path's actual typed input normalization, generic binary-literal
normalization, context-read points and signature-specific NULL demand.
Do not reuse comparison demand as an eager arithmetic policy. Whole-source
formatting budgets must also cover diagnostic formatting work buffers before
capturing text; nested unsupported diagnostic shapes keep the original
unavailable-text behavior. Remove the temporary adapter only as remaining
source/caller coverage becomes native. The global no-fallback goal is active.


N5g3 outcome: additive producers now compose inside existing native CAST and
row-demand conditional boundaries. Numeric conversion is shared as plain
value policy, with original typed/generic modes, NULL demand, subtraction and
configuration reads. Failed typed probes use fresh argument requests within
the same HostFrame. The previous CAST(PLUS(...)) admission fallback is closed.

The implementation audit exposed a required distinction: existing complete
compiled arithmetic finishes the entire left operand batch and entire right
operand batch before arithmetic; scalar row signatures have their own NULL
short-circuit rules. Automatic native root selection changed those observable
phases. Existing root source selection and proved ABS/arithmetic typed closures
are therefore preserved. A bulk-aware extension of the canonical driver is
required before retiring root profiles that still use migration dispatch;
merely forcing them into row frames would violate the objective.

Source diagnostics are bounded before capture or formatting. A RED regression
also proved that eager formatting of a nonfinite raw JSON literal could fail
inside a dead branch. The compiler now retains the actual fixed literal and
first rendering disposition as immutable metadata; the runtime invokes the
original formatter only on actual diagnostic demand. Earlier unavailable text
still prevents a later formatting failure, preserving the original order.

Next complete bulk-aware orchestration and remaining source/caller profiles,
then migrate other functions, effects and dynamic sources and remove global
migration dispatch. The goal is not complete at this checkpoint. The N5g3
receipt records the phase evidence and regression commands for this change.


N6a outcome: the existing canonical HostFrame has explicit whole-window
callbacks with checked row maps, full-vector Fresh/Reuse and compact retained
copies. Nested window graphs preserve complete argument phases. Missing window
callbacks are terminal contract errors, never row retries. Five protocol tests
cover phase order, exact error identity, cache/failure scope, cancellation,
limits and unchanged row-host admission. Core1049 and frontend1464 pass with
exactly their inherited failure sets; Projection22, lint, clippy and formatting
checks pass. See evidence/no-fallback-n6a.md and paired TiKVae8e9f0.

Next implement frontend window providers and canonical row-host wrapping with
per-edge row/bulk demand semantics. Current mixed row/control descendants still
force singleton admission, and windows are bounded at1024. Whole-selection
phase preservation must precede wider root/caller promotion. No new frontend
root fallback is removed by this protocol-only step; all remaining migration
work listed above remains active.


N6b implementation: RowHostScope is a scheduling frame in the
existing canonical loop. Under an explicit window root, it finishes each
complete row-host operand subtree in original occurrence order before returning
that operand's full compact vector. It does not call any other evaluator.
Scoped row maps preserve original occurrence identity, including through an
inner window callback. Window start/reply therefore carry occurrence_start;
a column-only batch import cannot represent that identity and is bypassed
inside a row scope. Row roots and typed-control descendants retain singleton
admission at this step.

Acceptance is Window -> RowHost -> Window over repeated/reordered physical
rows, with exact read order and identities; an inner-row failure must preserve
the original boxed error and cancel all pending enclosing tasks. Whole-operand
Fresh must restart every row, and resource limits must remain terminal with
cleanup. Core1051 pass with the exact inherited six failures and one ignored;
frontend106 focused and full1464 pass with the exact inherited four failures
and95 ignored. All22 Projection tests, required lint, scoped SDK clippy,
pinned formatting and diff checks pass. Paired TiKVec9cc67 and
evidence/no-fallback-n6b.md record the completed SDK checkpoint.
Frontend window providers, per-edge numeric conversion and whole-selection
orchestration still follow this SDK step. Global dispatch remains unfinished.


N6c outcome: the original native batch reference owner now
shares plain source-domain selection, strict Int/UInt bit extraction/repacking,
and vectorized numeric cast completion with the future window provider. The
new value-only cast seam receives actual values, immutable field/domain facts
and bounded diagnostic metadata; it has no expression or evaluator callback.
Source-typed owner evaluation remains a separate phase and retains its original
literal-once broadcast. Matching source/target domains skip the cast entirely.

Discovery: eval_integer_batch has its own bulk integer arithmetic loop when
its operand domains are Int. It is not the row typed probe and does not always
use the generic eval_arithmetic_batch ladder. The frontend window provider must
represent this mode explicitly, preserving its bit normalization, overflow
formatting, context reads and result handling. Per-edge lowering must distinguish
that bulk owner from row arithmetic beneath row-demand boundaries. N6c validates
the shared value interfaces first; frontend window providers and root promotion
remain unfinished.


N6c validation: six numeric-argument tests pass, including120 new value/error
and diagnostic/context-trace comparisons. Matching-domain tests retain even
an actual Float32 carrier without context reads or eager diagnostic rendering.
Full frontend1465 pass with the exact inherited four failures and95 ignored;
all106 existing batch tests pass within that run. All22 Projection tests,
required lint, pinned formatting and diff checks pass. TiKVec9cc67 is unchanged;
its N6b core evidence remains applicable. See evidence/no-fallback-n6c.md.
Next implement actual window providers and per-edge lowering, preserving the
bulk integer versus generic arithmetic distinction described above. No new
frontend root fallback is retired by this value-interface checkpoint.


N6d outcome: NativeBatchHosts now implements explicit
window arithmetic callbacks over actual native capabilities. Window tasks own
a physical map and complete converted left operand; they share the row host's
generation namespace, task installation and cancellation. Each complete typed
source vector precedes numeric casts; literal normalization occurs once before
broadcast. Both operands finish before shared PreparedArithmetic.compute and
publication. The metadata distinguishes bulk integer mode, generic domain,
requested integer output and source-owner classes. Existing row metadata
explicitly has no window mode.

Initial existing batch106 tests pass. Direct new provider/reference tests are
running, covering empty/repeated selections, constants, integer signedness,
real/Decimal/string diagnostics, actual native overflow and input-failure order.
Their source descriptors are immutable; runtime bindings own values only.
Source lowering still emits existing operations: actual window root promotion
and whole-selection phases beyond1024 remain separate unfinished steps.


N6d validation:98 direct window/native batch cases pass with exact values,
native errors, diagnostics and configuration reads, including integer signedness
and overflow. An additional148 provider resource-refusal invocations prove
pre-input and post-left-input/pre-conversion refusal, no right demand or
context effect, and task cleanup. Full frontend1466 pass with the exact
inherited four failures and95 ignored; all107 batch tests pass within that run.
Projection22, required lint, pinned formatting and diff checks pass. The paired
TiKVe9b0a92 changes only the maintenance guide; SDK code remains N6b's ec9cc67.
See evidence/no-fallback-n6d.md.

Next linearize the provider's repeated payload bookkeeping before widening
selection phases, then implement whole-selection execution and per-edge source
lowering. Source promotion cannot reuse the caller's current1024 root tiling:
a right failure in the first tile would preempt a left failure in a later tile.
Audit actual SDK kernel width assumptions before choosing a checked wide-frame
profile or phase-internal tiling. This checkpoint still closes no frontend
root fallback; remaining family/caller/source/multi-output migration is active.


N6e outcome: explicit SDK window roots now record a
whole-selection contract and enter a separate checked profile in the SAME
canonical driver. Complete layout/resource checks remain; ordinary roots keep
1024 and row roots keep one. Typed-control descendants still constrain width
to one and are unfinished coverage, not permission to tile a window root.
The actual prepared root is rechecked at entry, preventing caller relabeling.

Discovery: Generated RPN vectors indexed a fixed1024 identity array even
though LogicalRows already has a dynamic-length Identical representation.
Checked wide vectors now use that representation; legacy vectors retain their
previous slice behavior. Generated kernel wrappers use usize iteration and
VectorArg logical-row indexing rather than a fixed-width lane array. The audit
covers all BATCH_MAX_SIZE/IDENTICAL_LOGICAL_ROWS references in the expression
crate and the generated evaluator access path; legacy row/decoded entry caps
remain intact.

The provider source/cast ledger now updates payload totals incrementally and
caches suspended-left bytes; RowHostScope similarly caches completed child
storage. This prevents repeated prefix/ancestor scans as a selection grows.
New2050-occurrence tests cover late-left versus early-right failure order,
original error identity, generated kernels, scalar broadcasting, nested row
scope identity, limits and ordinary-root guard preservation. Native arithmetic
also compares complete2050-row phases and results to the original batch owner.
Initial native tests pass; final SDK/frontend regressions are running.

Future source/caller promotion must also resolve actual row/bulk evaluation
BEFORE effects when vectorization is disabled. The current caller's forced
window_size=1 changes demand semantics if blindly applied to a bulk program;
ignoring that flag would also be incorrect. Both domains must use canonical
programs, preserving original eligibility and short-circuit rules, without a
legacy evaluator callback or fallback. This remains implementation work along
with general control wrapping, bulk ABS/CAST source phases and other families.


N6e validation: all ten SDK window protocol tests pass, including the2050-row
late-left versus early-right error, generated kernel/scalar broadcast and
2050-row Window -> Row -> Window identity cases. Core1054 pass with the exact
inherited six failures and one ignored. The native window tests cover102
primary invocations (100 native-reference comparisons plus two injected input
failures) and156 provider resource refusals, including four2050-row cases.
Full frontend1467 pass with the exact inherited four failures and95 ignored;
all108 batch checks pass in that run. Projection22, required lint, scoped SDK
clippy, pinned formatting and diff checks pass. Paired SDK7ac5b37 and
evidence/no-fallback-n6e.md record the checkpoint.

Source lowering/caller selection is still unfinished. Next implement actual
arithmetic root and edge lowering with correct pre-effect row/bulk domain
selection, then bulk ABS/CAST phases and general control wrapping. Both domains
must remain in the common driver; missing bulk coverage cannot silently become
row execution. Metadata/source expansion, other functions/effects, dynamic
sources, multi-output coverage and global migration-dispatch retirement remain
active. No frontend root fallback is claimed closed by N6e alone.

### N6f Decimal priority discovery and next implementation

Native `run_with_consumer` calls `vec_eval_decimal_arithmetic_in` before
`enable_vectorized_expression`, including for empty input and a disabled flag.
The fast arithmetic completes all left inputs, all right inputs, then checked
coefficient operations. Unsupported is a typed outcome; native then reads the
flag and follows the ordinary bulk or row rules. SQL and bridge/resource errors
are terminal. A successful fast stage skips the flag and generic return
coercion. Constants are converted before broadcasting, even for empty input.

The new storage domain prepares that contract without putting a marker in SQL
NULL or user bytes. Next, compile the fast stage into the same SDK frame driver,
using plain coefficient kernels already exported by the datatype crate. Its
value provider must own immutable metadata and values, never Expressions or an
old ready-arguments program. Only a typed Unsupported outcome may select the
subsequent canonical bulk/row stage; errors must never select another route.
The original native phase, not a preparation refusal, defines this transition.
Retained node/metadata budgets must include every prepared phase. Fast integer
column casts and empty constant conversion must be covered before root
promotion is accepted. Context traces in the uncommitted integration test are
intentionally exact and must not be relaxed.

### N6f implemented state (2026-10-18)

The priority stage is a bounded factory and a value-only window provider,
using existing Add/Subtract/MultiplyWindowNative descriptors and the existing
single frame driver. No new SDK evaluator or native Expr callback was added.
Factory literals convert once; raw column imports stop reading physical values
on representation refusal and complete the private capability vector with
typed Unsupported outcomes. Integer-column CAST shape/value layout is a pure
shared helper, also consumed by its original native owner. Fast input/schema
metadata and all prepared graphs share the suite's checked node budget.
The factory gates borrowed fields and literal payload before snapshot/conversion.

Operand vectors and final stage results authenticate every returned capability
before Unsupported can select a subsequent stage. Final MyDecimal conversion
must succeed for the entire selection before any output publication. The native
per-output getter is after priority decline, including for empty selection, and
is skipped after priority success even when its value would be false. Generic
row and bulk programs share the same source owner and driver; row mode does not
latch subsequent bulk execution to a tuning width of one. Only explicitly
whole-selection programs ignore tiling knobs.

Validated so far: 673 root/profile comparisons (including144 actual rewritten
SQL executions), 150 fast-source integer CAST cases and99 actual fast callback
cases, plus terminal resource, nine-node aggregate phase budget, stale-purpose
and retry cleanup checks. Existing tests provide the remaining native-value,
lazy control, source diagnostics and Projection coverage. Complete frontend1478
passes with the exact four inherited failures and95ignored; full suite remains
non-green. Projection23, lint, pinned fmt and diff checks pass; receipt
`rust/docs/evidence/no-fallback-n6f.md` records the signed checkpoint scope. The global alternate evaluator has not been retired.

Empty-selection audit: generic EligibleBatch::eval_native checks num_rows()==0
before any batch value worker, after the getter/admission point. Therefore the
ordinary generic program correctly has no value demand for empty selection.
The earlier literal-before-empty-broadcast rule applies to the separate native
Decimal priority function, which runs before that generic guard; its immutable
empty_supported evidence is deliberately distinct. Do not introduce synthetic
rows or an empty-expression interpreter to conflate these two owners.
Standalone CAST priority is not yet integrated; completing bulk CAST/ABS and
then broadening root promotion remains required before global retirement.

### N6g implemented state (2026-10-18)

The three unary operations now lower only through their genuine whole-child
recipes, replacing the earlier exclusion with actual implementation. The row
companion remains separate and uses the same driver. Arithmetic, ABS and CAST
share source-vector ownership/normalization; only their subsequent value
kernels differ. Source normalization is complete before ABS/Decimal/JSON
conversion. Integer-source Decimal conversion is also shared with its original
native owner. Neither an Expr callback nor an old ready-arguments program was
introduced. A borrowed Decimal work bound includes visible/storage padding
before a conversion can format it.

Standalone Decimal priority is a genuine one-argument cast window over the
original strict integer-column padding import. Its metadata now records the
actual one/two argument ordinals and checks that cardinality against the closed
catalog. A non-column/unsupported CAST shape becomes typed Unsupported without
child evaluation. Empty evidence still precedes the generic guard, and only
that typed result/final-cell representation refusal may continue to native's
subsequent canonical bulk/row phase. Every error remains terminal.

Verified: core11 window cases/full1055 with exact same6 failures/1ignored,
scopedclippy89warnings, prior122 frontend batch tests and336 new root/nested
comparisons; provider71 value/input-failure cases plus108 resource attempts,
source admission before data and1M-digit Decimal work refusal before context.
Additional12 rewritten SQL forms and a production mixed-column Projection test
pass in full frontend1483same4/95ignored, Projection24 and lint. Do not mark the goal
complete: control wrapping, other functions/callers, dynamic sources and ordered
effectful multi-output execution still require implementation and retirement.

### N6h control-demand investigation (2026-10-18)

The native comparison reproduced a real mismatch before changing production:
IF(1, NULL-column + (MAX_INT * 2), 0) succeeds natively but the typed unified
control subtree evaluates the overflowing RHS. Production control/comparison
roots with a closed native-value arithmetic subtree now select RootValue;
explicit old test-consumer policy is unchanged. A second comparison showed that
native eligible batches defer output publication across those row evaluations,
while global row mode may retain a successful prefix. PendingNativeOutput
therefore stages owned final Datums only for these eligible batch roots, under
checked cell capacity and conservative payload allowance before cloning. SQL,
SDK and resource failures never publish this pending batch or retry evaluation.

The remaining SDK change must scope typed control operands inside an explicit
window root, preserving original occurrences and full-parent argument phases.
Its RED fixtures cover all four selection controls, a logical condition,
Window→control→Window,2050 occurrences, Fresh/Reuse and failure/resource cleanup.
Root control and legacy entry limits must remain intact; allowing multiple rows
in a ControlFrame directly is not the implementation.


### N6h verified outcome (2026-10-18)

All new demand/publication/protocol regressions pass. Full frontend1485 and
core1058 pass with exactly the inherited four/six failure names,95/1ignored;
these are intentionally reported as non-green full suites. All126 batch and
14 window protocol tests, production Projection25, lint, scoped clippy and both
pinned format/diff checks pass. The SDK final test additionally exercises
Int/Real/Decimal/Time/Duration control outputs through Coalesce→IsNull→Substring
inside the window graph, including a wide Decimal value. Commit cff06c7 is local
and signed off. No push is authorized.

The next native comparison must audit output publication for other eligible
row-demand roots, including ABSReal/Decimal, and ordinary typed roots whose
selection exceeds1024. Do not infer their publication/demand contract from the
control fix. Broader functions, deferred/type-before-value sources, effectful
multi-output ordering and other callers remain required before alternate
execution can be removed globally.

### N6i native value publication audit (2026-10-18)

The next native comparison checks eligible Real/Decimal ABS roots that use row
operand demand despite the outer batch flag. Publication must be selected from
the actual native batch eligibility and compiled whole-selection contract,
independently of function spelling or a RootValue versus BulkRoot annotation.
The N6h control fix only staged one such owner class; that restriction is not a
final publication architecture. Typed ordinary roots and their>1024 operand
phases remain a separate required audit: output staging alone cannot repair
an operand phase order changed by root tiling.

The Real ABS comparison reproduced a successful output prefix on the unified
route before a late error, while native batch output remained empty. The first
revision generalized native-value result staging beyond control names. Its
expanded Real/Decimal ABS and CAST comparisons pass, but source inspection
found that batch_supported is weaker than actual native candidate eligibility:
non-deferred constants and columns are rejected by batch_candidate even when
their storage types are supported. A low-budget2050-occurrence leaf regression
therefore checks that such roots do not gain needless batch-owned result
storage. The final rule will share the native static scalar-function candidate
arm; deferred type observation remains in the original dynamic arm and is not
replaced with a compile-time guess.


N6i final static eligibility is shared with the native candidate owner through
static_batch_target, after source preflight. Direct constants/columns are not
mistaken for candidates; the32KiB/2050-occurrence regression now passes for
both. Actual batch row-root publication and row-mode prefix behavior match
with preexisting output cells. The initial full1488 result was superseded by
full1489same4/95ignored with all130 batch cases. Final source has no SDK change.
Do not retire or complete the goal at this checkpoint: typed ordinary roots,
controls with non-arithmetic ABS descendants, dynamic sources and ordered
multi-output execution still need semantic coverage.


N6i final validation is complete:25 production Projection tests pass after the
shared native candidate refactor, including the five-root unary matrix; lint,
all four changed Rust files' pinned format checks and git diff checks pass.
There are no active build/test handles. This checkpoint is ready for a local
signed-off commit; no push is authorized and the overall goal remains active.

### N6j control roots with non-arithmetic descendants (2026-10-18)

The production control/comparison promotion still requires a PLUS/MINUS/MUL
somewhere in the source tree. That does not prove row operand demand: ABS can
also fail on a later left row before an earlier right-row failure in an eager
typed graph. Compare EQ(ABS(column0),ABS(column1)) on rows(1,MIN_INT) and
(MIN_INT,1) against the actual native owner before changing that predicate.
If confirmed, native-value-closed control/comparison roots must select their
actual row program regardless of descendant spelling. Explicit old strict
test-consumer contracts stay separate, and unsupported source closures do not
gain fake value callbacks. Validate selection controls, logical/boolean
predicates and comparisons, then full frontend/production Projection/lint/fmt;
SDK code need not change for this source-domain correction.


### N6j verified outcome (2026-10-18)

The original regression reproduced an SDK Evaluation/Invoke wrapper instead of
native DataOutOfRange(BIGINT,abs(t.c1)). Selecting the actual native row control
program preserves both demanded row/operand order and the original SQL error.
The23-root matrix and actual SQL/Projection coverage pass, full frontend1491
retains exactly the inherited four failures/95ignored, and all required gates
for this frontend-only change pass. No SDK code changed and all build handles
are terminal. Sign the local checkpoint and continue typed output publication
and ordinary operand/source coverage; do not mark the overall goal complete.

### N6k typed final-output ownership (2026-10-18)

The actual CONCAT(CAST_CHAR(ABS(column)),"x") comparison reproduced a successful
prefix before a later error, while native eligible batch output remained empty.
Its children already use native row demand; the root publishes a typed Bytes
carrier, so the native-value Output-role staging predicate excluded it.
Generalize publication eligibility using the same static native candidate fact,
then capture typed carriers in one private single-column Chunk using the
existing output materializer. Check layout, retained owners and conservative
copy/conversion allowance before copying; measure actual buffer capacities
before further demand. Final range append must preserve preexisting output,
column alias/COW behavior and output position. Global row mode keeps streaming
and non-call leaf roots keep their original broadcast/row policy. All errors
remain terminal; there is no second expression evaluator or delayed conversion
after publication begins. SDK code does not need to change.


### N6k verified outcome (2026-10-18)

Typed batch output is captured by the existing materializer in a private
one-column owner and appended to the actual destination only after all windows
and conversions succeed. Existing-output aliases/prefixes and row error
prefixes match native. Shape and budget errors remain terminal. The capture
accounts actual retained buffers plus an explicit wrapper/copy allowance;
it does not claim a hard allocator peak.

The cross-carrier RED also proved an existing12ns Duration transport loss.
Native raw parts now preserve all signed nanoseconds while validating FSP;
SQL from_nanos still rounds. Datatype's two unchanged panic assertions were
reproduced at clean parent cff06c7, not merely presumed inherited. Frontend,
core and datatype full suites remain non-green with exact known failure lists.
All required scoped gates and305 new native comparisons pass. No build jobs
remain. Sign the local checkpoint and continue ordinary typed operand demand,
value bridges, dynamic sources and other callers; the goal is not complete.

### N6l bitwise raw-operand bridge (2026-10-18, in progress)

BITXOR(ABS(column0),ABS(column1)) reproduces the same ordinary-root gap:
rows(1,MIN_INT),(MIN_INT,1) return an SDK Evaluation/Invoke wrapper instead
of native DataOutOfRange(BIGINT,abs(t.c1)). Binary bitwise signatures demand
both raw operands per row, even when the first is NULL; only then do they
read div_precision_increment and perform the original value conversion ladder.
The ladder includes observable Decimal pre-conversion warnings and JSON text
conversion, so it must remain single-source rather than being rewritten.

Add a closed bitwise host family for AND/OR/XOR/shifts/NOT, raw capability
arguments and real output provenance. Preserve actual unsigned declaration
flags throughout recursive value coercion. Replace the old final ready-value
bitwise adapter with a direct shared SDK value kernel, also used by the
existing RPN workers. Source promotion must close the native value subtree
and preserve outer whole-child arithmetic/ABS/CAST scheduling. Bound payload,
Decimal scale work and JSON expansion before copy/conversion/effects. Validate
RED-to-green, all carrier/coercion kinds, row/whole compositions, terminal
resource errors and clean reuse, then full scoped gates before a signed local
checkpoint. The global migration remains active.


### N6l verified outcome (2026-10-18)

The original ABS/bitwise regression now returns the native SQL error from the
correct demanded row/argument. All six bitwise operators share final SDK
computation without ready-argument programs in the frontend value path.
13,012 actual native-owner profile invocations and42 direct provider cases
pass; whole-parent scheduling, original occurrence identity, actual context
reads, warning/error conversion and terminal resource/provenance failures are
covered. The full frontend has1500 passes and the exact inherited4 failures;
SDK core has1059 passes and the exact inherited6 failures. All141 batch tests,
15 window protocol tests,25 production Projection tests, lint, scoped clippy,
both pinned formats and both diff checks pass. The production matrix has15
output roots under both flags and serial/two-worker execution. All handles are
closed. Sign this local checkpoint and continue remaining ordinary typed
families/compositions, dynamic sources, multi-output effects and other callers;
do not mark the overall goal complete.

### N6m logical XOR coverage (2026-10-18, actual RED)

The actual XOR(ABS(column0),ABS(column1)) comparison does not reproduce an
error mismatch: admission refuses the root and fallback evaluates it correctly.
The new regression fails specifically on route counters(refusals=1,fallback=1),
which is a direct remaining gap in the active no-fallback goal. Both raw values
must be demanded even after a left NULL. After the binary div-precision getter,
range sentinels are rejected; both string numeric-warning probes run before
left then right truth conversion. The warning probes intentionally ignore their
conversion errors in the original helper. Preserve that contract and terminal
truth-conversion errors, and use a genuine closed XorNative operation with the
existing shared logical XOR computation. Do not use the AND/OR short-circuit
provider or identify logical XOR as a bitwise operation.


### N6m verified outcome (2026-10-18)

The actual XOR admission/fallback RED is now zero-fallback. The canonical raw
value host shares transport with bitwise operations while preserving separate
SQL operation identity, warning/truth rules and actual source declaration
facts. Original static native eligibility and configuration reads stay
unchanged; successful unified admission does not authorize vectorization.
3324 new native-owner comparisons and49 direct provider cases pass, including
strict warning-probe errors, original truth errors and low-budget million-scale
Decimal truth without unperformed formatting. All145 batch tests pass. Full
frontend1504 passes retain exactly the inherited4 failures; SDK1060 passes
retain exactly the inherited6 failures. All16 protocol/catalog tests,25
production Projection tests with18 mixed output roots, lint, scoped clippy89
inherited warnings, both pinned formats and both diff checks pass. Datatype
code is unchanged and was not rerun. All jobs are closed. Sign local commits
and continue ordinary source families/compositions, dynamic/deferred sources,
multi-output effects and other callers. The overall goal is not complete.

### N6n unary sign coverage (2026-10-18, actual RED)

Unary plus and unary minus over ABS(column0), rows1,MIN_INT, each refuse
unified compilation and fall back(refusals=1,fallback=1). Both independent
regressions fail on route counters while preserving the legacy ABS SQL error.
Preserve actual Datum kind, Float32 full-f64 payload, String collation, Decimal
declaration/storage metadata and constant-only integer promotion when adding
native-value producers. Owned unsigned/constant declaration facts suffice for
integer recipe selection; no Expression callback or fake ready child is needed.

The current value helper still selects11 old ready-argument recipes. Expose
single-source value kernels and retire those adapters before connecting the
new raw-value provider. The existing authenticated unary-minus failure mapping
is already known: ready_value.rs maps NativeUnaryMinusError bits/signedness to
DataOutOfRange(BIGINT,"-<original bits>"); signed MIN deliberately renders two
minus signs. Constant promotion and computed overflow must remain distinct.
Inspect old Decimal recipe budgets and actual conversion/clone work before
choosing new bounds; unperformed scale formatting must not be charged merely
because the shared transport also serves bitwise conversions.


### N6n verified outcome (2026-10-18)

Both unary sign admission/fallback REDs now execute through the canonical
native-value driver. Eleven frontend ready-argument recipes have been retired
in favor of shared SDK value kernels, with original integer-constant promotion,
computed overflow diagnostics, full f64 payloads and Decimal headers/scales.
The enlarged matrix caught and corrected an invalid omission of the public
ScalarFunction result normalizer. The separate CHAR qualification RED caught
compiler capability growth changing the original native signature set; native
eligibility and compiled transport now use independent signature facts.
2176 new native-owner comparisons,63 shared provider cases and10 qualification
assertions pass. Full frontend1509 passes retain the exact inherited4 failures;
SDK1062 passes retain the exact inherited6 failures. All150 batch tests,
18 protocol/catalog tests, the original unary expected-result tables,
25 production Projection tests with22 output roots, lint, scoped clippy89
inherited warnings, both pinned formats and both diff checks pass. Datatype
code did not change. All jobs are closed. Sign local commits and continue
source result flags, remaining operator/function families, dynamic sources,
effectful multi-output and other callers; the global goal is not complete.


### N6n additional source-flag probe before commit (2026-10-18)

The original Expression.eval applies ENUM_SET_AS_INT after evaluating a leaf or
scalar function. A wide SET ordinal2^53+1 under unary minus distinguishes its
exact integer view from real conversion (which loses one). Check flagged
literal/column inputs and flagged IF/COALESCE producer results before signing
the checkpoint. If normalization is missing from native-value transport, fix
that generic source/result boundary rather than narrowing valid inputs or
hiding the issue with an operator-specific conversion. Prior passing gates
remain evidence for their covered cases, not proof for this added concern.


### N6n result-view correction (2026-10-18, RED to green)

The valid SET ordinal2^53+1 probe failed: unary minus received an unflagged
Set instead of the public Expression.Eval integer view and lost one through
f64 conversion. Extract the original declaration/value rule and share it with
native constant/column imports and SQL result publication. Existing host slots
supply result types in O(1); unchanged constants stay borrowed until their
copy-budget check. Literal, column, IF and COALESCE result paths now all pass
(16 added flag comparisons), with no tree callback or new runtime path.
Final frontend1510 passes retain exactly the inherited4 failures; all151 batch
tests pass. SDK source stayed unchanged after1062 passes/exact inherited6 and
clippy89 inherited warnings. Frontend Projection25, make lint, pinned format and diff gates pass again
after the generic boundary correction. See evidence/no-fallback-n6n.md.
The overall goal remains active.

### N6o quotient value kernels (2026-10-18, in progress)

Before admitting MOD, / and DIV to native source programs, expose the original
SDK arithmetic as value-only entries shared by existing RPN workers. Keep all
four integer signedness policies, full-f64 outcomes, Decimal '/' disposition,
and Decimal DIV's separately demanded probe/fallback precision reads. The
borrowed Decimal DIV entry returns only its computed owned warning and outcome;
its existing wire adapter handles frame validation and report encoding. Do not
make the native source provider encode ready-child expressions or run another
RPN program. This preparatory checkpoint does not admit new SQL roots yet.
Validate independent boundary expectations and existing wrapper tables, exact
inherited full-suite failures, scoped clippy, frontend consumers and format.


N6o direct value validation now passes: SDK1066 tests with exactly the six
inherited failures/one ignored, frontend1510 with exactly the four inherited
failures/95 ignored. Lists mechanically match N6n. The initial added DIV test
used a malformed scale4 raw coefficient; corrected to actual1.0000/3.0000
without relaxing warning/demand assertions. Four direct value tests and the
original quotient recipe tests pass. Preserve the existing wire adapter's
wrong-kind frame rejection before borrowed core dispatch. SDK source changes
are limited to kernel extraction and adapters; no new frontend roots are
admitted. Final scoped clippy passes with89 inherited warnings; make lint, pinned SDK
format and both diff checks pass. All jobs are closed before signing local
commits and continuing source integration. The global goal remains active.

### N6p frontend quotient adapters (2026-10-18, in progress)

Replace the remaining native MOD, / and integer/Decimal DIV ready-argument
program adapters with the N6o SDK value entries. First compare the new plain
preparation/computed outcome policy against the still-existing adapter over
all original Datum fixtures, source signedness, precision values, strict
warnings, typed overflow and actual conditional getter traces. Only after
that differential gate passes, switch the callers and delete their adapters.
Keep bounded dense Decimal conversion, genuine NULL vs zero-divisor warnings,
and warning-before-integer-error behavior. Source root admission and full
row/whole-argument integration remain subsequent work, requiring actual route
REDs; this step must not silently change bulk demand or native eligibility.


N6p pre-retirement differential proof passes51388 cases, including exact
values/representations and context traces against the still-active old
adapters. Fixed witnesses captured before deletion preserve an independent
expectation after the caller switch. The native MOD/division preparation and
Decimal DIV frame/finisher adapters have now been removed; additive/explicit
legacy adapters and quotient source-root admission remain open. Full frontend
1513same4 and Projection25 pass. Added resource checks exposed an incorrect
new test assumption: the existing opaque boundary classifies only the outer
SDK Evaluation discriminant and retains NativeDecimalError::Resource inside.
Keep that contract unchanged; correct the assertion and check actual retained
causes privately for four quotient refusal paths. Final full suite/lint/format
and local signed commits follow. See evidence/no-fallback-n6p.md.


N6p final checks pass: frontend1515 with exactly the inherited4 failures and
95 ignored (names mechanically match N6o); all151 batch,25 Projection, three
old-adapter witnesses, resource behavior and four actual resource-cause checks
pass. Both lint runs, pinned seven-file format checks and both diff checks
pass. SDK/datatype production code unchanged and not rerun. All jobs closed.
Sign local commits, then continue authentic quotient source/root demand
integration and remaining global coverage; the overall goal is not complete.

### N6q native quotient source integration (2026-10-18, in progress)

Add independent MOD, / and DIV root regressions over ABS(column0), rows7,MIN,
to prove the remaining admission/fallback defect before source integration.
Extend authentic row and whole-argument descriptors through the same SDK
frame protocol, preserving source types, original quotient domain selection,
conditional Decimal DIV precision reads and original SQL diagnostic rendering.
The generic signature mode must not invent the additive-only typed fast probe
for quotient operators. Native batch eligibility remains a separate immutable
fact; growing compiled closure does not authorize native CHAR vectorization.
Whole left and right input phases must remain whole when the original owner
uses them; row callbacks keep their own NULL/argument demand. Resource/SQL
errors remain terminal, without a second evaluator or fake ready operands.


### N6q0 protocol outcome

The six quotient row/window operation identities and frontend exhaustive enum
consumer bridge are validated. SDK1068 passes retain exactly the inherited6
failures/one ignored; two new protocol fixtures pass. Existing-consumer frontend
1515 passes retain the inherited4 failures/95 ignored with exactly the three
new pending admission REDs filtered out. Those three tests were separately
rerun and still show the original refusal1/fallback1. This is protocol-only
compatibility evidence, not quotient SQL coverage. Clippy89 inherited warnings,
lint, both pinned formats and both diff checks pass; all jobs closed. Sign the
self-contained protocol checkpoint, keep the actual root regressions as local
TDD work, and continue genuine quotient factory/host metadata and value phases.
Neither the source-integration milestone nor the global goal is complete.

### N6q1 source implementation (in validation)

All three original ABS-operand admission REDs now pass with zero refusal and
fallback. Quotient operations have genuine row and whole-window lowering,
while the original additive-only typed probe remains limited to its old
operators. Int/Real MOD retains right demand after left NULL. Window source
collection/cast phases are unchanged, and quotient computation uses the direct
N6o/N6p value kernels with the original precision-read points. Detached DIV
argument text/wrap/strict facts preserve row decimal diagnostics; typed integer
DIV reports actual integer operands, and whole Decimal DIV reports its actual
converted decimals. Original diagnostic cast decoration is shared with the
new value layer. The old fake literal Expression used only to carry a real
source type has been removed. A shared raw DIV preparation policy serves both
old tree callers and the canonical provider's already-fetched value casts.
Expanded literal/column/type-pair/parent/control/selections tests are running;
full regression, resource/provenance checks and production Projection remain
before signing. No global completion claim follows from the three smoke REDs.


N6q1 expanded validation now passes all159 batch tests, including the three
original admission regressions and19,500 new source/value/context comparisons.
Direct whole-window tests cover105 SQL/source-fault cases and180 terminal
resource refusals. The existing fixed N6p pre-retirement witnesses remain
active. A proposed extra low-budget source probe was discarded: its raw
Decimal declared one million visible fractional digits with only one digit of
coefficient storage. Chunk encoding rejects it, and detached SQL diagnostic
formatting panics just as the original formatter does. Those setup failures
are not evidence of a resource-admission bug. No source work bound was changed
on that premise; the valid borrowed-kernel fast-path test remains active.
Production Projection now includes six quotient outputs in the existing mixed
serial/two-worker and vectorization-on/off matrix (28 outputs total). Full
frontend, Projection and lint gates are in progress before signing.


### N6q1 verified outcome (2026-10-18)

All159 batch checks,19,500 source/value/context comparisons,105 direct window
SQL/source-fault cases and180 terminal resource checks passed. Complete frontend
1523 pass, exactly the same four inherited failures/95 ignored, zero filtered;
fixed N6p independent witnesses remain active. Production Projection25 pass,
including28 mixed output roots with six new quotient outputs under both flags
and serial/two-worker execution. make lint, pinned frontend formatting for12
changed Rust files and both diff checks pass. SDK production/datatype code is
unchanged, so N6q0 SDK1068/same6/one ignored and clippy89 are reused rather than
rerun. All build/test handles closed. Receipt:docs/evidence/no-fallback-n6q1.md.
Sign this runnable source integration locally; no push or global completion.
Next work includes remaining ready adapters, source/builtin/composition closure,
other callers, global dispatch retirement and inherited-failure/final SQL gates.


### N6r remaining additive adapters (2026-10-18, in progress)

The canonical additive provider already uses direct computed values, but
ordinary generic and typed-integer callers still build evaluated argument
programs through PreparedArithmetic::into_evaluated. Capture independent old
result/context witnesses before removing those two adapter call sites, retaining
full IEEE bits, Decimal shape and vector elements. The existing32x32 fixture
matrix covers24,576 combinations; add1176 typed integer signedness/mode/boundary
comparisons. Preserve fixed old hashes and reproduction patch after switching;
do not compare only two new helper callers. Keep genuine finite-value budgets
and terminal resource causes, then fullfrontend/Projection/lint/format gates
before a signed local runnable checkpoint. Explicit SDK recipe compatibility
surfaces remain until their own consumers are retired.


N6r initial25,752 old-vs-direct comparisons pass. Before switching, expand the
32x32 generic matrix across Warn/Error/Ignore policies and observe actual
truncate-level reads:73,728 generic plus1176 typed integer comparisons. The
initial warning-only hash is preliminary evidence, not the final fixed witness.
Production adapters are still intact during this capture.


N6r deletion-before-switch proof completed:73,728 generic and1176 typed integer
pairs matched the independently active old programs, including exact f64 bits,
Decimal representation, vector bits and actual context events. Final OLD hashes
are7246d6c6e2263fe1 and6e729cf695ab6ca5. Reproduction patch retained against
TiDB223553c1/SDK6a36ba0. Generic and typed caller adapters now use the direct
compute entry, and PreparedArithmetic::into_evaluated plus Cells/result packers
were removed. Fixed old hashes remain asserted in tests; full regression and
lint are running before signing. SDK production code is unchanged.


### N6r verified outcome (2026-10-18)

Deleted remaining generic/typed additive argument-program adapters after74,904
independent old comparisons; fixed generic/typed witnesses remain active.
Complete frontend1524 pass, exact same inherited4/95 ignored/zero filtered;
all159 batch tests and old quotient witnesses pass. Production Projection25
pass (28 mixed roots, flagsT/F, serial/two workers). make lint, three-file pinned
formatting and both diff checks pass. All jobs closed. SDK source/datatype is
unchanged; N6q0 full1068/same6/one ignored/clippy89 is reused, not rerun.
Receipt:docs/evidence/no-fallback-n6r.md. Sign locally, no push; the global goal
remains active, with remaining comparison/other adapters and builtin/source
coverage, alternate dispatch retirement and final inherited/SQL gates ahead.


### N6s comparison argument-program retirement (2026-10-18, in progress)

Generic, typed integer/Decimal, actual NULL and NULL-safe comparison callers
still use temporary evaluated-argument programs, although canonical providers
already compute genuine PreparedComparison/PreparedNullSafeComparison values.
Capture independent OLD witnesses before deleting their descriptor conversion
and extra predicate execution. Expanded27-fixture generic157,464 +NULL-safe
26,244 comparisons span all six ordinary predicates, three collations, four
literal/column fact combinations and Warn/Error/Ignore contexts with actual
truncate getter/warning traces. Another1566 typed integer/Decimal/NULL cases
cover signedness and representation boundaries. Preserve original duration
text normalization versus datetime conversion NULL, finite resource errors
and actual native causes. Then fullfrontend/Projection/lint/format gates and
signed LOCAL checkpoint. No SDK production changes or global completion claim.


N6s independent old proof passed all185,274 comparisons before switching.
Actual OLD trace hashes:generic4e3678354b488fcd, NULL-safe93bb0d8b5b45573b,
typed3d32f94b793fd8a9. Saved test-only reproduction patch against
TiDB7e76f620/SDK6a36ba0. Removed six argument-descriptor constructors/conversion
and replaced five caller families with genuine direct compute. NULL-safe
presence/equality/duration normalization now uses the existing shared value
compute instead of extra argument programs. Fixed OLD witnesses are asserted;
new boundary checks cover zero-work Decimal comparison, original outer SDK
classification, vector terminal refusal and operator-before-bridge precedence.
Full validation is next; production SDK code is unchanged.


N6s initial full run passes1527 with inherited4 plus one new test-assumption
failure:Decimal comparison computes under zero budget because Grow Ord borrows
all words without scratch allocation (impl_compare::native_compare_decimal).
Correct the test to require that success, actual SDK evaluation failure from
malformed JSON transport, and actual vector ResourceLimit refusal. No production
change or weakened limit follows. Production Projection25 and make lint already
pass against the final production source; only the test expectation changes.
Rerun the complete frontend and final pinned formatting before signing.


### N6s verified outcome (2026-10-18)

Comparison caller adapters retired after185,274 independent old comparisons;
three fixed OLD witnesses remain asserted. Complete frontend1528 pass, exact
same inherited4/95 ignored/zero filtered, all159 batch and older witnesses pass.
Actual zero-work Decimal, SDK evaluation error, terminal vector resource and
operator precedence checks pass. Production Projection25 pass(28 mixed roots,
both flags, serial/two workers); no production changes after that gate. make
lint, pinned formatting for two changed Rust files and both diff checks pass.
All jobs closed. SDK production/datatype unchanged; N6q0 full1068/same6/one
ignored and clippy89 reused, not rerun. Receipt:docs/evidence/no-fallback-n6s.md.
Sign locally, no push. Remaining adapter/source/builtin/caller coverage, global
alternate dispatch removal and final inherited/SQL/server gates still ahead.


### N6t Boolean/logical argument-program retirement (2026-10-18, in progress)

Boolean and logical normalized-value entry points still dispatch temporary
Ready argument programs while canonical providers already use direct shared
kernels. Capture OLD outputs across all eight Boolean operations, all three
logical operations and true/false/NULL, explicit demanded/undemanded-right
markers, no-cache/fresh-cache/reused-cache profiles and repeated passes:720
cases. Invalid undemanded markers must fail before cache lookup and retain the
original private ExpressionAdapterFailure Scope/Contract kind/reason; they
must not be recast as runtime/resource/SQL errors. Verify private cause fields
inside the adaptation boundary. Then move the exact marker policy to a plain
value helper, call direct impl_op kernels, and share those helpers with the
canonical provider after its original conversion/demand. Preserve all original
truth/warning/source effects. Fullfrontend/Projection/lint/format gates and a
signed LOCAL runnable checkpoint remain before reporting this stage finished.
No global completion or SDK production change is implied.


N6t OLD720-case witness passes, captured hash8c5608c8b554ec31. A private
adaptation-boundary test confirms all seven illegal markers retain the exact
original Scope/Contract cause/reason. The test-only reproduction patch is
saved against TiDB473b648a/SDK6a36ba0. Before production changes, two new route
regressions trap actual old Ready-worker lookup for legitimate normalized
Boolean/logical calls; after migration they must pass without that lookup.


N6t both new worker-lookup regressions fail against the old callers at the
actual cache lookup, proving the secondary program path. Public wrappers and
the direct predicate value module are now connected; old logical Ready adapter
removed. The first edit script stopped at a stale canonical-host function name
(finish_truth is authoritative). Its earlier public-wrapper changes remain;
finish canonical helper sharing and install the captured OLD fixed hash after
the current compatibility build closes, then rerun final full gates. No hash
may be recaptured from the partially switched code.


The partial-wrapper compatibility run passes1532 with exactly inherited4 and
95 ignored; both real old-worker REDs are green. Canonical truth/logical hosts
now share the same direct helpers after their unchanged source conversion and
short-circuit demand. The captured OLD hash is now a fixed assertion. Final
fullfrontend/Projection/lint/format gates follow this completed implementation.


### N6t verified outcome (2026-10-18)

Boolean/logical secondary worker routes retired and canonical hosts share
plain normalized value helpers. OLD720 witnessfixed8c5608c8b554ec31 and actual
worker-lookup RED-to-green tests pass; original private marker Scope/Contract
kind/reason retained. Complete frontend1532 pass, exact inherited4/95 ignored/
zero filtered; all159 batch and prior fixed witnesses pass. Projection25 pass
(mixed28 roots, both flags, serial/two workers). make lint, pinned formatting
for eight changed Rust files and both diff checks pass; all jobs closed. SDK
production/datatype unchanged, so N6q0 full1068/same6/one ignored/clippy89 reused,
not rerun. Receipt:docs/evidence/no-fallback-n6t.md. Sign LOCAL, no push; other
adapters and broader source/caller/global dispatch/final SQL gates remain.


### N6u control identity argument-program retirement (2026-10-18, in progress)

IF/CASE/IFNULL/COALESCE/NULLIF still encode selected native values as identity
frames and execute secondary Ready programs. Canonical providers already use
shared pure branch/nullable selectors and native owned values. Before switching,
capture OLD selected representations and exact source/config/warning traces:
40 native Datum fixtures (including raw Decimal shapes, full Float32 f64 bits,
nonfinite vectors, opaque names), all five control families, no/fresh/reused
cache profiles, zero-pair CASE/base initialization, empty/all-NULL/2050-prefix
COALESCE and eager nullable NULLIF comparisons. Planned7680 result/demand cases.
Add actual source SQL/resource error identity/no-replay and invalid-comparison
scope-cause checks, plus worker lookup REDs, before deleting old stages.

Use genuine existing native_if_choose_branch/native_if_null_choose_first
selectors. The original coalesce_end_native recipe calls public
impl_compare::coalesce_bytes(&[]), so genuine exhaustion can use that same
empty-remaining-list value entry without a fabricated NULL input or new SDK
logic. Owned selected values should move; borrowed selected values need a
bounded owned copy. Retained output/copy allowances must account actual payload
without formatting visible Decimal padding or preserving obsolete packet work.
Source/resource errors stay terminal and are never retried through another
engine. Preserve actual original context properties and conversion locations;
no Expr callback/fake ready-child tree is added to the compiled provider.
Fullfrontend/Projection/lint/format gates and signed LOCAL commits remain
required; no global completion follows from this adapter checkpoint.


N6u initial fixture import failed Rust visibility (E0603), corrected only the
cfg(test) module visibility to reuse the actual32-case existing fixtures.
Snapshots now spell all raw representation fields, avoiding invalid Decimal or
duration formatting in the oracle. Source-fault matrix adds78 SQL/resource
error cases with original opaque capture identity, no source replay, exact
warning/config prefix and successful later reuse. Five actual worker-lookup
regressions and private invalid NULLIF scope-cause checks are compiled alongside
but remain separate from OLD proof filters. Production is still unchanged.


N6u OLD proof passed7680 full-representation/demand/context cases, fixed trace
8addb0a0f0899f79, and78 source SQL/resource error identity/no-replay/warning/
reuse cases, fixed trace4576b55725e08f88. All five worker-lookup traps fail old
code; private invalid NULLIF comparator causes match exact Scope/Contract kind
and reason. Reproduction test-only patch saved against734d3745/SDK6a36ba0.

Five frontend control wrappers now use genuine shared selectors and direct
owned values; old head reports, identity codecs, Ready programs, scoped context
rebinding and CASE OnceCell protocol are removed from those callers. Empty
candidate lists use existing coalesce_bytes(&[]) kernel policy. Selected owned
outputs are checked then moved; borrowed outputs check logical source-plus-copy
payload before clone, under the existing finite default64MiB allowance. No
Decimal display padding, frame header or obsolete packet work is charged.
Original contexts and source conversions remain authoritative. No SDK source
change was needed; old compatibility recipes/identity codecs used by other
families remain.

The initial post-switch complete frontend passes1540 with exactly inherited4/
95 ignored; all159 batch, fixed OLD witnesses and five route regressions pass.
Additional selected-owner budget/raw-metadata tests are running with the final
frontend/Projection/lint gates before signing. Logical retained allowances do
not promise recovery of allocator failure or whole-statement peak bounds.


### N6u verified outcome (2026-10-18)

Five control identity argument-program wrappers retired. Fixed OLD7680-value/
demand and78-source-failure traces pass; all five actual worker-lookup REDs are
green, exact private invalid NULLIF scope cause retained. Complete frontend1541
pass, exact inherited4/95 ignored/zero filtered; all159 batch and earlier fixed
witnesses pass. Selected payload/finite budget/raw metadata/no-format checks
pass. Production Projection25 pass(mixed28 roots, both flags, serial/two workers),
make lint, pinned formatting for ten changed Rust files and both diff checks
pass. All jobs closed. SDK production/datatype unchanged; N6q0 full1068/same6/
one ignored/clippy89 reused, not rerun. Receipt:docs/evidence/no-fallback-n6u.md.
Sign LOCAL, no push; remaining families/source/caller coverage, global alternate
dispatch retirement and final inherited/SQL/server gates still remain.


### N6v IN source closure and value migration (2026-10-18, actual RED)

Three authentic SQL-source regressions (generic ABS operand, duration columns,
computed JSON) each show one refusal and one fallback. They stay active until
genuine IN host/source integration fixes them, not ignored or permanently
refused. Ordinary generic IN compares every candidate, even after NULL/match;
prepared strings use only the original stored dynamic indices and late
coercion/collator resolution. Typed temporal/JSON IN retains ALL evaluation then
ALL casting before membership, with distinct first/member JSON cast modes.
Cast argument temporal helpers are already pure SDK value entries.

Fresh source inspection corrects a prior design assumption: typed-IN wire
validation checks complete identity framing, not domain membership. Wrong-domain
non-NULL values are accepted and compare false; they must not gain a new error.
Arity<2 is a distinct invalid-frame contract. Begin with genuine borrowed typed
values and shared encapsulated membership/prepared-key state, preserving the
old frame recipe as an independent oracle until comparison proof is captured.
Then remove frontend typed frames and connect genuine vararg source demands in
the canonical driver. Value/protocol availability alone cannot satisfy the three
pending admission REDs. No global completion or push is authorized.


N6v actual root cohort confirms three refusal1/fallback1 REDs, with matching
SQL results before the route assertion. OLD typed value proof now spans45 raw
native fixtures, all four typed domains, four list arrangements and three cache
profiles, plus arity0/1 contract errors:97,752 cases. It includes wrong-domain
values deliberately, because the old private interface accepts their frames
and compares false. A separate actual worker-lookup trap remains RED until
frontend typed frames are retired. Backend production has not changed yet.
Proposed allocation-free typed view iterator must only view already-fetched,
already-cast values; it cannot fetch expressions lazily or bypass the original
source/cast barrier. Encapsulated membership/prepared-key probe state will let
future canonical host tasks reuse the exact existing policy without callback
expression evaluation or host-precomputed membership answers.


N6v OLD97,752 typed value/arity results captured hash e128f6d8e515d162.
Independent comparison of the still-active old frame recipe against the new
borrowed value helper passed all cases with the exact same OLD hash before
switching the wire body. Historical test-only reproduction patch is saved
against0ab81038/SDK6a36ba0. The typed Ready-worker lookup trap fails old code.
The borrowed API now serves frontend typed values without encoding, and the
ordinary wire recipe delegates after its unchanged full-frame validation.
Wrong-domain non-NULL values still compare false; arity errors retain the old
opaque scope cause. Encapsulated membership/prepared-key state exposes existing
SDK policy for future canonical host steps, not host-precomputed membership.
Core tests and frontend value tests are running. One new low-level time test
uses raw0/1 as unequal, but Time Ord clears packed FSP/type bits; after builds
close correct only that test to assert equality and use1<<32 for a different
core. This is a test assumption, not a production semantic change. Three actual
source admission REDs remain explicitly pending canonical IN host integration.


### N6v0 typed IN value checkpoint (2026-10-18)

Corrected only the new time-test assumption: native Time Ord clears packed
metadata, so raw0/1 compare equal; raw1<<32 supplies an actual different core.
SDK final full1071 pass/exact inherited6/one ignored/zero filtered, clippy89
warnings unchanged. Existing-consumer frontend1544 pass/exact inherited4/
95 ignored with exactly the three UNCOMMITTED source admission REDs filtered
for this value-only compatibility gate. Separate current-binary rerun confirms
all three still refusal1/fallback1; they remain active, not ignored and not
part of the value-only commit. The complete tracked frontend checkpoint will
therefore have zero filtered tests when these pending source tests are absent.
OLD97752 hash e128f6d8e515d162 remains fixed; exact private arity scope cause
and retired Ready-worker lookup test pass. Original reproduction patch passes
cached apply-check against0ab81038 using a separate temporary Git index.
Production Projection/lint/fmt/diff and receipt are the remaining checkpoint
gates. Then sign local commits and continue actual canonical IN source closure;
legacy int/bytes loops and global fallback retirement are still outstanding.

N6v0 final checkpoint: production Projection25 pass, mixed28-root matrix both
flags/zero-two workers. make lint, pinned rustfmt9Rustfiles and both diff checks
pass. All jobs closed; exact inherited failure comparisons saved. Receipt
`docs/evidence/no-fallback-n6v0.md` records the filtered compatibility gate and
separate still-RED source gate explicitly. Sign SDK value implementation/guide
and frontend typed adapter/tests/receipt; leave batch_tests.rs source regressions
unstaged for next actual integration. No push; overall goal remains active.


### N6v1 genuine IN source closure (active)

Add one vararg row-scoped InNative operation to the same SDK driver. Immutable
metadata carries actual original cache keys/dynamic indices, source fields and
comparison facts. Typed tasks demand all source capabilities before cloning/
casting the complete value vector; generic/prepared tasks compare every required
candidate through the shared generic preparation and SDK membership state.
No expression callbacks, fake literals/Ready programs, cached membership answers
or permanent source refusals. Preflight budgets detached metadata, tasks charge
capabilities/clones/conversion work before allocation, every reply authenticates
actual role/origin, and SDK cancellation owns task cleanup. Three existing REDs
stay active. No new checkpoint until source/effects/resources/projection gates.


N6v1 first genuine source gate passes all three original REDs and three demand/
cast/cache regressions. Complete frontend1552 pass/exact inherited4/95ignored/
zero filtered, then expanded raw-source matrix yields1554 pass/same4/95ignored/
zero filtered. Generic8192 source cases x four flag/reuse phases compare against
unchanged original ScalarFunction evaluation, including raw representations,
unsigned facts, NULL placement and original SQL failures. Duration-column versus
constant-text facts, four temporal/JSON domains, source-error-before-cast order,
prepared-hit dynamic errors, 2050 repeated/late-error selections, nested row IN
inside whole arithmetic and controls pass. Direct capability tests authenticate
stale/wrong-role/wrong-node/wrong-occurrence inputs after earlier matches; actual
resource refusal, repeated cancellation and exact-provider reuse pass in three
domains. SDK1071 pass/exact inherited6/one ignored/zero filtered. Projection
mixed31roots and clippy are still running; lint/fmt8files/bothdiff final gates.

N6v1 FINAL: frontend1554 exact inherited4/95ignored/zero filtered, all169 batch;
SDK1071 exact inherited6/one ignored/zero filtered, scoped clippy89 unchanged.
Projection25 pass with31mixedroots under both flags/zero-two workers. make lint,
pinned fmt8Rustfiles, both diff checks pass; all45599/36345/4068/74634/76715
closed. Three originally uncommitted root REDs are now green and included in
this coherent source checkpoint. Receipt docs/evidence/no-fallback-n6v1.md;
SDK maintenance guide reconciles source admission and contracts. Sign local
commits, no push. Remaining other source/function/caller/legacy/global dispatch
and final inherited-failure/SQL/server gates keep goal/task4 ACTIVE.


### N6w legacy IN Ready-loop retirement (active)

Previous turn made authoritative progress: signed N6v0/N6v1 value and genuine
source integration, with all3 actual source REDs green. Continue the complete
goal, not a global completion claim. Legacy integer/byte channels still use
serialized head/step reports and scoped Ready workers in tikv/in_list.rs.
Capture independent OLD return/demand/context/warning/source-error identity
and late collation behavior before production changes, plus two actual worker
lookup REDs. Mandatory operand0 for count0/1, fulli128, base-NULL exhaustive
reads, early match, generic E identity, signed original ID and late mutable
mode resolution are contracts. Add pure genuine shared SDK value/progress
policy, reusing existing comparison primitives over original typed source channels,
without manufactured Exprs or a nested Ready evaluator. Retire caller frames after independent comparison proof;
bound actual retained payload/comparison work with finite logical allowances.
Validate frontend/SDK/real unistore consumers, Projection, lint/fmt/diff and
sign only the coherent runnable step. No push/PR/deploy/subagents authorized.


N6w actual OLD proof captured before production changes:27360 return/demand/
context hash03fe1f325a53c789,336 original generic SourceE Arc-identity/retry
hash7643f3333ea24bfd,160 late-mutable-collation hash86adc5e714cb9209. The late
mode test runs a single-test child process, never mutating the normal parallel
suite or adding an ignored test. Both actual worker lookup traps fail old code.
Test-only reproduction patch targets762c0152/SDKe5da224 and cached apply-check
passes with a separate Git index. All three cohorts then independently compare
old active head/step workers to new SDK values BEFORE either old wire body or
caller is changed (n6w-old-vs-legacy-values.log). Fixed hashes are never
regenerated from new code; snapshots preserve original and independent tests.

After proof, frontend legacy drive/head/step/scoped-context machinery is removed.
SDK wire validation/protocol remains unchanged and delegates equality/no-match
primitives. Source E returns unchanged; only explicit new SDK ResourceLimit
maps to existing ResourceLimit/phaseNone. Finite actual byte capacities and GB
binary encoding work are bounded; no retired frame charge or unperformed string
work is imposed on empty/streaming values. Whole-heap/source-provider allocation
recovery is not claimed. Source callback timing and late original signed-ID
policy remain intact. Complete frontend1560 pass/exact inherited4/95ignored/
zero filtered, including fixed OLD proof, both worker traps and actual-capacity
refusal/no further read/retry checks. SDK full/Unistore consumer/Projection/
clippy are running; lint and pinned fmt5Rustfiles/bothdiff pass. Do not sign
until all remaining results are reconciled. Overall goal remains incomplete.

N6w FINAL:frontend1560 exact inherited4/95ignored/zero filtered, all169batch;
SDK1073 exact inherited6/one ignored/zero filtered, clippy89 unchanged. Both
actual Unistore cophandler IN tests pass; Projection25/mixed31roots bothflags/
serial-two workers pass. make lint, pinnedfmt5Rustfiles and bothdiffchecks pass.
All53550/80104/40099/11371/5469/72773 CLOSED. Receipt docs/evidence/no-fallback-n6w.md
and SDK guide record actual original source channels, independent OLD proof and
finite logical capacity/work boundary. Sign local runnable commits, no push.
Other source/function/caller/global dispatch and final inherited/SQL/server gates
remain; goal/task4 NOT complete. Next inspect INTERVAL's eager/lazy value and
source closure together, with actual source REDs before changing its transport.


### N6x INTERVAL value and canonical source integration (active)

Previous goal turn is progress: N6w signed actual legacy channel computation and
caller program removal with complete local gates. INTERVAL retains two native
Ready controllers and no canonical source closure. First reproduce actual
integer/NN-real/computed-JSON source route REDs and independently capture OLD
eager/lazy results, precise cast/read/warning/context/error traces, arity/private
cause and worker lookup traps. Eager sentinel-before-NULL and all-real conversion
before <= partition differ from lazy metadata-selected linear/binary < probes;
raw NaN, mixed signed/unsigned, missing fields and full flags must remain exact.
Expose real borrowed input classification and typed search progress in SDK,
then retire caller transport and emit a genuine same-driver vararg row host.
No Expr callbacks/fake literals/another SDK program in canonical host; no new
permanent valid-SQL Unsupported. Preserve finite resources, source identity,
authenticated capabilities/cancellation, whole-parent scopes and native return
publication. No push/PR/deploy/subagents, overall goal remains incomplete.

N6x OLD proof captured at95399873/SDK3a7b17c: eager73728 fixedhash
319a9e8733ea8257; lazy294912 fixedhash242c1ac24fae8178; lazy source960
fixedhash2bb943c97c73bb21. Test-only baseline patch cached apply-check passed;
original and independent comparison snapshots preserved. Actual three source
REDs each refuse/fallback once, and all three actual Ready lookup traps fail
before replacement. First helper-insertion script failed before writing, so
n6x-old-vs-interval-values.log is OLD-only evidence, not independent comparison.
Actual independent comparison found an arity0/1 difference: the original codec
allows these counts until evaluated-argument admission produces InvalidBatch
at Invoke, with its exact closed-input-shape reason. Reused that actual SDK
producer via LocalError::evaluated_argument_shape_mismatch; did not reclassify
it as ScopeContract or RuntimeInputContract. All369600 independent comparisons
then passed BEFORE old controllers/wire primitives were changed. Fixed OLD
hashes are never regenerated from the new API.

New SDK borrowed eager classification/state and NativeIntervalSearch now own
actual policy/progress. Frontend interval.rs no longer constructs Ready programs,
identity heads, continuation reports or scoped context adapters. Original cast
helpers are already direct SDK value operations; no extra cast evaluator is
needed. Ordinary explicit wire profiles keep original complete validation and
protocol, sharing classification/search only after proof. Eager state/views and
demanded conversion work use finite allowances; huge Decimal padding is checked
before formatting, and skipped probes never inspect payload/work.

Canonical IntervalNative source host snapshots complete FieldTypes and producer
ordinals, authenticates actual nonlinear probe capabilities, casts only SDK
requests, and publishes through original SF RT/outer expression flag seams.
All3 source REDs turn green; 12288 raw-source/metadata cases pass. First full
frontend run had an additional parent-test refusal: the NEW fixture used
NOT_NULL schema columns but nullable declarations for another use of the same
column. Diagnostic PreparedBatchSuite::compile correctly reported complete-row
binding mismatch. Corrected fixture declarations, retaining all parent/short-
circuit/error/duplicate/reuse assertions, and the targeted parent test passes.
No production admission relaxation or permanent unsupported case was added.
SDK full1076 passes with exact original6 failures/one ignored/zero filtered.
Provider provenance/resource/cancel/reuse and mixed34-root Projection plus final
frontend/clippy/lint/fmt/diff gates pending. Do not sign until reconciled.


N6x canonical provider provenance/resource/cancel/exact reuse checks pass,
including actual nonlinear0→2→1 probes, demanded huge Decimal resource errors
and clone precharge before effects. Final frontend1574 passes/exact inherited4/
95ignored/zero filtered, all175batch; SDK1076/exact inherited6/one ignored;
clippy89 unchanged, lint passed. Expanded single34-root Projection encountered
existing finite equal-share source metadata refusal in OLD slot2 CAST_JSON,
BEFORE any INTERVAL. Diagnostic records exact LocalResourceLimit source metadata
cause; do not enlarge policy or recategorize as Unsupported. Final fixture
preserves complete prior31 roots and adds mixed8 roots spanning arithmetic,
string, three IN domains and three INTERVAL domains under unchanged limits.
Projection rerun and final lint/fmt/diffs pending; all temporary print diagnostics
removed. This is a genuine finite resource envelope, not a new source refusal.


N6x FINAL:complete frontend1574 exact inherited4/95ignored/zero filtered,
all175batch, SDK1076 exact inherited6/one ignored/zero filtered; fixed369600
original witnesses and actual3source/3worker REDs now green. Scoped clippy89
unchanged. Final productionProjection25 PASS: original mixed31-root matrix
intact plus mixed8 roots with new INTERVAL domains, both vectorization flags and
serial/two workers. Final lint, pinnedfmt14Rustfiles and bothdiffchecks PASS.
All33239/1413/94325/63902/22122/76871/8779/66738/65566/57106/4322 jobs CLOSED;
firstProjection34root metadata envelope documented, no production policy change.
Receipt docs/evidence/no-fallback-n6x.md records independent original proof,
exact arity provenance, source capabilities and resource/reuse validation.
Sign coherent local checkpoint without push. Goal/task4 stays ACTIVE; next
inspect GREATEST/LEAST extremum value/caller/source closure together. Other
families/global alternate dispatch and final inherited/SQL/server gates remain.

### N6y GREATEST/LEAST actual values and canonical source closure (active)

N6x signed checkpoint is progress (29fb79f7/aa69754); current trees otherwise
clean except unrelated BUILD.bazel. Extremum retains eight Ready profiles and
no same-driver source host. Preserve actual full NULL scan before domain choice,
original signature/metadata/constant precision, numeric NoColumns comparison
policy, first-winner ties, full signed/unsigned/NaN, datetime kind/precision,
vector and direct-collation comparison, string-as-time text fallback and per-
item modes/zone reads. First capture independent OLD value/context/representation
witnesses and genuine source/worker REDs before implementation changes. Expose
real borrowed values/typed reducer state in SDK (not transport reports or fake
Exprs), independently compare before changing wire/controller, then retire
caller programs and integrate eager-source collection+actual kernel phases in
same SDK driver. Preserve terminal finite resource/error provenance, cancellation,
return coercion/flags and multi-output/whole-parent demands; no permanent valid
SQL Unsupported escape. Full relevant regression/Projection/lint/fmt/diffs,
receipt+guide and signed coherent runnable checkpoint required; no push,
subagents or final global-completion claim.

N6y first OLD capture at29fb79f7/aa69754:331776 raw values/signatures/
layouts/want/mode-cache cases fixedhash71134e1c9a2de957,802 caught original
invariant panics;33696 arity/singleton/collation/time-text cases fixedhash
5a25c52fc09e4be5,no panics. Both pass before implementation changes. Original
snapshots and test-only n6y-original-extremum-baseline.patch cached-apply-check
against29fb79f7 passed. Seven actual integer/real/decimal/string/time/string-as-
time/vector sources each match SQL/context first and then fail route assertion
with1refusal/1fallback. Actual cache lookup trap fails original code.

SDK new typed actual sequence state and frontend independent bounded helper
have been added alongside STILL ACTIVE old caller and wire implementation.
Boundary33696 independently passes. Main independent comparison finds OLD cache
poison contamination after a caught Rust invariant panic: next input
[Real(-0),Bytes(-3.25tail),NULL] under reused Int signature returns ScopePoisoned
instead of being evaluated; pure values return NULL. This is the retired mutex's
state, not the following input's value semantics. Historical7113 digest and
full original poisoned sequence remain immutable in baseline patch/snapshot.
Do NOT regenerate that hash from new code or silently claim its exact equality.
New distinct OLD-only fresh-authority cohort resets the OLD cache only after a
caught invariant panic (not normal error/resource paths), so each next value
is actually evaluated. Capture its own hash BEFORE independently comparing and
before replacing old caller/wire. All historical cases remain in the artifacts;
new native path must not recreate obsolete poison to emulate an unused mutex.
Independent comparisons run in isolated single-test child processes: global
panic-hook suppression belongs only to that child, and actual original panic
messages are recorded as dispositions, never SQL NULL/error replacements.

LIVE87719 captures the distinct fresh-authority cohort; production old extrema
caller/wire remains active. No switch or green source claim yet. Need independent
proof, raw representation/resource units, actual wire primitive sharing, caller
retirement, genuine same-driver eager-source host+parent/Projection coverage,
full gates and signed coherent checkpoint. Overall goal remains active.

N6y distinct OLD fresh-authority capture:331776 actual value computations,
2400 original invariant panics, fixedhash645bc94e1a54782d. The OLD cache is
recreated ONLY after caught panic, not normal SQL/resource failures. Separate
n6y-original-fresh-authority-extremum-baseline.patch applies at29fb79f7/aa69754;
original snapshots preserve both historical poisoned and actual fresh cohorts.
Do NOT overwrite/regenerate any hash. n6y-old-vs-fresh-authority-extremum-values.log
(77759 closed0) independently matches ALL365472 actual cases, exact result
identity bytes/cause/phase/context/warnings and2400 panic messages, BEFORE
caller or wire replacement. Historical poisoned hash71134e1c9a2de957 remains
an explicit retired-cache artifact, not claimed equal to new stateless values.

After proof, old frontend Stage/identity/head/step/evaluate_args/scoped rebinding
loop is removed. Explicit SDK wire profiles keep complete validation/protocol
and share time/vector/collation/time-text comparison plus actual parse primitives.
New helper uses genuine PreparedComparison with passed finite remaining bytes
and NoColumns (not another worker). New SDK sequence units added. First frontend
post-retirement build15150 failed only a missing FieldTypeCode import after old
imports were deleted; corrected. LIVE13818 caller-value gate; LIVE96240 SDK
native_extremum target gate, both actual processes confirmed, never restart on
quiet output. New source host draft /tmp/n6y-batch-extremum.rs NOT APPLIED;
7source REDs still pending integration. No sign before all coverage and gates.

N6y FINAL:all seven original source REDs turn green (85685 closed0). Same SDK
frame collects all arguments before kernel work, with detached signature/decimal/
STRICT constness/collation/result metadata and authenticated eager source tokens.
Source factory adds no Expr callbacks/alternative program or new eligibility
whitelist. Native casts/context and SF RT/outer flags publication stay intact.
Whole-parent/error/2050duplicate phases, actual storage-word packing/visible-text
resource prechecks and provider stale/wrong-role/node/occurrence/clone/native-work/
repeated-cancel/exact success+NULL reuse checks pass.

Complete frontend1588 exact inherited4/95ignored/zero filtered, all185batch;
SDK1079 exact inherited6/one ignored/zero filtered. Actual PreparedComparison
receives remaining finite bytes; fresh actual OLD645b and boundary5a25 fixed
witnesses/2400 invariant panics remain exact. Historical poisoned7113 artifact
is retained and explicitly distinguished, not regenerated or falsely claimed.
Projection25 passes all prior31-root and8-root matrices plus heterogeneous9 roots
with eight extremum domains, both flags/zero-two workers. Lint/pinnedfmt13Rustfiles/
bothdiffchecks pass; scoped clippy89 unchanged. Only unused Time import cleanup
after full gates, compiled in final clippy lib/tests; no runtime changes after.
All96240/15150/13818/85685/46354/82837/44387/51809/49495/32690 jobs CLOSED.
Receipt docs/evidence/no-fallback-n6y.md and SDK guide record original evidence,
cache-poison boundary, actual typed values and finite logical resources. Sign
coherent local checkpoint, no push. Overall goal/task4 ACTIVE; other functions/
sources/caller transports/alternate dispatch and final inherited/SQL/server
closure remain unproven. Next inspect remaining native text/collection callers
and their actual source coverage together; don't substitute performance work.

### N6z charset actual bytes and canonical source closure

N6y signed6e005bde/1e3efd8 is progress. Native to_binary/from_binary/
convert_using and actual-NULL adapters still construct Ready programs, and no
canonical charset source host exists. Capture actual OLD reader/result/charset
metadata/NULL/unknown-target/error/warning/cached behavior BEFORE switching, plus
source REDs. Exact source spelling and effective field charset must remain
separate: lookup is case sensitive while FieldType canonicalization is not.
Typed reader NULL produces empty bytes in direct codec APIs; actual source NULL
short-circuits convert_using's target and connection-metadata read entirely.
Unknown target precedes ETString reader in the direct API, but scalar source
order is source→target/coerce→value cast→connection metadata→conversion. Preserve
all those phases and late GB default-collation projection after actual conversion.
Expose genuine bounded borrowed-byte SDK services, independently compare OLD
before wire/caller retirement, then integrate the actual source driver without
Expr callbacks/another program or static-target Unsupported escape. Field and
coercibility snapshots, dynamic connection getters, resources/cancellation/
provenance/publication, parents and mixed Projection must remain covered. Final
full frontend/SDK/Projection/clippy/lint/fmt/diffs and signed coherent checkpoint,
no push/subagents; global fallback goal remains incomplete.

N6z OLD proof captured at6e005bde/SDK1e3efd8 BEFORE production changes:
4200 binary reader/name cases fixedd7142611e2de991d;88200 exact/effective/
target/reader cases fixede3a22bd8fcc8c70f;1604 isolated live GB default/source
NULL cases fixed7d8948d00184e03d. All fixed witnesses pass, six source REDs
match SQL/context then fail1refusal/1fallback, three actual Ready lookup traps
fail old code. Original test-only baseline patch applies at6e005bde/1e3efd8
(cached separate-index check pass), original production/tests/source-RED
snapshots retained. Every cohort runs in an isolated child to avoid global
collation races. NEVER regenerate fixed hashes from new code.

New SDK borrowed-byte services plus shared borrowed ETString reader are added
alongside still-active OLD workers. All94004 independently match OLD before
caller/wire replacement (36578 closed0). Only afterward ordinary wire producers
are delegated to actual values while preserving all original validation/protocol,
and native frontend Ready wrappers/report copying/DateDiffNull dispatch retire.
Direct NULL-reader APIs still compute empty bytes; actual source NULL uses the
SDK's distinct null primitive. Bytes move directly; retag projection bounds its
owner overlap and reads the current default collation after codec computation.
Binary/Latin1 no-op copies and empty values pay no retired report framing charge.

28246 post-retirement fixed+three worker-trap checks all6 pass. New canonical
CharsetNative operation consumes SDK actual source-null demand. Source FT/exact
names/result FT/coercibility/producer ordinals are detached snapshots; target
remains actual dynamic input, not static admission. Runtime follows original
source→target coercion→source value cast→unconditional connection getter→actual
metadata selector→conversion→late retag. Store borrows end before connection
getter, and retained source token is reauthenticated before target work. Metadata
cloning precharges only names selected by actual shared selector (ignored large
connection names do not invent work). Readonly string owners remain shared;
actual Vec/format work and huge Decimal padding are bounded before copying/
formatting. All six actual source REDs turn green (29406 closed0).

Parent checks exposed genuine CONCAT admission/refusal/fallback (59819/17026).
Fixed by allowing CharsetNative to publish the same computed value to ETString
while always consuming native input capabilities; parent's original demand stays
unchanged, no cast/evaluator/NativeEligibility widening. 90114 closed0 all4parent
profiles; temporary diagnostics removed. 95939 closed0 all11charsetsource tests:
full dynamic getter/order/partial outputs/2050duplicates, source name variants,
wrong/stale/role/producer/occurrence and retained-first expiry before target work,
source/target Decimal refusal and sentinel precedence, actual codec resource
cause/phaseNone, huge readonly String unknown-target SQL error after getter,
double cancellation and exact NULL/success output-provenance reuse. Isolated
callback obtains store.borrow_mut(), flips GB mode, and verifies actual current
default collation after conversion; actual NULL skips callback/target entirely.
Raw target/source coercion work includes real raw byte copies.

Full frontend79252 closed101:1606pass EXACTsame4/95ignored/0filtered/all196batch.
SDK1082pass EXACTsame6/1ignored/0filtered, failure lists machine-match N6y.
Clippy25520 closed0 warnings89unchanged; lint21878 closed0; pinnedfmt14Rustfiles
and bothdiff pass. SDK guide targeted paragraph updated, receiptN6z records proof
and remaining limitations. Projection77779 closed0 all25pass/four bounded mixed
31/8/9/5root suites, bothflags workers0/2. Initial44107 reference-only failure was
new test typo cast_string, corrected to existingcast_char TESTONLY; production
unchanged after full checks. Finalfmt/diffs pass. Coherent signed local checkpoint,
no push. Global fallback/remaining families and inherited failures remain incomplete.

### N7a JSON_SUM_CRC32 actual documents and sources

Signed N6z6263a88b/93d7375 is progress; JSON_SUM_CRC32 still serializes an actual
parsed document into a separate Ready program which reparses it. Original10926
value/error/cache observations fixede762b2ff1936744f; never regenerate from new
code. Old source text/JSON/numeric all match SQL/context then fail1refusal/
1fallback; actual rejecting-cache trap also fails. Original production/tests and
test-only reproduction patch retained; cached apply-check at6263a88b passes.
New actual borrowed-document/input SDK services are added alongside old workers;
independent OLD-vs-new10926 comparison is running58323 BEFORE wire/caller changes.
Preserve ordinary document admission (SQL NULL vs JSON null), exact first array
scalar/homogeneity error, integer/u64/f64 Go-g spelling and wrapping checksum.
Bound demanded parse/Decimal display/number formatting before work, without
invented string copies or skipped later work. Invalid UTF-8 precedes parser
allocation checks. Then retire serialized/report caller, share actual wire
computation under unchanged ordinary validation/bounds, integrate genuine source
closure/parents/provenance/resources/cancel/reuse, mixed Projection and final
full gates. No push/subagents; global goal remains incomplete.

N7a independent10926 proof66501 closed0 BEFOREcaller/wireswitch; originalfixed
hash unchanged. Afterretirement17707 closed0 all9source/value/provider checks:
3sourceREDs+Readytrap green, native/typedInt carriers with exact provenance,
wrong/stale role/node/occurrence before Decimal work, huge parse/display refusal,
invalidUTF8/unsupportedRaw earlier SQLerror, repeatedcancel/reuse/NULL. Fullfront
1613pass EXACTsame4/95ignored/0filtered/all201batch; SDK1084same6; Projection25
fivebounded31/8/9/5/5groups bothflagsworkers0/2 PASS. Lint/pinnedfmt15/bothdiffpass.
Finalreview caughtfloatformat's fixed Display intermediate (up to327chars) before
shortGo-goutput; increaseONLYdemandedfloatformatwork128→4096 beforeformat,
keepInt128, addMAX/minsubnormal+/- actualDisplay>128/refuse128/succeed4096probes.
No production codec/text semantics changed, no old hash regenerated. FINAL
fullfront40383 closed1011613pass EXACTsame4/95ignored/0filtered/all201batch,
SDK13443 closed1011084pass EXACTsame6/1ignored/0filtered, clippy35853 closed0
89exactdiagnostictitles unchanged, Projection49375 closed0 all25pass fivebounded
mixedgroups31/8/9/5/5 bothflags workers0/2. Lint/static15Rustfiles/bothdiffpass.
Guide/receiptN7a final; coherent signed local checkpoint, no push. All jobs closed.
Global goal remains active with remaining date families/source closure and
inherited failures pending; no global completion claim.

### N7b STR_TO_DATE actual parse states and sources

At signed fec0abbf/8c6e58a, STR_TO_DATE still builds three Ready/report stages.
Capture actual OLD values/warnings/modes/metadata identity before replacement,
including named Datetime versus Unknown(12), wide day/year, partial formats,
Unicode parser classes, malformed arity, source NULL and later source errors.
Both source children evaluate eagerly before a NULL prepass suppresses coercion;
then input coercion precedes format coercion. SDK scan alone decides whether
and when date_modes is requested. Scalar source finishing additionally invokes
original parse_computed_time/parse_computed_duration by complete result field;
that must not be replaced merely by the generic coerce_function_result helper.
Expose actual parsed state and bounded actual string/warning computation, not
encoded reports pretending to be values. Preserve original ordinary wire
validation/protocol and share actual grammar/policy after independent proof.
Then integrate full source/parent/capability/resource/late-context coverage,
mixed Projection and complete checks/signing. Existing partial-format failures
remain explicitly real pending global semantic work, not a completed goal.
Current original witness/source RED capture68316 is running; production unchanged.

N7b OLD844344 fixed d7dd9f9ee5b55431 atfec0abbf/8c6e58a, count includes62inputs/
63formats×9resultidentities×8modebits×3cacheprofiles plus648badarity. Fixed8996
witnessPASS, five text/Datetime/Date/Duration/NULL-with-late-error source REDs each
matchSQL/context thenrefusal1/fallback1; actualReadycachetrapRED. Originaltest/
production snapshots and test-only n7b-original-date-baseline.patch retained,
separate-index cachedapplycheckfec0abbfPASS. Neverregeneratehashfromnewcode.
New SDK actual prepared strings, opaque parsed Date state and TypedClock state
are added alongside OLD. Original scanner/render helper visibility/Debug only,
no OLD body changes. SDK owns actual NULL prepass, ordered coerce_str and actual
text/NULL/warning computation, late mode decisions and finite demanded copies/
Decimal/f64/JSON/vector/scanner/formatter capacity. Two Vec<char> work bounded
before scanner; empty scanner adds no fake frame cost. Frontend precharges text
owner overlap before Arc publication and uses actual context callbacks after
owned preparation, not a borrowed value-store reference. First12888 was only
prototype compile+OLDwitness, not independent proof (test insertion script had
stopped before editing tests). Actual independent all844344 comparison54980 and
SDK2units36652 now running. Await actual proof before changing caller/wire bodies.

N7b ACTUAL independent comparison54980 closed0 ALL844344 pass, immutable OLD
hash verified afterward, BEFORE any caller/wire body switch. New SDK2units36652
closed0 actualstates/lateflags/namedUnknown/resources/earlyerror/NULLskip and
emptywarningexactbudget pass. No fake empty-scanner frame allowance. Original
source/caller/wire remain active; no integration/source completion claim. All
jobs closed. Next retire old native caller and delegate ordinary wire's actual
head/date/typed computation while preserving complete transport validators and
original output bounds; native finite work allowance must not be imposed on
ordinary wire owners. Then canonical eager2sources, ownedprepare before ctx,
originalspecialtemporalfinish PLUSgenericRT/Enum, allnaturalparentcarriers,
provenance/resources/cancel/reuse/2050/mixedProjection and fullgates/signing.

After independent proof, native caller's Cell/report/Ready/scoped-rebinding
stages retire. Ordinary wire shares actual head/date/clock logic with its own
None allowance (not native Somefinite), preserving validators/whole-state
reports/input+64bound and no extra clock clone. Postretirement67074 fixedOLD+
workertrap2pass;33203 oldwire+new2core tests3pass. Source StrToDateNative now
reads both authenticated sources eagerly, ownedprepare understoreborrow,
dropsborrow beforeactualmodes/warnings, dropsprepared before original shared
specialtemporalfinish, genericRT/Enum andnativeortyped4carrierpublication.

Five original source REDs green32619; new11parent/dynamic8mode/toggle/2050 test
found Month outside eager signature closure2707. Existing pure stored-field
MONTH kernel's exact DateTime→signedInt signature now composes for CompiledSource,
with explicit regression retaining previous NativeEligibility contract. 80928
then exposed genuine ResourceLimit Invoke. Temporary cause diagnostic19756:
`retained evaluation storage exceeded`, because host_copy_precharge treated
DateTime/Duration as usizeMAX, and host copies supported only Int/Bytes. Shared
host compact scalar/window copies now meter actual temporal minimum/capacities
and copy unchanged fields; no budget increases. 18288 all11parents×dynamicmodes/
selections pass. Temporary diagnostics removed. 18412 adds actual fatal UTF8
source errors early/late, resultFSP/kind and constant-relaxed typed-clock/month0
cases, currently running; no premature parent/error completion claim. Direct
provider resources/provenance/cancel/reuse/borrowmutcallback and Projection/full
gates/guide/receipt/sign still required. Goal remains active.

N7b final direct provider26998 closed0 all8source/provider/context tests pass;
fatalUTF8early/late and2050prefixes, constantrelaxedclock/month0 andFSP/kind
variants pass. All4carriers' stale/role/node/occurrence/retainedepoch checks
precedework, nativeResourcecauseNone/earlySQLerror/cancelreuse/provenance pass.
Everymode/warning/zone/clockcallbackobtainsstore.borrow_mut; noheldborrow or
retiredReadylookup. NULLDuration's original finalzonegetter ispreserved even
thoughparser/coercionsskip; correctedTESTONLYpreviouszero-callbackassumption.
SDKcompacttemporalcopyregressionchecksraw/FSP/NULLand1/3rowbroadcast. Fullfront
18723 closed1011623pass EXACTsame4/95ignored/0filtered/all209batch; SDK95169
closed1011087pass EXACTsame6/1ignored/0filtered (failure lists machine-matchN7a).
Finalclippy89562 closed089exactdiagnostictitlesunchanged, afterunusedtestimport
cleanupONLY; finalclippycompiledlib+tests, no SDK runtime bodychangeafterfull.
Projection54048 closed0 all25pass sevenboundedgroups31/8/9/5/5/4/5 bothflags
workers0/2, oldmatrices/allowancesunchanged. Lint88401 closed0, pinnedfmt17Rust/
bothdiffpass. Guide/receiptN7b final, coherentlocalDCOcheckpoint; noPush.
Globalfallbackgoal remainsactive; inheritedpartial-formatmonth0bug andother
families/source closure/remainingfailures areexplicitlypending, notignored.

### N7c actual TIMESTAMPDIFF states and source demand (in progress)

OLD392556 fixed b4faecb69db19986 at996b942d/611b076. The original test-only patch
cached-applies to996b942d; snapshots/logs remain in workspace evidence/no-fallback.
The independent actual SDK text/core comparison70960 passed392535 paired observations
BEFORE native callers and wire-core delegation changed. The remaining21 arity/NULL-witness observations were guarded by the immutable
whole392556 hash, not separate prototype comparisons. Three genuine source
REDs and a Ready-lookup trap failed at that point; original fixed witness passed.

Native text coercion still observes all three values in order even after NULL;
PB exact TimestampDiff stops on its first actually observed NULL without coercing
the prefix. Legacy coprocessor keeps its own unit-first/two-endpoint demand.
The SDK now has opaque actual parsed endpoints and an actual computed delta,
so parser scratch and late unit folding are bounded at their demanded stages.
Shared bounded string coercion is extracted unchanged from the preceding date
checkpoint; date resource diagnostics remain date-specific. Ordinary wire
validation remains with its original owner. Source integration is unverified WIP.

First source integration18021 passed PB-prefix and Ready-trap checks but generic
sources failed context equality: original ScalarFunction applies endpoint
datetime argument casts after ALL sources, before the text kernel. The provider
now detaches bounded actual source ownership, drops store borrows, casts left
then right with full source FieldTypes, then calls the actual text kernel. PB
retains its distinct no-cast path. These casts share the existing plain SDK
conversion owner; no Expr callback or secondary program is introduced.

PB origins remain scoped to exact TimestampDiff and its descendants. Current and
original effective FieldTypes are both admitted by byte budget BEFORE equality;
all other PB function signatures remain outside this checkpoint's closure.
The expanded test initially64589 failed only to compile because it tried to use
the private runtime-cause accessor from outside tikv; corrected to public class
and phase without widening the error API. Actual test results remain pending.

Final pre-review frontend46659 closed1011633PASS exactinherited4/95ignored/0filtered,
all217batch pass including Duration statement-clock and true decoded PB columns.
Projection50168 closed0all25/eight bounded groups pass; SDK32044 1089same6 and
datatype88482 564same2, lint68255pass, clippy80476 89exacttitlemultiset matchN7b.

Self-review then found YEAR UInt outside i64 fell through the original SDK year
route into formatting, while the new frontend work classifier assumed allUInt
YEAR was allocation-free. Genuine regression48257 closed101 reproduced date_modes
callback under zero work budget. Before changing code this was preserved as
n7c-year-resource-red.log. The correction moves work classification into
`native_cast_time::native_datetime_argument_cast_work`, sharing the actual private
`year_source_value` decision instead of copying it into the frontend. UIntMAX is
now metered, while NULL/Time and actual i64 YEAR construction/errors remain
allocation-free. Tests include Known13 vs Unknown13, UIntMAX vs i64MAX, invalid
UTF8 and negative year, plus a zero-budget native callback trap. Final complete
frontend/SDK/Projection/clippy are being rerun after this runtime correction;
the original datatype arithmetic has not changed since its complete sweep.

N7c final YEAR regression GREEN in fullfrontend95104 closed1011634PASS exactsame4,
95ignored/0filtered/all217batch. FinalProjection86279 closed0all25/eightgroups
underunchangedlimits. SDK98646 closed1011090PASS exactsame6/1ignored/0filtered,
newclassifiertest explicitlyregisteredandPASS. Initialtest accidentallynested
insideexistingtest causedclippy91; onlytestplacementcorrected afterfinalfront/
Projection, no productionbodychange. Finalclippy93873closed0 89EXACTtitlesmatch
N7b;lint13889closed0/pinnedfmt19/bothdiffpass. Datatype88482 564same2 remains
final because no laterdatatypebodychange. Allcheckpointjobsclosed; sourceReady
retirement is coherent and verified, but globalfallback and inheritedsemantic/
validation failures remain pending. Next audit remaining date semantics/callers.

### N7d partial-calendar validation and historical evidence

The next intentional semantic correction addresses the inherited relaxed-mode
partial-date failure. Local Go SELECT sets IgnoreZeroInDate(true); Time.Check
allows month0/day0, and the builtin separately applies NO_ZERO_DATE. Original
integration issues.result354–400 provides six concrete outputs. The current
SDK unconditional month0/fullzero checks contradict that behavior. Its invalid
wide-day branch and scanner remain separate from this validation correction.

Before changing production code, capture the entire original844344 observation
stream as a compressed deduplicated first-profile fixture, verify all three
profiles agree and retain d7dd9f9ee5b55431. Also capture original actual calendar
fields from the WHOLE officially validated SDK head report, only in a temporary
test generator; this adapter never enters production. A standalone Go probe
using original types.Time.Check will independently validate representable zero-
month calendar fields and render expected accepted text. Full original parser
parity is not implied by a calendar-validator oracle; six direct StrToDate
fixtures provide independent parser/builtin boundary evidence. Final tests will
compare every unchanged observation to original data, and changed observations
to explicit Go-validated expectations; the old hash remains historical and is
never regenerated from new code. Remove the temporary capture generator before
committing. Current production trees remain unchanged at this stage.

N7d original capture39668 closed0 ALL844344 d7dd9f9ee5b55431, three profiles
independently identical. Frozen15188byte gzip SHA256
fd9ed6080649e8987dd86b64bd761998c7db2bf66cd8aff52fdbd4a9a9464571;534observation
strings/281448profileindices/3906originalcalendarstates. Temporarycapture
generator removed; test-only patch cached-applies64bce47f. Go standalone oracle
build80601 succeeded and independently verified168 actualzero-monthcalendar
states plus six partial/four fullzero StrToDate fixtures. Original Go %j999
parser behavior differs from native wide-day storage and remains REAL separate
work; this fixes calendar validation, not the entire scanner or Go package.

Genuine RED5998 old history+Go fixtures+inheritedpartial; source67817 typedGo
values;SDK1502 actualvalidator. Production two-check removal only after REDs.
Focus30841 all18date tests pass; fullfront18207 closed1011637PASS, EXACT only
remaining3 failures/95ignored/0filtered/all218batch. Previous partial-format
failure is removed and original history is explicitly replayed from captured
data, with826200 unchanged observations+18144 Go-validated deltas. Source tests
cover8modes/fourflags/empty,10row,2050duplicate selections/String,Date,Datetime
FSP0/3/6/CHAR/MONTHparents/explicitGoNULL/rawvalues/noWarningwhenrelaxed, plus
original scalar effects and0fallback/refusal. Projection45395closed0all26;new
serial/parallel0/2workers validatesfourpartial/fullzeromixedoutputs with
NO_ZERO_IN_DATE retained andbothNO_ZERO_DATE values, underunchangedlimits.

Initial SDK89821 hadsevenfailures: inheritedsix plus the ordinary stages test's
old relaxed2020/%Y warning expectation. That assertion now requires exact Go
Value2020-00-00 and a separate strictNO_ZERO_DATE1411control. Onlythiscfgtest
expectationchangedafterfinalfront/Projection; productionbodyunchanged. Final
SDK/clippy rerunning before signing. Earlier test-only &String/FSPtype compile
mistakes were fixed before genuine REDs; they are not behavioral evidence.

N7d final SDK48699closed1011091PASS EXACTinherited6/1ignored/0filtered; both
newcalendarvalidator andupdatedordinarywire relaxed-value/strict-warningcontrol
explicitlypresentPASS. Clippy25766closed0 89EXACTtitlesmatchN7c. Lint99982closed0,
pinnedfmt5/fixtureJSONgzipfingerprints/bothdiffpass. Front18207 andProjection45395
remainfinal: onlySDKcfgtest expectationchangedafterthem, no productionbodychange.
Allcheckpointjobsclosed; record newremaining3 frontend failures and stillreal6SDK/
2datatype gaps, native%j/scanner audit andglobalalternate-dispatch work. Coherent
localDCOcheckpointonly; goal/task4 remainsactive.


### N7e reject mutated ready-value dispatch before entering a kernel


The five inherited `test_evaluated_*` identity tests reproduce on N7d: the
ASCII case actually enters the substituted panic kernel; four other tests
accept a changed kernel under the original display name. RED log
`n7e-identity-red.log` has20pass/5fail/1ignored, all five exact inherited names.
The common compiler already resolves the checked FunctionRef. However, comparing
its function address against a separately instantiated official factory is
unsound across codegen units: Rust can duplicate the same function. Restore
integrity without that comparison. Generated RpnFnMeta construction will keep
an immutable copy of its original name and three function pointers. The closed
recipe guard compares each current pointer with its own construction-time copy,
plus the existing official name, arity, types and typed metadata checks. It
executes no function to validate identity and adds no alternate dispatcher.
This is a contract for generated metadata and the checked compiler, not an
adversarial authentication API for arbitrary Rust code calling its constructor.

Update the macro source and its exact token expectations rather than generated
outputs. Preserve const generic constructors and unannotated raw/wire execution.
Extend the composite negative case to a whole wrong factory with a forged name,
and check validator/metadata-constructor drift as well as the actual run pointer.
Run focused RED/GREEN, codegen tests, full SDK tests, complete frontend and
Projection because metadata size and monomorphization affect their actual
retained-storage checks, then scoped clippy, lint and pinned formatting. Do not
raise resource limits or remove inherited failures. Expect five SDK failures
removed and the original STR_TO_DATE worker failure retained. Record exact
remaining names, update the maintenance guide and sign a local commit pair.

Read-only calendar audit confirms original Go ignores the `%j` context entry,
while native stores wide day999; its invalid-calendar diagnostic preserves
parsed fields. Do not hide either difference by changing the worker oracle.
A scanner-semantic change needs separate independent fixtures and explicit
deltas against the immutable N7b history, beyond N7d calendar validation.
Global migration, frontend3, datatype2 and remaining old callers stay open.


N7e final: focused85034CLOSED0 25pass/0fail/1ignored; codegen49394CLOSED0
20pass; SDK82401CLOSED1011096pass/1fail/1ignored/0filtered, machine-proven removal
of exactly five inherited identity failures. Front36571CLOSED1011637same3/all218
batch; Projection20373CLOSED0all26; isolatedbodyoriginCLOSED0onepass; lint22784
CLOSED0; clippy90817CLOSED0 89expressionwarnings and171all diagnostic titles
EXACTN7d. Pinnedfmt3files/bothcompilers/diffs/unchangedfixturehashes pass. All jobs
closed. No calendar production or test expectations changed, no limits raised,
no new dispatcher or evaluator. Receipt evidence/no-fallback-n7e.md records
commands and limitations. SDK local DCO commit940f123; finalize the paired TiDB receipt commit, no push; global task4
and remaining SDK1/frontend3/datatype2/scanner/source/oldcaller work stay open.


### N7f actual bounded-staleness values and canonical source closure


The remaining native caller in `tikv/bounded_staleness.rs` encodes typed times,
executes a head Ready program, decodes a tag and may execute another finish
program after the original context supplies SafeTS. Replace that protocol with
actual SDK NativeTemporalValue classification and clamp. Ordinary explicit
wire/Ready compatibility APIs may remain but must delegate to these same value
kernels. Preserve every raw core bit and original kind/FSP until the chosen
result receives DATETIME/FSP3 metadata; FSP3 changes metadata without rounding.
Do not validate SafeTS calendar fields or eagerly request it. Invalid left
precedes invalid right, which precedes reversed-range NULL; neither NULL nor
non-Time admission can invent a SafeTS request. The actual frontend retains
handle_truncate disposition and owns warning formatting under finite bounds.

Before production changes capture a fixed digest of the old native calls under
three cache profiles, all truncate levels and varied raw endpoints/SafeTS,
including zero components, invalid calendar fields and kind/FSP metadata. Add
an actual Ready-lookup panic RED and generic source fallback RED. Then share
SDK actual operations, compare every original call while old implementation
still exists, retire old caller, and add a two-argument source host with actual
value capabilities. Both sources are evaluated before NULL suppression and
callbacks. Copy fixed-size times and drop the value-store borrow before calling
the statement's SafeTS/warning service. Preserve source provenance, occurrence,
resource/cancellation and publication checks, plus typed Time or native-value
output according to the caller's declared carrier.

Validate focused history/value/source checks, full frontend/SDK, mixed serial
and parallel Projection, lint/clippy/pinned formats and exact inherited failure
lists. No resource limits or failure allowlists are to change. Calendar `%j`
parser divergence remains separate: changing its accepted day policy requires
independent Go scanner fixtures and explicit deltas against the frozen history.


N7f original capture27882CLOSED101 records468099/hash58044483da932cf4 before any
production changes. Its hash0 assertion was a capture boundary, not a semantic
RED. Actual Ready-cache-panic and source(1refusal,1fallback) failures reproduce.
Added actual SDK value functions beside untouched wire bodies and new frontend
bounded helper beside untouched caller; paired40439CLOSED0 confirms467856actual
old/new calls plus243 hash-protected arity boundaries. Only afterward retired
caller reports/Ready programs and delegated old wire functions to the value
kernels. Temporary paired assertions removed after old retirement, since a
new/new comparison would not be independent. Fixed hash and full count remain.

Initial source91494/59806 found invalid preparation descriptors: child sources
still used typed Time while host declarations expected value capabilities.
The promoted-root list alone is insufficient; argument_view and native_value_call
must also admit the exact new family. After correcting both,73549CLOSED0 all7
focused tests pass, including all8dynamic date modes,3truncate strategies,
String/YEAR casts, temporal metadata, eager later errors after NULL, CHAR parents,
empty/2050duplicate selection and stateful SafeTS under both flags. Source
ownership is cloned only after payload budget checks and detached before all
original callbacks. Additional metadata field copies are conservatively charged.
Provider and final full validations remain pending; goal/task4 active.


N7f final handles all CLOSED:11419focusall8;93291front1644same3/95ignored/
0filtered/all222batch;28506SDK1096same1/1ignored/0filtered;5179Projectionall26
including extra mixed bound/CHAR/IFNULL group in serial/parallel/bothflags;
5718clippy89expr/171all diagnostic titles EXACTN7e;26407lint0. Pinnedfmt12files
under both compiler versions, both diff checks and immutable STR history/Go
fixture hashes+JSON/gzip pass. Receipt evidence/no-fallback-n7f.md final with
exact commands, scoped proof and limitations. No datatype changes/rerun;564same2
baseline unchanged. No%j policy/oracle changes, no larger limits or failure
allowlists. Ready/old caller and source REDs green, no second dispatcher/probe.
SDK local signed e212b91; finalize paired TiDB implementation/receipt commit only; task4 remainsactive, no push.


### N7g ordinary date arithmetic actual values and source execution


At the N7f starting point, `tikv/date_arithmetic.rs` serialized both actual
values, unit and metadata into a head program, then drove step/overflow programs by
serialized requests. The SDK controller owns the original different calendar
and duration demand policy; `native_date_arithmetic_helpers.rs` already owns
pure calendar and duration arithmetic. Replace ordinary native execution with
an actual SDK controller using borrowed native string/numeric representations,
owned demanded scratch and typed metadata. Conversions must call the very same
shared datatype primitives used by original coerce_str, Datum.to_i64 and
Decimal.parse_mysql, preserving their distinct rounding, raw-coefficient and
error policies. Statement callbacks are only unconditional warning delivery and
late overflow disposition, not expression-evaluation callbacks. No new native
expression walker or alternative signature dispatch is allowed.

Preserve composite-calendar amount-before-date behavior even for NULL dates;
duration checks the date and supported unit first. Single string amount
truncation warns before subsequent date demand; SECOND text/Real parse_mysql
uses the original fixed-word parser and ignores its disposition. Overflow reads
truncate_level only after actual calendar overflow, with exact1441 outcomes.
AST default mode must use NoColumns for semantic callbacks, independent of the
outer statement cache/context; removing the cache wrapper must not change that.
Check demanded Decimal/float precision/text/parse/result work before allocation,
using actual value routing rather than eager conversion of suppressed operands.

First freeze original native calendar/default/duration values, raw identities,
unsupported/overflow errors and getter/warning events under three cache profiles
and all truncate levels, and reproduce a forbidden Ready lookup plus generic
source fallback. Keep old bodies independent while comparing the new value
implementation. Only after that pass retire native reports/programs and make
explicit wire compatibility share the same actual semantics. Factor the scalar
function's exact type/FSP/Duration-to-Datetime/pre/post result policy so both
original row calls and canonical source resume use it once over both eager
owned operands. Unit is a static keyword encoded in date_add_/date_sub_ names.
Preserve source capabilities, occurrence, resource refusal, warnings, NULL,
late-error prefix/publication and cancellation. Validate typed parents, dynamic
contexts, duplicates, mixed serial/parallel Projection, full frontend/SDK and
exact inherited failure/lint lists; then sign the coherent local pair without
pushing. Legacy PB date arithmetic, remaining old callers and the global goal
stay open.

N7g implementation progress: original2325024/FEEA99CD98022F82 was frozen at
TiDB49333e87/TiKVe212b91; all2325024 independent old/new observations matched
in60826 before retiring old callers and wire controller bodies. All nine direct
entry Ready REDs and generic source refusal/fallback are now green. Actual SDK
state, wire delegation, shared scalar pre/post policy and canonical eager source
host are implemented. Source context/provider checks pass all four outputs,
wrong/stale capability prefixes, callback-exclusive store borrowing, changing
modes/timezone/clock, late overflow policy mutation and2050duplicates. Reference
suite scope-binding cache lookup alone is allowed by the test reference context;
actual source/direct contexts still reject every Ready query.

Two actual WIP API resource defects were independently reproduced: an oversized
coercion failure could be followed by a NULL replay (11110), and incoming spare
String capacity was ignored while producing a warning (93651). Failed advances
now invalidate the state and incoming capacity is charged until transferred or
dropped. Nonfinite formatting ignores valid fractional precision but the Rust
formatter still panics for a precision parameter above65535; independent boundary
probe confirms the inherited panic. The finite API now checks invalid-parameter
work first while preserving the panic with sufficient budget. Wire compatibility
remains on its existing private bound/codec policy. Final frontend45587 has1652
passes with the exact same3 inherited failures/95ignored and all226 batch tests
passing; SDK58621 has1099 passes with the exact same1 inherited failure/1ignored.
Projection83008 passes all26 with the new mixed group; clippy60088 retains89
expression warnings and the exact171-entry all-crate warning-title multiset.
Lint6348,15 Rust files under both pinned format parsers, both diff checks and
unchanged original calendar fixture SHA256/lengths pass. No datatype source
changed or datatype rerun claimed. Receipt `evidence/no-fallback-n7g.md` records
proof, commands and exact open failures; signed SDK3e94811 is paired with this
TiDB implementation, without push. Legacy PB arithmetic and the global goal remain pending.

### N7h legacy date arithmetic actual demand and values


The remaining `tikv/legacy_date_arithmetic.rs` frontend encodes every original
requested child channel and runs Ready head/step/zone-bound parse programs.
Its consumers in `tidb-unistore/src/cophandler.rs` use a legacy SimpleExpr seam;
this is distinct from ordinary scalar date_add_<unit> and its canonical source
host introduced by N7g. Do not substitute ordinary scalar semantics for legacy
folded i128, typed-channel filtering, MyDecimal behavior or presence conditions.
A new actual SDK continuation must retain demand, zone parsing and arithmetic
without an expression tree, fake child response or encoded report. It may request
original source channels from its owner and the original generic MyDecimal
primitive. The explicit wire adapter must delegate to the same actual controller
after independent comparison; do not keep a second production algorithm.

Text domains request unit first, then date, then zone-bound parsing before the
interval. Typed Time/Duration request date first, then unit and interval; zero
Time still demands both later sources. NULL stops at the actual requested
source. Folded i128 dates outside i64 become NULL before zone parse; intervals
retain full i128 display. String SECOND requests MyDecimal parse/render without
rounding; Decimal non-composite intervals request MyDecimal HalfUp to zero.
Presence is a distinct SDK output from one execution, not host ToBool. The
explicit legacy evaluator timezone is authoritative, with parse flags fixed at
zero; never replace it with statement date modes or a fresh timezone getter.

First freeze public text/time/duration outcomes, raw identity/presence, requested
channel/index/authority and original callback errors across all date/interval
metadata, signs, nullable values, units, zones and cache profiles. Reproduce
prohibited Ready lookup for each entry. Add the actual continuation beside the
independent original wire controller and temporary native driver beside the
original public caller; compare every recorded old/new case before retirement.
Then replace native encoding/Ready calls and wire control bodies, check demanded
text/decimal formatting/parsing/math/output work before allocation, poison failed
advances and preserve original callback E without sentinels. Run consumer-level
unistore date arithmetic tests, full frontend/SDK, clippy/lint/pinned formatting,
immutable fixtures and exact inherited failure checks before signing locally.
Global unistore walker/source retirement and its child-free unsupported ids stay
open; removing this Ready layer is a verifiable step toward that same full goal.

N7h implementation progress: the old public caller at dc69eb6f/3e94811 produced
1188864/F0099F79736B34C1 (69542) and all3 no-Ready REDs. New actual state/caller
were kept beside independent old control until98843 matched all1188864. A second
12-unit matrix (10524) matched another1188864 before its capture-only zero-count
assertion, yielding1F5BF8B3E75AD934. Both fixed digests are now checked against the
actual producer after retirement; no new/new pairing is represented as old proof.
Combined2377728 independent observations cover all20 valid units. Old Ready calls
and SDK arithmetic/controller bodies are removed; explicit wire adapters preserve
the original raw date identity bytes at pending parse boundaries.

Focused frontend81306 passes all3; SDK51234 passes all5; unistore98991 passes all4
original/new consumers, including6 date x4 interval x2 signs and zero-duration
presence1 with no prepared Ready program. Initial full front1656/core1102 preserve
exact3/1 failures; clippy89 expression and exact171 all-crate titles, lint76769,
six whole-file plus one added consumer block under both pinned format parsers,
diff checks and immutable STR fixtures pass. Unrelated existing unistore format
hunks were restored, leaving only141 added lines. No batch/executor/datatype
production changed, so prior Projection/datatype results are prior baselines.

Self-review found a new actual-state affinity gap: Err closed phase254, but a
caught raw Decimal ASCII-invariant unwind left phase3 awaiting another source.
68787 reproduces the original invariant panic followed by an incorrectly accepted
Decimal(None) SQL NULL. All public advances now set liveness false BEFORE running
and reopen only on Ok, leaving unwind closed without catching or replacing its
panic. Pure decimal-text access rejects a closed state; its internal accessor
still works during the active attempt. Final frontend3640 has1656 passes/exact3
inherited failures/95ignored/all226batch; SDK87869 has1103 passes/exact1 inherited
failure/1ignored, with the invariant-unwind regression green. Consumer60787 passes
all4; clippy4787 retains89 expression warnings and exact171 all-crate titles.
Final pinned source/hunk formatting, both diffs and immutable fixture hashes pass.
Receipt `evidence/no-fallback-n7h.md` records proof/commands; signed SDKcba2532
is paired with this implementation without push. Broader unistore walker/source retirement and all
global gaps remain open.

### N7i EXTRACT actual values and canonical source integration


The current `tikv/extract.rs` is the complete ordinary EXTRACT frontend, not just
a legacy MONTH facade. It selects datetime/duration/mixed signatures through a
Ready report, performs original casts, then executes further Ready extraction
stages; the mixed path separately reads allow-invalid-dates before duration parse
and only after success before datetime finish. The broad composite calendar
helper is a different one-argument policy with right-aligned duration groups,
unrestricted u32 hours, trimmed text and warning1292 for invalid dates. Its
ordinary signed arithmetic, raw cores and NULL/arity behavior must stay intact.

Scalar EXTRACT first eagerly evaluates both children, including a value error
when the unit is NULL. It then byte-coerces the unit, lossily decodes and ASCII
uppercases it, and uses the original value FieldType in signature selection.
Known named timestamp/date/datetime types count as temporal; Unknown lookalike
codes do not. Keep original datetime, duration and string argument casts, their
metadata, context getter/warning/error order and early returns. Scalar and named
composite calls return their original Datum without a new generic result cast.

First capture original outputs/raw identities/errors/context reads under three
cache profiles, dynamic date-mode seeds, truncate dispositions, source metadata,
all actual datum kinds and clock/date/composite/unknown units. Reproduce all
Ready entry routes and canonical-source refusal/fallback for ordinary EXTRACT and
a named composite. Introduce actual SDK selection, extraction, mixed parsed state
and composite result functions beside independent old wire bodies; compare old
and new before replacing old caller/control bodies. Pure existing arithmetic
helpers may be shared. Mixed state must hold actual nanos/FSP and borrowed original
text/unit, consumed once by the finish function; never serialize a continuation
or recompute the first parse from a cached flag.

Add finite work gates before demanded unit/cast/decimal/text/parser/diagnostic
allocation using actual routing. Preserve byte-unit lossy behavior, string UTF-8
error identities after the original cast route (Bit becomes Bytes, Raw becomes
String), duration cast SQL-display behavior, YEAR fast paths and original raw
invariant panics. Do not reuse a string preflight that rejects a byte or duration
route's originally accepted invalid UTF-8. Explicit wire compatibility keeps its
validation/report format/output planner and delegates to the same actual SDK
functions after old/new comparison.

Integrate scalar2-argument and static-composite1-argument native-capability host
operations in the existing canonical source frame. Demand every original eager
source, authenticate each token/node/role/occurrence, bound detached payloads and
drop store borrows before all casts, mode/zone/clock/truncate/warning callbacks.
Snapshot original value metadata and immutable composite keyword within metadata
bounds. Support integer and native-value outputs with original raw/NULL/error
prefix semantics, including parents, dynamic contexts, repeated vector flags,
empty/full/2050-duplicate selections, provider failures/cancellation/reuse and
serial/parallel heterogeneous Projection. Run full frontend/SDK, clippy/lint,
pinned formatting/diffs and immutable fixture checks with exact inherited failure
lists before signing a local pair. Other timestamp functions, generic admission
fallback, unistore walker and the global migration remain open.

N7i original62568 captured1107720/9EC51A59F6C44C40 and reproduced five Ready
routes plus both canonical-source refusals. Independent paired73850 matched every
observation while old callers/wire controllers were untouched. Actual SDK state,
wire delegation and scalar/composite source hosts were integrated only afterward.
Dynamic-source19211 passes all15 focused checks; SDK7571 has1107 passes/exact1
inherited failure, clippy12609 keeps89 expression/171 all-crate warning titles.
Initial new-test assumptions about zero-clock FSP and the HostContract/native
error channel were corrected without changing the original runtime behavior.

Self-review found an actual new source-host resource bug: a NULL scalar unit
still caused an unnecessary detached right-value clone.87053 reproduces a valid
16KiB right capability being rejected under an8KiB host allowance, even though
both original children already ran and further conversion is suppressed. The
fix authenticates both original capabilities before any early publication, then
publishes NULL without cloning the ignored right value. All stale/role/node/
occurrence checks remain demanded, including forged right replies after NULL
left. Final frontend60057 has1664 passes/exact3 inherited failures/95ignored/
0filtered; all231 batch tests pass. Projection71301 passes all26, including the
mixed5root EXTRACT group with both flags and workers0/2. SDK7571 has1107 passes/
exact1 inherited failure/1ignored/0filtered. Frontend warning titles and scoped
clippy's89 expression/171 all-crate titles exactly match N7h. Lint60091, pinned
formatting of9 frontend+6 SDK Rust files under both compilers, both diff checks
and immutable STR_TO_DATE fixture checks pass. All jobs are closed. Receipt
`evidence/no-fallback-n7i.md` records commands/proof; signed SDK1017778 is paired
with this frontend checkpoint, with no push. The global migration remains open.

### N7j UNIX_TIMESTAMP, FROM_UNIXTIME and their DATE_FORMAT handoff


The ordinary timestamp functions in `time_fn/session_tz.rs` still execute Ready
parse/value/local stages. Legacy `tikv/unix_timestamp.rs` encodes typed Time and
`tikv/from_unixtime.rs` encodes Decimal and retains a selected wrapper through the
pack callback. Eliminate those encoded in-process handoffs in favor of actual
SDK parsed time, numeric result and epoch values. Keep native two-zone demand,
no-clock error, unconditional bad-date warning, full-zero NULL versus partial-zero
numeric zero, range and normalized decimal coefficient/scale. Legacy typed Time
uses strict zone conversion, ignores raw kind/FSP for numeric output and returns
Decimal scale0 zero or scale6 valid micros. Legacy FROM_UNIXTIME uses visible
Decimal text, f64 conversion and its original extra ordinary u32 nanos*1000; this
is a different policy from native rounded epoch preparation and must remain so.

Native FROM_UNIXTIME rounds according to original numeric kind/visible scale,
keeps warning plus its computed zero epoch through HandleTruncate, and reads the
zone only afterward. Optional format expressions have already been eagerly
evaluated by the ordinary scalar owner; their conversion/DATE_FORMAT execution
happens only after a valid local result. The legacy pack callback receives the
original context identity and keeps its original demanded-child ordering. It no
longer needs an alternate selected execution wrapper. DATE_FORMAT's native text
and legacy raw-core adapters are part of this handoff and should call the existing
shared datatype format primitives directly, with finite work gates and unchanged
conversion/NULL order. Do not introduce a new datetime parser/formatter.

First capture outputs, raw representations, errors, exact mode/zone/clock/
truncate/warning and pack identity under absent/shared/fresh Ready caches. Include
DST gaps/folds, raw offsets, full/partial zero dates, raw Time metadata, numeric
bounds/nonfinite floats, raw Decimal visible/storage/shape, invalid UTF-8 and
format arguments. Record true forbidden-Ready and canonical-source refusal REDs
before implementation. Add actual SDK/front candidates beside untouched old
callers and wire controller bodies and pair observations before old-body removal;
then preserve wire validation/codec/output plans while delegating to actual
functions. Fixed old hashes must never be regenerated from replacements.

Canonical source hosts for unix_timestamp0/1 and from_unixtime1/2 must preserve
every eager source, original native/typed output semantics and the existing
FROM_UNIXTIME post-result temporal conversion policy. Bound detached originals,
unit/text/Decimal/parser/diagnostic/output work, authenticate every token and drop
store borrows before every context callback. Check NULL-suppressed copies,
warning/zone ordering, source errors, metadata, parents, duplicates, all provider
prefixes, cancellation/reuse and mixed serial/parallel Projection. Run full
frontend/SDK suites with exact inherited failure lists, clippy/lint, pinned
formatting, diffs and immutable old calendar fixtures before signed local commits.
TSO, other monolithic time callers, generic fallback, unistore's old walker,
semantic calendar gaps and full-server gates remain global work after this scope.


N7j original96492 captures384264/14B4FDA02CC55EED and reproduces12 Ready routes
plus all4 canonical source refusals. Independent53693 matches every old/new
observation while old caller/controller bodies remain untouched. Actual SDK
numeric/parsed-time/epoch values and consuming finishes then replace native and
legacy Ready callers; wire adapters retain their planners/codecs and delegate to
the same actual functions. DATE_FORMAT handoff uses unchanged datatype kernels.
Native and legacy DST, rounding, raw metadata and extra nanos multiplication
policies remain distinct. Format values are eagerly evaluated/authenticated,
but are only copied/converted after valid local output; callbacks hold no store
borrow. Clock0 returns without a dummy child, with cleanup after a failed start.

Source matrix32118 exposes a real missed ScalarFunction return-family boundary:
Unix Int under Decimal metadata writes the wrong representation. Restoring the
existing value-only coerce_function_result once (with finite work) fixes it.
55144 proves the same omissions in earlier EXTRACT(Double/Decimal) and DATE_ADD
(Timestamp) sources; they now share that original ordinary return policy.
72957 additionally proves that treating a huge VARCHAR declaration as padding
wrongly rejects a tiny actual conversion. Shared control_value coercion now
charges actual Decimal scale/fixed-binary/Bit padding and input formatting, never
an unused VARCHAR truncation width; original conversion error dispositions and
same-family behavior remain authoritative. No timestamp arithmetic or historical
digest is changed by these source fixes.

Final frontend30633 has1676 passes/exact3 inherited failures/95ignored/0filtered,
all240 batch tests green. SDKfull has1111 passes/exact1 inherited failure/1ignored/
0filtered; all4 new actual tests pass. Projection95218 passes26 including a new
six-root mixed timestamp group with both flags and workers0/2; all older groups
remain. Provider28212 and final full tests cover every prefix/provenance forgery,
clock cleanup, deferred-format/resource/cancel/reuse and exclusive clock/zone/
truncate/mode callbacks. Four existing timestamp/formatter unistore consumer tests
pass without changing unistore code. Clippy24644 keeps89 expression/171 all-crate
titles exactly as N7i; frontend warning titles also match. Lint65515, pinned
17frontend+6SDK source formatting under both compilers, both diffs and immutable
calendar fixture checks pass. Earlier new-test wrong error-variant/ample-budget
expectations and a new Projection moved-variable compile failure were corrected
only in tests, not reclassified as semantic defect evidence. All jobs are closed.
Receipt evidence/no-fallback-n7j.md records commands/proof; signed local SDK
baea038 is paired with this frontend checkpoint, with no push. TSO/other Ready callers, generic fallback, unistore old
walker, %j/other semantic audits, inherited failures and full server gates remain
open; this is not complete package transcreation or broad PB-source coverage.

## N7k: transaction timestamp values and canonical sources


This next checkpoint removes the remaining physical/logical TSO Ready calls and
adds TIDB_CURRENT_TSO, TIDB_PARSE_TSO and TIDB_PARSE_TSO_LOGICAL to authenticated
native value sources. Freeze original results, raw temporal identity, errors and
context getter order before production edits; compare additive candidates with
unchanged callers before retirement. Reuse native_tso_core/native_tso_utc and
extract the existing logical mask into one shared value primitive, keeping wire
validation and identity codecs unchanged. Positive physical values alone demand
the actual session zone, with raw fixed offsets and named DST resolution.
Nonpositive values suppress zone reads; current TSO demands the transaction
getter, not a statement clock. Source publication preserves the original ordinary
ScalarFunction return-family boundary, finite work, provenance, cancellation,
per-occurrence selection and context borrow ownership. No clock-family migration
or full package completion is claimed by this checkpoint.

Run release locked/offline tidb-expr and tidb_query_expr tests, source/projection
checks, scoped clippy, lint, pinned formatters and immutable fixture guards.
Full suites must preserve the exact inherited failures recorded in N7j; new
failures are blockers. Keep original observation hashes independent of new code.
Review explicit diffs, then sign local paired commits without pushing. Receipt
will be rust/docs/evidence/no-fallback-n7k.md. Global clock/calendar, generic
fallback, unistore walker, semantic failures and full server gates remain open.

N7k pre-edit observations reproduce actual physical Ready lookup and current-TSO
source refusal. Original8586/05FEB3EE0578F58D is frozen; additive candidate51563
matches every observation before caller/logical-worker retirement. Earlier new
fixture compilation mistakes (literal argument order, Time kind method and
Duration signed FSP conversion) were corrected before executable comparisons;
they are not semantic RED evidence. Actual integer conversion remains the
original time_fn::int_arg. Sources use one closed operation enum, original
return-family coercion, immediate current-TSO without dummy children, and release
store borrows before actual transaction/zone getters. Validation is in progress.

Source matrix43303 supplies a true RED: invalid textual input returned an error
where the original ScalarFunction succeeded, because the candidate had omitted
its earlier ETInt argument cast. Sources now snapshot/charge the original source
FieldType and call the exact cast_arg_as_int before the unchanged signature;
direct time_fn calls retain their distinct int_arg conversion. This also retains
cast-owned zone/warning/truncate reads for logical TSO and independently demanded
physical-zone reads. Bounded work includes Decimal visible/storage scale and
JSON/text diagnostic expansion before casts; original return conversion still
runs once afterward. Fixed20363 passes all10 TSO tests and the complete source
matrix; the new provider's mistaken logical literal35 was corrected to246563
(test-only), not reported as a semantic repair. Additional typed cast/effect and
finite-budget cases are now included in the final full-suite run.

Final review40023 independently proves a second real RED: a one-byte Decimal
with million-digit visible scale and integer storage1 was rejected at16KiB even
though the original signed integer cast returns1 without rendering. N7k now
charges visible Decimal scale only when the original cast actually demands its
signed-overflow or unsigned-negative diagnostic. Direct signature int_arg never
uses visible precision for that conversion. Added positive signed/unsigned,
negative-zero and genuinely demanded huge-diagnostic tests distinguish the
cases. The earlier new test that expected every huge visible shape to fail was
incorrect and has been replaced; no historical fixture/hash was changed.

Decision (2026-10-19): preserve both the raw signature input contract and the
ScalarFunction's earlier declared ETInt cast instead of merging these entry
policies. Both delegate to existing value primitives and keep original argument
metadata/return coercion. Only demanded diagnostic display shape enters work
planning. This prevents unused display metadata from manufacturing a resource
failure, while still refusing large actual formatting before callbacks.


N7k final89683 passes1685/exact3/95ignored/0filtered, all244 batch tests. SDK14527
passes1112/exact1/1ignored/0filtered; Projection18448 passes26 including all3 TSO
outputs/CHAR parents in a seven-root mixed group with bothflags/workers0/2.
Clippy22863 retains89expression/171all titles EXACT N7j. Final dual pinned15-file
format/immutable fixture guards and both diff checks pass; root lint passes.
All new source/callback/resource/unused-display tests pass and the original8586
digest is unchanged. Receipt evidence/no-fallback-n7k.md preserves commands,
original proof, true repairs, test-only corrections and exact inherited failures.
No datatype or unistore production change/rerun is claimed. All jobs are closed;
paired local signed commits follow without push. Global clock/calendar, generic
fallback, old unistore walker, semantic failures and full-server gates stay open.

Paired signed local SDK checkpoint is 86d8cf1d842f7e1ebaf57ac0e3bfa98beea90d7b; this frontend checkpoint remains local with no push.


Final portability review isolates the frozen Local capture in an exact-test
child process with original TZ=Asia/Shanghai, never a concurrent global TZ
mutation. No production body, actual Local input or historical digest changes.
Final35991 full frontend under parent TZ=UTC again passes1685/exact3/95ignored/
0filtered, all244batch and warning-title multiset EXACT N7j. Both pinned15-file
formatters pass after this test-only wrapper. All jobs are now closed.


## N7l: statement and live clock values


Retire NOW/CURRENT_TIMESTAMP/LOCALTIME/LOCALTIMESTAMP, UTC_TIMESTAMP,
CURDATE/CURRENT_DATE, UTC_DATE, CURTIME/CURRENT_TIME, UTC_TIME and SYSDATE native
Ready callers, adding their valid zero/one-argument sources to the same frame.
The SDK already owns all pure clock policies in native_clock.rs. Keep those
kernels and wire validators unchanged; add a closed operation identity and finite
actual-clock renderer instead of copying calendar/rounding math.

First capture untouched caller outputs/errors/getter sequences across cache,
precision, raw clock fields, alias mode and missing context. Reproduce Ready and
canonical-source refusals. A test-only observer in prepare_clock_args records the
actual tuple after the old live SYSDATE sampling. Compare additive candidates
against old callers while both remain independent; for live success replay that
same real sample into the candidate's clock service. Live outcomes are separately
paired, never hashed as deterministic timestamps. Preserve deterministic corpus
hashes and actual live-pair counts before caller retirement. Archive old callers
and observer proof; remove the temporary wire observer after promotion.

SYSDATE reads its alias flag before arity/FSP validation; alias NOW keeps NOW's
error labels and truncation. Live mode obtains the statement offset before the
actual SystemTime sample. It must never substitute statement seconds. Empty
CURTIME truncates whereas explicit zero rounds after microsecond truncation;
UTC_TIME(NULL) returns NULL without a clock. Ordinary precision helpers reject
noninteger values without cloning/formatting them. Host sources authenticate
before callbacks, resolve the alias flag with no value-store borrow, then borrow
only for original precision validation, release that borrow, and finally demand
clock/rendering. Budget only actual numeric diagnostics/formatting work, not
unused large argument payload or declared widths.

Source finishing preserves existing temporal-result policy and ordinary return
coercion once. SYSDATE has a distinct unconditional DateTime parse even with a
string/other declaration. Validate dynamic contexts, all result declarations,
parents, eager child errors, empty/full/duplicate selections, provenance/resource/
cancel/reuse and exclusive callbacks; validate live results against independently
sampled before/after instants as well as actual-tuple old/new pairs.

Run locked/offline release frontend/SDK lib suites, scoped clippy, Projection,
root lint, both pinned formatters, immutable fixture guards and diff review. Keep
the exact N7k inherited failure lists visible. Sign paired local checkpoints;
no push. Receipt rust/docs/evidence/no-fallback-n7l.md. Calendar/component callers,
generic fallback, old unistore walker, semantic failures and full-server gates
remain global work, not evidence this clock scope is complete transcreation.


N7l independent10292 matches all134784 observations before caller retirement:
132408 deterministic/0CD20F17C50A8D2B and2376 actual old live samples replayed into
candidate clock service. Original91277 proves all12 Ready/source refusals. The
observer and paired test retire only afterward, preserving archived evidence;
frozen hash/live count unchanged. There was no production semantic mismatch.

Actual clock sources now borrow only precision inputs after capability
validation/flag resolution, release before all clock/result callbacks, preserve
raw tuple/precision and both original result boundaries, including forced
SysdateDateTime parsing. Unsupported huge ordinary precision never copies data;
actual live ToInt64 work ignores unused visible Decimal shape. Original MAX_FSP
is reused; uncalled frontend clock encoder and prepare_now_args are removed.
New-test enum exhaustiveness/Display compile mistakes and matching literal
static-type corrections are test/integration fixes, not semantic RED evidence.

Final73602 full1692/exact3/95ignored/0filtered/all248batch, SDK37631
1114/exact1/1ignored/0filtered, Projection43046 26PASS with new25-root fixedclock/
Sysdate-is-now group under bothflags/workers0/2. Actual live source bounds,
eightprecision types/5duplicates/root+CHAR/exactgettercounts separately pass.
Provider and dynamic context/metadata matrices pass. Clippy38891 retains
89expression/171all titles EXACT N7k, frontend titles also EXACT; lint56689,
pinned15-file double-format/immutable fixtures/diffs PASS. All jobs closed.
Receipt evidence/no-fallback-n7l.md records commands/evidence/limits. This local
signed checkpoint does not close generic fallback, calendar/component callers,
unistore walker, inherited semantic failures or full-server gates.

Paired signed local SDK checkpoint 52cfb464e31bd14ab1f3e7babf7e8ecdd6849909 accompanies this frontend checkpoint; no push.
