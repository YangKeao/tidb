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
