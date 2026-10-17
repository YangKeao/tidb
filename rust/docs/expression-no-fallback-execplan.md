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
