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
