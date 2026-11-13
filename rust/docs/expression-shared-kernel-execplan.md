# Share TiDB and TiKV expression execution from the original baselines

This living ExecPlan follows the repository root PLANS.md. Update it with each checkpoint.

## Purpose / Big Picture


Both Rust frontends will compile expressions into one TiKV RPN program, where RPN means the flat instruction sequence that computes child values before invoking a function. One evaluator must own branch demand, argument coercion and function execution. This replaces the previous experiment's multiple profiles and host task protocols while retaining SQL behavior. The first checkpoint is explicitly opt-in; it must demonstrate real TiDB Expression/Chunk and TiKV protobuf inputs, not a second synthetic evaluator. Default execution changes only after full coverage.

## Progress


- [x] 2026-11-13: create independent expression-shared-kernel-m1 worktrees from TiDB364aef2bab5cc633ecb76a775ae8f36f86a6687d and TiKV548812e1ef57aef077a2062a9cc356640a6347f5.
- [x] Read original entry points, repository instructions and Designer ddb87c63ff2d0dbe40ede1e84d4b1182b6541cbe design.
- [x] Inventory245 family identities (including five exceptions), fixed source witnesses and deduplicated conditional cost packages; per-overload compatibility remains unproved.
- [x] Build opt-in shared MD5 through native and protobuf inputs; checkpoint1 published 7e3a636 / 52d7be2.
- [ ] Add exact Decimal/full-domain floating carriers and demanded complex input conversion.
- [ ] Extend original RPN control flow to lazy IF, state and repeated evaluation; verify original PB behavior independently.
- [x] Publish checkpoint1 with four passing tests, counts and explicit remaining M1 gates.
- [x] Demand column reads from the RPN loop; missing unselected column and integer [7,2,7] tests pass.
- [x] Reproduce three review regressions, then fix/restrict admission; seven shared tests pass.
- [ ] Complete dynamic branching, scalar/vector contracts, complex error guards and state execution; M1 is not complete.

- [x] Task36: reuse existing native Decimal and String-to-Real algorithms;31 native and429 SDK tests pass. See shared-kernel/datatype-reuse.zh.md for dependencies, first test failure, full counts and incomplete gates.

## Context and Orientation


The worktrees live side by side at /home/agent/tidb/expression-shared-kernel/{tidb,tikv}. TiDB rust/crates/tidb-expr/src/expression.rs defines the native Expression tree; scalar_function.rs dispatches function names and has scalar/vector-specific demand. TiKV components/tidb_query_expr/src/types/expr.rs defines RpnExpression, expr_builder.rs compiles protobuf, and expr_eval.rs invokes typed RpnFnMeta functions. components/tidb_query_datatype holds vectors, Decimal and the NotNan Real carrier. Native f64 can contain NaN, so blindly converting it to Real is incorrect. Neither Decimal nor JSON storage APIs are being wholesale migrated.

The historical experiment remains in expression-unification sibling worktrees and supplies coverage identity only. Its local/profile/HostTask code must not be imported wholesale. The five existing exceptions are json_schema_valid, tidb_decode_plan, tidb_decode_binary_plan, tidb_encode_sql_digest and validate_password_strength. Their exceptions must remain visible.

## Plan of Work


M0 records each family and original implementation anchors, separating source candidates from verified equivalence. It accounts separately for core/integration/tests, algorithm moves and deletion of superseded algorithms. All additions plus deletions in both repositories count; documents are reported separately. A 13–22K core estimate is not a full-project promise. If the actual design needs generic host recovery, complete storage migration or approaches100K total changes, bring concrete code to Designer before expanding scope.

M1 adds an opt-in shared-kernel feature in tidb-expr. Native shared_compile converts Expression structure, types and literals; shared_input reads Chunk selections. It must never call native Expression::eval as a fallback. Existing TiKV protobuf compilation must feed the same program/loop. Start with MD5, then exact Decimal and NaN, IF guarding invalid JSON/constant regexp, RAND and user variables, BENCHMARK. Shared state belongs to an evaluator instance across batches; fresh evaluator resets statement state. Execution mode explicitly distinguishes scalar from one-row vector and row-major from column-major multi-output groups. Demand is owned by the shared loop, including repeat-child evaluation for BENCHMARK. Keep existing vector calls for pure eager regions; lazy regions must not decode skipped native input. Original PB eager IF and flagged short-circuit AND/OR remain intact.

M2 follows successful M1 review and covers remaining native AST/SimpleExpr, Unistore, projection/filter/sort/aggregate and remaining families. M3 removes obsolete production evaluation paths and algorithms, then validates defaults and measures release SQL against both frozen original and historical experiment. M1 completion does not claim M2/M3 or performance success.

## Concrete Steps


From tidb/rust, use ../../tools/cargo-tidb test -p tidb-expr --features shared-kernel shared_kernel. First use -- --list and assert the selected test count is nonzero. The wrapper pins nightly-2026-08-22, GCC14, one build job, shared download cache and a new isolated target-tidb directory. Record full command, exit status and selected tests in local evidence outside sealed historical archives. Run git diff --check in each worktree and make lint from TiDB root. Use the SDK Makefile clippy workflow with the same pinned toolchain when SDK code changes. Commit with DCO and push both expression-shared-kernel-m1 branches, preserving original references.

## Validation and Acceptance


Real native and protobuf MD5 inputs produce identical SQL values and NULLs. Decimal tests preserve large exact digits, scale, NULL and overflow. NaN input and generated NaN must preserve native behavior. IF(false, invalid JSON/constant regexp, value) suppresses only errors that original native execution suppresses; eager PB behavior is independently compared to original. Two-row/two-root tests detect RAND instance separation, cross-batch advancement, row/column ordering and user-variable effects. BENCHMARK repeats its child including effects. Selection[7,2,7], empty input, rebinding, deep expressions and input errors test demand and state cleanup. Compare error and warning contents/counts; warning relative order alone is allowed to differ. No semantic or performance claim can be inferred from compilation alone.

## Idempotence and Recovery


All work is on new branches. Do not reset or modify historical/frozen worktrees or sealed measurement evidence. Failed builds/tests remain recorded; rerun after addressing their cause. Keep opt-in until coverage is complete, with no runtime failure fallback. No service deployment or external data mutation is needed for M1.

## Surprises & Discoveries


The first test link failed because a newly resolved kvproto selected grpcio0.13 alongside the original0.10. The lock now uses original SDK kvproto623e58e60fa95c26b356cc6b9c266b0418a08710 and tipb5f9928e91afe4ac82c770cc9cd5fb2c804a7555e. A check launched from the SDK directory used a different workspace build configuration; it was interrupted before repeating from tidb/rust.

Original RPN already supports vector calls, but its short-circuit node owns nested expressions and invokes them recursively. Deep execution and drop therefore require flattening, not just a new frontend. Original native arithmetic demand depends on both signature and scalar/vector invocation. New-worktree make bazel_prepare was attempted and failed because bazel is absent (exit2, underlying127); no Go/Bazel changes are intended, and this limitation must remain reported.

## Decision Log


2026-11-13 / Engineer: use isolated original-base worktrees and an explicit opt-in Cargo feature. This preserves historical evidence and makes M1's incomplete coverage visible. Reuse original typed kernels and RPN structures; no duplicated tree walker or host-resume protocol.

## Outcomes & Retrospective


The first runnable checkpoint has native/PB MD5 through the original RPN loop, a full-domain native scalar carrier and flat literal-condition IF. Three initial tests passed before the final IF branch test was added; final checkpoint validation is recorded separately. Dynamic IF, bad JSON/regexp, operations on wide values, scalar/vector demand, state, multi-output groups and default integration remain pending. Byte literal output identity is explicitly outside this checkpoint admission; no runtime fallback exists.

## Artifacts and Notes


Designer reference: https://github.com/YangKeao/tidb/blob/ddb87c63ff2d0dbe40ede1e84d4b1182b6541cbe/rust/docs/expression-shared-kernel-design.zh.md . Local evidence is in the agent workspace evidence/shared-kernel-refactor. Historical coverage JSON identifies families but supplies no correctness credit for this new candidate.

## Interfaces and Dependencies


The expression crate gains optional path dependencies on the sibling tidb_query_expr and tidb_query_datatype, with the original tipb revision locked by Cargo. Native adapters build typed RPN nodes and supply input/context/output only. RpnExpression remains the one program type; the shared loop owns all function/branch/repeat scheduling. Borrowed inputs and owned intermediates replace value handles. Any specific API adjustment must be recorded here with its concrete semantic reason.

## Review 1 discoveries and decisions

The first IF admission compared EvalType only, which incorrectly accepted FLOAT/BIT result shapes. Regression tests first failed against the candidate and captured native Float32/Bit results; the adapter now explicitly rejects these shapes until shared coercion exists. Non-integer NULL is lowered by declared type and the program stores an explicit root type instead of inferring it from the last else instruction. Decimal parts additionally check leading integer residual range and trailing fractional alignment. Seven targeted tests pass after these changes; all three before-fix failures are retained.

The optional feature changes workspace dependencies even when disabled: flate2 1.1.9→1.0.11, protobuf registry2.28→PingCAP2.8 and cmake source. Default compression tests are required; disabling the route does not isolate dependencies. The SDK executors library check passed, but was built across input-adapter edits and will be rerun at a frozen source checkpoint. SDK loop tests are also required.

Whole-file candidate scanning fixes production omitted after inline test attributes; candidate identity and exact ledger input hashes are recorded. Inventory/build/prose overhead already costs8767 changed lines at checkpoint1. Revised total known estimate is22–34K (13–22K core +9–12K evidence/build), before unbounded algorithm/semantic/retirement work. No full-range10K proof or M1 completion claim is made.

## Checkpoint 3 progress

Dynamic IF now has explicit else/end edges and a shared branch-frame join, preserving logical occurrences across nested partial selections. Structural finish validates/freezes native control programs;511+ nested levels are tested. Full-domain native intermediate vectors keep float bits and Decimal metadata. Real SDK JSON parse/length and delayed native regexp kernels are covered by demanded-error and bad-unselected-row cases.11 native cases and34 SDK loop cases pass; the added PB eager-IF case also passes against original548812e in a separate test-only worktree. Scoped expression/executor clippy completes with warnings; required make clippy fails before checking because rustup is missing from PATH.

This proves the current pure/control subset only. Scalar versus vector invocation, arithmetic range/error/warning prefixes, shared state/multi-root ordering and BENCHMARK remain M1 work. The program does not yet provide complete native diagnostic adaptation. No decision has been made to relax original native/PB semantic differences.

## Checkpoint 4 progress and corrected constraint

Explicit Scalar/Vector invocation and signed integer plus/multiply now retain
NULL right-child demand. Native IF uses a shared scalar region because its
original row fallback skips that right child even inside Vector; a regression
first reproduced the candidate's incorrect overflow. Caller Vector mode is
restored after the region. JSON coercion preserves3140/empty versus invalid
text diagnostics through a structured code/message boundary.15 native and34
SDK tests pass; scoped library clippy passes with warnings.

Designer06e8cdf0 withdrew strict first-fatal ordering after rereading the user's
permission to reorder warning/error diagnostics. The earlier failing ordering
test remains evidence, not an acceptance requirement. The region mechanism is
required by independently tested child demand, not error ordering. Real state,
wide arithmetic and warning-prefix completeness remain unresolved M1 gates.

## Checkpoint 5 progress and corrected root admission

Checkpoint4's outer Vector assertion was missing a complete native baseline and
was wrong. Original numeric admission rejects the whole root containing IF or
JSON_LENGTH; the shared compiler now selects a single root scalar region for
fallback trees, preserving pure numeric Vector behavior. Both discrepancies were
reproduced before fixing. Invalid UTF-8 JSON retains native1105 diagnostics.

Shared group traversal now preserves RAND column order and variable row order.
Session capabilities carry values/state only; flat BENCHMARK repeats reexecute
children and preserve count/type demand.19 native and34 SDK tests pass; state
admission is deliberately limited to integer seeds and string variables. Wide
arithmetic, warning prefixes and remaining error adaptation still block M1.

## Checkpoint 6 minimum M1 probe outcome

Native full Real/Decimal arithmetic, string-to-Real warning prefixes and remaining
probe diagnostics now run inside the shared loop.29 native probes and all429 SDK
expression-library tests pass. A5525-file source snapshot brackets final reruns;
scoped expression/executor Clippy completes with warnings. Final evidence and
bounds are in shared-kernel/checkpoint-6.md; full workspace Clippy is separate.

Review exposed two corrected boundaries: session vector mode is independent of
multi-output traversal, and byte-only variable transport must reject metadata it
cannot preserve. New comparisons also exposed native Real-plus eager NULL demand,
full Real columns under temporary branch selections, and REGEXP sequential
argument effects. Actual failing records precede their fixes. Decimal arithmetic
helpers are counted even while original copies remain for the default route.

M1's minimum numeric/control/state probe set is ready for review. This does not
complete all expression signatures or justify default integration. M0 candidate
coverage needs semantic classification; M2 requires a bounded migration and
retirement budget. Full SQL/performance validation and old-path deletion remain
later work; broad10K and universal-performance goals are not established.


## Checkpoint 7 — review corrections


Designer review found two real gaps in checkpoint6: SETVAR directly or indirectly
accepted byte/non-default-collation literals despite dropping carrier metadata,
and omitting the original Decimal multiply fast branch changed hidden precision.
Both have original-entry failing regressions. SETVAR now rejects unsupported
literal carriers before encoding; string IF remains explicitly unsupported and
has a rejection assertion. The shared Decimal kernel restores the original
checked-i128 branch and exact storage/result scales. Native full-part comparisons
cover multiplication and a following division, including negative and zero factors.
See shared-kernel/checkpoint-7.md for final validation and counts. Minimum M1
remains subject to review; these fixes do not extend the admitted type families.


## Task35 — full-scope cost audit


The fixed checkpoint7 pair is the input for shared-kernel/cost-report.zh.md and
cost-inventory.py. All245 identities reference18 work packages, each budget summed once;
five exceptions are within245, not additional families. Include algorithm
migration/semantic revisions, original-code retirement, new/modified tests and
parameter/column/lifecycle integration. Distinguish observed current size from
conditional estimates and unbounded dependencies; no10K feasibility claim.
The largest five packages split semantic additions from module-boundary wiring,
old-code deletion and tests, with explicit overlap against checkpoint7's16348
changed lines. No product code changes or M2 rollout in this audit. Next work
selection is under design review: reuse existing lower-level value algorithms
before deciding whether to expand input/lifecycle implementation. FullM1 and product validation remain open.

Review correction: Rust escaped-newline strings desynchronized the lexical mask,
misclassifying10 test references in4 files. Regression assertions fail before the
fix and pass after it; full regeneration leaves327 witnesses. Budget grouping
does not establish disjoint actual line edits; those require final Git diffs.


## Task36 — reuse existing value algorithms


Designer d461fa2a limits this experiment to native Decimal and String-to-Real.
Keep the current admission checks and PB kernels. Add a path dependency from
SDK tidb_query_expr to the sibling TiDB tidb-datatype crate; verify both workspace
resolutions and absence of a dependency cycle. Re-export the existing Decimal
as ExpressionDecimal, pass that value directly through RPN scalars/vectors, and
remove the copied arithmetic and field reconstruction. Use str_to_float and
float_warning_input with the existing RPN diagnostic policy.

Retain invalid-value admission checks and full-value/hidden-precision regressions.
Run the31 native probes, full SDK expression library tests, scoped Clippy and
root lint; record incomplete formal gates separately. Compare both repos against
the original bases, including manifests/locks and docs. If a real cycle or
per-node carrier conversion is necessary, report evidence before widening the
boundary. No time/JSON/lifecycle expansion or default frontend activation.


Release follow-up complete: four paired rounds (192 cells), separate preparation
and reuse timings plus allocation counts, full-value preflight, and the existing
release invalid-Decimal test. Original datatype benchmark also ran unchanged.
Production reduction remains812 lines; small-value64-row reuse regresses35–38%,
while wide multiplication and the hidden-precision chain improve locally.
SDK default Jan30 toolchain fails MSRV before compilation; successful builds use
the adjacent fixed pair and Aug22 wrapper. See shared-kernel/datatype-reuse-performance.zh.md
for raw data, exact build limits, failed attempts and updated complete counts.
No broad performance, independent SDK build or completeM1 acceptance claim.


## Task37 — isolate the small Decimal regression


Designer01f727b4 authorizes the existing Decimal path only. Add unwrapped timing
to the existing harness, keep instrumented allocations, compare fixed before/after
inputs and separate native vector projection from explicit scalar evaluation.
EvaluatorSuite tries its Decimal fast path before the session vector switch, so
the scalar control must directly call Expression::eval per row in the harness.
Measure arithmetic, shared values and final Chunk writing separately or sample
their stacks. End-to-end projection always retains Chunk costs. Only change
evidenced dominant work; retain original datatype ownership and native fast path.
Do not clone algorithms, add a product evaluator loop or expand time/JSON/lifecycle.
SDK default-toolchain incompatibility remains a separate known limit.

### Progress

- [x] Claim task37 and identify native vector-switch measurement boundary.
- [x] Build unwrapped/instrumented harness and isolate fixed-pair costs.
- [x] Apply only justified minimum correction and validate complete values.
- [x] Recompute comparisons and prepare complete counts for fixed-pair review.

Preliminary unwrapped measurements reproduce small_mul64 regression (11.3 to
16.0 microseconds); separate Chunk writing stays near2.1 microseconds. Independent
user-cycle sampling attributes43.24% self samples to DecimalDigits::from_unsigned.
The five-line trial in that existing owner uses the standard integer formatter
only above24 inline digits; shorter coefficients retain the existing loop.
No RPN validation, invocation, native vector path or Chunk conversion changes.
89 datatype Decimal tests,31 native shared probes,429 SDK tests, scopedClippy and
rootlint pass. Final three-version four-round full-entry comparison completed,
including24/25-digit products and explicit shared/native Scalar controls.

Final1440 timing cells and360 separate allocation cells pass. small_mul64 shared
Vector falls15.49→9.61us, below the old copy in all four rounds;25-digit spill also
improves.24-digit inline Vector remains19.9–21.0% above the old copy; hidden-chain
64-row Vector is0.7–1.6% above the immediate baseline. Retain the bounded correction
and report these remaining costs; no universal-performance acceptance. Evidence,
source/test identity and complete counts are in shared-kernel/decimal-regression.zh.md.


## Task38 — real Decimal columns and cross-Chunk calls


Designer9355929a authorizes native Decimal column binding with complete original
FieldType metadata. Retain the original field in SharedExpression, compare full
fields for repeated bindings, and restore values through Row::get_datum before
passing original Decimal values to the existing shared RPN arithmetic and IF.
No callback to Expression evaluation and no copied conversion algorithm.

First add native differential tests that fail with the current unsupported input.
Then implement the existing binding/read boundary, checking complete Decimal
parts and output Chunk values for NULL, signed/wide values, unspecified scale,
unordered repeated rows, changed Chunk/context and recovery after errors.
Reuse the existing benchmark with one real-column complete-entry scenario.
Run shared native tests, SDK expression regression, scoped checks and root lint.
Retain fixed inputs, failed attempts, complete two-repository counts and reports.
Parameter rebinding and full lifecycle remain pending, as do default SDK builds
and the existing constant/native-vector performance gaps.

### Progress

- [x] Claim task38 and inspect existing binding and original row conversion.
- [x] Record failing native column regression and implement complete metadata binding.
- [x] Verify cross-Chunk/context/error recovery and complete output values.
- [x] Measure one real-column complete entry and prepare fixed review evidence.

Task38 implementation uses the full native FieldType directly in each binding,
rejects conflicting declarations for the same physical column, and converts only
demanded Decimal cells with Row::get_datum. Existing Int/Real/Bytes admission and
shared RPN are retained. New tests cover the native storage domain (including
40-digit values),24/25-digit cells, NULL, sign/zero, declared/unspecified scale,
scalar/one-row vector/multirow projection, IF demand, context precision4/8,
strict-error recovery, reordered duplicate rows and output after dropping input.
Unsigned bare columns retain their metadata; unsigned arithmetic remains rejected.
33 shared-native and429 SDK expression tests pass; scopedClippy completes with
warnings. One incorrect unsigned-arithmetic test expectation and an accidental
root-cwd Cargo invocation are retained as failed attempts, not product failures.
The one64-row real-column benchmark retains all four complete entry routes and
separate allocation runs; see shared-kernel/decimal-columns.zh.md for measurements
and remaining performance/default-build/full-M1 limitations.

Task38 outcome: the limited real-column slice passes33 native/429 SDK tests.
Four rounds produce16 unwrapped timing cells plus4 separate allocation cells;
all complete-value and Chunk checks pass. SharedVector median18.85us remains
slower than nativeVector6.59us for64 rows; sharedScalar31.56us also exceeds native
Scalar21.59us. This slice demonstrates input/reuse behavior, not performance
acceptance. Full inputs, failed attempts, library hashes and total changes are
preserved in shared-kernel/decimal-columns.zh.md and its receipt.


## Task39 — bounded parameter rebinding contract


Designer edc94a9c requests a short contract table and minimal native differential
proof/prototype before any product parameter admission. First correct task38's
metadata-conflict wording to per compiled root. Trace the actual prepared-plan
owner, dynamic parameter type inference and rebuild decision. Distinguish generic
parameter evaluation, typed evaluation and simultaneous ParamMarker/DeferredExpr.
Use existing Decimal arithmetic/IF to demonstrate one frozen program with changed
value/NULL/context, no prepare/dead-branch/empty-batch parameter reads, error
recovery and old result survival. Check overflow diagnostics use the current value.
If current interfaces cannot preserve this contract, retain product rejection and
report the smallest counterexample and required interfaces; no broad binding
framework or fallback to the old evaluator. Tests may use the existing RPN input
interface as an explicitly test-only fixed-type slot prototype.

### Progress

- [x] Claim task39 and correct per-root metadata wording.
- [x] Trace actual SELECT cache owner: asymmetric Decimal compatibility, separate NULL/type keys, per-execution parameter Arc and generation check.
- [x] Run focused native forms and demanded-slot proof: two new tests pass; integer conversion and current-value overflow text remain concrete counterexamples. RPN empty batches require the existing adapter guard.
- [x] Publish bounded contract and decision: keep product parameters rejected. 35 shared/native, 8 existing prepared and 1 actual session cache test pass; root lint passes. Source inputs/logs and complete counts accompany the report; no SDK changes or new performance claim.


## Task40 — execution-owned typed Decimal parameters

Authority: Designer cda358b9 (task39 accepted), task40 self-claimed. Compile only
parameter leaves in existing Decimal arithmetic; IF may contain that arithmetic.
Reject bare/IF-direct/deferred parameters. Extract existing Decimal conversion and
padding into datatype, reused by Constant and shared RPN. Read raw current Datum
only on demand; immutable binding throughout each call. Overflow rendering uses
static fragments and parameter slots only when an actual overflow occurs, with
native generic fallback. No default executor, worker or cross-EXECUTE program
cache changes. Explicit callers own bound-signature programs for one execution.

- [x] Record initial admission failure and share the original conversion, padding and diagnostic-value primitives in datatype.
- [x] Add demanded typed parameter instruction and error-only dynamic diagnostics; preserve inferred NULL types and reject unresolved signatures using the native domain check.
- [x] Compare real native arithmetic Vector and IF row paths, warnings/strict,
      runtime carriers, demand, empty input, recovery and retained outputs.
- [x] Run affected datatype/native/SDK checks and lint; publish fixed evidence,
      complete counts and remaining limitations.

### Task40 review correction (Designer 2fa948e4)

Keep the existing signed, nonarray Decimal static source boundary for parameter
leaves. Static String enters a different native conversion path and is rejected.
Diagnostic-only parameter read failure must retain generic Decimal overflow1690;
a demanded parameter leaf still propagates its original1105. No broader casts.

- [x] Preserve both before-fix native differential failures.
- [x] Apply the two minimal shared fixes: 41 shared, 8 prepared, 429 SDK pass; root lint/scoped Clippy complete.
- [x] Publish corrected fixed pair, evidence and full incremental/cumulative counts in typed-parameters.zh.md and its review receipt.

## Task41 — standalone SDK expression build

Authority Designer dd2cd5b5; task40 bounded corrections accepted. Keep the original
datatype owner, resolve SDK via fixed TiDB Git revision, and patch that Git source
to local datatype in the TiDB root workspace. Verify the entire datatype leaf
closure has one type identity. Align the declared SDK nightly with Aug22 without
lowering MSRV. Validate ordinary locked Cargo in a separate SDK checkout with no
adjacent TiDB, plus native shared/prepared regressions. Record toolchain-wide CI
impact without claiming all TiKV packages validated. No SQL/lifecycle expansion.

### Progress

- [x] Preserve standalone path/MSRV failures; verify Git nested workspace resources.
- [x] Wire fixed Git dependency and local root patch; inspect metadata/tree identities.
- [x] Validate isolated SDK429 and Main41/8 with declared toolchains, lint and fixed evidence.
- [x] Publish source/lock differences, complete counts and explicit remaining limitations.

Task41 outcome: independent SDK build and429 tests pass with declaredAug22 and
fixedGit8abb owner; Main41/8 pass with one local datatype identity. Metadata,172
owner files,1421 SDK compile inputs and5525 local source inputs match. Formal SDK
makeclippy exceeded our120-second bound in deny tool installation and was
terminated by our runner; full workspace/macOS compatibility is not claimed.
The enterprise submodule default is explicitly skipped for Cargo consumers;
enterprise users must request checkout explicitly. See independent-sdk-build.zh.md.

### Task41 review corrections (Designer 5caea732 / 227514d5)

The enterprise init target must explicitly select checkout despite update=none.
The SDK deny sources policy must admit only the chosen YangKeao/tidb Git URL,
retaining unknown-git/registry deny and the existing organization list.
Use a local temporary submodule for the first regression, and cargo-deny0.18.9
sources checks for the second. Inherit product regressions only after comparing
all recorded inputs and isolating the deny policy change from compiler inputs.

- [x] Preserve skipped-file regression; explicit checkout and repeated init pass.
- [x] Preserve five source rejections; exact URL addition passes cargo-deny0.18.9 sources.
- [x] Record lint, input correspondence, fixed pair and full counts in the review receipt/report.

## Task42 — real Decimal column cost and bounded reuse

Designer963621fa authorizes remeasurement of the existing column_mul64 complete
entry on the current fixed source, then targeted attribution to input
materialization, arithmetic, shared RPN dispatch and output writing. Preserve
the existing four routes and full-value checks. Implement at most one small
original-owner/shared-path optimization if evidence supports it; otherwise
publish the compact-native-representation cause and a counted reuse proposal.
No copied Decimal algorithms, old evaluator callbacks, expanded matrix, default
SQL or lifecycle changes. Original datatype edits require matching SDK Git pin.

### Progress

- [x] Build fixed current baseline; retain complete four-route timing and separate allocation records.
- [x] Input/representation costs dominate; publish an 810-line estimated owner-reuse proposal, no product change.
- [x] Validate full values, same-library binaries, diagnostic phases and source correspondence; preserve all records.

Task42 outcome: the same product repeated with the same final harness gives
sharedVector19.173us versus nativeVector6.590us median. Input materialization
and ASCII coefficient conversion remain costly; allocator direct self samples
are3.29%, not the whole gap. RPN direct samples are not a complete cost bound.
A compact original-owner value plus root output adapter needs broader interfaces,
so this authorized finite batch ends with attribution and a counted proposal.
No optimization, lifecycle/default SQL change or universal performance claim.
See shared-kernel/decimal-column-cost.zh.md for evidence, limits and counts.

## Complete compact Decimal and root-output prototype (task43)


This finite prototype follows the accepted task42 attribution. Its baseline is
TiDB e18f0b5c282c728192b0c502d2e151c405807b0d and SDK
780ab3e776f47d634ac8963437baf8477a7b2df7. The goal is to remove avoidable
Decimal text materialization while retaining one original RPN evaluation.
Designer bcf4e55a authorizes the entire prototype without intermediate review.
Default SQL admission, lifecycle and additional function families remain outside
this scope; a need for a generic numerical rewrite or a new execution mechanism
ends this prototype at a fixed, reviewable result.

Milestone one moves the existing native signed i128 coefficient primitive into
`rust/crates/tidb-datatype/src/decimal/compact.rs`. All existing native add,
subtract, multiply, integer and cast callers in `tidb-expr/src/scalar_function.rs`
use that owner; the original Decimal fast paths share it and retain wide fallbacks.
The full private arithmetic carrier keeps hidden scale and declared column shape.
Only shared multiplication takes its compact path; other arithmetic delegates to
original Decimal with already evaluated operands. The ordinary Chunk reader and
shared reader use one datatype-owned column shape rule.

Milestone two keeps `SharedProjection::evaluate` and adds
`evaluate_into_chunk(input, rows, ctx, effects, output)` through the same
`RpnExpression::eval_group` call. Detect input/output and output/output aliases;
the caller additionally must own output independently of other live chunks.
Evaluate all roots and validate their results before resetting output. Errors
leave output untouched; session effects are not rolled back. Wide result writing
retains original lossy Chunk conversion. No second interpreter is introduced.

Milestone three validates both old and direct APIs against native output, including
NULLs, hidden precision, wide fallback, declared shape, selections, empty batches,
multiple roots and error/effect behavior. Run targeted datatype Decimal tests and
`cargo test -p tidb-expr --features shared-kernel shared_kernel`; record failures
and corrections. Run existing SDK expression regressions, scoped checks and
root `make lint`. Commit the datatype owner, pin the SDK Git dependency and lock,
and verify the independent SDK and TiDB patched dependency graphs each have one
owner identity. Repeat the existing real-column complete-call benchmark with
separate timing and allocation binaries; compare native and old owned API before
and after, listing the new direct API separately. Preparation remains excluded.

Initial 810-line estimate is provisional. Actual additions/deletions must include
all native callers, datatype, SDK, tests, configuration and evidence separately.
Commands and build environment follow `shared-kernel/independent-sdk-build.zh.md`;
use declared nightly with ordinary Cargo, preserving failed logs outside immutable
previous evidence. Local evidence for this batch is `evidence/compact-output` in
the Engineer workspace. Never overwrite earlier measurements or protected trees.

### Progress


- [x] Claimed task43; inspected primitive, native callers, original Decimal paths,
  shared typed values, column conversion and Chunk alias interfaces.
- [x] First implementation compiles in the TiDB patched graph.
- [x] Complete boundary tests, independent SDK pin and build, fixed measurements.
- [x] Self-review and fixed signed product checkpoints; final report/counts prepared for review.

### Surprises & Discoveries


The native primitive also serves addition, subtraction and integer cast; removing
only its multiply use leaves a duplicate owner. Chunk already exposes column
identity checks, so no new ownership mechanism is needed.

### Decision Log


Use a private compact/wide carrier under the existing datatype owner. Preserve
owned output and add a common result consumer after the existing group evaluator.
This changes the output boundary without adding execution dispatch. (2026-11-13)

### Outcomes & Retrospective


Fixed product source005b431ce6 and SDK5582f06 complete this finite prototype.
43 shared, 8 prepared, 90 datatype, 10 Chunk, 1 native vector and 429 independent
SDK checks pass. The original owned Vector call improves46.40–47.24% in the fixed
single column scene; direct output is a separate API, still22.18–30.95% above
native Vector. Allocation bytes increase. Initial810-line estimate was exceeded
by the first complete1045-line version before final evidence. See
`shared-kernel/compact-output.zh.md` for all counts and failures. Full SDK Clippy
fails19 dependency advisories, also reproduced on baseline; no full-gate claim.
An initial timing/allocation library mismatch is retained; final measurement uses
identical explicit libraries within each product version and all rounds rerun.
Default SQL, broader semantics and lifecycle remain outside this task.


## Task44: one private carrier layout change


Designer180bc037 authorizes moving declared shape into the private Compact variant in rust/crates/tidb-datatype/src/decimal/compact.rs. Wide keeps the original Decimal shape and uses Decimal::from_column. Keep native CompactDecimal and all arithmetic, admission, fallback and output contracts; do not add per-value boxing, packed/unsafe layout or external metadata. The observable goal is fewer requested allocation bytes without additional calls or reproducible same-route slowdown, not new SQL coverage.

### Progress


- [x] Record baseline actual carrier/Option/embedded node sizes and alignment; seal twelve direct library inputs before measurement.
- [x] Change only private storage, publish original owner revision, synchronize SDK Git pin/lock and verify both dependency graphs.
- [x] Run existing shared, prepared, Decimal, Chunk, native-vector and independent SDK regressions; retain every failure.
- [x] Compare both five-route binaries with four alternating rounds (40 timing cells), plus 10 separately counted allocation cells; preserve old four-route runner mode.
- [x] Publish fixed result, all cost categories and full-base counts for review, even if negative; end this Decimal single-case optimization window.

Use rust/docs/shared-kernel/build-decimal-bench.py with --direct-chunk for both versions. Link plain/counting harnesses against the same twelve copied libraries per version, verify hashes before measurement. Use run-decimal-bench.py --columns --direct-chunk --before-direct-chunk with 20000 iterations and four rounds for timing; --allocations --iterations 100 --rounds 1 for separate counters. DECIMAL_BENCH_LAYOUT=1 prints actual Rust type sizes without running measurements. Existing tests and make lint remain required. SDK tests use the standalone checkout, with no adjacent TiDB and a fixed Git dependency. Preserve existing numerical tests rather than invent new semantics. Receipt/scripts/raw records live in evidence/carrier-layout; never overwrite task43 evidence. All support material counts in the final diff.

### Decision Log


One layout trial only; a negative result is a complete bounded deliverable. Same-ID advisory observations from task43 did not pin the advisory database revision and do not establish a fixed audit snapshot. Task43 limited review accepted, broader M1 lifecycle and full migration budgets remain open.

### Outcomes & Retrospective


The fixed result is in shared-kernel/carrier-layout.zh.md with 40 timing and 10 allocation cells. Carrier/Option/ExpressionScalar shrink from112 to80 bytes; RpnExpressionNode240 to208, RpnStackNode160 unchanged. Owned Vector and direct each request8064 fewer bytes per call, with unchanged allocation counts. Direct paired timing ranges−2.33% to+1.88%; no stable speedup claim. All original regressions and scoped checks pass; full SDK Clippy remains the inherited19-advisory failure, not rerun. End this Decimal single-case window; broader lifecycle and migration budgets remain open. Final report, receipt and publication are tracked by task44 review handoff.


## Task45: execution owns seeded state


Keep each compiled SharedProjection and RPN program frozen. Add an explicit execution object borrowing that program and one fixed native effects context. It owns SDK seeded RAND slots indexed by the original program's local node ordinal. Reuse the object across chunks; creating another object starts a fresh seeded stream. The existing eval_group and frame loop still compute all demand and effects. Native old-path address keys stay untouched; shared-path address keys are removed. A declarative capability defaults false for context-free first-sample behavior and is true for real StmtContext and supporting tests. Unseeded RAND, variables, parameters and warnings still use actual statement/session capabilities.

### Progress


- [x] Preserve a deterministic before witness on the same program and effects; do not label this a default SQL bug.
- [x] Implement frozen-program executions and migrate all experimental context callers without per-batch recreation.
- [x] Verify independent executions, continuation, dynamic seeds, branch/selection/repeat demand and recovery after errors; inspect real EXECUTE context ownership.
- [x] Run targeted native/SDK regressions, scoped lint and source correspondence; publish full counts and fixed results for review.

### Plan and acceptance


Edit SDK components/tidb_query_expr/src/types/expr_eval.rs for owned stream slots and pass them through the existing loop. In TiDB rust/crates/tidb-expr/src/shared_kernel.rs expose start_execution with a fixed effects borrow, preserving no-context convenience semantics. Add the capability in expr context.rs and real executor StmtContext implementation. Migrate tests and the opt-in benchmark to hold executions across repeated evaluations. First run cargo test -p tidb-expr --features shared-kernel --lib shared_kernel_independent_executions_reset_seeded_state --locked --offline -- --nocapture and retain its expected before failure; then run the shared suite and SDK expression library regressions, plus actual session EXECUTE seeded/unseeded checks, make lint and formatting. All failures stay in evidence/execution-lifecycle. No default SQL integration or cross-EXECUTE cache is introduced; existing EXECUTE creates a new StmtContext and must retain that behavior. Do not clear session state, use global IDs, leases, or another evaluator. No Decimal algorithm or measurement expansion. The Designer owns the full migration budget update; this batch reports actual changes only.

### Decision Log


Designer7db0fd17 authorizes one complete lifecycle task through fixed implementation review. A fresh EvalContext on the same program/effects is a deterministic old-API isolation witness, not evidence that the default SQL EXECUTE already leaks. Known full SDK advisory gate is inherited; no unrelated dependency upgrade or repeated advisory run.

### Outcomes & Retrospective


Pending implementation and regression evidence.

### Outcomes and limits

Task45 preserves the original shared frame loop and isolates seeded state by execution/local instruction. The runtime before witness fails as expected; 47 shared, 8 parameter, 430 independent SDK, one real StmtContext integration and one actual PREPARE/EXECUTE check pass. The initial executor unit-test target is blocked by seven unchanged TableResolver fixtures missing clause_message; the dedicated integration target exercises the real owner without compiling those unrelated unit fixtures. New test import/admission corrections and all failed attempts are retained. State sizes are SDK64/expression88/projection48 bytes, excluding dynamic maps/root vectors; no performance improvement is claimed. See shared-kernel/execution-lifecycle.zh.md and its validation receipt for fixed sources, all counts and limitations. Default SQL and cross-EXECUTE caching remain outside this implementation.

## Task46: typed DATE through the original value owner


This plan follows PLANS.md and Designer b9f2f967. Native and protobuf DATE must share a pure algorithm in tidb-datatype while preserving their different zero-date and hidden-clock policies. Shared values carry the original Time; only Time/NULL constants, actual Chunk columns and IF results are initially admitted. No string/number/Duration/parameter conversion, default SQL integration, other time families or new performance matrix belongs here.

### Progress


- [x] Pin old native and PB expected behavior, including all-zero/partial-zero and hidden clock.
- [x] Extract original-owner value policy, migrate old native/PB DATE and pin SDK Git owner.
- [x] Add shared Time scalar/vector, input/IF/DATE/owned/direct output through the existing frame loop.
- [x] Verify policies, FSP/domain distinctions, demand/diagnostics, columns, error recovery and SDK regression; publish source correspondence and all counts.

### Plan and validation


Edit rust/crates/tidb-datatype/src/mysql_time.rs for a pure DATE operation returning no value when the explicit zero policy rejects input. Keep diagnostics in native time_fn/mod.rs and SDK impl_time.rs. Add narrowly named unchecked memory-bit accessors to SDK codec/mysql/time/mod.rs; expression code maps semantic TimeType/FSP and calendar bits, never storage packed/timezone conversion. Native Time/SDK Time metadata are not universally interchangeable: SDK nibble15 is Date, native nibble15 is Timestamp/FSP7. Preserve pre-clear zero checks and existing Chunk FSP validation. Extend shared carrier variants, checked RPN node, original loop and NativeInput/native_output; no second evaluator or host subtree callback. DATE policies run for every demanded row, even constant input, because native DATE is scalar fallback.

Run cargo test -p tidb-expr --features shared-kernel --lib shared_kernel and targeted native DATE/SDK DATE tests before/after; full independent tidb_query_expr library regression, datatype and Chunk time selectors, scoped Clippy, rustfmt and root make lint complete validation. Use the existing declared toolchain and environment from the shared-kernel design. Fix only scoped failures; preserve every attempt under evidence/typed-date. If necessary interfaces force a general time framework, save the smallest fixed prototype and concrete counterexample instead of expanding scope.

### Surprises & Discoveries


Native DATE checks all-zero separately and always clears the clock. SDK DATE rejects zero month/day regardless of mode and its existing Date setter preserves hidden clock. Original Time/NULL argument casts are identity; other domains require policies outside this batch.

The original checked constructor clamps FSP7 to6; it does not reject it. The expected-panic test failed and the Designer corrected the design (6e2c852f). Keep unchecked constant FSP7, original Chunk getter FSP6, and SDK nibble15 Date/FSP0 as three separate contracts. Full native Expression formatting of FSP7 already panics, so its typed DATE signature is the limited original oracle. New NULL-array rejection was tested failing before the guard and passing afterward.

### Decision Log


Keep frontend diagnostics and conversion admission; share only the original-owner pure value operation. Original Time carriers retain hidden fields, while Chunk retains its original precision normalization. Frozen programs and explicit execution objects remain task45's owners.

### Outcomes & Retrospective


Implementation and validation are complete: 53 shared, 34 original Time, 21 native time, one DATE source-row, one SDK memory-bit and 432 independent SDK expression tests pass; scoped Clippy and root lint exit0. Both graphs have one identity per original owner and all 172 owner files/1421 SDK inputs match. See shared-kernel/typed-date.zh.md and its receipt for exact sources, failures and full counts. No human acceptance, default SQL integration or performance claim.

## Task47: lazy preparation for literal REGEXP_LIKE


Following PLANS.md and Designer871fddab, reuse a compiled native regular expression within one explicit execution, without changing when its arguments or errors are demanded. A strict literal is Constant::literal_value with no parameter/deferred source. Only literal pattern and omitted/literal match_type qualify; dynamic native expressions and PB compile-time metadata retain their current paths.

### Progress


- [x] Record baseline repeated preparation with exact tested source reconstruction.
- [x] Add narrow checked instruction and execution-owned per-node preparation; preserve sequential argument guards.
- [x] Verify successful/error reuse, distinct executions/nodes, cross-batch/error continuation, empty/dead paths and dynamic/PB behavior.
- [x] Run scoped checks and independent SDK regression, verify graphs, publish fixed sources and all incremental/original counts.

### Plan of work and validation


The native compiler in rust/crates/tidb-expr/src/shared_kernel.rs currently emits sequential RegexpArgument guards followed by an ordinary FnCall. Emit a dedicated native preparation instruction only for strict literal pattern/flags, leaving all argument instructions and guards. SDK components/tidb_query_expr/src/types/expr.rs validates the instruction's bytes inputs and integer output. Its existing expr_eval.rs frame loop accesses an execution-owned map keyed by local instruction ordinal after the guards. Regex construction and error mapping remain in impl_regexp.rs; successful Regex and only original native1105 errors can be replayed. No cached host expression, global key, shared cache, new evaluator or regex family is added.

Use test-only preparation counts to prove the old instruction prepares repeatedly and the new one only once; capture commit, binary patch and selected input hashes before each command. Unit tests must prove parameters/NULL/UTF8 and side effects still precede cache lookup, distinct executions are independent, repeated selections/chunks retain state, dead/empty input does not prepare, and an error does not prevent later legal batches. Preserve PB's earlier error timing. Run Main cargo test -p tidb-expr --features shared-kernel --lib shared_kernel, independent SDK cargo test -p tidb_query_expr --lib, affected scoped Clippy, rustfmt and root make lint. Full workspace's inherited advisory failure is not repaired or retried here. Count all files against task46 and original endpoints; no performance claim.

### Surprises & Discoveries


The native helper currently recompiles after all sequential guards. PB uses init_regexp_data during expression construction, which must remain untouched. Frozen-program metadata cannot own native mutable preparation state.

### Decision Log


Keep one narrow execution map and reuse the existing regex builder. Error caching must preserve numeric identity/message without stringifying the outer error wrapper. Test snapshots use immutable commits plus complete working patches and selected-input hashes; unlike task46 this makes each attempt reconstructible within the declared source scope.

### Outcomes & Retrospective


Implementation and validation complete; fixed-source review pending.

Task47 implementation: literal native nodes retain preparation in the existing
RpnExecution; dynamic calls keep FnCall. All argument guards precede lookup.
Only inner 1105 code/message is retained; other errors pass through uncached.
The before witness reached an assertion (six builds versus one), then the same
single-execution case passed. Expanded checks cover two executions, nodes,
BENCHMARK, dead branch, errors/guards, and native dynamic Chunk/output recovery.
Local 56 shared, 436 SDK, original regexp 24 passed/1 existing ignored; scoped
Clippy and root lint passed. Independent SDK 436 tests passed; dual graphs match
172 owner files and 1421 SDK inputs. See shared-kernel/regexp-state.zh.md for
fixed sources, failed attempts, per-command source reconstruction and total counts.
Native literal NULL remains on the existing uncached path, with guards preventing
preparation. No generic cache framework or PB timing change was needed.
