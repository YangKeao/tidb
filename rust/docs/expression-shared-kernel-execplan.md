# Share TiDB and TiKV expression execution from the original baselines

This living ExecPlan follows the repository root PLANS.md. Update it with each checkpoint.

## Purpose / Big Picture


Both Rust frontends will compile expressions into one TiKV RPN program, where RPN means the flat instruction sequence that computes child values before invoking a function. One evaluator must own branch demand, argument coercion and function execution. This replaces the previous experiment's multiple profiles and host task protocols while retaining SQL behavior. The first checkpoint is explicitly opt-in; it must demonstrate real TiDB Expression/Chunk and TiKV protobuf inputs, not a second synthetic evaluator. Default execution changes only after full coverage.

## Progress


- [x] 2026-11-13: create independent expression-shared-kernel-m1 worktrees from TiDB364aef2bab5cc633ecb76a775ae8f36f86a6687d and TiKV548812e1ef57aef077a2062a9cc356640a6347f5.
- [x] Read original entry points, repository instructions and Designer ddb87c63ff2d0dbe40ede1e84d4b1182b6541cbe design.
- [ ] Inventory 245 function families, five exceptions, production entry points and file-level cost.
- [x] Build opt-in shared MD5 through native and protobuf inputs; checkpoint1 published 7e3a636 / 52d7be2.
- [ ] Add exact Decimal/full-domain floating carriers and demanded complex input conversion.
- [ ] Extend original RPN control flow to lazy IF, state and repeated evaluation; verify original PB behavior independently.
- [x] Publish checkpoint1 with four passing tests, counts and explicit remaining M1 gates.
- [x] Demand column reads from the RPN loop; missing unselected column and integer [7,2,7] tests pass.
- [x] Reproduce three review regressions, then fix/restrict admission; seven shared tests pass.
- [ ] Complete dynamic branching, scalar/vector contracts, complex error guards and state execution; M1 is not complete.

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
