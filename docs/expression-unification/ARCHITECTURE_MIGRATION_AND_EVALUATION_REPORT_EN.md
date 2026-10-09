# TiDB / TiKV Expression Implementation Unification: Architecture, Migration, and Evaluation Report

> Report baseline: TiDB `364aef2bab5cc633ecb76a775ae8f36f86a6687d`, TiKV `548812e1ef57aef077a2062a9cc356640a6347f5`
> Current implementation: TiDB `a3cf8821f63bcb39460d4d7fc80db336b63fc3f9`, TiKV `196f8025f03f250bd70fe39553bb3c5e4ff74e5a`
> Report checkpoint: `architecture-performance-report-217`
>
> Subsequent architecture state (`lane-cache-only-runtime-220`): The pool/execution performance and lifecycle sections in this report are retained as records of the experiments at that time and no longer describe the current implementation. The shared `ReadyValuePoolPolicy/Owner/Execution/Scope`, slot/lease state machine, and session/request runtime have been removed; each executor evaluation lane now directly owns an operation→worker `ReadyValueCache`, while unbound calls use a stack-local one-shot cache.

## 1. Executive Summary

The goal of this experiment was not to write another cross-repository expression interpreter, but to eliminate duplicate implementations of expression algorithms between TiDB and TiKV: **TiKV becomes the sole owner of value algorithms and the RPN execution kernel, while TiDB retains the type metadata, evaluation order, session/context, warning/error projection, and lifecycle that the SQL host must control.**

The overall dependency order and objective proceed bottom-up: first unify value and type foundations such as collation, Decimal, and temporal types; then establish the TiKV local RPN facade, strict control flow, and bounded resource protocol; finally switch the AST, typed scalar, PB, vector, Unistore, and other entry points one function family at a time, deleting the corresponding native algorithms in TiDB after each successful switch. To accelerate the Demo, the actual checkpoint advanced work in an interleaved manner: runtime/control and subsequent Decimal work overlapped in parallel, so M0–M6 are conceptual workstreams and dependency relationships, not strictly sequential commit phases. Falling back to the old TiDB algorithm when TiKV rejects or fails is prohibited, because otherwise the same semantics would still have two owners.

The frozen denominator is 245 implemented pure/contextual expression families; 240 have now completed functional TiKV-only takeover and native algorithm removal, or **240/245 (97.96%)**. Five families with excessively high compatibility costs remain temporarily in TiDB, but have been narrowed to one closed `host_compat` entry point and no longer depend on the complete generic native evaluator. The strict per-family final audit count remains 0, and `pr_ready=false`; this report does not characterize Demo completion as release-ready or as complete Go package transcreation.

## 2. Overall Design

### 2.1 Ownership Boundaries

| Layer | TiKV responsibilities | TiDB responsibilities |
|---|---|---|
| Values and algorithms | collation/key/LIKE, Decimal operations, temporal/JSON/vector primitives, math/string/crypto kernels | No longer retain a second implementation of migrated algorithms |
| Expression execution | official RPN wrapper, local compiler, strict selectors, fixed recipes, batch/selection driver | Integration of AST/PB/typed expressions, argument demand, result projection |
| Types | `FieldType`/`ScalarValue`/`VectorValue` representations and checks required by kernels | Complete SQL `FieldType`: flags, flen, decimal, charset/collation, ENUM/SET, array, etc. |
| Context and side effects | Accept explicitly passed timezone, precision, cache invocation, and typed host requests | SQL mode, statement clock, packet limit, warning sink, sysvars, identity, parameters, and correlated columns |
| Lifecycle | Creation, execution, cleanup, and resource limits for workers/programs/frames | Executor evaluation lanes directly own operation→worker caches; statements/sessions do not own worker pools |
| Errors | Structured admission/runtime/resource errors and actual TiKV causes | Convert to TiDB SQL errors/warnings while preserving error timing and warning order |

This boundary deliberately distinguishes “algorithms” from “host effects.” For example, SQL coercion before converting a string to an integer, the session timezone for TIMESTAMP, deprecated JSON warnings, and whether an unvisited branch is evaluated are not facts a pure kernel can infer independently; TiDB must explicitly select or pass them. Conversely, comparisons, hashes, Decimal arithmetic, or JSON primitives after coercion should not be implemented again in TiDB.

### 2.2 Three Core Constraints

1. **Single implementation**: The final kernel for a migrated family exists only in TiKV. A TiDB adapter must not reimplement that kernel, but it may perform explicit child demand, SQL coercion, typed staged-host orchestration, context/effect projection, and representation conversion, and must demonstrate that shared comparisons, casts, and similar operations it invokes do not form a native fallback.
2. **No native fallback**: Admission, resource, bridge, or execution failures propagate directly; the old TiDB algorithm must not be run after a failure.
3. **Compatibility is not limited to return values**: NULL, unsigned values, scale/FSP, collation, error/warning timing, child demand, selection order, cache/rebind behavior, and lifecycle are all part of the contract.

### 2.3 Simplified Call Graph

```text
SQL AST / typed row / PB / Unistore DAG
                  │
                  ▼
TiDB frontend: signature + FieldType + demand + coercion + effects
                  │
                  ▼
TiDB glue: checked value bridge / fixed recipe / execution scope
                  │
                  ▼
TiKV local compiler + official RPN wrapper + shared algorithm
                  │
                  ▼
ComputedValue / structured error / warnings
                  │
                  ▼
TiDB Datum + declared SQL metadata + SQL error/warning projection
```

For overlapping real PB signatures, the wire builder and local facade share `prepare_selected_call`, RPN metadata, and the kernel; the wire path does not pass through TiDB. Local-only operations instead use a closed local selector without broadening wire admission. The purpose of the local facade is to let TiDB reuse the same TiKV implementation in the same process, not to copy the wire evaluator.

## 3. Interface Design and Glue Code

### 3.1 Cross-Repository Dependencies and Type Boundaries

In `rust/Cargo.toml`, TiDB directly depends on `tidb_query_crypto`, `tidb_query_datatype`, and `tidb_query_expr` through paths to an adjacent worktree. This shares Rust types and function calls without introducing RPC or fabricating protobuf signatures for local-only functions.

`rust/crates/tidb-datatype/src/tikv_compat/` is the checked value boundary. It performs only exact representation transport: it does not perform SQL casts, infer missing schemas from values, convert branches that have not yet been demanded, or route Decimal/temporal/JSON values through strings or `f64`. Complete SQL metadata remains in TiDB.

TiKV uses `types/function.rs::FunctionRef` to distinguish:

- `FunctionRef::TiPb(ScalarFuncSig)`: a real wire signature;
- `FunctionRef::Local(LocalFunctionId)`: a closed TiKV-owned local function with no wire ID.

This avoids “borrowing” a nonexistent or semantically inequivalent PB number for local reuse.

### 3.2 Local Expression, Compilation, and Runtime Interfaces

`components/tidb_query_expr/src/local/spec.rs::LocalExpr` is immutable typed construction input containing:

- `Constant`;
- `InputSlot`;
- `Call { function, args, return_type, metadata }`;
- `HostCall { slot, args, return_type }`.

It does not own a session, row, or native expression callback. `LocalCompileContext` and `CompileLimits` bound the number and depth of nodes; excessively deep trees are dropped iteratively so that rejected input does not overflow the stack again during drop.

Compilation interfaces are separated by provable semantic domains:

- `compile_local` / `compile_local_with_hosts`: ordinary local programs;
- `compile_local_profiled`: strict profiles;
- `compile_control_with_lineage`: control flow and result lineage;
- `compile_numeric_batch`: accepts only declared numeric batch graphs.

The outputs are `LocalProgram`, `LocalControlProgram`, or `LocalNumericBatchProgram`. Distinct `ProgramEntry` variants cannot impersonate one another; even an empty batch must validate its entry instead of bypassing admission.

The core runtime interfaces include:

- `LocalBatch`: borrows input columns, physical rows, and a selection;
- `LocalProgram` uses width-one row scratch within a call; `ExecutionLimits` stores only immutable execution bounds;
- `LocalRuntimeServices::binding_schema/read_input/host_services`: reads one bound value only when it is actually demanded;
- `InputRow { occurrence, input_row }`: distinguishes the occurrence position within a selection from the physical row, preserving duplicate and out-of-order selections;
- `ExecutionLimits`: bounds steps, frame depth, active host tasks, and retained bytes.

`LocalRuntimeServices` returns values rather than TiDB `Expr` objects or recursive evaluator closures, so the TiKV driver cannot covertly re-enter the TiDB expression evaluator.

### 3.3 Fixed Ready-Value ABI

Many functions have already completed their original child demand and coercion in TiDB. To avoid inventing a separate protocol for each family, TiKV provides a closed ready-value ABI:

- `EvaluatedBytesOp`: a finite set of operations;
- `EvaluatedArgs`: finite typed shapes that explicitly distinguish nullable Int/Bytes, IEEE bits, Decimal, temporal, LIKE/regexp invocations, and so on;
- `prepare_evaluated_bytes` / `EvaluatedBytesWorker`: prepare and reuse an official RPN worker;
- `ComputedValue` / `EvaluatedBytesResult`: return owned typed results.

Before execution, the worker validates the operation, shape, type, and role; NULL also enters the real wrapper, and TiDB is not allowed to fabricate an “equivalent result” for NULL to bypass error, metadata, or resource timing.

### 3.4 TiDB Glue

`rust/crates/tidb-expr/src/tikv/mod.rs` collects crate-private adapters. General-purpose glue resides in the historically named `tikv/ready_value.rs`, which now handles more than ASCII:

- `ReadyValuePoolPolicy`: explicit resource policy;
- `ReadyValuePoolOwner`: pool root;
- `ReadyValueExecution`: an independent statement/request execution that can be closed separately;
- `ReadyValueScope`: an affine scope for one lexical invocation;
- `evaluate_prepared_args_in` / `evaluate_args_in` / `evaluate_bytes_in`: prepare arguments, lease a worker, execute, and project the result.

`ScopedReadyValueColumns` overrides only the lane-local `ready_value_cache` capability; all other `Columns` methods delegate to the original context, preventing the adapter from losing host information such as timezone, SQL mode, warning sink, parameters, and identity.

Family-specific glue resides in `tidb-expr/src/tikv/{cast_*,date_arithmetic,interval,extremum,in_list,extract,...}.rs`. The original SQL frontend in `ops.rs`, `func.rs`, `scalar_function.rs`, `builtin_ext/`, and `time_fn/` continues to determine demand, coercion, metadata, and effects; the final family kernel enters TiKV, while TiDB glue may still perform host coercion/effects according to TiKV staged requests and complete required suboperations through shared comparison/cast paths.

### 3.5 The Host Protocol and the Five Final Exceptions Are Not the Same Thing

The TiKV local runtime has a general typed host protocol:

- `HostCatalog` / `HostSignature` / `HostSlot`: unforgeable catalog identity and typed slots;
- `PreparedHostCall`: a compiler-validated call;
- `HostStep` and `LocalHostServices::{start,resume,cancel}`: the host requests arguments in stages and returns results.

This protocol contains no arbitrary recursive callback; the driver releases borrows before computing requested arguments, and task generations prevent incorrect reuse.

The five currently deferred families have **not** received TiKV/PB/Unistore admission and do not masquerade as completed migrations through this host protocol. They are actually managed by TiDB `rust/crates/tidb-expr/src/host_compat.rs`:

- `eval(name, values, ctx)` matches only five exact names/arities;
- unknown names return `None`, with no generic fallback;
- `eval_json_schema` is the only expression-level adapter, preserving the schema cache and ensuring that a NULL schema neither evaluates the document nor performs I/O.

Production matches in generic `builtin_ext::{json,info,crypto}` no longer admit these five items, so retaining the exceptions does not mean retaining a complete native expression evaluator.

### 3.6 Error and Diagnostic Interfaces

TiDB's `tikv/runtime_failure.rs` and `tikv/adapter_failure.rs` preserve, respectively, runtime and bridge/admission error categories, phases, and original causes. Errors must not trigger native re-execution through string matching; the TiDB executor ultimately maps structured errors to SQL errors/warnings.

This design addresses two problems simultaneously: first, it preserves whether the error occurred during prepare, bind, execute, or cleanup; second, it prevents an adapter failure from being mistaken for a SQL value-domain error and silently routed through another algorithm.

## 4. Relationship Between Expression and Other Modules

| Module | Relationship to expression | Boundary after this change |
|---|---|---|
| `tidb-parser` | Produces AST; SQL digest exceptions depend on the lexer/normalizer | Parser syntax and normalization remain in TiDB; ordinary value computation is not in the parser |
| `tidb-datatype` | `Datum`, complete `FieldType`, and collation/Decimal/time/json facades | SQL metadata stays in TiDB; underlying representations and algorithms reuse `tidb_query_datatype` wherever possible |
| `tidb-expr` | AST/value/typed/PB dispatcher, coercion, metadata, adapter | No longer the second algorithm owner for migrated families |
| `tidb-session` | Statement lifecycle, parameters, sysvars, clock, identity | `SessionReadyValueRuntime` can explicitly install an experimental pool/execution; dispatch/record-set closes it; the default policy is `None` |
| `tidb-executor` | Selection/projection/default/DML, statement context, error projection | Passes the actual execution/context; retains scheduling rather than duplicating kernels |
| `tidb-unistore` | PB/DAG, request flags/TZ/division precision, warning sink | Creates a local owner for each request; migrated signatures reuse the same TiKV program |
| `tidb-util` | AES facade, plan codec, password policy, etc. | Migrated utilities are reduced to facades; plan/password remain explicit exceptions |
| `tidb_query_datatype` | Shared values, collation, Decimal, temporal/vector primitives | Primary owner of value algorithms and underlying representations |
| `tidb_query_expr` | Official RPN, function metadata, local compiler/runtime, kernels | Sole owner of expression computation |
| `tidb_query_crypto` | Crypto primitives such as AES | TiDB retains only SQL mode/IV/warning glue |
| `tidb_query_aggr` / `tidb_query_executors` | Aggregation, selection, projection, TopN, etc. in TiKV production DAGs | Use the strict builder; deep control does not degrade to eager evaluation |
| `tipb` / protobuf | Wire signatures and complete wire `FieldType` | PB origin is preserved, without rewriting through SQL names or fabricating local IDs |

### 4.1 Lifecycle Relationships

`tidb-session/src/ready_value_runtime.rs::SessionReadyValueRuntime` owns a session pool root only after an experimental policy is explicitly installed; the current default policy is `None`, and no production caller automatically enables it. Once enabled, each outer statement call starts an independent execution, while nested calls do not create a second execution; a captured closer moves with the record set and closes only the actually captured execution on normal, exceptional, or Drop paths. Session Drop closes all surviving attached/detached executions. AST/value calls without the capability create a one-shot owner/execution for each call.

Unistore does not borrow session tokens across processes. `tidb-unistore/src/cophandler/eval_context.rs::RequestEvalContext` creates a request owner from the actual DAG flags, timezone, division precision, column types, and warning sink, and closes it on Drop. Independent, ownerless helper APIs preserve their previous contract and create a one-shot owner per call; the current session also does not install a pool policy by default. The actual call frequencies and cache hierarchies of different production callers were not modeled in this microbenchmark.

### 4.2 Metadata and PB Relationships

TiDB lowering is value-free: a dynamic column produces only a binding description and does not read a value. Sidecar metadata preserves the complete SQL `FieldType`, collation snapshot, source identity, and PB origin, but does not retain executable children, a native evaluator, or a cache.

The PB path selects an implementation by the real `ScalarFuncSig`; the display name is used only for diagnostics. A mismatch in type or origin after rewriting is rejected rather than reconstructing a schema from `Datum`. The migration also did not add previously nonexistent PB/Unistore signatures merely to increase coverage.

## 5. Incremental Replacement Strategy

### 5.1 M0–M6

The following are design workstreams, dependencies, and convergence order, not a strictly sequential timeline of actual commits. To shorten the Demo cycle, non-conflicting datatype, runtime, lowering, family, and audit checkpoints proceeded in parallel or interleaved; each item was closed only after reaching its own evidence threshold.

1. **M0: Freeze the baseline and denominator**
   Consolidate aliases, operators, synthetic CASTs, and AST/PB/Unistore/helper entry points; freeze the denominator at 245 families; and record each item's type domain, context, source implementation, tests, and final owner.
2. **M1: Collation / LIKE**
   First migrate the lowest-level compare/key/pattern semantics, distinguishing PAD SPACE, signed wire IDs, byte/rune/collator policies, and GB/UCA special cases; then delete TiDB's duplicate weight tables and matcher implementation.
3. **M2: Types and values**
   Integrate NULL/int/real/bytes first, followed by Decimal, temporal, JSON, and vector, prohibiting string/f64 round-trips. Decimal coverage simultaneously includes arithmetic, comparison, hash/group keys, assignment, and codec consumers.
4. **M3: Local runtime and strict semantics**
   Build a local facade on the official RPN; implement lazy IF/CASE/COALESCE/NULLIF, deep AND/OR, runtime-bound regexp, IN prepare/rebind, resource limits, and structured diagnostics.
5. **M4: Connect all entry points**
   Make AST/value, typed row, PB, vector/selection, fold/default/DML, executor, and Unistore/TopN/aggregation all point to the same TiKV implementation; each entry point retains its own metadata/effects.
6. **M5: Migrate and delete synchronously by family**
   For each item, complete “old implementation → TiKV kernel → TiDB adapter → all-entry-point testing → native-body deletion”; genuinely difficult items enter an explicit deferred list instead of becoming hidden fallbacks.
7. **M6: Integration, cost, and independent review**
   Freeze coverage; check values/metadata/warnings/errors/cache/parallel/index bytes/execution origin; run TiDB lint, TiKV workspace check/clippy, and core tests; record compilation and width-one/batch costs; and have an independent reviewer inspect the work.

### 5.2 Replacement Template for a Single Family

Each family actually advances through the following closed loop:

1. Enumerate all entry points and overloads, and freeze Go/source vectors and current behavior;
2. Determine whether TiKV already has a reusable kernel; if not, implement it exactly once, only in TiKV;
3. Add a `FunctionRef`/fixed recipe and typed carrier for the actual input domain without broadening wire admission;
4. Preserve the original child demand, coercion, context, metadata, and warning/error projection in the TiDB adapter;
5. Connect actual entry points that exist in AST/value, typed scalar, PB, vector, Unistore, and helpers;
6. Add targeted tests for NULL, boundary values, selection, laziness/errors/warnings/lifecycle;
7. Delete the TiDB native algorithm and bypass paths;
8. Add family credit only after source code and call-graph inspection confirm a single owner.

This order avoids false migrations that merely add a wrapper while leaving the old algorithm reachable, and avoids miscounting shared formatters, leaf helpers, or test-only code as completion of an entire family.

## 6. Problems Encountered and Their Resolution

### 6.1 SQL Compatibility Is Not Pure-Function Equivalence

Identical return values can still be incompatible in warning count, error timing, unvisited branches, side-effect count, or metadata. This is especially apparent for IF/CASE, REGEXP, IN, temporal, JSON, and packet-limited strings.

The solution is to make demand and effects part of the interface: a selector returns the arguments needed next, `Columns` provides the actual statement context, and warnings continue to be written to the original sink; a dead branch does not cast, compile a regexp, or access the host.

### 6.2 TiDB and TiKV Have Different Type Models

A complete SQL `FieldType` cannot be compressed into a kernel enum; a Decimal's visible scale, internal precision, and declared shape cannot round-trip through a string; and TIMESTAMP and DATETIME cannot be reinterpreted solely by casting their struct layouts.

The solution is to establish checked projections and exact carriers: schema metadata remains in TiDB, while numeric representations and algorithms move into TiKV. Domains that cannot be represented losslessly are explicitly rejected rather than silently truncated.

### 6.3 Ordering Differences Between Row-Major and Node-Major Evaluation

TiDB's original path produces warnings/errors/volatile effects by row and source order, while RPN tends to evaluate by node/batch. Simply increasing the batch size changes the first error or warning order.

The solution is to enable numeric batches only for graphs proven safe; all other paths invoke the same compiled RPN width-one. Selection occurrences and physical rows are recorded separately, so duplicate and out-of-order selections are not reordered.

### 6.4 Lazy Control and Degradation at Depth

An early production PB builder could fall back to eager evaluation beyond depth 32; constant REGEXP expressions were also once compiled prematurely in dead branches, and IN metadata could be prepared repeatedly.

The solution was to switch production construction sites to the strict builder and verify depths 33/256; CASE runs through bounded iterative demand; REGEXP is deferred until its first actual invocation; and IN is prepared once and invalidated on rebind.

### 6.5 Lifecycle, Caching, and Concurrency

Creating workers/caches per row incurs significant cost; incorrectly reusing them across statements contaminates context. The concurrent pool also previously exhibited cached poisoning and torn observations of epoch/debt.

The solution was a session/request owner + independent statement execution + lexical scope; nested calls reuse the current execution, while another live/detached statement is not invalidated by new admission or peer closure. Clone/reset/failure retry behavior has independent tests. Pool state uses consistent synchronization and sticky poisoning, and a worker with cleanup failure is not returned to the pool.

### 6.6 Error Provenance and Fallback Risk

If an adapter returns only a string error, callers may mistake an infrastructure failure for a SQL domain error, or rerun the native implementation for “compatibility.”

The solution is to preserve the failure class, phase, operation/profile, and original cause; no-fallback is a hard boundary. Projection to a corresponding SQL error is permitted only when supported by the actual operation/profile and the witness from the current input.

### 6.7 PB, Macro ABI, and Cross-Crate Visibility

After integrating the strict builder into the executor, expansion of the `rpn_fn` macro once produced 18 compilation errors because `CallShape`, `CallArg`, `CallBuild`, and other items were private.

The fix exposed only the narrowest `#[doc(hidden)] pub` ABI required for macro use across crates; metadata mutation and the internal registry remained private. The full executor library's 120 tests and the aggregate library's 40 tests subsequently passed.

### 6.8 Native Toolchain and Workspace Clippy

The complete TiKV clippy run was initially blocked because CMake 4 removed old policy compatibility and because GCC 16 conflicted with old RocksDB/Abseil include assumptions. The compatible environment used GCC 14, `-include cstdint`, and a wrapper that appended `CMAKE_POLICY_VERSION_MINIMUM=3.5` only during configure, without modifying vendored dependencies.

Clippy further found that flate's `find_match` did not decrement `tries`, which was an actual transcription bug. The cyclic-chain regression timed out with the decrement removed and passed after restoring `tries -= 1`; this was not merely a lint suppression.

### 6.9 Pre-Existing Red Tests and Baseline Noise

The complete `tidb-expr` suite still has 4 historical failures, and the complete Unistore suite has 1 historical failure; they remain reproducible after freezing/reverting local changes, so this migration did not quietly patch or hide them. These failures remain disclosed in compatibility reporting, and targeted green results are not upgraded to “the entire test suite is green.”

### 6.10 Five Difficult-to-Migrate Families

- `JSON_SCHEMA_VALID`: validator, statement cache, file/HTTP `$ref`, and no-I/O-on-NULL;
- `TIDB_DECODE_PLAN`: complete plan codec/tree/text renderer and malformed-raw fallback;
- `TIDB_DECODE_BINARY_PLAN`: Explain protobuf, recursive tree, warning/panic policy;
- `TIDB_ENCODE_SQL_DIGEST`: the real algorithm is parser lexer normalization, not just SHA-256;
- `VALIDATE_PASSWORD_STRENGTH`: identity, seven ordered live GLOBAL reads, and byte/Go-rune/Unicode policy.

These algorithms are currently retained, but a closed `host_compat` has replaced the generic native evaluator. Future migration requires first designing resource retrieval, codec errors, parser bytes, and identity/sysvar reads as a typed staged protocol, and then migrating the real kernel; passing precomputed answers or adding only an adapter wrapper cannot receive credit.

## 7. Compatibility Evaluation

### 7.1 Executed Representative / Targeted Validation Dimensions

The following dimensions are covered by different focused suites; this does not mean that every one of the 240 families has completed matrix validation across every entry point and dimension. The strict per-family final audit count remains 0.

- Frozen denominator of 245 families, with 240 functionally TiKV-only families;
- Go/source-derived vectors and existing immutable fixtures;
- Implemented domains at real entry points including AST/value, typed row, PB, vector/selection, and Unistore;
- NULL, signed/unsigned, Decimal scale/FSP, collation, timezone, and SQL mode;
- Warning/error counts and timing, lazy child demand, cache/rebind/clone/reset;
- Selection widths 0/1/1024/1025, with duplicate/out-of-order selections;
- Strict depths 33/256, and CASE with 1024 pairs;
- Session/request pool lifecycle and exceptional/Drop closure;
- TiDB `make lint`, TiKV changed-crate locked checks, and a full `make clippy` in the compatible environment;
- 120 executor tests, 40 aggregate tests, and multiple family-focused suites.

### 7.2 Current Conclusion

Among the accelerated Demo's targeted receipts, no new confirmed discrepancy was found other than the MD5 release issue below: functional coverage reached 97.96%, no implicit native fallback was retained for failure paths, and the five exceptions are explicitly listed without receiving credit. Metadata, effects, and lifecycle are treated as first-class contracts rather than comparing result values alone. This conclusion summarizes focused evidence; it is not a quantified whole-domain compatibility rate.

However, this conclusion does not mean “whole-domain equivalence”: the strict per-family final audit count remains 0; the current focused tests cannot cover every cross-product of SQL types/collations/contexts and cannot prove that arbitrary PB trees are admissible.

This report's release performance probe also discovered a new compatibility issue: the identically named `test_md5_hash`, using the same MD5 row table, passes in the frozen release test binary, while the current release test binary returns `ExpressionRuntimeFailure { class: InvalidSpecification }` during TiKV prepare. The two binaries and test helper source are not byte-identical; the receipt demonstrates a divergence in results for the same set of MD5 rows at the two revisions. This issue did not surface in previous debug-focused gates. Consequently, there is no current MD5 performance value, and “unverified in a release environment” has moved from a theoretical risk to an issue with an actual receipt; the **MD5 family/path** cannot be described as release-compatible until it is fixed and a release regression is added. This receipt does not independently make a determination about other crypto families.

### 7.3 Known Unverified Areas and Risks

- Exhaustive differential testing and complete release/TiFlash/FIPS environments;
- Allocator physical peak, OOM thresholds, and fault recovery;
- Strict final audits for every family;
- Complete Go package transcreation;
- TiKV ownership of the five deferred families;
- Production server/sysbench-level end-to-end throughput and tail latency.

The current state therefore remains `pr_ready=false`. The 4 historical failures in the complete `tidb-expr` suite and the 1 historical Unistore failure also remain disclosed.

### 7.4 Real TiDB/TiKV SQL Smoke (round230)

Round230 additionally started three independent real processes: PD, a `tikv-server` built from the paired TiKV revision, and a Go `tidb-server` built from the current TiDB revision. A client used the MySQL protocol to create a 4,096-row table and execute a small representative SQL set. Integer/NULL/string/Decimal/JSON, control-flow, regexp, MD5, and prepared-parameter results were byte-identical to an independently generated expected TSV; warning 1292 was observed separately and was not part of that TSV diff. `EXPLAIN FORMAT='brief'` confirmed that integer PLUS/filter/aggregation and the mixed IF, Decimal PLUS, and REGEXP_LIKE expressions entered `cop[tikv]`; the real storage/RPC/coprocessor path was therefore exercised. All processes and data were cleaned up afterward, and all six ports were confirmed free.

This evidence has one boundary that must be explicit: the current Go `tidb-server` does not link `rust/crates/tidb-expr`, so it did not execute the TiDB Rust `EvaluatorSuite`/lane cache discussed in this report. It directly validates only the real SQL path from the Go TiDB host to the current TiKV coprocessor. TiKV was also an unoptimized dev build, so the single-client warm sequential timings are smoke measurements only: three runs of 2,000 integer statements took 1.58/1.55/1.50 seconds (median 0.775 ms/statement), while three runs of 1,000 mixed statements took 0.88/0.88/0.84 seconds (0.880 ms/statement). This is not a release, concurrent, or production-QPS result, and there is no frozen comparator. Complete commands, oracle, plans, timings, and teardown evidence are in `logs/real-cluster-sql-smoke-round230.txt`.

## 8. Performance Measurements

### 8.1 Method

This work added a same-machine, same-toolchain frozen-before/current-after microbenchmark. The measurement boundary is the stable TiDB `eval_in` AST/value API: SQL is parsed only once, and parsing is excluded from the loop; each workload first warms up for 2,000 iterations and then runs 9 samples. The baseline uses the old native implementation; the primary current result uses the public AST/value default ownerless context: because the current Session default policy is `None`, each call creates a one-shot owner/execution. A separate pooled warm-path set explicitly injecting a reusable `ReadyValueExecution` was also measured as the best case for a candidate lifecycle design; it is not the current production/default path.

The initial one-shot probe had byte-identical source in both trees (SHA-256 `fdd542d9…c187e`); the final probe capable of continuing execution added only harness control to “continue after recording the MD5 error” and did not change the expressions, loop counts, black boxes, or output checks of any other workload. The pooled variant additionally provides a `ReadyValuePoolOwner/ReadyValueExecution` context; the baseline does not have this capability. Each revision was compiled into an independent target, and the final test binaries ran interleaved on CPU 2. The temporary probe and detached baseline worktree were deleted after measurement and were not committed.

Environment: AMD Ryzen 9 9900X (12 cores / 24 threads), 46 GiB RAM, Linux 7.1.8; TiDB nightly-2026-08-22, release profile, locked dependencies. The final comparison run was pinned to CPU 2, but CPU boost/scaling remained enabled. Results are in ns per expression evaluation; they are not database QPS and exclude parser/planner/storage/network.

The correctness prerequisite is identical before/after `Datum::label()` output. Workloads cover integer addition, lazy IF, string comparison, Decimal addition, MD5, JSON_TYPE, and REGEXP_LIKE.

Three independent processes ran in the interleaved order before/after/after/before/before/after; each process had 9 consecutive samples per item, producing 27 observations for each successful item, but only 3 truly independent process-level replicates because samples within a process are correlated. The table below reports the median and full range across all samples; the full range is not a confidence interval, and neither confidence intervals nor statistical significance were calculated.

**Current default one-shot path (primary result)**

| workload | frozen before (ns/eval) | current default (ns/eval) | current / before | result |
|---|---:|---:|---:|---|
| integer add | 34.072 (33.331–34.969) | 2,259.850 (2,252.348–2,360.737) | **66.33× / +6532.6%** | `INT:3` identical |
| lazy IF | 94.867 (94.532–95.944) | 4,334.148 (4,303.269–4,383.556) | **45.69× / +4468.7%** | `INT:2` identical |
| STRCMP | 165.926 (164.619–167.422) | 2,872.959 (2,850.228–2,908.208) | **17.32× / +1631.5%** | `INT:-1` identical |
| Decimal add | 549.241 (546.452–563.021) | 4,006.884 (3,992.414–4,053.350) | **7.30× / +629.5%** | `DEC:124.00` identical |
| MD5 | 281.331 (279.155–283.846) | **No value: Prepare/InvalidSpecification** | Not comparable | frozen `STR:b1b5...ddd9`; current release fails |
| JSON_TYPE | 237.289 (234.354–238.858) | 2,172.428 (2,157.067–2,803.819) | **9.16× / +815.5%** | `STR:OBJECT` identical |
| REGEXP_LIKE | 8,994.609 (8,959.193–9,049.748) | 12,099.094 (11,915.070–12,175.097) | **1.35× / +34.5%** | `INT:1` identical |

**Explicit pooled execution (candidate best case, not the current default)**

This set was run separately using three-process ABBA ordering. It is intentionally asymmetric with the native baseline and is used only to estimate a candidate lower bound after explicitly reusing an execution: integer add 1,284.414 ns (37.50×), lazy IF 3,360.578 ns (35.53×), STRCMP 1,670.081 ns (10.13×), Decimal add 2,818.449 ns (5.17×), JSON_TYPE 1,393.691 ns (5.92×), REGEXP_LIKE 10,695.672 ns (1.22×); MD5 likewise fails during Prepare. The difference cannot be attributed to any single adapter, kernel, or pool component.

The results clearly show that the current implementation is not a performance-neutral replacement on the measured public AST/value default warm path. The simpler the expression, the greater the proportion of fixed adapter/runtime cost; an intrinsically heavier algorithm such as REGEXP regresses by approximately 35% on the default path, while the relative regression for integer and lazy control reaches 46–66×. An explicit pool can significantly reduce fixed costs but still does not approach the old native path. The current MD5 release issue is a correctness failure that cannot be obscured by performance numbers.

### 8.2 Compilation and Memory Costs

The same release `tidb-expr` libtest was built in two independent empty targets, both using 4 Cargo jobs:

| Metric | frozen before | current | change |
|---|---:|---:|---:|
| Cargo `Finished release` | 2m30s | 5m30s | approximately 2.20× |
| `/usr/bin/time` wall | 151.62s | 341.13s | approximately 2.25× |
| Maximum RSS | 2,679,980 KiB | 3,252,576 KiB | +21.4% |

The current build has a substantially larger Rust/C++ transitive closure, consistent with introducing the direct TiKV dependency closure; however, the two revisions also contain other source, lockfile, and dependency-graph changes, so this before/after comparison cannot attribute the increase solely to direct TiKV dependencies. Cargo `Finished` is the cleaner compile-only marker; `/usr/bin/time` wall/RSS also includes execution of the compiled tests (approximately 1.35s for the baseline; current returned 101 after approximately 10.60s due to MD5). These data are local cold-target records, not CI guarantees, and must not be interpreted as evaluator runtime. A separate, narrower M6 dev check (four TiKV production crates) recorded 10.19s wall / 1,472,560 KiB RSS; the two scopes differ and are not directly interchangeable.

An existing M6 current-only diagnostic reuses one compiled Plus program and `EvalContext`, and on each call copies the same `ExecutionLimits` and creates a new budget/row scratch; 10,000 width-one executions take approximately 1,915 ns/eval. Retained output is 16 B, input payload copy is 0, and logically each call has two owned vectors—an output collector and a temporary result—and one append. This number has no frozen-before comparison and is not an allocator-call/peak-memory measurement, so it is not used to claim an improvement.

### 8.3 Prepared Worker Construction Cost and Statement Lifecycle Selection

On the current TiKV revision in release profile, pinned to CPU2, with 500 warmup iterations, 20,000 `prepare_evaluated_bytes + operation + retained_storage + drop` iterations per sample, and 11 samples, the measured steady-state median construction/destruction times were: ASCII 543.153 ns, integer add 687.988 ns, STRCMP 844.112 ns, Decimal add 851.984 ns, JSON_TYPE 530.752 ns, and REGEXP_LIKE 851.114 ns. The 11-sample min/max ranges for the respective families were 539.806–544.024, 686.950–695.065, 842.857–854.526, 850.536–881.954, 525.982–556.141, and 842.361–853.710 ns. Raw records are in `logs/worker-prepare-probe-build-run.log`.

These figures include worker drop but exclude the TiDB pool owner/slot/Arc/Mutex and public Datum glue; a warm allocator/thread cache also means this is not a cold-process or physical allocation peak measurement. Nevertheless, the 0.53–0.85 µs construction cost is small relative to the default public one-shot path's 2.17–12.10 µs, supporting a simpler statement-owned strategy: a worker is reused only within the same statement execution; statement close retires only its own idle workers; different live/detached statements no longer invalidate one another through a rotating global epoch. This choice explicitly gives up cross-statement worker reuse, thereby avoiding incorrect sharing while the current metadata still contains mutable per-invocation bindings.

### 8.4 Performance Interpretation

Explicit execution reuse is important but insufficient to eliminate fixed costs. Integer add, lazy IF, STRCMP, Decimal add, JSON_TYPE, and REGEXP_LIKE decline from approximately 2,260, 4,334, 2,873, 4,007, 2,172, and 12,099 ns/eval on the default one-shot path to approximately 1,284, 3,361, 1,670, 2,818, 1,394, and 10,696 ns/eval on the pooled path. This shows that the experimental pool/execution policy can avoid some of the overhead of creating a new owner for every call; the current Session default has not yet installed that policy. Even when explicitly enabled, operation/shape checking, typed-carrier construction, worker leases/guards, width-one `VectorValue` materialization, and projection back into TiDB `Datum` still produce substantial fixed per-call costs.

The round218 pooled figures above represent only the AST/value, width-one, warm-execution path. They exclude the SQL parser/planner/storage/network, do not measure typed batches or a real server, and cannot be converted directly into TiDB QPS. Instead, they serve as a clear optimization signal: the current architecture establishes a single owner and compatibility boundaries, but hot-path glue still does not meet production performance requirements. More complex expressions can amortize fixed costs, whereas simple arithmetic/control is where layers and temporary vectors most need to be reduced.

round228 additionally measured real column-backed `Chunk` execution using 5 independent processes: 15 common workloads; dense batch sizes 1/8/64/256/1024; and NULL/sparse/reverse/duplicate selections for four representative workloads, for a total of 155 cells. The three PLUS workloads hard-asserted to enter the TiKV numeric route showed 6.59×–7.82× per-row ratios between batch 1 and batch 1024; the 12 workloads explicitly labeled with the production fallback route showed 1.02×–1.18×. Each size used a deterministic prefix rather than a matched input distribution, so this does not establish a causal effect isolated to batch size. At batch size 1024, the numeric route remained 12.31×–13.29× the frozen native implementation, while workloads on the production fallback route were 4.89×–42.90× the frozen native implementation. This fills the evidence gap for evaluator batch scaling, but still excludes server, storage/RPC, and concurrent QPS; see `LANE_CACHE_PERFORMANCE_REPORT.md` for the complete method, paired bootstrap, and environment QC.

The release-only MD5 failure takes priority over performance optimization. The identically named current debug-profile test still passes, but the current release path returns `InvalidSpecification` during Prepare; the root cause has not been identified. A release regression should be established and root-cause analysis completed before attributing the issue to admission, metadata, or any other specific component.

Performance shortfalls must not be addressed by reintroducing a native TiDB fast path. Subsequent optimizations should continue to target the same TiKV kernel, for example by reducing width-one materialization, reusing prepared workers by operation, expanding the proven-safe batch domain, and providing an explicitly reusable execution for ownerless helpers, rather than restoring a second algorithm implementation.

## 9. Conclusion and Future Work

Targeted evidence from this accelerated Demo indicates that, within the covered domain, TiDB can preserve the measured SQL host semantics while unifying the overwhelming majority of expression value algorithms in TiKV; this does not constitute whole-domain equivalence or a complete semantic proof for every family. The key is not adding a cross-repository function call, but making types, demand, metadata, effects, lifecycle, error provenance, and resource boundaries fully explicit.

The next stage should prioritize:

1. Establishing stable release benchmarks and regression budgets for high-frequency families, separately measuring cold/first/warm and width-one/batch paths;
2. Reducing fixed scheduling and materialization costs in the local adapter;
3. Running real session/Unistore/server workloads and adding p95/p99 and allocator data;
4. Migrating the five deferred families one by one according to effect-protocol difficulty;
5. Beginning strict per-family final audits instead of upgrading the 240/245 functional count into a stronger conclusion.

## 10. Evidence Index

- Overall plan: `EXPRESSION_UNIFICATION_PLAN.md`
- Current status: `docs/expression-unification/README.md`
- Frozen denominator: `docs/expression-unification/evidence/coverage-baseline-notes.md`
- Completion checkpoint: `docs/expression-unification/evidence/m0-m6-accelerated-complete-checkpoint.md`
- Five exceptions: `docs/expression-unification/evidence/final-five-exceptions.md`
- Host adapter narrowing: `docs/expression-unification/evidence/five-host-adapters-checkpoint.md`
- Local runtime contract: `docs/expression-unification/evidence/runtime-contract.md`
- Lowering contract: `docs/expression-unification/evidence/lowering-contract.md`
- M6 costs: `docs/expression-unification/evidence/m6-cost-record-checkpoint.md`
- Before/after performance receipt: `docs/expression-unification/logs/performance-before-after-summary.txt`
- Independent report review: `docs/expression-unification/logs/architecture-performance-report-review.txt`
- Full clippy: `docs/expression-unification/evidence/m6-full-clippy-checkpoint.md`
