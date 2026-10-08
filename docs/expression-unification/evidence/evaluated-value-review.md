# EV-r1 independent review: execution-owned ready-value runtime leasing

**Revision: EV-review-r1. Status: independent source review, with separately attributed parent compilation receipts. Not an implementation, another ExecPlan, or a product-write grant.**

## 1. Authority, scope, and conclusion

Foundation A independently reviewed the execution-owned idle-pool proposal in [evaluated-value-contract.md](evaluated-value-contract.md), revision EV-r1. The original review was strictly read-only and was delivered to parent `session-3ddde407-3291-4e66-92b9-005d1e704d5a`. The parent subsequently granted **this one new evidence file only** to persist the complete review and future proof obligations. No other evidence, product, build, manifest, export, or main-plan write permission follows from that grant.

The sole execution plan remains `/home/agent/tidb/EXPRESSION_UNIFICATION_PLAN.md`. Its active C3c ownership and the separate C4/ASCII design boundary remain authoritative; this document neither releases those files nor changes the ledger. At review time the seven C3c product files were actively owned by C, D6 was a source proposal, and E owned EV-r1. The parent now has a refreshed production-library trait receipt, distinguished below from still-pending C3c full-test validation.

**Conclusion:** moving an owned `Send` runtime out of an execution-owned `Mutex` idle pool is feasible without making the runtime `Sync`. No additional Send blocker was found. This is not acceptance of an implemented pool: explicit lifecycle wiring, caught-panic disposal, complete context forwarding, caller scope propagation, and an owner-level allocation/slot ledger remain necessary. A general one-shot entry wrapper alone cannot prove absence of per-row compilation. Current TiKV admission still requires the separate closed ASCII factory/entry described by EV-r1.

This review contributes **zero complete families**. The frozen denominator remains 245; ASCII may contribute at most one only after all applicable implementation, entry-route, runtime, deletion, performance, and parent-acceptance gates. No SQL activation, whole-package completion, full-hot-entry coverage, allocation bound, or performance result is claimed here.

## 2. Source basis and navigation

Pinned worktree bases:

- TiDB: `364aef2bab5cc633ecb76a775ae8f36f86a6687d`.
- TiKV: `548812e1ef57aef077a2062a9cc356640a6347f5`.

Observations concern the working sources inspected during this review, including the concurrent expression-unification changes, not only those base commits. Line anchors are precise navigation points in that observed source; they are not a frozen patch or proof that a later edited tree has been compiled.

All abbreviated paths below are relative to `/home/agent/tidb/`:

| Prefix | Directory |
| --- | --- |
| D | `expression-unification/tidb/rust/crates/tidb-expr/src/` |
| X | `expression-unification/tidb/rust/crates/tidb-executor/src/` |
| S | `expression-unification/tidb/rust/crates/tidb-session/src/` |
| P | `expression-unification/tidb/rust/crates/tidb-planner/src/` |
| K | `expression-unification/tikv/components/tidb_query_expr/src/` |
| Q | `expression-unification/tikv/components/tidb_query_datatype/src/` |

The source review read the main plan's active ledger, EV-r1, relevant native callers and TiKV runtime/context definitions. Required TiKV maintenance context was also read: `doc/maintenance-guides/README.md`, `repo-overview.md`, and `src/coprocessor.md`. No guide was modified.

## 3. Evidence separation: source findings versus actual parent trait compilations

### 3.1 Parent's original H/C3b artifact receipt

The parent reported an actual successful pinned-rustc compilation using `--crate-type lib --emit metadata` against the existing H/C3b libraries, exit 0. The reviewer did **not** run or duplicate that compilation.

Inspected receipt inputs:

- The probe source recorded by this receipt (SHA-256 below) defined `CandidateParts { LocalProgram, LocalEvalState, EvalContext }`, wrapped `Vec<CandidateParts>` in `Mutex` as `IdleOwner`, and instantiated the trait requirements. The current file at `expression-unification/tools/local-runtime-send-probe.rs` was updated in round224 to use `ExecutionLimits`; it is a different source artifact and was not independently rerun.
- `expression-unification/logs/local-runtime-send-before.log` is empty. It contains no diagnostic text, but is not by itself a command/exit-status transcript; the successful exit is attributed to the parent's execution report.
- `expression-unification/logs/local-runtime-send-before-artifacts.sha256:1–3` records the exact source and linked-library identities below. The reviewer read this manifest, not independently rehashed or rebuilt the artifacts.

| Original receipt item | SHA-256 recorded by parent |
| --- | --- |
| Probe source | `c4626e845c5ec203c3447879524b68b9fc846cb0774083bf788ce0d5bbcd4443` |
| `target-tikv/debug/deps/libtidb_query_datatype-c654d7c6d7b1484d.rlib` | `e46f4fb16b058fd6d62bf58aa0d571be5c7d340cdbe19c610a264364dda032c5` |
| `target-tikv/debug/deps/libtidb_query_expr-61c532d900245edb.rlib` | `d3d1df66d38b74129fa2a4e6fc46aee5b8eaed633a1c9d5d8acae325a8cdaea4` |

The two library paths in this table are under `expression-unification/`. These are historical artifact identities; a rebuild at the same path does not preserve their identity.

### 3.2 Parent's refreshed TEST-profile C3c production + B2.2-I receipt

This refresh was pending when the doc-only grant arrived. During preparation of this document, the parent supplied the following **actual successful** result:

- Same private Send-probe source, compiled with `--emit metadata` against refreshed, matched **TEST-profile C3c production + B2.2-I libraries**, after the parent's aggr40 gate.
- Parent execution job **96**, exit **0**.
- `tidb_query_datatype` library SHA-256: `186ef1671a1b311e3c658e95db4dba036fbb654538aa0c32176592bf04b276ec`.
- `tidb_query_expr` library SHA-256: `93dcca9455bab38aa1108bf57edc6dc80265f0a079d00239d7dc82719dfd0697`.

This is a parent-provided compilation receipt, persisted here verbatim in its substantive facts. No independent compilation, binary rehash, or full command reconstruction was performed by this reviewer. No separate refreshed log/manifest path was supplied with that report.

**Do not credit the earlier DEV-build attempt.** The parent explicitly reported that a prior attempt used a stale library SHA and stopped **before trait compilation**. Neither that attempt nor mere DEV-build success establishes the refreshed trait result. Job 96 is the separate accepted trait compilation.

### 3.3 Exactly what those receipts establish

For each receipt's exact linked artifacts, the instantiated requirements at probe lines 20–25 establish:

1. `LocalProgram: Send`.
2. `LocalEvalState: Send`.
3. `EvalContext: Send`.
4. The historical `CandidateParts` aggregate is `Send`.
5. `IdleOwner(Mutex<Vec<CandidateParts>>): Send + Sync`.

The round224 current source instead asks for `ExecutionLimits: Send + Sync`; the green crate-level `tidb_query_expr --tests` check compiles that source's underlying types, but the standalone artifact-link probe itself was not rerun and is not attributed to these historical SHA-256 receipts.

They do **not** establish the traits of an actual future C4 prepared type, lease, execution owner with additional fields, or borrowed `ScopedColumns`. The probe does not itself assert a negative `Sync` bound. They do not prove lifecycle, reset, panic, resource, semantic, allocation, or performance behavior. The refreshed receipt is also **not C3c full-test success**: the parent separately reported test-only fixture compilation errors pending resolution.

The independent findings below remain source review. Existing source assertions are identified as such, rather than reported as tests run by this reviewer.

## 4. Independent findings and minimum adjustments

### F1. Owned leasing fits the current traits; shared executable placement does not

**Source evidence:**

- K`local/compile.rs:26–35`: `LocalProgram` owns the RPN expression, full schema, optional host catalog key, return type and entry tag; its comment explicitly rejects an implicit Sync promise from Any+Send metadata.
- K`local/tests.rs:360–366`: the source instantiates `LocalProgram: Send`, `LocalExpr: Send + Sync`, and `assert_not_impl_any!(LocalProgram: Sync)`.
- K`types/expr.rs:717–720`: a separate source assertion requires `RpnExpression: Send`.
- K`local/runtime.rs`: `ExecutionLimits` contains four scalar limits and is `Copy`; K`local/batch.rs` creates row scratch and budgets inside each invocation, so no input/program borrow or mutable service survives evaluation.
- K`local/runtime.rs:49–56`: those limits contain numeric fields, not borrowed input/context handles.
- Q`expr/ctx.rs:225–241`: `EvalContext` owns `Arc<EvalConfig>` and `EvalWarnings`. Q`expr/ctx.rs:69–81` lists the configuration fields; Q`codec/mysql/time/tz.rs:9–21` defines the owned timezone variants. Source structure alone was not treated as a substitute for the parent's actual aggregate trait compilation.

**Required adjustment/proof:** share only synchronized idle ownership/bookkeeping, move a runtime out, and exclusively own it while active. Do not use `Arc<LocalProgram>`, put a bare runtime/RefCell in shared statement data, strengthen `Any + Send` to `Any + Send + Sync`, or introduce unsafe trait implementations.

D`builtin_ext/cache.rs:22–34,64–83` stores `Arc<T>` inside `RwLock`, not an owned-take idle pool. It is not a drop-in cache for `LocalProgram`; its `Clone` also starts empty (`44–48`). Wrapping a shared executable in a lock held throughout evaluation is not the proposed short-bookkeeping-lock model.

A borrowed `ScopedColumns` is a different trait/lifetime question from the owned runtime. `&dyn Columns` does not supply a Sync guarantee, and a local interior-mutability scope need not be Sync. Do not capture that borrowed scope into a shared worker object or Send+Sync callback. Capture the synchronized owner and construct the local scope inside execution. X`projection.rs:170–183` requires `Columns + Clone + Send + Sync + 'static`; its job closure at `295–304` is a suitable local-scope seam. D`evaluator.rs:392–397` itself requires only `Columns`.

### F2. Scope creation can be inert, but checkout must follow the actual coercion seam

The native AST schedule is explicit:

1. Child evaluation: D`func.rs:262–271`.
2. Existing BinAware charset conversion: D`func.rs:272–286`.
3. Existing argument wrappers: D`func.rs:291–293`.
4. Values dispatch: D`func.rs:297–299`.
5. Current ASCII helper's own byte-preserving coercion: D`string_fn.rs:138–145`.

An outer loop may create a scope containing no lease. Pool reservation/exhaustion, new-runtime allocation, preparation and pure-context initialization must wait until a demanded ASCII occurrence has passed its **actual original** checks/coercion. Do not perform them merely because a tree contains ASCII, or when binding a scope for a query with no ASCII. Preserve the original arity/child/error order rather than moving a later check earlier for convenience.

An invoked ready NULL still crosses the checked nullable value boundary; retaining D`string_fn.rs:142–143` as a native NULL fast path would not migrate that invocation. The official generated nullable RPN wrapper owns its NULL handling. A test must distinguish entry into that wrapper/driver from entry into the non-null kernel body: K`impl_string.rs:211–219` defines `ascii(BytesRef)`, not an Option-taking SQL-NULL body. A truly skipped/dead call must invoke neither.

Children, transcode and coercion run before the short mutable runtime borrow. Release that borrow before existing native return coercion at D`scalar_function.rs:1192–1212`. This allows nested ASCII to use sequential borrows, not recursively hold a runtime borrow or pool mutex while evaluating another expression. Failed reentry must be a structured contract failure, never `borrow_mut` panic or native replay.

### F3. Reused pure context needs a health invariant, not per-row warning draining

K`impl_string.rs:211–219` confirms that this exact kernel takes Bytes and has no context argument. That supports EV-r1's **sealed pure-kernel capability**, not a generic default context standing in for the SQL session. All native child/coercion/result work must keep the actual caller's `Columns`.

Q`expr/ctx.rs:184–208` has both `warning_cnt` and capped stored warning details. `append_warning` increments the count even when no detail can be retained. Health must therefore require **zero count and empty details**. `warnings.is_empty()` alone misses an unexpected warning when the detail limit is zero. Unexpected private warnings are contract failures to investigate; do not silently discard or merge them into native warning state and call the runtime healthy.

Q`expr/ctx.rs:196–201` allocates `Vec::with_capacity(max_warning_cnt)`. `take_warnings()` at `329–334` replaces it with another newly constructed warning buffer. Using that as unconditional per-row reset can allocate repeatedly and hide violations. Initialize the approved private context once per created runtime, keep its health invariant, and preserve all native warning counts, limits, bookmarks, drains and TryFold behavior.

K`local/batch.rs` constructs invocation-local budgets/output and row scratch before evaluation. Copying `ExecutionLimits` into a worker is compatible with this design, but proves neither allocation-free driver execution nor a total memory cap. No current input/result/row/native context reference should become an idle-cache member.

### F4. Clone/COW policy must distinguish handles from new execution lifetimes

X`stmt_context.rs:298–317` documents shared statement-effect owners, derives Clone over `Arc<StmtContextData>`, and uses `Arc::make_mut` in `DerefMut`. `configure` also uses `Arc::make_mut` at `1823–1829`. Consequently:

- A same-statement handle clone or native configuration COW detachment may share an `Arc` to the synchronized execution owner, not an aliased active runtime.
- A new execution/recipe instance must receive the appropriate empty/new owner or explicitly retired epoch.
- Do not copy `BuiltinFuncCache`'s reset-on-Clone policy onto ordinary statement-handle cloning: that would discard expected same-execution reuse.
- Conversely, do not make an execution-recipe clone inherit a live executable/lease merely because a handle clone is cheap.
- If compile/build/resource policy changes, explicitly detach/rekey/revalidate the owner. Native SQL configuration is not automatically a key for the fixed context-independent ASCII ABI; caching native descriptors would require the additional EV-r1 invalidation contract.

Production context IDs are assigned at X`stmt_context.rs:1842–1845`; default contexts use `Arc::as_ptr` allocation identity at `4041–4050`. That address can change on COW. The ID is not an execution-close epoch, a global pool lookup key, or a substitute for lifecycle wiring.

### F5. Dropping a pipeline handle is not closing its shared owner

X`projection.rs:293–307` clones the shared Arc into a queued/running worker task. `open` and `close` currently just set `self.parallel = None` at `342–346` and `386–390`; worker tasks can retain the old shared state after that drop. `close` explicitly expects a late task to find its receiver gone and drop its chunks.

Thus an owner inserted in shared projection state needs **explicit retirement**, not only a last-Arc Drop implementation. Close/reset marks the old owner closed and releases idle runtimes. Late returns must check owner/epoch identity and cannot replenish or publish into a replacement epoch. Already in-flight leases remain under their existing worker lifetime until they finish/drop; resetting a counter must not pretend their allocations have disappeared.

Owner clones within one execution must not retire each other on ordinary handle Drop. Actual close/open/rebuild boundaries need the retirement action. How existing cancellation/error policy treats an in-progress factory or worker remains an implementation decision to make explicitly; no new cancellation semantics are established by this review.

### F6. A caught panic defeats an eventual `thread::panicking()` check

X`sort_util.rs:106–121` catches an unwind and converts it to an executor error. X`projection.rs:298–304` invokes that recovery around the per-task suite. A long-lived scope outside that catch can later be dropped with `thread::panicking() == false`, despite an unwind through its evaluation.

Minimum acceptable enforcement of EV-r1's no-recycle-after-panic rule:

- Construct the worker scope inside the recovered closure where its lease is disposed during unwind; **or**
- Install an evaluation-lifetime unwind/poison guard that records the affected scope/lease as unhealthy and survives recovery. Its state must prevent later recycling even when normal Drop occurs after `catch_unwind` has returned.

This applies to a retained serial scope as well as a transient parallel one. A short-lived mutable-borrow guard being dropped is not proof that the retained runtime is healthy. Likewise, a pool mutex released before evaluation is not poisoned by a panic during unlocked evaluation, so its poison bit cannot stand in for runtime health. Do not catch panic and replay native ASCII.

### F7. Pool bounds need a live + creating + idle ledger, separate from call scratch

K`local/runtime.rs:47–71` explicitly describes Demo limits and excludes immutable program/input storage and allocator bookkeeping from retained-scratch accounting. K`local/batch.rs:478–495` creates a new budget per invocation. Applying `max_retained_bytes` from the copied `ExecutionLimits` on each invocation does **not** by itself bound pooled programs, warning buffers, input transport, or total owner retention.

The following is **review notation for the selected design**, not a shipped struct, new API, or implementation plan:

- **live**: checked-out runtime slots, including old-epoch leases that still exist.
- **creating**: reserved slots whose factory/allocation is in progress.
- **idle**: prepared runtime slots available for checked-out ownership transfer.
- `reserved_slots = live + creating + idle`, within the assigned owner/execution slot allowance.
- Retained-byte accounting covers actual owned capacities and in-progress reservations, not only logical lengths. Old-epoch debts stay charged until the corresponding storage is actually released; dropping/retiring storage is not free merely because it is no longer eligible for reuse.

| Transition | Required accounting/lifetime property |
| --- | --- |
| Create/bind an inert scope | No runtime reservation, preparation or pool-admission error yet. |
| First-ready checkout of idle runtime | Atomically move idle to live under the short bookkeeping lock; allocation ownership remains charged. |
| First-ready miss requiring preparation | Reserve creating slot and approved allocation allowance under the lock **before** compiling outside it. Concurrent empty-pool misses must not each bypass the cap. |
| Preparation succeeds/fails | Convert/release its reservation exactly once; failures cannot leak quota. Completion after close cannot publish into a replacement epoch. |
| Healthy same-epoch return | Move live to idle only when the owner is live and retained-byte/entry limits permit it; no cached call inputs/results. |
| Unhealthy, panicked, closed or ineligible return | Dispose instead of making the runtime available; keep storage charged until it is actually released. |
| Close/reset | Retire the old epoch and release idle storage; do not erase outstanding live/creating debts just to admit a new epoch. |
| Nested/reentrant demand | Reuse a propagated active scope where applicable; do not indefinitely wait for a lease held by the caller itself. |

The byte charge must include prepared RPN/schema/metadata storage, private context warning capacity, any retained transport/scratch capacities, the idle container's own capacity, and relevant peak replacement overlap. K`types/expr.rs:170–185,195–204` also shows structure metadata caching/invalidation: measuring only an empty/new shell is not a proof of warmed retained size. Exact Bytes transport and external native coercion allocations still need their respective EV-r1 accounting boundaries; do not claim that TiKV node/depth limits cap those allocations.

The initial recipe key stays finite and closed: reviewed domain/version, ASCII operation, canonical complete ABI types, metadata-none and approved compile/build/resource policy. It is not argument bytes, results, native AST pointers, a thread ID, or a context-ID map. Permitted nested scopes count toward the allowance; OS thread count is not a count of all live scopes.

A healthy fixed-policy row/chunk loop must not silently evict/recompile the sole ASCII recipe in steady state. Lease exhaustion remains an explicit resource failure at the demanded call **after frontend coercion**, never native fallback, a failure on a dead call, or indefinite waiting under native/session locks. This document proposes no numeric production default for these limits.

### F8. Forwarding can preserve reuse, but concrete caller loans remain

`Columns` deliberately has permissive defaults, including context ID zero (D`context.rs:522–527`), vectorization enabled (`540–543`), unbound prepared-parameter failure (`545–549`), native sleeping (`800–805`), no-op warning append/count/bookmark/drain behavior (`807–835`), and a default packet limit (`838–848`). Merely implementing `get`, TZ and warning append on a wrapper changes semantics.

`ScopedColumns` must forward **every existing method**, including methods whose caller implementation overrides a default policy method. Forwarding only primitive accessors and re-running the trait's default policy body is not equivalent to delegating an overridden method. Only the new opaque owner/worker capability is intentionally overridden. Coverage must include parameters/current INSERT values, context ID, charset, TZ/clock, SQL/type flags, RNG/sequences, packet and error policies, warnings/notes/count/bookmarks/drains, session services, and kill behavior. Future additions to `Columns` need a delegation-drift review.

An active worker scope takes precedence over an available owner, and recursion forwards it. D`expression.rs:719–730` and D`evaluator.rs:436–449` already pass the caller context through ordinary evaluation; scope propagation is compatible with their source shape. It does not require native subtree callbacks inside TiKV.

| Concrete caller | Source evidence | Minimum loan/reuse consequence |
| --- | --- | --- |
| Serial `EvaluatorSuite` | D`evaluator.rs:353–377,392–449`; X`projection.rs:381–383` | Bind once around the existing loop, retain execution ownership across `run` calls where the execution persists, and leave `EvaluatorProgram` and ColumnSwapHelper semantics unchanged. |
| Parallel projection | X`projection.rs:170–183,293–307` | Every chunk task creates a fresh native suite. A pool only in that transient suite would compile again per chunk. Keep the synchronized owner in actual shared execution/pipeline state, and make a local lazy scope inside each task/recovery boundary. |
| Normal statement helpers/DML | X`stmt_context.rs:298–317,1823–1845,4041–4050` | Attach only opaque synchronized ownership to the true execution handle, and borrow a scope across hot loops; apply the clone/COW/reset rules above. |
| SHOW filtering | S`show.rs:491–505,524–525,653–669` | `ShowRowResolver` is freshly built per row and implements only `get`. Hoist one owner/scope in `filter_show_output`, and pass the capability into each row binding. An automatic one-shot wrapper in `eval_in` alone would still compile per row. |
| Partial-index evaluation | X`kv_table.rs:423–442,3198–3218`; X`kv_table/index_entries.rs:488–505,526–541,567–583` | The real API receives index/row/timezone, then builds a new get/TZ-only context. Existing callers carry no execution capability here. Add an explicit caller loan across the row/index operation; timezone, a row address, or context ID cannot recover the owner. |
| Cached-plan deferred evaluator | P`physical_plan_cache.rs:310–318,331–345` | Callback type is Send+Sync. Capture synchronized ownership, not a borrowed non-Sync active scope. Construct the local scope inside an invocation and reuse the owner across calls; hoist a scope only where the callback interface permits it. |
| Generic NoColumns/custom callers | D`context.rs:518–527`; D`lib.rs:669–675`; EV-r1 sections 2 and 6.4 | A genuinely one-shot entry may use a stack owner/scope. Repeated production callers must supply execution lifetime outside their loop. NoColumns is not permission for per-row compilation or native fallback. |

These examples are concrete remaining loans, **not a substitute for EV-r1's complete caller inventory**. Joins, selection, grouping, aggregates/windows, sort, DML/generated/default/check expressions, planner/ranger/folding, session helpers and the remaining listed consumers still need their own full-entry ownership/route evidence. Checking projection alone cannot establish family-wide reuse or deletion.

### F9. Existing TiKV entrypoints do not yet admit this ASCII boundary

K`local/registry.rs:24–49` admits the signed-Int seed signatures and excludes ASCII. K`local/compile.rs:20–24,85–105` distinguishes Row, ControlLineage and SqlNumericBatch ownership domains and retains signed-Int checks outside lineage. K`local/batch.rs:406–436` validates full binding schema and compiled-entry compatibility before evaluation.

The new closed factory/entry/collector in EV-r1 is therefore still required. Do not enable ASCII by widening ordinary203, labelling it as lineaged control, or exposing a raw-RPN escape. The approved shape must still select the existing official ASCII kernel and driver, with canonical checked Bytes input and computed signed-Int output identity. No arbitrary graph, host/native expression callback, native retry after a TiKV failure, lossy UTF-8 fallback, or fabricated PB provenance is admitted.

Implementation requires the parent's actual C4 grant and coherent/released C3c ownership. This evidence file grants neither.

## 5. Future proof matrix — obligations, not results

Every row below is a **future acceptance obligation, not a test run or an existing test name**. Exact commands/targets must be selected by the designated implementer and parent once the authorized APIs exist. Small budgets, arithmetic-only extent cases and deterministic synchronization should suffice; no giant allocation or broad build is required merely to formulate these checks.

| ID | Proof surface / scenario | Required observable evidence | Present status |
| --- | --- | --- | --- |
| EVR-01 | Actual C4 prepared aggregate, execution owner, lease and borrowed scope traits | Compile assertions on the real integrated types and intended sharing boundaries; reject illegal executable/scope sharing without unsafe trait implementations. | Open. Parent receipts in section 3 cover CandidateParts/IdleOwner only. |
| EVR-02 | Empty input, no ASCII, and dead/unselected ASCII, including exhausted pool policy | Zero checkout/reservation, preparation, private-context creation and ready-kernel entry for unused calls; existing native outcome/error order unchanged. | Not run. |
| EVR-03 | Native child/transcode/coercion failure and warnings before first ready occurrence | Native effects/errors occur once at their original position; zero new runtime checkout/preparation before the ready boundary. | Not run. |
| EVR-04 | Ready NULL, empty bytes, binary/malformed UTF-8 bytes, and native result coercion | Checked official nullable driver/wrapper is entered for invoked NULL; one kernel route, computed Int metadata, no native first-byte/NULL result shortcut, no transcode replay; existing outer typed coercion remains once. | Not run. |
| EVR-05 | Nested ASCII and recursive expression entries | Existing scope propagated; sequential short runtime borrows; no recursive pool wait or borrow panic, no native evaluator callback into TiKV. | Not run. |
| EVR-06 | N rows, multiple chunks, W warmed concurrent lanes under fixed policy | Preparation and context-creation counts track actually created runtimes/policy versions, not N or chunk count; healthy reuse after return; no silent steady-state eviction. | Not run. |
| EVR-07 | SHOW and partial-index hot loops, plus all other EV-r1 inventory callers | Route/capability counters through real production entries establish scope lifetime outside each relevant row/index loop; a new resolver cannot erase the active scope/owner. | Not run; source loan sites recorded in F8. |
| EVR-08 | Every Columns delegation, including overridden default policy methods | Sentinel context observes exact calls/results for IDs, parameters/INSERT values, vectorization, charset/TZ/clock, flags, warning lifecycle, packet/error policy, RNG/sequence/session/kill services; only new capability differs. | Not run. |
| EVR-09 | Same-statement handle clone, COW configuration detach, new execution/recipe clone and policy change | Correct owner identity/reuse within one execution; no active runtime aliasing; new execution/policy cannot inherit incompatible state; no global context-ID lookup. | Not run. |
| EVR-10 | Close/reset with an old checked-out lease and queued/creating work | Idle storage released; old owner retired; late completion/return cannot populate a new epoch; all outstanding slot/byte debts remain charged until resolved. | Not run. |
| EVR-11 | Panic recovered while the scope survives outside the catch, plus ordinary worker unwind | Affected lease is not recycled even when its later Drop sees panicking()==false; parent receives existing panic-to-error behavior; no replay or double release. | Not run. |
| EVR-12 | Simultaneous empty-pool misses and preparation failure | Live + creating + idle reservations never exceed assigned allowance; construction occurs outside bookkeeping lock; failed factory releases exactly once; no resource failure on a dead call. | Not run. |
| EVR-13 | Tiny slot/byte limits, nested scopes, retained capacities and reset overlap | Structured demanded-call resource errors; input/native coercion and owner scratch/program charges are not conflated; no length-only accounting or premature release of old-epoch debt. | Not run. |
| EVR-14 | Private warning count with empty stored details, and native warning/TryFold state | Unexpected private warning is detected even with zero detail capacity; no silent warning drain; native count/bookmark/drain semantics remain untouched; context not rebuilt per row. | Not run. |
| EVR-15 | Input/result lifetime, healthy return, error and drop | No borrowed row/context/input pointer or result survives into idle storage; no retained SQL value cache; dirty/ineligible runtimes are disposed with correct accounting. | Not run. |
| EVR-16 | Closed ASCII factory versus legacy row/control/numeric entries and PB-negative routes | Exact factory shape/schema/entry/Int-result checks; old admission remains closed; no arbitrary graph/host/callback/raw-RPN escape, PB impersonation, or native retry. | Not run. |
| EVR-17 | Full hot-entry behavior, deletion and performance acceptance | Required EV-r1 route/inventory gates, native arithmetic deletion, allocation/retention and performance measurements, complete validation receipts and parent acceptance. | Open; no complete-family credit. |

## 6. Validation record and remaining risk

**Files changed by this follow-up:** only `/home/agent/tidb/expression-unification/evidence/evaluated-value-review.md`, created under the parent's one-file documentation grant. The original review changed no files.

**Reviewer execution:** shell command `pwd` confirmed `/home/agent/tidb`; source/evidence inspection used the read, glob and grep tools. The file is checked by rereading its written contents. No Cargo, rustc, Go, formatter, test, benchmark, lint, allocator probe, or hash computation was run by this reviewer. No background build or helper agent was started. The actual trait compilations are the separately attributed parent receipts in section 3, not reviewer test results.

**Unverified locally:** all matrix behavior, actual future C4 types, actual pool/lease implementation, cancellation/close integration, full forwarding, full caller reuse, retained-allocation bounds, whole-hot-entry coverage and performance. The refreshed trait gate does not resolve the parent's separate C3c test-only fixture compilation issues.

**Risk boundary:** correctness depends on preserving native demand/coercion/result/error/warning order; compatibility depends on not fabricating native context or PB/source metadata; lifetime safety depends on explicit epoch retirement and panic health; memory/performance depend on complete owner accounting and real caller scope propagation. The narrow minimum is to implement and prove EV-r1's existing ownership model at the identified seams, not to introduce a new evaluator, global/TLS cache, broad context redesign, or fallback algorithm.
