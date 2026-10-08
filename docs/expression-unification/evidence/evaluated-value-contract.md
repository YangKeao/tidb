# Evaluated ASCII value: C4 caller contract and source inventory

**Revision: EV-r3 addendum (round10). The explicit capability/value cut below is authorized; implementation and actual acceptance are tracked in the sole ExecPlan. The original EV-r2 proposal/history follows and is not a fresh activation claim.**

## EV-r3: narrow explicit capability/value authorization

- Publish existing opaque native policy/owner/execution/scope/owner-error types and the borrowing ScopedReadyValueColumns shape. Keep all data fields and backend machinery private. Lifecycle/config methods retain Result<_,ReadyValueOwnerError>; only the value evaluation boundary returns EvalError. No default policy, session epoch integration, implicit scope creation or SQL dispatcher switch is authorized by this cut.
- Columns gains two pure borrowed optional queries: ready_value_scope and ready_value_execution, both default None. ScopedReadyValueColumns has63 ordinary native forwarders plus TWO effective-capability overrides; the latter are not arbitrary forwarding. Public with_columns takes a Sized opaque borrowing wrapper so existing Sized consumers can use it without unrelated signature rewrites.
- Existing active scope wins over the explicitly requested fallback scope, even if stale/poisoned. Wrapper, execution capability and normal-operation unwind guard all use that SAME effective scope. Before capability discovery succeeds, a temporary guard covers the requested scope; getter panic conservatively quarantines only that requested scope, not an unknowable hidden active scope. Establish the effective guard before disarming the discovery guard, without an intervening callback/allocation/fallible operation. No catch/retry/replay is introduced.
- Public ReadyValueScope::evaluate_value delegates the existing checked closed-worker value boundary for an already evaluated/context-transcoded Datum. It is not AST argument evaluation, SQL charset/return-coercion handling, or global ASCII activation. Existing frontend coercion errors still precede scope/pool/backend admission, including when the scope is closed or has no slots. NULL must actually go through the official worker when admitted.
- Actual LocalError is captured once at its failing producer as Prepare (factory), Observe (retained_storage), or Invoke (eval_one). Invoke does not establish kernel-body entry. Keep primary cause identity/phase when secondary cleanup/observation fails; do not manufacture natural Observe failures where only structural coverage is available.
- Pool, Scope and result-Bridge failures retain their ORIGINAL native causes in a separate opaque Arc carrier, never a fabricated LocalError or arithmetic status. Public adapter class/origin are native-only, private constructors/accessors remain within the adapter. Fixed terminal1105/HY000 messages distinguish PoolPolicy/Resource/Closed/Poisoned/Contract, ScopePoisoned/Reentry/Contract and ResultContract. Class/origin do not replace SQL evaluation-origin handling.
- Worker/pool accounting remains the pinned conditional request/retained basis, not portable ABI, physical heap, factory peak or OOM recovery. Native handles/coercion and error-carrier allocations are outside that ledger. No test_policy constants become production defaults.

Round10 file loans: E owns ready_value.rs plus its tests; D owns new adapter_failure.rs and executor terminal mapping/tests; parent owns context/lib/mod exports, runtime_failure documentation, compilation and acceptance. A reviews read-only. Real public value→wire resource tests must use the production policy/owner/execution/scope path, not a new test-only constructor. Normal SQL entrypoints, business-context capability propagation, lifecycle/catcher integration, native deletion and performance remain separate open gates; complete-family credit stays0.

## Historical EV-r2 proposal

The sole execution plan is `/home/agent/tidb/EXPRESSION_UNIFICATION_PLAN.md`. Parent has now released C4's exact six TiKV files under `runtime-contract.md` §C4-proposal-r0, with **mandatory metadata-cache prewarm before worker publication**, the actual function-pointer wrapper witness, and A's separate private Arc-accounting probe. That release is C's, not E's; it does not establish a completed C4 native gate. E's present grant remains **this document only**. Root exports, `lib.rs`, Cargo, shared module wiring and all acceptance/activation decisions remain parent-owned.

EV-r2 incorporates `evaluated-value-review.md` EV-review-r1. **§11 is the concrete closed-ASCII API/owner/lease proposal and supersedes the earlier generic API roles and any weaker lifecycle reading in §6.** The full §2 entry/deletion inventory and §6.4 placement map remain obligations, not permissions. §8 separates the first private two-file loan request from later capability/activation/hot-loop work.

No product code, build, test, benchmark, formatter, or helper-agent execution accompanies EV-r2. The parent's actual OLD native baseline is separately accepted in `ascii-baseline-contract.md` AB-r1; its source/binary remain immutable and it proves no C4 worker/scope behavior. The frozen denominator remains **245**, target **221**. A private prototype contributes **0** complete families. ASCII can contribute **1** only after all implemented-domain, route, runtime, deletion, and performance gates below pass and the parent accepts the result. LENGTH/OCTET_LENGTH together are one family, not two.

## 1. Source basis, notation, and decision

Pinned bases: TiDB `364aef2bab5cc633ecb76a775ae8f36f86a6687d`; TiKV `548812e1ef57aef077a2062a9cc356640a6347f5`. Observations below are from the current expression-unification worktrees, not just those base commits. Line anchors are navigation aids, not a frozen patch or proof of coverage.

Paths below use:

- **D** = `expression-unification/tidb/rust/crates/tidb-expr/src/`
- **X** = `expression-unification/tidb/rust/crates/tidb-executor/src/`
- **P** = `expression-unification/tidb/rust/crates/tidb-planner/src/`
- **S** = `expression-unification/tidb/rust/crates/tidb-session/src/`
- **U** = `expression-unification/tidb/rust/crates/tidb-unistore/src/`
- **V** = `expression-unification/tidb/rust/crates/tidb-datatype/src/`
- **K** = `expression-unification/tikv/components/tidb_query_expr/src/`

**Decision:** start with ASCII. BIT_LENGTH would require its own separately reviewed closed factory/domain; C4's named ASCII-only ABI does not accept another operation. Defer the LENGTH/OCTET_LENGTH cut until its public builder/helper and its shared byte-count arm with CHAR_LENGTH are accounted for. Do not substitute CHAR_LENGTH merely to obtain an existing PB route.

| Family | Native arithmetic seam | Existing authoritative TiKV kernel | Actual TiPB ID | Initial status |
| --- | --- | --- | --- | --- |
| ASCII | D`string_fn.rs:138–145`: coercion, first byte or zero | K`impl_string.rs:211–219`; selection in K`lib.rs:834` | `Ascii = 7003` | Recommended first; not migrated |
| BIT_LENGTH | D`string_fn.rs:150–157`: coerced byte length times eight | K`impl_string.rs:158–161`; K`lib.rs:830` | `BitLength = 7001` | Follow-on, separately accepted |
| LENGTH / OCTET_LENGTH | D`build.rs:240–255`, `BuiltStringLength::eval` | K`impl_string.rs:91–95`; K`lib.rs:826` | `Length = 7029` | One alias-normalized family; not first cut |

The original ASCII and BIT_LENGTH argument fronts can remain, but their numeric bodies must disappear. A thin adapter may retain the old helper name; it must not retain `first()/[0]`, empty-string-to-zero arithmetic, byte-count arithmetic, or a native NULL/result fast path for an invoked ASCII call. TiKV's generated nullable RPN wrapper owns NULL propagation for the invocation. A skipped call in an unselected/dead branch is different: no kernel invocation is required or allowed.

## 2. Candidate entry and deletion inventory — full activation audit still required

### 2.1 Expression dispatch, helper, and optimizer entries

| Entry / consumer | Current source route | ASCII/BIT_LENGTH effect | LENGTH/OCTET_LENGTH difference |
| --- | --- | --- | --- |
| Public AST `eval` / `eval_in` | D`lib.rs:460–463,796–928` → D`func.rs::eval_func` | Child evaluation, existing BinAware transcode and argument wrapping, then values dispatch → `string_fn` body | D`func.rs:113–131` uses `BuildContext`/`BuiltStringLength` before the ordinary values dispatcher |
| AST already-evaluated dispatch | D`func.rs:260–304,447–470,506`, ASCII at `765`, BIT_LENGTH at `789` | Both end at one native numeric body per family | Values-only dispatcher explicitly excludes length/character-length functions; do not add a schema-guessing fallback |
| Typed SQL row expression | D`expression.rs:719–730` → D`scalar_function.rs:1192–1212` → `eval_by_signature` → D`func.rs::eval_func_values_in` at `scalar_function.rs:3045` | Same numeric body as AST; final native `coerce_to_ret_type` remains outside it | Signature choice/helper at D`scalar_function.rs:2382–2394` |
| SQL rewritten arguments | D`rewriter.rs:432–475`, `wrap_binary_literals` | Explicit `to_binary` child for BinAware calls; argument evaluation performs it | Same frontend ownership of charset-sensitive argument preparation |
| Manually constructed typed node | `ScalarFunction::new` + `ScalarFunction::eval` | Does not spontaneously gain the AST/rewriter's `to_binary` child; preserve this existing difference | Static type still determines the selected length signature |
| Constant/deferred/parameter descendants | D`expression.rs` and `constant.rs` return through normal typed evaluation when evaluating a deferred expression | Nested ASCII reaches the same body; value/parameter reads stay at their current demand point | No new constant-result cache |
| Constant folding / NewFunction folding | D`constant_fold.rs:158,325–326,390`; D`lib.rs:669–675` `eval_expression_once` | Evaluates the same expression; TryFold warning bookmark/truncation and fold stash remain native frontend responsibilities | Existing GBK `length(to_binary(...))` fold is a useful charset regression |
| Private/direct value helper tests | D`string_fn.rs`; `func` and `string_fn` are private modules | No separate public ASCII/BIT_LENGTH value API was found; direct/private tests must also use the new single kernel | D`lib.rs:393` exports `BuiltStringLength`; this is a real public value-helper obligation |
| Type inference / remote-name policy | D`rewriter/result_type.rs:2004–2007`; D`infer_pushdown.rs:294–302` | Metadata and authorization, not numerical execution; ASCII flen is 3 | Length/bit-length flen is 10; a pushdown name whitelist is not an encoder/decoder implementation |

Arity and coercion stay at their **existing** frontend positions. In particular, do not move a late arity check ahead of already-performed child evaluation merely because the new kernel factory accepts one argument.

### 2.2 Vector, filter, and executor surfaces

D`evaluator.rs:386–450` keeps its existing select-list/row-major versus column-major schedule. The candidate functions have no dedicated numeric batch kernel: D`scalar_function.rs:3899–3946` (`numeric_batch_supported`) and `3303–3406` (numeric comparison / boolean fast paths) do not implement them. Their nested calls eventually use the ordinary row evaluator. Initial integration is **width one after the current frontend has evaluated and coerced that occurrence**. It is not a new whole-column argument evaluator.

X`selection.rs:62–73,110–121,157–233` has fast null/string-IN/conjunction predicates, not an ASCII or byte-length implementation. Unrecognized filters call `Expression::eval`; an ASCII child inside a general expression still reaches the common body. Do not remove unrelated selection kernels as part of this family cut.

The following are live generic consumers of typed expressions, hence obligations for origin, semantics, and reusable execution ownership. They are not additional ASCII arithmetic implementations:

| Consumer cluster | Current call-site anchors | Required ownership / proof |
| --- | --- | --- |
| Projection, serial and parallel | X`projection.rs:75–115,295–307,381–383`; D`evaluator.rs:303–377,392` | Execution-owned runtime across rows/chunks; shared native program stays free of mutable RPN instances |
| Selection and pushed local predicates | X`selection.rs:118`; X`predicate_pushdown.rs:374`; X`access_path.rs:6414` | Preserve live-row order and early stop; scope spans the loop, not each row |
| Join residuals and join expression arguments | X`joiner.rs:205`; X`join.rs:1113,5508` | Per execution/lane ownership, no expression replay after error |
| Sort / shuffle keys | X`sort.rs:289,386–387`; X`shuffle.rs:377` | Retain original comparator/key evaluation schedule |
| Group keys and aggregate arguments | X`hash_agg.rs:2938,3580–3657`; X`hash_agg/group_key.rs:163`; X`hash_agg/input.rs:588`; X`stream_agg.rs:156,246`; X`vec_group_checker.rs:457` | Per partial lane or checked-out runtime; native aggregate-state sharing is not RPN-state sharing |
| Window expressions and defaults | X`window.rs:266–267,376–382,580,601–603`; X`hash_agg/window_numeric.rs:137`; `window_extremum.rs:63` | Preserve repeated evaluation and frame/demand order |
| Generated columns, defaults, CHECK/partial index | X`generated_column.rs:530–552`; X`column_default.rs:730,827–859`; X`kv_table.rs:3198–3230`; X`kv_table/index_entries.rs:505,541,582–583`; X`admin_check.rs:207–222,352` | Statement or explicit write/scan operation owns reuse; not table metadata or a per-row resolver; ADMIN CHECK's row loop is included |
| DML / correlated and subquery expressions | X`driver/dml.rs:880,1629,1906,4152`; X`driver/multi_dml.rs:712,909`; X`driver/dml/correlated.rs:99–146`; X`driver/subquery.rs:654` | Borrow the active execution scope through helper calls; no cached current INSERT row or correlated value |
| Partition/range/pruning and optimizer sampling | X`partition_pruning.rs:669,995,1032,1062`; X`access_cost.rs:2584,2867`; P`physical_plan_cache.rs:341–345` | One rebuild/pruning/sampling invocation owns its loop scope; preserve existing failure handling |
| Virtual columns in union scan | X`union_scan.rs:612` | Borrow the scan execution scope |
| Planner/build/DDL constant evaluation | P`logical/rewrite.rs:3208,3567`; X`driver/catalog.rs:3072`; X`driver/planner_bridge.rs:2315,2526`; X`ddl/alter_table.rs:2531`; X`ddl/table_partition_range.rs:348`; `table_partition_list.rs:198,386`; X`hash_agg/builder.rs:86,123` | One-shot remains supported; repeated invocations in one operation need an explicit surrounding scope |
| AST SHOW filters | S`show.rs:491–529,645–667,1056–1057,1147–1148,1209–1210,1264–1265,1350–1351,1822–1823` | A real non-`StmtContext` hot AST consumer: own one scope outside the SHOW row loop |

This inventory distinguishes call sites from proof: each cluster still needs an executed representative and a final changed-tree call-graph audit. Public/custom callers repeatedly using a one-shot API must be given the explicit reusable API; merely changing projection does not close the family performance gate.

### 2.3 PB, remote pushdown, and unistore: preserve the negative domain

For **ASCII, Length, BitLength**, current TiDB `pushdown_catalog` has no executable signature row, `PbBuiltin::new` has no matching string arm, and unistore `SimpleSig` has no corresponding candidate arm. Frozen M0 family records likewise list no supported typed-PB or unistore converter signatures for these candidates.

Relevant checks:

- D`scalar_function/pb_builtin.rs:106–137,182–187,422–469`: the fallback to `cast_types` is a finite cast list, not support for arbitrary string signatures.
- D`distsql_builtin.rs:83–84,235–236`: unsupported signatures fail the typed decoder.
- D`pushdown_catalog.rs:2350–2384`: resolution requires an actual matching catalog row.
- U`cophandler.rs:2036–2046,2077–2082,2670`: `convert_shared` calls the same decoder; the final fallback to `convert_shared` is **not** fallback to a native ASCII body.

Therefore there is no current native ASCII/Length/BitLength PB kernel to delete. Assert that these unsupported PB signatures remain unsupported; do not count this as successful PB execution. Selecting TiKV's existing `ScalarFuncSig::Ascii` internally does not prove PB ingestion and does not authorize remote serialization, blacklist changes, or a fake `PbRow` label.

**Separate CHAR_LENGTH residue:** D`build.rs:243–247` shares byte counting with `CharLengthBinary`. D`scalar_function/pb_builtin.rs:366–375` and U`cophandler.rs:4381–4394` contain character-length implementations; converter arms remain at U`cophandler.rs:2220–2221` (normal supported signatures prefer the shared decoder first). These belong to a different family. Moving LENGTH alone is not deletion of every native byte-count operation. UTF8 character-length is not source-equivalent: D`build.rs:283–305` counts invalid bytes with Go rune behavior, K`impl_string.rs:927–929` validates UTF8, and the unistore body uses lossy decoding. Track these separately, without claiming an ASCII dependency on their closure.

## 3. Frontend semantics that the new domain must preserve

### 3.1 The input is the result of the existing coercion, not an original SQL datum

D`arg_eval_type.rs:348–384` does not put ASCII/BIT_LENGTH into `string_arg_mask`. Their actual conversion is D`coerce.rs:181–201`, `coerce_str_bytes`:

| Original datum class | Existing pre-kernel action |
| --- | --- |
| String | Copy its stored bytes; do not require UTF8 validity |
| Bytes / Raw | Copy the byte payload |
| Int / UInt / Decimal / Real / Float32 | Existing native display-to-bytes rule, including its current formatting |
| BinaryLiteral / BIT | Stored literal bytes, including leading zero bytes; not numeric value reinterpretation |
| Duration / Time / JSON / VectorFloat32 | Existing display-to-bytes rule |
| ENUM / SET | Name bytes, not the numerical member/set value |
| NULL | Nullable Bytes with no payload |
| MinNotNull / MaxValue | Existing `Unsupported("range sentinel byte coercion")` before the kernel |

Do not route original Decimal/JSON/temporal/ENUM/BIT/float values through a primitive transport API which does not admit them. The frontend first performs its already-existing SQL coercion. The new domain then transports **only the explicitly normalized nullable Bytes result**. This is not lossless transport of those original types, not Decimal closure, and not permission to invent a new SQL string/f64 bridge. Non-finite floating inputs and unusual display forms must be characterized through this same existing coercion, not accidentally subjected to TiKV's finite-Real admission.

Coercion errors remain pre-kernel errors; they are not evidence that the ASCII RPN kernel ran. There is no native retry after a TiKV error. No new formatter, UTF8 decoder, collation comparator, byte-length computation, or first-byte arithmetic belongs in the adapter.

### 3.2 Concrete distinctions

Source-derived examples to preserve and execute as tests, not new oracle results:

- `ASCII(2)` yields 50 after numeric-to-string coercion, not integer 2.
- Empty Bytes yields 0; NULL yields NULL; a first raw byte `0xff` yields 255; embedded/leading NUL is data.
- UTF8 `你好` starts with byte 228, not a Unicode scalar value or character count.
- A GBK-tagged StringDatum containing UTF8 storage for `中文` becomes bytes `d6 d0 ce c4` through the existing non-legacy transcode: ASCII 214, byte length 4, bit length 32. Without that transcode its UTF8 bytes give 228 / 6 / 48.
- AST D`func.rs:272–286` applies `to_binary_by_collation`; rewritten typed SQL has its explicit `to_binary` child. A manually built typed call without that child currently does **not** acquire the same transcode. Preserve each actual front. The new kernel must never transcode again.
- Binary-literal/BIT width and leading zeros affect the operand byte sequence but must not leak into the computed integer's metadata.
- CHAR/VARCHAR width, padding, collation names, and flen are not instructions to trim or pad bytes inside ASCII. Do not reinterpret a byte count as a character count.

The original frontend still owns argument/dead-branch demand, charset errors and warnings, current parameter/correlated values, RNG/sequence calls, and first-error order. Width-one invocation after each coerced occurrence preserves these schedules; pre-evaluating a whole column of children would not.

## 4. Fixed C4 boundary and TiDB adaptation

The following TiKV ABI is fixed by the parent's C4 release. It is not an existing C3 entry, nor a claim that C4 has passed native validation. The TiDB APIs in §11 are proposals awaiting their own loans.

### 4.1 TiKV boundary

```rust
prepare_evaluated_ascii(
    cx: LocalCompileContext,
    execution: ExecutionLimits,
    max_worker_retained_bytes: usize,
) -> LocalResult<EvaluatedAsciiWorker>;

// Opaque Send worker; no Clone, Deref, program/context/state getter or Sync promise.
EvaluatedAsciiWorker::eval_one(&mut self, ready: Option<Vec<u8>>)
    -> LocalResult<ComputedInt>;
EvaluatedAsciiWorker::kernel_invocations(&self) -> u64;
EvaluatedAsciiWorker::is_healthy(&self) -> bool;
EvaluatedAsciiWorker::retained_storage(&self) -> LocalResult<WorkerStorage>;

ComputedInt::value(&self) -> Option<Int>;
ComputedInt::into_option(self) -> Option<Int>;
ComputedInt::metadata(&self) -> ComputedIntMetadata; // OwnSignedInt only
WorkerStorage::inline_bytes(&self) -> usize;
WorkerStorage::owned_heap_bytes(&self) -> usize;
WorkerStorage::total_bytes(&self) -> usize;
```

Only the named ASCII factory is admitted: no operation enum, signature selector, caller graph or native callback. The worker owns program/state/sealed context. It prepares the exact canonical Bytes-slot0 → Ascii7003 → signed-Int two-node program through the shared driver. `ComputedInt` follows generated owned Int-output/width/type/health checks, including NULL. `retained_storage` is nonmutating; all metadata-cache population must finish **inside preparation, under the caller's creating reservation, before publication**. E does not add a dummy evaluation or invent a separate prewarm/get-context API.

The wrapper witness advances at the real `func_meta.fn_ptr` invocation. Its per-worker value is usable in an ordinary TiDB dependency build; it is not a non-NULL-body counter. The separate isolated cfg(test) TiKV body test and A's Arc allocation receipt remain parent gates. TiDB cannot manufacture that body evidence from a facade call count. Concrete TiKV types in this subsection remain confined to the private adapter; no public `Columns` signature exposes them.

Required properties:

1. The factory constructs and validates the exact two-node shape. No caller-provided arbitrary graph, recursive callback, cast, host, ordinary203/control profile, literal folding, PB Expr or arbitrary Any metadata is admitted.
2. It uses the existing official call preparation/selector and RPN assembly/driver. `Ascii` resolves to K`impl_string.rs::ascii`; no copied algorithm, direct alternate native implementation, or new interpreter.
3. Canonical kernel argument/result FieldTypes are explicit internal ABI types, not reconstructed original SQL schema or original wire types. Full equality is checked for the binding schema; carrier, cardinality, NULL representation and output Int shape are checked before/after execution.
4. The initial API evaluates one ready occurrence. It does not open native batch or C3b control composition. The caller may invoke it repeatedly in its existing order. Any future wider already-evaluated batch API needs its own reviewed shape, selection, accounting, and ordering proof.
5. Any binding-services adapter is closed storage over that ready value. It cannot call `Expression::eval`, `eval_in`, a native subtree closure, or HostCall.
6. Result metadata is a computed signed-Int identity owned by the result boundary, not an input identity or a selected-branch lineage ID. NULL still has the checked Int carrier/identity.
7. Build/admission, binding, resource and evaluation failures remain distinguishable structured failures. No string-driven fallback or native replay.

Current barriers are deliberate: K`local/registry.rs:24–49` excludes these signatures; K`local/compile.rs::CompileMode::check_type` retains signed-Int type checks outside the admitted lineage mode; `compile_local_profiled` is the closed PlusInt203 entry, and K`local/lineage.rs:5–8` limits the separate Int/Bytes domain to leaves/controls. New `OutputMode`/accounting plumbing must not silently widen those old entrypoints.

Historical EV-r1 source recheck (not the current C3c acceptance status) also observed K`local/compile.rs:16–34` adding explicit `ProgramEntry::{Row, ControlLineage, SqlNumericBatch}`, with `LocalNumericBatchProgram` and numeric-batch policy work appearing concurrently. The signed-Int gate is now at `compile.rs:98–105`, and `profile.rs:465–479` still restricts that new numeric-batch policy to signed LongLong. No ASCII admission was found. This document makes no acceptance claim for that concurrent work. The evaluated-value factory must have its **own compiled entry identity** and opaque facade, not reuse Row/ControlLineage/SqlNumericBatch as a misleading ownership tag; wrong-entry calls, including empty calls, must fail closed. Coordinate any added enum arm with C rather than editing its files under this document grant.

### 4.2 TiDB boundary

```text
FrontendConsumer = AstValueOnly | TypedRow | ValueHelper
ReadyBytes = output of the ORIGINAL frontend's coerce_str_bytes/transcode
KernelComputedInt = (checked Int carrier,
                     ValueMetadata { kind: Int,
                                     string_collation: None,
                                     decimal_declared_shape: None })
```

The consumer fact describes the actual frontend invoking this value operation. It is not a C3a profile assertion, authentication of a whole native tree, proof of PB ingestion, or a new source FieldType inferred from runtime Datum. Rewritten and manual typed calls share the TypedRow value boundary but retain their different, actual argument graphs. Optional diagnostic/test call-site identity must not be passed off as an original PB/source ordinal.

D`func.rs` supplies the existing evaluated/coerced value to the concentrated D`tikv/` adapter. Only that boundary knows concrete TiKV types. The factory/profile remains distinct from D1–D5 lowerers and from any future C3c full-expression bridge.

### 4.3 Context capability

ASCII's official kernel takes Bytes, reads no SQL configuration, and emits no SQL warning. The proposed context is therefore a **positively sealed pure-kernel capability**, not a default context pretending to represent a statement:

- Its factory may create the underlying TiKV `EvalContext` once when a runtime is first prepared, with the no-context dependency justified by the exact admitted shape/signature.
- Never create that context per row, and never extend the enum to a context-sensitive operation without reviewing this capability.
- All frontend child/coercion/transcode/post-result behavior uses the caller's actual `Columns`; never substitute NoColumns, default SQL modes/TZ, or a fabricated statement context there.
- Private kernel warning state must remain empty for this domain. Unexpected warnings/context dependence are a contract failure to investigate, not warnings to silently discard. Native warnings, counts, limits, and TryFold bookkeeping are never reset when reusing the pure runtime.

## 5. Full FieldType and result-coercion contract

There are **two result boundaries**. Conflating them would regress manual typed callers.

1. **Kernel result:** computed signed Int, including NULL. Use `ValueMetadata { Int, None, None }`; V`tikv_compat/value.rs:261–311` validates carrier/metadata before handling NULL. Never reuse String/BinaryLiteral/BIT/UInt operand identity for this result.
2. **Existing frontend result:** retain the actual complete native return FieldType and the current post-result adaptation. D`scalar_function.rs:1192–1212` calls `coerce_to_ret_type` after the non-PB body. D`scalar_function.rs:1029–1109` preserves NULL, has BIT-width handling, same-family behavior, and the existing conversion/failure policy. Execute that path once, in its existing position.

For normal SQL ASCII, inference yields signed integer with flen 3 (D`rewriter/result_type.rs:2007`). Do not replace the actual return descriptor with a generic `LongLong` just because the kernel uses an Int carrier. Preserve every native flag, flen/decimal, exact charset/collation name, element list/markers, ARRAY information, and other FieldType fields even when irrelevant to this particular kernel.

Important manual-node regressions to prevent:

- A manually declared BIT result must still undergo the existing **outer** integer-to-BIT width conversion. That is different from incorrectly widening the kernel result from the operand's BIT metadata.
- A manually unsigned native SQL return descriptor must keep the current non-PB behavior. Do not add the PB-only signed/unsigned reinterpretation at D`scalar_function.rs:1195–1204` to this new domain.
- A different manually declared evaluation family must retain the existing `convert_to` behavior, including its existing error handling. This task does not clean up that unrelated policy.
- Value-only AST/helper calls retain the absence of a SQL return schema. Do not synthesize a schema and then apply typed return coercion to them.

**Minimal cache separation:** the cached RPN recipe contains only the fixed, normalized Bytes→Int ABI. It stores no native AST, child graph, SQL return descriptor, parameter, collation-derived transcode decision, or row pointer. The original frontend continues reading its real complete type/arguments at the original evaluation points and performs post-result coercion itself. Thus changing a native type/argument cannot leave an old cached native projection active: none is cached here. A same-op fixed ABI recipe is not a cache of the native expression.

If implementation later chooses to cache a frontend descriptor, metadata record, or native specialization, that is an additional contract: use V`tikv_compat/value.rs::snapshot_field_type` (deep copy, not shallow `FieldType::clone`), private immutable records, complete matching/invalidation, metadata byte bounds, and no mutable publication getter. Snapshot once per specialization, not per row. Never use an AST pointer, hash alone, context ID 0, or a lossy type projection as the identity. A cache holding native args/return metadata must follow the main plan's full invalidation rules; the fixed-ABI separation above is not an exemption for such a cache.

## 6. Caller lifetime and preparation cache: feasible placement, no TLS

### 6.1 Why the obvious placements fail

- K`local/compile.rs::LocalProgram` (currently `26–35`): `LocalProgram` is worker-owned; Any+Send metadata does not promise Sync. Do not put it in `Arc<LocalProgram>`, in the shared native `Expression`, or in `Arc<EvaluatorProgram>`.
- D`builtin_ext/cache.rs:22–34,64–83` stores `Arc<T>` inside an RwLock. It is not automatically a valid cache for this non-Sync executable; no unsafe Sync assertion or Any+Sync conversion is permitted.
- X`projection.rs:75–89` shares native program/context; `295–303` creates a **fresh EvaluatorSuite for every chunk task**. Adding a field only to that fresh suite does not provide reuse across parallel chunks.
- `StmtContext` is `Arc<StmtContextData>` (X`stmt_context.rs:301–316`), and projection requires `Columns + Send + Sync`. Inserting a bare RefCell/non-Sync runtime into shared statement data is not a valid solution.
- No process-global/TLS runtime, session-reference cache, thread-ID cache, or context-ID-to-runtime map. Pool threads outlive statements; thread identity is not the expression's semantic lifetime.

### 6.2 Selected model: owned worker runtime plus execution-owned idle pool

Use an **opaque TiDB execution owner** with synchronized bookkeeping for *idle owned runtimes*. An active runtime is moved out and exclusively owned by one worker/lease. This is ownership transfer, not shared access to a non-Sync program.

```text
execution owner (statement / executor invocation / SHOW operation)
  Mutex<IdlePool<OwnedValueRuntime>>   -- owner is Sync if runtime is Send
       | take an owned runtime; release lock immediately
       v
  WorkerLease owns OwnedValueRuntime  -- Send, NOT required to be Sync
    prepared ASCII program (lazy, once per recipe/policy)
    reusable local state / bounded scratch
    private pure-kernel EvalContext (initialized once, not per row)
       |
       + borrowed ScopedColumns(native Columns, worker access)
           existing native evaluation and result coercion
           short exclusive runtime borrow ONLY for each ready-value invocation
       |
       + on normal/error scope exit: clear per-call references/state;
         return healthy runtime to SAME live owner, otherwise drop it
```

Rust feasibility requirement: a `Mutex<Pool<Runtime>>` is a Sync owner when `Runtime: Send`; the runtime itself need not be Sync. An Arc, if needed for the execution owner, wraps that synchronized owner, **not** a raw non-Sync program. Add compile-time Send/Sync assertions for these intended types before integration; source analysis is not a completed trait check. Do not add unsafe trait implementations to force it.

A serial owner may retain one worker runtime directly. The idle-pool path is needed where existing Send+Sync contexts or transient pool tasks require shared ownership of the *factory/available leases*. Prefer retaining one lazily acquired worker lease for a whole row/chunk loop, not locking a pool for every character operation. The new scope is a transport/lifetime adapter, not an expression evaluator.

EV-r2 originally introduced an **ASCII-only** concrete proposal; round224 generalizes the authority to ready values and supersedes its rotating/current-epoch lifecycle with the independent-execution contract in §11:

```text
ReadyValuePoolOwner::new(accepted_policy) -> synchronized stable accounting root
owner.begin_execution() -> ReadyValueExecution (independent statement handle; peers remain live)
execution.scope() -> ReadyValueScope (inert, no lease/preparation yet)
scope.with_columns(native, body) -> body result through ScopedReadyValueColumns
Columns::ready_value_scope() -> Option<&ReadyValueScope>     [later hook loan]
Columns::ready_value_execution() -> Option<&ReadyValueExecution> [later hook loan]
private eval_ready(scope, ReadyAsciiBytes) -> checked native computed Int
```

The scope owns an independent execution handle and later an affine lease, not a native context. `ScopedReadyValueColumns` borrows the real native context and active scope only while its body runs. Root capability exports/hooks are a separate parent-owned cut; the first private two-file prototype uses an explicit scope and does not pretend those hooks already exist.

The default optional capabilities on an ordinary Columns implementation are absent; that absence is not a request to use native arithmetic. A public one-shot wrapper makes an explicit stack owner/scope when necessary. An active scope takes precedence over an owner and is propagated through recursion. A provided execution owner permits reuse across separate calls, but hot loops should bind one scope outside the loop. Concrete TiKV types stay inside the opaque TiDB boundary types. The scope borrows the real context; it does not copy it into a long-lived runtime, use TLS to discover it, or pass a native evaluator closure to TiKV.

Creating the scope is only inert bookkeeping. Its interior state starts without a lease; **checkout, allocation of a new runtime, preparation, and pure-context initialization are all lazy at the first ready-value invocation**. This prevents an unused/dead ASCII branch or a query with no ASCII at all from failing an unrelated pool-admission check. Once acquired, the lease may remain in the scope across successive invocations, with no active runtime borrow between them.

### 6.3 Lifecycle and safety rules

1. **Create:** each actual executor evaluation lane directly creates one empty `ReadyValueCache`. Lazy preparation occurs at the first demanded ready-value invocation, after the original frontend's checks/coercion. A dead/empty path must not prepare or execute a call merely to populate a cache.
2. **Reuse:** each lane cache retains at most one prepared worker per operation, including its immutable fixed ABI recipe, prepared program and approved private context. Invocation inputs/results are not cached. Across N rows on W active lanes, preparation/context creation is bounded by the operations demanded on those W lanes, not N or the number of chunks.
3. **Borrow order:** evaluate children, transcode and coerce with the native context first; then take the short mutable worker borrow; execute checked ready Bytes; release it; then run native return coercion and diagnostics. Never hold a worker borrow while evaluating a child or calling the native warning/session interface. Nested `ASCII(ASCII(...))` uses sequential borrows, not recursive borrowing.
4. **Lane binding:** `ScopedReadyValueColumns` binds the lane cache through `&self` without making it `Sync`. Failed re-entry is a structured contract error, not a `borrow_mut` panic or native fallback. An already-bound inner context keeps its original cache.
5. **Context forwarding:** `ScopedColumns` must delegate **every existing Columns method**, including overridden methods with defaults, to the original context. This includes context ID, parameters/current INSERT values, connection charset, TZ/clock, SQL/type flags, warnings/counts/bookmarks/drains, RNG/sequences, packet limits, session services and kill behavior. Forwarding only `get`, TZ and `append_warning` would silently alter semantics. Only the lane-local `ready_value_cache` capability hook is an intentional override. Keep forwarding coverage and review new `Columns` methods for delegation drift.
6. **Reset/drop:** the cache lifetime is the executor lane lifetime; dropping/rebuilding that lane drops only its workers. No borrowed input/context/row reference survives a call. Panic poisons that lane cache and is never caught and replayed natively.
7. **Clone:** `ReadyValueCache` is deliberately not `Clone` or `Sync`; a new parallel evaluation lane creates a distinct cache. Never clone a live RPN runtime by cloning its `Any` metadata or aliasing scratch.
8. **Bounds:** cap retained prepared entries and worker storage within each lane. Propagate an already-bound lane cache where appropriate rather than creating a per-row or per-expression cache. Do not grow a new worker per row or silently evict/recompile a fixed operation recipe in steady state.
9. **No semantic cache keys:** the initial finite key is the reviewed domain version, operation, canonical full ABI types, metadata-none and compile/build/resource policy. It is not argument bytes, a SQL value/result, statement warning state, NOW or a borrowed native node pointer. Any later compilation-sensitive key extension must be explicit.

### 6.4 Actual placement map

| Caller | Feasible owner placement | Required change / lifetime check |
| --- | --- | --- |
| Serial `EvaluatorSuite` | Execution-owned worker/owner alongside the suite, not `EvaluatorProgram` | Reuse across `run` calls; reset for a new execution; preserve ColumnSwapHelper behavior |
| Parallel projection | Execution-owned synchronized idle owner in `ParallelProjectionShared`/pipeline; task checks out a runtime and makes a borrowed scope | Keep the current fresh native suite per chunk unless its ownership cache is separately proven reusable. Recycle only the value runtime across tasks. `open/close` at X`projection.rs:342–390` delimit independently closable executions |
| Normal statement-driven helpers and DML | Opaque synchronized owner attached to the real statement/execution handle, plus a borrowed worker scope around hot loops | X`stmt_context.rs:301–316,1834–1845,4041–4050` provides the actual ownership/COW/ID seams. Do not use the ID as a global lookup. `StmtContextData` cloning must not accidentally clone/share a live executable across distinct statement executions |
| Generic executors using NoColumns/custom contexts | Explicit execution-owned worker/owner in the executor's operational state; bind a scope before its loops | A shared recipe/native meta is not the owner. May centralize an opaque owner in an execution support object, but do not hide unreviewed fields in all metadata. Listed consumer loops remain required loans/proofs; NoColumns does not excuse per-row compilation |
| Public one-shot AST/typed/helper APIs | Stack-owned execution scope when none is supplied; recursive calls reuse the borrowed active scope | Main-plan one-shot allowance. Repeated production callers must hoist the scope. Existing public APIs remain; no global/TLS convenience cache |
| SHOW AST filters | Owner outside `filter_show_output` and each independent SHOW row loop; borrow into `ShowRowResolver` | Current resolver is rebuilt per row and only implements `get`; S`show.rs:491–529,645–667` is a concrete mandatory loan, not covered by StmtContext |
| Partial-index maintenance | Write/index-build/scan operation owns the runtime and lends it to `IndexConditionContext` | X`kv_table.rs:423–442,3198–3230` currently receives row + timezone only. Extend the internal operation/helper seam to carry an explicit scope; never place live RPN in cloned/shared KvTable/index metadata |
| Folding and cached-plan rebuild | One fold/build/rebuild invocation owns a scope; preserve actual context and warning stash | D`constant_fold.rs:850–885` and P`physical_plan_cache.rs:310–362` are separate owners. The deferred evaluator callback is Send+Sync; it may capture a synchronized owner/recipe, not a raw runtime |
| Unistore | No ready-value runtime owner integration in this first domain | ASCII PB remains unsupported. A future PB release must use request lifetime, not masquerade as TypedRow/AST |

**Feasibility conclusion:** no TLS or Arc<non-Sync> is necessary. Execution-owned affine runtimes, leased through a synchronized idle owner where sharing is required, fit the existing `Executor: Send` and shared-program/context architecture. This does require caller lifecycle loans. Withholding those loans leaves a private prototype at zero; it is not permission to replace missing owner plumbing with per-row compilation or a native fast path.

## 7. Exact accounting and ordering

The fixed C4 contract uses its own EvaluatedAscii execution/entry identity and the existing exact retained-storage driver machinery, not ConservativeInt accounting or C3b lineage. Its ready input is a moved `Option<Vec<u8>>` borrowed as a scalar by the driver. **Do not add a Bytes vector, offsets/bitmap container or another transport copy just to follow EV-r1's earlier generic sketch.** C4 charges the moved Vec's capacity once throughout input/output overlap, and checks generated Int output before publication. The caller pool's distinct conservative reservations and actual owner/container observations are specified in §11.4.

Account explicitly for:

- C4's moved ready Vec capacity once (NULL has no payload); do not invent absent Bytes-vector offsets/bitmap/copies; any later transport owner needs separate review;
- runtime scratch/temporary vectors and the Int output;
- peak donor/replacement overlap and arithmetic overflow in size calculations;
- prepared program/metadata and idle-pool storage under the caller's separate retained-owner budget.

Existing C3b scratch limits do not automatically account for external native coercion allocations, immutable input storage, SQL FieldType allocations or an idle pool. The caller must bound/charge those explicitly; do not claim a total allocation cap from node/depth limits. No giant allocations are required for boundary tests: use small budgets and arithmetic-only extent checks.

A retained byte buffer cannot become free for budgeting purposes merely because it is empty. C4 drops invocation-owned ready/output buffers before healthy publication; its idle worker must not retain a transport buffer, operand or row reference. Any later retained-buffer/arena proposal would require a new observation and admission review, not an inferred permission from this document. Preserve only the fixed approved reusable program/context state; `ExecutionLimits` is an immutable policy and not a reusable allocation arena.

Selection stays owned by the current caller. Repeated selected occurrences remain repeated calls, in order. Empty selection evaluates neither children nor kernel and produces no warnings/RNG/host effects. Test 0/1/1024/1025 rows, sparse/reversed/duplicate selection and scalar constants with no physical input columns. Initial width-one operation is deliberately not a new batched child evaluator; compiling once does not permit result caching or hoisting side effects.

## 8. Proposed file loans — requests, not authorizations

No file in this section is granted to E for product changes. Parent assigns owners and serializes shared files; C's private ledger and D5's module lock remain authoritative. Names of new files are proposals pending owner coordination. D separately owns the source-only exact activation/deletion/error-renderer loan map; EV-r2 does not duplicate that work or release its files. D's parent-reported source check confirms the values dispatcher already has `&dyn Columns`, so a later context-preserving ASCII handoff does not require moving the whole dispatcher. That smaller concentrated change still cannot substitute for the complete caller-lifetime/entry audit.

| Loan set / owner boundary | Exact proposed paths | Intended cut |
| --- | --- | --- |
| C, **already separately released by parent** | K`local/compile.rs`, `local/batch.rs`, `local/mod.rs`, `types/expr_eval.rs`, `types/expr.rs`, `impl_string.rs` | Exactly C4's six files; body instrumentation only cfg(test); no new K module, registry/profile/runtime/Cargo expansion by E |
| **FIRST private TiDB loan requested**, designated adapter owner | **D`tikv/ready_value.rs`, D`tikv/ready_value_tests.rs` (two new files only)** | Concrete §11 adapter/pool/lease/forwarder using actual accepted C4 worker; no existing SQL dispatch, public capability or native-body edit |
| Parent-only first-cut compilation wiring | D`tikv/mod.rs` | Declare the new crate-visible private module/test inclusion after KV gate; adapter owner does not take this file |
| Separate additive capability cut, not in first loan | D`context.rs`, D`lib.rs`, D`tikv/mod.rs` as parent coordinates | Root-public opaque TiDB types before public Columns capability signatures; two default-None hooks and corresponding forwarder overrides; no concrete TiKV types leak; no dispatcher activation implied |
| Frontend activation and deletion | D`func.rs`, D`string_fn.rs`, D`scalar_function.rs`, D`expression.rs` | Preserve original scheduling/coercion; common ASCII dispatch; exact result-coerce position; no body left behind; one-shot scope propagation |
| Projection lifecycle | D`evaluator.rs`; X`projection.rs` | Serial reuse and explicit cross-task runtime ownership without sharing native ownership caches |
| Real statement / DML lifetime | X`stmt_context.rs`; X`driver/dml.rs`, `driver/multi_dml.rs`, `driver/dml/correlated.rs`; X`generated_column.rs`, `column_default.rs` as necessary to forward existing operation scopes | Statement/execution owner, scoped hot loops, COW/reset/reopen behavior; no row-state cache |
| Non-statement hot AST / index fronts | S`show.rs`; X`kv_table.rs`, `kv_table/index_entries.rs`, `admin_check.rs` | SHOW loop scope; lend a write/index-build/ADMIN CHECK operation-owned scope to partial-index context, not per-row creation; upstream write-loop scopes must reach these helpers |
| Other typed hot consumers | X`selection.rs`, `predicate_pushdown.rs`, `access_path.rs`, `joiner.rs`, `join.rs`, `sort.rs`, `shuffle.rs`, `hash_agg.rs`, `hash_agg/group_key.rs`, `hash_agg/input.rs`, `hash_agg/window_numeric.rs`, `hash_agg/window_extremum.rs`, `stream_agg.rs`, `vec_group_checker.rs`, `window.rs`, `union_scan.rs` | Forward supplied scopes; add actual operation/lane ownership only where the existing caller does not provide it. Do not blanket-edit all files before this per-caller check |
| Build/rebuild/pruning loops | D`constant_fold.rs`; P`physical_plan_cache.rs`, `logical/rewrite.rs`; X`partition_pruning.rs`, `access_cost.rs`, `driver/catalog.rs`, `driver/planner_bridge.rs` | Explicit invocation lifetime for repeated evaluation; preserve folding/deferred semantics |
| Existing expression tests | D`tests/mod.rs`, `tests/constant_test_go_tables_source.rs`, `tests/go_string_values.rs`, plus corresponding caller test modules | Retain source vectors, add route/origin/lifetime/metadata regressions; do not delete private-helper coverage |
| Negative PB/unistore gates | D`distsql_builtin.rs` test section; U`cophandler.rs` test section / existing tests | Assert unchanged refusal; no production signature or pushdown admission changes |
| Follow-on BIT_LENGTH only after ASCII gate | D`func.rs`, D`string_fn.rs`; separately reviewed named factory/domain/tests | Do not widen the named ASCII factory; delete its arithmetic only after its own resource/route gates |
| Follow-on LENGTH/OCTET_LENGTH | D`build.rs`, D`func.rs`, D`scalar_function.rs`, corresponding helper tests | Public BuiltStringLength lifecycle; alias count one; record separate CHAR_LENGTH residue |

K`impl_string.rs` and its existing selector in K`lib.rs` should not need an algorithm change for ASCII. Do not write them just to duplicate the kernel. Manifests, lockfiles, root plan, frozen M0 evidence, old `expression-reuse`, primitive/Decimal ownership and current D1–D5 product surfaces are outside this grant. If actual implementation requires an additional file or changes a maintenance-guide contract, obtain the corresponding loan first.

## 9. Proof matrix and acceptance evidence

All rows remain **required / unverified for the proposed C4/TiDB route** in EV-r2. AB-r1 supplies only EV-23's separate OLD-native baseline setup, not a new-worker or scoped comparison. Existing source tests and C3 trait receipts are not proof that the proposed route works. §11.7 adds the exact first-private-cut gates and identifies what must wait for later capability/activation loans.

| ID | Surface / counterexample | Required assertion |
| --- | --- | --- |
| EV-01 | Closed factory and old entrypoints | Only reviewed Ascii two-node Bytes→Int shape admitted; C3a/C3b/legacy refusals unchanged; no fake PB/control profile |
| EV-02 | AST, typed SQL, manual typed, private value helper | Same official RPN entry hit; correct distinct frontend behavior; no native first-byte code |
| EV-03 | All `coerce_str_bytes` datum classes | Original formatting/name/raw-byte/null/error behavior; no generic numeric/UTF8/BIT transport substitution |
| EV-04 | NULL, empty, NUL, raw ff, malformed UTF8, long payload | Correct bytes/Int result; NULL enters official nullable wrapper; own computed Int metadata even for NULL |
| EV-05 | UTF8, GBK/GB18030 front conversion, explicit to_binary | No omitted/double transcode; manual typed-without-child stays distinct; unsupported charset behavior unchanged |
| EV-06 | BinaryLiteral / BIT widths; ENUM/SET names; numeric/Decimal forms | Input representation retained through existing coercion; result identity never inherited from operand |
| EV-07 | Full FieldType and result-coerce | Normal flen3, complete flags/names/elements/ARRAY survive; manual unsigned/BIT/different-family results exactly match current native front; no PB reinterpretation added |
| EV-08 | Mutate/rebind native parameters/types/arguments between calls | Fresh frontend values/metadata; no stale native graph/return descriptor hidden in the fixed ABI cache; any retained snapshot alias test is RED before fix and GREEN after |
| EV-09 | Wrong arity, bad child cast, range sentinels | Preserve original error timing and earlier side effects/warnings; no claim of kernel execution for pre-kernel failure |
| EV-10 | IF/CASE/dead branch, repeated BENCHMARK, RNG/sequence/current INSERT/correlated child | Original demand and evaluation counts; no child evaluation under mutable kernel borrow; no result cache |
| EV-11 | Projection / selection / join / sort / group / aggregate / window | Test-only origin at official RPN invocation, value/metadata/order equality, no alternate numeric/vector/filter body |
| EV-12 | Fold / TryFold / default / generated / CHECK / DML / pruning / deferred rebuild | Source values and warning sequence/count/bookmark behavior preserved; scoped reuse in repeated callers |
| EV-13 | SHOW predicate and partial-index resolver | Real non-StmtContext hot routes hit official kernel and reuse owner across rows; row/TZ lookup unchanged |
| EV-14 | 0/1/1024/1025 rows; zero-column constants | Correct cardinality, empty no-op, compile/context counts not proportional to rows; no default-context churn |
| EV-15 | Sparse/reversed/duplicate selection | Preserve occurrence order, duplicates and first failure; no reading/coercing unselected bad rows |
| EV-16 | Exact budgets / capacity growth / overflow arithmetic | Charge C4 ready Vec capacity, actual Int/NULL storage, peak overlap and complete caller reservations/retirement; no absent Bytes-vector owners invented; small-budget and arithmetic-only tests |
| EV-17 | Serial cache and multi-chunk parallel projection | Cache hit across chunks despite fresh native suites; one mutable owner at a time; no Arc<non-Sync>, unsafe Sync, TLS or native/session capture |
| EV-18 | Parallel statements, COW clone, reset/reopen, close with in-flight lease | Owner/execution isolation; a closed execution's lease cannot populate a peer; Send/Sync trait assertions; dropped/failed worker does not leak or replay |
| EV-19 | Nested ASCII, error cleanup, low pool limit, failed preparation | No reentrant borrow panic, stale input, lock held during native callbacks, unchecked pool growth, or fallback/retry |
| EV-20 | Context forwarding | Every overridden Columns method remains observable through scope; non-default SQL/TZ/charset/warning policies and native identity preserved |
| EV-21 | PB / unistore candidate signatures | Existing ASCII/Length/BitLength refusals unchanged; genuine unsupported tests, not fake runtime coverage or expanded authorization |
| EV-22 | Deletion and feature audit | No ASCII arithmetic in AST/typed/helper/vector/PB/cophandler, feature-off backend, runtime toggle or renamed helper; only official TiKV implementation computes it |
| EV-23 | Performance | Cold preparation separate from warm throughput; compile/context/alloc/copy/pool-lock counts; direct helper, AST SHOW, typed serial/parallel and DML; compare end-to-end with parent baseline |
| EV-24 | Baseline/full regression and independent review | Parent-owned targeted + full existing gates, same separately recorded baseline failures, final call-graph/deletion review and actual-family decision |

Useful existing fixtures: D`tests/mod.rs::ascii_source_vectors_preserve_first_byte_and_string_coercion`, `bit_length_source_vectors_preserve_utf8_byte_count`, `length_and_octet_length_source_vectors_count_evaluated_bytes`; D`tests/go_string_values.rs::go_test_length_and_octet_length`; D`tests/constant_test_go_tables_source.rs::constant_folding_sees_through_internal_charset_transcodes`; K`impl_string.rs::tests::test_ascii` at `1919–1939`.

Origin counters belong at the official RPN invocation/wrapper boundary, not solely inside `ascii`'s non-null body. Keep separate counters for frontend rejection, preparation, runtime entry, non-null kernel body and caller return coercion. Invalid input rejected before invocation must not be mislabeled a TiKV SQL-kernel error. An admitted runtime failure must have no native replay; the pure ASCII arithmetic itself has no intrinsic SQL error formatter to port.

No numeric performance threshold is invented here: the parent must record the baseline, commands, dimensions, acceptance criterion and measurements. Width-one overhead, owned transport copies, pool synchronization, context-scope forwarding and allocation reuse are real risks. Optimize only the same official kernel path; a poor result cannot be repaired with the deleted native arithmetic.

## 10. Handoff and limits

This revision resolves a feasible ownership model and identifies the real extra callers/loans; it does not assert that the model has been implemented or that every source file above needs a change. C and the adapter owner must confirm the concrete API/type trait checks under separately released files. Parent chooses actual product loans, controls shared module wiring, builds/tests/perf, and updates the sole main plan and acceptance counts.

The new consumer enables a genuine family-kernel deletion while general Decimal/C3c expression closure proceeds. It does **not** claim M4's final replacement of the generic AST/typed evaluators, migrate argument-family algorithms, expand PB/remote support, complete CHAR_LENGTH, or constitute a whole Go-package transcreation claim.

**EV-r2 evidence:** source/contract/review reads and document self-review only. No new test/build/performance results are claimed. Parent's separately accepted AB-r1 OLD-native baseline and earlier generic Send receipts are not actual C4/caller-pool proof. The only file changed in this turn is `expression-unification/evidence/evaluated-value-contract.md`.

## 11. EV-r2: concrete closed-ASCII TiDB caller proposal

### 11.1 First-private-cut objective and ordering

The first implementation request is **exactly two new files**: D`tikv/ready_value.rs` and D`tikv/ready_value_tests.rs`. Parent owns their module/test wiring and waits for the C4 KV gate. No edit to `string_fn.rs`, `func.rs`, `scalar_function.rs`, `Columns`, root exports or Cargo is included. The prototype must link/use the **actual** `prepare_evaluated_ascii`/`EvaluatedAsciiWorker`, not pass tests only against a mock runtime.

One finite recipe, one factory, one worker type, one-slot ready input and one computed output: no registry of operations, trait-object backend factory, graph cache, eviction framework, generic runtime pool or alternate evaluator. Preparation/context creation is lazy at the first ready call; all prepared-worker metadata caches must be warm before that worker is published to a lease or idle slot.

The private adapter pipeline is:

```text
original native child evaluation and existing transcode/arity schedule
    -> original coerce_str_bytes (may fail before any worker admission)
    -> private ReadyAsciiBytes(Option<Vec<u8>>), fields/constructors sealed
    -> short checked scope state transition; owned lease leaves scope cell
    -> actual C4 worker.eval_one(ready), no native callback/context reaches KV
    -> C4 health/storage/epoch check; restore healthy lease or retire/drop
    -> inspect ComputedIntMetadata::OwnSignedInt and consume Option<Int>
    -> from_scalar(ScalarValueRef::Int(value.as_ref()), EvalType::Int,
                   ValueMetadata { kind: DatumKind::Int,
                                   string_collation: None,
                                   decimal_declared_shape: None })
    -> native computed Datum, including checked Int-identity NULL
    -> ORIGINAL caller's return coercion, outside runtime borrow/owner lock
```

`ReadyAsciiBytes` is not a raw SQL Datum, provenance proof or native FieldType. Its production private constructor calls the existing coercer on the frontend's already evaluated/transcoded value. A lower-level constructor from `Option<Vec<u8>>` is internal to the module for that handoff/tests, not public capability surface. `None` must call `eval_one(None)`; no caller NULL/empty arithmetic shortcut. An optional `AsciiConsumer::{AstValueOnly, TypedRow, ValueHelper}` is a private diagnostic/test description, never a factory selector or C3/PB/source-authentication assertion.

Concrete first-cut adapter signatures (proposal, not existing entrypoints):

```rust
pub(crate) fn evaluate_ascii_value(
    scope: &ReadyValueScope, value: &Datum,
) -> Result<Datum, ReadyValueBoundaryError>;

// Module-private pieces; no caller can forge a ready/computed wrapper.
fn coerce_ready(value: &Datum) -> Result<ReadyAsciiBytes, ReadyValueBoundaryError>;
fn eval_ready(
    scope: &ReadyValueScope, ready: ReadyAsciiBytes,
) -> Result<NativeComputedInt, ReadyValueBoundaryError>;
impl NativeComputedInt {
    fn into_datum(self) -> Result<Datum, ReadyValueBoundaryError>;
}
```

`evaluate_ascii_value` composes those three pieces, without child evaluation or return coercion. The private return wrapper has no public constructor or mutable metadata view. It must consume the actual C4 result, not construct `Datum::Int` by reading an operand. No native type/argument specialization is cached. `ComputedInt` and the concrete TiKV types never leave this module's public API.

**Return boundary remains two-layered:** the private prototype proves C4 computed identity and checked Datum materialization. It cannot claim to have exercised an integrated call through private `ScalarFunction::coerce_to_ret_type`, nor copy/expose that private routine merely to obtain such a test. During a later dispatcher loan, `ScalarFunction::eval` must continue to apply its existing full-FieldType coercion exactly once after the common adapter returns. AST/value-only paths keep their existing result behavior. EV-09/EV-10 full manual-return-descriptor regressions remain activation gates.

**Error boundary:** private `ReadyValueBoundaryError` distinguishes original frontend `EvalError`, original C4 `LocalError`, bridge metadata errors, pool resource/closed/poisoned errors and checked reentry failure. Keep the moved original engine error; simultaneous cleanup/health failure retires the worker without replacing that primary error. No text parsing, diagnostic formatter or native replay. D`context.rs::EvalError` currently derives Clone/PartialEq/Eq; K`LocalError` does not. The first cut must therefore NOT silently add a leaking LocalError variant or stringify the error to satisfy those derives. Public SQL activation requires a separately reviewed structured `EvalError` mapping that preserves the native error contract. Public capability methods below return no TiKV error.

### 11.2 TiDB-owned capability and lifetime API

These are **proposed TiDB signatures**, not present `Columns` methods. All fields are private. Public reachability is a later parent-owned additive export cut; the first private module can declare these concrete types without publishing them externally.

```rust
pub struct ReadyValuePoolPolicy { /* immutable TiDB integer limits only */ }
pub struct ReadyValuePoolOwner { /* Arc<PoolCore>; no direct worker access */ }
pub struct ReadyValueExecution { /* same root Arc + immutable checked epoch token */ }
pub struct ReadyValueScope { /* execution handle + local state + sticky poison */ }
pub struct ReadyValueOwnerError { /* opaque TiDB-only configuration/lifecycle error */ }

impl ReadyValuePoolPolicy {
    // No numeric defaults, generic op, TiKV type, SQL context or callback.
    pub fn checked(
        max_workers: usize, max_creating: usize, max_pool_bytes: usize,
        worker_retained_cap: usize, creation_reservation: usize,
        max_steps: u64, max_frame_depth: usize, max_call_retained_bytes: usize,
    ) -> Result<Self, ReadyValueOwnerError>;
}
impl ReadyValuePoolOwner {
    pub fn new(policy: ReadyValuePoolPolicy) -> Result<Self, ReadyValueOwnerError>;
    pub fn begin_execution(&self) -> Result<ReadyValueExecution, ReadyValueOwnerError>;
}
impl ReadyValueExecution {
    pub fn scope(&self) -> ReadyValueScope; // inert: no checkout/factory/context
    pub fn close(&self);              // idempotent for THIS epoch
}
impl ReadyValueScope {
    pub fn with_columns<R>(
        &self, native: &dyn Columns,
        body: impl FnOnce(&dyn Columns) -> R,
    ) -> R; // native callback stays in TiDB; never passed to the C4 factory/driver
}

// LATER additive changes to the existing public Columns trait:
fn ready_value_scope(&self) -> Option<&ReadyValueScope> { None }
fn ready_value_execution(&self) -> Option<&ReadyValueExecution> { None }
```

The private conversion builds `LocalCompileContext` with the fixed two-node/depth-two construction allowance and `ExecutionLimits` from the accepted primitive policy, with no Host allowance; it passes the distinct worker cap to the fixed C4 factory. No public policy field is a TiKV `LocalCompileContext`, `ExecutionLimits`, `EvalContext`, `LocalProgram`, `ComputedInt` or `WorkerStorage`.

`ReadyValuePoolOwner`/same-execution handles may clone the synchronized root. `ReadyValueExecution` cloning preserves its exact execution state; it does not begin a new execution. `ReadyValueScope`, `ReadyValueLease` and the actual C4 worker are **not Clone**. Intended traits: root/execution handles Send+Sync; worker/lease/scope Send and deliberately not Sync; the borrowing `ScopedReadyValueColumns<'native, 'scope>` stays local. No unsafe Send/Sync assertion, Arc of a raw worker, native context capture or TLS/global lookup. These traits require actual C4-cohort compilation, not only A's earlier generic CandidateParts receipt.

The runtime backing is a unique `Box<EvaluatedAsciiWorker>` moved between an idle slot and one affine lease. It is never aliased through Arc. The Box keeps its worker inline allocation at one location across the move; caller container/handle storage is charged separately.

**Resolve the public-type problem explicitly:** a public `Columns` method must not name a private/unreachable type from `tikv::ready_value`. Parent first reexports the opaque TiDB types through `tidb_expr`'s public root, then the separately loaned `context.rs` adds the two default-None methods. The private module can be crate-visible for parent wiring while its fields and TiKV internals stay sealed. Do not accept a `private_interfaces` warning as an API design, put TiKV types directly on Columns, or use `Any`/downcast to avoid the export decision.

The first two-file cut **does not edit that trait**. Its `ScopedReadyValueColumns` forwards every currently existing method and its tests invoke the private ready boundary with an explicit scope. Once the two hooks are separately released, the forwarder overrides only them. Dynamic capability discovery/recursive propagation is then tested as a distinct additive step; it cannot be claimed from the explicit-scope prototype alone.

`with_columns` is an unwind-guarded lexical binding, not a second evaluator. In the later capability-aware version an already active scope exposed by `native` takes precedence; use it rather than acquiring another lease for nesting or silently switching execution. Otherwise bind this scope. The wrapper's execution capability is the active scope's independent execution, not an unrelated base owner. Binding itself performs no pool admission. A closed execution refuses at the demanded ready call, not by treating absence/staleness as permission for a fresh implicit execution.

No-scope-but-present-execution permits a short scope for a genuinely separate public call. Hot loops must hoist `with_columns`/scope outside their loop. A no-capability one-shot entry eventually needs a parent-approved explicit standalone policy/owner path, created only when demanded; the first private cut invents no default limits or per-row global cache. Absence never authorizes native ASCII arithmetic.

### 11.3 Shared root, independent executions, close and affine state

`PoolCore` contains one immutable policy and synchronized finite slot/byte ledger. `begin_execution` allocates a checked monotonic execution ID plus a private `Arc<ExecutionState { id, closed }>`; there is no root-global current epoch or implicit invalidation. No per-expression map or thread-ID map is added.

- `begin_execution` issues a fresh independently closable execution without closing or retiring another live/detached execution. **The same root ledger survives** and continues charging every leased/creating/retiring worker. Execution-ID overflow fails closed rather than wrapping. Policy replacement is not a first-cut API; a new accounting root requires old-root quiescence or a separately owned aggregate budget.
- `execution.close()` atomically closes only its own state. It rejects new ready admissions for that execution, detaches only its idle workers for retirement, and leaves its in-flight/creating debt charged until actual disposal. Peer executions remain live. Dropping a parallel receiver/handle is not close.
- A lease carries a private root/execution/token/slot identity. Its worker and immutable reservation belong to exactly one slot and may be reused only by that same execution. Creation checks its execution state again before publication; a same-execution close during factory/prewarm retires the result without publishing it, while beginning a peer does not interfere.
- A ready invocation checks its own execution before entering C4 and again before publishing successful computed output. The checked post-call observation is the publication linearization point: if its close wins, discard the result and retire; if publication wins, the outer executor still owns its normal cancellation/output-publication rule. An original engine error remains primary even when close races with cleanup.
- Use each execution's atomic closed observation for hot-path checks, with publication/close ordering specified and tested. Pool mutex work is limited to checkout/reserve/return/retire bookkeeping; the fixed worker reservation stays charged during use, so no per-character pool-lock cycle is required merely to observe unchanged storage. No mutex is held through native work or C4 preparation/evaluation/destruction.
- Reset/reopen calls close/begin explicitly. Same-statement handle clone and configuration COW retain the same execution capability; a new statement gets another execution on the shared accounting root. It neither reuses a peer's worker nor invalidates that peer.

Scope state is a closed local enum such as `Dormant | Leased(ReadyValueLease) | Busy | Poisoned`, plus a sticky poison bit. At first ready demand, reserve/check out; then use checked short cell access to **move the lease out**, mark Busy and release the cell borrow before calling C4. An invocation guard owns the lease while executing. On normal completion, validate worker health/storage and its execution state before restoring it. No runtime/cell borrow survives into native result coercion. Busy/reentry yields a structured contract error, not `borrow_mut` panic or another lease. An already-poisoned scope never silently repairs itself by preparing a replacement; an explicit new scope is a caller decision after handling the failed operation.

### 11.4 Two pool observations: actual footprint and conservative reservations

Do not equate an idle cache's length with all live memory. Use a finite slot slab, containing at most `max_workers` records, with states Empty/Creating/Leased/Idle/Retiring; a worker is boxed only once. The first policy is fixed for the root. No hot eviction, resizing or compaction framework is needed.

The root's **actual owned-storage observation** includes the synchronized control allocation, actual slot-container capacity (not length), per-record/Box-handle storage, and each existing C4 worker's `WorkerStorage.total_bytes()`. The latter already includes the worker header: when the worker is boxed, count that body through `inline_bytes` once, not again through the caller's container calculation. Do not count only `owned_heap_bytes`, omit the Box body, or charge the same header twice. The observation also includes any detached container/worker storage still pending destruction.

`Arc<PoolCore>` introduces a caller control allocation. A's C4 config-Arc measurement does **not** automatically validate this different payload's layout/alignment. Adopt the same explicitly pinned allocation-layout convention only with a caller-specific source/layout check and parent measurement for the actual PoolCore shape; otherwise keep that accounting gate open. No claim that `size_of::<Arc<_>>()` or just `size_of::<PoolCore>()` covers the allocation. Allocator-private usable slack remains the same explicitly declared exclusion as C4; no portable total-allocation claim follows.

In addition, use a **conservative reservation ledger**, not mutable cached cold-worker sizes:

```text
n_live + n_idle + n_creating + n_retiring <= max_workers
n_creating <= max_creating <= max_workers

reserved_bytes = control_and_container_charge
               + (n_live + n_idle + n_retiring) * W
               + n_creating * F
reserved_bytes <= max_pool_bytes

W = immutable worker_retained_cap
F = explicit creation_reservation, at least W plus any separately reserved
    caller/factory construction allowance required by the approved accounting
```

All arithmetic, slot/generation IDs and comparisons are checked before mutation. Static policy validation and finite control/slab allocation belong to owner construction/configuration; they perform no C4 preparation. Policies can allow zero available worker/creating slots for demand tests. Scope binding remains inert. Resource admission for a ready-value runtime occurs only after its original frontend coercion has completed.

A Creating slot and the **full F** are committed under the mutex **before** the real factory/Box allocation/prewarm is invoked outside the lock. A second thread cannot see the same free slot or uncharged creation. Cap creation explicitly even if remaining byte budget is large. Publication requires healthy C4 state, nonmutating observed `total_bytes <= W`, completed cache prewarm and matching live execution; only then replace F with W. Temporary creation owners must already be gone before the difference is released. Factory error/panic and close-during-create hold the reservation until partial/new objects have actually dropped. No retry or indefinite wait for a lease held by the caller itself; exhaustion is the demanded call's structured resource error.

W remains reserved while a worker is live **or idle**, even when its observed footprint is smaller. This deliberately trades utilization for simple safe hot-loop reuse. Continue checking actual storage on return and after invocations; never trust an initial observation. A larger/failed observation or unhealthy worker cannot reenter idle. Unmeasurable dirty warning payloads are disposed of immediately; do not hide them behind a reusable poison flag. If storage cannot be certified within W, mark **uncertain retirement debt** and freeze new checkout/creation/publication on that root until actual disposal clears it. W is then only a last certified reservation, not a fabricated measurement of the dirty payload. Epoch rotation must not bypass this freeze.

Retirement is a three-step operation: **mark/detach as Retiring under lock → drop outside the lock → acknowledge release under lock**. A Retiring slot and its byte charge remain unavailable until that final acknowledgement. Close, panic and epoch rotation follow the same path. Do not return credits before destruction or move still-live workers into an uncharged temporary Vec. A fixed slab permits per-slot detachment without allocating a retirement container; if any container is replaced later, its old capacity remains explicit retirement debt until freed. Mutex poisoning/bookkeeping inconsistency permanently refuses reuse; recovery may drain/drop, not quietly resume with uncertain counters.

The ledger bounds assigned **reservations and healthy retained ownership**. The fixed C4 ABI does not report factory temporary high-water allocation, native coercion allocations, arbitrary caller handle/stack storage, or process-wide memory. F therefore needs a separate finite factory/construction-envelope source or allocation receipt before anyone calls it a hard construction-peak bound. Node/depth bounds and the final `retained_storage` value alone are insufficient. Missing that proof is not permission to widen C4 or pretend it exists. Invocation-owned ready capacity/output overlap is C4's separate per-call budget; native coercion precedes it and must not be silently claimed bounded by the pool.

### 11.5 Sticky unwind guards and native recovery placement

There are **two** complementary guards, neither a new panic recovery policy:

1. The invocation guard owns the removed lease while C4 runs. It arms poison before the call and disarms only after normal C4 cleanup, both health predicates, storage validation and caller epoch observation. On unwind, retire/drop before the panic reaches an outer catcher. A worker left in C4's in-flight/unhealthy state is never reused.
2. `with_columns` creates a lexical **native-operation guard** covering child evaluation, existing coercion, kernel handoff and native return processing. Normal return, including a normal `Err`, disarms it after cleanup. Unwind makes the scope sticky-poisoned and retires any parked lease **while unwinding**, even if the worker had completed successfully before a later native return-coercion panic.

`thread::panicking()` in a late Scope/Lease Drop is only a defensive supplement. It does not detect a panic already recovered while that scope survives. The mandatory placement for a surviving scope is:

```text
recover_worker_panic(|| {
    scope.with_columns(&real_context, |scoped| {
        existing_native_work(scoped)  // includes its original result handling
    })
})
```

The guard is **inside** the catcher; the scope itself may live outside. Alternatively create/drop the scope inside the existing recovery closure. Do not wrap `recover_worker_panic(...)` inside an otherwise normally returning `with_columns` body and assume the outer guard saw the unwind. Any inner catch that swallows a panic before the active guard unwinds must carry an explicit poison/disposal notification or put the guard inside that catch's protected operation. Do not add a catch-and-native-replay branch or rely on AssertUnwindSafe in product code to make ownership safe.

Concrete source consequence: X`projection.rs:298–304` already constructs each suite **inside** `recover_worker_panic`; the later scope/guard must be placed there. X`projection.rs:345,389` merely discards parallel state today; the later loan must explicitly close the shared execution epoch before dropping that handle, so captured task Arcs cannot reopen it. Serial paths and other recovering workers need their own exact boundary review, not an assumption based on thread exit.

A normal C4 error can leave a healthy reusable worker only if C4 certifies cleanup. Healthy reuse requires **warning count zero AND detail vector empty**; zero detail capacity is not evidence of no warnings. The caller cannot inspect or reset the sealed context. It relies on accepted C4 health/observation gates, then disposes on failure before continuing native work. No per-row warning drain/reset, no warning merge, no per-row default context creation.

### 11.6 Complete current Columns forwarding contract

The wrapper delegates the method itself to the original object, not a reimplementation of its default body. At this source checkpoint the existing trait has **63 methods**. The mechanical method-name inventory is:

```text
get context_id use_plan_cache skip_plan_cache_for_comparison
 enable_vectorized_expression param_value current_insert_value get_param_value
 bounded_staleness_safe_time connection_charset_info no_unsigned_subtraction now
 cast_time_to_year_through_concat sysdate_is_now current_database current_user
 login_user current_role current_resource_group connection_id tidb_decode_key
 acquire_advisory_lock advisory_lock_owner release_advisory_lock release_all_advisory_locks
 found_rows current_tso ddl_owner_info sysvar tidb_info block_encryption_mode
 division_by_zero_level truncate_level type_flags strict_sql_mode handle_truncate
 handle_group_concat_cut handle_sleep_incorrect_argument sleep_for append_warning
 append_note warning_count truncate_warnings take_warnings_since max_allowed_packet
 handle_allowed_packet_overflowed date_modes handle_division_by_zero get_uservar
 set_uservar row_count last_insert_id set_last_insert_id time_zone like_default_escape
 default_week_format windowing_use_high_precision div_precision_increment rand_next
 rand_seeded_next sequence_nextval sequence_lastval sequence_setval
```

Only the two future ASCII capability methods are intentional overrides. Before adding any other method to Columns, update forwarding and its method-set test. A small test can mechanically compare this inventory/implemented forwarding names with the current `context.rs` trait method declarations (no parser dependency or generic context framework); this is a drift sentinel, not a substitute for behavioral tests.

Behavioral sentinel context must override **all** methods, including policy methods with defaults, and verify argument identity/order plus returned values/errors/mutations through both bare and nested scoped wrappers. Include nondefault timezone/clock/charset, unsigned/type/date/strict flags, parameters and current INSERT, RNG seed key, sequences, user variables, lock service calls, packet policy, warning/note counts, bookmarks/truncation/drains, and the sleep/kill return. Forwarding `get`/TZ/append_warning alone is not acceptable. No runtime/pool borrow or mutex may be held during any of these native callbacks.

### 11.7 Exact private proof cut and later stages

All following gates are **proposed / unexecuted**. First-cut tests must prepare actual C4 workers and call the real ready facade; a ledger-only fake is not sufficient.

| Gate | First-private-cut requirement | Boundary / later obligation |
| --- | --- | --- |
| PV-01 private opaque types/traits | Compile the proposed opaque types with real C4 worker; intended Send/Sync/non-Clone boundaries, no unsafe traits or exposed KV type | Root-public reexport/reachability and Columns hooks require the separate parent cut |
| PV-02 ready semantics | Original coercer: NULL, empty, 1/64/4096, raw ff/NUL, numeric2; actual C4 outputs and own Int metadata via from_scalar, including NULL | No claim of public AST/typed dispatch migration or every original datum class |
| PV-03 actual invocation | Before/after `kernel_invocations()` from the actual worker: successful ready NULL and non-NULL each add one; pre-dispatch refusal adds none; record successful/attempted factory creation separately | C4's native isolated wrapper/non-NULL-body and fn_ptr-site tests remain required; adapter-call counters cannot substitute |
| PV-04 reuse/prewarm | Multiple ready values on one lease and repeated scopes on one root/epoch; one actual factory/context per created worker, no stale bytes, nonmutating stable cold-published/warm storage | No warm caller-program/FieldType cache assumption; KV gate must prove prewarm before publication |
| PV-05 full scope order | Native child/coercion before checkout, no lease on skipped/coercion-error paths, nested ready calls use sequential ownership, output materialization/native callbacks after Busy ends | Integrated dead-branch/selection/full-ret-type tests await dispatch loan; no mocked whole-tree proof |
| PV-06 concurrency admission | Real workers with tiny slot/byte budgets; barrier after committed creating reservation but before factory; concurrent miss cannot exceed max_creating/total slots/F cap | A cfg(test) barrier is not a replacement backend; real factory runs after release |
| PV-07 close/retirement | Close/reset while creating/live; closing one handle cannot close a peer execution; a closed execution's lease cannot populate a peer; barrier before actual drop retains slot/byte debt | Actual executor/statement close/COW/reopen wiring remains a later hot-entry gate |
| PV-08 caught panic | Actual worker first used; then panic in guarded native work before kernel, after kernel, and in return-processing stand-in; catcher outside guard while scope survives; worker dropped and scope sticky-poisoned before next demand | Driver-internal poison/zero-count-vs-details cases require C4's own private tests; TiDB adds no context getter or production panic hook |
| PV-09 clean/error disposal | Actual low-budget C4 refusal, wrong/stale epoch, checked reentry and observer failure handling; preserve primary error; no native retry | Any synthetic corruption test is labelled structural only; no fake dirty-worker result passed off as C4 health proof |
| PV-10 full forwarding | All 63 methods' sentinel behavior plus drift check; no native callback with runtime/bookkeeping lock held | After additive hooks: active-scope precedence, recursive propagation, owner-only entry and default-None behavior |
| PV-11 byte accounting | Actual worker observations, Box-header once, real container capacity, pinned caller Arc charge, creation/final-observation differences, old-epoch retirement debt, arithmetic-only overflow tests | F's transient peak and allocator-private exclusions explicitly separate; A's config-only Arc result is not sufficient for PoolCore |
| PV-12 unchanged domains | New private module only; old public ASCII routes and baseline source/binary untouched; old C3 entries and PB/unistore admission unchanged | Later native deletion/whole-family proof still mandatory |

Proposed test-only rendezvous points may pause after reservation, after actual preparation and before disposal to make races deterministic. They must not let product callers install a factory, swap the worker, supply a native callback to KV, or fake a `ComputedInt`. Keep any fault injection under cfg(test) and label structural versus actual-worker evidence. No new allocator framework/global allocator is included in the two-file loan; parent coordinates independent accounting receipts.

**Stage separation:**

1. **KV gate:** parent validates the released six-file C4 implementation, mandatory prewarm, actual fn_ptr witness, health, storage/Arc receipt and unchanged native suites/layout.
2. **First private TiDB gate:** parent may release only the two files above and own minimal module wiring. Execute PV-01–PV-12 to their explicitly private extent with real workers. This is not SQL activation or public-capability propagation.
3. **Additive capability/error gate:** parent reexports the opaque TiDB types; separately loan two Columns hooks/forwarder overrides and the reviewed structured native error seam. No private type leakage or fake generic capability. Prove actual dynamic propagation before calling it complete.
4. **Full hot-entry integration + activation/deletion stage:** explicitly loan the entire required set from §2/§6.4, not just one helper or EvaluatorSuite. Preserve scalar/vector/filter ordering, show-row and partial-index ownership, parallel/caught-panic/close/COW semantics and all original return coercion. Delete the native ASCII computation only with synchronized real activation, route/origin proof and performance gates.

The first-cut success cannot abbreviate stage 4. In particular: serial and parallel projection (fresh suite per chunk); selection/filter; join/hash/merge/semi/outer residuals; aggregation/window/group/sort/shuffle; generated/default/check/DML/insert/update/index and partial-index/admin-check paths; constant/folding/TryFold/ranger/planner and cached-plan deferred callbacks; session SHOW/SET/DO and custom/NoColumns public helpers all remain in the preserved inventory. Some paths need propagation only rather than their own pool, but each needs a reviewed lifetime/forwarding decision. PB/unistore ASCII remains unsupported, not an excuse to fabricate executed coverage.

No performance threshold, warmed-scope receipt, release throughput, allocation count, native deletion or whole-family credit follows from this proposal. Compare later public and explicit-scope measurements separately with immutable AB-r1, and retain all its artifact/profile/checksum/kind limitations.
