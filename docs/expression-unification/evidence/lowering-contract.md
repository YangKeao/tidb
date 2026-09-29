# Caller D — TiDB lowering, ownership, and entrypoint handoff

Date: 2026-09-28. Revision D-r23. **Round5 adds READ-ONLY ASCII activation/deletion readiness and C4-to-native structured-error feasibility below. This is receipt-only work, not an implementation loan or public activation. Parent preserves existing fold/default suppression and approves the narrow future error policy: six outer-discriminant native classes and their exact fixed messages, existing1105/HY000 + Eval from_evaluation path, native-only opaque Debug, internally retained raw cause and same-cause identity Eq. Final Rust API/wiring and all product write authority remain separately ungranted; E's current two-private-file cut does not edit context.** **Parent actually executed import-corrected D6: focused18/18 (1389 filtered), full caller1310 passed/4 failed/93 ignored (1407 discovered). Parent reports all four complete failure blocks identical to C3c/I after thread-ID normalization ONLY. The four product files remain frozen; independent read-only eligibility/ownership/retained-accounting review is still in flight and may find gaps not covered by these tests. D ran no builds/tests. This is tested private-checkpoint evidence, not final review acceptance, public activation or a green full suite.** **Parent accepted D5's private Datum-control checkpoint: actual18/18,1371 filtered; full caller1292 passed/4 unchanged baseline failures/93 ignored (1389 discovered). All five D5 files are frozen with ownership returned. Prerequisites remain compiled C3b575/575+aggregate40 and observer8/8. No general ordinary composition, D4 diagnostic composition, PB/public route or family credit follows.** **Parent accepted D4's private exact203 overflow view:10/10, old D3 10/D1 16/D2 8, full1274 passed/4 unchanged baseline failures/93 ignored (1371 discovered), against native353/537/aggregate40. D4 diagnostic seams remain unchanged; only scalar_function.rs's numeric eligibility/worker block received the later narrow D6 reloan. No general SQL diagnostics, warning sites, context/severity handoff, public activation or family completion is claimed.** D ran no builds/tests; parent execution is distinguished below. This is not a green full suite or a generic metadata/profile/family-completion claim. The only authoritative ExecPlan is `/home/agent/tidb/EXPRESSION_UNIFICATION_PLAN.md`, whose writer remains the parent. This is not another ExecPlan or a package-transcreation receipt.

## D1 acceptance amendment — private seed, not activation

Parent explicitly released six new private boundary files plus narrow metadata/accessor/propagation edits in `distsql_builtin.rs`, `constant.rs`, `column.rs`, `scalar_function.rs`, and pure conditional facts in `pushdown_catalog.rs`. After C2a/B2.1 and caller compilation, parent accepted **15/15 seed tests** and the full expression-library comparison with **no new failing tests** (details below). D keeps all 11 D1 product files frozen; parent subsequently released the independent metadata regression and its focused origin fix to E, as described below. No general evaluator hook, arithmetic/deferred/Host/mixed-type expansion or family-completion claim was authorized. The D-r1 audit below remains historical design evidence: its earlier “proposed/not present/not authorized” status is superseded **only for the exact D1 artifacts in this amendment**.

### Actual source and API

Product paths here are under `expression-unification/tidb/rust/crates/tidb-expr/src/`:

- `tikv/mod.rs`: private module, explicit crate-private exports `lower_int_control_seed`, opaque `LoweredSpec`, `PreparedIntControlSeed`. `SeedError::{Admission,Bridge,Local}` distinguishes admission from the intact typed LocalError. Scoped dead-code/import allowances document the intentionally inactive private entrypoint.
- `tikv/catalog.rs`: exact PB signature number through the generated rust-protobuf enum conversion. SQL conditionals use the factored `pushdown_catalog::conditional_signature(name,arity,actual_result_eval_type)`; AND/OR reuse existing catalog rows. The finite control/arity check is admission, never another ID-to-kernel map. No serializer/value-binding function is called. PlusInt203 is rejected, not changed to222; opaque VALUES/grouping metadata is rejected.
- `tikv/lower.rs`: `lower_int_control_seed(&Expression,&[FieldType],new_collation:bool,CompileLimits) -> Result<Arc<LoweredSpec>,SeedError>`. Iterative bounded construction, no recursive LocalExpr clone/debug. Captures detached full SQL node/row/output types, initialized coercibility, repertoire, explicit charset, column/literal source facts and original PB data. Produces only Int/typed-Int-NULL constants, demanded input slots and admitted control calls. No native expression or cache survives in the spec. Only referenced input slots are projected; unrelated row values are not eagerly imported.
- `tikv/context.rs`: actual frozen `LocalRuntimeServices::{binding_schema,read_input}` with `InputRow {occurrence,input_row}`. Preflights full declared schema equality, column count, every physical column's row count/type width and selection universe. Reads only demanded `Chunk::physical_row` cells, validates projected type/occurrence, and returns exactly one Int through B's checked bridge. No Columns/Session evaluator, diagnostic sink, host task or TLS guard is stored.
- `tikv/batch.rs`: `PreparedIntControlSeed::compile(Arc<LoweredSpec>,ExecutionLimits)`, `eval_selected(&mut self,&mut EvalContext,&Chunk,&[FieldType],&[usize]) -> Result<Vec<Datum>,SeedError>` and `eval_one(...,physical_row) -> Result<Datum,SeedError>`. Each worker owns LocalProgram/LocalEvalState. Both call **C2a `LocalProgram::eval_with_bindings`** and reconstruct signed Int/NULL through B's `from_scalar`. Mutable context/program/state and borrowed inputs are disjoint. The test-only poisoned-service seam is not another production route.
- `tikv/tests.rs`:15 source tests, listed below; not executed by D.

D reread actual C2a `KV/components/tidb_query_expr/src/local/{mod,runtime,batch,spec,registry}.rs`. No guessed service names remain in D1. The seed does not imply ordinary arithmetic NULL-stop, HostCall, generic row evaluation or a full diagnostics journal.

### PB retention and transformation rule

`distsql_builtin::PbOrigin` is shallow: raw optional expr kind/signature, optional complete prost FieldType, optional current-node `val` bytes, child count, and detached **effective SQL type at ingestion**. No Expr/children are stored. The actual `pb_to_expr` ingress wrapper attaches it to each decoded node, including recursive children; existing effective-type/default behavior is untouched. Constant/Column/ScalarFunction retain optional Arcs through their existing clones and expose read-only accessors. ScalarFunction exposes a narrow VALUES-offset-presence accessor for rejecting opaque metadata.

Lowering validates current effective type against the captured baseline, exact signature/arity, encoded literal value/column offset, required original type and origins beneath PB parents. Stale/missing/incompatible origin is an explicit admission error, never guessed reserialization. Display names cannot change PB execution. Original wire FieldType and the scan's effective SQL type both survive. Original wire arrays/unsigned/unknown collation, missing required wire type, SQL Tiny/untyped NULL, params/deferred/correlated/virtual-expression columns and mixed carriers are outside D1. PB NULL decoding still yields its existing SQL Null type and is rejected rather than retagged LongLong.

Conditional factoring preserves the old remote guard and first-branch approximation; adding pure COALESCE identity facts does **not** add a remote CATALOG row. Native computation/evaluator entrypoints were not switched or deleted. **Complete evaluator-family count remains zero.**

### Parent-run acceptance tests; D reread logs

`tikv::tests::{sql_bigint_controls_use_local_rpn,demanded_input_only_and_local_error_retained,all_int_controls_have_strict_demand,physical_rows_and_occurrences,schema_and_layout_rejected_before_reads,pb_identity_and_wire_metadata_survive_lowering,pb_literal_transport_is_exact_and_stale_values_are_rejected,signature_factoring_keeps_remote_admission_separate,resource_error_is_not_replayed_or_stringified,pb_stale_or_missing_origin_is_not_guessed,control_tree_admission_is_closed,full_metadata_is_detached_and_collation_state_is_retained,lossy_metadata_and_unsigned_wire_are_not_normalized,immutable_spec_has_independent_worker_programs,deep_seed_prepare_eval_drop_and_limits}`.

They use real SQL parser/resolver/rewrite for BIGINT IF/nested controls; actual PB ingest; explicit typed control fixtures; poisoned/recorded demanded reads; 0/1/1024/1025 rows and repeated/nonidentity selections; schema/layout mismatches; i64 extrema; full metadata detachment and stale-origin rejection; independent workers; and depth33/64/256 limits/drop on a256KiB worker stack. Parent's `logs/tidb-d1-control-seed-v2.log:1953–1970` records all 15 passing. D's correction before that retry was limited to test fixtures: required `ColumnResolver::time_zone()` and the actual `tidb_codec::encode_int(&mut Vec<u8>,i64)` API. No fixture value or expected result was changed. The depth test dismantles native Expression separately and iteratively, so native recursive Drop does not masquerade as local-spec coverage.

Parent's full `--lib` comparison, `logs/tidb-d1-expr-full-comparison.log:3296–3328`, reports **1245 passed / 4 failed / 93 ignored**. D compared it with `logs/tidb-expr-lib-baseline.log:2078–2110` (**1226 passed / the same 4 failed / 93 ignored**): IFNULL reversed string column/literal admission, EXP FloatOverflow, negative Duration FSP, and partial STR_TO_DATE. Failure names and assertion/panic content match (thread IDs/source offsets differ). This is acceptance against the recorded baseline, **not a green full-suite claim**, and the pass-count delta includes other owners' work.

**Subsequent E review, after that acceptance:** E found an immutable-origin gap in the then-accepted D1 source: an `Arc<PbOrigin>` retained by `LoweredSpec::wire_origin` shared `effective_type` whose GoSharedSlice-backed `elems` could be mutated through a consumer's FieldType clone. This affects the documented metadata guarantee even for admitted LongLong-with-elems; no wrong Int result has been reported. E owns the new `pb_origin_metadata_cannot_mutate_a_published_spec_or_relowering` regression in `tikv/tests.rs`. Parent has now established RED in `logs/tidb-d1-pb-origin-alias-red-v2.log:1986–2003`:0 passed/1 failed, actual `(elems="changed", matches=false)` versus expected `(elems="one", matches=true)`. Parent released E's focused private-effective-type/matches/test-snapshot fix and then observed exact-regression GREEN (`tidb-d1-pb-origin-alias-green.log:1987–1990`) and the enlarged D1 cohort16/16 (`tidb-d1-post-origin-fix.log:1986–2004`). The full post-fix comparison was subsequently accepted as recorded in D2 below. D has not changed D1. The historical 15/15 result never proved this newly discovered metadata edge.

### Limits and parent handoff

- **Trusted native Chunk boundary:** preflight checks metadata/layout available from current safe APIs, not arbitrary corruption of private raw buffers. Parent acknowledged this. Identical8-byte Double and LongLong buffers cannot be distinguished if the owner lies about full SQL schema; that declaration remains a binding contract. No Chunk file/API was edited or invented.
- Signed controls need no native cast/host/diagnostic renderer, so no general native error enum change was needed. TiKV LocalErrors propagate intact. Full severity/note/bookmark/source-site/publication work, params/correlated inputs, Decimal/NaN/temporal/JSON/mixed provenance and C2b remain follow-ups.
- The lowerer is non-evaluating, but receives an already bound/inferred Expression. Existing SQL rewriting/folding is unchanged; this does not implement general StructuralOnly AST preparation.
- Parent owns private `mod tikv;` in lib.rs, all Cargo/locks/shared roots, guides, registry and plan. Parent reports wiring plus `protobuf.workspace=true` (workspace2.8.0), alongside already present tidb_query_datatype/tidb_query_expr/tipb. Parent reports successful offline/locked metadata checks and identical normalized dependency lock apart from that edge; D did not run those checks. No further dependency/file lock is requested for D1.
- Parent accepted the original seed/comparison above; the later E issue has independently observed RED→GREEN and a16/16 post-fix seed cohort. D ran **no cargo/build/test/lint/commit/fixture generator** and does not claim independent execution or repository readiness. D2-min changes only its four separately released files plus this receipt; no D1 product file was edited by D.

### D1 checks actually run

From `/home/agent/tidb/expression-unification/tidb`: `pwd`, narrow rustfmt, scoped diff review, line-numbered reads and static grep. Negative grep over `src/tikv` for `eval_in|eval_func|from_expression|to_pb|try_tikv|enter_cop_eval|unsafe|Any.*Sync|\.eval\(|\.get_row\(|Box<dyn|FnMut|FnOnce` returned zero matches. Positive review found official compile/eval_with_bindings/physical_row and only shallow field-type/metadata clones, not recursive LocalExpr clones.

Exact final formatter/whitespace commands (the first three were chained with `&&` and passed; scoped diff review also returned successfully):

```sh
rustfmt --edition 2021 --config skip_children=true rust/crates/tidb-expr/src/tikv/mod.rs rust/crates/tidb-expr/src/tikv/catalog.rs rust/crates/tidb-expr/src/tikv/lower.rs rust/crates/tidb-expr/src/tikv/context.rs rust/crates/tidb-expr/src/tikv/batch.rs rust/crates/tidb-expr/src/tikv/tests.rs
rustfmt --edition 2021 --check --config skip_children=true rust/crates/tidb-expr/src/tikv/mod.rs rust/crates/tidb-expr/src/tikv/catalog.rs rust/crates/tidb-expr/src/tikv/lower.rs rust/crates/tidb-expr/src/tikv/context.rs rust/crates/tidb-expr/src/tikv/batch.rs rust/crates/tidb-expr/src/tikv/tests.rs rust/crates/tidb-expr/src/distsql_builtin.rs rust/crates/tidb-expr/src/constant.rs rust/crates/tidb-expr/src/column.rs rust/crates/tidb-expr/src/scalar_function.rs
git diff --check -- rust/crates/tidb-expr/src/distsql_builtin.rs rust/crates/tidb-expr/src/constant.rs rust/crates/tidb-expr/src/column.rs rust/crates/tidb-expr/src/scalar_function.rs rust/crates/tidb-expr/src/pushdown_catalog.rs rust/crates/tidb-expr/src/tikv
git diff -- rust/crates/tidb-expr/src/distsql_builtin.rs rust/crates/tidb-expr/src/constant.rs rust/crates/tidb-expr/src/column.rs rust/crates/tidb-expr/src/scalar_function.rs rust/crates/tidb-expr/src/pushdown_catalog.rs
```

Earlier `rustfmt --check` over the five existing files exited1: new PB capture calls needed wrapping (fixed) plus one **pre-existing unchanged** `let required = signature.arg_type_at(...)` wrap in pushdown_catalog (currently2747). Scoped git diff established that old line is outside D's patch; it remains untouched with parent acknowledgment. This is not a full pushdown_catalog formatter pass or repository lint pass. After rustfmt, a stale observation required rereading tests.rs before editing; the receipt likewise required a fresh read before amendment. No sandbox denial was bypassed. No background build was started.

## D2-min implementation checkpoint — four approved files, not activation

Parent reviewed D-r3 and explicitly released **only** `rewriter.rs`, `new_function.rs`, new `rewriter/preparation.rs`, new `rewriter/preparation_tests.rs`, plus this receipt. Those four product files are now implemented at a coherent source checkpoint. No D1/TiKV/Cargo/lock/lib.rs/public evaluator file was written. **D ran no compile, test, lint, benchmark or fixture generator.** Parent's accepted cohort is recorded below; no public route or fallback was released. Migrated-family count stays zero.

### Parent gate amendment — D2-min accepted, not broad activation

After the documented two-call compiler correction and E's independent origin fix, parent ran and accepted the complete requested comparison cohort. D reread the following log evidence; D did not execute these gates:

| Parent gate | Observed result | Evidence under `expression-unification/logs/` |
|---|---|---|
| Exact origin-alias regression before E fix | **0 passed/1 failed**: mutated metadata plus false origin-match, not the original metadata plus true match | `tidb-d1-pb-origin-alias-red-v2.log:1986–2003` |
| Same exact regression after E fix | **1 passed/0 failed** | `tidb-d1-pb-origin-alias-green.log:1987–1990` |
| D1 seed after origin fix | **16 passed/0 failed** | `tidb-d1-post-origin-fix.log:1986–2004` |
| D2 structural tests, after fix via Cargo | **8 passed/0 failed** | `tidb-d2-structural.log:1986–1996` |
| Full TiDB expression-library comparison | **1351 discovered;1254 passed/4 failed/93 ignored** | `tidb-d2-expr-full-comparison.log:1986,3339–3371` |

The four failing names **and assertion/panic contents** match the previous D1 full cohort (`tidb-d1-expr-full-comparison.log:3296–3328`,1245/4/93): the IFNULL pushdown assertion, EXP FloatOverflow assertion, duration-FSP conversion panic, and STR_TO_DATE NULL-versus-zero-date assertion already enumerated above. D reread both failure blocks. Thus parent accepted **no new failing tests**, not a green full suite. The current pass delta also includes E's regression; it is not a migrated-family count or attribution solely to D.

All four D2 product files are frozen at the corrected checkpoint; D has no implementation release beyond it. C3 below remains design-only. There is no ordinary-call demand/profile activation, broader carrier acceptance, generic metadata guarantee, public evaluator route, fallback, or package/family completion claim. Migrated-family count stays zero. D awaits the parent's next explicit implementation release.

### Actual APIs and closed admission

- `rewriter/preparation.rs`: crate-private `PreparationPurpose::{SqlBuild,StructuralOnly}`, explicit `StructuralLimits {max_nodes,max_depth}`, and opaque `StructuralExpression::{checked,as_expression,into_expression}`. `into_expression(self)` moves the checked root without recursive Clone. Checked finishing validates the bounded tree and iteratively detaches every retained FieldType via the existing `deep_copy_like_go`; it does not compute values or insert inferred flags. The ordinary native Expression/AST Clone/Drop implementations remain unchanged.
- `rewriter.rs::rewrite_expr_structural(&Expr,&impl ColumnResolver,StructuralLimits) -> Result<StructuralExpression,EvalError>`: whole-syntax iterative preflight, shared purpose-aware rewriter, checked finish. `rewrite_expr_resolved` remains the public SqlBuild wrapper with its original signature; `rewrite_expr` still calls it.
- `new_function.rs::new_function_structural(&dyn Columns,&str,FieldType,Vec<Expression>,StructuralLimits) -> Result<StructuralExpression,EvalError>`: bounded typed-argument preflight including the new parent node, one shared function-construction body, checked finish. Existing `new_function_impl` and every public NewFunction wrapper keep their signatures and SqlBuild ordering. `new_function_impl_with_purpose` is actually `pub(crate)`, not an unusable private sibling seam; structural production callers go through the bounded wrapper. Its callback veto executes before any callback or construction work.
- Admission is syntax/type/provenance only: Int/String/NULL construction, complete signed-LongLong row columns, parentheses, AND/OR, IF/IFNULL/COALESCE/searched CASE, and explicit non-array `CAST(... AS SIGNED)`. All control inputs and inferred call results must have **real** signed-LongLong types. Casts, strings and untyped NULL accepted as preparation structure do not become D1 executable. Unsupported syntax is refused before binding, including dead arms; unsuitable bound types are refused before implicit wrap/probe paths. No Tiny/unsigned/NULL retag, no second inference table.
- This new SQL-only preparation seam rejects PB-origin/signature nodes, deferred/parameter Constants (`literal_value()` must succeed), correlated/virtual columns and opaque VALUES/grouping state. It does not modify or normalize frozen PB provenance. Actual literal flags and metadata-only inferred flags are retained; the missing **value-computed** nullability/precision is not manufactured. StructuralExpression remains preparation metadata, not fully refined SqlBuild planner publication.

### Enumerated recursion and effect-guard edges

The new purpose is an explicit parameter, not a fold counter, public resolver method, TLS state or a session field. Source review found and updated all these production edges:

| Edge group | Actual purpose propagation / veto |
|---|---|
| Public ingress / core | `rewrite_expr` → public `rewrite_expr_resolved` → `rewrite_expr_resolved_with_purpose(SqlBuild)`; structural ingress runs `check_ast` then the same private core with StructuralOnly. Core → `stacker::maybe_grow` → `rewrite_expr_resolved_inner` → `rewrite_leaf`. |
| Dispatcher / leaves | Parentheses recurse to the purpose-aware core. Binary children pass purpose through their FoldModeResolver. Dispatcher passes it to `rewrite_leaf_literal`, `rewrite_leaf_compound`, `rewrite_leaf_call`; literal CharsetBinary's direct `rewrite_leaf` edge also carries it. Assign/Collate child edges retain it even though not structurally admitted. |
| Row/comparison graph | `rewrite_comparison` ↔ `rewrite_row_comparison`, `compose_comparisons` → `binary_expression`; scalar comparison children, lexicographic prefixes/current element, CNF/DNF closures and row-IN/equality expansion all retain purpose. These shapes remain excluded from structural syntax, rather than silently restarting SqlBuild. |
| Compound graph | IN tested/list children, BETWEEN value/low/high, LIKE/REGEXP operands, CASE selector/WHEN/result/ELSE, MEMBER OF operands, IS and unary child edges all call the purpose-aware core. BETWEEN's two aliased `compare` calls also forward purpose after the D-r4a correction. Simple CASE is rejected by whole-syntax preflight before the legacy selector-copy branch. |
| Call graph | EXTRACT, GET_FORMAT, POSITION, TIMESTAMPADD/DIFF; GROUPING/sequence/date-interval/CHAR/generic function children; explicit and temporal CAST; CONVERT USING, WEIGHT_STRING, TRIM and ROW children all retain purpose. `literal_text` and `wrap_power_arguments` also receive it. The unsupported shapes stay SqlBuild-only by admission. |
| Resolver folding | All **10** production `resolver.fold_constant` call sites were replaced by `purpose.fold`: POW cast, three arithmetic wrap sites, logical conversion cast/wrapper, comparison wrap closure, post-rewrite fold, generic binary-literal wrap, explicit CAST wrap. The single new helper calls the resolver only for SqlBuild; StructuralOnly does not even call its Disabled hook. |
| Binary construction | StructuralOnly admits only AND/OR and checks both actual input types before any wrapping. Comparison refinement (including NoColumns fallback) and `prepare_numeric_arguments`/`comparison_context` are additionally inside explicit SqlBuild guards. Existing type helpers are reused unchanged. |
| CASE / finishing | Structural control-argument types are checked before CASE result wrapping. CASE's direct `fold_constant_in_mode` and `comparison_context` are enclosed in SqlBuild. Common finishing checks each structural node, derives ordinary collation, vetoes forced-Normal CAST folding, and prepares IN hash caches only for SqlBuild. |
| Binding | Structural Column syntax uses one complete `resolve_column` result, validates it, and returns without `resolve_constant`/`resolve_expression`; DEFAULT/parameter/grouping syntax is rejected by preflight. No Columns::get, Datum-derived type, or resolve-then-reconstruct path was introduced. Metadata-only resolver methods remain a caller contract. |
| Shared NewFunction body | Structural name/type admission and callback veto precede the existing body. Comparison refinement, binary-literal fold closure, numeric probing and final fold have explicit SqlBuild guards. DATE_ADD/SUB unit evaluation, sysdate and grouping paths are unreachable for structural admitted identities. SqlBuild still executes null-type inference, arity, unit normalization, inference, comparison refinement, collation/wrapping, numeric probes, grouping flags, callback and final fold in the original order. |
| Excluded effectful inference | Direct IN refinement/folding, Decimal cast precision, unary `folded_value`, ROUND scale conversion, CHAR invalid-charset warnings, temporal literal parsing, parameter reads and host builders remain unchanged behind rejected syntax/typed-call identities. They are not replaced with approximate metadata or fake warning sinks. |

The remaining `rewrite_expr_resolved(` production matches in `rewriter.rs` are its public definition and the no-scope `rewrite_expr` ingress; the other matches are existing tests. All recursive production calls now target the purpose-aware core. `rewriter/fold_mode.rs` and `impl ColumnResolver for &T` are unchanged, so SqlBuild's existing Normal/Try/Disabled forwarding behavior is preserved rather than repaired incidentally.

`Budget::push` bounds queued nodes and depth before queue growth. Typed constructor checks bound the initial argument metadata scan and each nested function's arity scan before descending; checked finish revalidates the resulting tree. These are node/depth bounds, not a claim to bound arbitrary metadata byte payloads, hostile resolver work, or native recursive Drop. No global default limit was invented.

### Eight structural tests, with paired legacy assertions — parent 8/8 GREEN

All are in `rewriter/preparation_tests.rs`. D wrote but did not run them. Parent ran the already-compiled artifact directly while E edited separate origin files, avoiding a Cargo/source race. `logs/tidb-d2-structural-pre-origin-fix.log:2–12`, reread by D, reports **8 passed,0 failed,0 ignored,1343 filtered out**. That was targeted pre-origin-fix evidence. Parent then repeated all eight through Cargo after the fix (`tidb-d2-structural.log:1986–1996`), again8/8, and accepted the full comparison cohort described above:

1. `structural_controls_never_call_value_hooks`: real parsed nested controls/constant trees, exact complete-column lookup counts, retained function nodes/declared types, and direct construction of all six control identities. A resolver panics on forbidden binding/evaluation/fold/context hooks.
2. `structural_explicit_signed_cast_ignores_forced_normal_fold`: root/nested/control-arm signed casts retain original `bad` string bytes without conversion, and the unchanged D1 lowerer still refuses them.
3. `structural_rejection_precedes_all_value_effects`: rejected dead-arm arithmetic/comparison/unary/ROUND/temporal/default/parameter/host/assignment/IN/simple-CASE shapes produce no column lookup; explicit invalid-CHAR builder shape, mutable/deferred typed arguments and a poisoned callback are refused.
4. `structural_preparation_preserves_diagnostics_and_state`: preserves an existing FOLD_WARNINGS sentinel and restores the prior stash; Poison Columns methods trap row/parameter, clock/RNG/uservar/sequence/advisory-lock, warning/note/bookmark/rollback/drain and plan-cache mutation calls. Both successful preparation and rejected stateful syntax are exercised.
5. `structural_purpose_survives_nested_resolver_scopes`: borrowed trait resolver, nested FoldModeResolver/function scope in all three modes, plus paired **public SqlBuild** assertions: explicit CAST still forces Normal, callbacks precede outer folds, and comparison refinement plus its two truncation warnings still precedes callbacks in Normal/Try/Disabled.
6. `structural_reuses_declared_metadata_without_normalizing`: complete column identity/hidden/collation metadata, element-metadata detachment under source mutation, direct-constructor detachment plus consuming extraction, full nonconstant-control FieldType comparison with SqlBuild, real Tiny/unsigned/NULL rejection/preservation, and explicit differentiation from SqlBuild's value-computed constant NOT_NULL flag.
7. `structural_limits_and_rejected_syntax_do_not_evaluate`: exact/over node-depth budgets before binding, direct-constructor budgets, bad arity/name, nested parentheses and zero-depth finish. No iterative native Drop assertion.
8. `structural_bigint_controls_feed_frozen_d1`: real bound control preparation → unchanged D1 lower/compile/eval, nullable inputs, native chunk selection deliberately distinct from physical repeated/nonidentity explicit selection, and empty selection. No public evaluation hook is installed.

These tests do not claim universal type-inference purity, simple CASE once-only execution, ordinary-call scalar/batch equivalence or D1's newly found PB-origin immutability edge. Parent has now accepted the post-origin-fix cohort above, without broadening these tests' claims. Source review corrected the string payload test to the actual `StringDatum::bytes()` accessor; no guessed replacement API remains there.

### Caller compile correction — BETWEEN helper alias

Parent's `logs/tidb-d1-pb-origin-alias-red.log:1817–1853` records two E0061 errors **before any test ran**: `let compare = binary_expression` in BETWEEN supplied only four arguments to its lower/upper comparisons after the helper gained `PreparationPurpose`. This is a D2 propagation omission, not an observed RED for E's D1 metadata regression. The initial named-call search missed calls through that alias; its census was incomplete despite formatting passing.

Parent narrowly released only `rewriter.rs` for these two fixes plus this receipt. Both calls at1729–1730 now forward the existing `purpose` as the fifth argument, preserving their order and all SqlBuild behavior. No syntax/type admission was widened. All other product files remain frozen. A fresh alias-binding search across `tidb-expr/src/**/*.rs` over every changed private helper found only `let compare = binary_expression`; the `compare` census contains exactly its binding and these two calls. A whole-word census of the helper names in rewriter.rs additionally returned91 definition/comment/reference matches, including the alias, rather than searching only `name(` calls.

From `/home/agent/tidb/expression-unification/tidb`, this exact command passed:

```sh
rustfmt --edition 2021 --config skip_children=true rust/crates/tidb-expr/src/rewriter.rs && rustfmt --edition 2021 --check --config skip_children=true rust/crates/tidb-expr/src/rewriter.rs && git diff --check -- rust/crates/tidb-expr/src/rewriter.rs && sha256sum rust/crates/tidb-expr/src/rewriter.rs
```

Corrected `rewriter.rs` SHA256: `9445cdd6fcae601cb3cad92707a909286fadaa659d31a149d08116fc2340bade`. The other three file hashes below are unchanged. D ran no build/test. Parent's retry compiled successfully (`tidb-d1-pb-origin-alias-red-v2.log:1981`) and reached the actual D1 RED recorded above; the same artifact then passed all eight D2 tests. D has refrozen rewriter.rs at this correction. The initial formatter/diff-check record below remains historical source-only evidence; the later compiler/test result comes from parent, not from those static checks.

### Initial D2 source checks actually run

From `/home/agent/tidb/expression-unification/tidb`, these commands passed; the final four were chained with `&&`:

```sh
git diff --stat -- rust/crates/tidb-expr/src/rewriter.rs rust/crates/tidb-expr/src/new_function.rs
git diff -- rust/crates/tidb-expr/src/new_function.rs rust/crates/tidb-expr/src/rewriter.rs
rustfmt --edition 2021 --config skip_children=true rust/crates/tidb-expr/src/rewriter.rs rust/crates/tidb-expr/src/new_function.rs rust/crates/tidb-expr/src/rewriter/preparation.rs rust/crates/tidb-expr/src/rewriter/preparation_tests.rs
rustfmt --edition 2021 --check --config skip_children=true rust/crates/tidb-expr/src/rewriter.rs rust/crates/tidb-expr/src/new_function.rs rust/crates/tidb-expr/src/rewriter/preparation.rs rust/crates/tidb-expr/src/rewriter/preparation_tests.rs
git diff --check -- rust/crates/tidb-expr/src/rewriter.rs rust/crates/tidb-expr/src/new_function.rs rust/crates/tidb-expr/src/rewriter/preparation.rs rust/crates/tidb-expr/src/rewriter/preparation_tests.rs
sha256sum rust/crates/tidb-expr/src/rewriter.rs rust/crates/tidb-expr/src/new_function.rs rust/crates/tidb-expr/src/rewriter/preparation.rs rust/crates/tidb-expr/src/rewriter/preparation_tests.rs
```

Initial checkpoint SHA256 (rewriter.rs superseded by the correction above): `rewriter.rs` = `696cbe722ee35b7ba10060928d6ddede7433296173aaa119f7c170da8e9827e5`; `new_function.rs` = `e472d65aee3e9bee39145816620fd27bc8a029c3aeee3fde86a9f311b5598da4`; `rewriter/preparation.rs` = `13dc5b1cb34937848f43d86bb47a9c64c961fd8d24ab7c23d60e1b3d2a058984`; `rewriter/preparation_tests.rs` = `365498c880d310ec96cdcabee04ab86b1ed5531dce11f9f532383c4f1195b8ad`.

Static tool searches enumerated the recursion/effect edges above and exactly eight test functions. Negative search in the new production preparation module for `eval_in|eval_func|from_expression|to_pb|try_tikv|enter_cop_eval|unsafe|Any.*Sync|\.eval\(|\.get_row\(|Box<dyn|FnMut|FnOnce|cast_arg_as` found zero matches. The preparation module intentionally contains the SqlBuild-only fold forwarding helper; grep-zero over the shared builders is not claimed. Formatting is parse/whitespace evidence, **not typechecking or runtime proof**. No background job, build, cargo metadata run or dependency change was made. Parent needs no new lib.rs export or Cargo wiring for these nested modules.

## D3 accepted checkpoint — exact six files, runtime-only

Parent released precisely new `tikv/{ordinary,ordinary_tests}.rs` and narrow `tikv/{mod,catalog,lower,context}.rs` changes plus this receipt, then compiled and accepted the runtime-only cohort. D reread C's actual `local/profile.rs`, exported APIs and tests and the parent-run caller logs below. **D ran no builds, tests, lint, dependency commands or fixture generators.** This is not native error-adapter acceptance, public activation or a family-completion claim.

### Actual parent acceptance and independent review

| Gate | Actual result | Evidence under `expression-unification/logs/` |
|---|---|---|
| New D3 ordinary caller | **10 passed/0 failed**, 1351 filtered | `tidb-d3-ordinary-seed.log:2011–2023` |
| Frozen D1 compatibility | **16 passed/0 failed** | `tidb-d3-d1-compat.log:1999–2017` |
| Frozen D2 compatibility | **8 passed/0 failed** | `tidb-d3-d2-compat.log:1999–2009` |
| Full expression-library comparison | **1361 discovered;1264 passed/4 failed/93 ignored** | `tidb-d3-expr-full-comparison.log:1999,3362–3394` |

D reread the full failure block: the four names and complete assertion/panic contents match the preceding1254/4/93 cohort, not merely the failure count. This is **no new failing tests**, not a green full suite. Parent reports the surrounding baseline as datatype347, RPN520 and aggregate40. E's independent read-only D3 audit is reported complete with no finding; a harness closing/bookkeeping failure was not a test failure or another runtime gate. All six caller files remain frozen. The initial source-only checks/hashes below remain historical evidence; the runtime proof is the parent-run log cohort.

### Actual caller surface and ingestion

- `ordinary.rs` provides the exact reviewed `lower_typed_int_plus_row`, `lower_pb_int_plus_row`, opaque `LoweredIntPlusRow`, and worker-local `PreparedIntPlusRow::{compile,eval_selected,eval_one}`. Both lowerers call one iterative Visit/Finish implementation; no native recursive clone, evaluator, resolver, cast insertion, SqlBuild rerun or new StructuralOnly grammar is present.
- Typed ingress requires a PLUS root and no PB builtin/origin throughout. PB ingress requires a real selected203 builtin and an immutable captured origin at every node, validates the existing checked primitive encodings/effective type, and compares raw optional node metadata and ordered children against the **trusted retained original wire input**. That original wire is only borrowed during ingress; an explicit test drops it and the native expression before compiling/evaluating the retained spec. No whole PB re-decode, reserialization or whole-wire clone occurs in production. Node-local D1 origin did not promise ancestry; this is the new stricter paired-ingress contract, not a D1 value bug.
- All participating declared types are signed non-array LongLong; strict literals are only actual Int/typed NULL and become `LiteralKind::Typed`. PB NULL remains refused because its actual effective type is Null. Dead children receive static admission checks. The new route admits neither222 nor controls, hosts, other operations, mixed native paths, AST/batch profiles or implicit conversions.
- `catalog.rs::int_plus_row` validates exact shape/types/opaque metadata, reads the existing signed/signed PLUS CATALOG row for TypedRow or actual validated PB signature for PbRow, and converts that exact203 through the generated enum. It does not synthesize a PbScalar/Expr, infer new types, or duplicate TiKV's kernel selector. The old `int_control` body is unchanged.

### Actual immutable facts, value boundary, and preserved D1 behavior

- Source IDs use explicit `source_unit` and all-node preorder ordinals (root0, left before right). C's `OrdinaryProfileSpec::new` snapshots the actual LocalExpr/schema and complete sorted call records. Each input slot corresponds to one leaf occurrence with a retained slot→node map; repeated physical rows are distinct InputRows. C validates the exact snapshot again during `compile_local_profiled`.
- Every source node, input binding, row-schema field, and computed result retains detached complete SQL FieldType metadata. Every PLUS has its own Int `ValueMetadata` and its own detached result FieldType; final materialization uses the root's record. No computed call is labeled a passthrough, including plus(x,0) or a NULL result.
- The non-executable source-shape sidecar records literal values, column identity via NodeMetadata, and call child ordinals plus a complete **CiString clone**, including original display spelling/case. There is no diagnostic text normalization/rendering. PB labels do not select execution. The original wire is absent from the owned executable graph; only node-local immutable origins are retained.
- `lower.rs` changes are limited to sibling visibility of the four existing type/origin/projection helpers. `context.rs` factors the original slot/type/occurrence validation and physical-row Datum read into `read_native_datum`. The old D1 `read_input` then runs its same existing `to_scalar`/VectorValue step in the same order; D1's entrypoint/domain/policies are unchanged.
- New `PlusInputs` wraps only that checked Chunk reader in production. It examines the actual demanded **Datum::Int/Null BEFORE to_scalar** can erase UInt identity. The raw-Datum trait is private; the alternative supplier exists only in the test seam. No production native-expression callback, host wrapper or second runtime is introduced. Static Chunk/schema/layout/selection checks still precede value reads; no dead row/slot is eagerly imported.
- Execution is only C's existing official `LocalProgram::eval_with_bindings` after `compile_local_profiled`. LocalError identity and caller-owned warning prefix are preserved; this code does not obtain SQL error codes, format/map native errors, guess a failure site, or retry. The site-aware diagnostic API in the reviewed proposal below is **not implemented or released**.

No D1 batch/tests, D2 files, PB decoder, scalar-function implementation, pushdown-catalog implementation, Cargo/locks, lib.rs, public evaluator or C/B product file was written by D in this cut. Node/depth limits are not a promise of bounded arbitrary metadata bytes or iterative native Expression Drop. Safe Chunk metadata does not certify corrupt private raw buffers or recover historical input Datum variants.

### Ten runtime-only tests — parent10/10 GREEN

The parent filter is `tikv::ordinary::tests`; all ten named tests passed in the accepted parent cohort. D authored but did not execute them. They are in `ordinary_tests.rs` (the module is a child of ordinary.rs):

1. Exact203, full all-node preorder/source IDs, child topology, input-leaf mapping and old-route refusal.
2. Genuine paired PB ingestion, raw203 facts, display-name independence/original-case retention, swapped genuine children against original wire, stale/missing/type/index/literal/arity/encoding provenance, fabricated from_pb refusal, nested PB and wire/native-owner drop.
3. PB NULL type refusal, TypedRow typed-NULL literal acceptance, and nullable PB columns with poisoned skipped RHS.
4. RAW UInt0/UIntMAX/Bytes/BinaryLiteral/String/Real veto **before** the bridge; an explicit assertion demonstrates to_scalar itself accepts UInt. Dead dynamic RHS is not read, but dead static unsupported literal still fails admission.
5. Left/right/nested kernel error demand prefix, no later selected row, intact LocalError::Evaluation, prior/synthetic-input warnings and success-prefix preservation. No native diagnostic/site parity assertion.
6. Per-call output identity and detached type metadata, retained nullable literal provenance and complete column/collation facts, source alias mutation, and plus(x,0) remaining computed.
7. Empty/1/1024/1025 and nonidentity/repeated physical selection, native Chunk::Sel independence, row schema/layout/length rejection and distinct slots for repeated source-column occurrences.
8. Stale value/type/slot snapshots and missing/duplicate/wrong-ordinal site records, exact/over node-depth budgets, work refusal before a poisoned read, and33/64-deep native source trees without a recursive lowering clone.
9. Negative AST/batch,222/local NULLIF, Int tagged Text/BinaryLiteral, other operations, unsigned/Tiny/untyped NULL, deferred/parameter/virtual/correlated/VALUES, mixed PB/typed and non-call roots.
10. D1 control/PLUS admission remains unchanged, D2 still rejects arithmetic before binding, and paired legacy SqlBuild arithmetic folding and admitted StructuralOnly controls remain intact.

C's actual immutable profile API has no source mutation accessor; the tiny stale-snapshot fixtures clone only small LocalExprs. No deep-clone guarantee is claimed. Native legacy evaluation/folding exists only in explicitly paired test fixtures, never behind the new runtime service. Parent still owns compilation, these ten tests, the16 D1/eight D2 regressions and full baseline comparison.

### Source checks actually run and checkpoint hashes

Working directory was confirmed by `pwd` as `/home/agent/tidb/expression-unification/tidb`. D ran `rustfmt --edition 2021 --config skip_children=true` on exactly the six approved files, followed by the same command with `--check`; both succeeded. The final validation invocation chained these exact commands with `&&` (no build or lint):

```sh
rustfmt --edition 2021 --config skip_children=true rust/crates/tidb-expr/src/tikv/ordinary.rs rust/crates/tidb-expr/src/tikv/ordinary_tests.rs rust/crates/tidb-expr/src/tikv/mod.rs rust/crates/tidb-expr/src/tikv/catalog.rs rust/crates/tidb-expr/src/tikv/lower.rs rust/crates/tidb-expr/src/tikv/context.rs
rustfmt --edition 2021 --check --config skip_children=true rust/crates/tidb-expr/src/tikv/ordinary.rs rust/crates/tidb-expr/src/tikv/ordinary_tests.rs rust/crates/tidb-expr/src/tikv/mod.rs rust/crates/tidb-expr/src/tikv/catalog.rs rust/crates/tidb-expr/src/tikv/lower.rs rust/crates/tidb-expr/src/tikv/context.rs
git diff --check -- rust/crates/tidb-expr/src/tikv/ordinary.rs rust/crates/tidb-expr/src/tikv/ordinary_tests.rs rust/crates/tidb-expr/src/tikv/mod.rs rust/crates/tidb-expr/src/tikv/catalog.rs rust/crates/tidb-expr/src/tikv/lower.rs rust/crates/tidb-expr/src/tikv/context.rs
git status --short -- rust/crates/tidb-expr/src/tikv/ordinary.rs rust/crates/tidb-expr/src/tikv/ordinary_tests.rs rust/crates/tidb-expr/src/tikv/mod.rs rust/crates/tidb-expr/src/tikv/catalog.rs rust/crates/tidb-expr/src/tikv/lower.rs rust/crates/tidb-expr/src/tikv/context.rs
sha256sum rust/crates/tidb-expr/src/tikv/ordinary.rs rust/crates/tidb-expr/src/tikv/ordinary_tests.rs rust/crates/tidb-expr/src/tikv/mod.rs rust/crates/tidb-expr/src/tikv/catalog.rs rust/crates/tidb-expr/src/tikv/lower.rs rust/crates/tidb-expr/src/tikv/context.rs
```

The actual final invocation chained these steps with `&&` and explicit paths. All six files are **untracked in this worktree**, including the existing D1 boundary files; the scoped `git diff --stat/diff/check` commands therefore emitted no content diff and are not represented as substantive diff coverage. Direct source review plus rustfmt/check and a content search for `[\t ]+$|^(<<<<<<<|=======|>>>>>>>)` across the exact six basenames found no whitespace/conflict matches. The new production ordinary.rs has no matches for `pb_to_expr\(|from_expression\(|expression_to_pb\(|\bto_pb\(|fold_constant|prepare_numeric_arguments|\.eval\(|eval_in\(|try_tikv|sql_error_code|arithmetic_overflow_error|enter_cop_eval|unsafe|impl.*Sync`. This is source inspection, not typechecking or runtime evidence.

| Frozen source file | SHA256 |
|---|---|
| `ordinary.rs` | `e627526a34c1f4f88627cf561f4ab8fd20545691c3ad44354fc4719b5cc46456` |
| `ordinary_tests.rs` | `44dbfec7d6714dd9e5eb161d8fa92c81b69d0c8a738e0199cc4ce01d97fa1341` |
| `mod.rs` | `110990476f907376b4716707d8995f6a6e3da9cec062e516ba629a3dc1263a59` |
| `catalog.rs` | `277301666a7853291c209da144d0c96df7f5d95beb6f27d91585ea2c4b8a79fb` |
| `lower.rs` | `4c2cb437a6c80922aed85bed7455fc0d18be24e51a518c31617c3e45354ab484` |
| `context.rs` | `1ea60da951aa4dc882848e18325e45b31e50571bd8578636f085c59156369bcd` |

D stops at this checkpoint. Migrated-family count remains zero. Ordinary-call demand/transport success is not native diagnostic equivalence, broader metadata/profile support, public activation, or whole-package completion.

## D4 accepted checkpoint — four files, private exact203 diagnostic view only

Parent reviewed the r9 design and released exactly NEW `tikv/{ordinary_diagnostics,ordinary_diagnostics_tests}.rs`, a narrow child-module/associated API seam in `tikv/ordinary.rs`, and **only pub(crate) visibility** on existing `scalar_function::{binary_op_for_name,arithmetic_symbol}`. D wrote those four files and this receipt, then froze product writes for parent compilation. D ran **no build, test, lint, fixture generator or dependency command**. Parent has since reported actual C3d537/537 plus aggregate40 and shared-parser353/353, then compiled and accepted the TiDB1.100 D4 caller cohort below. D reread these caller logs; these are parent-run results, not D-run commands.

### Actual parent gate — accepted, no source churn

| Gate | Actual result | Evidence under `expression-unification/logs/` |
|---|---|---|
| D4 private diagnostics | **10 passed/0 failed** | `tidb-d4-diagnostics.log:2039–2051` |
| Old D3 runtime | **10 passed/0 failed** | `tidb-d4-d3-compat.log:2027–2039` |
| Old D1 seed | **16 passed/0 failed** | `tidb-d4-d1-compat.log:2027–2045` |
| Old D2 structural preparation | **8 passed/0 failed** | `tidb-d4-d2-compat.log:2027–2037` |
| Full expression comparison | **1371 discovered;1274 passed/4 failed/93 ignored** | `tidb-d4-expr-full-comparison.log:2027,3400–3432` |

D reread the complete failure block: all four names and their full assertion/panic contents match the accepted D3 baseline1264/4/93. This remains **no new failures**, not an all-green full suite. Parent matched all four source hashes below and pinned rustfmt without source churn. The native353/537/aggregate40 cohort is the accepted dependency pin, not proof of broader caller semantics. D4 acceptance is only this private exact203 native-overflow view; there is no general SQL diagnostic adapter, warning-site attribution, context/severity/publication handoff, public activation or migrated-family claim. C3b is still source-proposal work and D has no new implementation grant.

### Actual API, joins and ownership

- The existing exported PreparedIntPlusRow now has `prepare_diagnostics(max_nodes,max_depth,max_rendered_bytes) -> SeedResult<PlusDiagnosticPlan>` and `eval_selected_reported(ctx,chunk,row_schema,selection,&plan) -> PlusEvaluation`. Their implementation is in the new child module; ordinary.rs adds only that module declaration. Old compile/eval_selected/eval_one and all D3 raw behavior are unchanged. Associated return types/getters are actually pub(crate) and usable by inference, without a mod.rs export loan.
- PlusEvaluation owns the result and raw WarningEndpoints. PlusFailure retains either Caller `{Preflight|Materialization, SeedError}` or the original owned C3d ReportedLocalFailure; its optional joined site/native EvalError are views, not replacements. `into_raw` recovers the original owned cause. No synthetic report is constructed for caller failures.
- Production adaptation occurs only at the own program's freshly returned result. Arc::ptr_eq binds the diagnostic plan to that prepared spec before any value work. Exact all-node call records/source/profile/raw PB signature, row/selection universe,203/signed LongLong argument/result domain and own computed Int identity are verified **before** the typed1690 getter. The site wins over raw-error variant; Input1690 or Input ResourceLimit never becomes PLUS overflow. D3's distinct per-leaf slots are validated as a unique mapping and joined to the actual leaf, not an enclosing PLUS. No root/ancestor repair or authentication claim for owner-supplied IDs is made.
- All source/rendering facts remain non-executable. A checked reverse-preorder/postorder plan validates bounded topology, lengths, depth and binding tables without per-subtree strings. It records separately each node's operand renderability and each call's **own** overflow length. Actual own-overflow uses `+`; nested display symbols use the retained full CiString plus the two existing pure facts. Default nested sig_PlusInt still causes native IntOverflow fallback, while that node's own leaf-operand failure can be source-shaped. Renamed mixed-case PB display names do not change execution203.
- A single bounded final String and an iterative token stack render the failing call only. Checked byte arithmetic/plan refusal is a distinct resource/admission result, never native IntOverflow fallback. A render/allocation refusal after a runtime failure leaves its raw report intact and omits a native view. There is no native formatter, eval/cast, row read, erased-code parsing, kernel duplication or native replay in adaptation.
- The outer reported wrapper samples live `{warning_cnt,stored_len}` before **all** invocation preflight, calls one inner function, then samples after normal return/cleanup on every success or returned failure. No warning cap is read or guessed from cfg, no vector is cloned or drained in production, and no delta/saturation/prefix-byte authentication is claimed. Panic produces no receipt/after endpoint; C's reporting state is invocation-local.

The only scalar_function.rs changes are the two visibility modifiers; its bodies/callers were not edited or reformatted. No mod/Cargo/context/lower/old-test/PB decoder/C-runtime/public evaluator/source-type widening or warning-sink change was made. This private single-overflow view is not context/severity/publication handoff or SQL activation.

### Ten D4 tests — parent10/10 GREEN

The filter is `tikv::ordinary::diagnostics::tests`. All ten names from the reviewed design passed in the accepted parent cohort; D did not execute them:

1. Correct nested/root kernel ordinals, source IDs and nonidentity/repeated row joins, isolated native error oracles, and no later-row read after failure.
2. Input1690/Resource/wrong-native-kind remain raw Input and map to the actual source leaf; repeated native column indices still have separate slots.
3. Wrong rows/universe/source records decline joins, invalid own output domain refuses planning, generic Other text containing1690 stays code10000, and legacy eager222's unsited1690 is not rebranded. C3d's own storage/getter cases remain its separate accepted gate; D adds no common-error Cargo edge.
4. PB own-error rendering versus nested operand fallback, default sig_PlusInt, retained mixed-case MiNuS display and unchanged203 execution, compared against isolated native reference evaluation.
5. Same owner-supplied IDs on a different Arc spec cannot substitute a diagnostic plan; refusal precedes poisoned reads and a subsequent correct call succeeds.
6. Exact/over node/depth/UTF-8-byte budgets, signed-minimum integer and Column# fallback,129-node/65-depth planning, and a private render-refusal fixture that preserves raw error rather than inventing fallback.
7. Before/after observations on empty/success, native caller schema/selection preflight, C preflight, input failure and defensive materialization failure. The same final sampler follows all normal inner returns.
8. Actual cap0/cap1/larger receivers with deliberately inconsistent cfg values; test-only prefix byte copies verify these paths while production receipts store count/length only.
9. Nonmonotone/reset endpoints—including a prior usize::MAX count—remain raw observations; panic returns no receipt and subsequent success/empty/error has no stale site.
10. Original common-error Box identity survives the native view and consuming raw recovery; old D3 raw error and the two legacy pure display facts stay unchanged. Native evaluation occurs only in explicit test oracles, never production adaptation.

### D4 checks actually run and frozen manifest

From `/home/agent/tidb/expression-unification/tidb`, D ran these commands (each paired group joined by `&&`):

```sh
rustfmt --edition 2021 --config skip_children=true rust/crates/tidb-expr/src/tikv/ordinary_diagnostics.rs rust/crates/tidb-expr/src/tikv/ordinary_diagnostics_tests.rs rust/crates/tidb-expr/src/tikv/ordinary.rs
rustfmt --edition 2021 --check --config skip_children=true rust/crates/tidb-expr/src/tikv/ordinary_diagnostics.rs rust/crates/tidb-expr/src/tikv/ordinary_diagnostics_tests.rs rust/crates/tidb-expr/src/tikv/ordinary.rs rust/crates/tidb-expr/src/scalar_function.rs
git diff --check -- rust/crates/tidb-expr/src/tikv/ordinary_diagnostics.rs rust/crates/tidb-expr/src/tikv/ordinary_diagnostics_tests.rs rust/crates/tidb-expr/src/tikv/ordinary.rs rust/crates/tidb-expr/src/scalar_function.rs
sha256sum rust/crates/tidb-expr/src/tikv/ordinary_diagnostics.rs rust/crates/tidb-expr/src/tikv/ordinary_diagnostics_tests.rs rust/crates/tidb-expr/src/tikv/ordinary.rs rust/crates/tidb-expr/src/scalar_function.rs
```

All succeeded. Only the three new/module-seam files were formatter-written; scalar_function.rs was check-only after the two literal visibility edits. As at D3, git diff does not validate untracked boundary-file contents; rustfmt/check plus direct file review/content searches are the source checks, not compiler proof. The new production diagnostic module has zero matches for calls to the native renderers/eval, PB reconstruction, retry, warning take/truncate/cfg-cap lookup/vector clone, unsafe or Sync widening. Exactly ten test functions were enumerated. No test/build result is inferred from these searches.

| Frozen source file | SHA256 |
|---|---|
| `ordinary_diagnostics.rs` | `ada427a07dedb7c96cad3bb59a49f9143a9a334e41793e5f23d7aba8c8822e77` |
| `ordinary_diagnostics_tests.rs` | `3559fcc56a64895e09df91a646e42cabe61b67e987586df8dc88902018e5ab78` |
| `ordinary.rs` | `d2b85bc336bffac23446c0e0ebdd35ba78372347bdc859ddefdd20b503e32168` |
| `scalar_function.rs` | `2d21803a5f48020e3815d6b032bcd9db2681e98525055b91d620e632b61407b3` |

Parent accepted the compiled D4/caller comparison cohort above and matched these hashes. All four products remain frozen; D awaits the next explicit implementation release. No general native diagnostic/profile/carrier/publication/activation or family-completion claim is added; migrated-family count remains zero.

## Round5 ASCII activation/deletion readiness — independent SOURCE-ONLY inventory

### Authority, evidence level and coordination

Parent assigned D read-only caller/entry/deletion/error analysis, with **this receipt as the only writable file**. All D6 products remain frozen. E owns the separate owner/lease/scoped-Columns two-new-file proposal; this section does not duplicate its pool design or grant any of its proposed public hooks. C's six-file C4 handback/joint gate is parent-owned and was still pending during this inspection. Current C4 source/API observations below are not a new native test acceptance.

D independently read the native source and consulted `evaluated-value-contract.md` EV-r2 and `ascii-baseline-contract.md` AB-r1. The baseline probe/source/binary and old test expectations remain immutable. A private ready-Bytes adapter, its unit tests, or the existing D6 numeric entry cannot be counted as the complete ASCII family. **Migrated/shared-runtime-verified family count remains0; denominator245/target221 unchanged.** This is not a second ExecPlan or a package-transcreation claim.

Path abbreviations for this section (all under expression-unification): **D** = tidb/rust/crates/tidb-expr/src/; **X** = tidb/rust/crates/tidb-executor/src/; **XT** = tidb/rust/crates/tidb-executor/tests/; **P** = tidb/rust/crates/tidb-planner/src/; **S** = tidb/rust/crates/tidb-session/src/; **U** = tidb/rust/crates/tidb-unistore/src/; **K** = tikv/components/tidb_query_expr/src/. Anchors refer to inspected current source, not promised post-edit line numbers.

### Complete located ASCII algorithm/deletion ledger

The focused whole-Rust-tree search found **one TiDB Rust ASCII computation**, not a separate scalar/vector/PB implementation:

| Actual location | Existing behavior | Eventual disposition, not permission now |
|---|---|---|
| D`string_fn.rs::ascii:138–145` | Checks arity1; coerce_str_bytes; native NULL return; first byte or0 | Replace the numeric body AND its NULL/empty shortcuts with the checked C4 invocation after the same frontend arity/coercion point. Keep native frontend errors; every successfully admitted ready value, including NULL, computes through the generated nullable wrapper. Admission/contract/resource refusal returns its structured failure, never a locally calculated value. No duplicated first()/[0]/default-zero calculation survives here. |
| D`func.rs::eval_func_values:506–510,765` | ASCII arm calls ascii(vals); **this function already receives ctx:&dyn Columns** | Thin dispatch remains; the smallest future edit passes that existing context. No need to relocate the whole values dispatcher or widen unrelated function signatures. |
| K`impl_string.rs::ascii:222–233`; K`lib.rs:834` | Authoritative #[rpn_fn] body: empty→0, otherwise byte0; ScalarFuncSig::Ascii selects ascii_fn_meta | KEEP this single computational body. C4 must call its official generated wrapper/function pointer, not copy the body into TiDB. The C4 NULL wrapper witness is distinct from the non-NULL body counter. |
| Go reference `tidb/pkg/expression/builtin_string.go::builtinASCIISig.evalInt:266–274` | EvalString then NULL/empty/byte0; builder236–247 stamps flen3 and PB signature | Pinned upstream semantic reference, NOT a Rust deletion target or another production Rust fallback. |
| Go reference `tidb/pkg/expression/builtin_string_vec.go::builtinASCIISig.vecEvalInt:939–963` | Whole child VecEvalString, then output lanes | Pinned reference only. Do not substitute this Go scheduling into the already implemented Rust ASCII row loop. |

No ASCII-specific body/arm exists in the inspected Rust PbBuiltin/unistore conversion switches. Other search hits in lexer/parser charset handling, ASCII collation/repertoire, byte/rune predicates, and table-index charset logic are **not this scalar family** and must not be deleted. BIT_LENGTH and LENGTH/OCTET_LENGTH remain separate accounting domains; their bodies are not part of this loan proposal. The generated RPN wrapper is generic dispatch/null handling, not a second TiDB numeric body.

Frontend payload coverage is wider than String: D`coerce.rs::coerce_str_bytes:181–202` already admits String/Bytes, signed/unsigned numbers, Decimal, Real/Float32, Bit/BinaryLiteral, Duration/Time, Enum/Set names, JSON, Raw and VectorFloat32; Null produces None and range sentinels error. Retain this exact existing boundary, rather than narrowing the caller to String/Bytes or inventing new byte formatting. D`func.rs:260–293` retains child order, BinAware charset conversion and argument wrappers. D`rewriter.rs::wrap_binary_literals:440–474` inserts/folds to_binary from declared charset. D`scalar_function.rs::eval:1192–1212` retains its existing final coerce_to_ret_type:1029 onward, including manually constructed return declarations; the adapter produces only the computed nullable signed Int, not a rewritten declared type.

### Actual entry/call graph and final activation obligations

All positive paths below converge on the same native ASCII helper; reaching Expression::eval alone does **not** prove an execution scope or a TiKV kernel invocation. Each operational owner/forwarder must use E's reviewed capability, not compile a runtime per row or store one in immutable metadata. Scope placement below is an obligation, not another pool design.

| Surface | Read-backed actual chain | Necessary proof / smallest relevant future owner seam |
|---|---|---|
| Public AST/value entry | D`lib.rs::eval_in:796,927–929` → func::eval_func → child eval_in:260–271 → BinAware conversion/wrappers:278–293 → eval_func_values_in:447–478 → eval_func_values:506,765 → string_fn::ascii | Public AST/custom Columns/NoColumns path must reach C4 too. One-shot use is explicit; repeated AST loops need an outer scope. Do not force AST through SQL rewrite, which would change coercion and error timing. |
| SQL scalar construction/execution | X`driver.rs::run_select_on:312–317` → run_select_meta_on/in; planner/rewriter creates D`rewriter.rs:2084–2103,2419–2435` ScalarFunction with inferred metadata/BinAware children. X`driver/physical_builder.rs:4842–4882` builds local ProjectionExec for computed expressions. Expression::eval → ScalarFunction::eval → eval_by_signature:1572 / eval_func_values_in:3045 → common helper | Check the actual SQL front, metadata, prepared parameters and frontend effects, not only a hand-built scalar unit. Existing numeric fast path and PB dispatch remain separate. |
| Projection, vector flag on/off | X`projection.rs::ProjectionExec::next:349–383` → EvaluatorSuite::run; D`evaluator.rs::run_with_consumer:464 onward` selects native Decimal/numeric eligibility then normal expression rows for ASCII | ASCII is not admitted by numeric_batch_supported and has no Rust ASCII VecEval body. Preserve actual Rust per-expression/per-row order and current global-flag behavior; do not route through D6's closed203 consumer. |
| Parallel projection | X`projection.rs::fetch_and_dispatch_parallel:267–307`: worker reconstructs EvaluatorSuite from shared program then run(&shared.ctx,...) | Scope must follow the real worker/task/operation, not the fresh per-chunk suite or the shared program Arc. Preserve output order, close and caught-panic lifecycle. Existing public run is still Native today. |
| Filters/selection | X`selection.rs::row_passes:110–123` and evaluate_selection_mask:133–147 → D`evaluator.rs::vectorized_filter_consider_null/filter_physical_rows:127–248` → filter.eval over physical/live rows when vec_eval_bool cannot serve the tree | Both row-based and filter-major schedules remain; later filters must not consume dropped rows. The physical-row mask is not the same API contract as projection Sel. No eager whole-column child evaluation merely to feed C4. |
| Constant folding / TryFold / once | D`new_function.rs:471–474,483–538` → fold_constant_in_mode; D`constant_fold.rs:77–107,158` → eval_expression_once(expr,ctx). D`lib.rs::eval_expression_once:669–675` uses one virtual row. Legacy fold_value/folded_value:291–390 evaluate through NoColumns | Scope around the build/fold operation, not a live worker in cached Constant/ScalarFunction. Preserve warning bookmarks, .ok() suppression and later native reevaluation semantics (parent decision below). Every actual attempt after deletion uses C4. |
| DEFAULT — negative and positive are different | X`column_default.rs::build_with_current_database:680–709` dispatches a root function through func_call_default:396–633; **ASCII is absent from that whitelist**. Non-function roots go to fold; build_in_context:715–733 calls rewrite + eval_expression_once. Computed defaults evaluate via evaluate:832–868 | Root DEFAULT(ASCII(...)) stays FunctionNotAllowed, even if constant. Source-derived positive candidates: non-function-root DEFAULT(ASCII('A')+1), and allowed JSON_ARRAY/JSON_QUOTE shapes containing ASCII (e.g. a runtime UUID child). Validate those shapes separately; this inspection is not a SQL run. Keep eval→Unsupported remapping unchanged. |
| DML values / duplicate update / predicates | X`driver/dml.rs::run_insert_with_physical` at867–881 evaluates prepared values in row/list order; apply_on_duplicate:1857,1904–1907 evaluates against the updated row; order_rows_for_dml:1586,1629 and row_is_selected:4141–4152 evaluate expressions. X`driver/multi_dml.rs::join_sources:594/712`, run_multi_update:828/909 do the same | The statement/write operation must lend a scope across its loops. Do not alter sequential assignment visibility, implicit casts, ON DUPLICATE rebinding or first-error order. DML order keys are real expression evaluation, unlike the direct SortExec comparator. |
| Generated columns / CHECK / index maintenance | X`generated_column.rs::eval_over_dependencies:510–526` → eval_over_row:536–552 → expr.eval(ctx,row); the actual ASCII generated-column SQL fixture is XT`db_integration_ddl_types_source.rs:934–953`. X`union_scan.rs::fill_virtual_columns:601–624` also evaluates virtual expressions | Scope belongs to write/read/scan execution, not a generated-expression/table metadata Arc. Preserve per-row dependency order and existing post-result assignment conversions. |
| Partial-index / ADMIN CHECK non-StmtContext front | X`kv_table.rs::IndexConditionContext:423–442` carries only row/columns/TZ; index_condition_holds:3198–3230 → generated dependency evaluation. X`kv_table/index_entries.rs:582–583` evaluates old and new predicates; X`admin_check.rs::partial_index_rows:207–222` loops over rows | Explicitly propagate an operation-owned scope through this narrow context; a StmtContext hook alone cannot reach it. No worker in cloned KvTable/index metadata. ADMIN CHECK currently flattens a failure into Decode text; no whole-chain structured-source promise is made. |
| Join residuals / range arguments | X`join.rs::residual_verdict:1904–1922`, chunk_rows_verdict:1928–1950 → X`joiner.rs::eval_bool:198–211` → expr.eval; index_task_probes:1040/1113 evaluates bound.arg. Parallel probe/build code uses cloned/shared contexts | Coarse execution/lane capability must survive residual and range paths, with original lazy truth/NULL behavior and no native retry. A generic helper unit is not evidence for hash/merge/outer/semi/index joins. |
| Sort / TopN — no new scalar comparator | P`physical/inject_extra_projection.rs::inject_proj_below_sort:299–386` extracts scalar keys into a bottom Projection, then rewrites sort items to columns. X`sort.rs::eval_sort_key:280–299`, validation:302–310 and comparator:370–398 accept only Column/Constant | SQL ORDER BY ASCII is a positive **projection** route. Direct SortExec/TopN scalar keys remain rejected; constants/deferred constants must not be evaluated by a comparator. No ASCII production loan to sort.rs/topn.rs is justified solely by this family; protect the existing negative tests. |
| Hash/stream aggregates and group boundaries | X`hash_agg.rs::eval_agg_input:3567–3657` evaluates arg/extra_args with kind-specific NULL rules; X`hash_agg/group_key.rs::prepare:53–176` completes one expression column at163; X`stream_agg.rs::group_key:149–160` evaluates groups. X`vec_group_checker.rs::eval_group_item:439–457` and split_into_groups read endpoints then possibly the interior | Keep aggregate/lane contexts and endpoint demand, not a new universal batch. Same endpoint keys may intentionally suppress interior evaluation; a wrapper counter should reflect actual demand, not row count. Planner argument projections are also positive routes, not grounds to erase standalone aggregate entry coverage. |
| Window / shuffle | X`window.rs::same_row_keys:259–277`, range_bound:344–392, window_value:509/571–605 evaluate comparison/calculation/value/default expressions at different times. X`shuffle.rs::PartitionHashSplitter::split:342–387` evaluates by-item columns in its loop | Preserve partition/update-before-result scheduling, lead/lag default demand and exhausted RANGE suppression; one blanket child pre-evaluation changes behavior. Reuse each actual worker/partition-operation capability, not window metadata. |
| SHOW AST filters | S`show.rs::filter_show_output:645–667` loops into show_row_matches:508–529 → eval_in; ShowRowResolver:491–505 only implements get | Real hot non-StmtContext route. Scope outside the row loop, borrowed through the resolver; no per-row worker creation. This is not covered by SQL ProjectionExec or an executor StmtContext alone. |
| Ranger / cached-plan deferred helpers | P`ranger/points.rs::evaluate_static:527–529` directly calls eval(NoColumns,Row::empty); P`physical_plan_cache.rs::CachedPlanRebuildContext::evaluate:341–345` invokes its current callback or eval_expression_once(self) | Forward current execution capability where supplied; standalone calls stay explicit. Do not retain params, evaluated bytes or workers in a cached plan. This is capability/lifetime work only, not permission to change planner error suppression. |
| Rust SQL pushdown / PB decode | D`pushdown_catalog.rs` has no ASCII row. D`distsql_builtin.rs::supports_signature:83–85` delegates PbBuiltin::new; PbBuiltin has no Ascii arm and falls through the finite cast_types table whose default is None (:422–469). decode_pb_node:229–236 recursively decodes children then rejects unavailable signature | Preserve **unsupported** ASCII7003 on this Rust producer/decoder surface. The broader infer_pushdown name list is not proof of a concrete encoder/decoder. Add explicit negative coverage with valid children; do not add PB support just to claim route coverage. |
| Rust unistore versus TiKV cop/RPN | U`cophandler.rs::convert_expr_with_context:2073–2082` uses shared decoder for supported signatures; the legacy SimpleSig match defaults to convert_shared:2670, which calls the same PB decoder:2037–2046. No ASCII SimpleSig/body exists. TiKV K`lib.rs:834` already maps wire Ascii to the official kernel | Rust unistore ASCII remains negative; do not claim C4 calls or new request-context wiring there. Existing non-ASCII shared eval at2050–2057 uses Debug-text errors and is not an ASCII mapping precedent. TiKV's existing PB/RPN test is a regression guard for the retained authoritative implementation, not evidence that TiDB SQL reached C4. |

The table is a concrete audit of activation obligations, not evidence that any of these paths is switched today. Public entrypoints, custom Columns, native child/coercion errors, call demand and post-result metadata must all remain observable in future differential tests. Test a mixture of built/folded and genuinely runtime column/parameter expressions so constant folding cannot masquerade as projection/filter/aggregate/window runtime coverage.

### Reusable existing tests and the missing ASCII-specific cross-surface gate

Existing tests are to be rerun **unchanged**. Most protect schedules or metadata, not ASCII origin; explicitly do not relabel them as ASCII family acceptance.

| Existing exact test/location | What it already protects; what remains to add |
|---|---|
| D`tests/mod.rs::ascii_source_vectors_preserve_first_byte_and_string_coercion:511–531` | Eight SQL/value rows and raw invalid-UTF8 first byte255. Preserve every expected value. A future context-parameter plumbing adjustment to the direct helper call at529 may be necessary; no expected-value rewrite. Add actual C4 wrapper witnesses and the full admitted Datum-kind matrix separately. |
| D`rewriter/result_type_tests.rs::string_builtins_returning_an_int_match_go:1138–1157` | ASCII LongLong/flen3, alongside other families. Do not replace native declaration metadata with C4's canonical recipe descriptor. |
| Go `builtin_string_test.go::TestASCII:107–166`; Go `builtin_string_vec_test.go` ASCII fixture; K`impl_string.rs::tests::test_ascii:1933–1954` | Reuse authoritative expected byte semantics, including GBK/GB18030 196/202 versus UTF8 228, NULL/empty and non-ASCII text. Go vector scheduling is reference, not an unimplemented Rust vector route to invent. |
| X`projection.rs::prepared_program_is_shared_but_parameter_contexts_are_isolated`, `prepared_deferred_conversion_uses_current_statement_diagnostics`, `prepared_lazy_folding_keeps_warnings_execution_local`, `projection_workers_answer_the_serial_rows_in_order` | Existing prepared/context/parallel ordering guards. Add an ASCII-bearing expression, actual execution capability and count/retained/close assertions in a separate new fixture; a shared immutable program is not a shared worker. |
| D`evaluator.rs::vectorized_filter_preserves_selection_and_null_mask`, `constant_batch_preserves_deferred_rows_and_side_effect_ordering`; X`selection.rs::partial_fast_filter_preserves_warning_and_error_order:588`, `side_effecting_filter_returns_one_row_before_projection_observes_it` | Live-row/physical mask, deferred demand and no effects on dead rows. New ASCII nested behind lazy/dead branches must have zero kernel attempts; nonempty selected duplicates must preserve occurrence behavior. |
| D`constant_fold.rs::interval_scope_does_not_raise_child_warnings_during_fold`; X projection prepared-fold tests above | Preserve warning rollback/bookmarks and failed-fold decline. Add injected private-adapter failure through a real fold: same unfurled expression/diagnostic state, then later normal execution still uses C4 rather than the deleted body. No rewrite of native .ok() behavior. |
| S`tests_column_defaults.rs::a_function_off_the_whitelist_is_refused_even_when_constant:597`, `a_folded_default_stays_a_settled_literal:575`, `an_omitted_expression_default_is_evaluated:557`, `computed_default_whitelist_evaluates_the_allowed_function_shapes:1422` | Preserve current whitelist and fold/computed distinction. Add explicit direct ASCII negative, allowed nested/non-function-root positive, and existing eval-error→Unsupported behavior. Do not simply reuse RAND/UPPER expected rows as ASCII proof. |
| XT`db_integration_ddl_types_source.rs::index_on_multiple_generated_column3_string_functions:934–953` | Actual ASCII inside generated-column/index construction and indexed/unindexed reads; native expected577 stays unchanged. Add repeated-write/update and execution-origin assertions without changing that baseline. |
| X`sort.rs::sort_rejects_non_column_by_item_like_go:1484`, `sort_does_not_evaluate_constant_by_item_like_go:1508`; X`topn.rs::topn_rejects_non_column_by_item_like_go:1834`; P`physical/inject_extra_projection.rs::scalar_order_by_item_wraps_the_sort_in_two_projections:791` | Protect no scalar comparator and the actual projected SQL order path. Separate positive SQL ORDER BY ASCII test must see the bottom projection. |
| X`vec_group_checker.rs::equal_boundary_keys_skip_interior_evaluation_warnings:1887–1921`, `arithmetic_grouping_preserves_go_scalar_and_batch_error_order` | Boundary shortcut runs only two endpoint evaluations, or endpoints plus interior. Add ASCII demand tracing without altering this algorithm or borrowing D6 batch eligibility. |
| XT`window_executor_source.rs::partition_update_precedes_lead_lag_result_evaluation:829`, `moving_count_evaluates_only_entering_and_leaving_rows:375`, `nonranking_windows_do_not_compare_order_keys:1000`, `ranking_comparisons_follow_result_evaluation_order:1066` | Frame/partition/demand order. Add ASCII args/defaults/keys and native/frontend error prefixes; these current tests use other expressions and do not themselves prove ASCII. |
| D`distsql_builtin.rs::every_encoder_signature_has_a_typed_decoder:271–278` | Existing catalog consistency. Add explicit ASCII7003 producer/decoder/unistore refusal; absence from a positive-catalog loop is not negative execution evidence. |
| X`driver/errors/exec.rs::a_porting_boundary_reports_its_reason_and_no_rust_syntax:401`, `an_internal_executor_error_is_not_reported_as_unsupported:445` | Existing rendering boundaries and Clone use. New backend-envelope tests need class/origin/identity/cause preservation and honest generic wire rendering, not edited expectations for unrelated errors. |

No existing focused ASCII SQL test was found for join residuals, ordinary sort projection, aggregate/window arguments, SHOW, partial-index operations or allowed nested defaults. Those are **missing acceptance cases**, not exclusions from the family. New integration fixtures can use the existing public SQL driver/session/executor constructors, but must distinguish actual frontend attempt count, C4 official-wrapper count (including NULL), factory count, worker reuse and output result. The old-native benchmark labels alone prove none of those counts.

### Future minimum loan cuts — staged, not an omnibus write release

1. **Private C4/E prerequisites:** parent first accepts C's exact six-file gate and E's two new private owner/lease/adapter files. Those loans do not activate a public entry or earn a family. D does not request their pool design or its implementation.
2. **Native capability/error cut, separately reviewed:** D`context.rs` for the opaque local EvalError variant and separately approved Columns hook(s); D`lib.rs` and D`tikv/mod.rs` only for parent-owned type/module visibility; E's actual private adapter module for the sealed conversion constructor; X`driver/errors/exec.rs` for the exhaustive new rendering arm and its focused tests. No Cargo/new TiKV API, generic LocalError→SQL From impl, planner error-timing rewrite, or D4 diagnostic plan loan is needed by the proposed mapping. D5's existing context/mod freeze requires an explicit later reloan, not inference from this receipt.
3. **Smallest arithmetic deletion/dispatch cut:** exactly D`func.rs` and D`string_fn.rs`; D`tests/mod.rs` only if the existing direct helper call needs context plumbing (keep expected255); preferably NEW `tidb-expr/tests/ascii_activation_source.rs` for the direct AST/typed/domain/metadata/negative-PB matrix. This cut alone is **not** full hot-entry activation readiness.
4. **Operation-scope propagation cuts, only at actual owners/forwarders:** X`stmt_context.rs` and the operational files from the table: X`projection.rs`, `selection.rs`, `driver/dml.rs`, `driver/multi_dml.rs`, `generated_column.rs`, `column_default.rs`, `join.rs`/`joiner.rs`, `hash_agg.rs`/`hash_agg/group_key.rs`, `stream_agg.rs`, `vec_group_checker.rs`, `window.rs`, `shuffle.rs`, `union_scan.rs`, `kv_table.rs`/`kv_table/index_entries.rs`, `admin_check.rs`; S`show.rs`; D`constant_fold.rs`/`new_function.rs`/`lib.rs` and P`ranger/points.rs`/`physical_plan_cache.rs` only where the existing supplied capability is otherwise lost or a coarse operation scope is needed. This is a **conditional file-loan map**, not a claim every file needs its own pool or edit: E's reviewed forwarding/lifetime plan must first show which can inherit the same scope unchanged. Further session/custom Columns forwarders in EV-r2's preserved inventory require the same review. No whole-chain error-source preservation or altered fold/default result is authorized in these cuts.
5. **No unjustified algorithm/route loans:** no production sort/topn comparator, pushdown catalog, PB decoder, unistore SimpleSig, D6 numeric consumer, datatype coercion, parser, or registry edit merely to activate ASCII. Sort/PB/default exclusions are protected by new negative tests. Unistore needs a standalone negative test surface, not a new positive path. If later expanding its supported domain is desired, that is another explicit decision.
6. **Final family gate:** separate NEW executor/session integration fixture(s) for the positive/negative route matrix, preservation of old tests and warnings/errors, current parameter/cached-plan isolation, actual official-wrapper/factory witnesses, bounded runtime reuse/lifecycle and cold/steady-state large/mixed-byte measurements against the immutable AB-r1 cohort. Exact target/full build commands and artifact cohort remain parent-owned. A count1 proposal is possible only after source deletion, whole implemented-domain runtime evidence and parent's performance/coverage acceptance together; none is claimed here.

There is intentionally no claim of a globally minimal number of plumbing files before the capability-forwarding proof. The exact two-file computational cut is known; pretending it also establishes all execution owners would repeat the private-unit-adapter-as-family error. Likewise a list of broad files is not a permission to edit unrelated algorithms or expectations inside them.

### C4 LocalError → native: narrow structured feasibility under Clone + Eq

**Existing source constraints:** D`context.rs:53–55` derives Debug/Clone/PartialEq/Eq for EvalError, whose variants contain native structured data. K`local/spec.rs:88–121` has six LocalError variants, derives only Debug, and its std::error::Error impl does not expose a source chain. `tikv/components/tidb_query_common/src/error.rs:89–107` carries a non-Clone/non-Eq Box<ErrorInner>, with Storage(anyhow::Error) or Evaluate(EvaluateError). EvaluateError:8–33 distinguishes DeadlineExceeded, InvalidCharacterString{charset}, Custom{code,msg}, Other(String). A direct LocalError field cannot satisfy the existing derives; Display text cannot recover this structure, and numeric code1690 is not an ASCII/native-arithmetic provenance fact.

**Parent-approved design direction, NOT granted API/implementation:** add a TiDB-owned opaque `ExpressionRuntimeFailure` (name proposed) whose private immutable Arc payload owns the moved original failure plus locally assigned operation/entry phase and a native-only category view. The public EvalError variant contains only that TiDB wrapper; no public TiKV type, schema, context, compiled program or concrete TiKV error-return signature escapes. The private ASCII adapter alone constructs it from its closed boundary; native frontend EvalError moves out unchanged. Clone shares the same cause. Parent explicitly chose **same-cause identity Eq**: clone==original; two independently created errors with identical text are not equal. Implement PartialEq with Arc identity for this new wrapper, not with error Display/SQL code or a fabricated universal comparison of anyhow causes. Existing EvalError variants retain their normal value equality.

Raw LocalError/common-error payloads stay private and immutable, available to internal diagnostics without rebuilding them from strings. Public accessors, if approved, expose TiDB-owned class/phase/origin tags only. The opaque wrapper also needs an explicit **native-only Debug implementation**: deriving Debug through the raw Arc would expose TiKV internals via EvalError's existing Debug and planner/unistore Debug-text wrappers. Render only the native class/known phase there; retain the original cause for private typed inspection, not public downcasting or a string-classification channel. EvalError does not currently implement std::error::Error; do not silently broaden its public trait/source/downcast surface merely to hide a concrete TiKV value behind dyn Error. Whether an additional native-only cause view is useful is a final API review choice, not required for the minimal opaque owner. Primary failure ownership ends at the private adapter contract; downstream native suppression/loss remains as decided below.

Parent reviewed this section and **approved the following six-class outer-discriminant mapping and each exact fixed client message** as the future narrow error policy. This policy uses the existing generic1105/HY000 error channel; it does not diagnose a builtin arithmetic condition. Final Rust API/wiring review and a separate product loan are still required before implementation.

| Moved C4 LocalError | Approved native-only policy class/origin | Approved exact fixed class message |
|---|---|---|
| InvalidSpec(String) | InvalidSpecification / specification-or-worker-contract | `Expression runtime specification failure` |
| InvalidBatch(String) | InvalidBatch / batch-or-result-contract | `Expression runtime batch contract failure` |
| BindingContract(String) | InputContract / demanded-input-contract | `Expression runtime input contract failure` |
| HostContract(String) | HostContract / unexpected-host-contract | `Expression runtime host contract failure` |
| ResourceLimit(String) | ResourceLimit / local-runtime-budget-or-reservation | `Expression runtime resource limit exceeded` |
| Evaluation(tidb_query_common::Error) | Evaluation / backend-error, retaining the complete original ErrorInner and EvaluateError/StorageError underneath | `Expression runtime evaluation failure` |

Do not parse a reason string to refine these classes. The actual invocation point can label Prepare, Invoke or Observe/Return as its known local phase; LocalError alone supplies **no exact kernel-failure site**. C4 eval_one/finish_invocation at K`local/batch.rs:1045–1078` preserves a primary Err even when cleanup also fails and retires an unhealthy worker; the native wrapper must preserve that same owned primary. A wrapper invocation counter is not a LocalFailureSite and cannot be relabeled as D4's checked203 call. For Evaluation, retain the inner Storage versus Evaluate discriminant and the original typed payload internally; Custom{code,msg} remains data, not authority to select a native SQL condition.

ASCII's actual ready-Bytes arithmetic has no admitted intrinsic SQL error: the body always returns Ok for a non-NULL input, and the generated nullable wrapper handles NULL. Therefore **no C4 LocalError variant currently justifies** EvalError::DataOutOfRange/IntOverflow, Conversion(TerrorError), Json, Vector or an existing Unsupported fake. ResourceLimit is not the executor's statement-OOM8175 (which requires query-memory/connection context at X`executor.rs:55–61`), nor a packet limit or MySQL arithmetic overflow. No D4 translation, warning-site/context/severity policy, native retry, or replacement of the original cause with a formatted SQL string is proposed. Adapter-owned pool/reentry/closed/bridge errors should keep their own native origin tags; they must not impersonate one of these six C4 errors.

**Existing honest wire path and real Clone consumers:** X`executor.rs::ExecError:30–35` already clones and nests EvalError. Planner PlanErrorKind::Eval retains it (P`plan_base.rs:519–523`); X`driver.rs:532–534` clones the typed Eval into ExecError, not a text classifier. X`driver/errors/exec.rs::to_mysql_error:67–81` calls exhaustive eval_to_mysql_error:117 onward then marks the result from_evaluation (apart from its existing charset exception). X`driver/errors/mod.rs::MysqlError::unknown:143–145` uses new(1105,...) and new:95–99 derives HY000 from the existing mysql_state registry. Parent approved that existing generic channel for these new backend classes. After a separate wiring loan, add one deliberate exhaustive arm rendering the approved fixed native class message, never `{LocalError}`/`{LocalError:?}` as a fake builtin SQL failure. Existing tests explicitly prohibit Rust Debug syntax and preserve internal-versus-unsupported distinctions.

The origin bit matters: S`lib.rs:2247–2252` suppresses an extra SHOW WARNINGS error row for evaluation-origin1105 (among other codes). Parent approved preserving the actual existing Eval from_evaluation path in the future new arm; do not set or clear the bit from a TiKV code or rebuild arbitrary SQLSTATE. Wire conversion necessarily emits a native code/state/message triple and is not a lossless cause transport. Native typed envelopes before that terminal renderer and existing lossy wrappers after it are separate claims.

### Explicit parent decisions and remaining decision points

Already decided in Round5 (communicated during this source audit):

1. **Preserve native fold suppression and DEFAULT remapping.** D`constant_fold.rs:158` .ok()? and warning rollback:89–106 remain; X`column_default.rs:730–731` eval→Unsupported remains. No broad planner/error-timing/error-propagation loan. Structure/primary-cause preservation is claimed at the private adapter, not through every downstream wrapper. ADMIN CHECK Debug-text flattening and unistore's shared Debug-text path are additional observed losses, not fixes smuggled into this work.
2. After eventual deletion **every real attempt uses TiKV**. A native fold-decline followed by a later normal execution is the pre-existing wrapper schedule, not a new probe/fallback/native replay. No retained old ASCII body may service that later attempt. Test the original expression/diagnostic state and both real-attempt witnesses without changing existing expected results.
3. Direct non-Column/Constant sort/TopN rejection and root DEFAULT ASCII whitelist refusal stay unchanged. Projected SQL ORDER BY and permitted nested/default-fold forms require their own positive tests, not a widening of the negative surfaces.
4. Opaque native wrapper + moved primary cause + Clone Arc and explicit same-cause identity Eq are approved **directions**. Native frontend EvalError moves unchanged. Final Rust API/wiring and product write rights remain ungranted.
5. **D-r23 parent policy acceptance after reviewing the error section:** the six LocalError outer discriminants map to the six native policy classes in the table, using those exact fixed client strings through existing MysqlError::unknown1105/HY000 and the existing Eval from_evaluation path. Opaque Debug exposes only native class/known phase; raw cause stays internal; no new public EvalError Error/source/downcast surface. Pool/bridge/frontend origins stay distinct and frontend errors move unchanged. No TiKV custom-code,8175 or D4 mapping. This is policy approval only: the current E two-private-file cut must not edit context, and any eventual Rust API/wiring needs its own review and explicit product loan.

Still require parent review/explicit release:

- Final TiDB wrapper/variant/accessor API, concrete Rust representation and reexport/wiring locations, implementing the now-approved class/message policy. No direct From<LocalError> spanning all local domains should be introduced by convenience.
- The exact minimal capability/error/helper/test file loans, reconciled with E's two-file proposal and the frozen D5/D6 modules; which operational contexts need scope forwarding edits versus already inheriting an approved scope. No per-row compile or shared-metadata runtime shortcut substitutes for missing loans.
- The parent-owned matrix of new positive/negative SQL/custom-context tests, full unchanged baseline comparison, exact C4 execution witnesses and performance/artifact gates required before deleting the old helper computation and proposing ASCII count1. PB/unistore expansion, if ever desired, is separate; it is not a prerequisite fabricated for today's implemented domain.

### Validation performed / not performed in Round5

Only this receipt changed. D executed **`pwd`**, with workdir `/home/agent/tidb`, returning that same workspace. All source inspection used read/glob/grep tools; there was no Rust formatter, compiler, Cargo/test/lint run, benchmark, fixture generator, algorithm edit, API edit, old expected-value change, guide edit or ExecPlan edit. The first attempted receipt edit hit the filesystem observation precondition (file not yet read in this turn), then succeeded after reading the actual header; no sandbox escalation or alternate write path was attempted.

Reproducible search scopes/patterns used with the grep tool included: TiDB Rust `(?i)\bascii\b` then the focused `fn (eval_)?ascii\b|"ASCII"\s*=>|"ascii"\s*=>|AsciiSig|ScalarFuncSig::Ascii|ascii\(&\[`; TiKV Rust `fn (eval_)?ascii\b|builtinASCII|ScalarFuncSig::Ascii\b`; TiDB Go expression `builtinASCII|asciiFunctionClass|TestASCII|ast.ASCII`; the exact Rust PB/catalog/unistore files for ASCII/7003; LocalError's enum/trait implementation; EvalError/Clone/error rendering/origin users; and consumer `.eval`/function declarations. Two speculative file reads (`local/error.rs`, `driver/partial_index.rs`) found no file; actual definitions were located at K`local/spec.rs` and X`kv_table.rs`/`kv_table/index_entries.rs` and inspected there. No missing-file attempt is counted as inspection evidence.

The source inventory/feasibility and parent's explicit decisions are the deliverable. Actual C4 compilation, new public wrapper traits, all newly proposed ASCII SQL cases, operation-scope propagation, kernel-origin/runtime counters, deletion closure, performance, error-renderer behavior and full family acceptance remain **unverified/unimplemented** here. D6's prior parent18/full1310/4/93 evidence below is unchanged and is not an ASCII gate. The D-r23 follow-up records only parent's narrow error-policy acceptance using receipt read/edit/grep tools; it runs no shell command, build or test and changes no product or old expected value.

## D6 parent execution amendment — focused18 and unchanged full failure blocks

**Actual parent runs, not D execution or inferred success.** After the import-only correction below, parent ran the focused numeric-batch module and complete tidb-expr library. D inspected these existing logs without running a build/test or changing any product:

- `logs/tidb-d6-numeric-batch-import-corrected.log:2086–2106`: running18, all18 named D6 tests passed, zero failed/ignored,1389 filtered. This establishes execution of the intended nonzero module, not only successful compilation.
- `logs/tidb-d6-expr-full-comparison.log:2085`: running1407 tests; `:3494–3528`: the complete four failure blocks and **1310 passed/4 failed/93 ignored**, zero filtered. The suite remains failed, not green.
- The four failures are `pushdown_catalog::tests::ifnull_string_column_literal_uses_go_signature_and_column_collation`, `tests::builtin_info_json_math_source::exp`, `tests::builtin_math_misc_op_source::vectorized_builtin_op_func`, and `time_fn::tests::str_to_date_partial_formats_follow_no_zero_date`. Parent reports the **complete** blocks match C3c/I with **only thread IDs normalized**; this comparison result is attributed to the parent, not a new D-run normalizer or a names-only equivalence claim.

Parent is capturing the frozen manifest/format and guide update; independent Agent A read-only review of native eligibility, ownership and retained accounting is in flight. Passing18 tests does not rule out a newly found admission/ownership/resource gap. **No code grant accompanies this validation notice; all four products remain frozen.** D changed this receipt only, preserving the corrected four-file hashes below and the first compile failure history.

Scope remains one private calculated signed203 native-numeric tree with actual suite/global-entry evidence. Public EvaluatorSuite::run stays Native; no filter/grouping activation, fallback substitution, generic ordinary/control composition, D4 diagnostic conversion, ResultMetaId, PB route, warning/context/severity policy, benchmark claim or migrated-family credit follows. This is not final review acceptance or proof of concurrent metadata/value alias safety or total allocator peak. Source budgets and the owner-stable interval/retained-next-copy limits remain explicit.

## D6 first-compile amendment — existing enum trait import only

Parent's first focused compile actually exited101 with **one E0599 and ZERO tests run**. D read `logs/tidb-d6-numeric-batch-first.log:1860–1874`: numeric_batch.rs's checked `tipb::ScalarFuncSig::from_i32` requires the already implemented `protobuf::ProtobufEnum` trait in scope. This was D's missing import, not a failed runtime assertion, new conversion/API requirement or justification to bypass checked conversion.

Parent released **only numeric_batch.rs** for this correction. D added `use protobuf::ProtobufEnum;`; the catalog lookup, checked conversion/filter and all other logic/tests are unchanged. New SHA256 **`3b3a94d1cb336fd6925e1ada67da80822a9fab2233f1aff3ee2e59b123c1b657`** supersedes initial `0e320697791def9b6a8438c1b68ba2ed6b51548a2c88352069eeae892a9b06af`. All other three D6 hashes were reread unchanged. No Cargo/dependency/trait implementation, unchecked numeric conversion, existing API or other product changed.

D ran only the following chained scoped commands from expression-unification/tidb (the hash command used explicit expanded paths), all successful:

```sh
rustfmt --edition 2021 --config skip_children=true rust/crates/tidb-expr/src/evaluator/numeric_batch.rs
rustfmt --edition 2021 --check --config skip_children=true rust/crates/tidb-expr/src/evaluator/numeric_batch.rs
sha256sum rust/crates/tidb-expr/src/{evaluator/numeric_batch.rs,evaluator.rs,scalar_function.rs,evaluator/numeric_batch_tests.rs}
```

At this correction's handback, D had run no build/test and did not yet claim compilation or18 passed tests. The subsequent parent retry is recorded above; the original compiler error remains part of this receipt.

## D6 implementation source checkpoint — exact four-file release

**Historical source checkpoint; subsequent parent execution is recorded above.** Parent reported C3c joint validation: native RPN608 (32 C tests plus1 B), aggr40, unchanged measured layout400/128/400/176/152, full caller1292 passed/4 identical complete baseline failure blocks/93 ignored, with only thread IDs normalized in its comparison. Parent then explicitly released `evaluator.rs`, the existing eligibility/worker extraction only in `scalar_function.rs`, NEW `evaluator/{numeric_batch,numeric_batch_tests}.rs`, and this receipt. D implemented that exact block and froze it for parent validation. C3c's seven files, D5's five files, D4 diagnostic code and all helpers/bridges/manifests remain untouched.

### Actual implemented boundary

- `scalar_function.rs` now factors the **unchanged native eligibility sequence** into crate-private `numeric_batch_candidate` and non-Clone `EligibleNumericBatch`. Public try_eval_numeric_batch calls this once and consumes the candidate through eval_native. The empty guard remains after native eligibility/deferred-wrapper get_type. Native arithmetic/eligibility rules and diagnostics are unchanged; neither D6 admission nor error discovery calls a value worker as a probe/replay.
- `evaluator.rs` shares its real dispatch through private `run_with_consumer`. Public run still selects zero-state NativeNumericConsumer. Mandatory private preflight runs before the earlier Decimal worker; the native no-op preflight does not add SQL observations. Decimal priority is preserved, the **current** global getter is called once at its original branch position, and only then does the native candidate produce a one-shot private NumericBatchInvocation joining actual program/calculated slot/output/input/candidate. Native row fallback remains; the mandatory consumer's row_route returns admission failure instead of evaluating a row or broadcasting a fallback constant.
- NEW `evaluator/numeric_batch.rs` owns a separate bounded source lowerer, complete detached native source/type/collation metadata, exact signed203 catalog fact, bindings, LocalExpr, NumericBatchFacts, immutable native-program Arc and worker-local LocalNumericBatchProgram/LocalEvalState. No D3/D4 plan conversion, raw LocalProgram escape or D5 API/helper export exists. The private entry requires exactly one calculation at output0 and no direct-column mapping/owner-transfer helper.
- Source/all incoming FT payloads and flat construction work are capped through the accepted checked_snapshot_payload_bytes observer **before** snapshots, projection or Eq. Every invocation reruns the bounded source closure, checks current source snapshots plus cached collations, full schema/column count/layout/length, output layout, indexes and actual selected physical rows. Unsupported Decimal/control/value work cannot precede mandatory rejection. Source metadata/input value aliases require an explicit stable owner interval; no atomicity or concurrent-alias snapshot equivalence is claimed.
- Selection is copied only from the actual invocation Chunk Sel; absent Sel yields bounded identity. More than1024 selected occurrences and malformed maps fail before value effects. A large physical universe with a small selected map remains valid. The binding reader uses physical_row and validates the same native Int/NULL before B erases kind; it never reads a physical index through get_row again. Test-only hostile providers still go through the genuine suite token and native Chunk preflight.
- Evaluation calls the own C3c reported facade once for both caller raw/reported methods. Raw strips that same error without rerun. Opaque NumericBatchFailure retains the owning source on a C report and validates source-ordinal lookup against its own call/binding records. No foreign report/table constructor, native203 message conversion, severity policy or ResultMetaId exists.
- Result materialization checks Int/length, measures actual C Int/bitmap storage plus the derived selection Vec and native Vec<Datum> capacities while they coexist, then uses fixed computed-Int B metadata. Retained-at-next-copy/publication policy remains distinct from allocator/callback peak, immutable program heap and external output Chunk buffers. Full calculated output is produced before the unchanged shared dispatcher appends it.

### Eighteen focused tests — source scope and parent run above

The actual module/filter is `evaluator::numeric_batch::tests::`; source search found18 `#[test]` functions. They cover own203 facts/all-node call IDs/declarations; real native/private result comparison; current flag changes and once-only observation; actual nonvectorizable suite refusal; unchanged native Decimal priority/leaf nonadmission; foreign suite and privately corrupted owner links rejected before Decimal/control/parameter work; native-identifiable whole-left versus whole-right failure winner; right-child failure before a potential parent overflow; complete-leaf-phase reads and reported occurrence/physical mapping; virtual constants and[]/1/1024 selected occurrences over larger physical universes; native1025 phase witness with mandatory zero-effect refusal; UInt-before-erasure input site; raw/reported error parity and warning/fresh-retry state; source alias detachment/staleness/incoming byte-first refusal; malformed/empty schema/layout/selection/output rejection; all-node PB/flags/types/shape refusals; depth/retained-resource failure and late-prefix preservation; and actual source/selection/native capacity accounting using public vector constructors.

Parent-owned target request (not run by D), from the Rust workspace with the parent's pinned environment:

```sh
cargo test -p tidb-expr --lib evaluator::numeric_batch::tests:: -- --nocapture
```

At the source handoff18 was only the expected source-defined count. The later parent execution now records actual18 and the complete caller failure-block comparison above; independent review remains in flight. No green suite/PR readiness/performance/family-completion claim follows.

### Frozen exact four-file manifest

Paths are relative to `tidb/rust/crates/tidb-expr/src/`:

| File | SHA256 |
|---|---|
| `evaluator.rs` | `e3f706fcee32816e28982345136505a1433d9598f318f8627718dfffd8be46b6` |
| `scalar_function.rs` | `b986c5a4a5538677d655ec9069d4c35ea10e3a7f552894774c582e7ed33e95b4` |
| NEW `evaluator/numeric_batch.rs` | `3b3a94d1cb336fd6925e1ada67da80822a9fab2233f1aff3ee2e59b123c1b657` (import-only correction) |
| NEW `evaluator/numeric_batch_tests.rs` | `75c44334b5e0af7898f06a23cf72635367ddeb033ad2d16e1e262340890f9ac9` |

D ran the following scoped operations from `expression-unification/tidb`; all succeeded. Braces below abbreviate expanded explicit paths actually passed to the tools:

```sh
rustfmt --edition 2021 --config skip_children=true rust/crates/tidb-expr/src/{evaluator.rs,scalar_function.rs,evaluator/numeric_batch.rs,evaluator/numeric_batch_tests.rs}
rustfmt --edition 2021 --check --config skip_children=true rust/crates/tidb-expr/src/{evaluator.rs,scalar_function.rs,evaluator/numeric_batch.rs,evaluator/numeric_batch_tests.rs}
# After strengthening the hostile owner-link fixture, only this file was formatted again:
rustfmt --edition 2021 --config skip_children=true rust/crates/tidb-expr/src/evaluator/numeric_batch_tests.rs
# The same four-file --check above then ran again.
git status --short -- rust/crates/tidb-expr/src/{evaluator.rs,scalar_function.rs,evaluator/numeric_batch.rs,evaluator/numeric_batch_tests.rs}
git diff --check -- rust/crates/tidb-expr/src/{evaluator.rs,scalar_function.rs,evaluator/numeric_batch.rs,evaluator/numeric_batch_tests.rs}
sha256sum rust/crates/tidb-expr/src/{evaluator.rs,scalar_function.rs,evaluator/numeric_batch.rs,evaluator/numeric_batch_tests.rs,tikv/lineage.rs,tikv/lineage_tests.rs,tikv/mod.rs,tikv/catalog.rs,tikv/context.rs}
git diff --stat -- rust/crates/tidb-expr/src/{evaluator.rs,scalar_function.rs}
git diff -- rust/crates/tidb-expr/src/evaluator.rs
git diff -- rust/crates/tidb-expr/src/scalar_function.rs
```

Format/check and status/diff-check/hash commands were chained with `&&` as recorded in the session. The two existing files are tracked; the two new files are **untracked**, so Git diff-check does not validate those new contents. Content searches separately found18 tests, no test-only `VectorValue::from(vec!...)` dependency fixture, no whitespace/conflict markers, and no native value evaluation/try_eval replay/row compiler/ResultMetaId/D4 adapter in numeric_batch.rs. Format is parse evidence, not typechecking. The tracked scalar diff also shows earlier D1/D4 provenance/renderer-access hunks relative to Git HEAD; those predate this release and were not D6 edits. The new D6 scalar change is only the eligibility/candidate/native-worker extraction.

All five reread D5 hashes exactly match the accepted manifest: lineage `0c7a2a84…92414c`, tests `dbf91cab…2c1055`, mod `ea227f75…cdf9c`, catalog `2a159d09…0aafc3`, context `e51dec2c…bb601`. D has handed back this coherent four-file source checkpoint and stopped product writes. At this source-only handback Rust typechecking/runtime validation was still unexecuted; the subsequent parent results and still-pending independent review are recorded above. Parent owns all builds/tests. No lint, benchmark, production executor activation, native diagnostic adaptation, filters/grouping, ordinary/control composition or new migrated-family credit was verified or claimed.

## D6 reviewed proposal — historical D-r18 source design

**Historical D-r18 authority/status:** parent initially accepted this four-file proposal **IN PRINCIPLE, only after C3c's separate gate**, with permission for this receipt alone. The subsequent C3c gate, explicit exact D6 implementation loan and first-compile amendment are recorded above; release was not automatic. The five D5 products/APIs and D4's diagnostic plan remain frozen. E's evaluated-value ASCII design is independent and is not mixed into this numeric domain. This section preserves the reviewed design handoff, not another ExecPlan or runtime evidence.

### Actual native entry/schedule anchors

Paths below are relative to `expression-unification/tidb/rust/crates/`, with line numbers from the read-only design inspection:

| Source | Existing fact, not a proposed inference |
|---|---|
| `tidb-expr/src/evaluator.rs:303–347,353–384` | EvaluatorProgram privately owns calculated expressions/output indexes, cached suite-vectorizable classification and direct-column mapping; EvaluatorSuite owns its Arc and execution-local owner-transfer helper. |
| `tidb-expr/src/evaluator.rs:392–421` | Run first selects actual suite vectorizability, then tries the Decimal-specialized path at408, then calls **current** `ctx.enable_vectorized_expression()` at412, then numeric admission/evaluation at414. Root shape alone is not this consumer decision. |
| `tidb-expr/src/scalar_function.rs:4447–4458` | Decimal priority returns false without appending for a non-NewDecimal root. D6's signed LongLong proof explains why this earlier path does not consume its admitted source; the public ordering must still remain unchanged. |
| `tidb-expr/src/evaluator.rs:422–459` | Unsupported numeric roots retain whole-expression constant/row routes; nonvectorizable suites use row/select-list order. Calculations complete before direct-column owners move. |
| `tidb-expr/src/context.rs:540–543` | Global vectorization is a Columns method, not an immutable compile-time fact. Do not cache it or call it an extra time for D6. |
| `tidb-expr/src/scalar_function.rs:1217–1256,3899–3945,4197–4225` | Native numeric eligibility is an existing recursive rule. The public try_eval_numeric_batch currently combines admission and actual evaluation. Calling this effectful function as a probe would perform/replay work. Plain leaf roots return None; deferred-wrapper/get_type and empty handling have their own existing order. |
| `tidb-expr/src/scalar_function.rs:3951–3978,3986–4010,4024–4063,4075–4086` | Constants broadcast for a nonempty chunk; columns follow selected physical indexes; a call evaluates whole left then whole right, then its parent integer/NULL lanes. Left NULL does not skip the right subtree, even at N1. |
| `tidb-chunk/src/chunk.rs:239–259,417–424,473–476` | num_rows is selection-aware, physical_rows is not, sel borrows the actual occurrence map, get_row applies it and physical_row does not. Repeated selected physical rows remain repeated occurrences. |
| `tidb-expr/src/evaluator.rs:124–172,188–257` | Filters evaluate/prune physical demand, use scalar arithmetic fallback, and reapply selection membership; this is not the projection numeric-batch universe. |
| `tidb-executor/src/vec_group_checker.rs:104–128,145–176` | Grouping evaluates first/last boundaries before interior work and may omit its later numeric batch. It cannot obtain D6 evidence. |
| `tidb-expr/src/evaluator.rs:825–861` | The existing native scalar/batch NULL/error divergence test must remain unchanged; its mul/Long source is not itself a D6-positive declaration. New positive witnesses must use the exact closed LongLong203 source. |
| `tidb-expr/src/tikv/context.rs:37–99` | Existing D binding preflight checks full schema, column count/layout/length and physical selection; demanded reads use physical_row. This is source guidance, not a grant to expose/reuse a row plan or modify D5 APIs. |

D also read C3c-proposal-r0 (`runtime-contract.md:1687–1767`), the parent's current plan rows69/354/502, and the TiKV maintenance README/repository overview/coprocessor guide. Those records do not establish a new compiled D6 gate.

### Proposed exact four-file loan, not yet granted

All paths are under `tidb/rust/crates/tidb-expr/src/`:

1. **`evaluator.rs`** — narrowly factor the current run dispatch into a shared private consumer seam, mint the per-invocation suite token at the actual native branch and add the private mandatory-D6 entry/module wiring. The public run still selects the native consumer; do not activate C3c in it or change its native error/fallback/publication/owner-transfer behavior.
2. **`scalar_function.rs`** — narrowly extract existing numeric eligibility into an opaque eligible candidate and an eligible native worker. Keep exactly the original eligibility, requested-domain/deferred-wrapper ordering, zero-row guard, arithmetic implementation and public try_eval_numeric_batch behavior. The native consumer consumes the candidate, rather than invoking the public effectful function again. This needs an explicit reloan of the currently frozen D4 file; it does not release D4 diagnostics.
3. **NEW `evaluator/numeric_batch.rs`** — independent D6 source lowering, detached metadata/bindings, immutable source-site ownership and worker-local C3c program/state, plus the private consumer/materializer.
4. **NEW `evaluator/numeric_batch_tests.rs`** — entry, ownership, source, schedule, selection and resource proof matrix below. Do not rewrite existing tests to fit D6.

The child module can inspect the evaluator's private program fields without making D5 helpers public. No tikv/mod.rs change, D5 API alteration, raw LocalProgram escape, D4 plan relabel/conversion, new kernel registry, executor/Chunk/helper/bridge/Cargo/guide edit, public route or automatic next-stage loan is included.

### Proposed entry seal and own-program chain

These are design descriptions, not existing API names:

- The first private consumer admits **one calculated expression**, with a genuine binary PlusInt203 root and a closed signed LongLong tree. No ColumnSwapHelper/direct-owner transfer or multiple-output migration is included. Plain constant/column roots are refused even though C's generic numeric facts admit them: the real native try entry does not choose them.
- Preparation owns the exact `Arc<EvaluatorProgram>` and calculated/output slot, bounded all-node source snapshots, full native SQL declarations/bindings, one LocalExpr, NumericBatchFacts and a source-call table. Each worker owns its own LocalNumericBatchProgram and LocalEvalState. The table/spec/program cannot be mixed with a foreign or row-profile artifact merely because IDs or values compare equal.
- At **each invocation**, use the same native dispatcher branch and eligibility logic. A private, non-cloneable, non-publicly-constructible token must bind the real program/slot/root, the actual input Chunk and the current consumer decision. It is minted only after actual suite vectorizability, actual preceding Decimal-path refusal, **one current global-flag read at the native point**, and actual native numeric eligibility. Consume it synchronously; never return it for later reuse, cache a boolean, or offer a raw arbitrary-expression/selection bypass.
- The default public consumer stays native. A private mandatory D6 consumer refuses nonvectorizable, disabled, leaf, row-route, unsupported or stale-source cases before value effects. Absence of a D6 token must not silently fall through to row evaluation. No native retry follows a C effect or error.
- Pure D6 source/binding/resource preflight must precede its native eligibility probe. The closed source excludes parameters/deferred wrappers, so D6 does not treat a generally context-reading native get_type path as a universally effect-free predicate. Factor and preserve native behavior; do not evaluate try_eval_* to discover a route or rerun it after eligibility.
- Private admission/resource/reported failures remain distinct from native EvaluatorError. Raw/reported evaluation uses this same own C3c program and invocation. No native203 diagnostic text conversion, warning-site/context/severity policy or D4 adapter composition is implied.

### Independent source, binding and publication obligations

1. Require exact binary203/unit call metadata, signed actual LongLong declarations, strict Int/NULL literals and genuine nonhybrid signed columns at every node. Refuse all PB state, UInt, Tiny/untyped NULL, hybrid/ARRAY/unsupported flags, casts/other arithmetic, hosts, mutable/deferred/parameter/correlated/virtual sources, and either control/ordinary composition direction. Preserve full accepted native type/source metadata rather than normalizing it to a prototype.
2. Bound nodes/depth and all logical source/type/name payloads before snapshots, projection or equality. At invocation, bound the entire incoming row_schema and current source metadata **before** allocating Eq/snapshot work, then validate cached collations, complete declarations, native column count/layout/length, indexes, and source/program ownership. C's signature/profile snapshots do not replace caller-native proof.
3. Metadata owners must remain alias-stable across observe/check/copy/publication; input values/column owners must also remain stable across the borrowed invocation. In particular native integer columns hold a column read across a leaf phase whereas singleton binding callbacks can have shorter borrows. Do not claim concurrent-alias snapshot equivalence or atomicity from these checks. Earlier SQL building or EvaluatorProgram construction effects are not undone.
4. Derive the occurrence universe from this invocation's actual `Chunk::sel()` only. Some(sel) preserves its ordering and duplicates; None constructs bounded identity over physical_rows. Reject malformed rows and **more than1024 selected occurrences** before values/output/owner movement. A large physical universe with a small selected map is not itself over width. Empty selection still validates source/route/schema, but performs no value read or kernel work.
5. Read the already-physical row exactly once per demanded binding occurrence; never feed it back through get_row. Validate that same returned native Datum is Int or NULL before B erases kind. Do not reinterpret UInt bits or obtain a second provider metadata/value read. Immutable constants remain broadcasts, not repeated callbacks.
6. C performs whole-left, whole-right, then parent kernel-only lanes under one invocation budget. The caller does not tile/replay/root-loop the program or infer a failing lane. Reported sites join only the own all-node source-call table and preserve occurrence versus physical coordinates; input-slot identity is not globally unique leaf provenance.
7. Output is computed Int/NULL under the own checked declared boundary, not selected-origin identity. No ResultMetaId, boolean/leaf ID reuse or value-based provenance. Validate result shape, measure actual C-source/native Vec coexistence before subsequent copy/publication, and keep retained-at-next-effect limits distinct from allocator/callback peak claims. Existing/external Chunk owners are not silently covered by a value-buffer cap.

### Focused proof matrix for a later parent-run gate

No D6 test file or executable test count exists yet; this is the required matrix, not test results.

| Gate | Required observable proof |
|---|---|
| Actual entry/ownership | Same shared native dispatch and eligibility probe; wrong suite/slot/source snapshot rejected; token cannot be independently constructed, retained, cloned or supplied to another program. |
| Current global decision | Reuse a worker while changing the current flag; exactly one getter call at its native position, no preparation-time caching or duplicate probe; disabled/nonvectorizable routes refuse without C/native value effects. |
| Native compatibility | Existing public consumers, Decimal priority, row fallbacks, error prefixes and owner-transfer tests remain unchanged. Private unsupported/control/leaf roots do not silently use a row route. |
| N1 NULL witness | Signed LongLong `plus(NULL, plus(MAX,1))` still demands/errors in its right child; no row NULL-shortstop shortcut. |
| Operand phase order | A later-occurrence left-child failure suppresses an earlier-occurrence right-child failure. A right-child failure precedes a potential earlier parent-kernel overflow. Verify winning source sites, not just is_err. |
| Selection universe | Empty/identity/reverse/[2,0,2], constant-only virtual chunks,1024; actual Chunk Sel is applied once, duplicates retain distinct occurrence indexes and expected physical rows. |
|1025 boundary | Reject with no effects. Preserve the native all-signed tiling witness (L[0]=0,L[1024]=MAX,R[0]=MAX), but do not claim positive1025 equivalence or root tiling. |
| Binding/source rejection | Stale metadata, oversized incoming metadata before Eq, cached-collation/type/layout/count/index mismatch, PB/unsupported descendants and hostile UInt replies; validate even for empty selection. |
| Diagnostics state | Raw/reported parity, actual call/occurrence/physical sites, demanded input failure prefix, no later-phase replay; fresh sites after success/empty/retry; resource/preflight/publication errors stay unsited. No native-message translation claim. |
| Ownership/resources | Worker-local compiled state with immutable own source; no foreign table/row-plan join; actual retained source/materialized output/selection capacity and tiny limits, without peak assertions. |
| Excluded consumers | Filters/grouping cannot mint the suite seal or reuse their physical membership/boundary demand as the selected occurrence map. |
| Regression comparison | Parent chooses exact compiled filters after implementation exists, retains old native/D1–D5 tests, and compares full caller failures against the accepted baseline rather than calling the full suite green. |

### C API observation, prerequisites and validation limits

The live C3c implementation inspection found `NumericBatchFacts::sql_native_numeric_batch(spec,schema,sites,limits)` at local/profile.rs:235, `OrdinaryCallSite::sql_native_numeric_batch(ordinal,source)` at:100, `compile_numeric_batch(spec,schema,cx,facts)` at local/compile.rs:318 and LocalNumericBatchProgram's raw/reported binding methods at local/batch.rs:611/626. C's comments explicitly leave native entry evidence and metadata-byte policy to the caller. These were **under-implementation source observations**, not a stable compiled gate or authority to fabricate a missing API. Recheck exact frozen signatures and accepted C3c behavior with parent before any D6 implementation loan.

The unresolved native API seam is the proposed eligibility/consumer seal itself; it requires the two existing-file loans, not a bool argument or D4 plan reuse. Correctness risks are changed getter/probe order, whole-expression fallback substitution, duplicate selection application, stale alias-backed metadata, missing independent layout proof and source/site joins. Compatibility of the proposed factoring is untested; performance is unmeasured and no SIMD/throughput claim is intended. D used read/grep/glob tools only during feasibility, ran **no shell validation commands**, and has now changed only this receipt as parent authorized. No source format, build, test, lint, benchmark or new family credit is claimed.

## D5 acceptance amendment — private control checkpoint, not activation

Parent accepted the corrected exact-five-file checkpoint and returned/froze all five product-file ownerships. D reread the actual logs and accepted manifest:

| Parent-run gate | Actual result / evidence |
|---|---|
| Corrected D5 filter | **18 passed,0 failed,0 ignored,1371 filtered**; `logs/tidb-d5-lineage-fixture-corrected.log:2041–2061` lists all18 tests |
| Full caller comparison | **1292 passed,4 failed,93 ignored,1389 discovered**, not a green full suite; `logs/tidb-d5-expr-full-comparison.log:3433–3463` |
| Failure compatibility | Parent programmatically compared **all four complete failure blocks** against its C3b baseline, normalizing only thread IDs, and reported identical blocks. D reread the current full blocks; D did not run that comparison program itself. |
| Accepted source manifest | `logs/tidb-d5-accepted-sources.sha256:1–5` matches the exact five hashes below, including corrected tests `dbf91cabe…2c1055` |
| Parent formatting | Parent reports scoped five-file formatting passed. Untracked Git diff remains empty and is not content-diff evidence. |

The four unchanged names are `pushdown_catalog::tests::ifnull_string_column_literal_uses_go_signature_and_column_collation`, `tests::builtin_info_json_math_source::exp`, `tests::builtin_math_misc_op_source::vectorized_builtin_op_func`, and `time_fn::tests::str_to_date_partial_formats_follow_no_zero_date`. Their current assertion/panic contents remain the IFNULL remote admission assertion, EXP FloatOverflow match, negative duration FSP conversion panic, and STR_TO_DATE NULL-versus-String assertion, respectively.

This acceptance is only the implemented private native-Datum-returning SQL TypedRow control-lineage slice and its bounded caller handoff. It does **not** grant general ordinary/control composition, arithmetic/203, PB/AST/batch/host or a public evaluator route, D4 native diagnostic composition, warning-site/context/severity policy, generic ID/byte authentication, atomic metadata capture or peak-allocation guarantees. Migrated-family count remains0. Observer and D3/D4 products remain frozen. C3c's separate seven-KV-file implementation release is a future independently gated activity, not evidence for D5 or permission for D writes.

D ran no build/tests/lint in this acceptance turn and changed only this receipt. The first compile's zero-test failure and fixture correction remain below as historical evidence rather than being erased by the later successful gate.

## D5 first-compile amendment — five fixture constructors only

Parent's first real compile exited101 with **five E0277 errors and ZERO tests run**. D read `logs/tidb-d5-lineage-first.log:1871–1952`: the five materialization fixture sites in NEW `tikv/lineage_tests.rs` used `VectorValue::from(Vec<Option<i64>>)`, which is not a production dependency trait. A TiKV test-only conversion is not usable merely because the dependent TiDB crate is compiling its own tests. This was D's fixture API mistake, not runtime/domain evidence or a failed assertion.

Parent released **only** lineage_tests.rs plus this receipt. D changed those five expressions to existing public `VectorValue::from_scalar(&ScalarValue::Int(Some(7)), 1)` after reading its production definition at TiKV data_type/vector.rs:40–55. Foreign/unknown/predicate/generated-NULL IDs, mismatched length/count, equal-value identity and all associated assertions are unchanged. No datatype conversion trait, source admission, caller runtime, existing test or extra file changed.

D ran only scoped rustfmt and rustfmt --check on lineage_tests.rs, then SHA256 on the five D5 paths; all succeeded. New tests hash **`dbf91cabeaca22743c360d6aa8f782a94bb20beb62844d8788792583ad2c1055`** supersedes `fb7c86001c150408286d4776e72dfaf0f39e7d59b6ea01089130af525b387525`. The other four D5 hashes are unchanged. No build/test ran under D; the subsequent parent18/18 acceptance is recorded above. Lesson: verify dependency-public construction APIs, not cfg(test) conveniences observed in that dependency's own tests.

Exact commands from expression-unification/tidb:

```sh
rustfmt --edition 2021 --config skip_children=true rust/crates/tidb-expr/src/tikv/lineage_tests.rs
rustfmt --edition 2021 --check --config skip_children=true rust/crates/tidb-expr/src/tikv/lineage_tests.rs
sha256sum rust/crates/tidb-expr/src/tikv/{lineage_tests,lineage,mod,catalog,context}.rs
```

The tool received expanded SHA paths and chained the commands with `&&`. The correction is handed back frozen; only receipt updates may follow while parent reruns.

## D5 caller source checkpoint — exact five-file implementation release

**Accepted private source block; gate details above.** After accepting the observer, parent reported stable compiled C3b575/575+aggregate40 (measured EvalFrame400), matching its nine-file checksum, and a pinned pre-D5 caller1274 passed/4 unchanged full baseline failure blocks/93 ignored. Parent then released exactly NEW `tikv/{lineage,lineage_tests}.rs`, `tikv/mod.rs` private wiring, `tikv/catalog.rs` pure existing signature facts, and `tikv/context.rs` **documentation only**, plus this receipt. D implemented that block and handed it back frozen; D did not run build/tests/lint, update a manifest/dependency, touch the observer/C runtime/value bridge, or activate a native public evaluator.

### Actual source and invariants

- `lower_typed_control_lineage` now preflights an already bound Expression tree for the real native **Datum-returning** control path. It admits only IF/IFNULL/searched CASE/COALESCE Int/String and binary AND/OR, with actual LongLong/string-family declarations, closed full flags and same-family value children. No PB at any node, casts/203/222/other ordinary/host operations, virtual/correlated/parameter/deferred source, AST/batch route, native retry or scope-expanding typed subentry is used.
- PredicateInt demand propagates through every potentially selected producer, including dead alternatives. A signed-return selection can materialize UInt bits as a **value**, but that source is refused in a predicate closure. Native UInt truth is not alleged to fail.
- `ControlSourceLimits` uses explicit node/depth and logical payload/work policies: two literal payload charges (transport/fact), fixed flat scaffolding, native/projected/fact declaration groups, extra detached binding/root/schema types and exact source/collation/display-name bytes. Every source and the entire row_schema is budgeted before equality, deep copy, projection or C snapshots. This is not total compiler/allocator heap accounting. The accepted no-allocation FT observer supplies private visible marker lengths.
- Every invocation first caps the entire incoming row_schema and checks cached native Collation enums before existing NativeInputs::new can perform allocating FieldType equality. Metadata owner stability through observe→Eq/copy/projection/publication remains an explicit prerequisite, not an atomic/race/authentication claim.
- One caller-provided ID names each all-node source-preorder producer. Sparse/high IDs (including u64::MAX) use an O(count) sorted index, never max-ID-sized allocation. Records join exact B identity, carrier/role, static root eligibility, display label and the spec's detached complete SQL/source metadata. One LocalExpr graph and C's checked facts compile into the opaque LocalControlProgram. There is no second executable tree, value-based origin inference or generic foreign batch/table join.
- The same demanded native Datum is checked for non-NULL kind and actual String collation **before** B to_scalar. All admitted binary/blob Chunk columns remain String. NULL does not fabricate collation or replace the immutable producer contract. Bytes singleton construction uses the released fallible capacities helper and borrowed push_ref, not ScalarValueRef::to_owned normalization.
- Chosen child/current IDs survive selection, including NULL. No-ELSE CASE/all-NULL COALESCE and computed AND/OR retain their own generated/computed IDs. `NativeControlBatch` keeps values+IDs and its owning spec; root and producer SQL types escape only as detached snapshots. It never substitutes selected metadata for the root declaration or routes controls through D4's PLUS diagnostic adapter.
- Exact returned-vector/ID shape, namespace/membership/root-role/carrier validation precedes per-occurrence B from_scalar using the **returned record**. The private materialization ledger measures C Int/Bytes source buffers, actual ID Vec capacity, native Vec<Datum> capacity and native byte-owner capacities while coexisting. StringDatum is moved through into_bytes and reconstructed with the same Vec/collation, not copied/decoded to discover capacity. The source is charged until its actual drop; no further copy or publication follows refusal. This is actual accepted retained storage, not callback/allocator/B-copy peak protection. Immutable program/spec and externally owned Chunk/source owners remain separately scoped.

### Eighteen focused tests — parent18/18, not executed by D

The new inline module targets `tikv::lineage::tests::` and currently contains18 `#[test]` functions: sparse all-node IDs/facts/real signature and FT; invalid IDs; paired native Datum references for every admitted control; equal String/Bytes payloads and different String collations/invalid UTF8; all seven binary/blob native column type codes; selected versus generated/Boolean NULL IDs and nested current-ID forwarding; UInt value bits versus transitive predicate refusal; before-erasure raw kind/collation rejection; lazy poison/occurrence/Chunk::Sel/NULL-left logical demand; demanded error and late-materialization prefix preservation; all-node PB/unproved-shape/arity refusals; actual-code/flag/literal-provenance refusals; static and runtime incoming byte caps before snapshots/Eq; metadata detachment/non-shallow output types; materialization ID/shape/role/NULL validation; same-Vec native capacity measurement; source/bitmap/offset/ID/native coexistence and tiny resource limits; and worker-local programs sharing only immutable specs.

Requested parent target/filter under its existing pinned settings, **not executed by D**:

```sh
# From expression-unification/tidb/rust, with the parent's toolchain/target pins.
cargo test -p tidb-expr --lib tikv::lineage::tests:: -- --nocapture
```

The actual nonzero parent count is18, with the full-caller comparison1292/4/93 recorded above. No green full suite, PR readiness or package transcreation claim is made by this accepted private checkpoint.

### Exact five-file frozen manifest and actual checks

| File under `tidb/rust/crates/tidb-expr/src/` | SHA256 | Scope |
|---|---|---|
| NEW `tikv/lineage.rs` | `0c7a2a84d1bef3cce1873465097736e1f9048b2803bd55bf237c0f378292414c` | Caller lowering/ID ownership/adapter/materializer only |
| NEW `tikv/lineage_tests.rs` | `dbf91cabeaca22743c360d6aa8f782a94bb20beb62844d8788792583ad2c1055` |18 scoped tests; five public-API fixture constructor corrections after first compile |
| `tikv/mod.rs` | `ea227f759246c4b94a04197d7916659237a55f9af981279d56d642496a7cdf9c` | Private module/exports only |
| `tikv/catalog.rs` | `2a159d09f3dd87f494af020e9d515a102d300653f524272333c890d4ad0aafc3` | New pure existing conditional_signature/CATALOG facts; no old entrypoint change |
| `tikv/context.rs` | `e51dec2ce34703fef4311fde6fe0cba29ee805e17fe1209128a91639e08bb601` | Only the existing reader's signed-Int-only comment was generalized; no code change |

From `expression-unification/tidb`, D ran these scoped commands (all successful):

```sh
rustfmt --edition 2021 --config skip_children=true rust/crates/tidb-expr/src/tikv/{lineage,lineage_tests,mod,catalog,context}.rs
rustfmt --edition 2021 --check --config skip_children=true rust/crates/tidb-expr/src/tikv/{lineage,lineage_tests,mod,catalog,context}.rs
git status --short -- rust/crates/tidb-expr/src/tikv/{lineage,lineage_tests,mod,catalog,context}.rs
git diff --check -- rust/crates/tidb-expr/src/tikv/{lineage,lineage_tests,mod,catalog,context}.rs
sha256sum rust/crates/tidb-expr/src/tikv/{lineage,lineage_tests,mod,catalog,context}.rs rust/crates/tidb-datatype/src/field_type/memory.rs rust/crates/tidb-expr/src/tikv/{ordinary,ordinary_diagnostics,ordinary_diagnostics_tests}.rs rust/crates/tidb-expr/src/scalar_function.rs
```

The tools received expanded explicit paths rather than brace syntax; formatting/check were chained with `&&`, and status/diff-check/hash were chained separately. The five caller paths are **untracked** in this worktree: the empty Git diff-check is not content-diff proof. Tool content searches found no whitespace/conflict markers in the new source/tests and no native evaluator/PB serialization/Sync widening in production lineage.rs. Format is parse/format evidence, not Rust typechecking.

Reread frozen dependency hashes match: observer `a6a69afe…c31f4a`, ordinary `d2b85bc3…e32168`, ordinary_diagnostics `ada427a0…822e77`, ordinary_diagnostics_tests `3559fcc5…ab78`, scalar_function `2d21803a…1407b3`. These are unchanged by this release. D has stopped all product writes at this coherent checkpoint; only receipt updates are allowed while parent builds/reviews.

## D5 accepted observer checkpoint — one separately released file

Parent reviewed r12's observer contract and granted **only** `tidb-datatype/src/field_type/memory.rs` plus this receipt. D added public `FieldType::checked_snapshot_payload_bytes() -> Option<usize>`, a private checked-arithmetic helper and eight inline tests. Parent compiled and accepted that file's targeted helper gate; it remains frozen. D reread `logs/tidb-d5-field-type-observer.log:1543–1553`: **8 passed,0 failed,0 ignored,428 filtered out**, using pinned TiDB1.100 and the --lib filter below. Parent additionally reviewed the nonallocating observer/helper and unchanged old bodies. No field_type/mod.rs or clone.rs, Cargo, D5 caller, C product, D3/D4 source or other visibility was changed. D ran no builds/tests/lint or fixture generators.

### Actual observer and unchanged legacy behavior

The observer borrows `self.elems.with_visible`, reads the exact charset/collation byte lengths and independent private `elems_is_binary_literal.len()`, and lazily folds GoString lengths through checked arithmetic. The exact logical charge is `size_of::<FieldType>() + charset bytes + collation bytes + visible element count*size_of::<GoString>() + visible element byte sum + visible marker count*size_of::<bool>()`. Every addition/multiplication is checked and overflow returns None. No observer Eq/Hash, snapshot()/elems_snapshot(), clone, allocation, normalization, metadata mutation or default is introduced. The helper tests overflow without huge allocations or invalid headers.

The old `memory_usage` and `storage_length` bodies remain byte-for-byte unchanged in the scoped Git diff; two additive hunks introduce the observer and helper/tests. The new result is logical source/snapshot payload, **not** spare capacity, allocator capacity/peak, original ownership, byte authentication or an atomic snapshot. The public docs retain the accepted owner-stability requirement across observe→Eq/copy/projection. A sequenced alias mutation test deliberately demonstrates why an earlier observation does not freeze later source contents; it is not a concurrency guarantee.

### Eight inline tests, valid target/filter — parent8/8 GREEN

`field_type::memory::tests::snapshot_payload_` selects these eight source-defined tests:

1. `snapshot_payload_base_and_exact_name_bytes`: Rust fixed size, exact source spelling/UTF-8 name bytes and zero dynamic terms.
2. `snapshot_payload_counts_non_utf8_elements_verbatim`: arbitrary element octets including invalid UTF-8 and NUL, without decoding.
3. `snapshot_payload_counts_independent_marker_lengths`: empty elems with nonempty markers, zero/short/long marker arrays, no marker indexing by element count.
4. `snapshot_payload_nil_and_allocated_empty_are_not_capacity`: distinct allocation states/spare capacities but equal visible payload cost; old Go estimate still counts capacity.
5. `snapshot_payload_leaves_legacy_counters_unchanged`: unchanged120-byte Go counter/16-byte element-header estimate and capacities, repeated observations, LongLong/Varchar storage lengths. No unsafe Decimal fixture is exercised.
6. `snapshot_payload_borrows_shared_headers_without_mutation`: shared backing pointers, preserved contents/flags and repeated equal observations with no copied backing.
7. `snapshot_payload_is_observation_not_an_atomic_snapshot`: deliberate sequenced alias replacement changes later observed payload length; no lock/freeze persists across calls.
8. `snapshot_payload_checked_arithmetic_refuses_every_overflow`: both name additions, header multiplication, marker addition, element accumulation, exact usize::MAX boundary and one-byte excess, plus an ordinary mixed-term helper case.

Harness inspection: `tidb-datatype/Cargo.toml` has autotests=false and `--test all` aggregates **tests/*.rs** via `rust/scripts/aggregate-tests.rs`. These new tests are instead inline under `src/lib.rs::field_type::memory`, so the parent should use its existing pinned toolchain/target/dependency settings with this command from the TiDB rust workspace:

```sh
cargo test -p tidb-datatype --lib field_type::memory::tests::snapshot_payload_ -- --nocapture
```

Parent executed this target/filter with its pinned settings and confirmed the actual nonzero count of8; **D did not execute it**. No aggregate `--test all` zero-match result or broad/unsafe Decimal filter is claimed. This accepted helper gate does not grant or prove the later D5 caller; the C3b compiled API checkpoint and a separate caller release remain required.

### Actual source checks and manifest

From `/home/agent/tidb/expression-unification/tidb`, D ran:

```sh
rustfmt --edition 2021 --config skip_children=true rust/crates/tidb-datatype/src/field_type/memory.rs
rustfmt --edition 2021 --check --config skip_children=true rust/crates/tidb-datatype/src/field_type/memory.rs
git diff --check -- rust/crates/tidb-datatype/src/field_type/memory.rs
git diff -- rust/crates/tidb-datatype/src/field_type/memory.rs
sha256sum rust/crates/tidb-datatype/src/field_type/memory.rs
```

The first four were chained with `&&`; diff-check and SHA were repeated together afterward. All succeeded. Source/header searches enumerate exactly eight tests and no trailing whitespace/conflict markers. Formatting/diff evidence is not typechecking or execution. Frozen SHA256: **`a6a69afe9332d0843422cfc97c8b398d00f931ac2ff35c78a53065f087c31f4a`**. D has handed back the coherent single-file block and will not write products while the parent gate runs.

## D5 reviewed source/API design — historical r12

**Historical design, not the current gate status:** parent subsequently accepted the one-file observer and the compiled C3b API, then released the exact five caller files. Their actual implementation, frozen manifest and accepted parent compile/test gate are recorded above. Parent deliberately waited for B's independently owned Decimal-producer source freeze before compiling D5 to avoid a mixed candidate; that sequencing wait was not a D5 code failure or permission for additional edits. D3/D4 APIs/products and their prior gates remain unchanged.

### 1. Prove the native Datum-returning closure at every node

This boundary consumes an already bound native Expression tree for **Expression::eval → ScalarFunction::eval returning Datum**, not AST eval, numeric batch, EvalInt or EvalString. It does not claim earlier SqlBuild was effect-free and does not widen StructuralOnly.

| Actual source | Proof and narrow caller rule |
|---|---|
| `expression.rs:719–732,751–770` | Generic row dispatch calls Column::eval, Constant::eval_on_row_with_error_value or ScalarFunction::eval, then applies ENUM/SET-as-int. Reject PB state at **every** node, correlated/virtual/deferred/parameter sources, hybrid codes and ENUM/SET/ENUM_SET_AS_INT flags; do not bypass this tail by assuming typed subentry equivalence. |
| `constant.rs:293–297,318–343` | A strict literal's generic row path returns its actual saved Datum, not a declared-family reconstruction. Inspect literal_value only; no eval/fold probe. Bind Int/UInt leaf declarations to actual non-NULL kinds, String to its actual StringDatum collation, Bytes to ordinary Bytes, and typed NULL without invented collation. |
| `column.rs:238–250`, `tidb-chunk/row.rs:44–76` | The native row reader returns NULL first; signed LongLong→Int, unsigned LongLong→UInt. **All** admitted varchar/string/blob variants call set_string with FieldType::collation(), including binary/blob declarations. Such columns are String producers, never guessed Bytes/BinaryLiteral producers. |
| `scalar_function.rs:1192–1212,155–178,1029–1098` | PB dispatch occurs first and can reinterpret return bits; it is excluded throughout. Generic same-family Datum coerce preserves Int/UInt and String/Bytes identities, including the selected StringDatum collation. Prove every admitted argument/result remains in that family; reject cross-family/cast paths instead of invoking coerce_to_ret_type. Preserve actual type codes and all declaration metadata, not just EvalType. |
| `scalar_function.rs:1813–1857` | IF and matching searched CASE evaluate one chosen value and return that child's Datum, even NULL. IFNULL returns its first non-NULL child or evaluates/returns its second, even NULL. No-match CASE without ELSE and exhausted COALESCE return a **fresh** NULL. Conditions/arms retain native source order and skipped arms are not evaluated. |
| `scalar_function.rs:1217–1230,1612–1643,81–89` | AND/OR have no numeric_operand_domain, so they use Datum-returning child eval, not numeric casts. Left false AND / left true OR skip RHS; NULL left still demands RHS. Result0/1/NULL is an own computed Int boundary, not the operand that happens to have equal bits. String truth conversion can warn, so the first cut requires PredicateInt only. |
| `arg_eval_type.rs:426–438,451–461`, `constant.rs:160–214` | EvalString erases String/Bytes/BinaryLiteral identity; EvalInt reinterprets UInt bits and typed constants have additional conversion paths. They are not the target consumer and are never used as a proof shortcut. |

The exact control grammar is IF/IFNULL/searched CASE/COALESCE in Int or String carrier families, plus binary AND/OR. Initial root must be one such control. No203/222, arithmetic/comparison/NULLIF/CAST/host/other operation, PB/AST/batch route, or simple-CASE selector/BindOnce lowering is admitted. A typed simple-CASE rewrite retaining equality/cast nodes is therefore refused. Erased upstream syntax history cannot be reconstructed from a bound native tree; the claim is this actual Datum-returning control closure, not a new AST simple-CASE guarantee.

**Type gates:** value Int requires actual LongLong, signed or unsigned; computed Boolean and every PredicateInt declaration require signed LongLong. String declarations are exactly native Varchar, VarString, String, TinyBlob, MediumBlob, LongBlob and Blob. Preserve differing branch flags/lengths/collations and the parent's declaration separately. Reject ARRAY, hybrid flags/codes, Tiny/untyped-NULL retags and all other families. Full flags/lengths/decimal/charset/collation/element metadata must survive or be explicitly refused by checked projection—never mask it into an admitted type. Use existing conditional_signature/CATALOG facts, never a serializer or another kernel map.

**Transitive PredicateInt:** compute a bounded per-node possible-UInt bit from **every potentially selected value producer**, not from the intermediate result flag and not by evaluating predicates. An Int selection control propagates branch outcomes; AND/OR generate Int/NULL but their children must independently satisfy PredicateInt. A signed-return IF forwarding an unsigned leaf is valid as an Int value but refused anywhere a predicate is required. Include dead alternatives conservatively. Native Datum::to_bool actually accepts UInt (`datum/convert.rs:33–42`); this refusal is an explicitly stricter initial C3b policy, not a claim that native UInt truth is erroneous.

### 2. Proposed small caller API and one owning chain

Proposed names under `tidb-expr/src/tikv`, not existing APIs:

```rust
struct ControlSourceLimits {
    tree: CompileLimits,
    max_literal_bytes: usize,
    max_metadata_bytes: usize,
}
fn lower_typed_control_lineage(
    root: &Expression, row_schema: &[FieldType], new_collation: bool,
    producer_ids: &[ResultMetaId], limits: ControlSourceLimits,
) -> SeedResult<Arc<LoweredControlLineage>>;

impl PreparedControlLineage {
    fn compile(spec: Arc<LoweredControlLineage>, execution: ExecutionLimits,
        max_materialization_retained_bytes: usize) -> SeedResult<Self>;
    fn eval_selected(&mut self, ctx: &mut EvalContext, chunk: &Chunk,
        row_schema: &[FieldType], selection: &[usize]) -> SeedResult<NativeControlBatch>;
}
// Owned output, read-only getters: Datums and one ResultMetaId per occurrence.
// Root declaration stays in its owning spec, not substituted by a chosen leaf FT.
struct NativeControlBatch { /* values, result_metadata, owning spec */ }
```

Owner supplies one unique ResultMetaId per **all-node source preorder** position, root0 then children left-to-right. Its `record()` need not equal ordinal: sparse/high/u64::MAX record numbers are valid. Require one unit and reject missing/extra/duplicate IDs or foreign units before snapshots. Use O(node-count) tables and a sorted ID→ordinal index, never an allocation indexed by the maximum numeric ID. IDs are producer-record names, not row indexes, input slots, source pointers, graph links or value-cache keys.

An immutable table entry joins ID, ordinal, role, carrier, B ValueMetadata and that ordinal's deep-detached complete SQL FieldType/source/collation metadata. Preserve complete column identity and strict literal provenance. The table may reference the spec's already-owned flat NodeMetadata instead of cloning a second copy of its metadata; it contains no native expression or executable child graph. Keep one LocalExpr executable graph, C's checked flat facts, and worker-local LocalControlProgram/LocalEvalState. Producer/role/possible-kind/root-result-eligibility annotations are static facts, not another interpreter or transfer callback.

Use C3b-r1's proposed `ControlProducerFact::{constant,input_slot,selected_control,computed_boolean}` and `ControlLineageFacts::sql_typed_row`; all ordinals get one producer fact. Compile through `compile_control_with_lineage` only after those APIs are actually integrated. LocalControlProgram is a thin owner with no raw-program/Deref escape. D immediately consumes its **own fresh** LineagedBatch with its own spec/table. Matching unit/record numbers cannot authenticate a foreign table/program; no public arbitrary `(batch,table)` reconstruction seam or global/cache ID allocator is proposed. Keep IDs attached to NativeControlBatch and its owning spec after materialization.

Initial caller may expose the raw runtime path only; C3b's reported variant remains available for later explicit raw forwarding/testing. No control result/error is sent through D4's PLUS-only native overflow adapter. D3/D4 entrypoint contracts and domains remain unchanged.

### 3. Exact producer records and result materialization

| Producer / actual outcome | Immutable record and reconstruction |
|---|---|
| Strict Int/UInt literal | SourceValue; actual native non-NULL kind matching signedness declaration, Typed literal tag, no String collation. No UInt-bit retag into Int. |
| Strict String literal | SourceValue; actual StringDatum collation/raw bytes, Text literal tag. Its value identity need not be inferred from the declared FT's collation. |
| Strict ordinary Bytes literal | SourceValue; Bytes, no String collation, Typed literal tag. BinaryLiteral is refused even with identical bytes and binary metadata. |
| Typed NULL literal | SourceValue; kind Null, no collation/Decimal shape, carrier determined by its real admitted declaration. Untyped Null FT remains refused. |
| Native Column | SourceValue with the accessor's fixed **non-NULL** kind/collation contract. Nullable String input keeps that same producer ID when NULL; the contract's String collation is not a collation attached to Datum::Null. |
| Any selection-control node | One GeneratedNull record with kind Null, no collation/Decimal shape and that node's carrier/full declaration. IF/IFNULL normally forward selected child IDs; their own fallback records are not substituted for selected NULL. |
| CASE no match/no ELSE; COALESCE all NULL | Own GeneratedNull ID, not the last inspected child ID. |
| IF/IFNULL/CASE chosen child, even NULL | Chosen child's **current** ID. Outer selection of an inner Boolean/generated NULL forwards that inner ID, not an older leaf. |
| Binary AND/OR result0/1/NULL | Own ComputedBoolean Int record, even if equal to a predicate value or NULL. |

A static root-result eligibility bit may reject impossible returned producer roles (for example predicate-only leaves or unused IF generated-NULL fallbacks) without storing per-node ID sets or rerunning conditions. It never chooses an output ID; the exact chosen ID comes only from C. This is bounded validation of the native control proof, not a second runtime graph.

Validate returned values/IDs lengths against selection length, ID namespace/membership/eligible role, table carrier and actual vector/scalar carrier before publication. Materialize **directly from each borrowed scalar reference using that returned record's B ValueMetadata**. Do not infer origin from equal bits, equal bytes/collation, NULL, node name or final declared FT; do not call native eval/coercion or replay predicates. A generated-NULL record cannot reconstruct a non-NULL value. Keep selected-source SQL metadata distinct from the root declaration; a signed-declared selection may correctly return Datum::UInt(MAX), and a differently collated String remains that StringDatum at this boundary.

### 4. One demanded read, before-erasure checks and bytes transport

For each input slot store its source ordinal, native column index, declared FT, fixed non-NULL kind/collation and expected Int/Bytes carrier. Use one slot per source leaf occurrence even when native column indices repeat. Native schema/layout/physical-selection checks precede values; `physical_row` must not apply Chunk::Sel twice.

At the **same single demanded read**, obtain Datum using the detached native declaration, then before to_scalar:

- Int source: require non-NULL Datum::Int for signed or Datum::UInt for unsigned; do not accept a positive UInt merely because it fits i64.
- String-family column: require Datum::String and compare its actual `StringDatum::collation()` to the record's native accessor contract `FieldType::collation()`. Binary/blob still means String here. Validate exact SQL charset/collation names and checked projection facts independently; never use a registry fallback as a fabricated source identity.
- NULL: allow the carrier-correct NULL without demanding/fabricating String collation or replacing the immutable producer record with a guessed Null record.

Literal contracts are checked statically from literal_value before their transport. Reject generic dynamic-kind providers. No extra metadata callback, second read, eager dead-row import or comparison with already-erased ScalarValue can establish these facts.

After validation use unchanged B to_scalar once. Build a singleton Bytes vector through the released/frozen fallible ChunkedVecBytes capacities/reservation plus **borrowed push_ref** from that scalar; do not add ScalarValueRef::to_owned/from_scalar normalization clones. The native row read and B transport can already have allocated/copied payload before C receives the callback result. C must measure/admit the actual returned owner before its next effect; D does not advertise a pre-bound callback or peak-heap guarantee. Failed reservations remain resource/contract failures, not SQL warnings, NULL or native retries.

### 5. Static byte policy and the separately staged FieldType observer

An explicit preflight walks the native source and the **entire supplied row_schema** without cloning literal payloads, metadata, or C facts. Charge all strict literal byte lengths and all metadata/source names, full CiString spellings, collation snapshots, full FieldType payloads and fixed record/index scaffolding using checked arithmetic. IDs/counts/depth are independently bounded. Only after policy succeeds may it call deep_copy_like_go, project_field_type, full FieldType equality or C's immutable snapshot constructors. Charge per source occurrence/copy group, not by distinct equal value or maximum ID. Metadata/literal policies are explicit logical snapshot-content/work budgets, not exact allocator-capacity/total-heap promises; capacity and duplication/allocator overlap have separate accounting.

**Real current gap:** FieldType PartialEq/Hash at `field_type/mod.rs:534–565` allocate snapshots of elems and the private binary-marker slice. `memory_usage()` at `field_type/memory.rs:52–61` is an unchecked Go-style source estimate using capacities, not a checked pre-copy observer. Do not call either as if it were an allocation-free bounded gate.

The reviewed request was separate from C3b Stage A: exactly `tidb-datatype/src/field_type/memory.rs` for this additive observer and inline tests. Parent has now granted and accepted this one-file implementation as recorded above; caller integration remains ungranted:

```rust
// Accepted one-file helper; caller integration remains separately gated.
// No allocation, clone, normalization or change to old memory_usage.
pub fn checked_snapshot_payload_bytes(&self) -> Option<usize>;
```

Its documented count is checked `size_of::<FieldType>() + charset_name.len() + collation_name.len() + elems.len()*size_of::<GoString>() + sum(visible element byte lengths) + elems_is_binary_literal.len()*size_of::<bool>()`. Access the private visible marker length inside FieldType; never probe marker indices using elem count, infer its length from elements, or allocate snapshot()/elems_snapshot() just to count it. Borrow visible element strings under the existing scoped read view. Include marker storage when elems is empty or lengths differ. Count immutable string content conservatively per charged snapshot, even if GoString bytes happen to be shared. This describes logical detached/projection payload, not original unused slice capacity, allocator overhead, reserved capacity or source heap ownership. Return None for checked size arithmetic overflow, without warnings/evaluation or altered legacy memory_usage semantics.

Observer tests: fixed base plus exact charset/collation bytes; non-UTF8 element bytes; independent nonempty/empty/short/long binary-marker slices; nil versus allocated-empty visible payload; shared headers/alias copies without mutation; checked addition/multiplication overflow via a private arithmetic helper; and unchanged old memory_usage results. No timestamp/default/collation inference or bridge semantics is added.

**Before-equality runtime incoming-schema gate:** every D5 invocation observes/caps the incoming row_schema metadata **before** calling existing NativeInputs::new, because that method's existing equality can allocate snapshots. Also compare the cached native Collation enum where needed for String accessor contracts: FieldType equality compares names but does not directly compare that enum. Do not alter D1/D3/D4's preflight or claim this is a newly found value bug there. Then run the unchanged complete schema/layout comparison and read only demanded cells.

**Metadata-stability prerequisite, not atomic proof:** GoSharedSlice permits mutation through other FieldType aliases despite an `&FieldType`. This private first cut requires the owning front-end to serialize such alias mutation across observation→equality/deep-copy/projection/facts publication; at runtime keep incoming metadata stable through the byte gate and native schema preflight. Prepared detached metadata must remain private with no shallow-clone mutable-backing escape. One observer followed later by a copy is **not** an atomic budget or consistency proof under concurrent mutation, and the observer's per-slice read guard does not freeze an entire type/tree/schema.

If the owner cannot provide that stable interval, do not quietly proceed. Request instead a concrete separately reviewed `FieldType::try_snapshot_payload_limited(max_bytes) -> Result<(FieldType, usize), SnapshotRefusal>` that checks/counts and makes the detached copy while holding the necessary source guards. That would need explicit `field_type/{memory,clone,mod}.rs` loans for checked copy/error export and a reviewed cross-slice locking contract; the observer-only loan does not grant it. Even such a per-type operation does not provide whole-tree/schema atomicity or a hard allocator peak. No concurrent-mutation safety or byte authentication is claimed by the present proposal.

### 6. Memory boundary through caller publication, without peak claims

C3b's agreed contract is **actual retained storage accepted before the NEXT semantic effect and successful LineagedBatch publication**, not allocation-before-first-byte or unknown-provider/allocator peak bounds. Declared flen, row count, logical NULL/empty payload and requested reserve capacity are not payload/capacity upper bounds. Empty Bytes still needs offset/bitmap scaffolding and may refuse a tiny resource budget before reading anything. C's measured frame/owner/output/ID/AND-OR-accumulator ledger stays C-owned; D does not duplicate its interpreter or weaken that contract.

C publication is not the end of caller memory use: B from_scalar creates native Datum byte owners while C's values/IDs can remain live. The proposed explicit `max_materialization_retained_bytes` accounts the source values' actual Int/Bytes retained heap via the existing/new datatype capacity helpers, the consumed ID Vec's **capacity**, native Vec<Datum> actual capacity×size_of::<Datum>, and native byte-owner capacities during coexistence. Consume the own LineagedBatch through into_parts to retain ID capacity information; do not guess it from a slice length.

Reserve native output scaffolding fallibly, measure actual capacity before further copies, and precheck known next payload minima. Use one from_scalar per occurrence. Bytes exposes its Vec capacity; StringDatum can be moved through its existing `into_bytes()`, capacity measured, and reconstructed with the same collation/Vec via StringDatum::new—no decoder, payload copy or metadata inference. Check actual retained coexistence after each acquired native owner before the next copy/publication; on refusal drop the partial result/source and keep the already executed warning/error prefix. Move the ID Vec into the native result, release the source VectorValue when genuinely dropped, and check the final published native owners. This remains an accepted-boundary retained-storage contract, not a claim that an allocation/B conversion never transiently exceeded the number. Immutable source/program metadata and externally owned Chunk/source owners remain separately scoped.

If the final frozen C3b API does not expose the required capacity-safe construction/observation seams, report that gap and keep Bytes or publication inadmitted; do not substitute old capacity heuristics, declared flen, integer-vector accounting, a guessed default or unmetered all-rows accumulation.

### 7. Exact prospective files and acceptance witnesses

**Five caller files requested only after C API stability and an explicit grant:**

| File under `tidb-expr/src/` | Proposed narrow work |
|---|---|
| NEW `tikv/lineage.rs` | One bounded typed-control lowerer, role/kind/source preflight, immutable ID table/index, demanded adapter, own-program facade and exact materialization/accounting. Reuse the existing metadata storage envelope and checked projection helpers without changing their old entrypoints. |
| NEW `tikv/lineage_tests.rs` | Caller lineage/native-Datum/kind/collation/budget/ownership fixtures and paired native references. |
| `tikv/mod.rs` | New private module/exports only. |
| `tikv/catalog.rs` | Pure control signature facts through existing conditional_signature/CATALOG; no old admission change or kernel/serializer map. |
| `tikv/context.rs` | Documentation-only generalization of the already factored demanded Datum reader's signed-Int-only comment; its layout/read/old bridge behavior remains unchanged. D5's incoming-metadata byte gate stays in lineage.rs. |

**Separate sixth source loan, now accepted under its own grant:** `tidb-datatype/src/field_type/memory.rs` for only the checked no-allocation observer and inline tests above. Its source checkpoint does not release the five caller files. No value.rs bridge change, no D1 lower/batch/tests, D2 preparation, D3 ordinary/D4 diagnostics, scalar-function/decoder/pushdown implementation, Cargo, C code, public evaluator or activation edit is proposed. The atomic-copy alternative would be a different larger release, not a hidden addition.

Proposed caller tests (none written/run):

1. Exact native Datum control closure at every node: positive Int/String IF/IFNULL/CASE/COALESCE and binary AND/OR; negatives for nested PB,203/222/other ops, cast/coercion, host, malformed arity, unsupported real type codes/flags, deferred/parameter/correlated/virtual and direct AST/batch/simple-CASE selector paths.
2. Sparse/high/u64::MAX producer IDs, all-node preorder, one namespace, complete ordered roles and duplicate/foreign/missing/stale facts; no allocation by max ID or merging equal producers.
3. Equal bytes with String versus Bytes, equal String bytes with distinct actual collations, binary/blob native columns still String, `_binary` determined by actual literal Datum, invalid UTF8 preserved, BinaryLiteral rejected.
4. Selected NULL keeps IF/IFNULL/CASE child ID; no-ELSE CASE/all-NULL COALESCE get own generated-NULL ID; AND/OR get own computed ID; outer selection forwards an inner computed/generated ID rather than an old leaf.
5. UInt MAX value under signed-return selection reconstructs UInt exactly; signed/unsigned leaf mismatches are rejected before to_scalar; a possible selected UInt propagates rejection through any PredicateInt use, including dead alternatives. Do not assert native UInt truth itself fails.
6. One demanded native read with kind/collation checks before erasure, NULL without fabricated collation, poisoned skipped branches, ordered conditions and NULL-left AND/OR RHS demand, repeated/nonidentity selections and Chunk::Sel independence.
7. Static huge literal/type/name/marker payload refusal before projection/C snapshots, dead-child static checks, deep metadata detachment, incoming row_schema byte refusal before Eq, and tests conditioned on the declared metadata-stability interval rather than a bogus concurrent-snapshot promise.
8. Fresh own-program/table materialization, complete vector/ID length/carrier/namespace/role validation, bad/generated-NULL IDs with non-NULL values refused, root declaration distinct from producer FT; no public foreign batch/table reinterpretation or origin inference by value equality.
9. Actual returned Bytes capacity and NULL/empty scaffolding, cap refusal before a later effect/publication, output/ID/native-Datum coexistence, exact publication accounting and preserved diagnostic prefix; no peak or pre-callback allocation claim.
10. Frozen D1/D2/D3/D4 raw/diagnostic APIs and helper semantics remain unchanged; parent reruns their accepted16/8/10/10 tests and the full1274/4/93 comparison plus the separately integrated C3b/native/datatype gates.

D waits for Stage A/helper acceptance, an explicit C integration release and stable compiled facade, acceptance of the separately staged observer under the agreed metadata-stability prerequisite, and then an exact D5 caller release. No family credit, generic lineage authentication, expanded host/arithmetic/profile support, warning-site/context/severity contract or public activation follows from this design. The r12 design changed only this receipt; the later r13 source work is limited to the separately granted observer file above. Source findings were relayed through parent, including the non-atomic metadata observer limitation.

## D4 reviewed source/API design — historical r9

**Historical status:** the r9 proposal below was reviewed before the exact four-file D4 release. The implementation amendment above supersedes its proposed/not-written wording only for that private diagnostic slice. No broader interface or activation authority follows from this historical design.

### A. Consume C3d's actual report shape, not the earlier conceptual draft

C's staged source provides `ReportedLocalFailure::{error,into_error,site,stage,sql_error_code}`. `error()` borrows the intact LocalError; `into_error(self)` moves it back without cloning/replacing. The relevant enums are exactly:

```rust
LocalFailureSite::Kernel { call: OrdinaryCallSite, row: InputRow }
LocalFailureSite::InputSlot { slot: usize, row: InputRow }
LocalFailureStage::{Kernel, Input, Resource, Validation, Unattributed}
```

The Kernel record is the checked OrdinaryCallSite, so use its actual `ordinal()`, `source()`, `profile()`, and `original_pb_signature()` getters; OrdinarySourceId has `unit()` and `node()`. Do not invent a separate failure-node/source getter or an Identity-conversion field. Stage derives from the **site first**: an InputSlot error carrying ResourceLimit is Input; an input carrying typed1690 is still Input. Without a site, ResourceLimit is Resource, contract/spec/batch/host errors are Validation, and Evaluation is Unattributed. A malformed successful vector reply is unsited Validation, not automatically the last input site.

The proposed caller invokes C's additive `LocalProgram::eval_with_bindings_reported` with the same state/context/physical_rows/selection/services arguments as the existing method. C's source contract records at the actual ordinary-kernel or read_input terminal Err only, preserves first capture on propagation, and returns fresh owned data with no lingering program/context/service borrow. It does not report warning sites or authenticate native ingestion.

### B. Private entrypoint ownership and a usable four-file API

Keep the adapter private and invoke it **only on the own compiled program's freshly returned failure**, joined to that program's own LoweredIntPlusRow. A matching owner-supplied `(unit,node)` or call record is a consistency check, not authentication: another program can deliberately reuse the same IDs. No public arbitrary `(report,spec)` reinterpretation API, cache, global IDs or synthetic ReportedLocalFailure constructor is proposed.

Proposed crate-private associated methods on the already exported PreparedIntPlusRow:

```rust
fn prepare_diagnostics(
    &self, max_nodes: usize, max_depth: usize, max_rendered_bytes: usize,
) -> SeedResult<PlusDiagnosticPlan>;

fn eval_selected_reported(
    &mut self, ctx: &mut EvalContext, chunk: &Chunk,
    row_schema: &[FieldType], selection: &[usize],
    diagnostics: &PlusDiagnosticPlan,
) -> PlusEvaluation;
```

The three bounds are explicit owner inputs, not new defaults. PlusDiagnosticPlan is opaque, non-executable, and holds Arc ownership of the **same** immutable spec; compare Arc identity to the prepared caller before evaluation. It can be returned and passed back by type inference, so these associated methods need no new root tikv/mod.rs export. Diagnostic/result types and read-only methods must have actual `pub(crate)` visibility, not private unusable return interfaces. Existing `eval_selected/eval_one` remain unchanged raw-runtime APIs.

Proposed PlusEvaluation owns `outcome: Result<Vec<Datum>,PlusFailure>` and `WarningEndpoints { before, after }`, with read-only access/consuming `into_parts`. PlusFailure distinguishes **Caller** preflight/materialization SeedError from **Runtime** ReportedLocalFailure; Runtime may additionally own a checked joined site and an optional native EvalError view. Provide getters for raw/caller/native/site data and a consuming raw-error recovery path. Never fabricate a C report for a caller failure, replace an original LocalError because adaptation fails, or drop the raw error after constructing a native view. The exact public SQL error/sink interface is still outside this slice.

### C. Exact joins before any overflow classification

The diagnostic plan checks its source-table counts/topology and D3's slot mapping once without reading values. Input slots are deliberately unique **per source leaf occurrence in D3**, not generally unique in C. Require `binding_nodes.len()==core.bindings.len()==core.schema.len()`, every mapped ordinal in range and distinct, and each mapped source node to be the matching Column/index/full declared binding type. Repeated native column indices are legal but use distinct source slots. Node ordinal, binding slot, source(unit,node), selection occurrence and physical row are five different identities.

For any returned site first verify its row belongs to the same invocation: occurrence in selection bounds, `selection[occurrence]==input_row`, and physical row within the captured universe. Then:

- **Kernel:** stage must be Kernel and site must be Kernel. Find the call record by its all-node ordinal in this spec and compare the **entire** OrdinaryCallSite, including source ID, profile and raw optional PB signature. The ordinal must be a SourceShape::Call and SourceNode::Call selecting exact TiPb203, with two child ordinals, the correct computed-output record, and real signed non-array LongLong argument/result metadata plus own Int ValueMetadata. TypedRow must have no PB signature/origin; PbRow must retain raw203 and the D3-validated origins. The root/child row facts remain those accepted by C3a; Identity is derived from that closed route and D3's native-kind proof, not from an invented getter or a user label.
- **Input:** stage must be Input and site must be InputSlot. Bounds-check the slot and map it through D3's validated unique slot→leaf table. Join to that actual Column/binding and row. This is a caller-native leaf join, not a C-reported source ID and not the nearest PLUS call. It remains a raw input error even when the underlying code is1690 or the variant is ResourceLimit.
- **No/mismatched site, unknown profile/domain, inconsistent source facts, Resource/Validation/Unattributed:** preserve raw failure and no native overflow view. Never repair a mismatch by choosing the root or an ancestor. Do not mistake child propagation for a new parent failure.

For `plus(col0,plus(col1,col2))`, all-node ordinals are0/1/2/3/4, call ordinals0/2, and slot→leaf is0→1,1→3,2→4. Failure reading slot1 maps to leaf3, not call2; a failing inner prepared kernel maps to call2, not root0. `[2,0,2]` does not merge occurrences0 and2. These checks cannot establish ownership of a foreign report with identical IDs; the private own-program→own-spec call chain is the ownership guarantee.

**Only after the successful Kernel/site/domain join**, query `sql_error_code()==Some(1690)` and construct the narrowly admitted native overflow view. The getter in staged C source borrows the typed existing common error: Custom keeps its code, Other stays10000 even if text says1690, and storage/non-Evaluation returns None. D imports no tidb_query_common or error-code category API, changes no Cargo edge, and never parses/downcasts an erased message. Unknown/non-overflow codes remain raw. Report Display/Debug is not part of classification.

### D. Pure bounded rendering: two facts per call, not one normalized name

Use the existing retained SourceShape/NodeMetadata/ComputedOutput facts only. The renderer accepts no Expression, Constant, Columns, Session, EvalContext, row reader, or callback. It never calls numeric_expression_text, numeric_argument_text, arithmetic_overflow_error, Constant::eval_in, native eval, casts or signature dispatch. The only requested scalar-function loan is `pub(crate)` visibility for existing **pure** `binary_op_for_name` and `arithmetic_symbol`; both bodies and all native callers stay unchanged.

Build a flat plan in bounded iterative postorder with checked byte arithmetic, not an error string for every subtree. Each node has an **as-an-operand** renderability/byte-length fact. Each PLUS call additionally has its **own-overflow** fact:

- Literal Int renders its retained decimal integer value; typed NULL renders `NULL`. These are captured strict literal facts, not row/value evaluations.
- A Column renders its original name verbatim, or `Column#<unique_id>` when empty. D3 already excludes virtual/correlated evaluation. Preserve case/UTF-8 bytes, rather than quote/normalize or read a row.
- A call **as a nested operand** obtains an optional display symbol from the retained full CiString's lowercase lookup through the two existing pure facts. Render `(<left> <display-symbol> <right>)` only if that symbol and both operand forms exist. Unknown labels—including default `sig_PlusInt`—are unrenderable. Do not rewrite the stored CiString or derive a display name from203.
- A call's **own** overflow uses the actual verified PLUS operation's `+` over the two operand forms, regardless of its own display name. Thus a default PB `sig_PlusInt` with simple leaves can render its own failure, but that same node is unrenderable as an ancestor's operand. A renamed nested PB `MiNuS` may display `-` while still executing203; preserve that native behavior rather than normalize it.

If the verified1690 call's own form is natively unrenderable, the native view is exactly `EvalError::IntOverflow`, matching the existing renderer fallback. Otherwise construct `EvalError::DataOutOfRange { value: "BIGINT", expression }` from a single bounded iterative token walk rooted at the **failing call**. The root symbol is forced `+`; nested symbols use display facts. No evaluated operand values are needed from the runtime. Test this against native reference evaluation only in isolated oracle fixtures, never in production error handling.

Limits apply to graph visits/depth and the UTF-8 byte size of any admitted diagnostic expression (integer/Column# atoms have bounded sizes). Compute lengths with checked addition; do not build quadratic repeated subtree strings, recursively format, or fall back to IntOverflow merely because a byte/depth budget was exceeded. Plan refusal is a distinct pre-execution caller resource/admission error. A later bounded `try_reserve`/render inconsistency after a runtime failure leaves the original report raw with no invented native diagnosis. This is bounded diagnostic work/storage, not a total-process bound or a promise to bound pre-existing arbitrary FieldType metadata bytes and D3's schema preflight.

### E. Warning endpoints cover the entire caller invocation

Define Copy data only: `WarningEndpoint { warning_cnt: usize, stored_len: usize }`; WarningEndpoints contains before/after. Capture **before any invocation preflight**, including diagnostic-plan/spec identity, NativeInputs::new schema/layout/selection and C's own preflight. Use one inner ordinary function returning an owned outcome; the outer method samples after it returns normally, including cleanup, on success and every returned failure (caller preflight, runtime or materialization/adapter outcome). No outer early `?` may bypass the after sample. Preparing a static diagnostic plan separately has no context/value effects.

Read only live `ctx.warnings.warning_cnt` and `ctx.warnings.warnings.len()`. Do not consult ctx.cfg.max_warning_cnt as the receiver's cap, assume the default64, expose/guess a new cap API, drain/take/reset/merge/sort/truncate warnings, or clone the warning vector into production receipts. EvalWarnings::default has cap0 and can coexist with a differently configured ctx; after a receiver/config replacement, configured and live caps need not agree. At cap, count may grow while stored length does not.

Endpoints are observations only: no saturated delta, subtraction across wrapped/non-monotone counters, claim of retained-new-detail count, or general prefix-byte authentication. Equal endpoints cannot prove the contents were unchanged. Current pure NativeInputs/adapter never drain or mutate diagnostics, and tests may copy prefix bytes to assert preservation for those paths. Synthetic callbacks can test warning-prefix behavior and expose receiver replacement without turning it into a general authentication guarantee. There is no after endpoint for panic, no catch/reclassification, and no per-warning site attribution; a subsequent invocation uses fresh C reporting state and a fresh before endpoint.

### F. Exactly four proposed caller files and focused gates

| File under `tidb-expr/src/` | Proposed edit, only after explicit release |
|---|---|
| NEW `tikv/ordinary_diagnostics.rs` | Opaque bounded plan, immutable join logic, pure overflow view, endpoints/outcome types and reported associated methods on PreparedIntPlusRow. Private fresh-report adapter only. |
| NEW `tikv/ordinary_diagnostics_tests.rs` | Native-reference and counterexample gates below; no production evaluator wrapper. |
| `tikv/ordinary.rs` | Narrow child-module declarations/API visibility plumbing and, only where necessary, access to existing checked source/binding/materialization helpers. Existing six-file runtime behavior remains unchanged. |
| `scalar_function.rs` | Visibility-only changes on `binary_op_for_name` and `arithmetic_symbol`; no formatter/inference/evaluation body edit. |

Declare diagnostic modules under ordinary.rs; associated construction on the already exported PreparedIntPlusRow keeps the four-file API usable without an unrequested tikv/mod.rs export loan. No ordinary_tests/D1/D2/catalog/context/lower/PB decoder, Cargo/lock, C/common/datatype, public evaluator, warning sink or entrypoint write is proposed. If a concrete compiled API needs an additional file, ask rather than silently expanding this set.

Proposed tests (not written/run):

1. `reported_plus_joins_exact_inner_call_and_row`: nested and root overflow produce different correct all-node call sites/source IDs and native expressions; reversed/repeated physical selections retain distinct occurrence/row identities and stop at the error prefix.
2. `reported_input_is_a_leaf_not_an_enclosing_plus`: actual Input1690 and Input ResourceLimit join unique slot→leaf facts, keep raw variants and never produce PLUS native errors. Repeated column indices still use distinct source slots.
3. `reported_plus_uses_typed_code_only_after_join`: mismatched call/profile/source/row or invalid own output-domain facts decline adaptation. Unsited eager222, generic Other containing1690, storage/non-Evaluation and non-overflow codes stay raw; no parsed-message/root-name heuristic. These test consistency, not authentication of indistinguishable foreign IDs.
4. `pb_plus_keeps_own_rendering_and_operand_fallback_distinct`: default sig_PlusInt leaf failure is source-shaped; an outer failure after successful nested default PB addition retains IntOverflow fallback. Renamed mixed-case PB labels preserve native display behavior while execution remains203.
5. `plus_diagnostic_plan_is_bound_to_its_prepared_spec`: even equal owner IDs on separate specs do not allow the wrong plan; validation occurs before reads. No public arbitrary-report adapter exists.
6. `plus_rendering_is_iterative_and_byte_bounded`: exact/over node/depth/UTF-8-byte limits, long names, Column# fallback and signed minimum literal; no per-subtree string explosion. Budget refusal is not native fallback, and raw error survives any adaptation refusal.
7. `reported_endpoints_include_all_normal_exit_paths`: empty/success, wrong plan, bad native schema/selection, runtime input/kernel error and defensive materialization failure all get before/after endpoints; first capture precedes every preflight and after capture follows inner cleanup.
8. `reported_endpoints_use_live_receiver_not_configured_cap`: cap0 default receiver versus nonzero cfg, an already-full cap1 receiver after cfg changes, and normal larger receiver; counts/lengths are observed exactly without drains or inferred cap/new-detail arithmetic. Tests can compare copied prefix bytes; production receipt does not authenticate them.
9. `reported_endpoints_do_not_repair_nonmonotone_or_panic_state`: synthetic reset/wrap-like endpoint changes are not saturated/subtracted/repaired; panic returns no receipt, and later success/empty/error has fresh reporting state rather than stale attribution.
10. `pure_overflow_view_preserves_raw_error_and_legacy_paths`: original report/error ownership survives optional native view and consuming raw recovery; source-only negative-call audit plus isolated native reference oracles establish no native formatter/eval in adaptation. Old D3 raw entry and all D1/D2/native formatter results remain unchanged.

Parent must first accept C3d's reported API/wiring in both compiler contexts, then separately release these four caller files and run new tests plus the frozen10 D3/16 D1/eight D2 and full1264/4/93 comparison (same four complete failure blocks). Passing this private native-overflow view still is **not** general diagnostics, statement context/severity/publication handoff, warning-site support, widened carriers/profiles or public activation. Migrated-family count remains zero.

### G. Source-only D4 receipt

Only this receipt changed. D reread C's staged report/getter/recorder source, warning receiver/counter semantics, retained D3 tables, existing native rendering facts and actual D3 acceptance logs. Parent confirmed the private fresh-report ownership chain, non-authenticated source IDs, count/length-only endpoints and E's completed no-finding read-only audit. D4 code/tests/builds/formatting/activation remain absent. The D3 source hashes and products stay frozen while C3d and D4 grants/gates proceed independently.

## D3 reviewed source/API proposal — historical r7

**Historical grant/status:** parent first released this section of D's receipt for design only and selected strict paired PB ingress plus homogeneous no-PB TypedRow. The D-r8 amendment above supersedes r7's proposed/not-written wording only for the exact six-file runtime-only caller. The separate diagnostic API/locks below remain proposals, not implementation authority. The proposed caller is a small explicit consumer of C3a, not an ordinary-expression migration, zero-effect replacement for SqlBuild, or public evaluator route.

### 1. C handoff: stable source contract, not yet a compiled caller API

Parent relayed these concrete C3a names/shapes; they are the design dependency, not a claim that D has compiled them:

```rust
compile_local_profiled(spec, schema, cx, &OrdinaryProfileSpec)
OrdinaryProfileSpec::new(spec, schema, consumer, sites, limits)
OrdinaryCallSite::typed_row(node_ordinal, source)
OrdinaryCallSite::pb_row(node_ordinal, source, original_signature: i32)
OrdinarySourceId::new(unit: u64, node: u64)
```

The result of `OrdinaryProfileSpec::new` is `LocalResult`; the snapshot is immutable and covers the full flat graph/schema. Node ordinals count **ALL source nodes in preorder**, root0 then arguments left-to-right—not just calls, instructions, input slots or selected rows. Site records must already be strictly increasing, exactly cover calls, and match values/types/slots/shape; C does not auto-sort or infer missing facts. Root consumer is `OrdinaryProfile::TypedRow` or `PbRow`; AST and NativeNumericBatch are refused. Nested call profiles can be independently explicit in C, but the initial caller proposed here deliberately admits homogeneous trees only. The PB constructor stores the **raw i32** and `original_pb_signature()->Option<i32>` retains it; that constructor is an assertion, not an ingestion certificate.

Only exact203, two arguments, signed LongLong at every node and binding, `CallMetadata::None`, and `LiteralKind::Typed` with `ScalarValue::Int(Some(_)/None)` are admitted. An Int carrier tagged Text/BinaryLiteral is not Identity. C derives the actual left-to-right/Identity/left-NULL-stop/right/official-prepared-kernel schedule; D supplies neither an executable callback nor an arbitrary NULL mask. The old222/control/host routes are unchanged, and222 is refused by this new route.

### 2. Proposed caller surface and exact source-path proof

All names below are **proposed crate-private caller APIs** under `tidb-expr/src/tikv`; they do not exist yet:

```rust
fn lower_typed_int_plus_row(
    root: &Expression, row_schema: &[FieldType], new_collation: bool,
    source_unit: u64, limits: CompileLimits,
) -> SeedResult<Arc<LoweredIntPlusRow>>;

fn lower_pb_int_plus_row(
    root: &Expression, original_wire: &tidb_proto::tipb::Expr,
    row_schema: &[FieldType], new_collation: bool,
    source_unit: u64, limits: CompileLimits,
) -> SeedResult<Arc<LoweredIntPlusRow>>;

impl PreparedIntPlusRow {
    fn compile(spec: Arc<LoweredIntPlusRow>, limits: ExecutionLimits) -> SeedResult<Self>;
    fn eval_selected(&mut self, ctx: &mut EvalContext, chunk: &Chunk,
        row_schema: &[FieldType], physical_selection: &[usize]) -> SeedResult<Vec<Datum>>;
    fn eval_one(&mut self, ctx: &mut EvalContext, chunk: &Chunk,
        row_schema: &[FieldType], physical_row: usize) -> SeedResult<Datum>;
}
```

Require a PLUS root; its children are only recursively admitted PLUS, complete ordinary columns or strict Int/typed-NULL constants. Both ingresses use one bounded iterative lowering implementation; the only ingress distinction is proved native path and, for PB, the original wire comparison. No public consumer label can stand in for proof. `source_unit` is an explicit owner-supplied identity, not a new global/TLS counter or evidence of origin.

**TypedRow proof:** `ScalarFunction::eval` at `scalar_function.rs:1192–1212` chooses the PB branch first, then `eval_fast_integer_binary`. Require `pb_signature()==None` AND no PbOrigin on every node; call name exactly `plus`; actual arity2; signed LongLong arguments/result; no VALUES/grouping state; no virtual/correlated column or parameter/deferred Constant. The fast path at1457–1514 then evaluates left, returns on left NULL, evaluates right and calls integer arithmetic. `eval_numeric_row:3513–3557`, `constant.rs:160–214`, and `cast_numeric_argument_in_mode:3607–3612` establish Identity only after native kind/type restrictions; the broader native helpers do support conversions and must not be advertised as generally effect-free.

For SQL signature selection, reuse the **existing signed/signed PLUS CATALOG row**, `pushdown_catalog.rs:1337–1343`. Its `name/selector/arg_types/ret/sig` fields and ArgPattern facts are already public. Read that row's203 identity after validating its exact two signed Int pattern/Int result; do not call `resolve` through fabricated PbScalar arguments, `from_expression`, `expression_to_pb`, `to_pb`, or any builder. The generated numeric enum conversion remains the only cross-proto mapping; TiKV's canonical selector remains the only kernel selector.

**PbRow proof:** `distsql_builtin.rs:193–241` really decodes each child through `pb_to_expr`, chooses `PbBuiltin::new(sig)`, builds `ScalarFunction::from_pb`, and attaches a captured PbOrigin. `PbBuiltin::new` at `scalar_function/pb_builtin.rs:73` maps203 to Binary(Plus,Int); its row branch275–280 NULL-stops before the right operand. Demand proof therefore requires a real selected PB builtin203, not a display name or a FunctionRef alone. Each node must have an immutable origin whose effective type matches, original explicit wire type is signed LongLong/non-array, original signature/ExprType/arity/payload agree, encoded Int/column-offset bytes decode completely, and actual current value/column offset agrees. Use the existing E-hardened `matches_effective_type` and lower.rs origin checks. PB descendants must retain provenance; any missing/stale/replaced SQL child is refused.

**Why the original wire argument is required in this minimal proposal:** current PbOrigin stores node-local fields and `child_count`, not original child identities. Those alone cannot prove unchanged ancestry/order if a child is replaced by another genuine PB-origin node. Iterate `(current Expression, captured origin, original wire node)` together in source order, comparing raw optional fields/payload and corresponding children before publication. This strengthens proof without changing the decoder or recording another recursive tree. If the owner no longer has the original wire, this new PB ingress refuses; it must not reconstruct a PB Expr to manufacture evidence. The wire is borrowed only during lowering, never retained as executable state. This verifies consistency against the **trusted retained input supplied by the front-end**, not cryptographic object history. Node-local PbOrigin never promised original tree ancestry; this stronger new-ingress requirement is not a newly discovered D1 value bug or a change to D1's contract.

**Real API gap, not a retag:** `pb_literal` at `distsql_builtin.rs:105` builds PB NULL with actual SQL `FieldTypeCode::Null`, even if its wire field claims LongLong. Such PB NULL literals remain refused. Nullable, genuinely decoded LongLong columns may produce runtime NULL; a strict typed-NULL Constant is admitted on the separate TypedRow ingress. No synthetic FieldType fixes this gap.

Both entrypoints begin with bounded metadata/shape/provenance work. They consume an **already bound/inferred** native tree; they do not call rewrite/NewFunction, fold/probe constants, insert casts, bind names, or fetch row values. Ordinary SqlBuild may already have evaluated constants or emitted warnings before this tree existed. D3 promises no additional value effects during its lowering, not retroactive zero effects for SqlBuild. D2 StructuralOnly still refuses arithmetic; its grammar and purpose semantics are not widened.

### 3. Binding, immutable metadata and per-computed-node identity

- Proposed `LoweredIntPlusRow` privately contains the existing common LoweredSpec storage envelope, C's immutable OrdinaryProfileSpec, and per-node source/result records. Do not expose that envelope as a D1 compilation entry. Keep Arc sharing for immutable data and worker-owned LocalProgram/LocalEvalState; no native Expression, evaluator closure, Columns, Session, cache or diagnostic sink survives in the spec.
- Walk every node in source preorder while building LocalExpr iteratively. Every call gets `OrdinarySourceId::new(source_unit, checked_u64(node_ordinal))` and the actual path's site constructor; every PB call passes the captured raw203. Strict literals become **Typed** Int carriers only via `literal_value()`, never stored parameter snapshots. Bound columns get one input slot **per source occurrence**, with an explicit slot→leaf-node-ordinal map; no cross-occurrence memoization or coincidental ordinal/slot equivalence.
- Preserve the complete detached SQL FieldType at every source node, binding and result using the existing snapshot helper, plus original PB optional metadata, initialized collation/coercibility/repertoire and complete column identity. No public accessor may return a shallow FieldType clone that aliases mutable backing; any later accessor must snapshot or answer a predicate as E's origin fix does.
- Every computed PLUS node owns an explicit `ValueMetadata {kind:Int,string_collation:None,decimal_declared_shape:None}` tied to **its own** detached result type. This includes `plus(x,0)` and NULL results; neither is declared a passthrough. Literal-vs-input and literal NULL source facts remain separate. Intermediate RPN values need not be round-tripped through Datum; their output records are nevertheless explicit. Final materialization uses the root's record, not an arbitrary operand's metadata or a global same-eval-family assumption.
- Reuse the native schema/layout/physical-selection preflight from `tikv/context.rs:37–68`. Factor its current demanded value read76–102 into a pure-structure/demanded-Datum helper, preserving the D1 order. D1's existing adapter then continues its old conversion unchanged. The new PLUS adapter must **match Datum::Int(_) or Datum::Null immediately after that one demanded read and BEFORE `to_scalar`**; reject UInt, even0, before B erases its identity into an Int carrier (`tikv_compat/value.rs:245–246`). Do not eagerly scan values to prove this. Literal kind is checked before its transport as well.
- The source of a physical Chunk value is the declared schema plus its safe row decoder. This cannot recover the historical Datum variant passed into a raw cell or detect corrupted private buffers; do not claim that. A test-only raw-Datum supplier exercises the pre-bridge kind boundary because an already-converted VectorValue service cannot represent the lost Int/UInt distinction.
- Compile with C's reported `compile_local_profiled` using exactly the captured LocalExpr/schema/facts. Execution uses the existing `eval_with_bindings` and official Ordinary frame/prepared helper only. Empty/repeated/nonidentity physical selections retain row-occurrence order for these two row profiles, not batch semantics. Keep raw `SeedError::Local(LocalError)` intact on failure and preserve the caller-owned EvalContext warning prefix. No retry, SQL NULL substitution or native error formatter is called.

### 4. Requested runtime-only caller locks — exactly six files if released

| File under `tidb-expr/src/` | Narrow proposed work |
|---|---|
| NEW `tikv/ordinary.rs` | Both checked ingresses, one iterative PLUS lowerer, opaque LoweredIntPlusRow, source/result sidecars, strict native-kind adapter, PreparedIntPlusRow and explicit materialization. This is caller lowering/binding, not another runtime evaluator. |
| NEW `tikv/ordinary_tests.rs` | The dedicated caller gates below; test-only raw-Datum/recording services, never a production native evaluator wrapper. |
| `tikv/mod.rs` | Declare/export only these private entrypoints/test module; retain existing SeedError and D1 exports. |
| `tikv/catalog.rs` | Add an exact PLUS-row fact/admission helper using the existing signed CATALOG row or the validated genuine PB203. Leave `int_control` and its admissions unchanged. |
| `tikv/lower.rs` | Narrow sibling visibility/factoring of existing origin/type/projection predicates and storage helpers only. Leave `lower_int_control_seed`'s walk, output, domain and behavior unchanged. |
| `tikv/context.rs` | Factor its already validated single demanded native Datum read; D1 still performs its original bridge step. The new adapter alone adds the strict pre-bridge kind veto. No new session/value hooks. |

No D3 runtime-only edit is proposed to `tikv/batch.rs`, D1 tests, any D2 file, `distsql_builtin.rs`, `scalar_function.rs`, pushdown catalog production code, lib.rs, evaluator/AST entrypoints, C/B products, Cargo or lockfiles. Reusing the public CATALOG facts avoids a new pushdown-catalog lock. No implementation is authorized by this list; ask for a separate exact release after C's API/compile checkpoint.

### 5. Proposed caller tests — not written or run

All belong to new `tikv/ordinary_tests.rs` if released:

1. `typed_plus_uses_exact_203_and_all_node_preorder`: root and nested PLUS, sorted complete call facts, source-unit identity and raw203; D1 still refuses PLUS and no203→222 substitution occurs.
2. `pb_plus_requires_actual_ingestion_and_original_tree`: real `pb_to_expr` plus original wire succeeds; a `from_pb`-only node, label-only fake, missing/stale origin, changed type/literal/index, trailing encoding bytes, or swapped genuine PB children against original wire is refused before reads. PB display-name changes do not reselect execution.
3. `pb_null_literal_is_not_retagged`: actual Null effective type is rejected despite a LongLong wire field; nullable PB LongLong columns and TypedRow typed-NULL literals exercise the admitted NULL paths separately.
4. `plus_native_kind_is_checked_before_bridge`: demanded UInt0/UIntMAX/Bytes/BinaryLiteral/Real are refused through a raw-Datum test seam; a NULL-left skipped input is never read or kind-checked. Wrong **static** children still fail admission even when dead.
5. `plus_row_demand_preserves_error_prefix`: left NULL skips poisoned right, left failure stops right, non-NULL left demands right, inner overflow retains LocalError identity and no later occurrence is read. Existing warning count/details remain intact.
6. `plus_computed_outputs_own_detached_metadata`: every call has its own Int output identity/full result metadata, source/binding/type mutations cannot change the spec, and plus(x,0)/NULL do not borrow operand metadata.
7. `plus_selections_and_bindings_are_occurrence_local`: empty/1/1024/1025 and `[2,0,2]`, separate native Chunk::Sel, full schema/layout mismatch before values, duplicate physical row occurrences without value-cache reuse.
8. `plus_profile_snapshot_and_limits_are_checked`: real source-preorder map including leaf ordinals, stale values/types/slots or call records refused by C; bounded source/preparation limits and supported deep trees without a recursive native clone. No claim to change native Expression Drop.
9. `plus_closed_domain_never_reaches_native_hooks`: AST/batch,222, other ops, controls/hosts, mixed profiles, unsigned/Tiny/untyped-NULL, cast/default/parameter/deferred/correlated/virtual and opaque metadata are negative admission cases; no formatter, fold, eval, resolver or host callback executes.
10. `plus_caller_coexists_with_frozen_d1_d2`: paired source-level checks that D1's control entry still accepts its seed/rejects PLUS, D2 StructuralOnly still rejects arithmetic before binding, and ordinary SqlBuild's existing arithmetic construction/folding remains available unchanged. This test does not invoke Cargo.

Parent additionally reruns the released16 D1 and8 D2 tests, the legacy constructor/rewriter checks and full baseline comparison after the six-file caller cut. Runtime-only acceptance proves the explicit slice's transport, demand, immutable records and intact **raw** errors/prefix. It cannot prove native overflow text/site parity while C lacks a failure-site report, so it does not release a SQL caller or earn a migrated family.

### 6. Minimal later diagnostic seam — separately locked, non-evaluating

**Observed gap:** C3a retains source identity on PreparedOrdinaryCall but currently returns only LocalError. A numeric error code/message cannot identify which nested call failed. `IntIntPlus::calc`, `tikv/.../impl_arithmetic.rs:45–48`, reports evaluated operand values; TiDB `arithmetic_overflow_error`, `scalar_function.rs:524–539`, reports source-shaped operands. Its renderer333–400 calls `Constant::eval_in` and is forbidden on the adapter path. Do not reevaluate, call that renderer, parse TiKV's message into a source site, or guess the root.

Propose an **additive** C-owned API, not part of C3a's seven-file release:

```rust
// Proposed method signatures, NOT existing APIs.
impl LocalProgram {
    fn eval_with_bindings_reported(
        &mut self, state: &mut LocalEvalState, ctx: &mut EvalContext,
        physical_rows: usize, selection: &[usize],
        services: &mut dyn LocalRuntimeServices,
    ) -> Result<VectorValue, ReportedLocalFailure>;
}

// Opaque construction in the driver; retain the original owned LocalError.
impl ReportedLocalFailure {
    fn error(&self) -> &LocalError;
    fn into_error(self) -> LocalError;
    fn site(&self) -> Option<&LocalFailureSite>;
    fn sql_error_code(&self) -> Option<i32>;
}

// Conceptual read-only site data, written by the exact failing operation:
LocalFailureSite { row: InputRow, origin: FailureOrigin }
FailureOrigin::PreparedKernel { node_ordinal: usize, source: OrdinarySourceId }
FailureOrigin::InputSlot { slot: usize }
```

`sql_error_code` would inspect the existing typed common error, not stringify it. TiKV already has the dependency and `tidb_query_common::error::{ErrorInner,EvaluateError}` plus EvaluateError::code; the caller Cargo manifest does **not** currently depend directly on tidb_query_common, so the C-owned getter avoids pretending that import is already available or silently adding a Cargo edge. Non-evaluation/storage/contract/resource failures return no fabricated SQL overflow classification.

Attach `PreparedKernel` at the **actual prepared-kernel Err**, before frame teardown. Child errors propagate their existing report unchanged; an ancestor that never ran its kernel cannot replace the site. A read failure is `InputSlot`, not its nearest enclosing PLUS. Preflight has no row/call site; resource/shape failures remain their own LocalError and must not masquerade as a kernel error. Any additional resource location is optional separate data, not a SQL-call attribution. The new reported entry and old entry must share the same driver/prepared helper; the old return type remains unchanged, discarding only optional reporting data. A result-carried report avoids stale last-failure state leaking across successful/empty invocations.

Concrete identity example: `plus(col0,plus(col1,col2))` has node ordinals `[0 call,1 leaf,2 call,3 leaf,4 leaf]`, call sites0/2, binding slots0→1,1→3,2→4. Failure reading slot1 names leaf3; call2 is merely its parent. Failure in the inner addition names kernel-node2, not root0. `InputRow.occurrence` and `input_row` are yet another pair: repeated physical row2 in `[2,0,2]` occurs at0 and2 and must not be collapsed. The prepared caller joins a reported kernel site to the exact immutable spec/source ID; a mismatch is not repaired with a root fallback.

**Static diagnostic source sketch:** capture source atoms while lowering: strict Int/typed NULL values, column original-name-or-unique-ID facts, call display name and child ordinals. This is a flat non-executable sidecar, never retained native expressions or pre-rendered whole subtrees at every node. A later pure bounded renderer starts at the reported failing call and renders once; metadata/diagnostic byte limits must be explicit before activation, without quadratic subtree strings or native recursive formatting.

Execution identity and diagnostic display are distinct. `from_pb` at `scalar_function.rs:673–680` sets names such as `sig_PlusInt`; nested native rendering392–399 dispatches on the **display** name. Thus an inner PB PLUS failing on leaf operands can produce source-shaped DataOutOfRange, while an outer PLUS failing after a successful nested `sig_PlusInt` can fall back to `EvalError::IntOverflow` because that nested name is not renderable. A renamed PB child can also change diagnostic text without changing execution203. Retain those source facts and native renderability/fallback, rather than normalizing all PB display nodes to `+`. A narrowly factored **pure** display-operator fact helper from `scalar_function.rs:93–117,321–330` would avoid copying that mapping; no existing formatter/evaluator is called.

With a validated exact203/Identity/signed result site and typed overflow code1690, a future pure caller adapter can construct the native DataOutOfRange or its observed IntOverflow fallback from the sketch while retaining the original ReportedLocalFailure alongside that native view. Input errors—even if they carry code1690—must not become PLUS overflow. Unknown errors stay raw; source-string equality alone is not a classifier. No session callback or Columns argument belongs on this adapter.

**Warning prefix:** capture `{warning_cnt, stored_len}` before/after the explicit invocation and leave EvalContext untouched. `tidb_query_datatype/src/expr/ctx.rs:184–209` shows total count and retained details differ once the cap is reached, so a vector length alone is insufficient. The first admitted native PLUS/Identity/Chunk-read slice generates no new conversion/kernel warnings; preserve and test the existing prefix on success and every failure. Synthetic input-service warnings can test prefix order/count but do not prove per-warning source attribution. Current stored warnings have no node/occurrence tags; any later warning-producing slice needs a separately approved append-time site/event seam, not retrospective attribution, deduplication, truncation, draining or native reevaluation.

**Separate diagnostic locks, if parent/C/E agree:** C runtime: NEW `local/diagnostic.rs`, NEW `local/diagnostic_tests.rs`, narrow `local/mod.rs` exports, `local/batch.rs` additive reported entry, `types/expr_eval.rs` actual operation-site capture/plumbing. Reuse C3a's already retained identity/getters; if its final API requires another file, request it explicitly. Caller/E: NEW `tikv/ordinary_diagnostics.rs`, NEW `tikv/ordinary_diagnostics_tests.rs`, `tikv/ordinary.rs` report/source-sketch adaptation, and `scalar_function.rs` **pure display-fact factoring only**. Nested diagnostic modules can be declared in ordinary.rs. No kernel, native formatter, LocalError variant, common error representation, Cargo, public evaluator or session sink change is presumed. This is a proposed later release, not authority to edit C/E files.

Diagnostic acceptance must include nested-vs-root overflow, PB default-label fallback/renaming, input-slot failure versus enclosing call, duplicate physical rows with distinct occurrence sites, no ancestor overwrite, no stale report after success/empty input, preserved raw LocalError, warning count/detail caps/prefix and poisoned native format/eval hooks. Only after that scoped comparison, an explicit context/severity handoff, and a separate public-entrypoint release could any activation be considered. C3a runtime acceptance and D3 private caller acceptance alone remain insufficient.

### 7. Design receipt and next gate

D reread the actual native dispatch, PB decoder/origin predicates, bridge, native Chunk adapter, catalog facts, error renderers, warning storage and the existing shared runtime loop; parent relayed C's concrete source API and raw-signature/ordinal clarification. D sent the provenance-ancestry and PB-display diagnostic findings back through parent for C/E. This revision changes only `lowering-contract.md`; no D3 tests, implementation, formatter/build, dependency, cache or execution route was created. D waits for C's compiled checkpoint plus an exact caller file release; the site-aware diagnostic API remains an independently requested gate.

## C3 caller constraints — design-only relay, no product release

Parent requested this clarification for C after the two-call D2 correction was handed back. C owns its runtime contract; D has not edited it. **All D2 products remain frozen.** The following is source-derived C3 admission/design guidance, not an implemented profile API, ordinary-call activation or family-migration claim.

### Demand profiles must identify the actual call path

Keep `AstValueScalar`, `TypedRow`, `PbRow`, and `NativeNumericBatch` distinct in the proposed call/plan/cache identity, alongside the exact signature, operand domains/signedness and result metadata. A profile name alone does not prove every signature in that consumer shares a NULL rule. Initial admission needs an audited combination, otherwise Unsupported before execution—not an inferred generic eager/NULL-stop rule.

| Source path reread | Actual demand contract, not a blanket property of all calls |
|---|---|
| `lib.rs:903–920`, ordinary AST binary arm | Evaluate L then R before the operation. L=NULL does not suppress R; a failing L still stops before R through `?`. Existing AND/OR/CASE/etc. have their separate control handling, not this generic binary rule. |
| `scalar_function.rs:1491–1505`, `eval_fast_integer_binary` | Int Plus/Minus/Mul fast-path arithmetic returns after a NULL L, before R. The comparison arm in that same helper evaluates R even when L is NULL. Other typed dispatch paths require their own proof. |
| `scalar_function/pb_builtin.rs:79–94,248–280` | All four signedness variants of Int MOD use `Kernel::IntegerMod` and demand L then R even with NULL L; ModReal is also eager. ModDecimal NULL-stops. **EqInt/GtInt use Binary(Int) and NULL-stop**, unlike the eager comparison arm of the typed Int fast path. Thus PbRow must be keyed to the actual signature/domain, not just its result EvalType or SQL name. |
| `scalar_function.rs:4149–4158`, `eval_arithmetic_batch` | Evaluate the entire left selected batch, then the entire right selected batch, then the arithmetic-result loop. NULL-left rows do not suppress RHS batch work. An error in an earlier phase stops later phases; this is not one scalar L/R operation per occurrence. |

**Do not label NativeNumericBatch OccurrenceOrdered.** With two occurrences, a later occurrence's left-child error can precede an earlier occurrence's right-child error because the whole left phase comes first. Scalar occurrence order can choose the opposite failure. **Whole-expression1024-row tiling also changes that prefix**: a left-child error at occurrence1024 must precede any right-child error at occurrence0 under the full left-then-right batch schedule, whereas completing both sides of the first tile can report the right error first. Merely increasing scalar width to1024 is not this profile. Likewise all RHS child effects may already have happened before the first arithmetic-result error. If the initial C3 binding path remains width-one occurrence-ordered, refuse NativeNumericBatch there until the existing shared driver has a separately approved actual phase schedule. Do not silently change profiles because arithmetic outputs look equal on non-NULL rows. No second interpreter, native replay, fabricated protobuf or generic NULL shortcut is authorized.

### Stricter initial carrier and output-lineage cut

`same_eval_family` is only a storage-class check, not semantic admission. The actual bridge in `tidb-datatype/src/tikv_compat/value.rs:245–255` puts Int/UInt into the same Int carrier and String/Bytes/BinaryLiteral into the same Bytes carrier (Real/Float32 also share Real). Its `ValueMetadata` at121–135 retains distinctions that carriers erase; `from_scalar` at261–265 explicitly requires computed-output identity instead of arbitrary operand metadata.

The smallest safe proposed C3 caller cut is therefore:

1. **Every participating input, argument, intermediate and result** has a real signed, non-array LongLong SQL type; every admitted non-NULL value is actually `Datum::Int`. NULL is typed within that domain. Reject UInt even when its numeric value fits i64, Tiny/other integer codes rather than retagging them, and all String/Bytes/BinaryLiteral, Real/Float32, mixed-kind, hybrid and complex domains. This is deliberately narrower than B's exact-transport ability and C's physical EvalType compatibility.
2. Validate literal kind plus complete declared metadata before execution; validate a row's actual kind only when that slot/occurrence is demanded. Do not scan/import dead rows or branches to prove the carrier contract. Binding mismatch/admission failures are not SQL NULL, coercion or an invitation to native retry. Keep the complete detached SQL type and immutable origin facts alongside the kernel projection; a matching EvalType is not full schema equality.
3. Each **computed** result must have explicit result-boundary `ValueMetadata`/declared type from the admitted operation contract. In this cut the computed kind is Int (or a NULL carrier reconstructed under that Int result identity), not the first operand's kind or arbitrary selected arm's metadata. The unsigned flag, literal provenance and collation must never be guessed from raw bits/bytes.
4. A genuine passthrough/selected branch can reuse only the actually selected source occurrence's lineage, and only if the operation really preserves it after any source-required coercion. Initial homogeneous Int controls avoid a new dynamic lineage API; String/Bytes/BinaryLiteral mixtures would need such a proven rule or must remain refused, even with identical bytes and binary collation. A computed string must not become BinaryLiteral merely because one operand was a literal, nor may UInt bits become a signed result merely because the kernel uses i64 storage.
5. The exact verified kernel signature/domain, demand profile and computed-result identity must agree before publication. Do not reinterpret a legacy/unsupported signature as another signature in the same family. In particular, **PlusInt203 already has canonical TiKV selection** (`tikv/components/tidb_query_expr/src/lib.rs:472`, `map_int_sig(...,plus_mapper)`); `local/registry.rs:34–49` currently admits the addition identity PlusIntSignedSigned222 instead. C3 would have to explicitly admit203 and preserve it—not remap203→222 or invent a missing kernel. StructuralOnly is not exact SqlBuild inference, and D2 currently rejects arithmetic: this C3 design does not release a new D2 grammar, reinterpret incomplete planner flags, broaden D1 or install an evaluator route.

Suggested future gate witnesses (not tests run by D): L=NULL with a warning/error RHS across all relevant scalar profiles; PB MOD versus a verified NULL-stopping binary; two-occurrence cross-phase error/warning/read order for native batch; repeated physical rows under a nonidentity selection; and same-carrier but wrong-kind UInt/String/Bytes/BinaryLiteral inputs/results that must be refused or preserve a specifically proven output lineage. Actual selected diagnostics and demand order—not numerical equality alone—are acceptance criteria.

## D2 source/API refinement — historical reviewed D-r3 proposal

**Historical status:** this was the source/API-only proposal parent reviewed before the four-file D2-min release; the implementation amendment above supersedes its proposed/not-written wording only for that exact cut. The larger serial locks and runtime profile/simple-CASE work below remain unreleased. D keeps D1's domain/APIs frozen; the migrated-family count stays zero.

### Why the preparation purpose must be independent of fold mode

Fresh source inspection found these reachable effects. Offsets below are current observations; names are the stable anchors. All paths are under `tidb/rust/crates/tidb-expr/src/`.

| Existing source site | Actual behavior; required StructuralOnly disposition |
|---|---|
| `rewriter.rs:208–231`, `constant_fold.rs:47–48` | The default `ColumnResolver::fold_constant` calls `derive_constant_null_flag`, which calls `fold_value`: even a seemingly metadata-only fold evaluates. Do not call the resolver fold hook at all in StructuralOnly, including with Disabled. Do not call `eval_constant`. |
| `rewriter.rs:1026–1034` | Explicit non-JSON CAST forces Normal, overriding the inherited fold mode. Preserve this in SqlBuild; the separate preparation purpose must veto the fold in StructuralOnly. |
| `rewriter.rs:431–465,479–533,579–746` | `wrap_binary_literals`, `wrap_power_arguments`, and `binary_expression` insert and fold casts; binary construction also calls comparison refinement and `ScalarFunction::prepare_numeric_arguments`. Structural insertion and optional value work are different operations. |
| `rewriter.rs:1724–1803` | Searched CASE's result casts can directly call `fold_constant_in_mode` through `comparison_context`, bypassing the resolver fold hook. Simple CASE additionally clones its selector into every equality. The first is vetoed; the latter is rejected before rewriting the selector in D2-min. |
| `new_function.rs:293–307,320–406` | DATE_ADD/SUB evaluates its unit with `eval_expression_once`; shared construction runs comparison refinement, binary-literal folding, numeric probes, arbitrary construction callbacks, and final folding. Base/Disabled suppresses only the last fold. |
| `scalar_function.rs:1261–1359` | `prepare_numeric_arguments` evaluates strict literal conversions and strict Decimal subtrees (1306,1325), then refines return precision/scale. It is not a pure cast-insertion helper. Do not run it with NoColumns and call that structural. |
| `builtin_compare.rs:177–229,313–365,555–612,657+` | Even `refine_integer_comparison_for_rewrite` enters value refinement through NoColumns. The context-aware path can convert values, fold CEIL/FLOOR, produce diagnostics, and disable plan caching. No “context-free means effect-free” assumption. |
| `aggregation/wrap_cast.rs:180–225` | `wrap_with_cast_as_decimal` constructs a cast, then evaluates a strict cast against NoColumns to refine precision. Its structural portion must be separated before admitting this path. `simple_expr::build_cast_function` itself is a node/type constructor, not that probe. |
| `builtin_op.rs:79–122`, `constant_fold.rs:381–390` | Unary-minus type inference uses `folded_value`, which evaluates a foldable nonliteral subtree. Direct immutable literal inspection is different from this fallback. |
| `rewriter/result_type.rs:1051–1088` | Decimal ROUND/TRUNCATE scale inference calls `Constant::eval`, `cast_arg_as_int`, and `eval_int`. Keeping the same type table alone does not remove its value probe. Other helpers read literal values; mutable/deferred Constants must never be mistaken for literals. |
| `rewriter.rs:1183–1204,2233–2275` | Parameters obtain a current value at construction; invalid CHAR charset handling explicitly creates `FoldWarningContext` and reports code-point truncation before returning its error. Both are excluded before those hooks in D2-min. Typed temporal literals likewise stay outside this cut. |
| `rewriter.rs:1008–1019,1034–1045`, `simple_expr.rs:344–418` | DEFAULT/resolved-constant/expression hooks are broader than schema-only binding; finishing prepares native IN caches. D2-min binds only complete row columns via `resolve_column`, rejects virtual/deferred/correlated inputs, and does not prepare caches. SchemaNameResolver remains the existing name-resolution authority. |

### Smallest implementable first cut: D2-min

This is deliberately **not** a universal AST migration. Its accepted structural shapes are plain integer/string/NULL literal construction, ordinary signed-BIGINT row-column binding, parentheses, AND/OR, IF/IFNULL/COALESCE, searched CASE, and explicit `CAST(... AS SIGNED)`. Control predicates/results must already have real signed-LongLong types; mixed branches, implicit domain-changing casts and untyped NULL in control positions are rejected, not retagged. Plain string/NULL leaves and explicit signed casts may be prepared as structure for poison-fold tests, but are **not thereby admitted by D1**. The shared signed-cast constructor retains the unevaluated child. No ordinary arithmetic, comparison, unary minus, simple CASE, temporal literal, parameter/default/subquery/assignment/host function, charset-introducer conversion, IN, or unknown name is accepted by this first private API.

A bounded iterative AST shape preflight rejects unsupported constructs **before** invoking binding hooks, including constructs in a logically dead branch. A second check immediately after binding/type inference rejects unsuitable column/type/argument metadata **before** an implicit wrap/probe path. This admission list contains shapes and type predicates only, no inferred type formulas, conversion implementations, PB IDs or kernel map. Unsupported preparation returns a stable `EvalError::Unsupported` boundary reason; it is not SQL NULL, deferred speculative execution, or a request to retry the native evaluator. Existing SqlBuild callers retain their existing wider behavior.

Proposed signatures (none exist yet):

```rust
// rewriter/preparation.rs; crate-private, not a public SQL configuration knob.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum PreparationPurpose { SqlBuild, StructuralOnly }

#[derive(Clone, Copy)]
pub(crate) struct StructuralLimits {
    pub max_nodes: usize,
    pub max_depth: usize,
}

// Opaque and not an executable/precompiled program. No Deref or eval methods.
pub(crate) struct StructuralExpression { root: Expression }
impl StructuralExpression {
    // Final structural validation/metadata detachment; not an evaluator.
    // Earlier preflight/at-site guards are still required before building.
    pub(crate) fn checked(
        root: Expression,
        limits: StructuralLimits,
    ) -> Result<Self, EvalError>;
    pub(crate) fn as_expression(&self) -> &Expression;
}

// rewriter.rs: explicit new entrypoint, bounded preflight then shared core.
pub(crate) fn rewrite_expr_structural(
    expr: &tidb_ast::Expr,
    resolver: &impl ColumnResolver,
    limits: StructuralLimits,
) -> Result<StructuralExpression, EvalError>;

// Shared recursive entry used by the existing and new entrypoints.
fn rewrite_expr_resolved_with_purpose(
    expr: &tidb_ast::Expr,
    resolver: &impl ColumnResolver,
    purpose: PreparationPurpose,
) -> Result<Expression, EvalError>;

// new_function.rs: shared implementation, preserving the old wrapper exactly.
fn new_function_impl_with_purpose(
    ctx: &dyn Columns,
    purpose: PreparationPurpose,
    fold: ConstantFoldMode,
    func_name: &str,
    ret_type: FieldType,
    check_or_init: Option<ScalarFunctionCallBack<'_>>,
    args: Vec<Expression>,
) -> Result<Expression, EvalError>;

pub(crate) fn new_function_structural(
    ctx: &dyn Columns,
    func_name: &str,
    ret_type: FieldType,
    args: Vec<Expression>,
    limits: StructuralLimits,
) -> Result<StructuralExpression, EvalError>;
```

`rewrite_expr_resolved(expr,resolver)` and the entire existing public NewFunction family retain their signatures and call their shared cores with `SqlBuild`. The new typed constructor accepts only the same closed control call identities and recursively checked structural arguments, verifies arity with the existing registry, and passes **None** for `check_or_init`. Even a seemingly harmless callback is not an effect-free capability. The shared core must reject `Some(callback)` under StructuralOnly before invoking it. No new Fields on Columns/Session/EvalContext, no TLS mode, global flag, or public ColumnResolver method is required.

Pass `purpose` **explicitly** through recursive rewriting; a nested call must not accidentally go back through the public SqlBuild wrapper. In particular, add the same final `purpose: PreparationPurpose` parameter to existing private `rewrite_expr_resolved_inner`, `rewrite_leaf`, `rewrite_leaf_literal`, `rewrite_leaf_compound`, `rewrite_leaf_call`, `binary_expression`, `literal_text`, and `wrap_power_arguments`, and their recursive calls. The shared entry retains `stacker::maybe_grow`. Put the purpose check outside calls to fold/value-refinement hooks, not inside a fake warning sink. Fold counters and their Normal/Try/Disabled precedence remain exactly as today on the SqlBuild leg; the independent StructuralOnly veto always wins on the new leg, including explicit casts and CASE's direct cast fold.

Reuse `result_type::{int_literal_type,cast_target,validate_cast_type,builtin_return_type}`, `infer_type4_control_funcs`, the existing operator type helpers, collation derivation, literal constructors and cast node construction. Do **not** make a second type-inference table in `tikv/catalog.rs`. `wrap_binary_literals` already accepts a fold closure; the existing insertion logic can be reused with a closure that does nothing only after the new purpose/admission checks establish a structural path. Do not change `simple_expr::build_cast_function` into an evaluator. The accepted Int-only implicit branches do not reach the Decimal probe, and all other such branches are refused rather than approximately inferred.

For schema binding, call the existing `ColumnResolver::resolve_column` once per syntactic occurrence and keep the complete Column identity/type/collation, not `resolve` followed by reconstruction. Detach retained FieldTypes with the existing `FieldType::deep_copy_like_go` (including direct-constructor arguments at the checked finish); ordinary Clone is not proof that GoSharedSlice-backed metadata is detached. In StructuralOnly do not call `resolve_constant`, `resolve_default`, `resolve_expression`, `param_value`, `eval_constant`, `comparison_context`, or `rewrite_grouping`. The schema provider's `resolve_column` and declared configuration getters are metadata-only contracts; arbitrary malicious trait implementations are not sandboxed. Use an audited schema-backed resolver in real callers and poison the forbidden hooks in tests. `eval_in(&Expr,&dyn Columns)` does not provide this schema and cannot be routed by guessing types from `Columns::get`.

**Metadata completeness is explicit:** StructuralExpression preserves source literal/column types and the existing structural inference/derived collation. It deliberately lacks computed-value nullability, successful-fold replacement nodes, and precision refinements that require evaluation. It is not advertised as the fully folded SqlBuild metadata of a constant expression. No flag/precision is fabricated to disguise that distinction. D2's handoff smoke test uses nonconstant BIGINT row controls whose required D1 metadata is already structurally determined. General planner publication or wider lowering must request the missing SqlBuild facts in a separately approved stage, or reject; not silently use a conservative type as exact SQL metadata. No such fact executor is implemented in D2-min.

### Exact requested file cut; all larger effects remain separately gated

| Proposed writer/file | Exact change and limit |
|---|---|
| D `rewriter.rs` | Declare private preparation/test modules; add explicit structural wrapper and common purpose-aware recursive entry; thread the parameter through the helpers above. Guard all resolver/direct CASE folds, `prepare_numeric_arguments`, comparison refinement, and IN-cache finishing. Use schema-only binding on the StructuralOnly leg. Existing public entry stays SqlBuild. |
| D NEW `rewriter/preparation.rs` | PreparationPurpose, StructuralLimits, opaque StructuralExpression; bounded syntax/typed-argument admission checks and stable unsupported reasons. No evaluator, signature/type table or runtime services. |
| D `new_function.rs` | Factor the existing body once into `new_function_impl_with_purpose`; existing `new_function_impl` delegates with SqlBuild. New closed structural constructor validates before refinement/callback/fold. No change to existing callbacks or their ordering in SqlBuild. |
| D NEW `rewriter/preparation_tests.rs` | Tests below, declared from rewriter.rs. Cover AST and direct constructor through the same purpose contract. No fixtures/generator/Cargo edits. |
| No change: `rewriter/fold_mode.rs` | It continues to decorate fold policy only. Because purpose is an explicit separate parameter, neither FoldModeResolver nor `impl ColumnResolver for &T` can lose it. Add tests for nesting through both, rather than a public trait mode accessor. |
| No change: `simple_expr.rs`, `rewriter/{result_type,control_type}.rs`, `builtin_arithmetic.rs`, `builtin_op.rs`, `builtin_compare.rs`, `aggregation/wrap_cast.rs`, `constant_fold.rs` | Their effectful paths remain excluded from the new domain; their pure existing helpers are reused. Do not broaden scope by quietly returning “no refinement” where exact inference needs a value. |
| No change: all frozen D1 files, `lib.rs`, evaluator/Expression evaluation entrypoints, Cargo/locks, TiKV runtime | No D1 widening, public route, fallback, new export or dependency is needed. Parent retains shared roots and release authority. |

This four-file cut avoids reacquiring `scalar_function.rs` during B2.2/C2b. It is smaller than making every type constructor purpose-aware. If parent instead wants broader preparation admission in this release, the following are **additional exact serial locks**, not optional hidden edits:

- `scalar_function.rs::prepare_numeric_arguments(&mut self,&dyn Columns)` needs a purpose-aware Result-returning core: reuse its target-type construction and `refine_cast_arithmetic_metadata`, but return an explicit needs-value-facts boundary at the two `eval_numeric_operand_row` sites. The legacy wrapper runs SqlBuild unchanged; no new precision formula.
- `aggregation/wrap_cast.rs::wrap_with_cast_as_decimal(Expression) -> Result<Expression,EvalError>` needs a purpose-aware core separating `build_cast_function` from the strict-constant precision probe. Thread it to admitted callers in `rewriter.rs` and comparison wrapping. Do not merely delete precision refinement for SqlBuild.
- `builtin_compare.rs::{refine_comparison_dyn,refine_integer_comparison_for_rewrite,wrap_comparison_arguments_with_fold,wrap_integer_operand_as_decimal}` must separate structural signature/cast facts from value replacement, warning, and plan-cache mutation. Admission without the required value-derived facts must fail before conversion; a NoColumns substitute is forbidden.
- `builtin_op.rs::{infer_unary_op_type,int_negation_overflows}` and `constant_fold.rs::folded_value` need a shared inference core with an explicit immutable-literal versus needs-evaluation result. Inspecting a real strict Int literal is permitted; evaluating a retained scalar subtree is not. Existing SqlBuild inference stays on that same core.
- `rewriter/result_type.rs::{builtin_return_type,builtin_return_type_before_ret_tp,round_truncate_return_type}` and reached value-reading helpers need a Result-bearing purpose-aware core, with old wrappers preserving SqlBuild. The ROUND scale cast cannot be replaced with a copied int-conversion algorithm or a default scale. `Constant::literal_value()` is the provenance check, not a match on the stored `value` of a deferred/parameter Constant.
- Broader temporal/CHAR/default/parameter preparation needs dedicated typed facts before reaching the source sites above. Retain their exact SqlBuild error/warning ordering. This is an additional release, not an excuse to set StructuralOnly while still evaluating them.

### Proposed D2-min tests and static deletion/coverage hooks

These are names/obligations, **not implemented or passing tests**:

1. `structural_controls_never_call_value_hooks`: real parsed nested BIGINT IF/IFNULL/COALESCE/AND/OR/searched CASE, including constant subtrees. A resolver supplies real schema metadata and panics on fold/eval/parameter/default/resolved-constant/expression/comparison-context/grouping hooks. Count column lookups; inspect that control nodes and literal payloads remain, with no value-derived NOT_NULL flag injected.
2. `structural_explicit_signed_cast_ignores_forced_normal_fold`: preserve `CAST('bad' AS SIGNED)` as a cast over its original string literal, at root and under a control/fold-mode decorator; no conversion and no warning. D1 lowerer must still reject the cast. This is preparation-only, not cast migration.
3. `structural_rejection_precedes_all_value_effects`: dead branches containing decimal arithmetic, comparison with truncating text, nested unary-minus overflow, Decimal ROUND scale, invalid CHAR charset, typed temporal literal, DEFAULT, parameter, host/assignment, simple CASE and IN are rejected before any prohibited hook; direct constructor rejects mutable/deferred arguments and callbacks before invocation.
4. `structural_preparation_preserves_diagnostics_and_state`: seed `FOLD_WARNINGS` with an existing sentinel via `record_fold_warning`, check exact length/content after success and rejection using existing stash APIs, and restore the test's stash. Poison/record warning append/bookmark/truncate/drain, row-value access and stateful Columns services. Assert no SQL clock/RNG/uservar/sequence/lock activity. These checks supplement, not replace, source reachability review of hidden NoColumns paths.
5. `structural_purpose_survives_nested_resolver_scopes`: exercise `FoldModeResolver::{new,for_function}` and a borrowed `&dyn ColumnResolver`; all recursive calls retain StructuralOnly even when a nested cast requests Normal. Existing SqlBuild Normal/Try/Disabled behavior and callback ordering remain unchanged in paired constructor tests.
6. `structural_reuses_declared_metadata_without_normalizing`: compare complete column identities, detached FieldTypes, literal kinds/values and collation state, not just EvalType. Untyped NULL/Tiny/unsigned remain their source types or are refused; no LongLong retag. For nonconstant structurally determined controls, compare metadata with the existing SqlBuild path; do not assert equivalence of value-refined constant metadata that StructuralOnly never computed.
7. `structural_limits_and_rejected_syntax_do_not_evaluate`: count syntax/typed nodes and depth before descent, test boundary/excess shapes, unknown names and malformed arity. This bounds the new constructor; it does not claim to make all existing native AST/Expression cloning or Drop iterative.
8. `structural_bigint_controls_feed_frozen_d1`: prepare a real schema-bound, nonconstant control tree through the new API, explicitly lower/compile/evaluate through the unchanged D1 seed, and compare nullable/repeated-selection results. No hook in public `eval_in`/Expression/EvaluatorSuite is installed. A preparation-successful signed CAST still fails D1 admission.

Static acceptance must enumerate every production call in `rewriter.rs` to `rewrite_expr_resolved` (only public SqlBuild ingress may restart that mode); every `fold_constant`, `fold_constant_in_mode`, `eval_constant`, `prepare_numeric_arguments`, `refine_*`, `prepare_in_string_hash_sets`, `param_value` and `eval_expression_once` reachability edge; and the shared NewFunction callback/fold sites. Inspect guards and the closed admission predicate, not a grep-zero claim over a file that intentionally retains SqlBuild. Check the new preparation module for absence of native evaluation/cast kernels, remote serialization, fabricated PB, fallback, host closures, unsafe/Sync widening and duplicated type formulas. Parent runs targeted tests and baseline comparison after a separate implementation release; D does not run builds now.

### Simple CASE: preserve a selector-once node before expression expansion

Current AST `lib.rs::eval_in`, lines1219–1277, computes the selector once at1244, then each WHEN value in order and ordinary equality at1248; NULL does not equal NULL. Current typed rewrite at `rewriter.rs:1730–1746` copies that selector into every equality. Passing this expanded tree to C loses selector-once identity; trying to deduplicate equal/hash-equivalent expressions afterwards is wrong for stateful occurrences.

D2-min therefore rejects **all** simple CASE syntax before selector/WHEN rewriting. A later representation must retain one selector child, ordered `(when,result)` arms, optional ELSE, source identity and result/comparison types/collations. C then needs an explicit occurrence-local `BindOnce`/bound-value read (or a dedicated SimpleCase node with that exact behavior) in its own compiler/driver. It must not be a hidden native closure or InputSlot that re-evaluates the selector. Bound state is private to one active occurrence/invocation; duplicate selected physical rows are distinct occurrences. Ordinary typed equality still performs the approved comparison/coercion for each reached WHEN; sharing the selector is not sharing those comparisons or merging caches across rows.

Tests for that later C/D cut must count selector=1 per occurrence, WHEN evaluation until first match only, selected result only, no ELSE after a match, NULL selector with NULL WHEN taking no match, selector failure preventing every WHEN, repeated selections without cross-occurrence reuse, and cancellation/limits releasing the bound value. Under the currently observed AST profile, a NULL selector does **not** suppress evaluation of WHEN expressions; do not invent that optimization. Until this representation and comparison profile exist, simple CASE is not enabled by ordinary searched-CASE support.

### Ordinary-call demand metadata for C's later profile gate

The same name/type is not one demand contract. Verified source witnesses:

- AST value scalar: `lib.rs:903–920` evaluates both binary children left-to-right before the arithmetic call; NULL left does not itself stop right evaluation.
- Typed Int arithmetic row: `scalar_function.rs::eval_fast_integer_binary:1491–1504` evaluates the left typed operand, returns NULL at1497 before the right operand, then invokes the ordinary kernel. Comparison arms in this same helper do **not** take that arithmetic NULL-stop branch.
- Native numeric batch: `scalar_function.rs::eval_arithmetic_batch:4149–4155` completes the left operand batch, then the right operand batch, then arithmetic. NULL left rows do not suppress right-subtree errors. `evaluator.rs::numeric_batch_does_not_suppress_nested_errors_on_null_rows:825–861` explicitly requires error for vectorized execution and NULL success for scalar execution on `NULL + (MAX_BIGINT * 2)`.
- PB row evaluation has its own signature/domain-specific demand (`scalar_function/pb_builtin.rs:276` even distinguishes MOD/non-Decimal from other NULL-left cases). A TiPb signature number alone cannot select the AST/native-batch profile; preserve PB origin and inspect the exact arm before admission.

Proposed handoff facts, not currently added to C2's public API: an explicit **consumer profile** (`AstValueScalar`, `TypedRow`, `PbRow`, `NativeNumericBatch`), call source/provenance (including exact PB signature when present), and the **approved per-call demand rule**. That rule must identify left-to-right argument order, which typed/coerced argument NULL stops later arguments, whether all arguments are demanded despite NULL, conversion-before-next-child versus conversion-after-children, and first-error stopping. Batch additionally needs the operand-major versus occurrence-major schedule and the active selection/occurrence universe. “Strict function”, return nullability, deterministic/pure flags, or the choice of `eval_one` versus `eval_selected` cannot infer these facts.

These are execution semantics separate from PreparationPurpose: SqlBuild/StructuralOnly controls construction effects, not which consumer's runtime NULL/error behavior is selected. The future profile belongs in immutable compile metadata/cache identity and must be validated before effects. Unsupported profile/signature/domain/schedule is an admission failure, not automatic eager evaluation, force-scalar execution, or native replay. C's ordinary-call driver must implement the approved demand before evaluating children; returning NULL from an eager kernel is too late. Until then, exclude ordinary arithmetic even if all carriers are Int, and never switch from scalar demand to batch demand merely to obtain a fast path.

Required later tests: the existing scalar-vs-batch witness unchanged; poisoned right binding behind NULL left under both profiles; left conversion that itself returns NULL; right error/warning and first-error order; comparison versus arithmetic under the same types; PB exact-signature provenance; two rows showing argument-major versus row-major diagnostic order; empty and repeated/nonidentity selections. The pure D1 control seed proves none of those broader profile gates and remains unchanged while C2b is implemented.

### D-r3 checks and remaining approval boundary

Only `expression-unification/evidence/lowering-contract.md` was edited for this D1-acceptance/D2-refinement turn. D reread the three parent logs cited above and the exact source call sites; no product file, fixture, manifest, lock or plan was changed. No cargo/build/test/lint/commit/fixture generator or background job was started. The proposed four-file D2-min cut and eight tests await parent review and a separate explicit implementation release.

Validation command from `/home/agent/tidb`: `pwd; git status --short -- expression-unification/evidence/lowering-contract.md; git diff --check -- expression-unification/evidence/lowering-contract.md` failed (exit129): this workspace/evidence root is **not** a Git repository. This was a path-assumption error, not a sandbox denial. The corrected non-repository check was `git diff --no-index --check -- /dev/null /home/agent/tidb/expression-unification/evidence/lowering-contract.md`; it emitted no whitespace diagnostics (exit1 is the no-index added-file difference). A separate content search for `[\t ]+$|^(<<<<<<<|=======|>>>>>>>)` returned no matches. This is document/source validation only, not runtime proof of D2.

## Scope and evidence boundary — original D-r1 audit

At the D-r1 read-only checkpoint, this report was the sole file written by D. Product trees, other evidence, frozen inventory, manifests, locks, fixtures, and the main plan had not been modified by D. No builds, tests, commits, fixture generation, or worktrees were run/created. One permitted read-only child audited session ownership and returned its complete report; it wrote nothing and ran no builds/tests. That result was collected and incorporated here. No old `expression-reuse` implementation was used.

Verified HEADs: TiDB `364aef2bab5cc633ecb76a775ae8f36f86a6687d`; TiKV `548812e1ef57aef077a2062a9cc356640a6347f5`. Both fresh worktrees have concurrent parent/other-owner changes; a dirty status is not a D change. Source locations below refer to the inspected TiDB tree and C-r1/C2a-in-flight TiKV tree; symbols, rather than offsets, are the stable anchors.

Read the TiDB root AGENTS/PLANS and architecture index; no deeper AGENTS was found under `rust`. Read the TiKV root rules and maintenance-guide context. Also read `runtime-contract.md` including C-r1 receipt and C2-proposal-r1 at 528+, `datatype-contract.md`, `coverage-baseline-notes.md`, and relevant frozen JSON entrypoint/deletion/PB inventory. Older proposal language in those artifacts is not evidence of a subsequently implemented API.

The parent reports codegen20/expr438 for C-r1, the first admitted collation/LIKE removal gate, and 61 collation-consumer passes. D did not rerun or independently reproduce those runtime results. The value bridge now **exists** at `tidb-datatype/src/tikv_compat/value.rs`; it admits checked Int/Real/Bytes/NULL transport with detached complete SQL metadata, not Decimal/temporal/JSON. The approved SmallVec/wide Decimal architecture is still a separate safety/implementation gate.

**Frozen counting remains 245 families, target at least 221 complete, zero complete evaluator migrations established.** A signed-only control or arithmetic seed earns no complete family. Neither shared collation nor a value adapter establishes general expression migration.

Abbreviations: `DB/` = `/home/agent/tidb/expression-unification/tidb/`; `KV/` = its sibling `tikv/`; `E/` = `DB/rust/crates/tidb-expr/src/`; `X/` = `DB/rust/crates/tidb-executor/src/`; `S/` = `DB/rust/crates/tidb-session/src/`; `K/` = `KV/components/tidb_query_expr/src/`.

## Findings that determine the next release

1. **The proposed four-file boundary is supported, but is not already present.** `tikv/{catalog,lower,context,batch}.rs` can respectively select SQL signature facts, construct `LocalExpr`, adapt native owners, and schedule/materialize the existing RPN runtime. None may evaluate an expression tree or dispatch an ID to a native kernel. A small module root/test file is wiring, not another runtime.
2. **SQL typed nodes and PB-selected nodes need different identity inputs.** `ScalarFunction::pb_signature()` exists and wins over the display name. SQL nodes normally retain a name/result type, not an already selected universal PB code. Reuse SQL binding/inference and factor pure signature selection where necessary. The remote serializer is not that factoring boundary.
3. **Today's typed tree loses some original PB metadata.** `pb_type_to_field_type` drops `array` and protocol-ID sign/presence; NULL/JSON literal arms replace the supplied FieldType. `pb_column` can use scan-schema type instead of the reference's wire type. Retain original wire metadata at real PB ingestion, separately from the effective SQL type. It cannot be recovered faithfully by later serialization.
4. **A naïve `eval_in -> rewrite_expr -> Expression::eval` replacement is recursive and effectful.** Rewriting invokes folding, numeric argument preparation, comparison refinement, and some value-dependent inference. `ConstantFoldMode::Disabled` does not disable all these paths. Direct AST `Columns::get` also has no schema/type accessor. A non-evaluating structural preparation mode is necessary, not guessed type inference from a retrieved Datum.
5. **C2a is necessary but insufficient for broad strict SQL.** It covers Int controls and demanded input on the same official driver. Ordinary eager arithmetic still differs from scalar TiDB's NULL-stop behavior. Existing vector tests deliberately exercise another demand behavior. A profile/admission decision is required; do not silently make all paths row-major scalar semantics.
6. **The actual live owner is `StmtContext`, not `Session`.** A worker-owned TiKV `EvalContext` and one leaf/primitive service adapter can borrow disjoint data without any `&mut Session`. There is no existing `ExpressionContextGuard` to rely on.
7. **Diagnostics are not a final-list merge problem.** Native notes, warning caps, TryFold bookmarks, publication, and original error variants are not supplied by C2. In particular `enter_cop_eval` deduplicates/reorders native warnings and must not be repurposed for local RPN.
8. **First D release should be an explicit executable control-only seed.** Feed real typed SQL/PB trees to a private checked constructor, compile and execute through C2a, test demanded reads and worker isolation. Do not automatically hook the general evaluator until the admitted tree domain and its diagnostics close. This avoids both native replay and silent rejection of previously supported SQL.

## 1. Actual-source boundary map

### Construction and scalar evaluation

| Existing source/API | Actual responsibility; required integration action |
|---|---|
| `E/lib.rs:460,795–1279`: `eval(&Expr) -> Result<Datum,EvalError>`, `eval_in(&Expr,&dyn Columns) -> Result<Datum,EvalError>` | Direct AST interpreter. Column names use `Columns::get`; binary children are eagerly evaluated at 902–920; functions enter `eval_func`. Retain the public compatibility surface, but ultimately replace computation with structural binding, typed immutable spec, and prepared local RPN. Do not infer missing column SQL metadata from a value. |
| `E/rewriter.rs:57–220,982–1035`: `ColumnResolver`, `rewrite_expr_resolved(&Expr,&impl ColumnResolver) -> Result<Expression,EvalError>` | Existing binding/inference authority. `resolve_column` preserves IDs, source names, virtual-expression metadata and collation; `resolve_expression` handles correlated values. `resolve_default`, parameter binding and the caller's clause name also belong here. Reuse these facts rather than a second SQL resolver. |
| `E/new_function.rs:285–407` | Arity, arithmetic inference, comparison refinement, collation, wrapping, numeric preparation and fold policy. `NewFunctionBase` disables outer folding, **not** implicit-cast folding. Date arithmetic evaluates its unit at 297. This is not a value-free local compiler entry. |
| `E/expression.rs:719–748` | `Expression::eval(&self,&dyn Columns,Row<'_>) -> Result<Datum,EvalError>` dispatches Column/Constant/Correlated/ScalarFunction and applies ENUM_SET_AS_INT. `eval_with_error_value` preserves Constant's Datum/error pair for rewriting. Replace the computation seam, retain these host result contracts. |
| `E/scalar_function.rs:276–317,659–718,1182–1202` | Fields include `func_name`, full `ret_type`, args, private VALUES offset, PB builtin, grouping metadata, collation and native caches. `eval` is PB-first, then integer fast path, then `eval_by_signature`, followed by return adaptation. Replacing only `eval_by_signature` leaves two native evaluators live. |
| `E/constant.rs:122–169,286–339` | `literal_value()` distinguishes strict literals from parameter/deferred snapshots. `get_type(ctx)` reads a current parameter; `eval_in` and `eval_on_row_with_error_value` can evaluate a deferred expression. Neither is a general input-service accessor. |
| `E/column.rs:229–244,375–421,467–515` | Column reads `row.get_datum(index,ret_type)` after bounds checks. CorrelatedColumn owns an optional `Arc<RwLock<Datum>>`; `bind` updates the live cell, `eval` clones its current value. `eval_typed` additionally converts hybrids. Preserve binding identity, not its compile-time value. |
| `E/build.rs:54–116` | Existing `BuildContext` only selects LENGTH/CHAR_LENGTH using explicit type facts. It is **not** a complete session/compiler context and must not be advertised as one. |

Important preparation hazards, not speculative API gaps:

- `rewriter::binary_expression:579–748` calls `prepare_numeric_arguments` unconditionally. That method (`scalar_function.rs:1251+`) evaluates strict literal casts and changes metadata.
- `rewrite_expr_resolved_inner:1026–1033` forces ordinary CAST to Normal folding, regardless of the resolver's Disabled mode. CASE result casts can fold directly through `comparison_context` at 1790–1795.
- `rewriter/fold_mode.rs:132–145` forwards `fold_constant`/`eval_constant` to its base; the mode wrapper is not a global prohibition on value execution.
- `constant_fold::folded_value:381–390` executes a typed tree for value-dependent inference, e.g. unary-minus overflow inference. Preserve legitimate build-time evaluation through the shared runtime, rather than duplicating the operation in the lowerer.
- Simple CASE is lowered by cloning its selector into each equality (`rewriter.rs:1724–1746`), whereas AST CASE can evaluate its selector once. It is outside the first seed. Preserving once-only selector semantics needs an explicit compiler/runtime binding or a separately approved correction; no hidden input closure evaluating that selector.

### PB identity and remote catalog

Existing `distsql_builtin::pb_to_expr(&tidb_proto::tipb::Expr,&[FieldType]) -> Result<Expression,String>` recursively decodes a real wire tree (`E/distsql_builtin.rs:154–176`). `ScalarFunction::from_pb` retains `PbBuiltin`; `pb_signature() -> Option<tidb_proto::tipb::ScalarFuncSig>` survives clone and display-name changes (`scalar_function.rs:670–685`). `scalar_function/pb_builtin.rs` has a native `Kernel` selector independent of names. It must be deleted as computation after the relevant domains migrate, not consulted as a fallback from local lowering.

`pushdown_catalog::{resolve,from_expression,from_expression_in,to_pb}` is not a local lowering API:

- `from_expression_with_context:2390+` calls `constant.eval_in(context)` for bound values and reads correlated values. This can freeze runtime values or execute a deferred expression.
- `resolve_conditional:2269+` uses a branch's type as a remote projection approximation; local lowering must use the actual typed result and already required argument casts.
- `field_type_to_pb:2518+` narrows flags and stringifies elements. It is not the checked lossless datatype bridge.
- Remote admission/blacklists, implicit casts, serialized literals and scan descriptors are independent concerns. Do not widen remote admission merely because local execution is available.

**Proposed minimal PB retention cut:** define a small TiDB-owned `PbOrigin` in `distsql_builtin.rs`, containing original optional `Expr.tp`, raw optional signature number, optional complete prost `FieldType` (including presence and signed collation), and actual scalar-call `val` metadata bytes. Store an optional immutable origin on Constant, Column and ScalarFunction; CorrelatedColumn already embeds Column. Add read-only accessors. Attach it at ingestion **before** conversion/defaulting; preserve it through clones. Original wire FieldType, effective scan/SQL FieldType, and current executable selected signature are distinct records. A transformed/folded node must not execute using an old diagnostic-origin signature. This cut needs `distsql_builtin.rs`, `constant.rs`, `column.rs`, `scalar_function.rs`; it does not require changing generated protobufs.

The prost schema is real: `tidb-proto/proto/select.proto:75–84` has optional tp/flag/flen/decimal/collate/charset, elements and array. `Expr` at 818–825 has real signature and `val`. Preserve those data, not a fabricated `tipb::Expr`. Raw unknown/missing signatures must be rejected deliberately; calling an enum default accessor must not hide the original number.

### Batch, filters and helpers

| Existing boundary | Handoff obligation |
|---|---|
| `E/evaluator.rs:305–378`: `EvaluatorProgram::new(Vec<Expression>,bool)`, `EvaluatorSuite::from_program(Arc<EvaluatorProgram>)` | Existing shared program holds typed expressions/output mapping; suite owns execution-local ColumnSwapHelper. The natural future split is immutable lowered specs in the program and independent compiled RPN instances in each suite. Do not put `LocalProgram` inside the shared Arc. |
| `EvaluatorSuite::run<C:Columns>(&self,&C,&mut Chunk,&mut Chunk)` at 392–459 | Calculated outputs finish before direct input-column owners move. It has expression-major pure scheduling and row/select-list scheduling for effects. Calls decimal vector arithmetic, numeric batch and Constant broadcast before row eval. A suite owning mutable prepared RPN needs a reviewed `&mut self` cut and mutable call sites; no interior global shared compiled cache. |
| `vectorized_filter_consider_null`, `vec_eval_bool`, `filter_physical_rows` at 89–245 | Preserve filter-major survivor selection, EQ-from-IN NULL mask, physical universe and constant broadcasts. The filter API evaluates **physical rows first**, then intersects input selection at 248–257; blindly passing the original selection to local RPN changes existing effects/errors. Projection and filter adapters do not have identical row contracts. |
| `scalar_function::{vec_eval_bool,try_eval_numeric_batch,vec_eval_decimal_arithmetic}` at 3359,4187,4437 | Live native computation, not optional wrappers. Must route through the same prepared runtime with an explicit consumer demand profile or be deleted when those domains close. |
| `X/selection.rs:62–73,110–146,157+` | `FastSelectionFilter::{NullTest,StringIn,And}` is a separate nonbatched evaluator. Batched selection uses the filter API. Retain mask/output scheduling and memory ownership; delete/delegate this shortcut's computation. |
| `X/predicate_pushdown.rs:610–719` | `FastScanFilter::{StringIn,Like}` independently computes membership/LIKE. Shared collation is not deletion of this evaluator. Retain scan admission, negation and storage ownership. |
| `X/vec_group_checker.rs:104–190` | First/last keys run before interior keys; can skip the interior pass. Calls `try_eval_numeric_batch` directly. Retain grouping/order, route expression calculations. |
| `E/lib.rs:668–781` | `eval_expression_once`, `apply_binary[_with_div_precision]`, `apply_unary`, `concat_values`, `date_add_interval`, `avg_of[_with_div_precision]`, `fit_decimal_column`, plus `get_time_value` at 512 are public computation paths. Value-only helpers should use explicit typed shared calls, not fake literal AST/PB nodes or native arithmetic tails. Their historical context-free defaults remain explicit API obligations. |

The frozen inventory has additional consumer sites, not another denominator. Projection/Expand, join residuals, sort/group keys, partition/range expressions and old `tidb-exec` consumers must all be checked. For example `tidb-exec/src/multi_statement_transaction.rs:1183` directly calls `apply_binary`; changing only modern projection does not reach it.

### Folding, writes, aggregate and window arguments

- `constant_fold::fold_constant_in_mode(&mut Expression,&dyn Columns,ConstantFoldMode)` at 54–117 bookmarks warnings, attempts evaluation through `eval_expression_once`, rejects warning-producing Try folds, and rolls back an unsuccessful fold's warnings. `fold_value` and `folded_value` also execute trees. Keep the policy; move computation to the same prepared path. Do not add `lower -> fold -> eval -> lower` recursion.
- `X/column_default.rs::build_in_context:715–733` rewrites a DDL default then evaluates it; `evaluate:832–869` evaluates a computed default then performs assignment conversion. Keep default clock/source-zone/version semantics and the assignment boundary, rather than merging it with expression transport.
- `X/generated_column.rs::eval_over_dependencies:510–526` resolves current column names/types; `eval_over_row:536–552` materializes a row then calls typed eval. Do not cache dependency offsets across ALTER. Storage conversion at 467–484 remains a separate shared-datatype obligation.
- `X/driver/dml/correlated.rs::DmlExpression::{build,eval}` prepares subquery/apply outputs, then evaluates one typed scalar. `UpdateExpression::{Scalar,Physical,Applied}` selects the correct input row. DML outer subquery execution is not a HostCall reading an arbitrary SQL expression. Preserve original-row assignment semantics (`driver/dml.rs:3593–3623`).
- `X/hash_agg.rs::eval_agg_input:3567–3679` reads scalar args/extra args with function-specific order; COUNT and GROUP_CONCAT stop at NULL, AVG/JSON_OBJECTAGG have other ordering. `hash_agg/input.rs:568–590` includes fast cell updates and order-by expressions. Keep aggregate state machines; route arguments and common arithmetic. i128 Decimal accumulation is not removed just by routing scalar args.
- `X/window.rs:259–277,344–389,571–605` evaluates keys, frame-bound calculations, selected value rows and LEAD/LAG defaults on demand. In RANGE, start/end operand order differs and an exhausted cursor must not execute an unused bound. Do not precompute all defaults/arguments. Window/aggregate state itself is not an evaluator-family completion claim.

### Unistore

`RequestEvalContext` (`tidb-unistore/src/cophandler/eval_context.rs:23–103`) owns timezone, precision, flags, scan types and a warning mutex. `SharedExpression` owns the typed expression plus an Arc to that context. `convert_shared` and `eval_shared` (`cophandler.rs:2037–2057`) still use native `Expression::eval`, with Debug-string errors. `convert_expr_with_context:2073+` chooses Shared solely by top-level typed-signature support; otherwise it constructs `SimpleExpr::Func(SimpleSig,...)`.

The frozen sets matter for deletion: P=129 typed IDs, S=218 explicit native-converter IDs, intersection102, S−P=116, P∪S=245 **single-node IDs**, not tree closure or family coverage. A Shared-supported parent currently rejects a SimpleSig-only child. Native helpers can swallow Shared errors into `None`. `eval_datum:1248–1267` rejects Func aggregate computations but accepts Shared; TopN/aggregation/grouping retain their separate restrictions. Do not claim any newly representable tree was already supported.

Final route: actual PB ingestion -> TiDB typed immutable spec with exact wire sidecars -> the same TiKV local compiler -> request-owned input/diagnostics -> host-shaped result. Replace both Shared and residual Func computation; retire `SimpleSig` mapping/evaluator bodies, preserving source tests by moving them to the new boundary. Do not rewrite SimpleSig to SQL names or register its evaluator as a HostCall. Residual date/comparison/int/cast/IN/LIKE domains stay explicitly unclosed until their complete signatures and metadata work. No SimpleSig file lock is needed for the first control-only seed.

## 2. Concrete minimal TiDB boundary — proposed, not existing

These are implementation shapes, not a new generic backend trait or second execution graph. Only `LocalExpr` describes executable work; TiDB arrays below retain binding/diagnostic metadata, not another evaluator.

### `tikv/catalog.rs`: select identity, never a kernel

Input is the already bound, inferred ScalarFunction and immutable child facts. Output is `FunctionRef` plus typed `CallMetadata` and result/input obligations.

- If `pb_signature()` is Some, use that **exact official numeric identity**, checked into TiKV's corresponding generated enum. Do not remap `PlusInt` (203) to `PlusIntSignedSigned` (222), MOD signedness variants to a generic operator, or Upper/CharLength binary signatures from the display name. A checked numeric-enum bridge uses the generated enum API; it is not a hand-maintained ID-to-kernel table. Parent may supply a direct protobuf dependency or C may expose this tiny checked conversion helper if the trait is not available to the caller.
- For SQL nodes, factor the existing pure name/arity/type/provenance selection facts out of `pushdown_catalog` and/or the existing builtin builders. Both remote description and local catalog use that one facts function; remote admission remains separate. For first control seed, factor conditional identity selection using the **actual merged result type**, and use the already existing AND/OR official IDs. COALESCE's SQL builder can record CoalesceInt without inventing a private PB number.
- Do not call `from_expression[_in]`, `to_pb`, `Expression::eval`, `Constant::eval_in`, a native numeric kernel or a remote serializer here. Do not copy TiKV's selector match. The official TiKV selector remains the sole ID-to-prepared-kernel dispatch.
- Missing PB identities may use only C's explicitly registered closed `LocalFunctionId`. D cannot allocate IDs. The existing signed NULLIF ID is not evidence that all NULLIF domains are migrated. HostSlot is a checked catalog index with an explicit primitive descriptor, not another private protocol signature number.

The current remote catalog's SQL `plus` selects generic PlusInt. C-r1 admits only signed-specific PlusIntSignedSigned. This is an actual mismatch, not a reason for a hidden ID substitution. Arithmetic is excluded from the recommended first D seed.

### `tikv/lower.rs`: immutable spec and complete metadata

Proposed private entry and owned outputs:

```rust
// Proposed D API. Names/types here are a concrete implementation cut,
// not claims that these symbols compile in the current tree.
fn lower_int_control_seed(
    root: &Expression,
    row_schema: &[FieldType],
    collation_mode: bool,
) -> Result<Arc<LoweredSpec>, LowerError>;

struct LoweredSpec {
    expr: tidb_query_expr::local::LocalExpr,
    input_types: Box<[tipb::FieldType]>, // compact logical bindings, not all row columns
    inputs: Box<[InputBindingSpec]>,
    nodes: Box<[NodeMetadata]>,        // source/provenance only
    output: OutputMetadata,
}

struct NodeMetadata {
    sql_type: FieldType,               // snapshot_field_type; never shallow clone
    wire_origin: Option<Arc<PbOrigin>>,
    collation: CollationSnapshot,
    origin: OperandOrigin,
    source: SourceIdentity,
}
```

Implementation rules:

1. Walk iteratively with node/depth limits before cloning large data. Reject unresolved/missing type facts explicitly; do not default to an Int signature or mutate SQL types to meet C's admission. SQL Null FieldType and Tiny constants are not signed LongLong just because their runtime value is NULL/1. An upstream legitimate cast/type-inference result may be used; a fabricated type may not.
2. Use B's actual `snapshot_field_type`, `project_field_type(sql,signed_wire_collation)`, `to_scalar` and `from_scalar`. Preserve `raw_flags:u64`, code/array element code, flen/decimal `i64`, exact charset/collation spellings, arbitrary element bytes and binary-literal markers in the detached SQL snapshot. Projection failure is not SQL truncation. Retain the original PB FieldType independently, even where scan schema decides execution type.
3. Capture collation coercibility **and initialized bit**, repertoire, charset/collation and explicit-charset bit using `CollationInfo` accessors (`expr_collation.rs:143–192`). Capture legacy/new-collation mode and signed protocol ID. `collation_to_proto` at datatype `collation.rs:462` consults process mode; it is not a safe replacement for an original signed PB ID. Preserve +46 versus −46. A known checked name/mode projection is needed for SQL nodes; unknown/default fallback is not proof of equivalence.
4. Preserve literal-versus-input provenance independently of SQL type and carrier: Typed, ordinary text/bytes, BinaryLiteral, and source origin when available. `_binary '12'` and `0x3132` can share bytes/charset but select different numeric conversion. `Datum::BinaryLiteral` transports as Bytes today; a BIT column is not that literal and is not bridge-admitted. Parameter/correlated values remain dynamic even if the saved Datum looks literal.
5. Preserve source identities required outside kernels: column index/unique ID/original name/hidden-virtual provenance, `in_operand`, correlated identity, parameter order, `subquery_ref_id`, VALUES offset, grouping metadata, real cast InUnion metadata and temporal unit facts where applicable. Private VALUES offset needs a narrow accessor; do not parse it from display strings.
6. `Constant::literal_value()` may supply an exact representable Int/NULL for LocalExpr::Constant. Dynamic constants become declared input bindings or a structurally lowered deferred subtree. A deferred subtree is not a callback. Preserve Constant's result conversion and Datum/error-pair contract when that domain is admitted.
7. Build a compact binding schema over referenced inputs. Unused row columns of an unadmitted type must not be eagerly converted merely because C-r1 checks every declared slot. Retain source-row indexes in bindings, and check their effective complete SQL types against the row schema before evaluation.
8. Output adaptation is explicit: kernel carrier, desired Datum kind, string collation and declared shape/provenance are not interchangeable. The first seed's computed outputs are signed Int or NULL; no call to native `coerce_to_ret_type` is needed. Later keep Float32/result-width, unsigned return reinterpretation, hybrid/ENUM_SET_AS_INT, COALESCE FSP and deferred Decimal shape rules. Do not use `Datum::convert_to` as a blanket post-kernel escape.

There is a real future output-lineage gap: C2 returns only VectorValue. TiDB `same_eval_family` preserves String/Bytes/BinaryLiteral distinctions and some selected-argument metadata (`scalar_function.rs:127–178,1019–1100`). A mixed Bytes control cannot always reconstruct the selected Datum kind from return FieldType. Before admitting that domain, either establish and test an approved canonical host-result rule, or add selected-origin metadata to the same runtime. D must not evaluate branch conditions a second time to recover lineage.

### `tikv/context.rs`: actual native owners and a borrow-safe adapter

Existing `Columns` (`E/context.rs:518–1015`) uses `&self`, including warnings and effects. Live `StmtContext` is `Arc<StmtContextData>` (`X/stmt_context.rs:288–317`); configuration detaches with Arc::make_mut while effect fields retain shared owners. Its Columns implementation begins at 4041. `get()` is always None; row values come separately from Chunk/Row. `param_value()` clones from this execution's `Arc<[Datum]>` at 4085–4094. The distinct `get_param_value()` GETPARAM seam is not that prepared-marker accessor.

The separate `E/sessionexpr.rs::EvalContext` is not this live Columns owner. `Session` supplies native handles/snapshots when constructing StmtContext (`S/stmt_ctx.rs::statement_context_ignoring:1018–1469`); it is not lent mutably to the evaluator. `PendingQuery` owns QueryRecordSet plus StmtContext, and StatementRecordSet has no borrow into Session (`S/record_set.rs:85–150`). These existing owners permit this concrete split:

```rust
// Proposed worker/execution ownership, not fields added inside Session.
struct LocalExecution {
    kv_ctx: tidb_query_datatype::expr::EvalContext,
    // Later: ordered diagnostic/publication state and native primitive state.
}
struct Prepared {
    spec: Arc<LoweredSpec>,
    program: tidb_query_expr::local::LocalProgram,
    state: tidb_query_expr::local::LocalEvalState,
}
struct NativeInputs<'a> {
    chunk: &'a Chunk,
    bindings: &'a [InputBindingSpec],
    schema: &'a [tipb::FieldType],
    columns: &'a dyn Columns,
    native_failure: Option<EvalError>, // invocation-local, never shared or TLS
}
// Separate owners/fields: &mut prepared.program, &mut prepared.state,
// &mut execution.kv_ctx, &mut inputs. Never &mut Session plus &mut Session.ctx.
```

`NativeInputs` implements the **published C2a input service**, after its compile/test checkpoint. The proposal in runtime-contract uses `read_input(&mut self,&mut EvalContext,slot,HostRow,expected) -> LocalResult<VectorValue>`. Exact C2a names must come from C's released source; at inspection `local/spec.rs` had newly added BindingContract, but `local/batch.rs` still exposed only decoded eval. D has no basis to claim the binding ABI is already stable or passed.

A closed `InputBindingSpec` can identify:

- **Row column:** validated original index + full SQL type. Use `chunk.physical_row(input_row).get_datum(index,sql_type)`. `Chunk::get_row` reapplies selection; using it on C2's already physical row index would double-select. Preflight every relevant column's length, not only `Chunk::physical_rows()`'s first-column count.
- **Prepared marker:** `Columns::param_value(order)` on demand. Bind/re-admit its declared type per execute; do not read the old Constant snapshot or invoke `Constant::get_type` inside lowering to force a dead value read.
- **Correlated input:** execution-owned binding-cell handle; clone under a short read lock, drop the guard before return. Share the immutable binding identity in specs, not the live cell itself as an immutable constant. Rebinding and repeated occurrences reread the value.
- **Current insert value:** explicit offset through `current_insert_value`, once that host-specific leaf is admitted; absence differs from an out-of-range active row. It must not be captured at prepare time.
- **Strict literal:** normally LocalExpr::Constant in the exact Int seed. Fallible imports of future literals must remain demand-timed; pretending a delayed literal is an ordinary dynamic slot loses constant/provenance facts needed by cast/regexp preparation.

`read_input` retrieves and performs the approved value representation bridge only. It must not call `Expression::eval`, ScalarFunction::eval, `eval_in`, Constant::eval_in, a cached-plan deferred-evaluator closure, or a native SQL builtin. SQL casts remain explicit prepared calls. Shape/type failure is BindingContract, not SQL NULL. Empty selection produces no reads; repeated selected rows are distinct occurrences. Release every row/lock/RefCell borrow on return.

### C2b HostCall handoff, not initial D scope

One service owner should contain both inputs and a **registered primitive-state view**; no two overlapping mutable session providers. Catalog entries describe exact arg/result types and an explicit primitive variant. `start/resume` receive values or return staged NeedArg requests; they never receive expressions, evaluator handles or arbitrary child-evaluation closures. Use C2b's task/generation identity and Fresh/Reuse semantics, including innermost-first cleanup and no diagnostics during cancel.

Concrete decomposition opportunities exist, but are not already C2b implementations:

- `func.rs:309–334` has value-only `benchmark_loop_count(Datum,Option<&FieldType>,&dyn Columns)` and `ensure_benchmark_eval_type`. Existing scalar BENCHMARK at `scalar_function.rs:1694–1710` recursively calls `expression.eval`; that loop is forbidden as a host primitive. A staged task asks for count with Reuse, then body Fresh exactly N times on the same official driver; NULL/negative/zero never demand the body.
- `builtin_ext/crypto.rs:223–277::eval_aes_lazy` takes a child-evaluation closure and cannot be registered wholesale. ECB warns about an ignored IV without evaluating it; other modes demand IV. Its value-only `eval_aes_bytes` at 279+ is a possible terminal primitive after separately factored coercion/demand stages, not an excuse to leave the native AES family as dynamic fallback.
- User-variable/sequence/clock/lock/session operations use narrow existing Columns methods/handles. Register each admitted primitive and its effect order. Do not expose all of Columns as an unrestricted name-dispatch host.

For a primitive that needs native conversion/error policy, a **callback-local** Columns proxy may borrow the provided `&mut kv_ctx` for warnings and the disjoint primitive/config view for host operations. It is dropped before returning NeedArg/Ready. It must not forward diagnostics to StmtContext's old warning list while kernels write elsewhere. RefCell use to implement Columns' `&self` warning API is lexical, not a TLS/global guard.

Actual TLS hazards: native `FOLD_WARNINGS` (`constant_fold.rs:794–847`) and TiKV generated vararg buffers `VARG_PARAM_BUF`, `VARG_PARAM_BUF_BYTES_REF`, `VARG_PARAM_BUF_JSON_REF`, `VARG_PARAM_BUF_VECTOR_FLOAT32_REF`, `RAW_VARG_PARAM_BUF`. Generated kernels hold mutable TLS borrows while running. A service transition happens only after FnCall returns on the common driver. No lock/RefCell/TLS guard crosses a child demand, recursive eval, thread hop or public return. There is no `ExpressionContextGuard` API to make this safe automatically.

### `tikv/batch.rs`: prepare once, call official RPN, return native shape

C-r1 existing APIs are `compile_local(&LocalExpr,&[tipb::FieldType],LocalCompileContext) -> LocalResult<LocalProgram>` and `LocalProgram::eval(&mut self,&mut LocalEvalState,&mut EvalContext,LocalBatch) -> LocalResult<VectorValue>`. `LocalBatch` requires predecoded values, so it is **not** the native demanded-binding seam. C2a's released binding entry must be used for that purpose. Width-one local scheduling still supports 0/1/1024/1025 total rows and retains selection order/repetitions; do not build a second scalar interpreter to work around the RPN cap.

Proposed D methods are an explicit checked `Prepared::compile(Arc<LoweredSpec>)`, `eval_one` over one native Row and `eval_selected` over a Chunk plus physical-occurrence selection. They return `Result<Datum/Vec<Datum>,CallerEvalError>` with separate preparation/bridge/contract/SQL errors; broad native error-enum wiring is a later reviewed cut. The first seed's methods never call native eval on compile or execution failure. `batch.rs` only invokes local compile/eval, applies consumer scheduling, and calls the value bridge for output.

Share `Arc<LoweredSpec>` only. Each suite/worker owns its own `LocalProgram` and state (`Box<dyn Any + Send>` metadata remains non-Sync). No Sync bound widening, unsafe impl, shared mutex-wrapped evaluator, or compiled program hidden in a plan cache. Existing parallel projection constructs a suite inside each worker task (`X/projection.rs:295–307`); initial integration may compile per task, but must not claim reuse across chunks. A later persistent worker-owned suite is an ownership/performance change to measure, not a reason to share compiled metadata.

Direct-column owner transfer remains separate from computation. All calculated expressions and their input borrows finish before `ColumnSwapHelper::swap_columns`; on error input owners stay put (`EvaluatorSuite::run:454–457`). No native numeric fast path is an acceptable performance rescue after a domain migrates.

## 3. Demand, error and diagnostic contract

### Demand discrepancies that prevent broader admission

| Concrete evidence | Required treatment |
|---|---|
| AST binary at `lib.rs:902–920` eagerly reads both sides; typed/PB AND/OR short-circuit | Record the intended strict AST correction and prove it with independent regression/oracle evidence. Do not assert baseline equivalence. |
| Typed integer fast path `scalar_function.rs:1487–1489`, generic numeric `1611–1616`, PB binary `pb_builtin.rs:275–280` stop on NULL lhs for plus | C-r1 ordinary postfix Plus eagerly evaluates rhs before the parent call even when lhs is NULL. C2a controls do not automatically change ordinary calls. Admit arithmetic only after explicit null-stop scheduling on the same driver, or an independently approved behavioral correction. |
| `evaluator::tests::numeric_batch_does_not_suppress_nested_errors_on_null_rows:825–861` expects NULL+overflow-rhs to error only with vectorization | Preserve via an explicit consumer/call demand profile on the same runtime, or approve/test a correction. A universal width-one scalar schedule silently changes this test's contract. |
| Filter APIs evaluate all physical rows before reapplying input Sel; projections operate selected occurrences | Pass the actual consumer's demand set. Never deduplicate repeated projection occurrences; never invent repeated filter effects from a bool mask. |
| Top-level Constant/parameter broadcast occurs once per nonempty chunk in evaluator/filter; deferred constants run per row | Do not memoize every input by slot/physical row, and do not reread a broadcast parameter per output row. This is caller scheduling, not a new interpreter. |
| Native error formatting at `numeric_expression_text:337`, `math_overflow_expression:579` calls Constant::eval_in | Do not reuse these helpers as-is from error adaptation. Retain diagnostic source text and already-demanded values; never evaluate again merely to format an overflow. |

The released C2a implementation scope is strict AND/OR/IF/IFNULL/CASE/COALESCE over its admitted Int types; completion still requires C's actual checkpoint. That scope does not settle IN/FIELD/ELT/MAKE_SET/GREATEST/LEAST/REGEXP/JSON, simple CASE selector multiplicity, ordinary NULL-stop or mixed lazy host semantics. C2's signed local NULLIF remains eager and is not proposed for the first D seed.

### Preserve the executed prefix, not a sorted reconstruction

Native ordinary warnings are ordered `(WarningLevel,u16,String)` with a 65535 detail limit; `warning_count`, `truncate_warnings`, `take_warnings_since` inspect that ordinary list. `enter_cop_eval` sends warnings to a deduplicating buffer, and `take_warnings` appends cop/remote warnings after ordinary warnings (`stmt_context.rs:3835–3973,4393–4409`). The source comment claiming TiKV always deduplicates is not borne out by current TiKV `EvalWarnings::append_warning`, which counts and appends occurrences until its cap.

TiKV `EvalContext` owns `Arc<EvalConfig>` plus `EvalWarnings`; default detail cap is 64; total warning count and retained details are separate. It has no severity or native fold bookmark, and `take_warnings()` resets both (`tidb_query_datatype/src/expr/ctx.rs:184–242,329–334`). Therefore:

- One local ordered diagnostic owner must receive kernel and primitive events immediately. Preserve repeated warning occurrences and the prefix before the first failure. No end-of-statement merge/sort of independently accumulated lists, and no local use of cop deduplication.
- A successful TryFold is rejected if it produced diagnostics; failed speculative evaluation rolls back only its own diagnostics. A future shared journal bookmark must include **total count and retained length**, not just retained length at a saturated cap. Ordinary execution failures do not erase the already-produced warning prefix or undo host effects.
- Native notes/errors need severity support or an explicit restricted admission. D must not downgrade notes to warnings to fit tipb::Error.
- Preserve the actual publication lifecycle: StatementRecordSet::next drains after each pull; finish/collect and session error handling drain too (`S/record_set.rs:104–133,181–219`, `S/lib.rs:2222–2253`). A persistent local journal needs an incremental publication cursor or a common sink so publication does not reset counts or repeat earlier details. A source-only proposal is not proof that this is wired.
- Parallel diagnostic order is a separate scheduling contract. Existing shared StmtContext warning mutex records worker arrival, while projection row results are released in fetch sequence. Do not promise deterministic statement-prefix diagnostics merely by sharing that mutex or sorting results after speculative effects. First seed is effect-free; later diagnostic/host-bearing parallel admission needs ordered execution/publication and first-error tests.

### Original errors and code/message adaptation

`LocalError::Evaluation` carries `tidb_query_common::Error`. Its EvaluateError variants are DeadlineExceeded, InvalidCharacterString, Custom{code,msg}, Other(String); conversion from a boxed std::error **stringifies** (`tidb_query_common/src/error.rs:9–42`). Native EvalError is Debug/Clone/Eq, not Display/Error. An arbitrary `box_err!(native_error)` is not typed preservation.

A practical **proposed** D adapter can retain a callback's original native EvalError in invocation-local `native_failure`, return an Evaluation with a structured transport diagnostic, and, when the driver immediately aborts with Evaluation, return the original native error. Assert/refuse any mismatched failure/result state. The slot is cleared before every invocation and consumed on return; no SQL code or string is used to recognize/reconstruct the original. This relies on C2's immediate-abort/no-child-error-catching contract. Structured rendering must factor/reuse the existing exhaustive authority at `X/driver/errors/exec.rs:67–117,117+`, not copy that match into another table. An approved opaque typed-source carrier in C/common would be cleaner but is not present.

For **TiKV-origin** errors there is no old native enum instance to preserve. Structurally match `ErrorInner::Evaluate`/EvaluateError, checked-convert code width, and introduce a reviewed native `EvalError::Diagnostic {code,message}` plus one existing-renderer arm. Preserve evaluation-origin tagging and derive SQLSTATE through the existing `MysqlError::new/from_evaluation` (`driver/errors/mod.rs:95–135`). Do not parse Display's `Evaluate error:` prefix or stringify into Unsupported. Session's error-origin policy affects the extra warning row at finish. Contract/resource/admission failures remain distinct internal/boundary errors, not SQL NULL or generic retry requests.

Full source-shaped kernel messages have an additional **C API gap**: current local errors/warnings do not identify the failing originating node or retain already evaluated operands for TiDB's expression-text formatting. Node sidecars in D cannot recover that from a private compiled program after failure. Before admitting affected arithmetic/CAST domains, add an approved local diagnostic-site/operand-fact hook to the **same driver**, or establish exact compatible messages directly in the shared kernel. Do not re-evaluate or inspect the native evaluator to reconstruct them. Control-only signed transport avoids this unsolved kernel-message domain.

## 4. Actual lifecycle and configuration handoff

The following fields/APIs are source-backed; the RPN cache/reset wiring is proposed.

| Boundary | Existing source authority | New local obligation |
|---|---|---|
| New statement/attempt | StmtContext constructor `stmt_context.rs:1834–1905`: new context ID, diagnostic lists, cop depth, current-insert state, seeded-RNG map; `with_lazy_clock:1699–1710` | Fresh local execution/diagnostics owner or complete explicit reset after prior publication. A cloned StmtContext is the same statement, not a fresh ID. Do not call `now()` merely to configure Int controls. |
| Session variables | `SessionVars::generation` at session `vars.rs:2695`; successful set/restore increments; `statement_var_snapshot:662–671` caches by generation | Capture only actual compile-sensitive facts; refresh per statement. Global/instance-dependent fields are reread separately (`stmt_ctx.rs:1033–1039`), so generation alone is insufficient. |
| Prepared execution | `warnings.rs:268–270,319–365` clears/installs parameter snapshots; `dispatch.rs:1201–1208`; StmtContext::with_prepared_params at 1691 | Rebind on every EXECUTE, including cache hits. Changed parameter declaration/type requires new specialization/admission; a saved Constant value never becomes an immutable literal. |
| Plan rebuild | `physical_plan_cache.rs:431–466` replaces cached parameter/deferred values and invalidates scalar caches; `ScalarFunction::invalidate_cached_arguments:706–718` | Rebuild immutable lowerings after tree/type/argument changes. Do not cache by existing name-only hash or commutative canonical hash (which can reorder effectful arguments). Initial seed needs no global compile cache. |
| Statement-keyed metadata | `builtin_ext/cache.rs:15–85`: one context ID, clones empty, failed initialization not retained | Worker-local RPN metadata must respect captured state. Share only immutable specs; failed/demanded metadata initialization cannot become a global success/failure cache. |
| Prepared-plan environment | `S/prepared_ast.rs:129–217`: vars and blacklist generations, transaction/autocommit, SQL mode, timezone, charset/collation, LIKE setting | Add/check any new specialization dependencies explicitly: complete SQL/projected schema, original PB identity/metadata, provenance, catalog version, demand profile and actually captured config. Do not assume existing keys cover them. |
| Row/chunk/invocation | Physical Row and selection are invocation data; VALUES setter/clearer at `stmt_context.rs:3232–3257` | Clear borrowed input, original-failure slot, host task ledger and Fresh/Reuse cache at return; reset per-invocation work counters, retain capacities. Do not reset statement diagnostics per row/chunk/program. |
| Session-lived effects | Uservars, unseeded RNG, sequence last values, advisory locks, current TSO and publication cells remain native owners | Do not clear them as RPN scratch. Host task cleanup releases task resources, not SQL side-effect rollback. SET_VAR overlay restoration belongs to the next boundary (`S/warnings.rs:393–400`). |
| Unistore request | Fresh RequestEvalContext in `cophandler.rs:1446–1484`; warnings drained by handler at 150–175 | Separate request-owned local execution and inputs; preserve request flags, timezone, explicit zero precision and error/warning response rules. A request is not a Session substitute. |

Configuration mapping cannot be `EvalConfig::default()` or blindly `Flag::from_bits_truncate(native.push_down_flags())`:

- Reuse audited native statement class/policy, `Columns::truncate_level`, division-by-zero level, `type_flags`, timezone and division precision. `StmtContext::push_down_flags:2129–2154` and `statement_pushdown.rs:87–103` are existing projection facts, not proof of every local semantic equivalence.
- TiKV exposes only SQL-mode bits21–26 and its request-flag subset. Native IGNORE_ZERO_IN_DATE bit7, charset validation/negative-to-unsigned behavior and other ConversionFlags lack direct flag counterparts. Admit relevant kernels only after mapping their policy, not merely their representation.
- Native session snapshot currently filters `div_precision_increment == 0` to default4 (`S/stmt_ctx.rs:777–783`), while unistore preserves an explicitly supplied0. Record this discrepancy and follow the actual input owner's value; no silent normalizing change in D.
- Named timezone/DST rules must survive. KV config has timezone/flags/sqlmode/cap/precision plus paging/max_keys/is_test; it has no native context ID, fixed statement clock, max_allowed_packet, charset/collation, RNG, week format or encryption mode. Those are explicit native/config/host facts, not invented EvalConfig fields.

## 5. Smallest separately testable release after C2a

**Recommended D1: executable `IntControlSeed`, no automatic general-evaluator routing.** This is a deliberately named seed, not a migrated function family or transcreated package. It exercises the real handoff rather than adding an unused generic framework.

Release only after parent accepts C2a source/compile/tests and publishes its actual input-service ABI. Implement the four boundary files and a narrowly named private constructor. Accept trees closed under:

- signed LongLong row columns; strict Int and typed-Int NULL constants;
- strict AND/OR/IF/IFNULL/searched CASE/COALESCE whose **every** node and expected argument/result type meets the signed LongLong contract;
- complete metadata/provenance snapshots and no opaque function metadata other than None.

Use real existing SQL binding to produce `IF(bigint_condition,bigint_left,bigint_right)` and nested control fixtures. Nullable BIGINT columns provide real NULL cases; do not fabricate LongLong around an untyped NULL/Tiny expression merely to make the test pass. Use actual PB `IfInt`/`IfNullInt`/`CaseWhenInt`/LogicalAnd/Or with preserved ingestion sidecars for PB tests. COALESCE need not be falsely claimed as an already supported PB decoder ID.

Keep the smallest first product gate row/literal-only. Demanded prepared/correlated bindings can follow in the same boundary after the original-error and binding-lifetime tests; they need no new evaluator. Reject deferred, unsigned, mixed types, AST-only unresolved names, ordinary plus/ABS/NULLIF, non-Int controls, HostCalls and unadmitted nested operations at the explicit seed constructor. **Do not change existing public SQL behavior to that restricted error domain.** Do not automatically attempt the seed and then replay native evaluation on failure. Existing unported production routes remain visibly unmigrated until separately switched; the explicit seed entry either succeeds through TiKV or fails, with no fallback.

Minimal initial files requested, **not currently authorized**:

| Owner/request | Exact cut |
|---|---|
| D new private boundary | `E/tikv/{catalog,lower,context,batch}.rs`, `E/tikv/mod.rs`, `E/tikv/tests.rs` (or inline tests in those files) |
| D serial PB retention/accessors | `E/distsql_builtin.rs`, `E/constant.rs`, `E/column.rs`, `E/scalar_function.rs`; only source sidecars/read-only metadata accessors and their propagation tests, no native deletion yet |
| D serial signature-fact factoring | `E/pushdown_catalog.rs` conditional facts and corresponding pure builder references, retaining all remote admission/tests; no call to remote value-binding serialization |
| Parent wiring | `E/lib.rs` module export; all Cargo/shared-root changes if actual compiler/error/enum dependencies require them; registry/main plan remain parent-owned |
| C existing scope | C2a input/control/driver implementation and published API; any demanded-null-stop/diagnostic-site extensions require separate parent release |
| B/E/A | No datatype/collation lock requested. Consume the existing checked bridge; send representational gaps back to B/E/A. |

The following are **subsequent** atomic cuts, not part of D1. Request each group serially when its domain is ready, rather than reserving every file now:

| Subsequent owner handoff | Concrete necessary files |
|---|---|
| D, structural preparation/fold | `E/rewriter.rs`, `E/rewriter/fold_mode.rs`, `E/new_function.rs`, `E/builtin_{arithmetic,compare,op}.rs`, `E/constant_fold.rs`; corresponding `rewriter/result_type.rs` value probes if reached by the released domain. Parent retains `E/lib.rs` public route/wiring. |
| D, typed/PB/vector compute removal | `E/expression.rs`, `E/scalar_function.rs`, `E/scalar_function/pb_builtin.rs`, `E/evaluator.rs`, and only the admitted family's native module tails. Reacquire earlier metadata files rather than overlap another writer. |
| D, executor instantiation/scheduling | `X/projection.rs`, `X/expand.rs`, `X/selection.rs`, `X/predicate_pushdown.rs`, `X/vec_group_checker.rs`; later exact hash-agg/window/default/generated/DML caller files named in section1. Parent coordinates public constructor/API changes and shared worker-pool roots. |
| Parent-coordinated diagnostics, D caller half | `E/context.rs`, `X/stmt_context.rs`, `X/driver/errors/exec.rs`, `X/driver/errors/mod.rs`, `S/stmt_ctx.rs`, `S/record_set.rs`, `S/warnings.rs`, `S/lib.rs`. Factor the existing native renderer or pass its pure typed renderer at the executor-owned boundary; `tidb-expr` must not acquire a circular dependency on `tidb-executor`. No duplicate error-match table. |
| Parent/C shared runtime half | `KV/components/tidb_query_datatype/src/expr/ctx.rs`, optional `KV/components/tidb_query_common/src/error.rs`, and C's `K/types/{function,expr,expr_eval}.rs`/`K/local/**` only for approved diagnostic/demand/service changes. B/E coordinate datatype ownership; D does not write these files. |
| D, cache invalidation caller seams | `DB/rust/crates/tidb-planner/src/physical_plan_cache.rs`, `S/prepared_ast.rs`, plus the actual expression-owner rebuild sites; parent retains plan-cache shared integration decisions. |
| D, unistore route/deletion | `DB/rust/crates/tidb-unistore/src/cophandler.rs` and `src/cophandler/eval_context.rs`; only after complete relevant PB/transport domains, not as part of the control seed. |

This separation avoids freezing C's ongoing work behind a whole-package caller rewrite. Parent remains writer of Cargo/manifests/locks, architecture/maintenance guides, the registry and the sole plan throughout.

### Necessary structural preparation cut before the AST/public route

Add an explicit preparation purpose distinct from existing SQL fold policy, e.g. `StructuralOnly` versus `SqlBuild`; this name is proposed. In StructuralOnly, the existing resolver binds names/defaults/metadata and existing inference helpers derive types, but no `eval_constant`, fold, `prepare_numeric_arguments` value probe or comparison/value refinement may execute SQL. Reuse the existing casts/type builders; factor structural cast insertion from its optional fold. Value-dependent inference remains a separately scheduled SQLBuild operation through the shared runtime, not a guess in StructuralOnly.

`eval_in(&Expr,&dyn Columns)` lacks column type/binding metadata. Existing call sites with schema must pass the real ColumnResolver/typed preparation from their owner; a compatibility no-schema entry may only preserve its documented resolvable subset or require an explicit typed-binding extension. Do not secretly call `get` during planning and infer a type from that row's Datum. Whether all old no-schema callers can provide full type facts is a required call-site review, not asserted solved by D1.

## 6. Route/deletion handoff for every live bypass

These are removal targets, not deletion claims. Preserve the frozen source inventory and attach later receipts rather than rewriting the denominator.

| Release surface | Final route / deletion |
|---|---|
| AST `eval/eval_in`, `func::eval_func` | Structural typed binding -> local spec -> official RPN. Delete AST operation/control/function computation, retaining syntax/type/host-binding responsibilities. Explicitly test approved AST demand corrections. |
| Typed/PB scalar | Entry above PB-first/fast/generic branches. Delete migrated kernel arms in `PbBuiltin::eval`, fast integer computation and native signature bodies together. PB decoder retains exact identity; no dispatch by `sig_*` display strings. |
| Value-only helpers | Build explicit typed shared call over values and context or delegate an already shared datatype primitive at the appropriate non-expression boundary. Delete native arithmetic/string/temporal tails; never wrap values into synthetic AST/PB to reuse another evaluator. |
| EvaluatorSuite/Expand/projection | Immutable specs shared, each mutable suite owns prepared programs; preserve output indexes, virtual rows, constant broadcasts and owner moves. Remove native vec/numeric/decimal execution paths. `ProjectionExec`, parallel task suite binding and Expand call sites need the mutable-suite change together. |
| Filters | Keep scheduling, physical-mask/NULL logic and CNF effects; replace all compute, including FastSelectionFilter and FastScanFilter. Externally accepted pushed filters still obey their storage/diagnostic policy. |
| Group/join/sort | Replace direct numeric-batch bypass; retain group boundary-first order, correlated binding and IN unknown masks. Do not evaluate constant sort keys that were previously not evaluated or bypass projected TopN expressions. |
| Folding/planner/ranger/cache | All fold computation via shared prepared calls, preserving speculative warning rollback and Datum/error pairs. Replace deferred whole-expression callback implementations at their caller seam; never install them as input/host services. Inference/registry/schema/hash code remains TiDB-owned. |
| Default/generated/CHECK/partition/DML | Retain dependency/schema/default/subquery/assignment policy; route each scalar evaluation and shared primitive. Preserve row provenance and current bindings; do not treat storage conversion as an admitted expression cast automatically. |
| Aggregation/window | Route all arguments/order keys/frame/default expressions and migrate duplicate shared SUM/AVG/Decimal arithmetic. Keep aggregate/window state machines. No whole-executor completion claim. |
| Session/legacy callers | Route remaining AST calls, including SHOW and old helper consumers; keep refresh/drain/transaction/plan-cache lifecycle. No session state is stored in shared specs. |
| Unistore | One typed ingest and local runtime for the complete P∪S admitted source domains; delete Func/SimpleSig/native eval helpers as each domain closes, not only 102 shadowed wire mappings. Preserve tests that construct public Func directly by moving their vectors to the shared route. |
| Primitive modules | Per-family delete/delegate actual computations in `ops*`, `coerce`, `cast`, `arg_eval_type`, `row`, math/string/time/regexp/builtin_ext and datatype numeric/JSON/time code. Keep legitimate type derivation, codecs and host state. Shared kernels are not proof that every duplicate caller tail was removed. |

For each switched production entry, choose its route before value effects. A TiKV runtime/bridge error must return as that error; it cannot invoke `try_tikv -> native`, run two evaluators and pick one, or retain a native expression closure in metadata. Temporary explicit seed and still-unported entries are not a final dual-backend architecture.

## 7. Tests and static hooks

All following tests/commands are **requested, not run by D**. Parent owns serialized builds. Test filename is not automatically a Cargo target: tidb-expr uses autotests=false and aggregate `--test all`; private unit tests live under `--lib`.

### Existing source tests to retain

- `distsql_builtin::tests::{every_encoder_signature_has_a_typed_decoder,protobuf_signature_survives_display_name_changes,protobuf_control_does_not_evaluate_the_unused_branch,protobuf_binary_and_utf8_signatures_stay_distinct}` plus its wire-type/MOD-signedness and repeated-row JSON cases. These are execution identity tests, unlike remote predicate encoding tests alone.
- `evaluator::tests::{vector_filter_reads_current_parameter_once_per_nonempty_chunk,constant_batch_reads_current_parameter_once_per_nonempty_chunk,constant_batch_preserves_deferred_rows_and_side_effect_ordering,constant_batch_error_preserves_input_owners_and_skips_empty_input,numeric_batch_does_not_suppress_nested_errors_on_null_rows,user_variable_side_effects_follow_select_list_order_for_each_row,calculated_columns_finish_before_direct_owners_move,expression_error_does_not_move_a_direct_column_owner,vectorized_filter_preserves_selection_and_null_mask}`.
- `tests::control::{if_source_vectors_use_wrapped_condition_and_lazy_branch,ifnull_source_vectors_preserve_first_non_null_value,case_when_source_vectors_preserve_lazy_truthiness,coalesce_source_vectors_preserve_first_non_null_value}`; folding/deferred `constant` and `constant_fold` tests; existing scalar typed-return/provenance tests.
- Executor `projection`, `selection`, `predicate_pushdown`, `vec_group_checker`, `expand`, default/generated, DML/join/hash-agg/stream-agg/partition tests and `--test all window_executor_source::` / `default_on_update_source::`.
- Session `tests_prepared_plan_cache`, `tests_eval_bool`, `tests_in_list_full_evaluation`, generated/window and `--test all expression_default_fold_source::`. Frozen notes identify all-ignored planner cache suites; an empty filter is not a cache gate.
- Unistore `cophandler::` including RequestEvalContext policy tests; preserve direct SimpleSig vectors when changing their construction API. C retains wire legacy short-circuit/depth tests and all C-r1 tests alongside new strict-local controls/demand tests.

### Proposed first seed tests (`tikv::tests::`, not existing)

1. `sql_bigint_controls_use_local_rpn`: real parser/resolver typed IF and nested searched CASE/AND/OR/IFNULL/COALESCE; nullable BIGINT columns; assert local preparation and no native eval route.
2. `pb_identity_and_wire_metadata_survive_lowering`: PB IfInt display-name mutation, optional wire-field presence, +/− collate, original wire versus effective scan type; explicit rejection of unadmitted unsigned/array/metadata loss without altering snapshots.
3. `control_tree_admission_is_closed`: supported parent containing plus, NULLIF, cast, host or non-LongLong child fails at seed construction without value reads. No per-child native split.
4. `demanded_input_only`: poison binding on a dead branch stays unread; reachable poison fails at its occurrence; empty selection has no reads; no eager imported native vectors. A poison is an input-service failure, not a preconverted NULL.
5. `physical_rows_and_occurrences`: 0/1/1024/1025 rows; `[2,0,2]`; declared schema/column lengths checked; avoid double application of Chunk selection; validate len/type before internal loaders.
6. `full_metadata_detached`: mutate the original FieldType's shared element backing after preparation; snapshot remains unchanged. Keep collation initialized/explicit flags and literal-vs-text provenance even where the seed rejects the value domain.
7. `immutable_spec_worker_programs`: share only the immutable Arc, compile independent worker programs, differing bindings cannot contaminate results; no Sync requirement for LocalProgram. Count compilations to prevent accidental per-row preparation claims.
8. `deep_seed_prepare_eval_drop`: depths33/64/256 and rejected-limit cleanup on controlled small stacks; avoid recursive Debug/Clone in the test. D lowering and metadata destruction must not undo C's iterative safety.

Follow-up tests before expanding: `parameter_rebind_and_original_error`, `correlated_rebind_not_compile_snapshot`, `try_fold_restores_total_and_details_at_cap`, `warning_prefix_survives_kernel_input_host_failure`, `note_severity_and_incremental_publication`, `scalar_vs_vector_null_demand_profile`, `ast_no_fold_preparation_has_no_effects`, `simple_case_selector_once`, `host_fresh_reuse_cleanup_and_nested_call`, `output_passthrough_provenance`. These names are proposals, not a claim that the APIs support them yet.

### Parent-run recipes and static hooks

From `DB/rust`, after source release, the minimal first gate would use the parent's wrapper:

```text
/home/agent/tidb/expression-unification/tools/cargo-tidb test --locked -p tidb-expr --lib tikv::tests:: -- --test-threads=1
/home/agent/tidb/expression-unification/tools/cargo-tidb test --locked -p tidb-expr --lib distsql_builtin::tests:: -- --test-threads=1
/home/agent/tidb/expression-unification/tools/cargo-tidb test --locked -p tidb-expr --lib evaluator::tests:: -- --test-threads=1
```

Add the affected factoring/metadata tests and nonzero test counts, then broader consumer/SQL/diagnostic/readiness gates when routes actually change. First gates do not replace repository lint/readiness or parent M6 validation.

Static source hooks to add to a **new nonfrozen** receipt/check, not to today's frozen generator outputs:

- Every completed family links to frozen family ID, overload/signature IDs, type/collation/context/provenance domain, and **all** entrypoint IDs. Partial signed controls/arithmetic are listed separately. Count remains zero until complete domains/deletions are proven.
- Compare changed production function/source fingerprints against frozen `native_deletion_map`; use source exclusions only for legitimate inference/binding/host/codec responsibilities, not renamed native computations. Keep old tests; do not make a green result by deleting vectors.
- Search the new `tikv/` product boundary for `eval_in`, `eval_func`, `.eval(ctx`, `from_expression_in`, `to_pb`, fake `tipb::Expr`, dynamic function-name fallback, callback Expr parameters, Any+Sync/unsafe Sync, and native algorithm imports. Review each match; a textual zero is not call-graph proof.
- Track routing hits/compile counts in tests at each source seam: AST, typed, PB, vector, filter, scan, helper, fold, DML/default/generated, agg/window, session and unistore. A shared collation hit alone does not count as RPN execution.
- PB coverage checks retain exact official ID sets and nested-tree closure separately. No enum aliases or constants inflate family count. Do not change the fixed 245/221 denominator.
- After complete routing, fail tests/static review if migrated families remain reachable in PB Kernel, numeric batch, FastSelection/Scan or SimpleSig paths. Explicit host slots have finite registration and primitive-only ownership receipts.

## 8. Remaining hard domains and concrete C/B/parent questions

| Gap | Existing evidence / required owner action |
|---|---|
| C2a public API/test gate | Current source was in-flight; obtain actual released service signatures and compile/test receipt. C2b's staged HostCall API is still a later release, not implied by read_input. |
| Scalar versus batch call demand | C adds scheduling/profile support on the same official driver if preserving both contracts; parent approves any behavioral correction and its tests. D cannot solve it by eager native subtree evaluation or a second interpreter. |
| Source-preserving errors and diagnostic sites | Native-error sidecar or opaque typed source; shared code/message/severity/bookmark/publication decisions; node-site/operand facts for source-shaped kernel diagnostics. C/common/datatype changes need parent ownership, not guessed APIs in D. |
| SQL/PB metadata | D serial retention/factoring cut; checked prost-to-TiKV enum/FieldType conversion; original actual metadata bytes, not a synthetic Expr. InUnion is currently the only nonempty public CallMetadata variant; other real metadata needs separately reviewed typed extraction/admission. |
| NaN and float output | B bridge rejects NaN (never NULL); infinity and negative zero transport do not establish kernel policy. No eager failure of an unreachable NaN. Real output/Float32 width and grouping NaN inequality require tests and representation work before full Real families. |
| Decimal/MyDecimal | Wide+SmallVec architecture approved, implementation pending safety. Keep storage/result/declared scale, Res status, negative-zero/error values and aggregate/window arithmetic. No format/parse/f64 bridge or unsafe 40-byte struct copy, and no hidden loss of wide supported domains. |
| Temporal/JSON/enum/set/BIT/vector | Not bridge-admitted by current value.rs. Need exact type/timezone/FSP, SQL NULL versus JSON null, binary/text provenance, invalid encoding and hybrid flags. Ordinary temporal/JSON remain required targets, not blanket exceptions. |
| Delayed constants/metadata and output lineage | Current LocalExpr Constant requires an already representable ScalarValue; dynamic slots do not pretend to be constants. Deferred regexp/path failures and mixed selected-branch provenance require same-runtime extensions, not eager import or rerunning a condition. |
| Mixed lazy host | BENCHMARK/AES-style stages need explicit primitive decomposition, bounded task state, Fresh/Reuse and cleanup. C2b mock protocol tests are not native SQL migration. |
| Performance | Reuse worker programs/scratch, not mutable shared metadata. Width-one scheduling, owned value copies, compile-per-task and full snapshots can be expensive. Measure after correctness; no native fast-path fallback. |

## Validation record and handback

Exact Bash inspection commands run by D (all returned successfully; no build/test commands):

```text
# cwd /home/agent/tidb
pwd; ls -la; ls -la expression-unification; ls -la expression-unification/tidb

git -C expression-unification/tidb rev-parse HEAD; git -C expression-unification/tikv rev-parse HEAD; git -C expression-unification/tidb status --short; git -C expression-unification/tikv status --short
```

File/source inspection used line-numbered read, glob and grep tools. Two initial guessed paths, `E/collation.rs` and `E/correlated_column.rs`, did not exist; discovery corrected them to `expr_collation.rs` and `column.rs`. No conclusion depends on those failed reads. D's child independently read session/context ownership and returned before report completion. This report was reread/self-reviewed; no product diff or compiler proof is claimed.

Handback: parent owns all actual releases, manifests/shared roots/registry, heavy builds and the sole plan. D requests only the small D1 file cut above after the C2a checkpoint; broader route/diagnostic/AST cuts require explicit subsequent ownership. No current product authority was exercised. The report's implementation path is **TiDB typed immutable spec -> TiKV local prepared official RPN -> host-shaped value/diagnostics**, never remote serialization, native replay, private protocol IDs or a second evaluator.
