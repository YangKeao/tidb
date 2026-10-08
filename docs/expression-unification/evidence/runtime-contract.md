# Runtime C — local expression contract and scoped checkpoints

Status: **C3d/native537 and D4's private exact203 caller cohort remain accepted historical baselines. Parent accepted the C3b helper gate (datatype368/RPN537/aggr40) and explicitly released Stage B's seven expression integration files, in addition to the two new lineage files. The two datatype helpers stay frozen. Parent accepted C3b natively:575/575 expression tests, aggr40/40 and the exact nine-file source manifest; the isolated layout test reports EvalFrame400/Program128/Control400/FrameResult176/Node152 bytes. Full TiDB remains1274 passed/four unchanged baseline failures/93 ignored. Parent subsequently accepted C3c's separate closed SQL numeric-batch design and released exactly seven existing expression files plus this receipt for implementation; all other expression files and both datatype helpers remain frozen. Parent accepted the corrected C3c joint checkpoint:608/608 RPN tests (575 prior+32 C+1 concurrent B), aggr40/40, unchanged measured frame layout, and full TiDB1292 passed/four unchanged baseline failures/93 ignored. The initial zero-test compile failure and test-only corrections remain recorded below. The accepted C3c source checkpoint remains manifest413f4f…2894; four of those files were subsequently loaned for the separate C4 implementation, while profile.rs/profile_tests.rs/runtime.rs remain frozen. No public numeric-batch route or family credit is implied. C ran no builds or tests. Parent reports D5's separate caller cohort at18 passing and full TiDB1292 passed/four unchanged baseline failures/93 ignored before C3c; those are not C3c evidence. No public activation, wider family, whole-caller/native-origin, context/severity or warning-site claim is made. C4/evaluated-value work is separate: parent approved and C implemented the closed ASCII worker in exactly six loaned files. Parent reports the C4 DEV library build and post-J/C4 aggr40 succeeded. The first full RPN test compile failed E0624 with ZERO tests run; the exact new-fixture-only correction and superseding six-file manifest are recorded in C4-source-r1 below. Parent accepted the corrected C4 helper native checkpoint: full RPN636 passed/0 failed/1 isolated ignored, exact isolated origin1 passed, aggr40/40, and remeasured frame400/128/400/176/152. The actual post-C4/J full TiDB comparison finished1310 passed/four unchanged baseline failures/93 ignored; parent compared all four blocks against D6 with thread IDs only normalized. A's wider readonly review and native caller/pool/deletion/performance/family gates remain separate. Required metadata prewarm and the accepted both-pin isolated Arc-request accounting extent (88 bytes for this fixed configuration) do not establish C4 worker or native-caller correctness.** Interface revision: **C/M0-r0 design, C-r1 receipt, C2-proposal-r1, C2a-native-r2, C2b-source-r1, C2b-native-r1, C3-proposal-r0, C3a-source-r0, C3a-native-r1, C3d-proposal-r0, C3d-staged-r0, C3d-source-r0, C3d-native-r1, C3d-caller-r1, C3b-proposal-r1, C3b-stage-a-r0, C3b-source-r0, C3b-native-r0, C3c-proposal-r0, C3c-proposal-r1, C3c-source-r0, C3c-source-r1, C3c-native-r0, C4-proposal-r0, C4-source-r0, C4-source-r1, C4-native-r0**, 2026-09-28. The parent alone owns `/home/agent/tidb/EXPRESSION_UNIFICATION_PLAN.md` and checkpoint approval. This evidence file is not a second ExecPlan. Earlier design sections remain design, not a claim that the entire interface below has been implemented.

## Scope and evidence status

At the original M0 checkpoint, the only artifact written by Runtime C was this file; no product source or build tools had been changed/run. The later C-r1 release and actual implementation/validation status are recorded at the end. C has still not changed manifests, locks, datatype files, impl_like.rs, maintenance guides, the main plan, old `expression-reuse` source, or Git history, and has run no Cargo build/test, commit, reset, or push.

Observed official worktrees:

- TiKV: `/home/agent/tidb/expression-unification/tikv`, `548812e1ef57aef077a2062a9cc356640a6347f5`.
- TiDB: `/home/agent/tidb/expression-unification/tidb`, `364aef2bab5cc633ecb76a775ae8f36f86a6687d`.
- Read-only commands actually run: `pwd && git rev-parse HEAD && git status --short` in TiKV; `git rev-parse HEAD && git status --short` in TiDB. Both succeeded; both status outputs were empty at observation time. No Cargo command was run by C. Parent separately reported both pinned toolchains installed and successful `cargo metadata --locked --no-deps`; that is not runtime test evidence.
- Read the complete unique plan, TiKV `AGENTS.md`, maintenance `README.md`, `repo-overview.md`, and `src/coprocessor.md` in the required order. No subordinate `components/**/AGENTS.md` or TiKV `PLANS.md` was found. For the bounded sibling host audit, also read TiDB `AGENTS.md` and `PLANS.md`; no `rust/**/AGENTS.md` was found.
- TiKV `rust-toolchain.toml:1–4` specifies `nightly-2026-01-30`. `components/tidb_query_expr/Cargo.toml:1–45` confirms package `tidb_query_expr`, ordinary inline library tests, and existing codegen/datatype/tipb dependencies. No new dependency is needed for the first slice.

All source anchors below refer to those exact baseline trees. `K/` means TiKV root; `D/` means TiDB root; `E/` abbreviates `K/components/tidb_query_expr/src/`; `G/` is `K/components/tidb_query_codegen/src/`; `T/` is `K/components/tidb_query_datatype/src/`. Proposed symbols are explicitly distinguished from existing ones.

## Early recommendation to freeze

Use **one official RPN compiler/runner and its existing kernels**. Add a checked local construction facade, not a standalone engine, borrowed-backend abstraction, recursive TiDB evaluator, second lazy framework, or fabricated protobuf signature. Factor function selection, validation, metadata preparation, and assembly into shared TiKV internals before implementing the facade. Retain `Box<dyn Any + Send>` as an internal implementation detail and compile independently per worker; share only immutable typed specifications.

Three changes are prerequisites, not optional polish:

1. Preparation must expose retained/reordered original argument indices without exposing arbitrary metadata construction. The official IN initializer mutates the tree, so a typed facade cannot merely pair untouched local children with its result.
2. Strict local controls must use the official `ShortCircuitFnCall` family with an explicit iterative frame stack. Width one alone does not fix eager arguments or compile-time regexp errors; lifting the 32 cap alone risks native-stack overflow.
3. Host calls require a new explicit runtime service seam. `RpnFnCallExtra` currently contains only the return type. Never smuggle a mutable session through `Any`, TLS, a raw pointer, or a callback that re-enters an already borrowed host.

**Baseline correction domain:** strict local AND/OR matches the typed TiDB path, but not every old AST path. `D/rust/crates/tidb-expr/src/lib.rs:902–919` eagerly evaluates both `Expr::Binary` operands; `scalar_function.rs:1620–1633` short-circuits typed logic. Record AST eager-to-lazy behavior as an approved correction with before/after diagnostics tests, not a blanket compatibility claim.

## What the official code actually provides

| Anchor | Existing behavior and implication |
| --- | --- |
| `E/types/expr_builder.rs:88–108,291–360` | Public `build_from_expr_tree(tree_node: tipb::Expr, ctx: &mut EvalContext, max_columns: usize) -> tidb_query_common::Result<RpnExpression>`. Maps and validates an Expr, calls its mutating metadata initializer, then takes the possibly changed children. `max_columns` only checks offsets, not full schema. |
| `E/types/function.rs:39–89` | `ShortCircuitFnMeta { sig, fn_ptr }` receives context, schema, decoded columns, logical rows, row count, and child RPNs. No call metadata, return type, host, or scratch parameter. `RpnFnMeta` has `validator_ptr: fn(&Expr) -> Result<()>`, `metadata_expr_ptr: fn(&mut Expr) -> Result<Box<dyn Any + Send>>`, and a kernel pointer receiving `&[RpnStackNode]`, `&mut RpnFnCallExtra`, and `&(dyn Any + Send)`. Extra is only `ret_field_type: &FieldType`. |
| `E/types/expr.rs:13–39,132–135` | Actual nodes are Constant, ColumnRef, FnCall, ShortCircuitFnCall; nested children already are `Box<[RpnExpression]>`. Metadata is Send, not Sync. There is no official LocalProgram or HostCall. |
| `E/types/expr_builder.rs:24–27,363–448,538–584`; `E/lib.rs:976–989` | Only AND/OR can be lazy; request bit, worthwhile heuristic, and nesting <=32 are all required. Others use eager FnCall. Same-op chains can flatten. Over-depth switches to eager. |
| `E/impl_op.rs:35–73,250–365` | Existing AND/OR maintains pending output positions separately from physical row indices and merges SQL three-valued logic, but recursively calls child `eval_decoded`. This is the foundation to refactor, not code to copy into `local/`. |
| `E/types/expr_eval.rs:193–306,318–389` | `eval` eagerly decodes all referenced columns. `eval_decoded` requires decoded inputs and asserts `0 < output_rows <= BATCH_MAX_SIZE`; allocates a stack per multi-node evaluation; evaluates node-major. ColumnRef checks logical row count, not all input bounds/type invariants. |
| `E/types/expr_eval.rs:27–100,107–190` | Result can be a borrowed scalar, borrowed selected column, or generated vector. `RpnStackNode::take_vector_value` errors on scalar; vector variant already gathers a Ref through its logical row map. `get_logical_scalar_ref` broadcasts scalar and selects vector correctly. |
| `T/codec/batch/lazy_column_vec.rs:13–46,108–123`; `T/codec/data_type/vector.rs:28–81,94–169`; `T/codec/data_type/logical_rows.rs:5` | Existing `From<Vec<VectorValue>> for LazyBatchColumnVec`, `columns_len`, `VectorValue::{with_capacity,from_scalar,eval_type,len,append}` suffice. Zero-column `rows_len()` is always zero; explicit row count is necessary. Batch limit is 1024 selected occurrences, not maximum physical row index. |
| `E/impl_compare_in.rs:168–346` | IN metadata hashes/removes constants, swaps survivors, truncates children and unsigned flags. DateTime currently uses compare instead of hash (`164–166`, registry `E/lib.rs:606–613`). |
| `E/impl_regexp.rs:55–115,119–158,281–303,342–365` | Constant regexp compile is fallible at build time. Runtime already has a fallback when cached Regex is absent and checks NULL/input UTF-8 before building a dynamic regexp. Replacement has a separate instruction cache. |
| `G/rpn_function.rs:675–839,1248–1323` | Macro emits Expr-specific validators/initializers and a metadata downcast with `expect`. `raw_varg` checks return type/arity, but checks child types only with `extra_validator`. REGEXP declarations currently lack it. Raw vararg execution holds a mutable TLS `RefCell` borrow and uses lifetime transmutation; recursive kernel/host callbacks here are not safe API design. |
| `E/types/function.rs:288–343,346–382` | Standard type checks allow Enum as Int/Bytes; vararg buffers are TLS. `extract_metadata_from_val` parses protobuf metadata or defaults it. These are not a safe public local compiler contract by themselves. |
| `E/impl_cast.rs:34–108,236–269,279–295`; `E/impl_time.rs:927–953,1738–1757` | Cast selection depends on child constness, full source/result field type, binary provenance and InUnionMetadata. Date arithmetic and TIMESTAMPDIFF derive metadata by parsing constant unit bytes; metadata audit must include them. |
| `T/expr/ctx.rs:68–82,184–241,329–334` | EvalConfig has TZ, flags, SQL mode, warning cap, division precision, paging/read fields and test flag. EvalContext holds `Arc<EvalConfig>` plus ordered capped warnings and total count. There is no built-in host/session, packet-limit/statement-clock seam, row-tagged warnings, or warning bookmark API. |

Some official mappers index children **before** the generated validator: `E/lib.rs:67–74` (`ToBinary`) and `102–117` (`LIKE`). Shared preparation must check arity before invoking these selectors, or refactor them to checked access. Catching panic is not validation.

## Proposed public local facade (new API, not compiled)

Use the existing `tipb::FieldType`, `tipb::ScalarFuncSig`, `tidb_query_datatype::codec::data_type::{ScalarValue, VectorValue}`, `LazyBatchColumnVec`, and `EvalContext`. Do not introduce another Column/Datum enum. The following signatures fix responsibilities; their names are proposed C/M0-r0 additions.

```rust
pub enum LocalExpr {
    Constant {
        value: ScalarValue,
        field_type: tipb::FieldType,
        literal_kind: LiteralKind,
    },
    InputSlot {
        slot: usize,
        field_type: tipb::FieldType,
    },
    Call {
        function: FunctionRef,
        args: Box<[LocalExpr]>,
        return_type: tipb::FieldType,
        metadata: CallMetadata,
    },
    HostCall {
        slot: HostSlot,
        args: Box<[LocalExpr]>,
        return_type: tipb::FieldType,
    },
}

pub enum FunctionRef {
    TiPb(tipb::ScalarFuncSig),
    Local(LocalFunctionId),
}

pub enum LiteralKind { Typed, Text, BinaryLiteral }
pub enum SlotKind { Value, BoundLiteral(LiteralKind) }

pub enum CallMetadata {
    None,
    InUnion { in_union: bool },
}

pub struct HostSlot(pub u32);
pub struct HostRows<'a> {
    pub occurrences: &'a [usize], // positions in top-level selection, not row IDs
    pub input_rows: &'a [usize],  // corresponding physical row IDs, same length
}
pub struct CompileLimits {
    pub max_nodes: usize,
    pub max_depth: usize,
}
pub struct ExecutionLimits {
    pub max_steps: u64,
    pub max_retained_bytes: usize,
}
pub struct LocalCompileContext<'a> {
    pub eval: &'a mut EvalContext,
    pub hosts: &'a HostCatalog,
    pub slot_kinds: &'a [SlotKind], // exactly schema.len(), compile facts only
    pub limits: CompileLimits,
}

pub fn compile_local(
    spec: &LocalExpr,
    schema: &[tipb::FieldType],
    cx: LocalCompileContext<'_>,
) -> LocalResult<LocalProgram>;

pub struct LocalBatch<'a> {
    pub columns: &'a LazyBatchColumnVec,
    pub physical_rows: usize,
    pub selection: &'a [usize],
}

impl LocalProgram {
    pub fn return_type(&self) -> &tipb::FieldType;
    pub fn eval(
        &mut self,
        limits: ExecutionLimits,
        ctx: &mut EvalContext,
        batch: LocalBatch<'_>,
        host: &mut dyn HostEvaluator,
    ) -> LocalResult<VectorValue>;
}
```

`LocalProgram` privately owns the validated RPN, immutable schema/projection facts, return representation, host signatures, and compile-sensitive key. `ExecutionLimits` is an immutable `Copy` policy. Each public evaluation creates a fresh invocation budget and row scratch; neither is stored for reuse or mixed with statement diagnostics. Context is borrowed explicitly so several programs can use **the same** statement/request EvalContext. `eval(&mut self, ...)` expresses worker-exclusive use even though existing RPN entrypoints use `&self`. Neither compiled object nor metadata is promised Sync. `Arc<LocalExpr>`/`Arc<[FieldType]>` may be shared; assert their Send+Sync properties in implementation tests rather than adding unsafe traits. Build one compiled instance per worker; create one budget and row scratch per invocation, not per row.

`LocalResult` should distinguish invalid specification, invalid batch, resource exhaustion, host contract violation, and an original `tidb_query_common::Error` from execution. This is a structured classification, not string-matched fallback. `LocalProgram` never returns a native retry instruction. Its internals and mutation APIs are not public, even though the low-level official RPN type has mutable accessors.

`CallMetadata` is a closed typed input, not `Box<Any>`, raw function pointers, serialized local Exprs, or an extension bag. Initial InUnion covers the metadata actually used by CAST; IN/regexp caches and temporal unit facts are derived internally from typed children. Add other variants only for audited real requirements, with one owner. Invalid metadata/function combinations are rejected. PB adapters parse actual wire metadata exactly once into the same typed input and preserve default behavior for absent metadata. Local calls never create fake ScalarFuncSig values.

Initial `LocalFunctionId` recommendation: `NullIfIntSignedSigned`, a closed TiKV-owned ID for two signed Int operands and the first operand's result type. Add a tiny `#[rpn_fn(nullable)]` kernel in `impl_control.rs` that invokes existing `impl_compare::compare::<BasicComparer<Int, CmpOpEq>>` (`E/impl_compare.rs:15–19,66–81`) and returns NULL on true, else the saved first value. This proves no-PB-ID registration with a real pure function, without copying comparison logic. It is **only the signed-int seed domain**, not complete NULLIF migration. No NULLIF registry arm exists in baseline TiKV; TiDB's existing values arm evaluates ordinary equality and retains the first value (`D/rust/crates/tidb-expr/src/func.rs:736–755`). It does not require reserving/changing any protobuf number.

InputSlot represents data, parameters, correlated values and other correctly timed bindings. A slot is not a frozen result cache. Mutable user-variable/sequence/RNG reads must remain demand-time HostCalls, not values prefetched before an earlier side effect. Statement-stable clock values can be typed runtime bindings; never bake NOW into the shareable spec. Spec/FieldType/provenance/metadata/host-catalog/build policy or compile-sensitive configuration changes invalidate specialization; same-type rebinding does not.

### Value safety is separate from SQL conversion

A Constant already contains an exact shared runtime value. Local compilation must not serialize it to PB and decode it back. It checks representation/FieldType compatibility without executing a SQL CAST or temporal/UTF-8 validation on an unvisited value. Binary collation alone must not set BinaryLiteral; true hex/bit provenance comes from lowering. The wire adapter may retain its legacy constant-classification policy internally without imposing it on typed local text.

Foundation B's boundaries, verified in source:

- Do not use `ScalarValue::from(f64)` as a checked bridge: `T/codec/data_type/scalar.rs:142–174` silently turns failed `Real::new` into NULL. Use checked `Real::new` and a structured bridge result.
- `Time::from_packed_u64` applies timezone for Timestamp (`T/codec/mysql/time/mod.rs:2009–2049`). Component import must preserve typed representation without compiling in a session TZ or triggering SQL validation. Keep that import under B's ownership.
- `Duration::from_nanos` rounds to FSP (`T/codec/mysql/duration.rs:446–449`). A bridge that must preserve sub-FSP nanos needs an exact import; the old constructor is not that API.
- Preserve signed-bit carriers, Decimal scale/status, JSON and invalid-byte representation. Bridge failures are not permission to switch evaluation backends or pre-evaluate casts in dead branches.

### Required demanded-binding extension: conversion is observable too

The ready-decoded `eval` signature above is deliberately a narrow first slice: it accepts **already representable shared values**. It does not authorize Integration D to convert every native input row or literal up front. Potential failures such as NaN import, a Decimal beyond TiKV's representation, malformed JSON or a nonfinite vector component can otherwise escape from an unselected row or dead branch **before RPN runs**. Selecting width one fixes neither issue. Input validation checks representation tags/lengths, not SQL validity of every value.

Before admitting domains with fallible transport, extend the same official RPN leaf/frame path with a demand-input service. A proposed addition is:

```rust
pub trait LocalRuntimeServices: HostEvaluator {
    fn read_input(
        &mut self,
        ctx: &mut EvalContext,
        slot: usize,
        rows: HostRows<'_>,
        expected_type: &tipb::FieldType,
    ) -> LocalResult<VectorValue>;
}

impl LocalProgram {
    pub fn eval_with_bindings(
        &mut self,
        limits: ExecutionLimits,
        ctx: &mut EvalContext,
        physical_rows: usize,
        selection: &[usize],
        services: &mut dyn LocalRuntimeServices,
    ) -> LocalResult<VectorValue>;
}
```

The compiled InputSlot uses the ordinary RPN ColumnRef/input-leaf instruction. In demand-binding mode, that instruction calls `read_input` only when the frame reaches it, for its active occurrence subsequence; in decoded mode the same leaf uses the supplied decoded columns. Both produce the **existing** VectorValue/stack values and run through the **same** RPN node driver. This is an input service, not a second Column representation or interpreter. `read_input` may only fetch/copy/check a bound value; it cannot recursively evaluate a SQL expression. One combined services owner lends itself to an input read or host invocation at a time; never construct aliased `&mut` session borrows for separate provider and host objects. A ready-decoded adapter may preserve the existing fast path.

Lowering must never fetch runtime bindings while constructing the shareable spec. A literal that cannot yet be losslessly made into shared ScalarValue remains an immutable, statically typed binding slot whose source value lives in an immutable TiDB-side binding descriptor; it is converted only on demand. This keeps the four LocalExpr forms unchanged. Such a slot must not be treated as a hashable compile-time constant or prevalidated by the metadata cache. Typed literal provenance required for CAST remains a compile fact: `LocalCompileContext::slot_kinds` has exactly one entry per schema slot, normally `SlotKind::Value`, or `BoundLiteral(LiteralKind)` for an immutable literal binding. BoundLiteral carries provenance, not permission to read the binding during preparation or treat it as a cached constant. Store these facts in LocalProgram and include them in specialization identity. The shared local CAST selector uses explicit BinaryLiteral provenance independently of storage as a Constant versus a bound slot; LegacyWire preserves its existing classification.

Do not reorder conversion relative to child demand: a strict row's first error/preceding warnings arise at that read, not on a batch prepass. Empty selection does not read bindings. Repeated physical rows carry separate occurrence identities, and volatile sources must use HostCall rather than being cached as data. For batch-safe graphs, transport must also be proven exact/non-diagnostic before widening. Validate every provider result's type/count before any parent kernel. Metadata and input-row caches must not outlive bindings or turn Fresh host demand into result reuse.

**Demand deferral is not a representation fix.** A demanded valid TiDB value outside TiKV's representable domain is still an unresolved migration domain. B must supply exact representation, or parent must retain the precise existing feature/domain under the explicit exception policy; do not label a new generic compile-time/runtime Unsupported as compatible. The initial ready-value slice proves only its representable domains and cannot justify deleting a broader native entry. Add tests where an unrepresentable literal/row is never demanded (no conversion call/error), then is demanded (the explicitly recorded compatible route or known incomplete domain), and cross-check all original SQL outcomes before expanding admission.

## One shared prepared-call path

### Internal surface to add

Refactor, do not add parallel wire/local mapper tables. Keep `map_expr_node_to_rpn_func` as a wire wrapper if callers/tests require it; move its selection logic to a single typed call-shape selector, owned by the parent registry owner. Wire and local builder adapters feed it.

A proposed internal contract is:

```rust
pub(crate) struct ArgIndex(usize); // index in original source argument order
pub(crate) struct CallShape<'a> {
    function: FunctionRef,
    return_type: &'a tipb::FieldType,
    args: &'a [ArgShape<'a>],       // type, literal provenance, stable index
    metadata: &'a CallMetadata,
}

pub(crate) fn prepare_call(
    call: &mut CallBuild<'_>,
) -> tidb_query_common::Result<PreparedCall>;
```

`CallBuild` privately carries CallShape, `BuildPolicy::{LegacyWire, StrictLocal}`, and access to source literals through the **one** existing wire-literal decoder or directly to local ScalarValue. It must not expose child-evaluation callbacks. Implement literal access as cached construction-time decoding by original ArgIndex: only metadata-requested literals are decoded before child assembly; later Constant assembly consumes the same decoded value. This preserves the current preparation-before-child-traversal sequence and avoids eager normalization of every wire literal changing compile-error order. Extract the current `handle_node_constant` decoder (`expr_builder.rs:474–700`) once; retire IN's duplicate `Extract` wire decoding (`impl_compare_in.rs:62–155`) when converted. Local literals never enter that wire decoder. Share byte decoding without silently erasing legacy semantic differences: ordinary Float decoding currently uses `Real::new(...).ok()` (`expr_builder.rs:630–635`), whereas IN's `Extract for Real` rejects that failure (`impl_compare_in.rs:96–107`); Enum key extraction also depends on expected key domain. A canonical checked decode outcome plus explicit legacy policy/projection must preserve those differences (or parent must approve a correction). Test them before deleting the superseded Extract implementation; do not normalize every decode into a typed NULL.

`PreparedCall` has private fields: selected RpnFnMeta or validated control descriptor, exact output representation, initialized `Box<dyn Any + Send>`, retained argument mapping, source-order demand descriptor, and compiler-owned batching/effect classification. Its only assembly operation consumes it into the RPN builder; callers cannot substitute a different function/metadata pair. Keep `retained_args() -> &[ArgIndex]` crate-private. Validate the full original shape first; validate prepared arity/types again if the initializer changes shape. No public API returns an arbitrary `RpnFnMeta`+Any pair as a safe prepared expression.

Change generated validation/initialization to target this common shape/build context, with legacy Expr adapters only at the wire boundary. Existing kernel `fn_ptr` ABI can remain unchanged for ordinary calls. `rpn_fn` still generates the only row/vector invocation plumbing. Update its source generator and test expectations; do not edit generated expansion artifacts. Its extra-validator and metadata-mapper contracts, type-checker and all fixed/varg/raw_varg constructors move together.

Preparation ordering must be: structural shape/admission checks safe before indexing; choose specialization from complete argument/result metadata; generated and custom validation; metadata preparation with explicit policy; retained-child assembly; node emission. Do not confuse signature-selected signedness with result FieldType unsigned interpretation. Audit CAST's current field-type-based selector against typed PB signatures rather than assuming validation proves exact typed-wire semantics.

### IN: mapping is necessary but not sufficient

For source `[base, const1, dynamic2, const3, dynamic4]`, baseline reverse-scan/swap removal leaves `[base, dynamic4, dynamic2]`, i.e. original indices `[0,4,2]`. Pairing its metadata with `[0,2,4]` or all five local children is wrong. PreparedCall must carry this map and the corresponding unsigned flags. Never hash a runtime InputSlot/parameter/correlated binding as a permanent constant.

For **LegacyWire**, the common preparation path can preserve this existing layout and its tests. For **StrictLocal**, preserve left-to-right demand independently of physical kernel argument layout. In particular, constants after a diagnostic/volatile expression cannot leapfrog that expression merely because a hash lookup finds a match. Initial strict implementation should retain ordered arguments and decline global constant extraction. Add ordered IN demand through the same ShortCircuit/frame driver, factoring a per-candidate comparison helper out of `impl_compare_in` and reusing it for existing hash/eager kernels. Later, only proven-safe constant segments may be hashed. Width one does not repair `[0,4,2]` reordering, eager dynamic arguments, or base-NULL skipping decisions.

Tests must pin match-before-error, error-before-later-match, base NULL, NULL candidates, mixed signedness, collation comparison failure, and repeated input bindings. IN, FIELD, ELT, MAKE_SET, GREATEST/LEAST, REGEXP argument demand, and JSON path/update demand each need a per-signature admission record. They are **not all certified strict simply because IF works**. `E/impl_string.rs:656–717` shows ELT/MAKE_SET consume selected values only inside a kernel after official RPN already evaluated every child.

### REGEXP: defer value failure at its real demand point

Separate bad type/arity/metadata encoding (compile errors) from invalid constant pattern or match-type contents (execution errors). Smallest compatible strict change: keep the existing `Option<Regex>` runtime fast path; successful constant compilation may be cached. In StrictLocal preparation, a failed constant regexp compile retains its typed pattern/match arguments and leaves the cache absent so the **existing** runtime `build_regexp_from_args` raises the error when demanded. This is deliberate deferred validation, not an ignored execution error. LegacyWire may retain immediate preparation failure until a separately approved wire behavior change.

Do not install an unconditional “throw cached error” node: `regexp_like(NULL, invalid_pattern)` can return NULL before compiling the pattern, and input UTF-8 errors may precede pattern errors (`impl_regexp.rs:125–135`). The same ordering applies to REPLACE's regex and replacement-instruction metadata. Failure need not be cached; successful cache is instance-local. If adding a lazy cache later, `std::cell::OnceCell` in Send-only metadata is sufficient; no Sync requirement, locks, or cloning boxed errors is needed.

Complete REGEXP raw_varg shape validators are mandatory: LIKE is Bytes/Bytes/[Bytes]; SUBSTR adds [Int, Int, Bytes]; INSTR adds [Int, Int, Int, Bytes]; REPLACE is Bytes/Bytes/Bytes/[Int, Int, Bytes]. Respect each optional-argument prefix and NULL representation. Generated raw_varg currently does not establish these invariants. Temporal unit parsing also needs a documented structural-vs-value classification: a grammar-bound interval enum is build information; a runtime argument is not automatically a compile-time constant.

## Extend the official ShortCircuit runtime, not a second engine

### Control contract

Keep nested child programs in `RpnExpressionNode::ShortCircuitFnCall`. Extend ShortCircuitFnMeta with a closed control kind and preparation/return-type facts, rather than making it call a second local evaluator. A closed kind may identify AND, OR, IF, IFNULL, searched CASE, COALESCE, and the specifically admitted ordered-demand controls. Local-only control identity is a LocalFunctionId, never an invented ScalarFuncSig. Do not flatten unlike operators or reorder branch arguments.

Replace recursive `impl_op::eval_logical_short_circuit -> arg.eval_decoded` execution with explicit resumable frames in `types/expr_eval.rs`. The existing `eval_decoded` and the local facade call **that same driver**; ordinary FnCall still calls the original RpnFnMeta pointer. Wire wrappers use no-host services and their current build policy. A frame retains next node/child, ordinary operand stack, unresolved **occurrence positions**, their physical input rows, branch result storage, and accumulator/control phase. A driver step either schedules one child on an active subsequence, consumes its result, calls a normal kernel/host leaf, or completes the parent.

The host receives no mutable interpreter/program handle, and no kernel callback recursively enters this driver. The explicit EvalContext borrow is scoped to that invocation; no second session/context borrow is manufactured. Keep generated vararg buffers' dynamic borrows wholly within one kernel invocation. For nested frames, do not retain a `RpnStackNodeVectorValue::Ref` borrowing a row-map Vec owned by the same frame: gather/materialize at the retention boundary using existing VectorValue operations. Constants can borrow program-owned values; column and control results needing longer lifetime become owned selected VectorValue. This avoids self-referential frames without inventing a new borrowed backend. Variable-width branch results can be retained as owned branch vectors plus an output-position map and materialized once in output order; do not assume Bytes/Json have sized-vector `set` operations.

Required semantics:

- AND: only false resolves a row; NULL stays pending until false appears or all arguments finish. OR: only true resolves; preserve SQL three-valued logic. Existing `ScLogicalOp` normalization/merge rules remain the single logic implementation.
- IF: evaluate Int condition first; NULL/0 choose false; only the chosen branch runs for each occurrence.
- IFNULL: evaluate first once; demand second only for its NULL occurrences.
- Searched CASE: condition/result pairs in order, NULL condition is false; demand matching result and stop that row; ELSE runs only for unmatched rows, absent ELSE yields typed NULL.
- COALESCE: advance only NULL occurrences; stop at first non-NULL; all NULL yields typed NULL.
- Return all supported kernel domains without formatting/string conversion: Int, Real, Decimal, Bytes, DateTime, Duration, Json; add Enum/Set/VectorFloat32 controls only with checked branch representation and registry support, not by unchecked reuse of an Int/Bytes validator exception. Baseline control registry lists seven domains at `E/lib.rs:599–635`.
- Simple CASE must retain its selector once per occurrence; `D/rust/crates/tidb-expr/src/lib.rs:1242–1253` is evidence. Use a local closed control descriptor with prepared comparisons and internal retained-operand loads; existing comparison/CAST kernels evaluate each comparison. Do not desugar to repeated public subtrees. If comparison domains differ per WHEN, the retained selector is still evaluated once; any approved comparison coercions consume that saved value. Freeze that typed lowering descriptor with Integration D before admitting mixed-type simple CASE.
- NULLIF must retain its first argument; do not compile `IF(Eq(a,b),NULL,a)` by duplicating `a`. The signed-int seed kernel evaluates two arguments once in source order, matching the sibling's current values path. Other domains must pin second-argument demand and coercion/error order rather than assuming base NULL permits skipping it.

### Depth and resource contract

StrictLocal has no profitability gate, request-flag dependency, or >32 eager fallback. Give compiler and runner explicit `CompileLimits`/execution work limits from the host's existing expression resource budget. A budget refusal is a structured resource error, never eager SQL evaluation. Exercise 33, 64 and 256 alternating controls within the supplied budget and a separate over-budget refusal.

Use an explicit construction stack too; replace recursive flattening/metadata walks where deep strict graphs reach them (`expr.rs:95–115,176–193`, `expr_builder.rs:315–334,567–583`). Test construction, evaluation, metadata collection and teardown on a small thread stack. Simply deleting the depth constant leaves recursive execution and collection unsafe. Resource limits must also account for retained branch vectors, not only nodes. No claim of infinite nesting.

LegacyWire should keep flag/heuristic/32-level **admission policy** for this Demo unless parent explicitly changes the wire contract. Its accepted nodes run on the same new frame driver; keeping its admission rule is not retaining a second evaluator. `K/doc/maintenance-guides/src/coprocessor.md:120–130` must describe the distinction. Its `222–225` and `expr.rs:98–104` also require preserving logical work accounting when chains flatten; IF/CASE must not inherit the AND-chain `args.len()-1` estimate blindly.

## Checked batch and diagnostic contract

Before reaching assertion-bearing RPN loaders:

1. Compile-time: every slot is in schema, its full projected FieldType agrees with the schema, all literals match their transport representation, every call/host shape and result shape is validated, all limits are checked. Caller retains TiDB schema-only facts that PB FieldType cannot represent; changing them still invalidates lowering.
2. Eval-time: column count equals compiled schema, each column is decoded, each vector has the expected physical EvalType and exactly `physical_rows` elements. Check **every selection index** against physical_rows; allow sparse, reversed, duplicated indices. Reject invalid batches structurally before any kernel/warning/RNG/host. Do not call `eval`/`ensure_columns_decoded` on local data: they can decode columns in dead branches.
3. Zero selection: after cheap structural validation, return `VectorValue::with_capacity(0, prepared_result_eval_type)` without entering RPN or a host. Zero columns does not mean zero output rows. For constant-only evaluation supply an explicit physical row universe and selection, e.g. physical_rows=3, selection=[0,1,2], empty columns.
4. Nonzero selection: process consecutive **selected occurrences** in caller order, chunking at <=1024, not sorting/deduplicating or bounding physical indices to 1023. Default width is **one** until compiler-owned effect/demand facts prove a larger width safe; the consumer cannot assert “pure” to bypass validation.
5. Materialize result: scalar -> existing `VectorValue::from_scalar(value,n)`; vector Ref -> existing `take_vector_value` logical gather; generated vector -> move it. Validate type and length before append or feeding another kernel. The final output has exactly selection.len() elements, in selected-occurrence order. A generated vector's logical indices are local 0..n, not original physical indices.
6. Host vectors must be checked **immediately** before any parent loader, not only at final output: exact prepared representation and active row count. A malformed host response is HostContract error, never a panic or best-effort coercion.

Width one preserves row-major order **within one program call**. It does not fix subtree demand or the executor's scheduling across several select-list programs. Integration D must invoke programs in the required row/select-list order for diagnostic/volatile graphs; evaluating all rows of expression A then all rows of B is still different. New registry entries default to diagnostic/volatile unknown and width one. Numeric-looking calls can overflow; borrowed-column fast paths and proven total comparisons can be optimized only after proof.

One external `&mut EvalContext` persists across chunks and programs. Never call `take_warnings` per slice or reconstruct a default context in a hot loop. On `Err`, warnings emitted before the error remain in that context. Preserve MySQL code/message, detail ordering/cap and total count. `EvalWarnings` has no row tags (`T/expr/ctx.rs:184–219`), so sorting collected warnings cannot reconstruct a truncated prefix.

TryFold needs an explicit checkpoint/rollback addition or an integration-owned precise bookmark of warning total and retained detail length; do not just discard errors and leak fold warnings. Caller/session owns statement reset and one response drain. Host diagnostics must enter the same ordered sink during the invocation, not a separate list drained later. `Columns::append_note` is a real sibling seam (`D/rust/crates/tidb-expr/src/context.rs:807–835`), while TiKV warnings currently have no severity: preserving notes requires an explicit diagnostic-contract decision, not an assertion that existing EvalWarnings covers it.

EvalConfig already supports TZ/flags/SQL mode/division precision/cap; packet-limit/clock/charset defaults and source-shaped error policy need explicit audited additions or lowering/runtime slots. Their ownership is shared with Integration/B; do not add `caller_is_tidb` mode branches. Changed compile-sensitive config must reject/recompile a stale instance; do not let old metadata survive silently. No shared mutable session, warning list, metadata cache, or input-row pointer in the Arc spec.

## HostCall feasibility and actual demand

### Eager registered host leaf is feasible, but absent today

Add a `HostCall` RPN node, selected only by local construction from an immutable per-program `HostCatalog`. Store registered slot, prepared signature and child/arity data, not a borrowed session. The catalog records exact allowed host/deferred function identity, argument/result FieldTypes, effect and demand contract, and a version/key. Registry owner is the parent. Unknown slot, wrong arity/types, or catalog mismatch fails preparation. No name-based catch-all/native fallback and no automatic conversion of unsupported pure functions to HostCalls.

Proposed first host seam:

```rust
pub trait HostEvaluator {
    fn call(
        &mut self,
        ctx: &mut EvalContext,
        slot: HostSlot,
        args: &[RpnStackNode<'_>],
        rows: HostRows<'_>,
        return_type: &tipb::FieldType,
    ) -> tidb_query_common::Result<VectorValue>;
}
```

`HostRows` contains stable top-level occurrence positions and corresponding physical row indices; duplicates of physical rows are distinct invocations. Ordinary child arguments are evaluated by RPN first, left-to-right on active occurrences. The adapter's short-lived mutable borrow begins only after children finish and ends before another child/kernel runs. It receives values, **not child Exprs, program handles or eval closures**. A wire evaluation uses a no-host service and can never compile such a node.

TiDB can implement this as a thin explicit-ID adapter around its `Columns`/statement owner. Many existing Columns methods use interior mutability (`context.rs:909–932,970–978`); that does not justify reentrant mutable access. Pure parent/child calls around an allowed deferred host remain TiKV calls. For `add(exception(x), abs(y))`, only the registered exception goes to the host; all arithmetic/ABS/argument evaluation stays RPN.

Actual sibling responsibilities include prepared/current-insert values (`context.rs:545–567`), statement clock (`592–595`), user variables (`908–917`), last-insert-id (`924–932`), RNG (`970–978`), sequence operations (`func.rs:83–107`), locks (`scalar_function.rs:1739+`), and cancellable SLEEP (`context.rs:793–805`, `scalar_function.rs:1712–1737`). Freeze which are slots, host primitives, or explicit deferred kernels. A mere lack of a TiPb ID is not a reason to keep an ordinary algorithm on the host.

### Ordinary eager HostCall is insufficient for these existing functions

- BENCHMARK evaluates its count once, then evaluates its second expression zero or N times; NULL/negative count suppresses it. See `D/rust/crates/tidb-expr/src/func.rs:181–213`, `scalar_function.rs:1694–1710`, test `tests/builtin_info_json_math_source.rs::bench_mark` at `249–284`, and fold-disabled test in `constant_fold.rs::benchmark_scope_keeps_its_subtree_unfolded`.
- AES is already lazy: ECB ignores a third argument without evaluating it and warns 1618; IV modes demand three arguments; NULL input/key short-circuits. See `D/rust/crates/tidb-expr/src/builtin_ext/crypto.rs:218–276`. Its existing `FnMut(index)` API **cannot** simply be forwarded into TiKV while holding a mutable HostEvaluator/session borrow.
- Ordinary SQL conditional/demand behavior belongs in TiKV controls, not a host callback that recursively interprets children. BENCHMARK can ultimately become a closed local control; AES can move its pure kernel and demand policy into TiKV. Until then they are explicit incomplete/deferred domains, not safe eager hosts.

If an approved exception must stay host-owned and demand arguments, add a **staged protocol to the same RPN frame driver**, after the first eager host slice:

- `start(slot, invocation) -> HostStep`; `resume(task_id, typed_child_reply) -> HostStep`; `cancel(task_id)` cleans per-invocation host state. Task IDs are adapter-owned worker-local tokens, not session borrows or callbacks.
- `HostStep::NeedArg { task_id, arg_index, occurrences, mode }` or `Ready(VectorValue)`. `mode` explicitly distinguishes reuse of a retained demand result from **Fresh** reevaluation; BENCHMARK requires Fresh on every iteration. Requested occurrences must be an ordered valid subsequence of the currently active invocation; repeated physical input indices remain distinct positions. Repeated evaluation of a position uses a subsequent Fresh demand.
- The driver releases the host borrow, validates the request, evaluates that child with the same RPN driver and bounded frame/work budget, then resumes. It validates reply type/length and final result, honors cancellation/resource limits, and cancels all outstanding tokens on errors. No arbitrary recursive call while any host/TLS vararg borrow is held.
- A child error stops ordinary demand evaluation and preserves warnings/side effects already performed; cleanup does not replay or roll back SQL side effects. A host contract that catches errors would need its own explicit audited design, not a hidden fallback.

These are necessary **new APIs**; they do not exist at the official baseline. Do not lower a registered Demand host through the eager interface while waiting for them. Keep the old feature as a precise unfinished domain until its safe route is implemented; deleting it and returning generic Unsupported is not migration success.

## Smallest compile/eval vertical slice and implementation decomposition

The first post-approval slice should establish typed construction, reusable execution and safety with no semantic-heavy metadata:

1. Factor the typed call-shape selector, generated validators and opaque PreparedCall; leave one mapping/metadata implementation. Common eager assembly supports Constant, InputSlot and `TiPb(PlusIntSignedSigned)`/`TiPb(AbsInt)` with exact LongLong FieldTypes. Local path must never call Expr encoding. Wire path keeps existing behavior/tests.
2. Add LocalProgram/LocalBatch checks and result materialization over existing `eval_decoded`; start width one. A zero-input constant, a selected column, and `PlusIntSignedSigned(InputSlot(0), Constant(Int(1)))` establish all three output shapes, NULL and repeated/large selections. Reuse compiled instance across bindings.
3. Add `Local(NullIfIntSignedSigned)` via existing `rpn_fn` machinery and comparator, plus one registered eager host fixture. Validate result shape before parent evaluation. This proves the identity spaces without fabricating a TiPb ID or invoking native general dispatch.

That is a compile/eval slice, **not M3 completion**. No IF/AND/OR/IN/regexp graph should be advertised as strict-local-supported in it. A closed local admission table lists only verified domains; consumer migration/deletion waits for its domain to pass.

Next slices, each independently tested:

4. Replace official recursive ShortCircuit execution with shared explicit frames and extend strict AND/OR/IF/IFNULL/CASE/COALESCE across registered return domains. Update metadata walks/depth budgets, preserve wire admission behavior and work accounting, add missing validators.
5. Add demanded input-binding service before any fallible-transport domain is admitted; then convert IN/regexp/CAST/temporal metadata producers to common typed preparation, add IN mapping/demand and dead-branch regexp handling. Test config/cache/diagnostic timing before migrating those domains. Remove superseded Expr-only implementations rather than leaving independent “local metadata” code.
6. Integrate context/diagnostics and required host demand. Add simple-CASE selector reuse and full NULLIF domains only with Integration D's typed lowering agreement. Expand strict demand admission for FIELD/ELT/MAKE_SET/GREATEST/LEAST/etc. as tests justify it.

### Exact proposed write locks after approval

These are requested future assignments, not authority to write now. Current lock is still this evidence file only.

| Owner to assign | Exact files | Work |
| --- | --- | --- |
| Runtime C | `components/tidb_query_expr/src/types/expr_builder.rs`; `types/expr.rs`; `types/expr_eval.rs`; `types/function.rs` | Shared preparation/assembly, node additions, iterative execution, safety/metadata accounting. All abbreviated `types/...` paths here are under `components/tidb_query_expr/src/`. |
| Runtime C | `components/tidb_query_expr/src/local/spec.rs`; `local/compile.rs`; `local/batch.rs`; `local/host.rs`; `local/tests.rs` | New thin facade implementation and tests; no evaluator in local/. All `local/...` paths here are under `components/tidb_query_expr/src/`. |
| Runtime C | `components/tidb_query_expr/src/impl_op.rs`; `impl_control.rs`; `impl_compare_in.rs`; `impl_regexp.rs` | Refactor official control flow; tiny local NULLIF seed; IN preparation/demand; regexp deferral and raw shape validators. All `impl_*.rs` paths here are under `components/tidb_query_expr/src/`. |
| Parent, or explicit single-file handoff to C | `components/tidb_query_codegen/src/rpn_function.rs`; `components/tidb_query_expr/src/types/test_util.rs` | Generator's common-shape contract and fixtures/test construction. Parent must confirm the generator implementation lock before edits; codegen `src/lib.rs` entry remains parent-owned. |
| Parent, or serial explicit handoffs before family work | `components/tidb_query_expr/src/impl_cast.rs`; `impl_time.rs`; `impl_json.rs`; `impl_string.rs` | Their existing metadata constructors/custom validators must be mechanically converted to common call view. No duplicate shadow validators. Coordinate function-family ownership first. |
| Parent shared-file owner | `components/tidb_query_expr/src/lib.rs`; `types/mod.rs`; new `local/mod.rs`; new `local/registry.rs`; `components/tidb_query_codegen/src/lib.rs` | Exports/module wiring, the single TiPb/local registry and safe selector updates, closed control/host identity registration. |
| Parent / coordinated context owner | `components/tidb_query_datatype/src/expr/ctx.rs`; `doc/maintenance-guides/src/coprocessor.md` | Explicit diagnostic/config additions and required maintainer-contract update. B owns exact value import/representation; C does not edit those files. |
| Parent only | All Cargo manifests/locks and workspace dependency/config integration; unique main plan | No independent Cargo resolution, generated entry or registry edits by C. |

No C write is requested in TiDB. Integration D owns lowering, host-ID adapter, compilation cache and all consumer scheduling/deletions. The first runtime slice's removal list is limited to superseded TiKV duplicated preparation paths during the refactor; it proves no TiDB production deletion by itself. Later parent/D deletes migrated native implementations only when domain evidence exists. Old experimental standalone/borrowed/lazy APIs are not read into this contract or carried forward.

## Concrete tests and acceptance evidence to collect

All following checks are **planned / not run by Runtime C**. Existing tests are source anchors, not execution receipts. New tests should use stable `local::tests::` names, and validation must report nonzero selected test counts.

### Preserve existing checks

- `types::expr_builder::tests::{test_validator_fixed_args_fn,test_validator_vargs_fn,test_validator_vargs_fn_with_min_args,test_validator_raw_vargs_fn_with_min_args,test_max_columns_check}` (builder lines 938,975,1036,1067,1515).
- `types::expr_builder::tests::{test_short_circuit_call_is_embedded_in_parent_rpn,test_adjacent_short_circuit_calls_are_flattened,test_left_deep_short_circuit_chains_build_to_one_root_call,test_short_circuit_depth_limit_stress_on_small_stack}` (1205,1255,1359,1411). Last test currently **expects** wire eager fallback above 32 (1443–1451); retain that legacy-policy test, add distinct strict tests instead of rewriting it to hide a regression.
- `types::expr_eval::tests::{test_logical_short_circuit_skips_rhs_rows,test_constant_short_circuit_skips_all_rhs_rows,test_constant_short_circuit_without_logical_rows,test_short_circuit_suppresses_cast_warning,test_normal_parent_consumes_short_circuit_result,test_nested_mixed_short_circuit_calls,test_flattened_short_circuit_multiple_partial_compactions,test_rpn_fn_data}` (458,608,641,677,709,744,818,1622).
- `types::expr::tests::test_cached_metadata_is_deduplicated_and_invalidated_by_mutation` (257), plus work-count invariants.
- Existing `impl_control::tests`, `impl_compare_in::tests::test_in_constant` (415; asserts metadata removes constants), `impl_regexp::tests::{test_regexp_like,test_regexp_substr,test_regexp_instr,test_regexp_replace}` (455,567,791,1128), and `impl_cast::tests`.
- Codegen tests in `G/rpn_function.rs` must verify one validator/metadata implementation for fixed/varg/raw_varg; update source-generation snapshots with the contract, not handwritten generated outputs.

### New facade/control/host matrix

| Suggested test name | Required observation |
| --- | --- |
| `local_compile_eager_slice` | Real `ScalarFuncSig::PlusIntSignedSigned` over native Int vectors and typed constant enters official RpnFnMeta, preserves NULL and return flags; no PB serialization path. |
| `local_output_shapes_and_selections` | Constant-only, direct input, generated call; 0/1/1024/1025 occurrences; sparse/reverse/[2,0,2]; zero input columns with explicit row universe; broadcast; exact order/count. |
| `local_rejects_invalid_input_before_execution` | Out-of-range slot/selection, wrong FieldType/value type, raw column, unequal column lengths, malformed zero-arg LIKE/ToBinary and raw_varg REGEXP wrong type produce structured errors, no panic; counters/warnings unchanged. Host-result validation is tested separately after the host has actually run. |
| `local_demanded_binding_conversion` | Deferred slots include poison NaN/large Decimal/JSON/vector payloads on unselected rows and dead branches: no provider call/error. Active provider calls have correct occurrence order and shape. Unsupported active representation remains explicitly incomplete, not a passing compatibility case. Literal binding preserves CAST provenance without pre-reading values. |
| `local_registered_id_without_tipb` | Local signed NULLIF values (equal, unequal, NULL), exactly-once child counters, rejects outside seed domain; no conversion to fabricated protobuf signature. |
| `local_metadata_retained_arg_map` | IN [base,const,dyn,const,dyn] maps correctly for wire, strict preserves source demand; flags remain attached to original indices; rebinding slots changes outcomes without rebuilding permanent hash data. |
| `local_strict_controls_all_domains` | AND/OR full NULL truth tables; IF NULL condition; CASE matching/ELSE/no-ELSE; IFNULL and all-NULL COALESCE; each supported return EvalType; no dead-branch error/warning/RNG/host. |
| `local_deep_controls_never_eager` | 33/64/256 alternating controls on small stack; nested cast wrappers; poison in dead descendant; build/collect/eval/drop; explicit over-budget error, no eager fallback. |
| `local_dead_regexp_is_deferred` | IF(false, invalid regexp, 7) compiles/runs; live invalid pattern raises original error; NULL regexp input suppresses pattern init; invalid input UTF-8 retains error precedence; repeated calls don't lose diagnostics; replacement cache included. |
| `local_row_major_diagnostics` | Node A warns on row0/row1, node B errors on row0: width1 retains only row0's preceding warning and stops before row1. Cap 1 with multiple warnings preserves prefix+total. Repeat with reversed/duplicated selection and chunk boundary. |
| `local_host_active_rows_and_shape` | Registered eager host counts exact selected occurrences; dead/empty host never called; nested host->pure->host sequencing has at most one mutable host borrow; wrong vector shape rejected before parent kernel. |
| `local_host_staged_demand` | BENCHMARK 0/negative/NULL suppresses child, N repeats it Fresh exactly N times; nested staged hosts; AES-like unused IV is never evaluated; error/cancel cleans tokens and retains earlier warnings. Only after protocol exists. |
| `local_worker_and_binding_lifetimes` | Share immutable spec, separately compile two workers; no metadata leakage; same-type slot rebind; change full FieldType/TZ/mode/collation/catalog key forces correct invalidation; failure retry; scratch has no surviving input borrow. |
| Integration-owned entry tests | Typed/PB/AST demand correction, source-origin assertions including error path, select-list row ordering, simple CASE selector counts, mixed NULLIF, TryFold rollback and one unistore response warning drain. |

Parent-controlled commands after baseline approval and actual test registration, cwd `/home/agent/tidb/expression-unification/tikv`:

```sh
cargo check --locked -p tidb_query_expr
cargo test --locked -p tidb_query_codegen --lib
cargo test --locked -p tidb_query_expr --lib types::expr_builder::tests -- --test-threads=1
cargo test --locked -p tidb_query_expr --lib types::expr_eval::tests -- --test-threads=1
cargo test --locked -p tidb_query_expr --lib short_circuit -- --test-threads=1
cargo test --locked -p tidb_query_expr --lib impl_control::tests
cargo test --locked -p tidb_query_expr --lib impl_compare_in::tests
cargo test --locked -p tidb_query_expr --lib impl_regexp::tests
cargo test --locked -p tidb_query_expr --lib impl_cast::tests
cargo test --locked -p tidb_query_expr --lib local::tests -- --test-threads=1
cargo fmt --all -- --check
```

Use parent-assigned new-worktree Cargo target/cache paths and serialize heavy builds. The generator change also needs affected crate compilation and TiDB caller-toolchain integration checks, not just its string-generation tests. TiKV repo requires `make clippy` rather than ad hoc cargo clippy, and `make dev` before PR readiness; no PR-ready claim is made here. Parent applies TiDB's fresh-workspace/Bazel/lint gates when integration work starts. Filtered zero-test success is not evidence.

## Checkpoint risks and explicit remaining decisions

- **Ownership/refactor size:** real rpn_fn and selectors are Expr-specific. A facade avoiding this refactor by constructing fake Expr trees, manual fn metadata, copied validators, or a second mapper is not the approved thin interface. Freeze shared owner locks before implementation.
- **Runtime resource/lifetime:** lifting 32 without iterative construction/evaluation/metadata handling risks overflow. Retained branch vectors and temporary row maps need budget and lifetime tests. Rust borrow feasibility has been reasoned about, not compiled.
- **Demand coverage:** IF/COALESCE do not settle IN/FIELD/ELT/MAKE_SET/GREATEST/LEAST/REGEXP/JSON timing. Compile admission and coverage ledger must expose incomplete domains, not silently eager-run them.
- **Host feasibility:** eager host is straightforward after explicit service/node additions; demanded/repeated host arguments require staged APIs or migration of that control into TiKV. Existing FnCallExtra and closures do not already provide it.
- **Diagnostic contract:** batch width1 is conservative, not a universal order fix. Cross-expression scheduling, note severity, TryFold rollback and original error mapping need Integration/parent agreement. Default cap/config differs from TiDB and must be explicitly projected.
- **Value bridge:** NaN, Timestamp TZ capture, Duration rounding, unsigned bits and typed NULLs must be solved with B; no string/wire round-trip shortcuts. Enum validator exceptions are not a license to violate vector representation.
- **Behavior correction:** AST eager AND/OR versus typed lazy is a known baseline discrepancy. Parent must record its intended strict outcome and tests; do not state the old entrypoints were identical.
- **Performance:** width1 plus owned nested gathers adds overhead; instrument compilation count, allocations and copies after correctness. Reuse program/scratch and optimize only the same RPN driver. No native fast-path rescue.
- **Evidence:** all API additions, HostCall protocol, vertical slices and tests above are proposals. Product implementation, compile results, behavioral pass counts, migration percentage and performance numbers remain unverified.

Parent next actions: approve C/M0-r0 or revise public signatures; assign generator/custom-validator/context file locks; agree StrictLocal versus LegacyWire policies and AST correction; coordinate B's value import and D's host/demand/lowering contracts; then authorize the smallest slice and targeted build. Runtime C has sent early findings to the parent and will deliver this artifact with the final summary.

## C-r1 release and concrete factoring checklist

Parent subsequently approved the conceptual first slice and released implementation after reporting source-clean TiKV baseline results (collation 8/8, expression 428/428 and targeted Decimal). The earlier read-only status describes M0 evidence, not current permission. **No builds are authorized for C until the parent gives a build slot.** This supplement is the exact factoring checklist, not approval for the later strict-control/demand work.

Temporary exclusive locks transferred to C: `components/tidb_query_expr/src/lib.rs` (selector/closed local mapping/re-exports only), `types/mod.rs`, `types/{function,expr_builder,expr,expr_eval}.rs`, new `local/**`, `components/tidb_query_codegen/src/rpn_function.rs` and its direct test expectations, and exactly `impl_{cast,compare_in,regexp,time,control,json,string}.rs`. These abbreviated paths are under the same expression `src/`. No Cargo/lock/datatype/impl_like/maintenance-guide permission was transferred. Parent must explicitly receive lib.rs/types/mod.rs back before editing them. The granted seven impl files are required by the single initializer/validator signature change, not a release for broad function migration.

### Shared symbols and parent-owned wiring

Implement public identities `FunctionRef`, `LocalFunctionId`, `LiteralKind`, and `CallMetadata` in `types/function.rs`, re-export them from `types/mod.rs` alongside existing RpnFnMeta types. `FunctionRef::Local` initially has only `NullIfIntSignedSigned`. Private shared internals in that same file: `CallArg`, `CallShape`, `CallBuild`, `PreparedCall`, `prepare_call`, `prepare_call_with_meta`, `validate_field_type`, `validate_argument_count_eq/gte/lte`, and `extract_call_metadata`. All prepared metadata fields remain private. `CallShape`/`CallBuild` are shallow owned compile-time descriptors (FieldTypes, original argument facts/literals, real wire bytes or typed metadata), avoiding self-referential borrow adapters; they do not encode/decode a local Expr tree. This refines the illustrative borrowed CallShape sketch above. `CallBuild` retains original argument indices separately from shape, and its metadata input remains separate from the selector's shape.

`lib.rs` exact edits:

1. Add `pub mod local;` and imports of `types::function::{CallArg, CallShape}`. Keep existing `pub use self::types::*`. Local facade exports live under `local`, with no wildcard exposing PreparedCall or initializer pointers.
2. Move the **existing** ScalarFuncSig match at baseline lines 435–974 into `map_tipb_call_to_rpn_func(value: ScalarFuncSig, call: &CallShape) -> Result<RpnFnMeta>`; bind `children = call.args()` and `ft = call.return_type()`. Do not duplicate its arms. Add `pub(crate) fn map_call_to_rpn_func(call: &CallShape) -> Result<RpnFnMeta>` dispatching TiPb to that function and Local to `local::registry::map_local_call_to_rpn_func`. Keep existing `map_expr_node_to_rpn_func(expr: &Expr)` as a thin `CallShape::from_expr(expr)` adapter to the same dispatcher, preserving custom wire mapper tests.
3. Convert `map_int_sig`, `map_rhs_int_sig`, `map_upper_utf8_sig`, `map_lower_utf8_sig` to take `&[CallArg]` rather than `&[Expr]`; preserve their current arity checks and signedness/charset algorithms. Field access becomes `arg.field_type()`.
4. Convert `map_to_binary_fn_sig` to `fn(call: &CallShape)` with an explicit one-argument guard before reading arg0. Convert `map_from_binary_fn_sig` to `fn(ret_field_type: &FieldType)`. Update only ToBinary/FromBinary registry arms. Convert `map_like_sig(ret_field_type, children: &[CallArg])` and guard exactly three args before reading children0/1. **Keep all LIKE collation/charset branches and generic instantiations byte-for-byte semantically unchanged; no impl_like.rs edit.**
5. Add typed `map_unary_minus_int_call(value, children: &[CallArg])`; preserve public `map_unary_minus_int_func(value, children: &[Expr])` as a shallow argument adapter, and point the one registry arm at the typed helper. Keep public `impl_cast::map_cast_func(&Expr)` as a shallow CallShape adapter, while its shared body becomes `map_cast_call(&CallShape)`; the CAST arm group calls that typed function.
6. Existing FieldType-only mapper helpers and boolean arithmetic/comparison mapper helpers remain unchanged. Share short-circuit identity through `map_function_to_sc_func(FunctionRef) -> Option<ShortCircuitFnMeta>`; existing `map_expr_node_to_sc_func(&Expr)` just supplies TiPb(sig). Only existing AND/OR wire selection exists in this first cut; full strict control admission is not enabled.

`types/mod.rs`: keep existing modules/re-exports, adding only the four public identities to `function::{...}`. Do not export CallShape/CallBuild/PreparedCall; internal consumers import `crate::types::function`. `local/mod.rs` declares private implementation modules and `pub(crate) mod registry`; `local/registry.rs` contains the one closed Local-ID arm plus an explicit signed-int local-admission check, not a copy of the TiPb table. Local NullIf selects generated `impl_control::local_nullif_int_signed_signed_fn_meta`, reusing `compare::<BasicComparer<Int,CmpOpEq>>`. No codegen `src/lib.rs` or new Cargo entry is needed.

### One atomic validator/metadata signature cut

`RpnFnMeta.validator_ptr` becomes crate-private `fn(&CallShape) -> Result<()>`; `metadata_expr_ptr` becomes crate-private `metadata_ptr: fn(&mut CallBuild) -> Result<Box<dyn Any + Send>>`. Keep ordinary evaluation `fn_ptr` and the metadata representation unchanged. Codegen updates all fixed/varg/raw-varg constructors, validation/type-checker generators and direct expected expansions together. `metadata_type=tipb::InUnionMetadata` reads real wire bytes or builds the typed metadata from CallMetadata; it never serializes a local Expr. Keep the old public Expr validation helpers as thin wrappers over the common FieldType/count checks if compatibility requires them, not separate algorithms.

Mandatory constructor/validator conversions in the seven locked files:

| File | Exact symbols to convert | Shared input / preserved behavior |
| --- | --- | --- |
| `impl_cast.rs` | `map_cast_call` plus wrapper `map_cast_func`; generated metadata for all twelve InUnion-capturing functions | One CAST selector; existing `get_cast_fn_rpn_node` remains a trusted internal convenience API. Shared extractor preserves absent metadata defaults and real-wire decode errors; no need to change twelve kernel signatures. |
| `impl_compare_in.rs` | `init_compare_in_data<T>` | Accept CallBuild; run current reverse-scan/swap algorithm over retained **original indices**, with unsigned flags following indices, then set retained map. Never independently reorder the caller's local children. Preserve current checked wire key extraction/NaN behavior; typed key projection must not add another wire decoder. |
| `impl_regexp.rs` | `init_regexp_data<C,N>`, `init_regexp_replace_data<C>` | Accept CallBuild; read only actual bytes/string constants from shared argument facts. Preserve LegacyWire eager regexp error timing in this first cut; strict deferral is a later admitted domain. |
| `impl_time.rs` | `build_add_sub_date_meta`, `build_timestamp_diff_meta` | Accept CallBuild; preserve byte-constant unit classification, original indices, errors and interval unsigned/decimal facts. |
| `impl_control.rs` | `case_when_validator<T>` | Accept CallShape; use the same shared FieldType validation including legacy Enum exceptions. Add only the signed NULLIF seed kernel here. |
| `impl_json.rs` | `json_modify_validator`, `json_object_validator`, `json_with_paths_validator`, `json_with_path_validator`, `json_contains_validator`, `member_of_validator`, and helper `valid_paths` | Accept CallShape; retain original arity/type/error contracts, replacing Expr child access with shared args. No JSON kernel algorithm change. |
| `impl_string.rs` | `elt_validator` (used by ELT and MAKE_SET) | Accept CallShape; preserve first Int/rest Bytes checks. No string algorithm change. |

Additional direct callers required for coherent test compilation: `types/expr_builder.rs` uses one prepared helper after its selected mapper and assembles retained arguments; `types/test_util.rs` changes direct pointer calls to common CallBuild (parent may grant that direct test-file lock as allowed by the release); `types/expr_eval.rs::test_rpn_fn_data` changes `prepare_a`, `prepare_b`, `prepare_c` to CallBuild. Tests' actual protobuf fixture builders are not used by the local product path. Existing test-only custom `Fn(&Expr)->RpnFnMeta` mappers can stay via `prepare_call_with_meta`; metadata pairing remains private, not exposed to local callers.

This is an atomic compile-boundary change: updating only lib.rs plus codegen while leaving one of these producers on Expr will not compile. It does **not** require edits to impl_like.rs, impl_math.rs, impl_arithmetic.rs, other generated kernel source bodies, datatype crates, or shared manifests. All other rpn_fn sites get the common plumbing from the same updated macro. No heavy build has been run by C; request the parent's compile slot after the coherent diff and focused signed-int tests exist.

## C-r1 actual source receipt (native codegen and expression gates passed)

**Released scope implemented:** all shared selector/preparation/metadata factoring above; parent separately and explicitly confirmed the existing cfg(test) `types/test_util.rs` adapter lock. Fourteen tracked Rust files changed plus six new `local/{mod,spec,compile,batch,registry,tests}.rs`. `types/expr.rs` needed no edit in this seed; no core alternative evaluator, Cargo dependency, public opaque-metadata constructor, fake Expr/ID in the local product path, or duplicate TiPb signature table was added.

Actual first-slice exports (these supersede the broader illustrative facade only for this checkpoint):

```rust
// Re-exported from crate root/types and local:
FunctionRef::{TiPb(ScalarFuncSig), Local(LocalFunctionId)}
LocalFunctionId::NullIfIntSignedSigned
LiteralKind::{Typed, Text, BinaryLiteral}
CallMetadata::{None, InUnion { in_union: bool }}

// Under tidb_query_expr::local:
LocalExpr::{Constant { value, field_type, literal_kind },
            InputSlot { slot, field_type },
            Call { function, args: Box<[LocalExpr]>, return_type, metadata }}
CompileLimits { max_nodes: usize, max_depth: usize }
LocalCompileContext { limits: CompileLimits }
LocalError::{InvalidSpec(String), InvalidBatch(String),
             ResourceLimit(String), Evaluation(tidb_query_common::Error)}
LocalResult<T>
compile_local(&LocalExpr, &[FieldType], LocalCompileContext) -> LocalResult<LocalProgram>
LocalProgram::return_type(&self) -> &FieldType
LocalProgram::eval(&mut self, ExecutionLimits, &mut EvalContext, LocalBatch<'_>)
    -> LocalResult<VectorValue>
ExecutionLimits { max_steps, ..ExecutionLimits::default() } // Default permits u64::MAX steps
LocalBatch { columns: &LazyBatchColumnVec, physical_rows: usize, selection: &[usize] }
```

The compile context currently needs only resource bounds: exact Int constants and this kernel subset have no session-dependent construction. No default EvalContext is invented inside the facade. Compile limits default to 16,384 nodes/depth 256. The state reuses a width-one row-selection buffer and carries a work budget; **it does not claim reuse of the evaluator's existing per-call temporary RPN stack**. Metadata remains `Box<dyn Any + Send>` inside worker-owned RPN instances; new value handling uses native ScalarValue/VectorValue cloning/reference paths and adds no generic Copy requirement.

`compile_local` admits only exact signed LongLong schema/constants, the two TiPb IDs PlusIntSignedSigned/AbsInt, and the closed local NULLIF ID. It checks complete input-slot FieldType equality, shape/type/metadata admission and construction limits, then iteratively emits ordinary official RpnExpressionNodes. `registry::check_local_admission` is a capability allowlist, not a second kernel selection table. Local NULLIF uses the existing BasicComparer<Int,CmpOpEq>, stores/evaluates its first argument once, and does not introduce a PB value. Unknown/unsupported signatures, controls, other representations and irrelevant metadata are errors, not eager/fallback implementations or compatibility claims.

Batch checking verifies schema column count, decoded Int representations, every declared column's exact physical length, all selected indices and work budget **before any evaluation**. Then it returns empty typed output for zero selected rows, or invokes only `RpnExpression::eval_decoded` with one selected occurrence at a time. This handles >1024 total rows without violating the official batch cap; order and repetitions remain intact; scalar/column/generated output is materialized to exactly one Int per occurrence. External EvalContext warnings are never taken/reset. All declared schema columns in this seed must be signed Int and predecoded, even if unused: demanded or fallible native input import is explicitly not provided yet.

Scope of the preparation invariant: the wire expression builder and checked local facade share preparation. The pre-existing public `impl_cast::get_cast_fn_rpn_node` convenience used by aggregation implicit casts still chooses a trusted cast kernel and default InUnionMetadata directly, as specified in the checklist; old public RPN node construction and test-only metadata overrides also remain. This checkpoint does not claim to remove every legacy trusted constructor. They cannot inject arbitrary metadata into LocalProgram, whose compiled node storage is private.

Deletion/replacement checklist completed:

- Removed Expr-specific metadata pointers from RpnFnMeta/codegen and direct callers; the replacement `metadata_ptr` and shape validator are crate-private. Old public Expr validation utilities remain thin adapters over common FieldType/count validation, not duplicate validation algorithms.
- Replaced the single lib.rs Expr match body with a typed match and its wire adapter; kept map_unary_minus_int_func/map_cast_func wire-facing adapters and the original LIKE generic instantiations unchanged.
- Removed IN's protobuf child mutation; its initializer computes the same reverse-swap original-index map and aligned flags. Wire builder consumes that map. Shared typed IN key projection does not serialize a value or copy the wire decoder; IN is still outside local admission.
- Replaced direct metadata/validator invocations in wire assembly and tests. No ShortCircuitFnCall/legacy depth-limit behavior was deleted or broadened. Regex compile-time failures remain immediate for LegacyWire.

Ten new tests are present in `local/tests.rs`: seed arithmetic/rebinding/NULL, 0/1/1024/1025 scalar-column-generated shapes, repeated/reordered rows, closed NULLIF, malformed/unadmitted specs, batch validation before arithmetic error, limits, pre-descriptor budget/literal rejection, retained wire IN order `[0,4,2]`, safe bad selector arity (including exact malformed CAST wording), and sharing only immutable spec with independent worker programs. Some themes share one test; ten is the actual test-function count, not a coverage percentage. Existing codegen expected expansions and direct metadata test callbacks were updated.

Observed validation so far: pinned Rust formatter completed successfully on the explicit C-owned file list with `--config skip_children=true`, avoiding recursive edits to other owners; scoped `git diff --check` passed. The initial formatter command using `rustup` failed exit 127 because rustup was not on PATH. C read the parent tool helper and then used its pinned nightly toolchain's `bin/rustfmt` directly, read-only from the old toolchain location. No Cargo build/test was run by C. Main compile/test slot was requested from the parent after the coherent sources existed. An independent read-only source review completed without identifying a concrete compile mismatch, reachable local indexing/type panic, or IN layout/codegen expectation regression; it ran no compiler/tests and therefore supplied no compile proof. It confirmed the intentional legacy implicit-cast preparation exception noted above.

Parent-driven compile outcomes now read directly from logs:

- `logs/tikv-codegen-shared-call-checkpoint.log`: codegen compiled; **20 passed, 0 failed, 0 ignored, 0 filtered**. Since that run, only documentation text changed in the macro source.
- `logs/tikv-expr-local-first-checkpoint.log`: expression lib-test compile **failed**, with six E0282/E0283 diagnostics from two `ok_or_else(|| box_err!(...))` inference sites in `impl_time.rs`. No expression tests ran in that attempt. Parent then explicitly cleared C to edit those owned files with no Cargo process active.
- Both time sites now use `let Some(unit_bytes) = ... else { return Err(box_err!(...)); }`, preserving the legacy error constructor while making its target Result type explicit. Queued macro doc, malformed CAST wording, pre-descriptor budget and invalid-literal cloning hardening are also applied; the added resource/literal regression brings the local test count to ten.
- Pinned formatter and scoped `git diff --check` passed again on exactly the five touched Rust files. No C edit occurred in lib.rs/types/mod.rs after handback.
- Parent's correct-cwd native retry, `logs/tikv-expr-local-first-checkpoint-v3.log`, compiled and ran the **entire expression lib: 438 passed, 0 failed, 0 ignored, 0 measured, 0 filtered**, test time 2.49 seconds. C directly read the log: all ten `local::tests::*` entries passed at lines 389–398; final receipt is line 450. Parent confirmed process exit 0. This establishes the C1 compile/test gate, not full strict controls, caller integration, general value compatibility or throughput. C has still run no build/test itself.

Remaining releases: strict controls with the iterative official-RPN demand driver (including >32 semantics), deferred regexp metadata under strict admission, raw-varg full-shape guarantees before broader admission, demand input/runtime services and staged HostCall, config/statement-local cache/diagnostic contracts and their tests, more value domains, parent-driven integration/readiness/benchmark evidence. This checkpoint alone establishes none of those acceptance criteria. **C hands lib.rs/types/mod.rs back to the parent at this delivered checkpoint**, with no pending C edits in either file; further C changes there require renewed coordination. Other granted C source ownership remains for follow-up diagnostics. The parent owns maintenance-guide updates; C was expressly prohibited from editing guides. After the C1 gate, C read the parent's updated `doc/maintenance-guides/src/coprocessor.md:132–160,195–206`, which records the shared preparation, worker ownership, signed-Int boundary and absence of strict controls/HostCall/demand import.

## C2-proposal-r1 — concrete next handoff from frozen C-r1

**Permission/status:** proposal only. Parent has ordered a product freeze during the caller TiDB build. C has made no C2 product edit or build. This section records implementable next interfaces, deletion points and required locks; it does not authorize them or replace the parent's plan. An independent read-only audit checked the current frame/service seams and the lifecycle below; no C2 tests were run.

### 1. Recommended release boundary

Recommend two internally coherent checkpoints under a later explicit C2 release:

1. **C2a:** replace the official RPN short-circuit callback recursion with one iterative frame driver; retain the wire entry points/policy; add signed-Int strict AND/OR/IF/IFNULL/CASE/COALESCE and demand `read_input` on its ColumnRef seam.
2. **C2b:** add registered, typed, singleton staged HostCall on that **same driver**, with Fresh/Reuse, task cleanup and diagnostic-order tests. A mock BENCHMARK-like host and AES-like unused-argument fixture exercise the protocol; they do not establish migration of native BENCHMARK/AES or other SQL/session functions.

Keep exact signed LongLong representations and width-one local scheduling in both. More value domains, REGEXP admission/metadata deferral and complete native host migration are separate gates. The earlier broader facade's HostRows/subset protocol is deliberately narrowed to one occurrence here; C2 must not advertise vector host batching or general strict SQL readiness. Existing `compile_local`, `LocalCompileContext { limits }`, and decoded `LocalProgram::eval` remain source-compatible.

### 2. Exact core factoring and deletion points

Current source anchors, **frozen C-r1 rather than original baseline**:

- `types/expr_eval.rs:259–306` is a flat evaluator, and `:337–359` calls `ShortCircuitFnMeta.fn_ptr`.
- `impl_op.rs:250–365` still owns a recursive child loop; `:280` calls `arg.eval_decoded`. Its `ScLogicalOp`/merge helpers at `:75–248` encode the existing three-valued/Enum rules and must be reused/moved, not independently reimplemented.
- `local/compile.rs` emits all ordinary Call arguments into a single postfix buffer. A control or HostCall **cannot** take that eager emission path.
- `local/batch.rs` statically charges `expression.len() * selected_rows`; that cannot meter nested demand/Fresh work.
- `types/function.rs` leaves `RpnFnCallExtra` with only return FieldType. Keep it that way for C2: no mutable session/service in metadata, TLS, raw pointers or generated vararg kernels.

Concrete edit checklist:

1. In `types/function.rs`, add a closed internal `ControlKind::{And, Or, If, IfNull, CaseWhen, Coalesce}` and an optional private control tag on `RpnFnMeta`, defaulting to None in **all** codegen constructors. Add a crate-private `with_control(kind)` and `PreparedCall::control_kind()` / `into_control(args)` assembly seam. Annotate the **existing canonical selector arms** in lib.rs, rather than create another signature-to-kernel table. Generated validators/metadata still run through shared preparation before child assembly. The selected control families currently have unit metadata; make that invariant explicit when consuming preparation rather than silently throwing away unknown metadata.
2. Change `ShortCircuitFnMeta` from recursive callback metadata to `{ real sig, internal kind }`; remove its fn_ptr use. The real TiPb signature remains available for legacy flattening/debug/work accounting. The old AND/OR-only `map_function_to_sc_func` remains a LegacyWire admission adapter, not a parallel registry for the other controls. Wire behavior remains flag + worthwhile heuristic + depth <=32, with its current eager fallback. Local strict selection uses the selected control tag and its checked local capability allowlist, independent of the wire flag/heuristic; exceeding a local resource bound returns ResourceLimit, never an eager substitute.
3. In `types/expr_eval.rs`, replace the evaluator's ShortCircuit dispatch with `eval_frames` and private frame continuations such as `EvalFrame::{Program, Control, Host, AwaitChild}`. The public `RpnExpression::eval` / `eval_decoded` wrappers enter this same driver. Factor the ordinary FnCall application at current `:365–388` into one helper used by the driver/fast path, retaining its generated function pointer, typed args, return FT and Any+Send metadata. Do **not** add an AST interpreter or independent loop in `local/`.
4. Turn existing public `impl_op::sc_logical_and/or` into wrappers entering that same driver at a control frame. Delete the recursive `eval_logical_short_circuit` argument-evaluation loop. Retain/move its truth normalization, pending-row compaction and merge helpers. Existing eager logical/control/coalesce kernels stay for wire fallback; new strict branch scheduling belongs in the official frame path, not in a host callback or synthetic eager placeholder argument list.
5. Extend `local/compile.rs`'s iterative build steps with `FinishControl` and `FinishHost`, using transient output buffers to compile each child into its own RpnExpression. Ordinary eager children still emit postfix nodes; control/host children remain unevaluated subprograms. This is only compilation storage, not a second runtime graph. Compile all branches structurally and check complete signatures before evaluation, without reading bound values or invoking hosts.
6. In `types/expr.rs`, add official `HostCall { prepared: PreparedHostCall, args: Box<[RpnExpression]> }`, and cover it in type/metadata/test-helper matches. `PreparedHostCall` is an opaque typed preparation product with private fields/constructor; it stores a registered signature/catalog identity, not session references or arbitrary caller Any. If its name must be public because the existing RPN enum is public, expose the opaque name only. Wire protobuf construction never produces a HostCall.

Control continuation behavior is concrete: AND stops on zero, OR stops on nonzero, and a prior NULL does not resolve a row if a later absorbing value can still decide it; IF evaluates condition then exactly one branch (NULL is false); IFNULL evaluates lhs then rhs only for NULL; CASE advances condition/value pairs in order and evaluates the chosen value or optional ELSE only; COALESCE stops at the first non-NULL argument. Empty/missing-result cases follow the shared signature admission and produce the declared typed NULL where valid. C2 signed local NULLIF remains the existing comparator-backed eager kernel with one evaluation of lhs; do not invent new NULLIF demand semantics in this checkpoint.

### 3. Value and frame lifetime contract

Private driver input mode is `InputMode::{Decoded(&LazyBatchColumnVec), Bindings}`. The only ColumnRef dispatch either loads the decoded input or calls the one service owner. Binding mode must not call `ensure_columns_decoded` or pre-import native values. Legacy `eval` may retain its existing eager decode prepass; that is not the strict local entry.

Preserve root decoded borrow behavior: current `expr_eval` tests around `:888–943` expect a Ref over the **unsliced physical vector** plus the caller's logical rows `[2,0,1]`. Keep a root fast path borrowing only caller-owned rows/columns. A child whose selection map belongs to a frame must gather to an owned selected VectorValue before suspension/merge; never keep `RpnStackNodeVectorValue::Ref` borrowing that same frame's movable Vec. Scalar program constants may remain borrowed during a primitive step, but materialize with `VectorValue::from_scalar(value, 1)` at a service reply boundary. No new datatype container or Copy requirement is needed.

Also preserve the low-level legacy constant-only case `output_rows > 0` with an empty logical-row slice (existing test near `:641`). The checked local facade instead always has an explicit physical universe and selection. Do not conflate these contracts while changing the driver.

Frame/stack borrows are invocation-local. `ExecutionLimits` retains no capacities, counters, scratch, or prior program/input borrow; frame and row scratch are invocation-local. Do not claim reusable lifetime-bearing frame storage merely by adding it as a field. Fresh execution pushes a new child program frame at pc=0 on the **same** driver, not a recursive public eval call. All mutable vararg TLS borrows have ended before the driver calls a service or pushes another child.

### 4. Proposed additive public service API

Place the protocol/catalog definitions in new `local/host.rs`; it contains **no evaluator**. Re-export only the public protocol types from local/mod.rs. Opaque IDs/catalog construction must be checked; the immutable catalog has exact argument/result FieldTypes for every explicitly registered HostSlot and a stable identity shared by worker clones. A catalog key is identity/version, not a hash-only guess based on return type; no name fallback or callback/Any is stored in it.

```rust
// New spec variant; all existing variants stay unchanged.
LocalExpr::HostCall {
    slot: HostSlot,
    args: Box<[LocalExpr]>,
    return_type: FieldType,
}

compile_local_with_hosts(
    spec: &LocalExpr, schema: &[FieldType], cx: LocalCompileContext,
    hosts: &HostCatalog,
) -> LocalResult<LocalProgram>
// compile_local delegates to this common compiler with an empty catalog.

LocalProgram::eval_with_bindings(
    &mut self, limits: ExecutionLimits, ctx: &mut EvalContext,
    physical_rows: usize, selection: &[usize],
    services: &mut dyn LocalRuntimeServices,
) -> LocalResult<VectorValue>

ExecutionLimits {
    max_steps: u64,
    max_frame_depth: usize,
    max_active_tasks: usize,
    max_retained_bytes: usize,
}
ExecutionLimits // passed by value to each public evaluation
// Existing new(max_steps) remains; default additional cap values are a
// parent release choice, not a measured production recommendation here.

HostRow { occurrence: OccurrenceId, input_row: usize }
HostInvocation<'a> {
    slot: HostSlot, row: HostRow,
    arg_types: &'a [FieldType], return_type: &'a FieldType,
}
ArgMode::{Reuse, Fresh}
HostArgRequest { index: usize, mode: ArgMode }
HostArgReply<'a> { index: usize, field_type: &'a FieldType, values: &'a VectorValue }
HostStart::{Ready(VectorValue), Pending { task: HostTaskId, request: HostArgRequest }}
HostStep::{NeedArg(HostArgRequest), Ready(VectorValue)}

trait LocalRuntimeServices {
    fn binding_schema(&self) -> &[FieldType];
    fn host_catalog_key(&self) -> &HostCatalogKey;
    fn read_input(&mut self, ctx: &mut EvalContext, slot: usize,
        row: HostRow, expected: &FieldType) -> LocalResult<VectorValue>;
    fn start(&mut self, ctx: &mut EvalContext,
        invocation: HostInvocation<'_>) -> LocalResult<HostStart>;
    fn resume(&mut self, ctx: &mut EvalContext, task: &HostTaskId,
        reply: HostArgReply<'_>) -> LocalResult<HostStep>;
    fn cancel(&mut self, task: &HostTaskId); // no error, panic or diagnostics
}
```

For this C2 ABI every invocation/read/reply is **one occurrence**. NeedArg cannot supply a row list; its request necessarily uses the invocation's singleton. A future wider ABI must separately define ordered subset/repetition validation, not reinterpret these fields. `HostTaskId` is an adapter-owned worker-local generational token containing no borrowed session/value reference. Start/resume never receive RPN/native Expr, evaluator handles or child-evaluation closures. `read_input` only retrieves/converts a bound value; it must not evaluate arbitrary SQL. The host adapter may own registered primitive/session-operation state, but must not hide native `Expr::eval`/`eval_in`, a whole-expression closure, or a function-name fallback behind a slot. Lazy native builtins need staged primitive decomposition, not callbacks that recursively call the interpreter while its mutable session is borrowed. C2 slots remain dynamic facts, not pretend constants for metadata selection.

Existing decoded eval enters the same driver with decoded inputs and no host services. Programs requiring a catalog must validate that services are supplied before effects; the bindings entry validates schema/catalog identity and all row bounds without reading values. Same-type rebinding between invocations is permitted. Host type validation uses complete declared FieldTypes at compile/preflight and exact physical EvalType at value boundaries; it must not inherit LegacyWire Enum-as-Int/Bytes allowances. Invalid binding/host responses should have separate `LocalError::{BindingContract, HostContract}` variants; SQL `Evaluation(original_error)` is propagated intact. Legacy wrappers preserve existing SQL errors, not stringify them through the local error wrapper.

The caller must split mutable ownership: a diagnostics/EvalContext field and a runtime-primitives/input adapter borrow **disjoint** session fields. Passing `&mut whole_session` as services alongside `&mut whole_session.ctx` is not an implementation strategy. One service owner handles both inputs and host primitives; do not create two overlapping mutable provider/session borrows. Host diagnostics append immediately to the same external ctx. No C2 semantic-profile/Decimal policy expansion is proposed here.

### 5. Frozen task lifecycle and Fresh/Reuse semantics

| Transition | Required ownership and error behavior |
| --- | --- |
| Before `start` | Driver validates registered invocation/type facts and reserves its task-ledger/cache capacity before invoking the callback. |
| `start` Err or immediate Ready | Adapter retains **no** task resources; an adapter-side RAII guard cleans partial start on Err or unwind before a Pending token is returned. Ready must have exactly one correctly typed value. |
| `start` Pending | Driver registers the live token **before** validating the returned arg request. Reject duplicate live token identities. Out-of-range requests trigger cleanup, not indexing/panic. |
| `resume` NeedArg | Same token stays live; resume cannot silently replace it. Schedule or reuse the requested child through the frame driver. |
| Child error / resume Err | Abort the expression; do not resume with an error-valued argument or let a host silently catch/replace the child error. |
| `resume` Ready | Adapter releases terminal task state before returning. Driver validates the result and then disarms its token; a malformed Ready also receives idempotent cancel. |
| Any failure/refusal/unwind | Invocation guard cancels all live tokens innermost-first, then drops frames/cached values. Cleanup ignores exhausted work budgets and adds no SQL warning/error. No task/input borrow survives public eval return. |

`cancel` must be idempotent, infallible and non-panicking; it neither evaluates children nor mutates SQL diagnostics. The guard owns the single mutable service reference and live-token ledger, enabling safe unwind cleanup without mutable reentrancy. This is not rollback of host side effects, and does not promise recovery from allocator abort or panic=abort. The interface is synchronous staged **demand**, not pending asynchronous I/O; callbacks must be bounded/cooperatively cancellable themselves.

Cache exactly the last successful owned one-row result for each `(live host task, arg index)`. Reuse hit performs no child read/effect/diagnostic; Reuse miss evaluates once. Fresh discards the old result and restarts that child at pc=0 with fresh descendant tasks and no implicit input-result memo; success becomes the new retained value, failure aborts rather than returning the old cache. Cache lifetime is the host invocation only, not physical row, node ID, program or statement. Ordinary RPN constants remain immutable values; a Fresh request for a constant still completes a demand and participates in the work limit.

For BENCHMARK-like protocol tests: request count once with Reuse, then body Fresh exactly N times; zero/NULL/negative count never demands the body. For selection `[2,0,2]`, occurrences are `[0,1,2]` and physical rows `[2,0,2]`; Fresh repeats retain the same occurrence, and nested task IDs distinguish invocations. Generated child vectors are addressed by local index 0, never physical row 2. Change local batch iteration to enumerate occurrences rather than track only `for &row`.

### 6. Validation, resource and deep-stack obligations

Validate every read_input, child reply and Ready result **before** calling a parent typed loader or resume: exact admitted EvalType and len==1. Materialize scalar constants explicitly and gather selected references. Normalize retained Int replies to canonical singleton storage instead of retaining arbitrarily oversized provider capacity. A malformed host result is discovered after that callback, so preserve its already-produced diagnostic prefix; do not claim it was a pre-execution input failure.

Replace the static-only C1 work charge with one per-invocation counter shared across root rows and every nested frame. Charge actual node entries, child scheduling/merge and service start/resume/read transitions, including Reuse hits; an infinite host Reuse loop cannot be free. Pass the complete immutable `ExecutionLimits` value into each public evaluation; create one fresh budget shared across all rows and nested frames in that invocation. Bound frame depth, active tasks, retained replies/output and total work before allocation/callback where possible. All refusal paths use the same cleanup guard. Adapter-owned opaque task memory must also be metered by the adapter; the RPN byte budget alone cannot bound it.

Deep safety is a larger atomic change than deleting the evaluator recursion:

- Make `types/expr.rs`'s metadata visitor (`collect_metadata` at current `:95–105,189–192`) iterative and include HostCall children. `args.len()-1` work units apply only to flattened AND/OR; new controls/hosts count their own operation plus child work.
- Give RpnExpression iterative drain/drop for nested ShortCircuit/Host children, including partial-build/error cleanup. Once Drop exists, `into_inner(self)` must become `into_inner(mut self)` using `mem::take(&mut self.nodes)`; moving a field out of a Drop type will not compile. Empty each child node vector before dropping its expression owner.
- LocalExpr also owns recursive boxed children; rejected over-depth input teardown can overflow independently of evaluator safety. Add iterative spec teardown. Derived Clone/Debug are still recursive unless separately changed: use Arc::clone for sharing and explicitly limit/exclude deep clone/debug from the safety claim, or implement bounded/iterative versions before claiming them.
- Existing wire `check_expr_tree_supported`, recursive builder and flatten traversal remain separate legacy constraints. Strict local construction must never route through them. If reusing a flatten helper, make that helper iterative first. Preserving wire policy does not establish unlimited wire parsing/construction depth safety.

### 7. Requested file locks and atomic edits

These are **requests for the next release**, not current permission. Paths below are under `expression-unification/tikv/`.

| Lock | Exact paths and purpose |
| --- | --- |
| **New product lock** | `components/tidb_query_expr/src/impl_op.rs`: remove recursive short-circuit loop; preserve/reuse logical merge rules; public wrappers enter common driver. |
| **Reacquire from parent** | `components/tidb_query_expr/src/lib.rs`: attach control tags at existing selector arms; adapt old wire-only sc selector. `components/tidb_query_expr/src/types/mod.rs`: minimal internal/public exports. C handed both back after C1; no new write occurs without renewed explicit handoff. |
| **Re-release existing C scope** | `components/tidb_query_expr/src/types/{function,expr,expr_eval,expr_builder}.rs`: control/prepared metadata, official Host node, common driver, iterative metadata/drop and legacy assembly adaptation. |
| **Local facade/protocol** | Existing `components/tidb_query_expr/src/local/{spec,compile,batch,registry,mod,tests}.rs`; new `local/host.rs`. If tests are split, new `local/{control_tests,host_tests}.rs` remain under the same released local/** owner. |
| **Codegen** | `components/tidb_query_codegen/src/rpn_function.rs` including its inline expected constructors: default control tag None. No `codegen/src/lib.rs` change. |
| **Conditional same-owner helper** | Existing C-owned `components/tidb_query_expr/src/impl_control.rs` only if needed to factor truth/branch helpers. Existing cfg(test) `types/test_util.rs` only for direct constructor/test adaptation. Coalesce eager kernels in impl_compare.rs need no algorithm edit merely to tag their registry arms. |
| **Parent stays writer** | `doc/maintenance-guides/src/coprocessor.md` next semantic/ownership update, main plan, all Cargo/locks, shared exports outside the renewed handoff. No datatype/impl_like/TiDB/executor lock is requested for this narrow C2. |

Do not request broad impl_* ownership for service protocol work. `impl_regexp.rs` is not needed for this Int-only release; it and raw-varg shape/metadata deferral require a separately scoped admission gate. Existing trusted implicit-cast constructor remains outside the newly checked local entry, as documented for C1.

### 8. Acceptance matrix and parent actions

Before C2 acceptance, parent should serialize native codegen and full expression-lib tests from the TiKV root with the pinned tools/cargo-tikv helper, preserving all **438** C1 tests and adding these explicit cases:

- All 3VL combinations for AND/OR, IF NULL condition, IFNULL, ordered CASE with/without ELSE, and COALESCE; the untaken expression uses a poison **binding**, not eager pre-imported data. No wire flag is needed for strict local; the existing wire >32 fallback test still passes unchanged.
- Empty selection issues no read/start/resume; scalar/column/control/host output for 0/1/1024/1025 total rows; `[2,0,2]` has distinct occurrence traces in source order. Invalid schema/catalog/selection fails before effects.
- Reachable poison binding fails at the demanded position; dead binding stays unread. Wrong-type/length input/Ready and bad argument index are contract errors before parent loaders/resume, never panic or fallback. Constant argument replies work.
- BENCHMARK-like 0/NULL/negative/N demands; Reuse does not repeat child effects, Fresh does; a fresh child containing another host gets new task state. Host -> ordinary pure RPN -> Host demonstrates no recursive service borrow or TLS guard crossing.
- Child/kernel/binding/host failure and work/frame/task/cache refusal cancel all live tasks once effectively, innermost-first, preserving the diagnostic prefix. Include start Err cleanup, malformed immediate/terminal Ready, duplicate live token and repeated cancellation/idempotence cases.
- Compile/eval/metadata-walk/drop and rejected/partial construction at depths 33/64/256 under a controlled small-stack test; no eager substitution. Independently compiled Any+Send programs over shared immutable specs remain worker-isolated. Do not use unbounded derived Debug/Clone inside the deep-stack test itself.

First parent action is to approve/revise this bounded C2 scope and reacquire/reassign the listed locks after the caller build. Then implement C2a and obtain a real compile/test checkpoint before layering C2b; do not call either complete from a static review. D must supply a disjoint-borrow service adapter and registered host signatures; native SQL-host migration/diagnostic profiles remain its own approved follow-up. No C2 source, build, benchmark or pass count was claimed at that proposal-only handoff. The subsequently released C2a receipt follows.

## C2a-native-r2 — released implementation, native gate accepted

**Release/amendment:** after the caller and post-prune consumer 61-test gates, the parent released C2a only, not HostCall/C2b. The parent amended the proposed public RpnFnMeta tag: the actual implementation uses **private `SelectedCall`/`PreparedCall`**, leaving RpnFnMeta and codegen unchanged. C reacquired lib.rs/types/mod.rs and received impl_op.rs. No Cargo/lock, datatype, impl_cast, impl_math, guide or main-plan write was made by C. A later explicit coordination request allowed the single existing Decimal unary negation in impl_op.rs to become `-val.clone()` for B/E's non-Copy work; no Decimal algorithm or admission was added.

### Actual factoring and scope

- `lib.rs::select_call/select_tipb_call/select_expr_node` return the private descriptor. The existing canonical selector arms attach control tags in that single match; the old meta-only wrappers remain adapters. There is no second control signature lookup. `prepare_selected_call` is shared by the wire builder and `prepare_call`; it validates before metadata construction, revalidates the retained shape, and checks that tagged controls have **unit metadata and unchanged source-order arguments**.
- `PreparedCall::short_circuit_meta/into_control` consumes only checked preparation. ShortCircuitFnMeta retains the real TiPb signature plus an internal ControlKind rather than a recursive fn_ptr. **Removed:** `map_expr_node_to_sc_func`, `map_function_to_sc_func`, `prepare_call_with_meta`, and `impl_op::eval_logical_short_circuit`. The old `sc_logical_and/or` wrappers enter the common driver. Existing logical normalization/NULL/Enum merge rules are reused through `LogicalAccumulator`, not rewritten.
- `types/expr_eval.rs::eval_frames` is the official Program/Control continuation loop. Legacy eval/eval_decoded and the checked local facade use it and the same `eval_one_node` primitive helper. Root caller-owned decoded Ref results remain borrowed; a frame-owned selection is gathered before returning a child. The legacy constant-with-output-rows-but-empty-logical-map case remains supported. No second local evaluator, fake protobuf Expr/ID, host callback recursion, or service capture in function metadata/TLS was introduced.
- `local/compile.rs` uses `FinishControl` with separate child RpnExpressions; ordinary Calls keep their original eager postfix emission. Construction charges scheduled descendants before descriptor/buffer allocation, in addition to depth/visited-node bounds. Local strict controls do not consult the wire flag or depth-32 heuristic; wire construction still uses its old AND/OR-only flag/heuristic/depth-32 admission and eager fallback.
- New `local/runtime.rs` contains only the service protocol and limits/accounting, **not** an evaluator. Binding mode reaches `read_input` only at a demanded ColumnRef, validates exact Int/len1 before loaders, and normalizes returned storage to a singleton. Schema/selection checks precede value effects. Width-one occurrence order is retained, including repeated physical rows.
- Metadata traversal, RpnExpression destruction/into_inner and LocalExpr destruction are iterative, including rejected/partial construction. Deep derived Clone/Debug are explicitly **not** guaranteed; share specifications with Arc::clone. Legacy protobuf checking/construction/flattening retain their separate depth constraints.

**Critical origin/profile boundary from D's audit:** ordinary Plus and other ordinary calls remain eager, including importing RHS when LHS is NULL. Scalar TiDB/PB numeric NULL-stop behavior differs from the original eager batch profile. C2a intentionally does **not** fix this with blanket NULL propagation or another interpreter. D1's first seed must be leaf/control dependency-closed; later ordinary-call demand needs an approved origin/profile contract on this same driver. Local NULLIF demand semantics are unchanged. Thus this checkpoint is **strict integer controls**, not general strict SQL evaluation.

### Frozen caller API (C2a only)

All names below are re-exported from `tidb_query_expr::local`; `FunctionRef`, `LocalFunctionId`, `LiteralKind`, `CallMetadata`, the existing LocalExpr three variants, `compile_local`, `LocalCompileContext { limits }`, `CompileLimits` and decoded `LocalProgram::eval` remain available.

```rust
InputRow { pub occurrence: usize, pub input_row: usize } // Copy + Eq
ExecutionLimits {
    pub max_steps: u64,
    pub max_frame_depth: usize,
    pub max_active_tasks: usize, // reserved; C2a has no tasks
    pub max_retained_bytes: usize,
}
trait LocalRuntimeServices {
    fn binding_schema(&self) -> &[tipb::FieldType]; // pure/stable during eval
    fn read_input(&mut self, ctx: &mut EvalContext, slot: usize,
        row: InputRow, expected: &tipb::FieldType) -> LocalResult<VectorValue>;
}
LocalProgram::eval_with_bindings(
    &mut self, limits: ExecutionLimits, ctx: &mut EvalContext,
    physical_rows: usize, selection: &[usize],
    services: &mut dyn LocalRuntimeServices,
) -> LocalResult<VectorValue>
ExecutionLimits // passed by value to each public evaluation
// new(max_steps) and Default remain.
LocalError::{InvalidSpec(String), InvalidBatch(String), BindingContract(String),
             ResourceLimit(String), Evaluation(tidb_query_common::Error)}
```

`VectorValue` is `tidb_query_datatype::codec::data_type::VectorValue`; `EvalContext` is `tidb_query_datatype::expr::EvalContext`. The public row name is **InputRow**, not BindingRow/HostRow. There is no HostCall spec/node, catalog, compile_local_with_hosts, or start/resume/cancel API in this release. No further C2a facade ABI change is planned. Parent received this frozen API while D1 source work could proceed independently. The native C2a gate has now passed; D1 caller integration/activation is not implied and remains separately gated.

The common driver returns LocalResult. `read_input` errors pass through unchanged, including their actual LocalError variant; only legacy wrappers convert non-SQL structural errors and directly forward `Evaluation(original_common_error)`. This does not solve D's native typed EvalError carrier problem or authorize recovering native error identity from text.

### Limits and validation facts

Parent-selected **Demo defaults** are max_steps=u64::MAX (or the explicit new(max_steps)), max_frame_depth=1024, max_active_tasks=256, max_retained_bytes=64 MiB. Task capacity is reserved and inert because C2a creates no host tasks. Work is per invocation across all occurrences: node entries, control child scheduling/merge and demanded reads consume the same counter. Limits return ResourceLimit, not an alternate eager execution path or an external-cancellation guarantee.

Retained accounting conservatively covers output/Int data and nullable bitmap storage, frame/operand-stack capacities and owned row maps; immutable program/input storage and allocator bookkeeping are excluded. Provider-owned memory during the callback is not bounded by an RPN retained-storage cap. Saturated accounting/checked-add overflow is a refusal. The primitive result is allowed for while arguments remain live, and returned-control operand-stack growth is charged before allocation/next effect. No allocator-abort recovery or production/performance claim is made.

Static review caught and C corrected (1) an uncharged stack slot on a returned empty CASE result, including stale accounting before the next import; and (2) loss of the old allocation-free leaf path/repeated operand-stack reallocations. Two new private regression tests cover (1). Root noncontrol singleton evaluation again uses the shared primitive helper without frame/operand-stack allocation, nested noncontrol leaves do not allocate an operand stack, and legacy multi-node stacks reserve once per program (checked local growth is geometric). The reviewer re-read those changes and found no additional concrete issue, **not** compile proof.

### Changed source and test receipt

C2a touched these 15 Rust files under `components/tidb_query_expr/src/`: `lib.rs`, `impl_op.rs`, `types/{function,expr,expr_builder,expr_eval,mod}.rs`, and `local/{mod,spec,compile,batch,registry,tests,runtime,control_tests}.rs`. The last two are new. `types/test_util.rs`, `impl_control.rs` and codegen needed no C2a write. Existing C1 assertions were retained except the obsolete unsupported-domain fixture: valid LogicalAnd was replaced by unadmitted LogicalXor, non-Int IfReal remains unadmitted, and malformed IfInt arity is tested separately.

**24 new tests added; all passed under the parent's native v2 run (none run by C):**

- Thirteen `local::control_tests`: all NULL/zero/nonzero/negative AND/OR combinations and skipped RHS; IF/IFNULL/ordered CASE/ELSE/empty CASE/COALESCE poison branches; 0/1/1024/1025 and `[2,0,2]` occurrence/rebinding; schema/selection/type/length validation; original service error variants; explicitly eager ordinary NULL behavior; earlier-row parent overflow before later imports; malformed-return warning prefix; bad control arity; explicit limits/defaults and small refusals; shared work budget/state reuse; 33/64/256 nested strict controls on a 256 KiB thread stack; decoded facade using the same driver.
- Two `types::expr_eval` resource regressions: empty CASE's return stack is charged before success, and before the following ordinary-call input effect.
- Three `types::expr` tests: iterative metadata/work counting/deep teardown/into_inner, including exact-once opaque metadata destruction.
- Three `local::spec` tests: valid/rejected deep specification compile/drop and BindingContract display. Deep teardown fixtures extend to 4096/16384 as appropriate without deep Debug/Clone.
- Three `impl_op` tests: retained logical truth tables, compacted/reordered/repeated output-position merge, and retained-capacity accounting.

Pinned nightly rustfmt (`--edition 2021 --config skip_children=true`) on exactly these 15 files and scoped `git diff --check` passed with process exit 0. Source searches found no remaining old sc mapper, prepare_call_with_meta, recursive logical traversal, or eval_decoded call in impl_op. **No Cargo, native build, test or benchmark was run by C.** The parent's actual native sequence supersedes the earlier pending-build status:

1. Parent reported native datatype **330/330** and codegen **20/20** green before the expression retry. The first expression attempt, `logs/tikv-c2a-expr-full.log`, reported eight Decimal non-Copy diagnostics and no frame-driver/compiler diagnostic. C read that full log and, under the exact diagnostic grant, changed only `impl_op.rs`'s two Decimal test fixture `.push_param(arg)` calls (UnaryNotDecimal/UnaryMinusDecimal) to `.push_param(arg.clone())`, retaining diagnostic args/assertions. E owned the other three files. Scoped formatting/diff checks passed; there was no runtime/control change.
2. C directly read the compile header, new-test entries and final summary of `logs/tikv-c2a-expr-full-v2.log`: **462 passed, 0 failed, 0 ignored, 0 measured, 0 filtered**, 2.64 seconds (summary line 471). All **438 retained + 24 new** tests executed. The 13 control/service tests, metadata/drop/helper tests and both resource regressions are explicitly `ok`; legacy Ref, constant/no-logical-rows, flatten/partial-compaction and short-circuit warning tests also passed.
3. C read `logs/tikv-b21-aggr-full.log`: **40 passed, 0 failed, 0 ignored, 0 measured, 0 filtered**, 0.00 seconds (summary line 59). This consumer run emitted the existing non-test unused `Expr` import warning in impl_compare_in.rs and the nom future-incompatibility warning; neither is a test failure, and C made no out-of-scope warning cleanup.
4. Parent explicitly accepted **native C2a**, not general SQL strictness, broader domains, C2b, caller activation or performance. Source is frozen while the separate D1 caller gate runs.

C handed lib.rs/types/mod.rs back at the source checkpoint; the parent reclaimed both. All C product files are now frozen. The parent remains guide/main-plan owner. Pending gates: D1 caller integration without activation, approved ordinary-call origin/null-stop semantics, C2b staged hosts/task lifecycle/external-cancel discussion, broader domains/REGEXP metadata deferral and general readiness/benchmark evidence.

### C2b compatibility amendment proposed after the native gate (no code)

Parent requested additive no-host defaults so the existing D1 implementation of the two-method LocalRuntimeServices trait remains source-compatible. The earlier C2 proposal made catalog/start/resume/cancel mandatory on that trait; do **not** apply that breaking shape directly.

**Subsequently approved amendment:** add just one optional host-capability reborrow to LocalRuntimeServices, with a default returning None:

```rust
fn host_services(&mut self) -> Option<&mut dyn LocalHostServices> { None }

// New trait, implemented only by an adapter explicitly opting into hosts.
trait LocalHostServices {
    fn catalog_key(&self) -> &HostCatalogKey;
    fn start(&mut self, ctx: &mut EvalContext,
        invocation: HostInvocation<'_>) -> LocalResult<HostStart>;
    fn resume(&mut self, ctx: &mut EvalContext, task: &HostTaskId,
        reply: HostArgReply<'_>) -> LocalResult<HostStep>;
    fn cancel(&mut self, task: &HostTaskId);
}
```

The host-capable adapter can implement both traits and return Some(self), or return a disjoint internal primitive-service field. The driver/cleanup guard still owns **one** outer mutable LocalRuntimeServices reference. It reborrows the host view for one callback and releases it before driving any argument/input; it never holds overlapping mutable input/session providers. View presence and catalog identity must be stable throughout an invocation, including cleanup after an error/unwind; obtaining the view must be side-effect-free and non-panicking. A host-free program does not call the hook at all. A program requiring hosts must reject a missing/mismatched capability before value effects.

Reason for recommending a separate opt-in trait rather than four independent defaults: silently inheriting a no-op cancel after overriding start to return Pending would allow accidental task-state retention. Existing input-only D1 adapters should inherit no-host support, while adapters advertising staged tasks must explicitly implement the infallible/idempotent cancellation contract. This does not protect against an adapter that deliberately violates its contract; source/lifecycle tests still apply.

Other proposed compatibility details: reuse frozen **InputRow** in HostInvocation rather than introduce a second HostRow/OccurrenceId facade; add compile_local_with_hosts without changing compile_local or LocalCompileContext struct literals; add LocalError::HostContract only at the host release. Keep the existing same-driver singleton Fresh/Reuse, generational-token, pre-reserved ledger/cache and innermost-first cleanup semantics. The task limit becomes active only when C2b actually creates tasks. An external cancellation signal/hook is **not** smuggled into resource budgets and remains a separate contract decision. No product change had been made at the proposal handoff. Parent then reported D1 15 tests green and full DB expression 1245 passed / four unchanged baseline failures / 93 ignored, and explicitly released scoped C2b. Implementation/native acceptance remains pending. Root lib.rs, codegen/RpnFnMeta, datatype/Cargo and guides stay parent-owned; only local/**, the listed C types files and optional types/mod.rs loan are released. Cleanup guarantees apply only to a contract-compliant stable provider: cancellation cannot be promised after the provider disappears, changes catalog or panics. No actual SQL host migration or ordinary-call null-stop extension is included.

## C2b-source-r1 — coherent staged-host source, first native gate pending

The parent released this scope after the D1/caller gates, approved the optional host-view amendment, and required a source-only coherent checkpoint. This receipt records implementation, **not a native pass**. No Cargo/build/test/benchmark was run by C or its scoped children. Native C2a's 462/462 result is historical evidence for the preceding source only. B's later datatype work, D2 and E's caller RED/fix cohort are independently parent-coordinated; C did not build over their moving sources.

### Actual interface and construction

- New `local/host.rs` contains **protocol/catalog validation only**, not an evaluator. `HostCatalog::new(Vec<HostSignature>) -> LocalResult<_>` admits exact signed LongLong signatures; `HostSignature { arg_types: Box<[FieldType]>, return_type: FieldType }` retains complete types. Catalog identity is a process-local checked monotonic atomic u64, wrapped in a private-field Copy/Eq/Hash `HostCatalogKey`, not a signature hash. Cloning shares immutable `Arc<[HostSignature]>` and preserves identity; separately constructed identical signatures receive different keys. Exhaustion refuses without wraparound. Catalog memory is immutable registration/program storage, not caller task state.
- `HostCatalog::key() -> &HostCatalogKey` and `slot(index) -> LocalResult<HostSlot>` are public. HostSlot has private catalog key/index and is Copy; `index()` is only the adapter's dispatch coordinate. It is not sufficient for runtime compatibility. Public opaque `PreparedHostCall` has a crate-private checked constructor, private catalog/slot fields, and read-only slot/catalog/argument/result getters. No session, closure, native expression or arbitrary metadata is captured.
- Additive `compile_local_with_hosts(spec, schema, cx, &catalog)` uses the **same** iterative local compiler as compile_local. The latter has no host registry and rejects HostCall. `LocalExpr::HostCall { slot, args, return_type }` checks catalog-bound slot and exact full argument/result FieldTypes; node/depth checks precede descriptor/child-buffer allocation. All argument graphs are constructed/validated, even if a host later never requests them. Ordinary Call preparation/retained ordering and eager emission are unchanged. Existing LocalCompileContext struct literals remain valid.
- The **official** `RpnExpressionNode::HostCall { prepared, args }` holds checked metadata and child RpnExpressions. It does not invent a TiPb signature, route through RpnFnMeta, or add an evaluation closure. Metadata counting, result-type lookup and iterative program/spec teardown include host children; a host contributes one work unit plus child work. Any+Send compiled-program isolation and deep Clone/Debug exclusions remain unchanged.
- All new public names are under `tidb_query_expr::local`: HostCatalog/Key/Signature/Slot, PreparedHostCall, HostInvocation, HostTaskId, HostArgRequest/Reply, ArgMode, HostStart/Step and LocalHostServices. `InputRow` is reused. `HostInvocation { slot, row, arg_types, return_type }` and `HostArgReply { index, field_type, values }` borrow only for one synchronous callback. HostTaskId has `slot: usize, generation: u64`; generation zero is invalid, and duplicate **live full identities** are rejected. Different generations in one slot remain distinct.
- LocalRuntimeServices retains its two required input methods and adds only the approved `host_services() -> Option<&mut dyn LocalHostServices> { None }`. LocalHostServices requires catalog_key/start/resume/cancel, with no silent cancellation default. HostStart is Ready or Pending { task, request }; HostStep is NeedArg or Ready; requests carry `{ index, mode: Fresh | Reuse }`. `LocalError::HostContract(String)` is distinct, and callback errors retain their original LocalError variants.

### Same-driver demand and ownership

`types/expr_eval.rs` now has Program/Control/Host continuation frames in the **existing single official loop**. Host nodes are excluded from the allocation-free primitive singleton shortcut and cannot reach eval_one_node's kernel branch. No recursive public eval call or local lazy runner exists. Native implementations may execute only the registered primitive work/protocol; no Expr::eval, evaluation closure or statement interpreter is passed through a service.

A HostFrame owns one argument cache for its live invocation. Reuse miss evaluates once; a hit causes no child read, host start or child diagnostic. Fresh drops the previous cached result, restarts the requested child at pc=0 on this driver and creates fresh descendant tasks; success replaces the cache, failure aborts without returning the old value. SQL NULL is a cacheable successful Int value. Requests, accepts, start and resume transitions share the invocation work counter, including cached Reuse loops. Repeated physical rows retain separate InputRow occurrences, and caches never survive an invocation or enter the immutable `ExecutionLimits` policy.

Binding schema, selection and required catalog identity are checked before value effects (also for an empty selection). A host-free program never calls the optional hook, even when compiled with an otherwise unused catalog. Empty selection creates no frames/read/start/resume. Host-aware execution is singleton signed Int; a decoded-only eval of a program requiring hosts refuses instead of inventing services. Child field type/shape/Int representation is checked before resume; every Ready must be exactly one Int value and is normalized to bounded owned storage. Callback borrows and provider-owned capacity do not become retained RPN values.

The RAII TaskGuard holds **one outer mutable EvalInput/service reference**. Each provider view is reborrowed for one callback and released before any child/input work. The guard is established before frame storage, reserves ledger capacity and the potential task slot **before start**, and arms the raw Pending token before validating generation/request. Generation-zero and malformed-request tokens are therefore known to cleanup. A duplicate live identity is owned once, not canceled twice under invented identities. Resumed Ready remains armed through result validation and the storage postcheck; only then is it retired.

On child/kernel/read/host errors, budget refusal or healthy-provider unwinding, all remaining known tasks are canceled once in reverse start order. cancel returns no error, does not panic and emits no diagnostics; it is idempotent for stale/already-completed tokens. Primary LocalError/panic and prior statement warnings are not replaced/reset. Immediate Ready and start Err expose no token: the adapter must release partial state itself before returning them. Resume must release its task before Ready; subsequent cancellation after a malformed Ready is harmless. Cleanup directly checks an optional matching provider without allocating error strings or canceling an unrelated catalog.

**Guarantee boundary:** host view, catalog and task namespace must remain stable/non-panicking for the invocation, including cleanup. A disappeared, changed-catalog or panicking provider has violated the contract; the driver cannot promise to recover unreachable task resources, and must not cancel another catalog's task. Tests explicitly distinguish this from healthy-provider cleanup. There is no external stop/cancel hook and no recovery claim after allocator/process abort.

### Resource policy and scoped evidence

C2a defaults remain MAX work / 1024 frames / 256 tasks / 64 MiB retained scratch. The task cap now includes a **potential** new task reserved before start; max_active_tasks=0 can conservatively refuse even a host that would return immediate Ready. Ordinary/host-free evaluation remains unaffected by that zero cap. Retained checks include output, frame capacity, operand-stack capacity, owned row maps, task-vector capacity (even after tokens retire), cache slot capacity, cached Int/bitmap payloads and overlapping child-to-cache copies. Immutable program/input/catalog memory, allocator bookkeeping and provider-owned/transient callback allocations are outside that retained-scratch policy. Normalization does not retain arbitrary returned capacity. Adapters must separately bound their opaque task heap and callback duration; these caps are **not** a total host-heap, process-memory, performance or external-cancellation guarantee.

**37 new test functions, authored but not executed by C:** 10 catalog/protocol unit tests; 21 service/integration tests (including compilation of undemanded children, benchmark-like count/Fresh, Reuse miss/hit/NULL, Fresh replacement/failure/new descendants, AES-like skipped poisoned arguments, Host→ordinary RPN→Host, empty/1025/repeated rows, D1 two-method/hook compatibility, callback reborrows, full schema/catalog preflight, malformed/duplicate/zero tokens, same slot/different generations, immediate/resumed malformed Ready, primary errors/warnings, start cleanup, healthy-provider unwind, deliberate provider contract violations, limits and deep relays); three official metadata/drop tests; two local spec/drop/error tests; one private byte-accounting regression with four tight caps. The expected source total is **462 + 37 = 499**, but the parent's actual native count/result is authoritative.

The private byte regression checks refusal before start, during child push, during child result reservation with a live ledger, and before overlapping owned-cache allocation/resume. Static review improved the sensitivity of the child-running case by using `3F + C + 2I` without the token charge, rather than letting a still tighter cap mask missing ledger accounting. Expected callback prefixes and exact-once cleanup were algebraically reviewed. Valid bounded Ready's postcheck is dominated by its earlier live-cache + singleton reservation; malformed Ready tests exercise continued cleanup ownership without inventing an unreachable allocation failure.

Read-only cross-repository matching and core-lifetime/resource audits found no additional file lock or concrete source blocker after these changes. No native compile/test result is inferred from those audits. Existing wire-only recursive test helpers are not used for deep host fixtures. The root borrowed Ref/constant-empty-logical-map paths, wire flag/depth-32 policy, original eager ordinary calls, and no-host D1 interface remain unchanged by design; native preservation is still a pending gate.

### Exact frozen source manifest

C2b changed **nine Rust files** (seven local files and two type files) plus this evidence file. Root lib.rs, RpnFnMeta/codegen, datatype, Cargo/locks, guides, types/function.rs, expr_builder.rs, test_util.rs and types/mod.rs received **no C2b edit**; the optional types/mod.rs loan was unused and is returned. Both new files are local/host.rs and local/host_tests.rs. Pinned nightly rustfmt (`--edition 2021 --config skip_children=true`) over exactly the nine files and scoped `git diff --check` exited 0. No Cargo/build/test/benchmark was run.

Relative paths below are from the TiKV root. SHA256 values were generated after the final formatter; the SHA256 of this **ordered sha256sum manifest text** is `fde1f0f6695a9abb461912fd296144c1fdb92f680004c55eece3baa841455f4d`.

```text
0d414dffb0f9ef6539959386283150da1f0ba4896072cfe3804ae7ab3b042624  components/tidb_query_expr/src/local/batch.rs
50baeff1e590ca6747e0bd2378671158f4e0f95afe7a605da3a4a98808451fc5  components/tidb_query_expr/src/local/compile.rs
1fc6702c878204a80bfdb96632c916ad4a78b723bb225cf0dbc06a017903e160  components/tidb_query_expr/src/local/host.rs
e9cae4e09b854823d9de6cc0b4360b4b8fd7a902e86a5c6664262afb87200545  components/tidb_query_expr/src/local/host_tests.rs
a99f11e3140b53b87683b31d101ff7b879b87c09752a65dfba2f706adc45bfcd  components/tidb_query_expr/src/local/mod.rs
68a4b83eaba5d4a9ca2d5a9dfac3524950f8605adf877ec1fcb94c727245dc70  components/tidb_query_expr/src/local/runtime.rs
1b864fc406295bc425cfff857354fcd4ed3a2f886f3f26fd3f1ac3636122b868  components/tidb_query_expr/src/local/spec.rs
45d26799983a9279ca05a5e05af5a276243ca5d61b12c3b69ceffa25dd1f82c0  components/tidb_query_expr/src/types/expr.rs
e3ff00af06c03dc54b9b9393b3df41c3788656acb15f577f1f69aea5db96197e  components/tidb_query_expr/src/types/expr_eval.rs
```

C stops at this coherent source checkpoint and holds product files only for parent-assigned diagnostic fixes. Parent owns the serialized native/caller gates and guide/main-plan update. No actual SQL host migration, ordinary-call origin/NULL-stop scheduling, broader datatype/REGEXP admission, general readiness or performance claim is added by C2b.

## C2b-native-r1 — actual native acceptance, product remains frozen

Parent subsequently reported native C2b GREEN and reviewed HostCatalog, TaskGuard and the shared-frame lifecycle without a concrete finding. C reread the actual logs: `logs/tikv-c2b-expr-full.log:30,435–455,520,531` records **499 passed, 0 failed/ignored/measured/filtered, 2.63s**; `logs/tikv-c2b-aggr-full.log:38,80` records **40 passed, 0 failed/ignored/measured/filtered**. These are parent-run native results, not C execution or a claim about the later caller cohort. Warnings included B's unused private Decimal workers, the existing aggr unused Expr import and the nom future-compatibility notice. No failure was hidden by a test filter.

All nine C2b product files remain frozen with the source manifest above. The parent separately owns the D2/D1-regression caller cohort and B's next Decimal workers. It subsequently accepted the post-alias-fix caller cohort; C reread `logs/tidb-d1-post-origin-fix.log:1986–2004` (**16/16**, 1335 filtered), `logs/tidb-d2-structural.log:1986–1996` (**8/8**, 1343 filtered), and `logs/tidb-d2-expr-full-comparison.log:3339–3371` (**1254 passed / four failed / 93 ignored**, no filtered tests). Parent reports the four failure names and panic/assertion text exactly match the recorded baseline; this is a no-new-failure comparison, **not an all-green full TiDB suite claim**. Parent also independently matched the ordered nine-file C2b manifest hash above. Product handback is complete, but no C3 product lock is granted. Only this evidence document may change. The independent next C3 task is **source/design only**, with no implementation, registry expansion, Cargo/build/test, formatter mutation or native activation authorized. Helper agents b7c8ccdf-dd84-47b6-a455-bcdb35a78804 and 91e4e4af-4fda-47c5-9293-984573303fea were resumed only for read-only source/design inventories; they have no C3 file-write lock.

## C3-proposal-r0 — the next implementable cuts, NOT a product release

This proposal is coordinated with D-r4's **Ordinary-call demand metadata** and output-lineage sections in `evidence/lowering-contract.md`, including D's subsequent parent-relayed recommendation: start with actual signed LongLong plus Int/typed-NULL at **every** input/result, not everything that happens to share an eval family. Only the parent may release implementation or amend the sole plan. All names below are proposed, not claims of existing public APIs.

### Recommendation: one small next release, two explicitly separate follow-ups

| Cut | Concrete work | Explicit non-claim |
|---|---|---|
| **C3a — next requested runtime cut** | Profile-checked two-argument **PlusInt203**, exact signed LongLong, typed Int/NULL, identity operand conversion, TypedRow or genuine PbRow origin; same official frame driver schedules left NULL-stop before right | Not AST/batch equivalence, mixed carriers, controls on this new route, native diagnostic parity, public caller activation or a wider registry |
| **C3b — later carrier/lineage cut** | A separately admitted SQL TypedRow control-only Int/Bytes carrier facade with static result-metadata IDs; UInt uses Int bits and String/Bytes use Bytes payloads; selected identity is carried by the actual selected value | No ordinary arithmetic, implicit conversions, PB-wide string controls, generic host payloads, dynamic origin inference, or whole-family completion |
| **C3c — later numeric-batch schedule cut** | Proven whole-selection left-subtree phase, right-subtree phase, then kernel phase on the same RPN driver; source-driven eligibility and demand set | Never obtain this by relabeling the current occurrence loop or tiling the whole expression |

C3b/C3c are concrete architectural seams, not permission to smuggle them into C3a. Keeping unsupported profiles visible and rejecting them is smaller and more honest than an always-OccurrenceOrdered promise. Existing compile_local, compile_local_with_hosts, LocalCompileContext struct literals, HostCatalog/signature/protocol APIs and D1 keep their released meanings.

### Actual source ledger: profile means a call-site path, not an enum-wide shortcut

Paths below use `DB = expression-unification/tidb/rust/crates/tidb-expr/src` and `KV = expression-unification/tikv/components/tidb_query_expr/src`. Function names are the durable anchors; line numbers were observed during this read-only review and may move with other owners' work.

| Actual path/domain | Observed order | Required treatment |
|---|---|---|
| AST value scalar, `DB/lib.rs:903–921` | Evaluate left, evaluate right, then signed-literal conversion/kernel; left NULL is not itself a skip | Record AstValueScalar separately. C3a rejects it; do not claim TypedRow fixes preserve the AST baseline |
| Typed integer row, `scalar_function.rs::eval_fast_integer_binary:1491–1504` | Int +/−/* finish typed left then NULL-stop before right. Integer comparison arms in the same helper are eager | Exact function/domain/path rule; not all Int operations or all comparisons |
| General typed numeric row, `scalar_function.rs:1614–1626`, `eval_numeric_operand_row:3800–3815` | Convert left before deciding; **Real PLUS and Int/Real MOD are eager**, while the other shown numeric arms (including Decimal MOD) stop on converted left NULL | C3a proves Identity conversion only. Raw-child NULL and post-conversion NULL are not interchangeable for future casts |
| PB row, `scalar_function/pb_builtin.rs:73–105,248–280` | Kernel::Binary stops after converted left NULL except non-Decimal MOD. The exact listed Int comparisons EqInt/GtInt therefore stop, unlike typed integer comparisons. Four signedness-specific MOD IDs223–226 evaluate/coerce both operands | Preserve exact PB ID/domain and genuine PB origin. Do not generalize the shown arm to every PB signature or infer baked-in MOD signedness from a row |
| Native numeric batch, `scalar_function.rs:4024–4061,4149–4188`, operand conversion at3858–3865 | Complete the **entire left subtree over the active selection**, then the entire right subtree, then combine results in occurrence order | This is argument-major at each eligible call, not general row-major execution |
| Caller dispatch, `evaluator.rs:400–451` | Numeric-batch path only after actual eligibility checks; nonvectorizable suites use row/select-list order | A batch API name, selection length or desired speed cannot choose the profile |

The existing `evaluator::numeric_batch_does_not_suppress_nested_errors_on_null_rows` (`evaluator.rs:825–861`) intentionally distinguishes scalar NULL success from vectorized nested-overflow error. The general diagnostic-order witness at `scalar_function.rs:4711–4722` distinguishes scalar `1x,3z,2y,4w` from batch `1x,2y,3z,4w`. Preserve both; do not “fix” the fixture to match one universal schedule.

There is also an all-signed, no-cast tiling witness: `plus(plus(L,1),plus(R,1))`, with L[0]=0, L[1024]=MAX and R[0]=MAX. Whole-selection operand-major evaluation fails in the left child at occurrence1024 before the right child starts. Whole-expression tiles of1024 fail in the right child at occurrence0 instead. C3a must refuse NativeNumericBatch before effects; a later C3c implementation must prove the real phase ordering, including the1024/1025 boundary.

`DB/pushdown_catalog.rs:1337–1343` already selects **PlusInt203** for signed+signed SQL operands. `KV/lib.rs:238–276,472` already sends203 through the canonical argument-flag mapper to the shared signed kernel. `KV/local/registry.rs:35–45` currently admits222 instead. Therefore C3a adds a **separate profile-gated203 admission**, not a203→222 rewrite, new function ID, copied selector or silent expansion of the old registry.

### C3a — exact admission, additive API and same-driver implementation

**Closed first domain:** two-argument FunctionRef::TiPb(PlusInt203), CallMetadata::None, signed LongLong at every declared argument/result/binding/literal and native Int/typed-NULL at every value boundary. Nested203 trees, strict constants and ordinary input slots suffice to exercise the bridge. No UInt, Tiny/untyped-NULL retagging, Real, Bytes, mixed-family value, deferred/correlated/parameter leaf, CAST, NULLIF, HostCall or control is admitted on this new tiny route. Existing C2 control/host routes remain available and unchanged; their presence does not automatically admit compositions on the new route. C3a accepts only proved TypedRow or genuine PbRow call-site facts. AstValueScalar, NativeNumericBatch and all other ordinary signatures are explicit admission failures, including222 on this new entry.

Proposed additive surface:

```rust
// Proposed only; preserve LocalExpr, LocalCompileContext and old entrypoints.
fn compile_local_profiled(
    spec: &LocalExpr,
    schema: &[FieldType],
    cx: LocalCompileContext,
    facts: &OrdinaryProfileSpec,
) -> LocalResult<LocalProgram>;
```

OrdinaryProfileSpec is immutable source metadata, not an executable graph. It contains the requested consumer facts and one call-site record for each ordinary CALL occurrence identified against the actual source-preorder walk: exact origin/profile, original PB signature when applicable, the approved operand-conversion/demand facts, and stable diagnostic/source identity. Validate coverage, ordering, duplicate/out-of-range/stale sites and source/ID/type consistency before effects. Missing facts are not an eager default. The compiled per-call record is opaque and checked; callers cannot attach an arbitrary NULL mask to any function. D validates genuine PB ingestion, original encoded/source facts and detached SQL metadata using its existing origin rules; C validates consistency with the actual LocalExpr, exact203, full FieldTypes and closed profile domain. A TiPb FunctionRef alone is not PB provenance because SQL lowering also uses official IDs.

C derives **LeftToRight / Identity-left / StopOnLeftNull / Identity-right / PreparedKernel** only for these two approved path/domain combinations. Conversion timing is explicit even though Identity is the only allowed conversion now. Static arity/type/source/admission failures apply to every child, including an unreachable right subtree. Actual read/type-kind conversion/warnings/errors occur only when that child is demanded. D must check native Datum::Int or Null **before** B's to_scalar erases Int/UInt identity, at the one demanded read, not by eagerly scanning dead rows. Constant facts come from strict literal_value, never eval.

Implementation uses prepare_call and the existing canonical selector/validator/metadata constructor exactly once. Require the retained-argument map to remain identity[0,1]. Add an official child-retaining ordinary-demand RPN node with opaque prepared metadata and one Ordinary continuation frame to the existing Program/Control/Host loop. Its states are: demand left → accept/check completed typed value → propagate its error or produce correctly typed NULL without starting right → demand right → invoke the **same** prepared-kernel helper used by ordinary FnCall. Factor that existing helper once; do not copy the addition kernel. Exclude the node from the primitive-leaf shortcut, and extend iterative metadata/Drop/type lookup. No recursive public eval, HostCall emulation, sidecar interpreter, evaluation closure or fake PB node is allowed.

Work and retained storage are charged before child demand, acceptance, retained overlap and kernel work; failure ends the prefix before later selected occurrences. Existing LocalProgram eval methods can retain their occurrence loop because this specific entry only admits the two row profiles. That statement is **not** a rule for the later batch profile. The host hook remains uncalled and HostCatalog stays signed-Int-only.

**Computed output and diagnostic boundary:** every PLUS node needs its own explicit computed Int ValueMetadata plus detached SQL result type in D's immutable metadata, even for `plus(x,0)` or an all-NULL result. It is not a selected passthrough and must not borrow an operand's unsigned/literal/collation record. Current programs have no artifact/plan cache; facts belong in the immutable prepared description now. Any future cache identity would have to include exact path profile, signature/domain/signedness, conversion order and source facts—no cache implementation is claimed here.

Demand correctness alone does not settle native overflow code/text. `scalar_function.rs:333–400,524–539` and AST `lib.rs:429–447` use source-shaped error descriptions; TiKV `impl_arithmetic.rs:45–48` uses evaluated values. Some native renderers call Constant::eval_in (`scalar_function.rs:339`). Do not call those from adaptation, reevaluate for formatting, stringify away LocalError identity, or replay natively on failure. Retain call-site source facts now; an independently locked, non-evaluating, site-aware error/diagnostic adapter remains a **native activation gate**, not an implicit part of runtime C3a acceptance.

**Smallest requested C3a lock set (seven Rust files, only after a new release):** new `local/profile.rs`, new `local/profile_tests.rs`, `local/mod.rs`, `local/compile.rs`, `types/function.rs`, `types/expr.rs`, `types/expr_eval.rs`. Profile admission can live in the new profile module, leaving `local/registry.rs` and its222 behavior unchanged. No LocalExpr shape, local/batch.rs/runtime.rs/host.rs, root lib.rs, RpnFnMeta, codegen, datatype, Cargo or HostCatalog edit is required for this first cut. A types/mod.rs opaque-export loan is unnecessary if the public opaque type is exported through local, as with C2b; otherwise it must be requested, not assumed.

D's later caller cut is separately owned/released under `DB/tikv/{catalog,lower,context,batch,mod,tests}.rs` or narrowly named additive siblings, preserving D1. It must factor actual signature facts without a serializer, preserve detached origin/type metadata, enforce native kind at demand, and declare computed-output records. Error/severity/source-site adaptation requires separately enumerated E/parent/common locks after its API is agreed. No source release or public activation is implied by this file list.

**C3a acceptance witnesses:**

- Exact203 + approved TypedRow/PbRow accepted; original222 eager route unchanged; new-route222, fabricated203→222 mapping, stale/missing PB facts, unsupported profiles/signatures/domains/conversions and unsupported dead children refused before reads.
- NULL left skips a poisoned right; left failure stops right; non-NULL left demands right; right failure/kernel overflow preserves the original LocalError variant and prior warning prefix. Nested203 and both path profiles use the same driver. Comparisons/MOD/AST/batch stay negative admission tests, not guessed positive cases.
- Empty,1,1024,1025 and `[2,0,2]` selections preserve occurrence order without cross-occurrence memoization; no later rows after failure. A host hook that panics if called remains uncalled.
- Work/frame/retained-byte refusal before the next read/kernel, and33/64/256-deep prepare/eval/metadata/Drop on a256KiB stack. Parent reruns the frozen499 native baseline plus new tests and then the separately released caller tests. None is run or claimed by this proposal.

### C3b — generic admitted carrier mechanics, with selected-output provenance

This is a separately requested representation cut, **not a relaxation of C3a**. “Generic” here means sharing the runtime's admitted Int/Bytes carrier mechanics, not accepting every EvalType or every native Datum in a family. Restrict the first lineage entry to SQL TypedRow IF/IFNULL/searched CASE/COALESCE, signed Int predicates, approved Int/Bytes value branches and any explicitly retained Int boolean AND/OR nodes. No ordinary arithmetic, host, AST/batch profile, implicit cast, arbitrary truth conversion or simple CASE selector is implied. Keep PB carrier expansion refused until each actual PB call-site path is separately proved: native `pb_builtin.rs:101–105` currently includes IfNullString but not every official string control.

Actual representation is already available: B's `tidb-datatype/src/tikv_compat/value.rs:224–258` transports UInt by unchanged64 bits in ScalarValue::Int and String/Bytes/BinaryLiteral in ScalarValue::Bytes, retaining ValueMetadata separately. `from_scalar:266–310` reconstructs by explicit kind/collation; its261–265 contract permits operand metadata **only for passthrough**. No new UInt vector, UTF-8 decoder, string kernel or bridge conversion is required. `KV/lib.rs:632,648,657,662` already tags the string control signatures; use the existing prepare_call/ControlKind path rather than modifying the selector.

**Why return FieldType alone is insufficient:** `DB/scalar_function.rs:127–178,1030–1098,1813–1857` preserves a selected SQL same-family Datum, including UInt kind, String collation, Bytes or BinaryLiteral identity. Identical payload bits/bytes do not identify the selected kind. In contrast, PB-first eval at1193–1206 reinterprets Int↔UInt through the **return** flags before same-family adaptation. Thus SQL signed-return IF selecting UInt(MAX) retains UInt(MAX), whereas the corresponding PB Int boundary produces Int(-1). Neither “always selected origin” nor “always result FT” is universally correct. `tidb-chunk/src/row.rs::DatumCell::datum_with_buffer:49–77` also materializes all string/blob columns as **String**, including binary/blob declarations; BINARY flags do not imply Datum::Bytes.

Proposed minimal, nonbreaking surface:

```rust
// Proposed only: thin ownership facade over the SAME compiler/RPN driver.
fn compile_control_with_lineage(
    spec: &LocalExpr, schema: &[FieldType], cx: LocalCompileContext,
    facts: &ControlLineageFacts,
) -> LocalResult<LocalControlProgram>;

// Same state/context/physical-row/selection/services inputs as the existing API.
// No Deref/legacy shortcut that silently drops required output identity.
struct LineagedBatch {
    values: VectorValue,
    result_metadata: Vec<ResultMetaId>, // exactly one per selection occurrence
}
```

TiDB owns the immutable ResultMetaId→record table next to its LocalExpr. A record contains admitted carrier, B ValueMetadata, detached complete SQL type/source facts, and source-value versus computed-boundary role. It has **no executable child links or callbacks**. IDs name materialization facts, not equal values, physical rows, CSE keys or caches. C validates a bounded flat fact per source node/ID/carrier against the existing graph and attaches a small private optional annotation while compiling. Old/wire programs have no annotation. LocalExpr, LocalCompileContext, LocalRuntimeServices and HostCatalog public shapes need not change.

Checked result flow is one of: **Leaf(id)**; **PreserveSelected { generated_null: id }** for a specifically proved same-family selection control; or **OwnResult(id)** for a computed boundary. The compiler derives/validates the rule from the approved profile and node; it is not arbitrary caller policy. AND/OR's newly computed Int is OwnResult. A later admitted PB Int control requires a return-flag-checked OwnResult/DeclaredIntBits rule, not operand provenance. An outer SQL selection forwards the **current** child ID, even if an inner computed/PB boundary replaced its original leaf ID. A missing ELSE/all-NULL result has an explicit carrier-correct generated NULL record; selected NULL may retain its selected source ID and still materializes SQL NULL.

In the official driver, a private FrameResult wraps the existing RpnStackNode plus an optional ResultMetaId. Legacy entry wrappers unwrap the node. For this closed one-structured-node-per-subprogram control shape, no public RpnStackNode or eager-kernel ABI change is needed. Nonlogical ControlFrame retains/moves the selected FrameResult rather than reducing it to Option<Int>. Only predicate roles extract Int truth; IFNULL/COALESCE inspect carrier-neutral nullness. Finish preserves the chosen payload/ID, supplies the declared result FT separately, and applies the checked flow rule. Generated NULL uses Bytes(None) for a Bytes result. No condition rerun, value-equality guess, eager branch import or native coerce_to_ret_type/convert_to escape is permitted. Future multi-node eager provenance requires deliberate operand-sidecar handling; it is not silently admitted by this control-only representation.

**Static source contracts avoid a service-ABI change.** Constants have exact kind/collation from immutable literal_value. Each admitted input slot has a fixed non-NULL kind/collation contract from the actual Chunk accessor and a source ID. At the **same one demanded read**, D obtains/to_scalar-transports the Datum and checks its non-NULL ValueMetadata against that contract; NULL is allowed without pretending the NULL Datum itself has a String collation. The driver seeds the compiled source ID at the input node. Unexpected dynamic kind/collation is a binding error at demand, not an eager whole-column scan or a second read. Variable-kind/collation params, correlated inputs and generic providers are outside this first static cut; later support would need an atomic value-plus-tag reply, not a separate callback that can evaluate again.

On output, D validates occurrence count, ID membership and carrier, then calls from_scalar with the selected table record. Keep the declared result SQL FieldType independently. UInt MAX remains i64 bits−1 internally with no numeric narrowing/saturation. String retains its actual StringDatum collation and arbitrary raw bytes; Bytes remains Bytes. BinaryLiteral may only enter under an explicit separate literal-kind admission proof, remains distinct from ordinary `_binary` text and BIT, and is not accidentally admitted by Bytes transport. There is no UTF-8 decode/re-encode step.

**Memory accounting is a release prerequisite, not a follow-up performance tweak.** Current C2 storage helpers assume Int. `codec/data_type/chunked_vec_bytes.rs::capacity()` reports max(data capacity,length), not row capacity or total heap: its data, bitmap and var_offset allocations must all be measured, as must frame/value overlap, output growth and lineage-vector capacity. Prefer a narrow parent/B-reviewed retained/growth helper in `codec/data_type/{chunked_vec_bytes,bit_vec}.rs`; without adequate accounting, Bytes remains inadmitted. Precharge fixed transport/frame headers before callbacks, inspect returned shape/byte length, and reserve normalized copies/output growth **before** retaining/copying or later work. Do not use from_scalar(&scalar_ref.to_owned(),1) for Bytes without charging its double copy; use direct push_ref or move an already normalized selected value. Accumulated output is metered incrementally. Provider-owned transient allocation remains outside the retained-scratch guarantee, as in C2b; no total heap bound is claimed.

**Minimal later C3b locks:** new `local/lineage.rs` plus focused tests; `local/{compile,batch,runtime,mod}.rs`; `types/{expr,expr_eval}.rs`; the two narrow datatype accounting files only by parent/B loan. Keep local/spec.rs public shape, local/registry.rs/host.rs, lib.rs selector, RpnFnMeta/codegen, LocalFunctionId, types/function.rs and impl_control.rs unchanged if this control-only cut holds. D separately extends/adds private `tikv/{lower,context,batch,mod,tests}` carrier siblings; B's existing value.rs transport remains unchanged. No file in this list is currently released.

**C3b witnesses:** UInt MAX under SQL selected passthrough; identical bytes from String(collation A), Bytes and separately admitted BinaryLiteral with a different result collation B; binary/blob input still materializing String; invalid-UTF8 bytes preserved; carrier-correct generated/selected NULL; poisoned unselected branches and once-only predicates; nested selected/computed IDs; empty/1025/repeated selections with aligned IDs; demanded dynamic kind/collation mismatch; byte/offset/bitmap/copy/output/tag-capacity limits. PB Int signed/unsigned reinterpretation and nested PB→SQL lineage are required **negative/deferred positive gates**, not enabled by SQL controls. Real/Float32/NaN, Decimal/declared scale, temporal/FSP, JSON, BIT/ENUM/SET/hybrid flags, arrays, cross-family conversion and host return lineage stay explicit separate domains.

### C3c — preserve genuine batch semantics, or reject before effects

Represent the consumer's actual schedule independently of row demand facts and structural-preparation purpose. Admission must identify the native vectorizable subtree closure and real active occurrence universe; filter-all-physical versus projection-selected and constant/parameter broadcast policies remain caller-specific. A vector API does not establish this closure. Initially reject NativeNumericBatch, mixed unproved profile trees and unsupported broadcasts before value effects—never retry natively, tile the current row loop or auto-force a scalar profile.

The later implementation should reuse the same official Program frame/postorder machinery: complete a left child program over the entire active selection, perform its approved conversion phase, complete the right child, then execute the parent kernel over that selection. Demand-import bindings at their node phase rather than preconverting every column. All repeated logical occurrences remain distinct. If a kernel is limited to1024 lanes, chunk **that node's kernel phase only after its required full child phases**, retaining/accounting intermediate values; do not chunk the whole root program and thereby reorder left/right failures. A bounded initial batch limit is acceptable only as an explicit pre-effect refusal, not a semantics-preserving fallback or broad batch-migration claim.

Required proof includes the unchanged existing scalar/batch witness, source diagnostic ordering, the all-signed1025 tiling witness above, first-error suppression of later phases, repeated/nonidentity/empty selections, selection-versus-filter demand sets, broadcast counts, metadata and retained-byte limits, and any admitted nested control boundary. Exact C3c locks can be enumerated only after that closure and conversion policy are approved; likely local/batch.rs/runtime.rs and types/expr_eval.rs plus private profile tests, not a new evaluator or kernel registry.

### Current design handoff / verification boundary

This turn changed **only runtime-contract.md**. The two helpers completed read-only inventories and stopped; neither wrote/formatted/built/tested any source. C read native/caller logs and the exact caller/bridge/runtime witnesses, and coordinated the D-r4 boundary through the parent. No C3 test, benchmark, implementation or cache exists. This is a request to release the seven-file **C3a** runtime cut when the parent chooses, while recording concrete C3b/C3c seams and their independent locks—not a second ExecPlan, automatic registry widening, public-route activation, completed SQL host migration or package-transcreation claim.

Validation of this document-only turn: the file-content grep `[\t ]+$|^(<<<<<<<|=======|>>>>>>>)` found no whitespace/conflict markers. Both helper registry entries were confirmed ready/not running. From the TiKV root, the following read-only command exited0 and reproduced the frozen C2b manifest hash `fde1f0f6695a9abb461912fd296144c1fdb92f680004c55eece3baa841455f4d`:

```sh
set -o pipefail; sha256sum components/tidb_query_expr/src/local/batch.rs components/tidb_query_expr/src/local/compile.rs components/tidb_query_expr/src/local/host.rs components/tidb_query_expr/src/local/host_tests.rs components/tidb_query_expr/src/local/mod.rs components/tidb_query_expr/src/local/runtime.rs components/tidb_query_expr/src/local/spec.rs components/tidb_query_expr/src/types/expr.rs components/tidb_query_expr/src/types/expr_eval.rs | sha256sum
```

No new test/build/lint/benchmark command was run; product/guide/main-plan/Git history remains untouched by C in this design turn. Performance, wider carrier semantics, site-aware native diagnostics, byte-accounting helpers and true numeric-batch scheduling remain the explicit unverified risks/gates above.

## C3a-source-r0 — released seven-file implementation, native gate pending

Parent subsequently released **only** new local/profile.rs and profile_tests.rs, local/mod.rs and compile.rs, types/function.rs, expr.rs and expr_eval.rs, plus this receipt. The proposal above remains historical for C3b/C3c and future diagnostics; it does not authorize additional edits. C3a now has source and21 new test functions, but C and its helpers ran **no Cargo/build/test/lint/benchmark**. The preceding499 native result is not a test of this changed source. The anticipated native count is **499 + 21 = 520**; the parent's actual run/count is authoritative.

### Actual immutable facts and numbering — published to D3

All names are exported through `tidb_query_expr::local`:

```rust
// Existing LocalExpr, LocalCompileContext, services and host APIs are unchanged.
compile_local_profiled(&LocalExpr, &[FieldType], LocalCompileContext,
                       &OrdinaryProfileSpec) -> LocalResult<LocalProgram>

OrdinaryProfile::{TypedRow, PbRow, AstValueScalar, NativeNumericBatch}
OrdinarySourceId::new(unit: u64, node: u64) // Copy, private fields; unit()/node()
OrdinaryCallSite::typed_row(ordinal: usize, source: OrdinarySourceId)
OrdinaryCallSite::pb_row(ordinal: usize, source: OrdinarySourceId,
                         original_pb_signature: i32)
OrdinaryProfileSpec::new(&LocalExpr, &[FieldType], OrdinaryProfile,
                        Vec<OrdinaryCallSite>, CompileLimits)
    -> LocalResult<OrdinaryProfileSpec>
```

Ordinals are **ALL-NODE source preorder**: root0, then every argument left-to-right, including constants and slots. They are neither call-only ordinals nor physical-row/selection indexes. Call records must be supplied in strictly increasing order, cover every CALL occurrence exactly once, and name no leaf/extra position; no automatic sorting or deduplication occurs. The root call's profile must match the declared consumer. A nested call may independently assert either admitted row profile. Equal caller source IDs at distinct ordinals remain distinct occurrences. Leaf-only roots need no call records, but still require an admitted row consumer.

OrdinarySourceId is an exact opaque caller identity, not a globally allocated key or proof of provenance. OrdinaryCallSite has read-only ordinal/source/profile/original_pb_signature getters; the original PB number is stored as i32 so unknown/mismatched raw identities are explicitly refused rather than normalized. OrdinaryProfileSpec has private immutable storage and consumer/node_count/call_sites getters. Clone/Debug/Drop operate on a **flat** snapshot, not a recursive clone of LocalExpr. There are no child links, evaluator callbacks, hashes, CSE or caches in that snapshot.

The checked snapshot retains every constant's exact Int value/NULL, LiteralKind and complete FieldType; every input slot index and complete FieldType; each fixed binary203/None call and result FieldType; and the entire complete binding schema, including unused entries. Its bounded borrowed traversal checks node/depth/scheduled-child budgets before descriptor cloning/stack growth. Before compilation, validate rewalks the actual tree under the current compilation limits and compares all facts without cloning a second snapshot. Changed literal values, NULL state, slots, ordering/shape, types, signatures/metadata/arity or schema invalidate reuse. Immutable call facts were checked when constructed and cannot be mutated through getters.

**Actual closed admission:** exact PlusInt203 only, arity2, CallMetadata::None, signed LongLong and Int/typed NULL at every boundary; constants additionally require LiteralKind::Typed. Int values tagged Text/BinaryLiteral are not an implicit identity conversion. AST/batch consumers,222 and every other operation, controls, hosts, casts, Tiny/untyped NULL, UInt/unsigned declarations, Bytes/Real or mixed carriers are refused on this entry. Existing compile_local, its eager222 behavior, strict-control/host entrypoints, LocalCompileContext literals, LocalRuntimeServices and HostCatalog remain unchanged. In particular, no registry.rs or root selector edit widens another route.

**Trust boundary required by the parent:** C checks consistency of asserted profile, raw PB203 identity, complete source snapshot and local call shape. It **cannot prove** actual native PB ingestion or distinguish two native sources with identical local syntax except through caller-supplied identities. D owns genuine PB/signature/private-origin checks at every native node, and must check actual Datum::Int/Null before transport erases Int/UInt kind at the demanded read. The native PB Null literal retains FieldType::Null and remains inadmitted; do not retag it LongLong. Unit tests cover mismatched/missing assertions, not detection of arbitrary truthful-looking forged origin labels.

### Actual official RPN/driver seam

`PreparedOrdinaryCall` is public opaque metadata with a crate-private consuming preparation seam; function/return_type/site are read-only. The shared prepare_call→canonical selector→validator→metadata constructor is used once. The conversion from PreparedCall requires exact203, no control tag, unit metadata and retained args exactly[0,1]. It owns the original function identity, result FieldType, prepared metadata and exact OrdinaryCallSite. The crate-private consuming seam relies on the validated profile/compiler factory for signed-domain and site-assertion checks; it must not be exposed/reused as a standalone unchecked constructor. No native expression, closure, fabricated PB Expr or alternate ID-to-kernel map is introduced.

`RpnExpressionNode::OrdinaryFnCall { prepared, args }` owns independently demandable child RpnExpressions. It contributes one work unit plus child work. Result type/classification and iterative metadata/Drop/into_inner traversal include its children without treating it as a flattened logical chain. The one-node primitive shortcut excludes it, and eval_one_node marks it driver-only.

The official loop now has Program/Control/Host/**Ordinary** frames. Ordinary reserves two operand slots before either child, demands/accepts completed typed left, and only then decides its Identity-domain NULL stop. NULL left produces declared signed Int NULL without requesting right or invoking the arithmetic kernel. Otherwise it demands/accepts right and enters the **same eval_prepared_kernel helper as eager FnCall**. Only invocation plumbing was factored; no kernel was copied. Returned operand shape, complete FieldType and Int representation are checked before use. Operand nodes remain owned/borrowed according to the existing RPN lifetime rules; no provider/ctx borrow escapes.

Work is metered at node, request, child, accept and kernel/NULL-finish transitions. Two constants need8 ticks; NULL-left constant needs5; two input slots add their existing read ticks and need10. Frame capacity, two operand-slot capacity, retained left/right payloads, returned payload overlap and output reservation stay charged while the next child/kernel runs. The private byte regression uses B=3×sizeof(EvalFrame)+2×sizeof(RpnStackNode), I=int_storage_bytes(1): caps B+I/B+2I/B+3I stop after0/1/2 reads, while B+4I reaches the deliberately overflowing MAX+1 kernel. This distinguishes missing retained-operand/kernel precharges from a later accidental refusal. No host task or optional host-view call is introduced.

### Source identity is retained — native diagnostics are NOT implemented

PreparedOrdinaryCall and OrdinaryFrame retain the exact call ordinal/source/profile/raw PB identity plus current operands/row stage. Original LocalError variants and the existing warning prefix propagate unchanged. That is **not** an error-site result API, source-shaped native message adaptation, severity/publication mapping, diagnostic rollback, or native parity/public activation claim.

D3's parent-relayed future proposal is separately gated: a reported-eval wrapper could attach the innermost **actual** checked kernel site/InputRow/stage before unwind, with ancestors unable to overwrite it; input errors need their real leaf/binding site, not a nearest-parent-call substitution. Leaf-error reporting is not present in C3a. Native source renderability is independent of execution identity: from_pb display names such as sig_PlusInt can cause nested PB root overflow to fall back to IntOverflow even when an inner failure yields source-shaped DataOutOfRange. Never infer '+' rendering from203 or call a formatter that evaluates Constant::eval_in. Retained IDs leave room for a later bounded source-shape sidecar and pure adapter, without doing replay now. A read-only audit noted that D's current lowerer allocates a binding slot per native Column occurrence; D3 could retain exact leaf identity in that binding record. C permits reused slots, so this uniqueness is a caller invariant, not a generic leaf-site receipt. Active kernel/leaf reporting still requires its separate API/gate. No cache identity implementation is claimed.

### Tests, ownership and source checks

**21 new test functions, authored/static-reviewed only:**16 in local/profile_tests.rs; four in types/expr.rs; one private retained-operand regression in types/expr_eval.rs. Coverage includes all-node ordinal gaps and mixed per-call assertions; high-bit source IDs; exact203/no-remap/old222 compatibility; full immutable snapshots and stale schema/literals/shape; static rejection of unsupported dead children; NULL-stop versus poison reads; nested demand; []/1025-reverse/[2,0,2]; worker lifetime/rebinding; Binding/Resource/Evaluation/shape/panic paths and warning prefixes; no replay or later rows after error; schema/selection preflight; work/frame/byte/task-zero limits; decoded input;33/64/256-depth execution on256KiB stacks; metadata/Drop up to16384 depth; and exactly-once destruction through mixed ordinary/host/control ownership-only fixtures. Metadata tests that deliberately replace child arities or attach mixed sentinels **never evaluate those invalid trees** and do not broaden public admission.

Exact helper ledger: b7c8ccdf-dd84-47b6-a455-bcdb35a78804 exclusively wrote NEW local/profile.rs, handed it back, then exclusively NEW local/profile_tests.rs and handed it back; no mod/artifact write. 91e4e4af-4fda-47c5-9293-984573303fea exclusively wrote types/expr.rs and handed it back; subsequent exhaustive/core audits were read-only. C owns compiler, exports, prepared helper/driver integration and this receipt. No child writer remains after the handbacks; final formatting is C's scoped pass. The exhaustive match and final core/lifetime/resource audits found no concrete production blocker or additional file lock. This is static evidence, not compilation/test proof. Work counts and private byte-cap sensitivity were independently traced. Both children stopped after their final handoffs. Wire-only recursive test helpers are not reused for deep ordinary fixtures.

Pinned nightly rustfmt with `--edition 2021 --config skip_children=true` over exactly the seven files and scoped `git diff --check` exited0. No native test/build/Cargo or broad formatting ran. No writes were made to registry.rs, root lib.rs, RpnFnMeta fields, codegen, datatype, Cargo/locks, spec.rs, batch.rs, runtime.rs, host.rs, maintenance guides, the main plan, old expression-reuse or Git history. Parent retains the serialized native/caller gates and guide/main-plan updates. C3b carrier/lineage and C3c batch scheduling remain deferred.

### C3a exact source manifest

Relative paths are from the TiKV root. After the final seven-file formatter/check, the SHA256 of this ordered sha256sum manifest text is `e33024a7d3bff2d2826972b86a8d1d0bfc977a9f140698aec33b547f698d73a2` (not a compiled artifact or cache key):

```text
bb5abe3831fd6a026daab444cb6c05e8d229fd4af1af5661638696fd1cc6b8b4  components/tidb_query_expr/src/local/compile.rs
413ec4f4e788cfddc58c2b00e0045a09346d6c52ec7064e5dee12d7f6f19aef5  components/tidb_query_expr/src/local/mod.rs
85169db78a962dc9aedc7c7a799fc751620cd3ada86f731d327514325274aa32  components/tidb_query_expr/src/local/profile.rs
3c76555256b0a9e07620074474407fff1ae94a9350c1310cb9c7bef7a9e0cdf7  components/tidb_query_expr/src/local/profile_tests.rs
9cce5809176a3c11b2925c1666ffe468312b13c03677dc77cc159e11ea115c45  components/tidb_query_expr/src/types/expr.rs
f97e52d9a157b953cb8ac036e0f3b8b21c8f85630acdc8440fe675600809bbe2  components/tidb_query_expr/src/types/expr_eval.rs
ece54c3871463a7532ff1b1f47e59e3c4f67c16c7b600fb1324470af673831cb  components/tidb_query_expr/src/types/function.rs
```

## C3a-native-r1 — actual accepted checkpoint, products handed back

Parent accepted the C3a cohort, independently matched the seven-file manifest above, and handed its product files back/froze them. C reread the actual results:

- `logs/tikv-c3a-expr-full.log:568`: **520 passed, 0 failed/ignored/measured/filtered, 2.83s**, including the21 new C3a tests.
- `logs/tikv-c3a-aggr-full.log:96`: **40 passed, 0 failed/ignored/measured/filtered**.
- `logs/tidb-c3a-d1-seed.log:2031`: **16/16**, 1335 filtered; `logs/tidb-c3a-d2-structural.log:2011`: **8/8**, 1343 filtered.
- `logs/tidb-c3a-expr-full-comparison.log:3380–3386`: **1254 passed / four failed / 93 ignored**, no filtered tests. Parent verified the failure names and panic/assertion text against the prior baseline. This remains a no-new-failure comparison, **not an all-green full TiDB suite claim**.

These are parent-run results, not C-run commands or evidence for any future diagnostic API. Parent has separately released D3's six-file private runtime caller; no diagnostic code or C3b/C3c expansion is thereby approved. The following C3d work changes only this receipt.

## C3d-proposal-r0 — additive reported evaluation, SOURCE/API DESIGN ONLY

This is the concrete five-file proposal requested after C3a acceptance, coordinated with D3-r7's diagnostic section (`lowering-contract.md:258–304`). Parent has accepted the design constraints—typed existing EvaluateError code, exact count/length warning endpoints, actual-operation failure-only capture, no ancestor or stale attribution—but **has not granted implementation locks**. No product/source, formatter, Cargo/build/test, guide or plan changes are made in this design round.

### 1. Exact proposed public surface

All new exports belong under `tidb_query_expr::local`; existing entrypoints and LocalError variants remain unchanged. Names/signatures here are proposals:

```rust
impl LocalProgram {
    pub fn eval_with_bindings_reported(
        &mut self, limits: ExecutionLimits, ctx: &mut EvalContext,
        physical_rows: usize, selection: &[usize],
        services: &mut dyn LocalRuntimeServices,
    ) -> Result<VectorValue, ReportedLocalFailure>;
}

// Private fields and driver-only construction; never clones the error.
pub struct ReportedLocalFailure {
    error: LocalError,
    site: Option<LocalFailureSite>,
}
impl ReportedLocalFailure {
    pub fn error(&self) -> &LocalError;
    pub fn into_error(self) -> LocalError;
    pub fn site(&self) -> Option<&LocalFailureSite>;
    pub fn stage(&self) -> LocalFailureStage;
    pub fn sql_error_code(&self) -> Option<i32>;
}

pub enum LocalFailureSite {
    Kernel { call: OrdinaryCallSite, row: InputRow },
    InputSlot { slot: usize, row: InputRow },
}
pub enum LocalFailureStage { Kernel, Input, Resource, Validation, Unattributed }
```

Kernel.call is the **exact already checked** call record, retaining all-node ordinal, opaque source(unit,node), per-call profile and optional original PB signature. InputSlot means the actual binding slot passed to read_input, not a source-node ordinal or an enclosing call. InputRow preserves both selection occurrence and physical row. Getter access is immutable; the report owns all its data and retains the original owned LocalError without formatting, cloning, remapping or replacing it. Display, if supplied, delegates to the raw error and is never used for classification. No caller can attach an arbitrary public site to a report through a public constructor.

`stage()` is a cheap derived classification, **not stored mutable execution state**. Its precedence is:

1. Some(Kernel) → Kernel; Some(InputSlot) → Input, irrespective of the callback's LocalError variant/code.
2. No site + ResourceLimit → Resource.
3. No site + InvalidSpec/InvalidBatch/BindingContract/HostContract → Validation.
4. No site + Evaluation → Unattributed.

Thus a read callback returning ResourceLimit has **Input stage with an intact ResourceLimit cause**. An input carrying typed code1690 is still Input. A legacy eager Evaluation error is not automatically a Kernel report. The absence of a site is meaningful; neither a root nor nearest-frame guess fills it in.

### 2. Minimal implementation choice: fresh failure-only recorder, outward owned result

Use a private invocation-local `FailureRecorder { site: Option<LocalFailureSite> }`, created fresh before the reported entry's shared preflight/empty-input handling. Thread `Option<&mut FailureRecorder>` through a single shared bindings/rows core and the existing input/frame/leaf helpers, reborrowing for one operation at a time. All legacy public routes pass None; the existing private eval_with_input can be a thin None wrapper around a reporting-capable internal entry. Do not duplicate the loop or kernel helper, change evaluation demand, or replay a failed run.

**Recorder rules:** write only immediately when one of the two named operations actually returns terminal Err; first write wins; propagate that original error immediately; no recovery/retry inside the invocation after a write; no write at operation entry, child request, accept, successful read/kernel, NULL finish, validation or budget check. At the outer Err boundary, move the original LocalError plus the recorded site into ReportedLocalFailure exactly once. On success/empty input the fresh unused recorder is dropped. On panic it unwinds normally—no catch/reclassification is added—and a later call creates a new recorder.

The outward API is therefore result-carried, despite using a small internal recorder. It has **no last/current-failure field** in `LocalProgram`, `EvalContext`, `TaskGuard`, any persistent worker field, or TLS; immutable `ExecutionLimits` carries policy only. This is smaller than converting every existing private LocalResult/explicit Err/legacy adapter into an internal reported-error type solely to decorate two operations. An internal result-wrapper implementation would also be viable, but is not the recommended first cut. The failure-only/no-recovery invariants are mandatory, not optional optimizations.

| Failure boundary in existing code | Proposed capture / classification |
|---|---|
| Actual Ordinary frame's `eval_prepared_kernel` Err | Copy its current InputRow and clone its exact PreparedOrdinaryCall.site immediately at that Err; Kernel. The helper's fallible operation is the real fn_ptr invocation, not an ancestor subtree |
| Actual services.read_input Err inside eval_one_node | Copy the **same** slot and InputRow passed to that callback; Input. Preserve any returned LocalError variant, including ResourceLimit or Evaluation1690 |
| A successful read's malformed Int/type/length reply | Existing BindingContract; **site=None, Validation**. This deliberately does not add a separate InputValidationSite API |
| Schema/selection/host-scope preflight | Existing error; site=None, Validation; no row/call attribution, including an empty selection |
| Work/frame/storage/output reservation denial before an operation | Existing ResourceLimit; site=None, Resource, even if an ordinary frame is pending |
| Operand/result validation, or storage check after a successful read/kernel | No captured site; Validation/Resource as appropriate. Success does not arm an attribution to be inherited by the next failure |
| Legacy eager FnCall error with no OrdinaryCallSite | Original Evaluation; site=None, Unattributed. Never infer203/source from a shared kernel pointer/name/code |

The singleton-input fast path already calls eval_one_node and must use that same instrumented read seam; no parent frame is required to report a lone failed InputSlot. In the reported public row path, row coordinates come from the checked selected occurrence used by the operation. Legacy raw RPN can have empty physical maps for constant-only evaluation; its None-report path remains unchanged and must not invent physical row0. A future expansion of reporting to such raw APIs requires its own row contract.

**No host diagnostic scope in this first API.** The reported method rejects a program with host_catalog present using an unsited HostContract/Validation after ordinary schema/selection checks and before host_services/start. Host-free reporting never consults the optional hook. The old eval_with_bindings still accepts its released host programs unchanged, so it must share a private core rather than blindly delegate to the host-rejecting public reported method. This narrows only a new API; it does not edit HostCatalog, providers, scheduling or C2b cleanup guarantees. Controls/legacy eager programs can keep their existing execution through the core, but unannotated kernel errors gain no invented source identity.

### 3. Exact site and leaf mapping; no ancestor overwrite

For `plus(col0,plus(col1,col2))`, source-preorder nodes are0/1/2/3/4. Failure in the inner actual kernel reports call2, not root0. If that inner kernel succeeds and the outer kernel overflows, the report is call0; no successful operation left a stale site. An error while reading slot1 reports slot1 with its actual InputRow—not call2 or root0. A parent/outer wrapper never blankets child errors with its own call record.

D3's lowerer uses a distinct binding slot for each native Column **expression occurrence**; D may therefore join slot1→leaf3 using its own immutable binding record. General C consumers may reuse slot0 in both operands, so an InputSlot report is not universally a unique leaf ordinal. C must not guess which native leaf or nearest parent it represents. D's join must validate that mapping, while a general shared-slot producer would need a separately approved leaf receipt. Kernel joins likewise validate exact call ordinal/source/profile against the owning immutable spec; a mismatch remains raw and is never repaired with a root fallback.

Selection coordinates are independent: physical row2 in `[2,0,2]` has occurrences0 and2. Copy the occurrence at the failing operation; do not deduplicate by physical row. Reports own these scalar identities, so later state reuse, dropped programs/specs/services or a second invocation cannot change a previous report.

### 4. SQL-code getter: existing typed API only, no new Cargo edge

Actual TiKV `tidb_query_common/src/error.rs:97–107` defines Error as a **typed** public Box<ErrorInner>; ErrorInner is Storage or Evaluate. There is no imagined common Error::code/as_inner API. `EvaluateError::code()` at25–33 is the existing numeric i32 method. The proposed implementation is solely:

```rust
match self.error() {
    LocalError::Evaluation(error) => match error.0.as_ref() {
        tidb_query_common::error::ErrorInner::Evaluate(error) => Some(error.code()),
        tidb_query_common::error::ErrorInner::Storage(_) => None,
    },
    _ => None,
}
```

Custom preserves its stored code (including1690); InvalidCharacterString returns1300, DeadlineExceeded9007, and Other the existing10000. ErrorCodeExt::error_code is a different TiKV category API, not this numeric getter. Storage and non-Evaluation LocalErrors return None even if their text/cause mentions1690. A generic Box<dyn Error> or a non-Eval codec error may already have been string-erased upstream into EvaluateError::Other; do not downcast/parse it back into1690. The common fallback10000 is also not the codec warning fallback1105.

C's expression crate already depends on tidb_query_common. The TiDB caller does not currently have that direct dependency, so this C-owned borrowed getter requires **no new caller Cargo edge**, common-error representation change, dependency re-export or LocalError variant. It allocates nothing and does not consume the owned error.

The getter reports the underlying TiKV evaluation code, **not a PLUS classifier or native SQL diagnosis**. A later caller adapter needs both typed1690 **and** an actual Kernel site joining an admitted exact203/Identity/signed call. Input1690, storage, Other/unknown, unsited legacy errors and site mismatches remain raw. No message/panic-text parsing, source-string equality heuristic, native formatter, Constant::eval_in or replay is allowed. Source display/renderability stays separate from execution203; PB sig_PlusInt nesting and the observed native fallback remain D/E's later pure-source adapter work.

### 5. Warning prefix: exact endpoints, not a journal or source attribution

Keep the successful result type VectorValue; add no C success wrapper solely for warnings. D captures `{ warning_cnt: usize, stored_len: usize }` from the **live** ctx.warnings before all invocation preflight and after the call returns normally (including cleanup), on both ordinary success and returned failure. C's reporting itself never appends, drains, takes, truncates, sorts, merges, deduplicates or resets warnings. Existing details/prefix stay in the caller-owned EvalContext; the report neither borrows nor clones that Vec. An alternative would attach four copied endpoint usize words to each failure plus a helper for successful calls; the recommended minimum instead leaves endpoint ownership in D's wrapper and adds no C warning DTO/getter. Uniform owned success receipts remain a separate optional API choice.

Actual `tidb_query_datatype/src/expr/ctx.rs:184–220` separates total warning_cnt from retained warnings.len(). The live receiver's detail cap is private with no getter, and ctx.cfg.max_warning_cnt can differ after a receiver/config replacement. EvalWarnings::default itself has cap0, unlike the EvalContext default configuration's64; take_warnings replaces the receiver and allocates a new configured buffer, while merge mutates/moves the other receiver's details. None is an observation API. Therefore the first seam uses **only the observed count and stored length**, not an assumed cap or inferred number of retained new details. At cap, count may increase while stored_len is unchanged. No datatype accessor/lock is necessary for these endpoints.

The existing counters use plain usize `+=`, **not saturation**; checked versus unchecked overflow behavior follows the existing build/configuration. C3d must not change it, fabricate a saturating delta, or promise a meaningful arithmetic delta after wrap, non-monotonic mutation or panic. Preserve raw endpoints; any caller delta must be checked, and no after-success/returned-failure endpoint is promised for a panic. Reporting does not catch panic or repair a contract-violating callback that drains/resets the receiver.

The admitted real203/Identity/Chunk-read slice generates no new conversion/kernel warnings. Synthetic service warnings prove executed-prefix preservation/count behavior and cap handling, not SQL per-warning site attribution. Existing stored warnings lack node/occurrence tags; a future warning-producing family needs an append-time event/site and context/severity/publication handoff, not retrospective attribution from counts or messages.

### 6. Allocation, ownership, ABI and budget invariants

- Failure capture copies InputRow/slot or clones the current OrdinaryCallSite, whose fields are only scalar IDs/profile/optional raw signature. That is fixed-size/allocation-free **with the current representation**. Do not clone FieldTypes, Any metadata, expressions, source strings or LocalError. A future variable-sized site extension must reopen this guarantee.
- Move the original owned LocalError, including its existing boxed common error, into the public report and back out via into_error. No extra Box, trace Vec, source lookup table or diagnostic-render allocation is required. Source sketches/renderer byte budgets remain D/E's separate lowering/adapter gate.
- Recorder/report owns no program, frame, input, selection, service, state or ctx reference. Its reborrow lifetime is independent of the RPN result lifetime; no view survives a child or callback. Dropping those owners after a failed call must leave report getters valid.
- Do not enlarge EvalFrame or TaskGuard or modify their declaration/drop order. One invocation-local fixed-size recorder adds no retained evaluation heap. Existing frame/operand/result/output metering and work ticks stay unchanged; recording a failure must not charge/fail/allocate in a way that replaces the primary error. This is not a total-stack/process-memory or allocator-abort recovery guarantee.
- Preserve old public signatures and LocalError identity, compile_local/profile/HostCatalog/LocalRuntimeServices/LocalCompileContext ABIs, allocation-free legacy leaf behavior and no-host-hook behavior. No native route is activated; no registry/profile/carrier is widened. Panics and contract-compliant cleanup follow the existing path without conversion into a synthetic report.

### 7. Exact proposed locks and proof gate

**Request only these five C Rust files, upon explicit future release:**

1. NEW `components/tidb_query_expr/src/local/diagnostic.rs`: opaque public report/site/stage, borrowed typed-code getter, private first-write-only recorder.
2. NEW `components/tidb_query_expr/src/local/diagnostic_tests.rs`: focused attribution/ownership/compatibility/prefix tests.
3. `components/tidb_query_expr/src/local/mod.rs`: exports/module declarations and crate-private recorder visibility.
4. `components/tidb_query_expr/src/local/batch.rs`: additive reported method, one shared preflight/rows core and outer owned report assembly; explicit reported-host refusal.
5. `components/tidb_query_expr/src/types/expr_eval.rs`: optional borrowed plumbing, actual ordinary-kernel/read-input Err capture, unchanged legacy None wrappers.

No spec.rs/LocalError variant, runtime.rs, profile.rs, compile.rs, function.rs, expr.rs, host protocol, kernel, RpnFnMeta/codegen, common-error, datatype, Cargo, TiDB caller, guide or plan write is part of this C cut. Any unexpected exhaustive-match/API need must be requested before editing. Parent alone owns the main plan and grants source locks.

D/E's separately proposed future files remain new `tikv/ordinary_diagnostics.rs` and `ordinary_diagnostics_tests.rs`, narrowly scoped `tikv/ordinary.rs` report/source-sketch adaptation, and scalar_function.rs **pure display-operator facts only**. No existing native formatter or public evaluator is invoked/rewired, and no C source-sketch renderer is introduced. D3 runtime-only caller work can proceed independently; C3d source/API design is not permission to add its diagnostic code.

**Required C3d tests, before any later native/caller diagnostic claim:**

1. Nested kernel2 overflow versus successful inner then root0 overflow; duplicate source IDs retain distinct ordinals; no ancestor overwrite. Drop source/spec/program/services before reading the owned report.
2. Lone InputSlot fast-path failure; nested read failure; repeated use of one binding slot failing on its second read. Report actual(slot,row), not a native leaf/parent guess. Callback ResourceLimit/BindingContract/Evaluation1690 keeps Input stage and the original cause.
3. `[2,0,2]` distinguishes occurrence2 from physical2; first error stops later work; retries on different rows/sites yield independent immutable reports with no caching/deduplication.
4. Schema/selection/reported-host preflight, including []; malformed successful input reply; work/frame/storage denial before and after a successful operation: Validation/Resource with **no site**. Constant203 budget7 denies before the kernel, budget8 reaches intentional overflow. NULL-stop success records no kernel.
5. Failure→success→empty→preflight failure and panic→retry leave no stale site. No persistent current/last failure state is used to pass these tests.
6. Code getter covers actual Custom1690/1300/9007/Other10000, storage and every non-evaluation error; strings/generic erased errors containing1690 are never parsed. Check into_error preserves the original variant and common boxed allocation/payload without unsafe code.
7. Old eager222 overflow remains raw/unattributed; old control/host/D1 paths remain unchanged. A host-free hook that panics if called stays uncalled; the new reported-host refusal precedes all host effects.
8. Warning cap0/1/full cases preserve both total count and exact stored prefix/order on success and failure, including count growth with fixed stored_len. No drain or native evaluation hook is touched; endpoints claim no per-warning source identity or typed delta after panic.
9. Deep ordinary reporting remains iterative and bounded; getters/Display add no input/kernel/host/native-format calls; no report-construction budget failure overwrites an earlier error.

Parent runs the existing520 native expression baseline, aggr40 and the appropriate caller D1/D2/D3/full-baseline comparison plus new scoped tests only after a C3d implementation release. A later D/E gate adds native nested/root/PB-label fallback, exact input binding joins, poisoned formatter/evaluator hooks and source-sketch byte/depth limits. Full context/severity/publication and public-entrypoint activation still require separate approval. C3b carriers/lineage and C3c numeric-batch scheduling remain deferred.

### C3d design handback and checks actually performed

Only `runtime-contract.md` changed in this round. C reread D3-r7's proposal, actual C3a/caller logs, local/batch.rs, the shared driver seams, common/error.rs and datatype/expr/ctx.rs. No C3d code, test, formatter, build, lint, Cargo edge, diagnostic renderer, guide/main-plan or Git-history change was made.

Read-only helpers were b7c8ccdf-dd84-47b6-a455-bcdb35a78804 (plumbing/ownership/tests) and 91e4e4af-4fda-47c5-9293-984573303fea (typed errors/warning endpoints). Both delivered their findings and stopped writing/work; the latter's harness closing notice reported a failed turn after its complete findings had been sent, so no native execution/completion evidence is inferred from it. Registry entries were subsequently ready/not running; neither had any write lock in this round.

The document-content grep `[\t ]+$|^(<<<<<<<|=======|>>>>>>>)` found no whitespace/conflict markers. This read-only command from the TiKV root exited0 and reproduced the frozen C3a manifest checksum `e33024a7d3bff2d2826972b86a8d1d0bfc977a9f140698aec33b547f698d73a2`:

```sh
set -o pipefail; sha256sum components/tidb_query_expr/src/local/compile.rs components/tidb_query_expr/src/local/mod.rs components/tidb_query_expr/src/local/profile.rs components/tidb_query_expr/src/local/profile_tests.rs components/tidb_query_expr/src/types/expr.rs components/tidb_query_expr/src/types/expr_eval.rs components/tidb_query_expr/src/types/function.rs | sha256sum
```

This proposes the exact five-file runtime receipt seam only. Its API, resource/lifetime behavior and tests await parent review and an explicit implementation release; native diagnostic equality, warning journaling/source attribution and activation remain unverified separate gates.

## C3d-staged-r0 — two new files authorized, integration still locked

Parent approved the C3d design and issued a **staged** release: only NEW local/diagnostic.rs and local/diagnostic_tests.rs plus this receipt may be written before its imminent D3 caller cohort gate. Existing local/mod.rs, local/batch.rs and types/expr_eval.rs remain frozen until a separate explicit unlock. The module is deliberately **not wired**, so current Cargo/module inputs retain the accepted C3a API. No build/test/Cargo command is run by C or a helper.

C has created/formatted diagnostic.rs with exactly the proposed LocalFailureSite, LocalFailureStage and private-field ReportedLocalFailure. Getters retain/move the owned LocalError; sql_error_code borrows common Error.0.as_ref and calls only EvaluateError::code; Display delegates the raw error, and Error::source exposes that same LocalError rather than reconstructing a deeper native diagnosis. Stage precedence is actual Kernel/Input site before unlocated Resource/Validation/Unattributed classification.

The crate-private FailureRecorder is Default-empty and stores only one optional owned site. capture_kernel(&OrdinaryCallSite, InputRow, LocalError) and capture_input(slot, InputRow, LocalError) return that **same moved error**, capturing only if empty; into_failure consumes recorder plus the final error. The consuming error argument makes the intended Err-decoration seam explicit. Correct pairing still depends on fresh invocation scope, first-write-only capture, immediate propagation and no internal recovery/retry after capture. Current site data is scalar-only; no error/FieldType/expression/warning clone, heap allocation, tick or fallible operation is added by capture. No debug assertion is inserted that could replace a failure with a panic.

Exact staged ownership: C writes diagnostic.rs and this receipt; b7c8ccdf-dd84-47b6-a455-bcdb35a78804 writes **only NEW diagnostic_tests.rs**, targeting the agreed future reported entry but never declaring its module. 91e4e4af-4fda-47c5-9293-984573303fea performed a **read-only** pure-type/getter/recorder review and found no concrete defect; its runtime closing notice failed after the full review memo was delivered, and no native evidence is inferred. Existing modules, driver, batch, profile/prepared metadata, Cargo, datatype, callers, guides and plan are untouched at this stage. Test authoring/handback and the later integration unlock remained pending at this staged receipt; no reported evaluation method or warning endpoint capture had yet been implemented at that point.

## C3d-source-r0 — five-file integration after explicit unlock, native gate pending

Parent accepted D3's runtime-only caller cohort and then explicitly unlocked local/mod.rs, local/batch.rs and types/expr_eval.rs. C reread `logs/tidb-d3-ordinary-seed.log:2011–2023` (**10/10**), `tidb-d3-d1-compat.log:2017` (**16/16**), `tidb-d3-d2-compat.log:2009` (**8/8**) and `tidb-d3-expr-full-comparison.log:3388–3394` (**1264 passed / four unchanged baseline failures / 93 ignored**,1361 discovered, no filter). Parent reports the same failure/panic text and native datatype347/RPN520/aggr40 accepted. This is the pre-C3d baseline, not a run of the diagnostic changes.

### Actual integration and unchanged contracts

The exact five-file C3d source is now integrated; the proposed public signatures/names at C3d-proposal-r0 are actual source APIs. LocalProgram::eval_with_bindings_reported creates a fresh FailureRecorder, invokes one shared private eval_bindings/rows core once, then moves the original terminal LocalError and optional site into ReportedLocalFailure. Old eval_with_bindings invokes that core with None; decoded eval also passes None. No error recovery/retry or last/current failure field exists. The new reported method checks schema and selection before refusing a host_catalog-bearing program with HostContract, before consulting the host hook; old host evaluation remains unchanged.

A private RpnExpression::eval_with_input_recording carries an optional recorder through the existing official loop. The existing eval_with_input/decoded/wire entrypoints remain None wrappers. EvalFrame and TaskGuard fields, allocation/Drop order, work ticks, scratch reservation and RpnFnMeta/codegen/kernel invocation stay unchanged. The singleton leaf path uses the same instrumented eval_one_node; no alternate evaluator, callback graph or replay is added.

There are exactly **two production capture calls**:

1. The actual Ordinary frame's eval_prepared_kernel **Err**, after its prechecks but before returning it: capture frame.prepared.site() and the current checked InputRow. No capture is made around operand accept, request, budget, result validation or storage checks. The legacy empty-physical-map case never receives a fabricated row0; the actual reported binding entry always supplies one checked physical row.
2. The actual services.read_input **Err** in eval_one_node: capture exactly the slot and copied InputRow passed to that callback, preserving whichever LocalError it returned. Successful malformed replies and normalization/retained-storage failures remain unsited.

Both capture functions return the same owned error immediately and write a site only if the recorder was empty. No ancestor wraps a whole subtree with its own identity. A successful kernel/reader never arms a location; a later resource/validation failure is therefore not blamed on that successful operation. Legacy eager FnCall errors remain Unattributed/None even if their code/pointer matches the203 kernel. Input1690 and input-returned ResourceLimit remain Input stage with their original causes. InputSlot still identifies a binding, not an inferred universally unique leaf.

The typed code getter is exactly the existing common ErrorInner::Evaluate→EvaluateError::code borrowed path; Storage/non-evaluation is None and Other stays10000. No message/downcast/panic-text parsing, native renderer or new caller Cargo dependency is present. The report owns only the error and scalar site data; no program/ctx/row/service/FieldType/warning borrow or clone escapes. The private recorder plus outer terminal map_err preserve the error/site pairing required by the staged review. Display delegates raw LocalError and does not add a source-shaped native view; std Error::source exposes that same LocalError.

Reporting does not inspect or mutate warning counters/details. Endpoint capture remains the caller's read-only before/after observation, on normal success or returned failure, using both live count and stored length. No C warning DTO, success wrapper, inferred cap, saturating delta, drain or warning-source attribution is introduced. Panic stays unwind, with a new empty recorder on the next invocation. Native SQL diagnostic equality, pure caller source rendering and public activation remain separate gates; C3b/C3c are still deferred.

### Seventeen tests and ownership evidence — NOT executed by C

`local/diagnostic_tests.rs` adds **17 test functions**: four pure type/ownership/code tests and13 runtime/compatibility tests. The anticipated native total is **520 + 17 = 537**, not a claimed run. Tests cover:

- The same original common Box address/payload through capture/getters/into_error, raw Display/source behavior, a Storage DisplayBomb proving getters do not render, actual code1690/−7/0/1300/9007/Other10000/StorageNone and every unsited error class.
- First capture wins in both Input→Kernel and Kernel→Input decoration attempts, retaining the same moved error; source records retain distinct ordinals despite equal high-bit source IDs.
- Actual nested call2 versus successful inner then root0 overflow; root-only input fast path; a shared binding slot's second occurrence without a guessed leaf/parent; all callback LocalError variants; repeated `[2,0,2]` with separate occurrence2/physical2; reports outliving program/spec/service owners.
- Malformed successful input reply, schema/selection/host preflight (also empty), budgets before/after successful reads, constant budget7 versus actual kernel error at8, and the paired budget10 witness: inner1+1 succeeds then outer accept11 is Resource/None, while innerMAX+1 at the same tick10 is an actual Kernel report.
- Failure→success→empty→validation failure→input failure→panic→retry without stale data; NULL/unselected poisoned subtrees;33/64/256-depth reporting and teardown on256KiB stacks.
- Caller-owned warning endpoint/prefix checks for cap0/1/default/full, and a default cap0 receiver while cfg cap remains7; no panic-warning-delta or overflow-at-MAX assumption. Old eager222 remains unsited, old/reported controls retain demand, old host succeeds after new reported-host refusal, and host-free hooks trap any consultation.

No test, Cargo/build, lint or benchmark was run by C or a helper. Parent alone runs the serialized native and caller comparison gates. No result from the17 new assertions is inferred from their source/formatting.

Exact helper ledger: b7c8ccdf-dd84-47b6-a455-bcdb35a78804 wrote **only NEW diagnostic_tests.rs**, formatted it and explicitly handed it back/stopped. C wrote diagnostic.rs and, only after the parent unlock, mod.rs/batch.rs/expr_eval.rs. 91e4e4af-4fda-47c5-9293-984573303fea's pure-type review was delivered read-only; its later integration review turn failed without a final finding. A fresh-context fallback audit by 0bdc100b-a51b-4903-b7a4-42f8ec917b19 read all five files/relevant support and all17 tests, found no concrete source/fixture/lifetime blocker, and completed/stopped without edits/builds/tests. It independently verified the two capture points, first-write/terminal propagation, optional reborrows, unchanged legacy wrappers, host ordering, raw Box ownership, stage/code behavior, warning caps and budget-tick assertions. Neither reviewer had a diagnostic write lock; static findings are not native execution evidence. All writers/reviewers have handed back and stopped. No other product, error representation, dependency, caller, profile, prepared metadata, guide, plan, old reuse tree or Git history was edited.

### Exact C3d source manifest

Pinned nightly rustfmt (`--edition 2021 --config skip_children=true`) over exactly the five paths and scoped git diff --check exited0. After that pass, the SHA256 of this ordered sha256sum manifest text was `9fd38c3284ba02a9676e03a96f8f93e2d9a68677c26917f4b5f659e10bada099`:

```text
c6b9ef1e63de73de2892ffd47133e6864de5780e69ddbb867a88f445d719e4e5  components/tidb_query_expr/src/local/batch.rs
1703884cbc887c862331f42492457f71232b85d363712d642cf7900204109352  components/tidb_query_expr/src/local/diagnostic.rs
04a56fcbf1611dffa23f78ef42fc2b511e9ce7fbe14e3729ce01bc5b59e30382  components/tidb_query_expr/src/local/diagnostic_tests.rs
b204b389485f423c2d58e63fd28db07f1cac21cd99f00035e8e945aeb1dbeb81  components/tidb_query_expr/src/local/mod.rs
158f02e176638d6c6cac5188566d2f0569626c450fd737daa3df983a44ed4ada  components/tidb_query_expr/src/types/expr_eval.rs
```

## C3d-native-r1 — actual native GREEN, caller diagnostic gate pending

The initial parent cohort stopped **before compiling C3d** at a B-owned new parser fixture assumption (vertical-tab acceptance). Parent reports that it checked the pinned pre-parser implementation, corrected only that new fixture, and reran the cohort with datatype353 green. C made no parser/datatype/fixture/product changes and did not claim C3d had run during that stop.

Parent subsequently accepted actual C3d native results. C reread `logs/tikv-c3d-expr-full.log:466–482,610`: all17 new diagnostic tests are individually marked ok, and the full expression library reports **537 passed / 0 failed / 0 ignored / 0 measured / 0 filtered out, 2.88s**. C also reread `logs/tikv-c3d-aggr-full.log:121`: **40 passed / 0 failed / 0 ignored / 0 measured / 0 filtered out**. These are parent-run results, not a C-run command or an inferred filtered success.

Parent independently reproduced the ordered five-file source manifest hash `9fd38c3284ba02a9676e03a96f8f93e2d9a68677c26917f4b5f659e10bada099` and reviewed the report types/typed getter/failure recorder/shared batch core without a concrete finding. The API is now natively compiled and the local reporting tests are green. **All five C3d product files stay frozen; only this receipt changed after handback.** C has no queued source edit or active writer and still runs no Cargo/build/test.

The caller gate is waiting for D4's coherent checkpoint. C3d native success alone does not prove the later caller source-shape renderer, exact native SQL code/message/fallback behavior, warning severity/publication mapping, whole-caller comparison, migration/family completion or public activation. D4, C3b/C3c and any further source release remain parent-owned separate gates.

## C3d-caller-r1 — actual D4 private-view acceptance, no activation

Parent subsequently accepted the D4 caller cohort. C reread `logs/tidb-d4-diagnostics.log:2039–2051` (**10/10** diagnostic tests), `tidb-d4-d3-compat.log:2039` (**10/10**), `tidb-d4-d1-compat.log:2045` (**16/16**), `tidb-d4-d2-compat.log:2037` (**8/8**) and `tidb-d4-expr-full-comparison.log:3426–3432` (**1274 passed / four failed / 93 ignored**,1371 discovered/no filter). Parent reports the same complete baseline panic/assertion text, checked D4's four-file hashes/formatting, and accepted only the private exact203 native overflow view. Native baselines remain expression537/aggr40/datatype353. This is not an all-green full caller suite, public route, generic SQL error parity, context/severity or warning-source attribution claim. The five C3d products remain frozen.

## C3b-proposal-r1 — implementable control lineage and honest retained-heap accounting

**SOURCE/API DESIGN ONLY.** This refines the earlier C3b sketch into a bounded next cut; it does not grant a source lock. C did not contact or distract D4 implementation. Coordination/follow-up is through the parent, which owns the sole plan. No ordinary signature, public evaluator route, AST/native-batch profile or host payload is added by this proposal.

### 1. The smallest useful domain: SQLTypedRow selection controls, no203 composition

Keep the first entry **control/leaf-only**, even though C3a separately supports203. Binary AND/OR are included to exercise a real computed-result/OwnResult boundary without admitting arithmetic. Ordinary and Host frames remain in the same driver but are not executable members of a C3b program. This avoids claiming a general provenance stack for eager expressions.

| Exact admitted signature | Arity and roles |
|---|---|
| IfInt / IfString | 3; signed-Int predicate, then two values in the declared result family |
| IfNullInt / IfNullString | 2 values in the declared result family |
| CaseWhenInt / CaseWhenString | At least2 flattened searched condition/value arguments; optional odd trailing ELSE |
| CoalesceInt / CoalesceString | At least1 value in the declared result family |
| LogicalAnd / LogicalOr | Exactly2 signed-Int predicates; signed Int computed boolean result |

Every call uses its exact FunctionRef::TiPb identity and CallMetadata::None, through the existing canonical preparation/ControlKind path. Official IDs are **not PB provenance**. The new entry asserts SQLTypedRow for the complete source closure; D must prove absence of PB origin at **every** native node, not only the root. C checks local facts/shape consistency, not the truth of that native-origin assertion. PB—including otherwise similar IfNullString or Int controls—stays excluded because source dispatch and return-bit adaptation differ. AST, NativeNumericBatch,203/222, every other ordinary operation, NULLIF, CAST, HostCall, simple CASE, parameters/deferred/correlated/virtual inputs and generic dynamic-kind providers are negative admission cases. Old compile_local, compile_local_profiled, control/host and reported APIs retain their existing domains.

**Type rules:** value Int is LongLong only, signed or UNSIGNED bit transport. PredicateInt and computed boolean declarations are **signed LongLong**. Bytes declarations are the explicit VarChar/VarString/String/TinyBlob/MediumBlob/LongBlob/Blob family, not arbitrary EvalType::Bytes extensions. Refuse ARRAY, BIT/ENUM/SET/hybrid domains and hybrid flags, Real/Float32/Decimal/temporal/JSON/vector, Tiny/untyped-NULL retagging and cross-family coercion. Preserve complete FieldTypes and compare complete slot/schema types; D also retains the full detached SQL types and rejects excluded/unrepresentable SQL facts rather than masking flags.

Value branches need the correct family, **not equality with the parent's entire FieldType**: signedness/collation differences are exactly why selected materialization metadata is needed. Propagate PredicateInt restrictions through every potential selected producer, not merely an intermediate signed return FT. A signed-return IF that could forward UInt is allowed as an Int **value**, but rejected as a predicate in this deliberately stricter first cut. D binds signed/unsigned leaf declarations to actual Int/UInt source kinds. Int/typed-NULL constants require LiteralKind::Typed. Bytes constants allow proved String/Text or ordinary Bytes/Typed; carrier-correct typed NULL is allowed. **BinaryLiteral is initially rejected**, even though B can transport it. `_binary` text and binary/blob column declarations must not be guessed into Bytes/BinaryLiteral kinds; actual Chunk string/blob reads produce String.

### 2. Concrete immutable producer/ID API and validation

All names below are proposed, exported only from local; fields stay private and getters immutable:

```rust
pub struct ResultMetaId { /* unit: u64, record: u64 */ }
impl ResultMetaId {
    pub const fn new(unit: u64, record: u64) -> Self;
    pub const fn unit(self) -> u64;
    pub const fn record(self) -> u64;
}
// ResultMetaId: Copy, Eq, Ord, Hash; it is not a pointer or value-cache key.
pub enum LineageCarrier { Int, Bytes }
pub enum ControlProducerRole { Constant, InputSlot, SelectedControl, ComputedBoolean }
pub struct ControlProducerFact { /* ordinal, id, carrier, role */ }
impl ControlProducerFact {
    pub fn constant(ordinal: usize, id: ResultMetaId, carrier: LineageCarrier) -> Self;
    pub fn input_slot(ordinal: usize, id: ResultMetaId, carrier: LineageCarrier) -> Self;
    pub fn selected_control(ordinal: usize, generated_null: ResultMetaId,
                            carrier: LineageCarrier) -> Self;
    pub fn computed_boolean(ordinal: usize, own_result: ResultMetaId) -> Self;
    pub fn ordinal(&self) -> usize;
    pub fn id(&self) -> ResultMetaId;
    pub fn carrier(&self) -> LineageCarrier;
    pub fn role(&self) -> ControlProducerRole;
}
pub struct ControlLineageFacts { /* immutable flat snapshot and producer facts */ }
impl ControlLineageFacts {
    pub fn sql_typed_row(spec: &LocalExpr, schema: &[FieldType],
        producers: Vec<ControlProducerFact>, limits: CompileLimits)
        -> LocalResult<Self>;
    pub fn namespace(&self) -> u64;
    pub fn node_count(&self) -> usize;
    pub fn producers(&self) -> &[ControlProducerFact];
    pub fn schema(&self) -> &[FieldType];
}
```

IDs name records in a **caller-owned materialization table**, not native source IDs, logical row indexes, child links or execution handles. Require one caller namespace/unit and a unique ID per producer occurrence in this first cut; sparse/high record numbers are valid and must never size an allocation by their maximum. Namespace uniqueness across native plans is D's ownership contract, not a C-generated global/cryptographic identity. Equal payloads, equal native source IDs or repeated physical rows do not merge producers. D must not join an ID against an unrelated table with coincident numeric keys.

Ordinals count **all** source nodes in preorder, root0 and children left-to-right. Exactly one correctly typed producer record must occupy each ordinal0..N−1 in that order. Reject missing/extra/out-of-order facts, duplicates/foreign units, role/node mismatch, wrong carrier/role/type, metadata or signature/arity violations, including dead children. computed_boolean fixes Int; selected_control's ID is that node's own **generated-NULL fallback** record, not a rule saying every successful result originates there. C derives checked flow from the node/profile; there is no arbitrary caller-supplied transfer policy for a function.

As in C3a, retain an exact flat snapshot of literal Int/Bytes payload and NULL/LiteralKind, input slot identity, call signature/arity/None metadata, every complete FieldType and the whole declared schema. Compile rewalks actual source under current limits and rejects stale values/types/shape/order/schema/facts. Bound node/depth/scheduled-child counts and fixed record/index reservations before copying descriptors. No recursive LocalExpr clone, executable child links, callback, kernel pointer or shadow evaluation graph enters these facts.

CompileLimits bounds tree counts/depth, **not arbitrary literal/FieldType/source-table byte size**. Immutable source/spec/program metadata remains outside evaluation scratch. D must apply an explicit source/metadata/literal byte policy before building/cloning these snapshots; do not infer it from declared flen or claim max_nodes is a total compile-heap cap. If a C-owned source-byte cap is later required, add it as a separate new facts/build option, not a field in frozen LocalCompileContext or an undocumented default.

### 3. Caller records: actual source identity versus computed boundaries

D owns an immutable ResultMetaId→record table beside the matching LocalExpr/facts. Each record contains admitted carrier, B ValueMetadata, deep-detached complete SQL FieldType, exact native source identity/origin, and SourceValue / GeneratedNull / ComputedBoolean role. It contains no executable children or native expression callbacks. Validate every ordinal/ID/role/carrier/projection join before compilation and keep the table/facts/worker ownership together.

- Source records come only from strict constant.literal_value or the actual Chunk accessor's fixed non-NULL kind/collation contract. B value.rs already preserves UInt bits and String/Bytes identity out-of-band; C does not add a TiDB datatype dependency or duplicate Datum semantics.
- Generated NULL has an explicit nullable record (kind Null, no collation/Decimal shape) with the generating parent's carrier/declaration. Computed boolean has an **Int computed-boundary** record even when its value is NULL or coincides with a leaf's0/1.
- The parent's declaration-only return FieldType is not the selected ValueMetadata. A selected String retains its actual StringDatum collation/raw bytes; Bytes remains Bytes; UInt MAX remains unsigned at materialization even under a signed declared result when the SQLTypedRow passthrough contract requires that.
- An outer selection forwards the **current child record**. If it selects an inner AND, that is the AND's OwnResult ID—not either original operand's source ID. A later ordinary composition would likewise need its own computed-boundary rule, never an operand-ID shortcut.

The bridge is used at the boundaries only: one demanded to_scalar import and one from_scalar materialization using the chosen record. There is no Datum↔carrier roundtrip inside control frames, no condition rerun/value comparison to infer origin, and no native coerce_to_ret_type/convert_to escape.

### 4. Thin facade, one compiler and one driver

Proposed additive facade:

```rust
pub fn compile_control_with_lineage(
    spec: &LocalExpr, schema: &[FieldType], cx: LocalCompileContext,
    facts: &ControlLineageFacts,
) -> LocalResult<LocalControlProgram>;

pub struct LocalControlProgram { /* private LocalProgram */ }
impl LocalControlProgram {
    pub fn return_type(&self) -> &FieldType;
    pub fn eval_with_bindings(
        &mut self, limits: ExecutionLimits, ctx: &mut EvalContext,
        physical_rows: usize, selection: &[usize],
        services: &mut dyn LocalRuntimeServices,
    ) -> LocalResult<LineagedBatch>;
    pub fn eval_with_bindings_reported(
        &mut self, limits: ExecutionLimits, ctx: &mut EvalContext,
        physical_rows: usize, selection: &[usize],
        services: &mut dyn LocalRuntimeServices,
    ) -> Result<LineagedBatch, ReportedLocalFailure>;
}
pub struct LineagedBatch { /* private values: VectorValue,
                              result_metadata: Vec<ResultMetaId> */ }
impl LineagedBatch {
    pub fn values(&self) -> &VectorValue;
    pub fn result_metadata(&self) -> &[ResultMetaId];
    pub fn into_parts(self) -> (VectorValue, Vec<ResultMetaId>);
}
```

Exactly one ID accompanies each selected occurrence, in output order; empty matches empty. No Deref/into_local_program/raw-RPN escape may silently discard required lineage. Initially omit a decoded-column facade because native kind/collation checks belong to the demanded adapter. Existing public LocalExpr, LocalCompileContext, LocalRuntimeServices, HostCatalog, ordinary/profile APIs and public VectorValue/RpnStackNode/kernel ABI stay unchanged.

**Compiler:** add a private Lineaged admission mode to the existing iterative compiler, not a second compiler/registry. Its closed checks replace only that mode's old signed-Int gate. Reuse prepare_call/canonical selector/validator/metadata and into_control; verify exact ControlKind/signature, unit metadata and identity retained-argument order. No eager or logical-chain flattening occurs on this route. Capture source ordinal at Visit, before descendants advance the counter.

Each actual RpnExpression subprogram in this route contains one structured node and a small **private optional** annotation, derived from checked facts:

```rust
enum CheckedResultFlow {
    Leaf { id: ResultMetaId, carrier: LineageCarrier },
    PreserveSelected { generated_null: ResultMetaId, carrier: LineageCarrier },
    OwnResult { id: ResultMetaId }, // admitted Int boolean only
}
```

Old/wire From<Vec> construction defaults to no annotation. Checked attachment requires one node; mutable/raw structural seams invalidate annotation along with metadata, and the lineaged facade exposes none of those seams. A lineaged entry must reject missing/inconsistent annotations, not silently execute with guessed tags. This is metadata on the single executable graph, not another graph.

**Driver:** introduce a private FrameResult { node: RpnStackNode, meta: Option<ResultMetaId> } only in the shared return/selected-value channel. Keep ProgramFrame.stack as Vec<RpnStackNode> so eager kernels still receive their unchanged slice without an unsafe layout cast or temporary operand-vector conversion. An annotated singleton Program frame needs one return-tag slot plus its checked annotation. Construct Program frames from the RpnExpression itself, not only child.as_ref(), so child annotations survive descent. Seed a leaf's ID when it actually produces its value, including the primitive singleton fast path. Legacy multi-node programs remain unannotated; this cut does not invent a general eager operand-lineage stack.

Nonlogical ControlFrame in lineaged mode retains/moves an optional selected FrameResult instead of collapsing it into Option<Int>. Conditions alone read Int truth; IFNULL/COALESCE inspect NULL carrier-neutrally. Before retagging, validate singleton shape, expected carrier and the complete child FieldType against that actual child expression. Then move the selected node/ID, rebinding **only** its FieldType reference to the declared parent return type. Constants keep their compiled borrow; generated/input values retain their owned buffers. IDs are Copy and borrow no service/ctx/recorder. Preserve the legacy nonlogical Int result policy when annotation is absent rather than changing its representation under an old entry.

Precise result rules:

| Runtime outcome | Result metadata |
|---|---|
| IF chooses either value, including NULL | Selected child's current ID |
| IFNULL chooses its second NULL child | That second child's ID |
| Searched CASE has no match and no ELSE | Own generated-NULL ID and carrier-correct NULL |
| COALESCE exhausts all NULL children | **Own generated-NULL ID**, not the last child's ID; native SQL returns a fresh NULL |
| AND/OR computes0/1/NULL | Own computed-Int ID, regardless of equal operand bits |
| Outer selection of an inner computed/generated result | Forward that inner current ID, never rewrite to an older leaf |

Ordinary and Host frames stay in this **same loop** with their unchanged bare operand stores. For their legacy paths, results carry meta=None, including ordinary left-NULL finish; reject an unexpected tagged operand instead of silently pretending it is valid composition. C3b admission forbids those nodes. A future203 composition would require OwnResult at both kernel and NULL-stop plus a separately checked mixed-domain contract. No new prepared ordinary metadata or host payload is part of this release.

### 5. Unchanged service ABI, demanded source checks and C3d receipts

At the same one demanded read, D reads the original Datum, checks/transports it using B, and compares its **non-NULL** kind/collation to the immutable source contract for that slot/producer. NULL is allowed without inventing String collation on Datum::Null. No eager scan of dead rows/branches, extra metadata callback or second read recovers lineage. Parameters/correlated/generic variable-kind providers remain inadmitted. C validates the returned singleton carrier, including carrier-correct NULL; same-family erasure is not permission to import another kind or coerce it.

For the new checked mode, adopt an owned, type/shape-correct singleton only after measuring and admitting its **actual retained capacity**. Extra provider capacity counts; an overlarge returned owner may be refused even when logical payload is short. This avoids an unconditional Bytes normalization copy. If materialization from a borrowed constant/reference is necessary, reserve/check the destination, then use borrowed push_ref. Do not mechanically widen `from_scalar(&scalar_ref.to_owned(),1)`: scalar.rs:214–221 clones Bytes once, vector.rs:42–58 clones it again, and compact byte push copies into its own data buffer. Selection itself performs none of those copies.

The batch collector has an explicit lineaged carrier mode but one existing occurrence loop: accumulate values plus ID capacity before starting the next occurrence. D validates output length/IDs/namespace/carrier against the owning table and materializes directly from the borrowed scalar ref with that record. The declared result SQL FieldType stays separate. Raw invalid-UTF8 bytes are preserved; no decoder/re-encoder is introduced.

C3d semantics stay exact: only an actual read_input Err gets Input(slot,row), including a D kind-contract rejection; a successful malformed reply, memory refusal or preflight remains unsited. No fabricated control-kernel/ancestor site, warning mutation, native renderer or replay is added. The optional recorder lifetime is independent of FrameResult. Host hooks are never consulted on this closed domain. Repeated physical rows remain distinct occurrences and are not memoized.

### 6. Exactly two datatype helper files — actual capacity, plus controlled fill

Actual source shows why the current public capacity methods are unusable as byte-heap accounting: ChunkedVecBytes owns data/bitmap/var_offset (`chunked_vec_bytes.rs:7–11`); capacity():96 reports max(data.capacity(),length). BitVec.capacity():66 reports **initialized word length×64**, not the allocation. Truncate frees neither allocation; append donors retain data/bitmap capacity and replace offsets with a fresh sentinel Vec. NULL/empty payloads still require row offsets/validity and must not be conflated.

Proposed inherent getters, with checked arithmetic and no changed old capacity() meaning:

```rust
// codec/data_type/bit_vec.rs
pub fn retained_heap_bytes(&self) -> Option<usize>;
// = self.data.capacity().checked_mul(size_of::<u64>())

// codec/data_type/chunked_vec_bytes.rs
pub fn retained_heap_bytes(&self) -> Option<usize>;
// = checked(data.capacity()
//         + var_offset.capacity()*size_of::<usize>()
//         + bitmap.retained_heap_bytes()?)
```

These count Vec element-buffer allocations, not initialized/logical lengths, inline struct headers, allocator usable-size/overhead or total process/host heap. Int needs **no third datatype file**: ChunkedVecSized.capacity() is its data Vec capacity and existing ChunkRef::get_bit_vec exposes the bitmap, so query_expr can add their actual checked sizes for Int.

**Getter-only is insufficient for controlled growth before copying.** The same two helper files must also supply narrow fallible scaffolding/reservation if Bytes materialization is included in this first cut:

```rust
// ChunkedVecBytes: no public VectorValue/ChunkedVec trait change.
pub fn try_with_capacities(rows: usize, data_bytes: usize)
    -> crate::codec::Result<Self>;
pub fn try_reserve_append(&mut self, additional_rows: usize,
    additional_data_bytes: usize) -> crate::codec::Result<()>;

// BitVec, used only inside its sibling datatype implementation:
pub(super) fn try_reserve_len(&mut self, total_bits: usize)
    -> crate::codec::Result<()>;
```

Check row/data additions, rows+1 offsets, element-size/Layout limits and bitmap rounding **before** allocating. Round bits as bits/64 + usize::from(bits%64!=0), not an overflowing bits+63. Start empty Vecs and fallibly reserve data/offset/bitmap/sentinel storage with try_reserve_exact, then fill. Reservation must leave logical lengths/contents unchanged on Err, but earlier successful reservations may retain increased capacity: no capacity rollback claim. C aborts/drops the failed evaluation or recomputes its actual charge before any further work. Helper allocation/layout failures use existing codec Other and are explicitly mapped to LocalError::ResourceLimit, not numeric SQL overflow/warnings; no new dependency or error variant is needed.

After successful reservation, a push_ref of exactly the reserved row/payload extent must allocate no more data/offset/bitmap storage. Use this path for checked Bytes construction/output, not unchecked writer/whole-vector append growth. Existing infallible constructors/writers/legacy behavior stay unchanged. Helpers and focused overflow/retention/failure tests belong in the same two files; no public VectorValue, ChunkedVec trait or chunked_vec_sized.rs edit is requested.

### 7. Static and dynamic memory proof — deliberately not a hard allocator cap

**Accepted-boundary guarantee proposed for C3b:** exact measured retained Int/Bytes/ID/frame storage is checked before each subsequent semantic effect and before successful publication; no later read/kernel/host callback runs after a rejected acquisition/growth. This is **not** a guarantee that allocation can never transiently exceed the number or that the incoming callback itself was pre-bounded. If a hard pre-allocation/peak guarantee is required instead, do not release Bytes under this API: a trusted payload bound/budget-aware producer/sink or bounded allocator is a separate, larger contract.

Static proof covers checked shape, arithmetic, source roles, demand and required minimum scaffolding. For R rows and D payload bytes, the initialized Bytes layout needs at least `D + (R+1)*sizeof(usize) + (R/64 + (R%64 != 0))*sizeof(u64)`, including the offset sentinel for R=0. This is **not an upper bound on Vec allocation**. Row count, declared flen, requested capacity and SQL NULL do not bound arbitrary payload/capacity. Precheck known minimum frame/tag/offset/bitmap requirements and static constant byte lengths before allocations/effects; inspect actual reserved capacities before the first effect. Empty Bytes output may still need its sentinel and can legitimately refuse a too-small resource cap without reading anything.

Dynamic ledger in the new mode includes:

- Actual generated Int/Bytes owners, returned values, selected values retained by Control/Program, and every live operand/accumulator.
- Frame-vector capacity×actual enlarged EvalFrame size, operand/index/row/task-vector capacities, and fixed inline tag/header space through those struct sizes.
- **Actual current output** allocation, not output_rows charged as an Int vector, plus Vec<ResultMetaId>.capacity()×sizeof(ResultMetaId).
- Source and destination coexistence during necessary materialization; ownership moves transfer one charge, borrows create no new owner, release follows actual drop rather than a cleared logical length.

Use checked sums/multiplications; unrepresentable accounting is ResourceLimit. Before growth, reject impossible/requested minima; reserve fallibly without filling, then measure actual H'. Conservatively account other-live + old H + new H' while replacement buffers/source-destination may coexist (not just H'−H), and recheck before copy/append/next effect. After a no-growth reservation, do not invent an overlap allocation. Recheck final actual capacities before publication. `try_reserve_exact` promises neither exact capacity nor a hard allocator peak; post-reservation checks cannot retroactively prove no transient overshoot. Intermediate reservation/allocator and callback-owned transient allocation remain outside that stronger guarantee.

A read_input Bytes reply arrives **already allocated with unknown size**. Current services cannot reject its size before that same read/effect. Measure/check it immediately after shape/type validation, before retaining it across frames, copying it or running another effect; if over budget, drop it and preserve the already executed read/warning prefix. Never retry or inspect other rows to guess a bound. Adopting a within-budget owned reply avoids duplicate payload copies; borrowed constants are copied only into a reserved/measured destination. Do not accumulate all selected rows and discover output overflow only at final concatenation: enforce output/tag growth before the next occurrence.

Keep the existing conservative Int policy for old entry modes unless separately approved; the new lineaged mode uses actual Int/Bytes owners and measured output. Frame layout changes still require recalculating sizeof-based charges/tests. **Binary AND/OR exposes one extra exact-lock need:** LogicalAccumulator's current retained_bytes (`impl_op.rs:311–328`) guesses bitmap growth from value capacity. Add a narrow exact accounting accessor using the same checked Int helper for the new mode, while leaving the old conservative method and all logical kernels/semantics unchanged. Claiming full exact accounting while omitting this private accumulator would be wrong.

### 8. Exact prospective file/owner cut and staged gate

No product file is released now. The recommended complete C3b cut is **nine expression files plus two datatype files**, not the earlier optimistic eight-file sketch. Expression paths below are relative to `components/tidb_query_expr/src/`; the two datatype paths are relative to `components/tidb_query_datatype/src/`:

| Owner | Proposed file | Narrow purpose |
|---|---|---|
| C | NEW local/lineage.rs | IDs, immutable producer facts, closed admission, thin facade/types |
| C | NEW local/lineage_tests.rs | Domain, source flow, demanded checks, budgets/report compatibility |
| C | local/compile.rs | One checked lineaged compiler mode and per-subprogram annotations |
| C | local/batch.rs | Same occurrence loop, carrier/output/ID collection, thin raw/reported wrappers |
| C | local/runtime.rs | Exact admitted-owner arithmetic/dynamic output ledger and resource mapping |
| C | local/mod.rs | Additive exports/module wiring |
| C | types/expr.rs | Private annotation, invalidation, iterative ownership/metadata behavior |
| C | types/expr_eval.rs | Shared FrameResult/Program/Control flow, imports and effect barriers |
| C, explicit parent loan | impl_op.rs | **Accounting accessor only** for private LogicalAccumulator; no kernel change |
| B/parent loan | codec/data_type/bit_vec.rs | Actual word allocation getter and checked fallible reserve helper/tests |
| B/parent loan | codec/data_type/chunked_vec_bytes.rs | Three-buffer getter, fallible capacities/reservation and tests |

Preserve spec.rs public shape, registry.rs, profile.rs/C3a admission, diagnostic.rs/C3d report semantics, host.rs/protocol, types/function.rs, selector lib.rs, impl_control kernels, RpnFnMeta/codegen, Cargo and all public native routes. A clean staged release can validate the two datatype helpers first while new lineage files remain unwired, then unlock the existing expression files for one coherent integration. If the helpers/accounting loan is unavailable, Bytes stays inadmitted; there is no guessed-budget fallback.

D's later private lineage lowerer/adapter/materializer and tests need their own explicit caller file release (prefer narrowly named lineage siblings under tikv, with only necessary shared metadata-helper exports). B value.rs transport remains unchanged. D4's accepted overflow-view implementation is not edited or tasked by this proposal. Parent owns guide/plan updates and all native gate serialization.

### 9. Required tests and caller obligations

- Closed positive signature/arity/role/full-type matrix; wrong/dead child, stale literal/schema/slot/order/ID/role/namespace, sparse high IDs, duplicate producer IDs, wrong source-tag/metadata-ID conflation. Reject whole-tree PB, AST/batch,203/222/control arithmetic composition, Host, BinaryLiteral and implicit casts before effects.
- Propagated PredicateInt proof: an intermediate signed result capable of forwarding UInt is a valid value but a rejected predicate; actual demanded source contracts must match before type erasure. Declared family alone is insufficient.
- Same bits/bytes but different source records: UInt MAX under signed SQL result, String(collation A) versus Bytes under result collation B, binary/blob column still String, invalid UTF8 bytes unchanged. No numeric narrowing/UTF8 roundtrip or ancestor-leaf provenance rewrite.
- IF/IFNULL selected NULL versus generated CASE/no-ELSE and all-NULL COALESCE IDs (including Bytes(None)); AND/OR OwnResult0/1/NULL; outer selection forwarding the current computed/generated ID. Poisoned branches, once-only predicates/reads, no second metadata read.
- []/1/1024/1025/reverse/`[2,0,2]`: exact values/ID alignment and per-occurrence effects, no memoization, resource failure stops later occurrences. Demanded kind/collation failure is Input report; successful malformed reply and storage failure stay unsited; host hook/warning prefix/C3d attribution unchanged.
- Helper actual-capacity formulas after unused preallocation, over-reservation, clone, truncate(0) and append donor; NULL versus empty, offset sentinel/R+1 and bit boundaries0/1/63/64/65. Check usize/Layout overflow before writes. Deterministic reserve-stage failure tests must preserve logical contents while observing any successful earlier capacity growth; use a scoped test-only reservation seam if necessary, not a global allocator/dependency change.
- Budget tests include long payloads, many empty/NULL strings, offsets/bitmap/ID capacity, incoming huge spare capacity, source/destination and old/new overlap, per-row/final-row output growth, and actual-capacity rather than requested/logical-capacity assertions. A successful read followed by size refusal retains that read's warning prefix and runs no later effect; do not falsely assert that the same callback was prevented.
-33/64/256-depth facts/prepare/eval/metadata/Drop on256KiB stacks, no recursive source clone/renderer; old C2/C3a/C3d entrypoints and native537/aggr40 plus the then-current caller baseline rerun only by parent after release. No performance, total-heap, warning-site, PB reinterpretation, binary-literal, ordinary-composition or public activation claim is earned by these tests.

Caller obligations are explicit: authenticate SQLTypedRow at every source node; retain full detached SQL metadata and source byte policy; bind the matching namespace/table/facts/worker; assign fixed source kind/collation contracts and validate them on the same demanded read; keep declaration and selected materialization metadata separate; validate output IDs/count/carrier before from_scalar; preserve raw errors/diagnostics and C3d's real binding-versus-leaf distinction. Representation transport alone is not wider admission.

### C3b-r1 design receipt and validation

Only `runtime-contract.md` changed in this turn. Read-only helper0106e623-3470-47dd-967e-cffd60f7a402 inspected actual byte/bitmap/Int storage and fallible-growth proof limits; helper93a02a6a-613c-47a9-871d-b6d75d7b34f3 refined producer facts, role admission and frame propagation. Both delivered findings and stopped with no writes, formatting, builds, tests, Cargo or D4 contact. C reconciled their file lists by adding the explicit **impl_op.rs accounting-only** loan required for the chosen AND/OR domain; the earlier eight-file claim would omit that private accumulator. The hard pre-allocation/callback-size limitations are not hidden behind an estimated logical capacity.

C reread the source/bridge/driver witnesses and actual D4 logs above. The document whitespace/conflict grep `[\t ]+$|^(<<<<<<<|=======|>>>>>>>)` is the scoped text check. This read-only command from the TiKV root exited0 and reproduced the frozen C3d checksum `9fd38c3284ba02a9676e03a96f8f93e2d9a68677c26917f4b5f659e10bada099`:

```sh
set -o pipefail; sha256sum components/tidb_query_expr/src/local/batch.rs components/tidb_query_expr/src/local/diagnostic.rs components/tidb_query_expr/src/local/diagnostic_tests.rs components/tidb_query_expr/src/local/mod.rs components/tidb_query_expr/src/types/expr_eval.rs | sha256sum
```

No C3b product/API/test has been implemented or run. Current C3d products, datatype/caller sources, guides/main plan, old reuse tree and Git history remain untouched by C. Parent must choose/release the exact scope and the stated accepted-boundary memory contract before implementation; stronger hard-heap/budget-aware producer guarantees, D's caller integration, C3c batch scheduling and public activation remain separate decisions.

## C3b-stage-a-r0 — datatype helpers coherent/frozen, expression integration locked

Parent accepted C3b-r1, including its measured-retained/before-NEXT-effect contract (not a hard allocator peak or pre-bound unknown callback), and the prospective nine-expression/two-datatype scope. **Stage A released only** `codec/data_type/{bit_vec,chunked_vec_bytes}.rs` plus NEW `local/{lineage,lineage_tests}.rs` and this receipt. All seven existing expression integration files, including mod.rs, remain frozen/unwired until the parent datatype-helper gate. B owns decimal.rs only; C's two datatype helper loans do not authorize any other datatype or caller edit. No C/helper Cargo/build/test run is permitted or claimed.

### Stable datatype API and first coherent handback

Writer0106e623-3470-47dd-967e-cffd60f7a402 completed and explicitly froze/handed back only the two datatype files. C read the complete production helper blocks and independently reran scoped diff checking/hashing. The actual APIs now match the proposal: public checked retained_heap_bytes on BitVec and ChunkedVecBytes; Bytes::try_with_capacities and try_reserve_append; pub(super) BitVec::try_reserve_len. A further internal checked_word_len shares safe division/remainder ceiling and Layout preflight with the Bytes helper; no public type/re-export/trait/layout/VectorValue or existing capacity() method changed.

The whole count/layout/aggregate representability plan—including already retained capacities—is checked before the first reserve. New nonzero capacity uses try_reserve_exact; constructor's initial BitVec::with_capacity(0) is allocation-free. Offsets' zero sentinel is initialized only after all reservations succeed. Error::Other carries resource/layout/reservation failures, not SQL numeric overflow. Logical contents/lengths are unchanged on Err, but completed earlier reservations may retain capacity growth. Successful promised push_ref extents require no later implicit buffer growth. Actual aggregate retained bytes are rechecked after reservation; no exact-capacity, capacity rollback, transient-heap cap or unknown callback pre-bound is asserted.

**Seven new inline tests, authored but NOT run:** three BitVec tests cover retained allocation/word boundaries, append/clone retention and overflow-safe ceiling; four Bytes tests cover actual three-buffer accounting/preallocation/truncate/append/clone, NULL/empty/0/1/63/64/65/sentinel boundaries without push growth, full preflight overflow before a reserve, and injected data/offset/bitmap reserve failures with preserved values/partial capacity growth/retry. The narrow injection is cfg(test) thread-local with RAII reset, not a GlobalAllocator/dependency/runtime-service capture.

Pinned nightly-2026-01-30 rustfmt (`--edition 2021 --config skip_children=true`) over only those two files and scoped git diff --check exited0. Both source hashes were independently reproduced by C after handback:

```text
0367a8c8105b392f9e7f602d67b89930c4979c5ee96fbf2f3bd43bda323c3443  components/tidb_query_datatype/src/codec/data_type/bit_vec.rs
b2f09169097158dcab9e0f6bbf7719ea0a8deb148ad2da3fd2cbfc4788113d53  components/tidb_query_datatype/src/codec/data_type/chunked_vec_bytes.rs
```

The ordered two-file sha256sum-manifest digest is `25e75406a8832e4f9bdd539140625b56e5187d66afde6c204070cf3eb4b55d4e`. Parent has the explicit **DT2 coherent/frozen flag** for its focused MOD→full datatype→legacy RPN/aggr gate. With the parent-reported B361 candidate, the anticipated helper-inclusive datatype count is361+7=368; only the actual parent log may establish that count/result. No C build/test or parser/MOD fixture edit occurred.

### Unwired lineage facts/types, separate test authoring

Writer93a02a6a-613c-47a9-871d-b6d75d7b34f3 handed back stable NEW lineage.rs before proceeding to only NEW lineage_tests.rs. It contains the exact ResultMetaId/LineageCarrier/ControlProducerRole/ControlProducerFact and ControlLineageFacts constructors/getters above, crate-private **revalidate**(spec,schema,limits) and flow(ordinal)→Option<CheckedResultFlow>, thin private-inner LocalControlProgram with return_type, and LineagedBatch getters/into_parts. It contains no compile/eval integration and remains **unwired**. The final lineage_tests.rs handback contains23 tests targeting the future compile/eval API; these are uncompiled/unrun assertions, not execution evidence. Both new files are now frozen with no queued writer. The constructor additionally rejects zero root node/depth budgets before allocating its temporary fixed ID index.

The all-node producer coverage/namespace/unique sparse IDs, exact snapshots, source/metadata byte-policy disclaimer, and inherited PredicateInt restrictions match the accepted design. Independent reviewer0bdc100b-a51b-4903-b7a4-42f8ec917b19 read all737 stable lines and the accepted matrix, found no concrete source/API/type/role/bounds blocker, and stopped without writes/builds/tests; it did not inspect moving helper/test files. This is not a compiled expression API claim.

**Raw-flag clarification grounded in actual definitions:** TiPB exposes get_array(), so ARRAY is rejected directly. The native TiDB `field_type/mod.rs:91–149` defines SQL flag bits0..24 (NUM/GROUP share15); TiKV's FieldTypeFlag names only six of them, so from_bits_truncate/all() is not a valid complete mask. The new closed lineage gate preserves all accepted raw fields, explicitly refuses ENUM8/SET11/PARSE_TO_JSON18/ENUM_SET_AS_INT21 and unknown bits>=25, and refuses UNSIGNED5 for PredicateInt/computed booleans. This is a new-entry refusal, never masking or normalization, and does not alter an old checker/entrypoint.

### D5 source-only relay, no caller lock added

Parent relayed the actual native boundary: generic Expression::eval→strict Constant/Column DatumCell/ScalarFunction::eval→generic control→coerce_to_ret_type preserves same-family Int/UInt and String/Bytes identity. The typed eval_int/eval_string helpers erase that identity and are **not** the boundary this lineage cut models. String/varchar/blob Chunk reads produce String with the field collation. Native truthy_of **does accept UInt**; C3b's possible-UInt predicate veto is an intentionally stricter first admission slice, not a claim that native UInt truth is wrong.

D5 also found native TiDB FieldType equality/hash paths allocate element/marker snapshots. Its source-byte policy may require a separately requested checked native observer; that is not either of C's two datatype helper files, and C has not edited caller/type observers. Native SQL origin/kind/collation/source-byte proof and the future caller integration remain separately owned. Any later enlarged frame/header/resource cost is to be reported explicitly at integration, not hidden by rerecording old resource fixtures.

### Stage-A four-file source freeze

Both writer handbacks are complete. The new lineage tests include nine fact/admission cases plus runtime-targeted cases for selected UInt/Bytes records, exact NULL/OwnResult/current-boundary flow, demand/occurrences, C3d attribution, source declarations versus raw bytes, empty sentinel, provider/output/offset/bitmap/ID resource boundaries and deep lifecycle. They intentionally cannot be compiled as a wired module until the later approved integration lands. Stage A adds seven datatype tests and23 unwired expression tests, with **none run by C/helpers**. Old expression module/core files are still unchanged.

After each writer's pinned scoped formatting and handback, C independently hashed the exact four frozen files. The ordered manifest SHA256 is `6d05e4e9ad8682b49931dc9d89c7ee0126cb4af8f096f0443066b50b001b4484`:

```text
0367a8c8105b392f9e7f602d67b89930c4979c5ee96fbf2f3bd43bda323c3443  components/tidb_query_datatype/src/codec/data_type/bit_vec.rs
b2f09169097158dcab9e0f6bbf7719ea0a8deb148ad2da3fd2cbfc4788113d53  components/tidb_query_datatype/src/codec/data_type/chunked_vec_bytes.rs
4f434fd7dddcdf6b61bb61d1f7ebe7b9cd61eb6e25f9960ea9d249fc78c00c70  components/tidb_query_expr/src/local/lineage.rs
565158f7ca47dc761f221f530a540f2d658e792c706919f7bb2aa1c998c68d1e  components/tidb_query_expr/src/local/lineage_tests.rs
```

Parent received the explicit datatype-two-file gate flag first, then the full Stage-A coherent/unwired flag. Integration into the seven existing expression files remains pending its explicit post-helper-gate unlock. No helper result is a native pass, ABI/runtime integration claim, C3b family completion or public-route activation.

## C3b-source-r0 — Stage B coherent source, parent native gate pending

### Accepted helper gate and exact Stage B unlock

Parent accepted actual Stage-A results and explicitly unlocked exactly the seven existing expression files, together with the two new lineage files. C reread `logs/tikv-b22f-c3b-helpers-datatype.log:376` (**368/368**, no ignored/filtered), `tikv-c3b-helpers-expr-compat.log:610` (**537/537**, no ignored/filtered,3.08s) and `tikv-c3b-helpers-aggr-compat.log:121` (**40/40**). Parent reports focused MOD3 green and independently matched Stage A's four-file manifest. The two datatype helpers remained frozen throughout Stage B, with their original hashes below unchanged.

The nine-file expression integration is now coherent and frozen after source reviews/formatting. C and its children ran **no Cargo/build/test/lint/benchmark**. The new expression test inventory is23 lineage +6 annotation +4 runtime accounting +1 logical-accounting +2 compiler +2 driver-owner/layout = **38 tests**. The anticipated full native count is **537 + 38 = 575**, not a claimed run; actual parent logs decide. Caller/source-kind/SQLTypedRow authenticity and D5's integration/byte-observer work remain separate gates.

### Actual APIs, compiler and one-driver value flow

The proposed ResultMetaId, LineageCarrier, ControlProducerRole/Fact, ControlLineageFacts, LocalControlProgram and LineagedBatch APIs are now wired through local/mod.rs. compile_control_with_lineage revalidates the exact source/facts/schema/current limits before entering the **same** compiler via a private mode. A saved flow per actual output buffer is captured at Visit's all-node ordinal, attached through take_expression before child/root consumption, and checked against the canonical selector's signature/ControlKind/identity retention. There remains one prepare_call site; old legacy/profiled modes stay unannotated and retain their admission. No selector, function.rs, registry, host, profile, public LocalExpr/LocalCompileContext or codegen change was made.

RpnExpression now owns only a private optional checked flow. From<Vec>/old wire construction defaults None; consuming attachment requires one node. DerefMut, AsMut and into_inner invalidate the root annotation alongside structural metadata. Intact extracted child subprograms retain their own annotations until mutation; the public lineaged facade exposes no raw evaluator/inner escape. The driver refuses missing/inconsistent root/child flow-mode/shape instead of guessing provenance.

The official driver returns a private FrameResult(node, optional ID). ProgramFrame receives the actual RpnExpression, retains a bare Vec<RpnStackNode> for kernel ABI, and uses one copied tag slot only for an annotated singleton. Primitive leaves seed their compiled ID; selected Control results move their current node/ID and rebind only the declared parent FieldType after exact child shape/carrier/complete-FT/namespace checks. IF/IFNULL selected NULL keep source identity; searched CASE/no-ELSE and all-NULL COALESCE create their own typed NULL IDs; AND/OR use the same LogicalAccumulator and return OwnResult even for0/1/NULL. An outer selection forwards the current computed/generated child record. No value comparison, predicate rerun, native conversion or carrier→Datum→carrier roundtrip occurs.

Legacy nonlogical controls keep their old Option<Int>/generated-Int policy in the unannotated branch. Ordinary/Host continuations still use bare operand nodes, reject an unexpected tagged value and emit None; they are not admitted inside this new control-only route. Existing eager kernel helper/argument slices, host lifecycle, repeated-occurrence order, C3a demand and the two C3d error-capture points remain unchanged. Source proofs are assertions checked for local consistency; no actual native PB/source-kind authenticity is claimed by C.

### Actual ledger/collector behavior and compatibility limits

StorageMode separates old ConservativeInt from ExactLineage. Old int_storage_bytes/local output policy/unchecked wire/ticks/depth/tasks remain intact. The new mode uses actual Int data+bitmap and Bytes data+offset+bitmap allocations, checked minimum-layout helpers and measured dynamic output. LogicalAccumulator received an **add-only exact accounting accessor**; old retained_bytes and every kernel are unchanged.

Lineaged read_input keeps its existing callback/error seam, checks singleton carrier/length, then moves the returned owned buffer without a Bytes scalar clone chain. Actual capacity is checked immediately by the shared driver before retention across another effect/publication. A successful malformed reply or oversized retained owner is unsited validation/resource, not an input callback error. Unknown allocation inside that already executed callback remains outside a pre-bound guarantee; no callback replay/source metadata read is added.

The shared batch core has one occurrence loop and a private collector mode. Legacy materialization/append is retained in the old arm. The lineaged arm prechecks full-N ID and vector scaffolding, measures actual reserved ID capacity and actual output heap, and installs the standing output charge. Int/ID buffers are reserved for N. Bytes tracks cumulative logical payload separately, reserves row/payload capacity before push_ref, checks actual source/destination and conservative old/new allocation overlap, copies once into the reserved output, and rechecks actual output+ID storage before next occurrence/final publication. Empty Bytes includes the offset sentinel. Failure aborts/drops the local output; no partial batch is published.

The driver counts returned, popped, suspended and selected owners. Transfers do not clone payloads, and actual drop releases their charge. Exact-mode stack/frame reservations inspect actual capacities and conservatively include old/new raw-buffer overlap; their payload owners remain counted once. The admitted singleton logical profile never constructs partial pending row maps. Fixed inline tags/headers are accounted through actual frame sizes; there is no hidden parallel operand-ID heap.

**Two explicit resource-policy caveats accepted/reported to the parent:**

1. ProgramFrame's flow/return-tag fields and ControlFrame's flow/inline selected FrameResult can enlarge EvalFrame for **both** modes. Actual sizeof(EvalFrame) is charged, never a hardcoded old size. A fixed absolute byte budget can therefore refuse earlier despite unchanged conservative payload formulas/ticks. Passing old sizeof-based resource tests would **not** prove identical absolute-budget effect prefixes. Parent acknowledged those cutoffs are not version-stable when real layout grows; unlimited wire semantics have no such budget refusal. No old537 expectation has been edited. The new `test_lineage_frame_layout_storage_is_actual` prints current EvalFrame/ProgramFrame/ControlFrame/FrameResult/RpnStackNode sizes for the parent's eventual `--nocapture` gate and checks actual frame accounting; C has not measured them by running code.
2. ExactLineage measures retained owners, but reservations can be conservative: logical minimum precharge may exceed a first generated operand's move-only or in-place merge allocation, and old/new overlap may overestimate the allocator's true peak. The mode does **not** promise iff/optimal admission whenever live storage would fit. As accepted, it checks retained owners before the next semantic effect/publication, not a hard allocator peak, capacity rollback or unknown-size callback pre-bound.

**Authorized NEW fixture correction, before native run:** lineage_tests originally asserted at least one read for16,384 selected empty/NULL Bytes rows with128KiB. IDs alone require262,144 bytes, so parent explicitly approved changing only that new/unrun assertion to0 reads. The corrected test also uses independent required offset/bitmap minimum thresholds (not exact-capacity assumptions) and a larger-cap successful case. This is not a silent old-fixture rewrite. Other new fixtures cover payload growth and final-occurrence denial; actual native results remain pending.

### Reviews, ownership and checks

Independent reviewer0bdc100b-a51b-4903-b7a4-42f8ec917b19 traced the integrated compiler/annotations/driver/lifetimes, all23 lineage cases and compatibility seams, finding no concrete blocker. Reviewer0106e623-3470-47dd-967e-cffd60f7a402 separately traced actual owner/output/callback/overlap accounting and the two final private driver tests, finding no retained-owner omission or apparent test/lifetime defect; it explicitly raised the frame-layout and conservative-refusal caveats above. Neither audit is native execution evidence.

Exact Stage-B writer ledger:93a02a6a-613c-47a9-871d-b6d75d7b34f3 only compile.rs/mod.rs (plus2 tests), handed back;0bd only expr.rs (plus6 tests), handed back;0106 only runtime.rs/impl_op.rs accounting (plus5 tests), handed back; b7c8ccdf-dd84-47b6-a455-bcdb35a78804 only batch.rs, delivered complete handback before its harness closing failure; C owned expr_eval.rs, tiny private CheckedResultFlow accessors in the new lineage.rs, the specifically approved new lineage budget-fixture correction and this receipt. Reviewers subsequently stopped, all registry entries were ready, and no writer has queued source changes. The datatype helpers were not edited/reformatted during Stage B.

Pinned nightly rustfmt (`--edition 2021 --config skip_children=true`) over exactly the nine expression files and scoped git diff --check exited0. No additional file/dependency/guide/plan/caller/old-reuse/Git-history write, no public route or domain expansion, and no C/child compile/test was performed. Parent owns the native expression/aggr/caller comparisons and any response to actual fixed-budget fixture failures.

### Frozen Stage-B source manifest

Paths are relative to the TiKV root. The ordered nine-expression-file sha256sum manifest digest is `276b2df89cecdb379da3099ee1cff51d432f2569a850980c649d3a7e4862590b`:

```text
b9a61fd725db5622a9a00e10d836babf7e6c35f9ba9dd155b67c48ceeaf6ec66  components/tidb_query_expr/src/impl_op.rs
6dd172efc5244b85732fba3efc0fc4760c9e3b03eb853d006c671b512b80f23d  components/tidb_query_expr/src/local/batch.rs
b005b353c5d2ac041cd1d4b6ca42b9f52ec93587115311f6fa0fffa4ad2b73d0  components/tidb_query_expr/src/local/compile.rs
bdf57c75cd3740dc7e7b8568b9e78d614fe9595478211faaa7706afa6cfc5acb  components/tidb_query_expr/src/local/lineage.rs
42d323d19700adc2445acb10bba0fbe54dccf5ab12169d6a89b53910495bdbdd  components/tidb_query_expr/src/local/lineage_tests.rs
92ff1d1c4bccc053ac08095d86c6ed4f66f809e731d73775d6ad665c480162f7  components/tidb_query_expr/src/local/mod.rs
b01934e3f755f9a3d170061ff0f2dc3c6b3177db64965e1acaa6c09a9f219665  components/tidb_query_expr/src/local/runtime.rs
4fcef5de6cfcbebd29cb6b9a5bb5a39da28f3be39eeff8f1741b36c03305b1d8  components/tidb_query_expr/src/types/expr.rs
b62698e381b96e56dcc90fb4f17b43154e94dbba63bd038a5516bb4bea880f27  components/tidb_query_expr/src/types/expr_eval.rs
```

The frozen datatype hashes still match `0367a8c8105b392f9e7f602d67b89930c4979c5ee96fbf2f3bd43bda323c3443` (bit_vec.rs) and `b2f09169097158dcab9e0f6bbf7719ea0a8deb148ad2da3fd2cbfc4788113d53` (chunked_vec_bytes.rs). No full C3b/native/caller success or family/activation claim is made at this source checkpoint.

## C3b-native-r0 — parent acceptance, all product files frozen

The parent subsequently ran and accepted the serialized native gate against the exact Stage-B nine-file manifest `276b2df89cecdb379da3099ee1cff51d432f2569a850980c649d3a7e4862590b`, independently matching it and the scoped diff check. C reread these actual logs; C/children did not execute the gates:

- `logs/tikv-c3b-expr-full.log:648`: **575 passed,0 failed,0 ignored,0 filtered**,2.96s. This is the full expression gate, including all38 added tests and the previous537.
- `logs/tikv-c3b-aggr-full.log:132`: **40 passed,0 failed,0 ignored,0 filtered**.
- `logs/tikv-c3b-frame-layout.log:69–72`: focused layout test **1 passed,574 filtered**; actual sizes: **EvalFrame400, ProgramFrame128, ControlFrame400, FrameResult176, RpnStackNode152 bytes**. This one filtered test is only size evidence, not a replacement for the full575 gate. No old fixture adjustment was needed. The actual frame size is charged; neither these passing sizeof-based fixtures nor this measurement proves identical fixed-numeric-byte refusal prefixes versus the older layout.
- `logs/tidb-c3b-expr-full-comparison.log:3424–3456`: **1274 passed,4 failed,93 ignored,0 filtered**,10.18s. C read all four blocks; failures remain the catalog IFNULL column-collation assertion, EXP overflow assertion, duration-FSP conversion panic in vectorized builtin-op, and STR_TO_DATE partial-date mismatch. Parent reports a programmatic **FULL four-block** comparison with b22e differing only by thread IDs; this is parent-supplied comparison evidence, not a new C test run or a name-only comparison.

Parent now released D5's exact five caller files to D, not C. C's nine expression products and both datatype helpers remain frozen; their source hashes are unchanged. No public activation, wider native-kind/source authentication, whole-control/native-origin cohort or completed expression family follows: overall coverage remains **0/245 families**. Parent requested **source-only C3c design next**, covering true batch/control scheduling, entry-equivalence and metadata-lineage requirements. No C3c implementation, API change or file loan has been granted.

## C3c-proposal-r0 — source-only batch schedule and entry-equivalence design

**Recommendation for parent review, not authorization:** a separate explicit **SQL NativeNumericBatch** admission for a closed signed LongLong/strict typed Int-or-NULL/**PlusInt203-only** tree, initially at most1024 **selected occurrences**. Reuse the official compiler/frame driver and canonical kernel. Evaluate whole operand phases, then run only the parent kernel one lane at a time. Do not silently change any existing row/lineaged/wire entry by input size. This is a bounded semantic scheduling checkpoint, not SIMD/performance evidence or completion of any of the245 families.

### 1. Actual source contract: batch is not row demand repeated

Native paths below are under `tidb/rust/crates/tidb-expr/src/`:

- `scalar_function.rs:4075–4086` routes Int batches into **eval_integer_batch**. The exact signed-Int schedule is whole left subtree at4024, whole right subtree at4025, then the occurrence-ordered integer/NULL loop at4041–4063. NULL in a left lane **does not suppress its right subtree**. In contrast scalar Int arithmetic at1491–1503 stops after left NULL, matching C3a rather than this batch route.
- Recursive native eligibility is at3899–3945 and the public entry4197–4225, with numeric operator domains1217–1256. IF/IFNULL/CASE/COALESCE/AND/OR are outside this closure, including nested occurrences. General numeric conversion at3818–3865 belongs inside its complete operand phase, before the next operand; only identity conversion is proposed here.
- `evaluator.rs:400–439` uses NumericBatch only inside a vectorizable evaluator suite with vectorization enabled. If the root is unsupported, the **whole expression** uses the row evaluator; it does not batch a numeric subtree hidden inside a selected control branch. A nonvectorizable suite has row/select-list ordering at441–451. Thus root shape alone cannot establish the consumer schedule.
- Native constants broadcast once per nonempty chunk at `scalar_function.rs:3971–3978`. Its private Int worker calls get_row(0); the real public zero-row guard4222–4223 must be preserved. Parameters, deferred constants and correlated values may have different observation counts and are excluded initially.
- `tidb-chunk/src/chunk.rs:241–245,417–424` makes row count selection-aware and maps each logical occurrence through selection. Int import at `scalar_function.rs:4005–4010` follows that mapping, so duplicates/reordering remain distinct. Apply selection exactly once.
- Filters are a different entry: `evaluator.rs:133–172` uses all physical rows, prunes between filters and later intersects the input selection at248–256. Duplicate selection entries become membership, and even an empty input selection need not suppress a physical-row error. Its arithmetic fallback204–218 is scalar, not this NumericBatch entry. Grouping (`tidb-executor/src/vec_group_checker.rs:104–128,150–176`) evaluates boundary rows first and may skip NumericBatch entirely. **Both consumers remain outside the initial cut.**

Upstream Go vector controls are not evidence for a generic branch-mask scheduler: `pkg/expression/builtin_control_vec_generated.go:50–109,816–833,1204–1235`, `builtin_compare_vec_generated.go:1640–1666` and `builtin_op_vec.go:76–95,368–386` have full-child speculative work and possible scalar replay. C must not import replay, warning rollback or an invented universal masked-child rule. C3b's SQL TypedRow selection remains separate and unchanged.

C reread the principal Rust Int/eligibility/entry/evaluator sources and the frozen TiKV collector/driver. Read-only reviewer93a02a6a-613c-47a9-871d-b6d75d7b34f3 independently checked native consumers, controls, selection/broadcast and witnesses and recommended the same narrow cut. These are source findings, not new executed comparisons.

### 2. Closed admission and actual entry-equivalence obligation

Separate three facts; none implies the others:

1. **Structural purpose:** the prepared source graph and canonical203 selector shape, complete schema/types/values, all-node preorder source identity and identity retained arguments.
2. **Native consumer schedule/closure:** the caller actually reached SQL NativeNumericBatch for the whole expression under the current vectorization/suite decision—not TypedRow, PbRow, AST-value scalar, filter, grouping, an eager tile wrapper or an arbitrary provider claiming “batch”. Parsing SQL through an AST does not itself imply the excluded AST-value evaluation profile; the actual consumer matters.
3. **Invocation universe:** explicit physical row count plus the ordered selected occurrence map for this particular invocation. A duplicate physical row is still a distinct occurrence. Multiple output-expression order/publication remains caller-owned; one expression's local success cannot silently authorize an entire suite migration.

Proposed positive nodes: exact binary203/unit metadata, signed LongLong at all admitted value/result boundaries, immutable strict typed Int/NULL literals and genuine nonhybrid signed columns. Preserve complete FieldType/schema comparison and actual native-kind checks before transport erasure. No UInt reinterpretation, untyped-NULL retag, Tiny/hybrid/array, casts/coercion, other arithmetic signatures, hosts, mutable/deferred parameters, PB-row or AST-value profile substitution, or mixed profile/lineage tree is implied. All unsupported/stale facts, malformed selections and the proposed >1024 width refuse before value effects; no native retry after a local effect. The row facades' existing1025 support stays intact.

The entry-equivalence seal is caller-owned native evidence, not a boolean C can authenticate from a LocalExpr. C should bound/revalidate the immutable local facts and reject inconsistent profiles before dispatch. A future explicit factory/facade must make it impossible to invoke the same batch program through a row shortcut. Exact public names/types remain undecided; there is **no API implementation or release** in this proposal.

### 3. Same-driver phase schedule; localized kernel errors without replay

For each admitted call, the continuation sequence is:

1. Evaluate its entire left child program over the active occurrence map; perform the admitted operand conversion phase (identity here).
2. Keep that complete result live and charged while evaluating the entire right child/identity phase. Never use C3a's left-NULL early stop.
3. Only after both children succeed, execute the existing prepared203 helper **one occurrence at a time**, with singleton views into the already materialized operands (or immutable scalar broadcast). Collect this node's complete result before returning to its parent.
4. On the first actual error, retain the exact error/warning prefix, suppress later phases/lanes, and drop scratch. Do not reread inputs or recompute children to locate it.

This still has whole-subtree/operand-major native order. The singleton loop is **kernel-only**, not a call to the existing whole-root occurrence loop (`local/batch.rs:484–514`) and not whole-expression tiling. It is an intentionally conservative first scheduler, with no throughput claim. Input callbacks stay at demanded leaf phases, ordered by occurrence; do not eagerly decode/convert unrelated slots. Bindings may use the existing atomic singleton read seam repeatedly within that phase, without a new bulk callback or a second metadata callback. Constants remain immutable borrowed broadcasts, not N repeated provider evaluations.

This also resolves a concrete diagnostics gap without changing public kernel ABI: a multi-lane RpnFnMeta returns only Err, not its failing lane. C3d must never use rows[0], parse an error string, or rerun a failed vector to fabricate a row. Kernel-only singleton invocation after completed child phases gives the **actual** failing `(call, occurrence, physical_row)` at the same genuine kernel-Err capture point. Input errors still capture InputSlot at their actual read; slot identity is not a universally unique source leaf. Budgets/validation/post-success checks remain unsited. Any extension of D4's private native203 error view to a batch source requires its own D gate; no diagnostic/context/severity activation follows automatically.

Preserve the distinction between local materialized-lane index and original physical row: for selection[2,0,2], operand vector lanes are0,1,2, while InputRow physical values are2,0,2 and occurrences0,1,2. A temporary kernel Ref uses the local lane index; diagnostics use the original invocation coordinate. No deduplication, CSE or cross-occurrence cache is proposed.

### 4. Width, ownership and lineage constraints

The initial width cap avoids pretending that arbitrary large Generated nodes already work: `RpnStackNodeVectorValue::logical_rows_struct` indexes IDENTICAL_LOGICAL_ROWS up to its vector length, and existing driver entries assert the official1024 bound. Reject1025 before effects on this **new** route. Later larger support must retain full operand phases while tiling only each node's kernel phase and must prove new ownership/row-map boundaries; never tile the root program.

A future batch frame must charge complete left/right/intermediate vectors while suspended, accumulating result capacity, input callback/result overlap, frame/stack/map storage, output publication and conservative old/new growth overlap. Use actual-capacity primitives already accepted in C3b; no allocator-peak or unknown callback pre-bound claim. There must be one invocation budget, not one reset per lane/kernel tile. Minimum reservation over-refusal may be explicit, not hidden as an optimal admission promise. Real frame-layout growth must again be measured/reported and charged; old sizeof-based test passes are not absolute-budget-prefix equivalence.

**Do not use the lineaged entry merely to obtain its exact ledger.** Today private checks couple ExactLineage storage mode to an attached control flow and a singleton domain. C3c needs explicit schedule/admission independent of measurement policy, preserving all old guards rather than weakening annotation checks or passing a fake C3b flow. The precise private state/refactor needs parent approval before any edit.

For a pure signed numeric root, output materialization is computed Int/NULL under the checked declared numeric boundary; it is **not** passthrough provenance, even for plus(x,0), nor the ComputedBoolean role. This allows the initial numeric-only cut to keep its own explicit computed boundary without composing with C3b's record table. If IDs are later exposed for numeric output, they must name a genuine **computed-numeric own record**, not reuse a boolean/leaf ID or conflate OrdinarySourceId with ResultMetaId.

Any later batch-control/lineage composition additionally requires:

- a proved native entry/schedule for that control, not source-tree appearance;
- carrier and metadata alignment per active occurrence, including mask/scatter/duplicate maps;
- a private uniform-ID versus per-occurrence-ID representation (or equivalent checked sidecar), measured actual retained capacities and safe movement with its value owner;
- selected NULL forwarding versus generated NULL/boolean/numeric own IDs, with outer selection forwarding the current child boundary;
- immutable program/table binding, namespace/membership validation and declared SQL type kept separate from native materialization kind/collation.

The current C3b FrameResult's single optional ID cannot describe a vector selecting different producers in different lanes. Both `IF(c,plus(...),x)` and `plus(IF(...),x)` therefore remain negative gates. These requirements do not release Bytes/numeric mixing, unsigned metadata, native Go control replay or a public sidecar ABI.

### 5. Proposed proof matrix and staged handoff

Before implementation release, parent should approve the bounded numeric-only entry and the kernel-only singleton choice, with controls/filters/grouping/lineage composition explicitly deferred. Required future evidence:

- Preserve existing native scalar/batch divergence (`evaluator.rs:825–861`) and warning-order fixture (`scalar_function.rs:4604–4741`, especially4711–4722: scalar1x,3z,2y,4w versus batch1x,2y,3z,4w). These prove schedules differ; mixed conversion-warning producers are still outside the positive signed203 cut.
- Within the admitted width, distinguish two competing nested-child failures: the whole left child's later-occurrence error must suppress the right child's earlier-occurrence error. Also show a right-child failure precedes an earlier possible parent-kernel overflow, since parent work has not started. Left NULL must not suppress a demanded right child in batch.
- Use[]/1/1024/reverse/[2,0,2], constant-only/virtual rows and typed NULL, exact once-only leaf/callback traces and correct error occurrence→physical mapping. A fresh reporter must not retain stale sites after success/empty/retry. Validate source/schema/width before effects.
- Keep the documented all-signed1025 witness: plus(plus(L,1),plus(R,1)) with L[0]=0,L[1024]=MAX,R[0]=MAX. Full native phases fail left/1024; root tiles fail right/0. The first C3c cut must **refuse1025 with zero effects**, not claim positive equivalence there. Any later width extension must turn that into a real positive schedule comparison.
- Prove suspended-left/right/result/output/mapping storage refusal boundaries and final-publication atomicity; no unaccounted root tiling, per-lane budget reset or hidden materialization clone chain.
- Negative entry tests must include vectorization-disabled/nonvectorizable suites, controls in either nesting direction, filter-all-physical versus projection-selected/empty/duplicate behavior, grouping boundary reads, params/deferred/correlated leaves, PB/AST-value/mixed profiles, other signatures/carriers/conversions and improper computed/selected lineage.
- D owns actual native entry/source proof and values/errors/warning-prefix/materialization/publication comparisons. C's test provider trace is scheduler evidence, not native-origin authentication. No native fallback after errors or performance/family coverage claim is permitted.

Likely future seams are immutable batch-profile/facade facts and tests, shared `local/{compile,batch,runtime,mod}.rs`, `types/{function,expr,expr_eval}.rs` only as the approved representation requires. Existing profile files may need an explicit reloan rather than silent C3a admission widening. Arithmetic kernels, selector/registry/codegen, public RpnFnMeta/stack/value ABI, Host and datatype helpers should need **no** change for this first cut. This is a provisional scope map, **not a file-loan request treated as granted**; exact files/API decisions and guide ownership must be fixed by the parent before implementation. C owns no D5 files or main plan.

This C3c design turn changes only this receipt. C reread the repository/compute maintenance guidance but did not modify it, and ran no source formatter/build/test/lint/benchmark. The source audit helper stopped with no writes. C3b's accepted575 and all9Expr/DT2 source hashes remain the frozen baseline; C3c remains design-only with **0/245** overall family completion.

Document validation: file-content grep pattern `[\t ]+$|^(<<<<<<<|=======|>>>>>>>)` on this receipt returned no matches. From the TiKV root the exact read-only fingerprint check below exited0, returning the accepted9-file digest and unchanged0367…3443/b2f0…3d53 datatype hashes; it writes no source/build artifacts:

```bash
set -o pipefail
files=(components/tidb_query_expr/src/impl_op.rs components/tidb_query_expr/src/local/batch.rs components/tidb_query_expr/src/local/compile.rs components/tidb_query_expr/src/local/lineage.rs components/tidb_query_expr/src/local/lineage_tests.rs components/tidb_query_expr/src/local/mod.rs components/tidb_query_expr/src/local/runtime.rs components/tidb_query_expr/src/types/expr.rs components/tidb_query_expr/src/types/expr_eval.rs)
digest=$(sha256sum "${files[@]}" | sha256sum)
printf '%s\n' "$digest"
test "$digest" = '276b2df89cecdb379da3099ee1cff51d432f2569a850980c649d3a7e4862590b  -'
sha256sum components/tidb_query_datatype/src/codec/data_type/bit_vec.rs components/tidb_query_datatype/src/codec/data_type/chunked_vec_bytes.rs
```

## C3c-proposal-r1 / C3c-source-r0 — separate bounded SQL numeric-batch entry

### Approval, scope and evidence boundary

After accepting the principle in r0, the parent requested an exact API, private-state and minimum-file proposal. The resulting seven-file proposal was reviewed read-only and then **explicitly released for implementation**. This section supersedes r0's no-loan status, not its closed semantic cut. C4/E's separate evaluated-value/ASCII design is **not** implemented, exported or silently embedded here. Parent reports D5's18 targeted passes and full TiDB1292/four unchanged failures/93 ignored before this release; none is C3c validation.

Exclusive writers were partitioned as follows (all under `components/tidb_query_expr/src/`):

| Writer | Released files |
| --- | --- |
| 93a02a6a | `local/profile.rs`, `local/profile_tests.rs` |
| 0bdc100b | `local/compile.rs`, `local/mod.rs` |
| 0106e623 | `local/batch.rs`, `local/runtime.rs` |
| C | `types/expr_eval.rs`, plus this owned receipt |

No new source file was needed. `types/function.rs`, `types/expr.rs`, `impl_op.rs`, registry/selector/codegen, lineage files, Host/services/value ABIs, datatype helpers, Cargo/locks, native callers, guides and the main plan were not in the loan. The parent remains sole build/test runner and guide/plan owner. **Implementation and source audits below are unrun evidence**, not a C3c native acceptance or family-completion claim.

### Additive API and closed construction

The new public surface is deliberately separate:

```rust
OrdinaryCallSite::sql_native_numeric_batch(ordinal, OrdinarySourceId) -> Self
NumericBatchFacts::sql_native_numeric_batch(
    &LocalExpr, &[FieldType], Vec<OrdinaryCallSite>, CompileLimits,
) -> LocalResult<NumericBatchFacts>
compile_numeric_batch(
    &LocalExpr, &[FieldType], LocalCompileContext, &NumericBatchFacts,
) -> LocalResult<LocalNumericBatchProgram>
```

Facts expose only immutable `node_count()` and `call_sites()` views. The opaque worker-owned program exposes `return_type()` and the existing binding-evaluator argument list through `eval_with_bindings` / `eval_with_bindings_reported`, returning `VectorValue` / `Result<VectorValue, ReportedLocalFailure>`. There is no public raw-RPN/LocalProgram/inner/Deref/decoded/row escape, generic value-consumer callback, mutable mode knob, lineage result table or per-call operator selection. Raw numeric evaluation delegates once to the reported entry and moves back the same original error; it never retries evaluation.

`OrdinaryProfileSpec` retains its **row-only** consumer and per-child site gates. The batch facts require every exact203 call's all-node source-preorder ordinal, `NativeNumericBatch` profile and no PB signature; extra/missing/out-of-order/mixed sites refuse. The two fact types share a private flat `ClosedInt203Snapshot`, not a widened public admission function. Batch captures preflight borrowed source/schema/type/node/depth constraints before descriptor copies; compilation revalidates the exact snapshot under the current limits before shared preparation/copying. Strict batch types are signed LongLong throughout the source and complete schema: ARRAY and raw flags5/8/11/18/21 or unknown bits25+ refuse, while remaining documented bits are preserved. RowBaseline admission is not tightened or widened.

The compiler adds `CompileMode::NumericBatch` but retains the same canonical prepare-call/`into_ordinary` seam, exact203/unit metadata/identity retained[0,1], structured single-node child programs, iterative construction/drop and all-node source identity. Batch programs have **no** C3b result-flow annotation. Their output is a newly computed Int/NULL boundary, not passthrough/boolean provenance; there is no output-ID ABI in this cut.

Native entry and input origin remain a **producer assertion**. C cannot prove that an InputSlot is a genuine native column rather than a parameter, or that vectorization/suite/consumer decisions actually selected whole-expression SQL NumericBatch. D's later native gate must establish those facts for every invocation before transport erasure. Controls in either nesting direction, other operators/carriers/conversions, PB/AST-value profiles, mutable/deferred/correlated leaves, filters/grouping, mixed lineage and native replay remain outside this cut.

### Independent ownership route, execution and measurement

`LocalProgram` now stores a private closed `ProgramEntry::{Row,ControlLineage,SqlNumericBatch}`, assigned only by exhaustive compiler-mode mapping. Its only local guard compares an expected entry; there is no public getter or conversion. Binding and decoded dispatch keep existing pure schema/selection preflight precedence, then check the requested route **before** reads, host hooks or kernels, even for an empty selection. This prevents internally misrouted numeric leaf/constant programs from silently taking a row shortcut. A test-only/raw-RPN extraction erases the wrapper identity; identical untagged raw leaves cannot be distinguished by the low-level driver, and C does not claim otherwise.

Separately, private `EvalExecution` is selected by fixed driver entry wrappers and checked against an independent accounting policy:

| Execution | Measurement | Program/ordinary admission |
| --- | --- | --- |
| Unannotated | ConservativeInt | No flow tag; legacy nodes and TypedRow/exact PbRow203 ordinary calls |
| SqlControlLineage | ExactRetained | Existing checked flow/node roles and singleton guards |
| SqlNumericBatch | ExactRetained | No flow; one structured signed leaf or batch-only ordinary node; consistent bounded occurrence map |

`StorageMode::ExactLineage` is renamed privately to **ExactRetained**. `EvalBudget::lineaged` remains an alias preserving C3b behavior; the new numeric entry uses the same private exact constructor. The budget mode does not select scheduling, authorize Bytes or imply lineage. Program/entry/precharge/input-normalization guards are domain-aware, not merely a renamed flag. No C3b public API, result-flow role, source table or old caller route was widened.

### Same driver, full operand phases, actual lane attribution

The numeric facade checks the **selected-occurrence count**, refusing1025 before effects, while allowing a larger physical universe. Empty selection performs pure preflight/entry/budget checks and publishes an empty Int vector with zero reads/kernels. Nonempty evaluation invokes the existing frame driver **once** for the whole borrowed selection; it never uses the old full-root `eval_rows` occurrence loop or root tiling.

No runtime frame field was added. `OrdinaryFrame` reuses `next/awaiting/stopped/values`: complete left phase, complete right phase, then a local synchronous kernel-only lane loop. Batch cannot NULL-stop, including width1 and all-NULL left. Constants are completed immutable broadcasts. Both the normal primitive dispatch and standalone root-leaf fast path import a numeric binding over all selected occurrences, preserving duplicates and order exactly once.

The existing prepared kernel receives singleton borrowed views into already materialized selection-order operands. View indices are local lanes; attribution uses `InputRow { occurrence: lane, input_row: selection[lane] }`. Thus[2,0,2] means local lanes0/1/2 and physical rows2/0/2, not vector indexing by physical row. Source identity is the prepared call's original unit/node/all-node ordinal/profile; input failures still identify a binding slot, so a caller needing per-source input joins must bind that distinction itself.

Only shared helpers at the genuine `read_input` Err and genuine ordinary-kernel Err capture a site. There is no failed-vector replay, formatter/parser inference, ancestor overwrite or manufactured row0. Shape/validation/budget/post-success/publication failures remain unsited; warnings and the original owned error are not replaced or rolled back. Kernel singleton invocation is a conservative correctness choice, **not SIMD or throughput evidence**.

### Actual retained owners and old-route compatibility

One invocation budget spans every phase and lane. Numeric imports receive explicit `other_live_bytes`, including suspended frames/left operands, then account minimum/actual destination capacity and the actual returned singleton owner alongside the partial destination before copying or another read. Provider capacity is not normalized away in this new domain. Kernel work retains both complete operand owners and their frame storage while charging the actual N-output plus actual one-lane kernel result. Per-lane values are dropped before the next kernel. Int destination slots/bitmap are preallocated for N; no hidden per-lane reallocation/clone chain or budget reset is used.

Exact frame/stack growth retains the accepted conservative old/new overlap checks. A generated root N-vector moves to final publication without a second collector or source/destination double charge. A borrowed scalar root reserves/measures and broadcasts exactly N Int slots. Final actual output is checked before publication after driver scratch is gone. This is the same retained-owner-before-next-effect/publication guarantee—not allocator peak, an unknown callback/internal-kernel pre-bound, optimal admission or universal OOM prevention.

Old row demand still normalizes its singleton Int replies and stops after left NULL. C3b still moves its actual Int/Bytes input owner through its checked singleton flow path; Host/eager/decoded routes keep their original domain. Existing runtime helper tests only received private accounting terminology/constructor updates, not rewritten expected values. Existing tests elsewhere were not rewritten to make C3c pass.

Source inspection shows no intentional change to `EvalFrame`, `ProgramFrame`, `ControlFrame`, `OrdinaryFrame`, `FrameResult` or `RpnStackNode` fields. Accepted old sizes400/128/400/176/152 remain **a baseline, not a newly measured result**. Parent must rerun the layout/resource gate; the new opaque compiled-entry tag may alter `LocalProgram` inline size and is not claimed free. Compiled-program metadata is distinct from the active retained-buffer ledger.

### Authored tests and read-only review

The C3c tranche authors **32 tests, all initially unrun**:27 `numeric_batch_*` cases in `profile_tests.rs`,3 compile-only cases, and2 private driver cases. The final appended case exercises actual inner raw/reported/decoded row dispatch for constant/input roots at N0/N1, requiring unsited pre-effect refusal and a successful correct numeric route afterward; it does not merely compare the compiled tag. From an otherwise unchanged575-test expression baseline this would imply607; parent joint gates may include other concurrently approved additions and their actual totals are authoritative. Coverage includes immutable strict facts/snapshots and old row refusal; leaf/empty internal route misuses; N0/1/1024 and pre-effect1025 refusal; root literal/input/broadcast; both competing nested-failure schedules; RHS failure before an earlier possible parent overflow; NULL-left demand at width1; duplicate coordinates; warning/error-prefix/no-replay behavior; fresh budgets/reporters/unwind; deep33/64/256; repeated IDs not CSE; provider overcapacity; and explicit suspended-base/actual-reply accounting.

Read-only reviewer0bdc100b checked compiler/API/arity, lane-view/result lifetimes, complete phase order, source/row coordinates, entry/domain guards and old-route isolation; no concrete major blocker was found. Reviewer0106e623 separately checked actual import/kernel/held-operand storage, frame growth overlap, tick scope, C3b routing and scalar/generated publication; no concrete omission or old-route regression was found. Neither audit ran code or wrote a reviewed file. The separate approved native caller/whole-entry comparison remains pending; no **0/245** family-credit change follows from these source checks.

### Coherent frozen source handback and commands

Production was affirmatively frozen for the parent's optional library check while the last test-only dispatch fixture was completed in `profile_tests.rs`; no production changes were queued. All writers and read-only reviewers have now stopped, and the final32-test source fingerprint below replaces the earlier31-test snapshot. The checks below succeeded; no Cargo/build/test/lint/benchmark was run by C. Pinned formatting was applied only to each writer's explicit released files with `/home/agent/tidb/expression-reuse/rustup-home/toolchains/nightly-2026-01-30-x86_64-unknown-linux-gnu/bin/rustfmt --edition 2021 --config skip_children=true <owned files>`. C's final driver command was exactly:

```bash
/home/agent/tidb/expression-reuse/rustup-home/toolchains/nightly-2026-01-30-x86_64-unknown-linux-gnu/bin/rustfmt --edition 2021 --config skip_children=true components/tidb_query_expr/src/types/expr_eval.rs && git diff --check -- components/tidb_query_expr/src/types/expr_eval.rs
```

Final ordered SHA256 manifest (paths relative to TiKV root):

```text
e04de45e68743dda468b455bb592d0a69ba93f7c8ababa42581ab8e5863dc5bd  components/tidb_query_expr/src/local/profile.rs
63f253ccd8fa13937b91402d44ed4ae1df63cfaa2f4eb0884a7352e5b560b8cf  components/tidb_query_expr/src/local/profile_tests.rs
d1d40435d12251800aab1f417c16ae90c4199f855621eeb20b3f215d99ae029d  components/tidb_query_expr/src/local/compile.rs
af67f7ddcffdd19ea0d19d7e8dca3abfad120cb70f388546814a942ead3f4c1a  components/tidb_query_expr/src/local/batch.rs
7791998e0f7612f9b17eced920adacfc75346d2d20f7e35e90bb5123e9683da0  components/tidb_query_expr/src/local/runtime.rs
9d8aac7dbcc94b25bbbbd5036cedb5d485048588c0d7c62fa7e12ad376c8c297  components/tidb_query_expr/src/local/mod.rs
5eca3609ce1af1bb38c0dde0c70bc476a81b4a593b653d43cb5daec5e40583d6  components/tidb_query_expr/src/types/expr_eval.rs
```

The ordered `sha256sum` stream digest is **cd6589e1454209d5285b02843d31d3b2697e4d37fb99a5b024b98341d5b731c0**. A final read-only check also returned the unchanged accepted C3b hashes for unloaned `impl_op.rs` b9a61f…ec66, `local/lineage.rs` bdf57c…c5acb, `local/lineage_tests.rs`42d323…bdbdd, `types/expr.rs`4fcef5…5b1d8 and both datatype helpers0367a8…3443 / b2f091…3d53. The exact aggregate command from the TiKV root was:

```bash
set -o pipefail; files=(components/tidb_query_expr/src/local/profile.rs components/tidb_query_expr/src/local/profile_tests.rs components/tidb_query_expr/src/local/compile.rs components/tidb_query_expr/src/local/batch.rs components/tidb_query_expr/src/local/runtime.rs components/tidb_query_expr/src/local/mod.rs components/tidb_query_expr/src/types/expr_eval.rs); git diff --check -- "${files[@]}" && sha256sum "${files[@]}" && printf '\nC3c ordered manifest digest:\n' && sha256sum "${files[@]}" | sha256sum; printf '\nUnloaned C3b expression and datatype fingerprints:\n'; sha256sum components/tidb_query_expr/src/impl_op.rs components/tidb_query_expr/src/local/lineage.rs components/tidb_query_expr/src/local/lineage_tests.rs components/tidb_query_expr/src/types/expr.rs components/tidb_query_datatype/src/codec/data_type/bit_vec.rs components/tidb_query_datatype/src/codec/data_type/chunked_vec_bytes.rs
```

After the final test-only stop, the exact refreshed seven-file command (exit0) was:

```bash
set -o pipefail; files=(components/tidb_query_expr/src/local/profile.rs components/tidb_query_expr/src/local/profile_tests.rs components/tidb_query_expr/src/local/compile.rs components/tidb_query_expr/src/local/batch.rs components/tidb_query_expr/src/local/runtime.rs components/tidb_query_expr/src/local/mod.rs components/tidb_query_expr/src/types/expr_eval.rs); git diff --check -- "${files[@]}" && sha256sum "${files[@]}" && printf '\nFinal C3c ordered manifest digest:\n' && sha256sum "${files[@]}" | sha256sum
```

Final receipt whitespace/conflict grep `[	 ]+$|^(<<<<<<<|=======|>>>>>>>)` returned no matches. C3c is **SOURCE READY / FULL NATIVE GATE PENDING**. Parent subsequently reported an actual successful library build; C read `logs/tikv-c3c-b22i-library-first.log:339–342`, whose line340 is `Finished dev profile [unoptimized] target(s) in2m08s` (spacing normalized here). That is compilation evidence, not execution of the32 new tests. Parent also identified a stale TEST-profile probe relink that reused byte-identical prepatch rlibs; its result is neither patched-behavior failure nor green evidence. Full expression/aggr tests and a matched TEST-artifact probe refresh remain parent-owned. The parent owns the frozen files and serial native gates from this handback. No additional source edits, C4 entry, native caller activation, performance claim or family completion is authorized by this receipt.

## C3c-source-r1 — two new test-fixture API corrections

Parent reported the first full-RPN test attempt exited101 with **zero tests run**. C read `logs/tikv-c3c-b22i-expr-full-first.log:64–118`: the new compile fixture passed integer701 to `HostCatalog::new`, which requires `Vec<HostSignature>` and returns a Result (two E0308 diagnostics); the new driver fixture relied on `VectorValue::from(Vec<Option<i64>>)` unavailable to this dependent crate's test build (E0277). The previous source audits did not catch these fixture API errors and must not be interpreted as native validation.

The parent granted only the two **new test hunks** for correction. C changed:

- `local/compile.rs`: `HostCatalog::new(Vec::new()).unwrap()` constructs the existing catalog API correctly; the constant/input leaves still test catalog-backed Row versus profiled Row, numeric and lineaged compiled entries.
- `types/expr_eval.rs`: expected[2,0,2] is constructed with public `ChunkedVecSized::<Int>::with_capacity`, `push`, and `VectorValue::Int`; the value/occurrence/resource assertions are unchanged.

No API, production path, frame, old575 expectation, test count or other source file changed. There are still32 authored C3c tests, with no execution result yet from this attempt. Parent separately reports aggr40 passed and refreshed TEST-profile library artifacts; that is attributed parent evidence, not C running a gate or proof that these32 tests passed.

C ran the pinned formatter on **only these two files** and scoped `git diff --check` (exit0):

```bash
/home/agent/tidb/expression-reuse/rustup-home/toolchains/nightly-2026-01-30-x86_64-unknown-linux-gnu/bin/rustfmt --edition 2021 --config skip_children=true components/tidb_query_expr/src/local/compile.rs components/tidb_query_expr/src/types/expr_eval.rs
git diff --check -- components/tidb_query_expr/src/local/compile.rs components/tidb_query_expr/src/types/expr_eval.rs
```

To verify that formatting did not alter any other hunk, a read-only Python check reversed **only** each authorized literal replacement in memory and hashed the reconstructed content. Both digests exactly matched the prior full-file handback (`d1d404…029d` and `5eca36…83d6`). No file rollback/write occurred in that verification. The exact check was:

```bash
python3 - <<'PY'
from pathlib import Path
from hashlib import sha256
checks = [
    ('components/tidb_query_expr/src/local/compile.rs',
     '        let hosts = HostCatalog::new(Vec::new()).unwrap();',
     '        let hosts = HostCatalog::new(701);',
     'd1d40435d12251800aab1f417c16ae90c4199f855621eeb20b3f215d99ae029d'),
    ('components/tidb_query_expr/src/types/expr_eval.rs',
     '        let mut expected = ChunkedVecSized::<Int>::with_capacity(3);\n        for item in [2i64, 0, 2] {\n            expected.push(Some(item));\n        }\n        assert_eq!(value, VectorValue::Int(expected));',
     '        assert_eq!(\n            value,\n            VectorValue::from(vec![Some(2i64), Some(0i64), Some(2i64)])\n        );',
     '5eca3609ce1af1bb38c0dde0c70bc476a81b4a593b653d43cb5daec5e40583d6'),
]
for path, new, old, expected in checks:
    text = Path(path).read_text()
    assert text.count(new) == 1, path
    restored = sha256(text.replace(new, old).encode()).hexdigest()
    assert restored == expected, (path, restored, expected)
    print('Exact new-test-only replacement verified:', path)
PY
```

### Current corrected frozen manifest — supersedes C3c-source-r0

```text
e04de45e68743dda468b455bb592d0a69ba93f7c8ababa42581ab8e5863dc5bd  components/tidb_query_expr/src/local/profile.rs
63f253ccd8fa13937b91402d44ed4ae1df63cfaa2f4eb0884a7352e5b560b8cf  components/tidb_query_expr/src/local/profile_tests.rs
bbba352b74ebe1849fcc1a9ab434ccecd6f06950abcc0dbbeb3a4229c49289be  components/tidb_query_expr/src/local/compile.rs
af67f7ddcffdd19ea0d19d7e8dca3abfad120cb70f388546814a942ead3f4c1a  components/tidb_query_expr/src/local/batch.rs
7791998e0f7612f9b17eced920adacfc75346d2d20f7e35e90bb5123e9683da0  components/tidb_query_expr/src/local/runtime.rs
9d8aac7dbcc94b25bbbbd5036cedb5d485048588c0d7c62fa7e12ad376c8c297  components/tidb_query_expr/src/local/mod.rs
003562f392e366a8e1e4d081f34d87bea35d61e1f7aac561419c8bc3c089a202  components/tidb_query_expr/src/types/expr_eval.rs
```

The ordered seven-file SHA256 stream digest is **413f4f5ccc5154888aee9f3e02ba3c166420de268f7306d40b01439c39342894**. It was regenerated with the same ordered seven-file array as r0 and `sha256sum "${files[@]}" | sha256sum`, after the formatter/diff/in-memory checks under `set -euo pipefail`; the command exited0. All source writes are stopped again. C ran no Cargo/build/test/lint; the parent owns the retry and native acceptance. C4 and family-credit remain unchanged and separate.

## C3c-native-r0 — parent-accepted joint native checkpoint

After the two authorized new-fixture corrections, the parent ran and accepted the coherent native gates. C read these actual result anchors:

| Gate | Log/result |
| --- | --- |
| Full RPN expression | `logs/tikv-c3c-b22i-expr-full-corrected.log:679`:608 passed,0 failed,0 ignored,0 filtered; parent accounts575 previous+32 C3c+1 concurrent B test |
| Full aggr | `logs/tikv-c3c-b22i-aggr-full.log:134`:40 passed,0 failed,0 ignored,0 filtered |
| Isolated frame layout | `logs/tikv-c3c-frame-layout.log:69–72`:EvalFrame400/ProgramFrame128/ControlFrame400/FrameResult176/RpnStackNode152;1 passed,607 filtered (one-test layout evidence, not a substitute full suite) |
| Full TiDB caller | `logs/tidb-c3c-b22i-expr-full-comparison.log:3469–3475`:1292 passed,4 failed,93 ignored; same four named baseline failures |

The parent separately compared all four complete caller failure blocks against D5 after thread-ID normalization and found them identical; that exact block comparison is parent-reported, not an independent C comparison. The parent also reports the same-source matched TEST-profile allocation probe GREEN at1-vs-1 MOD/DIV/DAY and positive runtime-Send / Mutex-owner-SendSync compile assertions. Those are the relevant joint B/I probes, not C3c performance or C4 API evidence. The earlier stale TEST-artifact relink remains excluded.

The corrected source manifest **413f4f5ccc5154888aee9f3e02ba3c166420de268f7306d40b01439c39342894** remains the C3c handback. All seven expression files remain **FROZEN**. D6's newly released four caller files belong to D, not C. The new C4 request is **separate exact design/document work only**: no ASCII/evaluated-value implementation, arbitrary signature admission, native caller activation or change to the0/245 family-completion count follows from this checkpoint.

## C4-proposal-r0 — closed ready-Bytes ASCII worker, design only

### Boundary and exact small API

C read E's `evaluated-value-contract.md` EV-r1, A's `evaluated-value-review.md` EV-review-r1, the canonical selector/preparation/ASCII/driver seams and maintenance context. This is a **separate proposal**, not implementation permission, another ExecPlan or a reopened C3c loan. Native caller policy, scope/epoch/pool ownership, context forwarding, deletion and performance remain the caller owner's work. ASCII family credit stays0 until the full EV matrix is accepted.

Recommend one **named ASCII-only factory**, not an enum or integer allowing arbitrary operations. Proposed signatures/types (not existing APIs):

```rust
pub fn prepare_evaluated_ascii(
    cx: LocalCompileContext,
    execution: ExecutionLimits,
    max_worker_retained_bytes: usize,
) -> LocalResult<EvaluatedAsciiWorker>;

impl EvaluatedAsciiWorker {
    pub fn eval_one(&mut self, ready: Option<Vec<u8>>) -> LocalResult<ComputedInt>;
    pub fn kernel_invocations(&self) -> u64;
    pub fn is_healthy(&self) -> bool;
    pub fn retained_storage(&self) -> LocalResult<WorkerStorage>;
}

// Fields and constructors remain private. Read-only value/metadata views only.
impl ComputedInt {
    pub fn value(&self) -> Option<Int>;
    pub fn into_option(self) -> Option<Int>;
    pub fn metadata(&self) -> ComputedIntMetadata;
}
pub enum ComputedIntMetadata { OwnSignedInt }

impl WorkerStorage {
    pub fn inline_bytes(&self) -> usize;
    pub fn owned_heap_bytes(&self) -> usize;
    pub fn total_bytes(&self) -> usize; // checked before this observation exists
}
```

`max_worker_retained_bytes` is a separate immutable worker-owner allowance, **not** a reinterpretation of per-call `ExecutionLimits`. No new numeric default is proposed. Public preparation accepts no SQL context, operation/signature, graph, schema, arbitrary metadata, services, host or native callback. The worker exposes no program/RPN/context/state reference, conversion, Deref or Clone. Its intended trait is Send, not Sync; no unsafe trait assertion is allowed. Keep the existing Any+Send executable contract.

The worker privately owns one `LocalProgram`, one copied `ExecutionLimits` policy, a sealed private `EvalContext`, health, and a checked invocation counter. Every call creates fresh budget and row scratch. `Option<Vec<u8>>` means an **already normalized nullable Bytes value** whose native coercion has finished; it is not a raw SQL Datum and proves no original SQL/PB provenance. The bytes move into an invocation-local `ScalarValue::Bytes` and are only borrowed during execution. No argument/result/native descriptor, row pointer or caller context enters idle state. NULL is one ready occurrence, not zero rows.

`ComputedInt` can be constructed only after validating the actual generated one-element Int output. `OwnSignedInt` is an explicit computed-result identity, including NULL—not an input identity, C3b selected ID or native return declaration. The TiDB adapter must create `ValueMetadata { Int, None, None }` and then perform the original native return coercion once, outside the runtime borrow. C neither stores nor reconstructs the original complete native return FieldType.

### Exact construction and independent execution domain

The private compiler factory creates exactly:

```text
Call(TiPb(Ascii7003), [InputSlot0(canonical Bytes)], canonical signed Int, None)
schema = [canonical Bytes]
source preorder = root0 / input1, local VALUE-call identity only
final RPN = [ColumnRef0, FnCall(args_len1)]
```

Canonical Bytes/Int FieldTypes are freshly built scalar-only protobuf ABI records (Blob/LongLong), never cloned native descriptors; no charset/element/unknown-field containers are populated. Their complete records are fixed and checked, not merely their EvalType. No original wire/source identity is inferred from the numeric selector7003.

Add private `CompileMode::EvaluatedAscii` and `ProgramEntry::EvaluatedAscii`. Validate the exact two-node/depth2 source, full schema/roles, metadata-none and absence of any other leaves/calls before invoking the existing shared compiler. Reuse its `prepare_call` and Emit seam: retained arguments must be exactly[0], control absent, and `into_node()` must yield a FnCall with arity1, canonical Int return type and unit metadata. `function.rs:424–433` already exposes the necessary crate-level inspection; no new PreparedCall/RpnFnMeta ABI is needed. The existing selector `lib.rs:834` resolves to the only authoritative ASCII body at `impl_string.rs:211–219`.

Add private `EvalExecution::EvaluatedAscii` independently of `StorageMode::ExactRetained`. The fixed C4 driver entry checks the compiled domain, exact flat shape, canonical full types, no flow/catalog, and width1. Require **ReadyBytes input iff EvaluatedAscii execution at entry**, not just at the leaf: otherwise an internally altered no-read program could avoid the input guard. Add a private `EvalInput::ReadyBytes` carrying a borrowed ScalarValue and a mutable plain wrapper witness. It is closed storage, not a LocalRuntimeServices/native/Host callback. ColumnRef is permitted only at slot0 and returns a borrowed Bytes scalar; all other C4 node/domain/input pairings refuse.

Use the same ProgramFrame loop and `eval_prepared_kernel`, not an Ordinary203 frame, general value interpreter, direct ASCII call, root replay or batch route. No runtime frame field is required. Old local row/profile/control/numeric admissions remain unchanged. In particular **do not prohibit existing raw wire RPN ASCII**: that older official route is already legitimate; the new compiled tag and opaque facade close C4 escapes without removing it.

The nullable generated RPN wrapper must execute for every admitted `None`. There is no frontend/native/C4 NULL fast return. Post-execution require an owned Generated `VectorValue::Int` with one slot and the canonical Int type; scalar/ref/wrong-width/wrong-carrier output is a contract error. Copy its Option<Int> into ComputedInt only after retained-byte and context checks; drop temporary output and ready input before making the worker idle again. Do not pretend that local singleton coordinate0 names a native SQL row or original PB node.

### State, pure context and failure lifecycle

Construct the private context once, at the caller's **first ready invocation** when an owned runtime is actually prepared. Its capability is justified positively by this exact ASCII body having no SQL-context parameter and no warning behavior. Proposed sealed configuration is the fixed UTC/default pure configuration with warning-detail limit0; this is not a surrogate statement context. Do not reset/drain/recreate it per call.

Health requires an idle/nonpoisoned worker, `warning_cnt == 0` **and** an empty warning-detail vector before and after execution. A zero detail limit makes the count check particularly important. Also check the owner storage allowance before publication/return to a pool. Never silently clear/merge private warnings into native warnings or retain an unexpected diagnostic as healthy.

Mark the worker in-flight/unhealthy **before** invoking the driver and restore healthy only after normal cleanup, result checks, context health and owner observation. Thus a panic caught outside a surviving scope leaves it quarantined even if its eventual Drop occurs when `thread::panicking()` is false. No panic-to-native replay is added. On an actual kernel error, move the original LocalError back; a simultaneous dirty-context condition poisons reuse rather than replacing the original error. An otherwise successful call with an invalid sealed context returns a structured domain/contract error and publishes no ComputedInt. Existing LocalError variants suffice; no error-text parsing, fabricated SQL site, new warning policy or new native formatter is proposed.

A dirty/poisoned worker must be retired and dropped by the lease owner before continuing native work or admitting any return to the pool—not left parked as an unmeasurable warning-payload owner behind a flag. The C API refuses further evaluation/reuse; the caller must keep its slot/byte debt until actual disposal. A normal clean budget rejection may restore reusable state only after invocation-owned data and borrows are gone. Each call has a fresh budget; program, state, private context and witness persist. No mutable runtime borrow exists while native children/coercion/return-coercion run. The caller must use checked short borrows, propagate its active scope for nesting, and not hold a pool/bookkeeping lock during native work.

### Two distinct retained-storage ledgers

**Invocation ownership.** Existing scalar nodes charge zero because they borrow. The moved ready Vec still has an owner: add a private ready-owner observation to EvalInput and include `Vec<u8>::capacity()` exactly once in `TaskGuard::storage`, through a shared reborrow of the input. NULL contributes0. That standing charge survives the function call, actual one-element Int output and publication/extraction checks until the ready value really drops; never double-charge it in scalar-node storage. The read-only shape is finite, so no Bytes vector, offsets/bitmap or transport copy is necessary in this C4 design. Existing exact frame/stack/Int-owner checks remain in use. A pre-dispatch resource error leaves the witness unchanged. Three explicit guard details matter: the exact two-node shape must not enter the existing singleton-leaf fast path that precedes TaskGuard creation; ReadyBytes has no Host provider in both normal and Drop matches, and must never reach the Host reservation delta arithmetic; after the driver returns and TaskGuard is gone, the boundary must still check ready.capacity()+actual Int output together (or truly drop ready before a result-only check), not release a live input charge merely because the driver ended.

**Reusable owner storage.** `retained_storage()` is a checked, nonmutating observation; it does not reset warnings or populate a lazy cache. It reports inline and owned-heap parts separately so a caller can avoid counting runtime headers twice inside an idle Vec/Box container. Count:

- Worker/program/state/context inline fields once in `size_of::<EvaluatedAsciiWorker>()`.
- Actual RPN node Vec capacity and schema Vec capacity, with checked arithmetic.
- Initialized `OnceLock<Box<RpnExpressionMetadata>>` allocation **and** its referenced-column Vec capacity, not the public slice length. This requires a narrow crate-private helper in `types/expr.rs` using `metadata.get()`; inspection itself must not initialize it. Prewarming the fixed program under a creating reservation before publication is recommended, but does not remove the observer.
- Canonical FieldType/container heap zero **by fresh scalar-only construction**, not from protobuf equality, serialization size or empty length. Closed FnCall unit metadata is a ZST owner; no constant/child programs/opaque metadata payloads are admitted. The observation is not a generic LocalProgram heap estimator.
- The private configuration allocation and warning Vec's actual capacity even when its length is0. The healthy-empty invariant means no live warning-message/unknown-field subowners. Current `ExecutionLimits` is four scalar limits and has no retained heap or row scratch; do not silently retain an arena/transport buffer later without extending this observation.

All adds/multiplies and total/allowance comparisons are checked. Unhealthy/overflowing observations are not acceptable idle-pool admissions. Never cache a cold footprint and assume a warmed worker cannot grow.

**Explicit Arc accounting choice for parent approval:** public Arc APIs do not expose the complete allocation layout. `size_of::<Arc<EvalConfig>>()` is only the inline handle and is NOT the configuration allocation. The recommended closed implementation charges a private pinned-toolchain layout proxy matching `ArcInner`'s strong/weak atomics plus EvalConfig and its alignment/padding, justified by the pinned Rust `alloc/src/sync.rs` definition and gated by an isolated parent allocator/layout probe. This is an accounting-only dependency: no pointer reinterpretation or unsafe behavior/trait ABI is proposed. Fixed UTC configuration has no nested owned heap. The probe must observe a real allocator request, not compare the proxy's size with itself: prefer a parent-owned isolated probe executable with its own explicit allocator instrumentation and matched dependency artifacts. Alternatively request a **separate test-only** integration-test file such as `components/tidb_query_expr/tests/evaluated_value_arc_layout.rs`. Neither that file nor new allocator_api features nor replacing the ordinary unit-test binary's global allocator is included in the six-file proposal. If the parent declines that pinned-layout contract, the design must explicitly revise its control-bookkeeping scope or obtain a different observer; it cannot claim full config-owner bytes while silently omitting them.

Allocator bookkeeping/usable-size slack and a universal allocation peak/OOM guarantee remain excluded. A worker-owner cap is not a bound on shared-compiler construction temporaries, native coercion, active call scratch, caller containers or simultaneous replacements. The caller must reserve live+creating+idle slots and an approved **creation allocation allowance before** building outside its short lock, retain old-epoch debt until disposal, and separately charge actual container capacity/overlap and call/native storage. Node/depth limits and a final owner observation do not prove a hard factory-allocation peak. The parent's pool policy/creation reservation proof is a separate EV gate, not an implicit promise of this factory.

### Wrapper evidence versus actual ASCII-body evidence

The always-available worker counter advances with checked overflow **immediately at the existing `func_meta.fn_ptr` invocation**, after shape/resource preflight. It is plain private state, not a callback, TLS/global runtime or RpnFnMeta field. Its public read-only value works in a normal TiDB dependency build. This proves actual generated-wrapper dispatch, including NULL and actual Err; it does not authenticate original SQL/PB provenance or prove non-null body entry by itself. A Some-input wrapper count must never be relabeled an observed body count.

EV-r1:357 additionally requires distinct non-null body evidence. Request a **separate strict cfg(test)-only instrumentation hunk** in `impl_string.rs`: increment/read an atomic at entry to the existing ASCII body, without changing its algorithm/signature or production code. Add an ignored exact C4 origin test that the parent runs alone; it may spawn/join its own parallel workers and compare aggregate actual BODY deltas with summed per-worker WRAPPER deltas. No unrelated ASCII tests run concurrently in that process. Ordinary full tests must not assert global deltas. Required distinctions are ready NULL→wrapper1/body0, Some(empty)→1/1, Some(raw bytes)→1/1, and pre-dispatch refusal→0/0.

That body hook is deliberately unavailable in TiDB's dependency build; D's integration witness proves wrapper origin, while the isolated TiKV receipt proves the wrapper/body distinction. This avoids repeating the dependency-cfg(test) fixture mistake. Native frontend rejection, preparation/context creation, runtime call entry and return-coercion counters remain separately named caller evidence. No counter is inferred from a facade entry or byte value.

### Exact requested file loan and pending gates

For the proposed worker **copying the existing `ExecutionLimits` policy** plus complete ownership observation and the separate body proof, request these **six existing product files only**, with inline new tests and no new module:

| File under `components/tidb_query_expr/src/` | Exact proposed change |
| --- | --- |
| `local/compile.rs` | Private fixed factory, closed compiler/entry arm, exact canonical shape checks through existing preparation |
| `local/batch.rs` | Opaque worker, sealed context, state lifecycle, ComputedInt/owner observation, inline new tests and isolated origin test |
| `local/mod.rs` | Additive small public exports/documentation only |
| `types/expr_eval.rs` | ReadyBytes/domain guards, standing input capacity, shared-driver entry and actual wrapper witness; no frame fields |
| `types/expr.rs` | Narrow read-only node/cache allocation-capacity helper; no fields/ABI/structural mutation |
| `impl_string.rs` | **Only cfg(test) ASCII-body counter/readback**; no algorithm/signature/production change |

`local/runtime.rs` already has ExactRetained/checked helpers and needs no edit. No profile/registry/function/codegen/spec/lineage/datatype/Cargo/lock/native caller change is required by this design. The worker stores the copied limits directly; no separate evaluation-state wrapper or arena is part of the ownership contract. A new evaluated-value module is optional organization, not a forced seventh loan. Parent owns corresponding guide/main-plan updates and any later caller loans.

Read-only reviewers0bdc100b (canonical/API/entry/witness/lifetimes) and0106e623 (retained owners/context/pool/capacity) independently found this six-file cut sufficient, with the explicit Arc/probe and caller-disposal conditions above. Neither reviewer wrote a file or ran validation. Current source anchors include `local/batch.rs:32–47` for index-only state, `types/expr.rs:119–141,225–267` for boxed lazy metadata, `types/expr_eval.rs:1329–1343` for the actual function-pointer invocation and `Q/expr/ctx.rs:184–241` for warning/config owners (`Q=components/tidb_query_datatype/src`). No EvalFrame/RpnFnMeta field addition is proposed; nevertheless the existing400/128/400/176/152 frame baseline and any compiled/input/worker sizes must be measured again by the parent, not declared unchanged from this design. New worker/context/program ownership is not claimed free, and warmed driver allocation throughput remains unmeasured.

Before native acceptance require closed old-route/empty misuse tests; exact factory/output/NULL checks; sparse ready-value sequences and no stale operands; retained input spare capacity and input/output overlap; cold/warm program cache/owner observations; checked arithmetic and both budget layers; zero-count/detail health counterexamples; caught-panic quarantine; repeated calls with one compilation/context; actual wrapper/body origin separation; Send and intended non-Sync assertions; unchanged old suites/layout; and the independent caller pool/forwarding/metadata/deletion/performance matrix. No native result is claimed for any C4 item. **Await the parent's precise loan and Arc accounting decision before any implementation.**

## C4-source-r0 — approved closed evaluated-ASCII worker, source-only freeze

This section supersedes the proposal's pending-loan status, not its boundary exclusions. Parent subsequently approved exactly the six existing files below, the accounting-only pinned Arc proxy conditional on actual both-pin allocation-request evidence, and **required** full program metadata prewarm before worker publication under the caller's creating reservation. C implemented that cut and handed all six files back together. No C4 build, test, benchmark or native caller result is claimed by C.

### Frozen sources and exclusive writers

All paths in this table are under `K/components/tidb_query_expr/src/`. The ordered `sha256sum` stream, using that relative prefix and the table order, hashes to **5868a59c478846cb6ef4a200e8f50f14777adcc4f844050e694e73537bbd0fa3**.

| File | Exclusive writer | Final SHA-256 |
| --- | --- | --- |
| `local/compile.rs` | 0bdc100b | `f87c907992ac80b89895c2a83013f1b416595a0fb6e6629aa6affc3024d34a05` |
| `local/batch.rs` | 0106e623 | `bdf773f1ecfa7f2b592b37511ff2941aa0ace83b3607ed83b72ba898611ca48d` |
| `local/mod.rs` | 0bdc100b | `fbf9c78cf8c1cbf54882b1dc4fc69a4aa76b5b5441c230955da8dad1a386a4b0` |
| `types/expr.rs` | 93a02a6a | `c462847ae15b26a7671510dcd9f612adeafa2d19ec7e6e2f89fbcc812f6f5906` |
| `types/expr_eval.rs` | C | `e002330050d81909ed3cb37291834d29412ed4376bd816282c7bae005af4fbd7` |
| `impl_string.rs` | 93a02a6a, cfg(test) hook only | `2fa890afc3c5362a562354d2c1bb0e866e03414b529ad9789f97767dc41e24c3` |

Every writer stopped before handback. The one subsequent `batch.rs` reloan changed only two NEW test bodies and stopped again; its older f72a949a… hash is superseded. Each writer used the pinned January rustfmt on only owned files. C's final explicit-six-file `rustfmt --edition 2021 --config skip_children=true --check` and scoped `git diff --check` both exited0. These are formatting/source checks, **not compilation or tests**. C rehashed `local/profile.rs`, `local/profile_tests.rs` and `local/runtime.rs`: all three still match the accepted C3c manifest. No frame fields, public RpnFnMeta fields, datatype/codegen/kernel algorithms, other product files, Cargo/locks, native callers or guides were changed by this C4 cut. C alone updated this receipt; parent retains the main plan, guides and all native gates.

### Implemented API and fixed entry

- `prepare_evaluated_ascii(LocalCompileContext, ExecutionLimits, max_worker_retained_bytes) -> LocalResult<EvaluatedAsciiWorker>` is the only public factory. It accepts no signature, graph, schema, descriptor, context, callback, native expression or service provider. The worker exposes `eval_one(Option<Vec<u8>>) -> LocalResult<ComputedInt>`, `kernel_invocations() -> u64`, `is_healthy() -> bool`, and `retained_storage() -> LocalResult<WorkerStorage>`; there is no Clone/Deref/program/context escape.
- `ComputedInt::{value,into_option}` return nullable signed Int. `metadata()` returns `ComputedIntMetadata::OwnSignedInt`, including for NULL; this is not forwarded operand metadata, a SQL descriptor or a control-lineage ID. `WorkerStorage` has private checked fields and separate `inline_bytes()`, `owned_heap_bytes()` and `total_bytes()` accessors.
- Private `CompileMode::EvaluatedAscii` / `ProgramEntry::EvaluatedAscii` build exactly `Ascii7003(InputSlot0(Bytes)) -> Int` through the existing compiler/prepare/Emit path, with canonical fresh scalar-only full FieldTypes and no source metadata. Whole-source checks precede preparation; retained arguments must be `[0]`, control must be absent and prepared metadata unit. The final program is exactly `[ColumnRef0, FnCall(args_len1)]`, with two source nodes, no host catalog or result-flow annotation. This is a value-boundary ABI, not a claim of original SQL/PB origin.
- The new execution domain is separate from `ExactRetained`. `ReadyBytes` is legal **iff** `EvaluatedAscii` at the shared driver's entry, before the old leaf fast path; the value must actually be Bytes. C4 checks the exact two-node shape, canonical schema/result, logical singleton row0, unit metadata and same-artifact official ASCII function pointer. It has no child/host/ordinary/control or arbitrary RPN path. Old Decoded/Bindings routes and raw wire/decoded ASCII remain legal in their existing domains.

### Ownership, prewarm and health

The factory fully initializes the fixed program's metadata via its structural node/work/column/reference getters, checks `2/2/1/[0]`, and only then publishes the worker. It runs no kernel and no fake NULL. `RpnExpression::retained_metadata_heap_bytes()` remains noninitializing: cold returns0; warm charges the metadata Box and actual referenced-offset Vec capacity with checked arithmetic. The worker refuses an unexpectedly cold cache rather than warming it during observation or reuse.

The persistent owner observation recomputes actual root node/schema capacities, warmed cache heap, the unique fixed-config Arc allocation accounting extent, and warning Vec capacity even when empty; `size_of::<EvaluatedAsciiWorker>()` covers inline program/state/config handles and counters. Fresh canonical FieldTypes and unit Any have no owned heap **by construction**, not by equality/serialized size/empty getters. This is deliberately not a general LocalProgram/protobuf/Any heap estimator. The owner maximum is checked against `total_bytes`, separately from execution limits. A fully warmed accepted footprint is retained only for comparison; observations are never answered from that cache. Any later change in measured ownership is refused before reuse/publication even below the configured maximum.

The private one-time context is named UTC, empty flags/SQL mode, max_warning_cnt0, default scalar configuration and a unique Arc (strong1/weak0). Health requires BOTH warning count0 and an empty detail Vec, not either alone. Poison is armed before mutable admission/execution and survives a caught panic; no warning drain/reset, context reconstruction, replay or native fallback occurs. Dirty/poisoned/unmeasurable workers are not idle admissions and must be dropped whole by the caller before credit/reuse. An existing owned primary Err is returned unchanged even when postflight detects a dirty context or changed footprint; that failure only prevents recycling. A successful result is refused if postflight fails.

Each invocation moves its nullable ready Vec into a call-local ScalarValue. A scalar stack node borrows that owner; it never borrows the witness and does not copy or pack Bytes. TaskGuard carries the ready Vec's **capacity** as a standing charge, once, through frame/stack/precharge/result checks; the scalar's node storage stays0. Ready input has no host provider, and host-start reservation refuses it before host-only delta arithmetic. After the driver guard is gone, publication checks still-live input capacity together with actual owned singleton Int output. The nullable Int is copied, both buffers are actually dropped, then health/owner postflight completes. No operand/result borrow or buffer remains in the worker.

A caller pool must separately charge retained idle Vec slot capacity and each idle worker's heap. A popped active worker outside that still-allocated Vec needs its own **total** charge, including its additional inline owner; after moving it back, only its heap is added to the already-counted slot allocation. The corrected NEW fixture demonstrates that accounting distinction, not an implemented pool. Creation reservation/allowance, live+creating+idle limits, execution debt, container growth/overlap and disposal remain E/parent work. No hard factory peak, allocator usable-size bound, or arbitrary native-coercion allocation bound is claimed.

### Dispatch and body evidence seams

`ReadyValueDispatchWitness` is private per-worker checked u64 state. It advances immediately beside the real shared helper's `func_meta.fn_ptr` call, with no intervening fallible operation; counter overflow refuses before dispatch without changing the counter. NULL also enters that generated nullable wrapper. Ordinary and other old eager routes pass no witness. A Some-input wrapper count is still **not** an observed body count.

The only `impl_string.rs` change is a strict cfg(test) AtomicU64 increment at the existing ASCII BODY entry and readback. Its signature, algorithm, generated-wrapper policy and production behavior are unchanged. The isolated ignored test below observes actual pre-dispatch refusal0/0, NULL1/0, and non-null1/1 wrapper/body distinctions, plus its own joined independent workers. Ordinary full tests contain no global body-delta assertions. The hook is unavailable in TiDB's dependency build; that build can use the public wrapper witness, not pretend it observed the body.

### New source fixtures and audit corrections — all unrun by C

There are **29 new test functions:28 regular and1 ignored**. No old test expectation was rewritten.

| Filter/group | Count | Primary scope |
| --- | --- | --- |
| `local::compile::evaluated_ascii_compile_tests::` | 4 | Canonical two-node construction; other source/type/metadata rejection; depth/node limits; old-route/tag refusal |
| `local::batch::evaluated_ascii_tests::` | 16 regular +1 ignored | Worker API/prewarm/capacities/two budgets/reuse/own metadata/config/warnings/panic/primary Err/ownership/origin |
| `types::expr_eval::tests::test_evaluated_ascii_` | 5 | Ready-domain iff/leaf guard, standing input/host refusal, nullable wrapper values, old raw ASCII and forged-pointer rejection, witness overflow/owned error |
| `types::expr::tests::test_retained_metadata_observer_` | 3 | Cold noninitialization, warm empty Box, actual offset capacity and repeat stability |

The exact isolated filter is:

```text
local::batch::evaluated_ascii_tests::test_evaluated_ascii_wrapper_body_origin_isolated
```

Parent must run it **alone**, ignored and exact (`--ignored --exact --nocapture`), not beside another ASCII test in the same process. Its own two spawned workers are joined; final body delta is5. Before those calls it invokes a real over-capacity ready buffer against a low execution budget and requires ResourceLimit, wrapper0/body0 and healthy cleanup. The static trait assertions are in `local::batch::evaluated_ascii_tests::test_evaluated_ascii_worker_ownership_traits`: Worker Send / not Sync / not Clone, and `Arc<Mutex<Vec<Worker>>>` Send+Sync. They remain uncompiled source assertions until the parent's native build.

Readonly audits closed these concrete NEW-code/fixture findings before handback: obtain capacity from Vec, not ScalarValue's borrowed BytesRef accessor; construct the structured sentinel directly rather than assuming `other_err!` lacks file/line text; copy `Option<&i64>` with `.copied()`; preserve the primary owned Error over dirty postflight; count the separate popped active inline owner while idle slots remain allocated; and include actual pre-dispatch body0 evidence in the isolated test. The primary-error regression uses private `finish_invocation` with a synthetic owned Evaluation box and proves exact box identity under count dirtiness and spare-capacity growth, plus refusal of a synthetic success. The shared-helper error test is likewise **not an admitted C4 recipe**. Neither is mislabeled an intrinsic ASCII error/body invocation. The caught-panic test uses the actual begin-invocation poison boundary, not an arbitrary public callback or substituted kernel. Readonly reviewers reported no remaining concrete blocker, but source review is not native validation.

### Parent's isolated Arc evidence and remaining gates

Parent reported actual isolated probe compile/run EXIT0 on both pins and accepted an **88-byte allocation-request accounting extent** for this fixed configuration. C read `logs/arc-eval-config-jan-c3c-i-test-run.log` and `logs/arc-eval-config-aug-ascii-dev-run.log`: each reports EvalConfig72/align8, proxy88/align8, handle8 (not allocation size), eight real Arc-new samples with one matching88-byte request, no requests during config construction/64 clone-drop/64 zero-detail context wraps/move, and one matching final free; before/after allocation-route/free controls pass. These are the linked January C3c/I TEST and August ASCII DEV datatype cohorts, **not future C4 worker artifacts**; parent owns their command/cohort/link/source/binary/dependency receipts. January's private repr(C,align(2)) source was inspected; August private offsets remain unknown and are not inferred from equal request sizes. C's proxy uses only sizeof of atomics plus EvalConfig with the pinned representation, no unsafe Arc access or offset assertion.

That evidence satisfies the narrow Arc-request prerequisite, not worker health, complete owner accounting, execution peak, caller-pool correctness or performance. The parent still owns actual C4 compilation/API/trait/28 regular +1 isolated test gates, old full RPN/aggr/TiDB regression comparison, actual frame/input/program/worker layout observation, and all later native caller/pool/epoch/forwarding/coercion/deletion/performance checks. Frame fields were not added, but no unchanged numeric layout is promised without remeasurement. The maintenance guide needs the separate value-worker/context/entry/accounting contract in the parent's patch. Family coverage remains **0/245**; this closed helper and its source fixtures earn no family credit.

## C4-source-r1 — first native test-compile failure and exact fixture repair

Parent subsequently reported the actual DEV library build EXIT0 and post-J/C4 aggregation40 passing. The first full RPN test command exited101 with **ZERO tests executed**. C read `logs/tikv-c4-b22j-expr-full-first.log:64–76`: E0624 at `types/expr_eval.rs:2661` because the NEW nullable-wrapper fixture called `ChunkedVecSized::get`, which is private in the datatype dependency. The isolated-body and layout commands were not reached. The library/aggr results are not substituted for the missing worker/body/layout gates, and the earlier readonly audits did not establish compilation safety.

Under the parent's exact one-new-test-hunk reloan, C read `Q/codec/data_type/chunked_vec_sized.rs:25–35,111–115` and `Q/codec/data_type/mod.rs:185–190`. The public `ChunkRef::get_option_ref` trait implementation provides the required nullable borrowed Int. C replaced only:

```rust
assert_eq!(values.get(0).copied(), expected);
```

with:

```rust
assert_eq!(ChunkRef::get_option_ref(&values, 0).copied(), expected);
```

The existing public `data_type::*` import supplies the trait. All expected values, NULL/empty/raw-byte cases, result shape and surrounding assertions are unchanged. No production/API/kernel body, datatype file, other test or import changed. Pinned rustfmt on this one file and scoped diff whitespace checking exited0. An in-memory inverse of that exact one-line replacement reproduced the entire previous `expr_eval.rs` SHA-256 `e002330050d81909ed3cb37291834d29412ed4376bd816282c7bae005af4fbd7`; this proves no unrelated formatter hunk occurred. No rollback or inverse write was performed.

The corrected `types/expr_eval.rs` SHA-256 is **c572b141191df62ded911db38f10e7b3b36ee4255a1d4b35eb1ec7720be5cf4e**. All other five C4-source-r0 hashes were rechecked unchanged. In the same six-file order and relative-path prefix, the corrected manifest digest is **3697a2482392124d830d3c876616707bba3da46d05438c002f6a00516090bd41**, superseding5868a59c…0fa3. C immediately stopped writes and handed this source set back for the parent's serialized rerun. C ran no build/test; no corrected test, isolated-body or layout result is claimed at this checkpoint. The source-level fixture count remains29 (28 regular +1 ignored), not29 executed tests.

## C4-native-r0 — parent accepted corrected helper checkpoint

The parent alone ran the actual commands, independently matched the corrected six-file manifest **3697a2482392124d830d3c876616707bba3da46d05438c002f6a00516090bd41**, and accepted this narrowly scoped native checkpoint. C read the logs below and changed only this receipt. All six product files remain frozen; C has no continuing product write loan and ran no native command.

| Actual parent gate | Observed result and log |
| --- | --- |
| DEV library build | EXIT0 reported by parent; `logs/tikv-c4-b22j-library-first.log:85` finishes the actual dev profile |
| Full corrected RPN suite | **636 passed,0 failed,1 ignored,0 filtered**, `logs/tikv-c4-b22j-expr-full-corrected.log:708` |
| Isolated official ASCII origin test | **1 passed,0 failed,0 ignored,636 filtered**, `logs/tikv-c4-ascii-origin-isolated.log:68–71`; this is the exact ignored test run alone, not a zero-test match |
| Actual frame layout | **EvalFrame400, ProgramFrame128, ControlFrame400, FrameResult176, RpnStackNode152 bytes**, `logs/tikv-c4-frame-layout.log:69`; 1 passed/636 filtered at72 |
| Post-J/C4 aggregation suite | **40 passed,0 failed**, `logs/tikv-c4-b22j-aggr-full.log:134` |
| Full post-C4/J TiDB comparison | Actual EXIT101 with **1310 passed,4 failed,93 ignored,0 filtered**, `logs/tidb-c4-b22j-expr-full-comparison.log:3506–3540`; parent compared complete four failure blocks against D6, normalizing thread IDs ONLY, and reported them identical |

The full corrected RPN log explicitly lists all C4 regular groups passing: worker16 at453–469 (the origin entry at468 is ignored there), compiler4 at470–473, metadata3 at642–644, and driver5 at685–689. The worker ownership assertion test passed at467, so the Send / not Sync / not Clone and synchronized-owner trait assertions were actually compiled. The later isolated command covers the29th C fixture. This accounts for **28 new regular +1 separately isolated** C4 tests, not29 regular passes or a full-suite body-delta observation.

The isolated passing test's unchanged source asserts actual resource-refusal wrapper0/body0, factory0, NULL wrapper1/body0, three non-null wrapper/body calls and two joined independent worker calls, for an actual BODY delta5. That is distinct from each worker's actual wrapper-dispatch counter, from the synthetic owned-error/postflight test and from the caught-panic boundary fixture. The same native binary path is recorded at line66 in the isolated/layout logs; no native caller or SQL/PB-origin counter is inferred from these unit tests.

The four TiDB failures remain explicitly non-green:

- `pushdown_catalog::tests::ifnull_string_column_literal_uses_go_signature_and_column_collation`
- `tests::builtin_info_json_math_source::exp`
- `tests::builtin_math_misc_op_source::vectorized_builtin_op_func`
- `time_fn::tests::str_to_date_partial_formats_follow_no_zero_date`

The parent, not C, performed the complete normalized comparison. No assertion, expected result or failure baseline was changed to obtain this checkpoint. The first E0624/zero-tests failure remains recorded in C4-source-r1, and its only correction remains the exact NEW fixture accessor replacement with inverse-hash proof.

Acceptance covers the closed helper's demonstrated construction, owner observation/prewarm/health, wrapper/body distinction, existing-suite comparison and remeasured frame footprint. It does not claim a generic evaluator/heap estimator, hard allocation peak, whole caller-pool/epoch/concurrency correctness, native context/provenance/metadata forwarding, native ASCII deletion or performance. Newly observed worker/program/input numeric layouts are not invented from the unchanged frame tuple. A's independent wider batch review is still pending at this receipt. Parent has separately loaned E only two NEW unwired TiDB adapter/test files for the actual C4 worker; no existing Ctx/dispatcher/native ASCII change is part of C's source cut. The parent owns guide/main-plan updates and further gates. Family coverage remains **0/245**.



