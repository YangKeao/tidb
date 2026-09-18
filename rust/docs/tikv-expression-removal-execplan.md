# Replace the native expression evaluator with TiKV's engine


This is a living ExecPlan governed by repository-root `PLANS.md`. It supersedes
`tikv-expression-coverage-execplan.md` as the plan of record: the earlier plan
added a second implementation, this one removes the first. It is an
exploration on YangKeao forks, not an upstream commitment.


## Purpose / Big Picture


Today `tidb-expr` contains two evaluators: its own (`scalar_function::eval`,
`ops`, `time_fn`, `string_fn`, `builtin_ext`, the comparison and control
families) and the TiKV RPN engine reached through the `tikv-expr` feature. The
goal is to make TiKV's engine the **only** evaluator, so that one set of
kernels defines SQL value semantics everywhere.


The purpose of the plan is to sequence that removal so each step is verifiable
and reversible. Definition of done, stated as behavior: the `tikv-expr`
feature disappears (the engine is always on), no code path evaluates an
expression through the old kernels, and the full Rust suite plus the
mysql-tester replay pass with the engine executing every expression. Until
then the native path stays and remains the default.

The feature gate is the temporary coexistence mechanism. It is deleted only
at milestone E, and deleting it is the signal that the replacement is
complete. A claim that the old implementation has been removed is only valid
once no `#[cfg(not(feature = "tikv-expr"))]` arm and no `tikv-expr` feature
remain in the workspace.


## Progress


- [x] Confirm the direction and record the semantic gap list in TiKV
      (`components/tidb_query_expr/EXPRESSION_SEMANTIC_GAPS.md`).
- [x] Milestone A (point 6): engine shareable and thread-safe. TiKV metadata
      is `Send + Sync` (`Box<dyn Any + Send + Sync>`, 24 codegen references),
      `PreparedExpression` is asserted `Send + Sync`, and a compiled program is
      split from caller-owned `ExecutionState` (`eval_shared`,
      `eval_with_state`, `eval_borrowed_shared`). A two-thread test evaluates
      one `Arc<PreparedExpression>` against a repeated, reversed selection and
      matches the single-threaded run; TiKV is at 443 passing tests. On the
      TiDB side the compiled programs are cached on the shared
      `EvaluatorProgram` behind a lock held only for the compile-on-context-
      change step, so every projection worker of one plan reuses one
      compilation and evaluation is lock-free.
- [x] Milestone B (point 2): explicit admission table and silent-fallback gate.
      `tikv/admission.rs` holds one sorted row per SQL name (384 rows = 309
      Go-derived registry names + 75 synthesized spellings; 232 admitted, 152
      excluded with a reason); `lowering.rs` takes its name-level gate and its
      family routing from the table, and tests fail if a registered or
      synthesized name has no row. The gate half is also in: `FallbackReason`
      is reported through `Columns::record_tikv_expression_fallback`,
      `StmtContext` counts the two reasons, and the differential tests fail
      when an admitted projection records a decline or a native one records no
      reason. The inventory reports per-signature
      admitted/excluded/untested status (385/129/126).
- [ ] Milestone C (point 1): lazy/short-circuit evaluation in TiKV, switch and
      vectorized short-circuit in TiDB.
      Partial: engine-side steps 1-4 are done and pushed (TiKV `d663e88`).
      `RpnFnMeta` gained `lazy_fn_ptr` and the evaluator became `eval_subtree`
      with `child_roots` (with `lazy_fn_ptr: None` everywhere the crate suite
      stayed at 443, proving the refactor behavior-preserving). All 31 control
      and logical dispatches are now lazy — IF, IFNULL, COALESCE, CASE WHEN and
      three-valued AND/OR/XOR across Int/Real/Decimal/Time/Duration/String/Json
      — at 466 passing tests, with `with_lazy` clearing `borrowed_fn_ptr` so the
      borrowed facade refuses a lazy program. The enrolled mysql replay is
      unchanged by the lazy kernels: 142 of 10,251 compared statements diverge
      in both copying and borrowed mode, with the same divergence set as before
      the change (`md5 7b6445a8493f445641a9d07d787f0cba`) and the same
      12,447 engine expression-row evaluations over 1,555 statements
      (10,670 of them borrowed).
      The adapter's shape relaxation is DONE: the admission table drops its leaf
      rule for exactly the names whose every dispatched signature is lazy
      (`if`, `ifnull`, `coalesce`, `case`, `casewhen`, `and`, `or`) and keeps it
      for the families the engine still evaluates eagerly (`elt`, `field`,
      `interval`, `greatest`, `least`, `in`). A control node with a nested child
      now executes in the engine; `tikv_lazy.rs` asserts the engine ran, with no
      recorded fallback, for every short-circuit case. With the relaxation the
      replay executes MORE in the engine with the SAME results: 12,493
      expression-row evaluations over 1,560 statements, 142 of 10,251
      divergences, divergence set md5 `7b6445a8493f445641a9d07d787f0cba`.
      DONE through the engine-enforced gate: TiKV `01780f8` exposes
      `has_lazy_nodes()` and `eager_lazy_risk()` with a dispatcher-integrity
      test over `ScalarFuncSig::values()`, and the adapter refuses exactly the
      dangerous mix of a lazy node and an eager lazy-sensitive node
      (`IF(1, ELT(1,'a'), 'b')` is refused; `IF(1,'a','b')` and `ELT(1,'a')`
      are admitted, so standalone Tier-2 coverage is preserved). Remaining: the
      TiDB-side switch and the Tier-2 signatures. The design is
      `components/tidb_query_expr/SHORT_CIRCUIT_DESIGN.md`; the adapter-side
      acceptance test is `crates/tidb-expr/tests/tikv_lazy.rs`.
- [ ] Milestone D (point 4): the type support the removal actually needs.
      Partial: the datatype half is done and pushed (TiKV `35fd80a`).
      `FieldTypeTp::Set` maps to `EvalType::Set` and `Set`/`SetRef`/
      `ChunkedVecSet`, the chunk and raw-datum codecs, the `Int`/`Bytes` hybrid
      borrows and the scalar/vector/datum encode arms now mirror `Enum`
      (318 datatype tests, +18). A latent `Column::get_enum` bug was found and
      fixed: it indexed `idx * fixed_len` on a var-length column and therefore
      failed for every row, not just `idx > 0`. The engine facade half is also
      done (TiKV `01780f8`): `Column::Set`, the owned round-trip through
      `VectorValue::Set`, and the reachable `cast_set_as_int` kernel, with the
      temporary `EvalType::Set` refusal removed and the old rejection test
      turned into a positive one. The TiDB bridge is done too (TiKV `866c575`):
      `Family::Set` reads the ENUM-shaped chunk cell and returns `Datum::Set`,
      the borrowed path excludes SET (its SQL eval family is String, so the
      Bytes loader would have fed `[bitmask][name]` to a string kernel), and the
      local wire helper builds a SET leaf itself because the shared catalog
      keeps Go's refusal of SET for distributed pushdown. Fixtures round-trip a
      SET column through the engine and read its name with `LENGTH`, which also
      exercises the engine's hybrid Int/Bytes carrier. Remaining for `Set`:
      cast targets other than `AS SIGNED` if the native surface needs them.
- [ ] Milestone E: flip the default, delete the native evaluator and the
      feature gate, prove parity.


## Surprises & Discoveries


The parity target is narrower than "all of MySQL": it is whatever the native
evaluator can do today. Functions the Rust port never implemented are out of
scope for the removal, because deleting code cannot lose behavior that does
not exist. Milestone D therefore starts from an inventory of the native
surface, not from a wish list.

`tidb_query_expr` is declared `publish = false` and the TiDB dependency is a
`git` dependency on the whole TiKV workspace. Removing native makes
`tidb-expr` unusable without that fork checkout, which is acceptable for this
exploration but is the reason packaging (the previous point 7) cannot be
postponed forever.

Only about 30 sites make the compiled engine non-`Sync`: four
`Box<dyn Any + Send>` metadata signatures and 24 codegen references. The
thread-local varg buffers do not block `Sync`; they require only that one
compiled program is not executed concurrently on two threads.

The upstream issue `pingcap/tidb#70156` proposes the switch
`tidb_enable_short_circuit_expression` and says TiKV "can introduce
short-circuit expression nodes". It is an open, unimplemented proposal: the
variable does not exist in either checkout and the dispatcher has no lazy
node. Milestone C therefore implements laziness with the wire format
unchanged, by making the evaluator consult a lazy-signature set instead of
emitting new node kinds. That keeps `tipb` untouched, which matters because
the wire schema lives in a different repository.

The lazy design (`components/tidb_query_expr/SHORT_CIRCUIT_DESIGN.md`) found
that materializing a lazy child at a subset boundary must produce an owned
`VectorValue`, because an `RpnStackNode` borrows one lifetime; that the
thread-local varg buffers must not be held across a nested evaluation; that a
lazy kernel must keep `borrowed_fn_ptr: None` or the borrowed facade panics;
and that `logical_rows()` has a latent panic at exactly `BATCH_MAX_SIZE`
generated rows, so the lazy path must use the indexed accessors. The first
implementation step registers no lazy kernel, so the existing suite proves the
refactor is behavior-preserving before any semantics change.

The type inventory (`tikv-expression-type-gaps.md`) finds that `Set` is the
only missing value type; `Geometry` and arrays have no native datum or builtin
and need only explicit refusal. TiKV already contains unreachable `Set`
scaffolding, and extending the existing `Int`/`Bytes` hybrid carriers makes
every string/int kernel accept `Set` without a new ordinary signature.


## Decision Log


- Decision: parity target is the current native surface, not all of Go TiDB.
  Rationale: removal cannot regress behavior that does not exist, and chasing
  the full Go surface would make the first removal unreachable.
- Decision: keep laziness out of the wire format. Rationale: `ExprType` lives
  in `tipb`; a signature-driven lazy path inside `tidb_query_expr` needs no
  schema change and no second fork.
- Decision: admission becomes an explicit allow-list keyed by signature and
  shape, and silent fallback becomes a test failure. Rationale: a deny-list
  is silently wrong the moment a new signature appears, which is exactly the
  failure mode removal must not have.
- Decision: engine host capabilities (clock, RNG, user variables, locks,
  sequences, statement state) are injected through a trait, not through
  globals. Rationale: the engine must stay usable from tests and from
  planning-time call sites with no session.
- Decision: an error is never retried natively once a kernel has run.
  Rationale: warnings, RNG draws and lock side effects are not replayable.
- Decision: engine wording does not have to match Go; error-versus-success
  classification does. Rationale: agreed scope with the requester.


## Outcomes & Retrospective


In progress. Nothing is removed yet; the native evaluator is still the default
and the engine remains opt-in behind `tikv-expr`.


## Context and Orientation


Repositories: `/home/agent/tidb/expression-reuse/tidb` (branch
`feat/tikv-expression-coverage`, engine code `abffd0ab`) and
`/home/agent/tidb/expression-reuse/tikv` (branch
`feat/standalone-expression-coverage`, code `6806dc4d`, gap list `f62bcfc6`).

The native evaluator lives in `rust/crates/tidb-expr/src/`:
`scalar_function.rs` (`ScalarFunction::eval`, `eval_by_signature`),
`ops.rs`, `compare`/`control` handling inside `scalar_function.rs`,
`time_fn/`, `string_fn.rs`, `builtin_ext/` (JSON), `arg_eval_type.rs` and the
`tests/` source-port corpora. Its consumers reach it through
`Expression::eval` (361 call sites outside the adapter: 274 in `tidb-expr`,
72 in `tidb-executor`, 15 in `tidb-planner`) and through `EvaluatorSuite`.

The engine adapter lives in `rust/crates/tidb-expr/src/tikv/` (`tikv.rs`
compilation and routing, `bridge.rs` value transport, `lowering.rs` and
`lowering/families.rs` signature selection) and in
`rust/crates/tidb-executor/src/stmt_context.rs` for the session context.

On the TiKV side the embedding seam is
`components/tidb_query_expr/src/standalone.rs` plus its child modules; the
evaluator is `components/tidb_query_expr/src/types/expr_eval.rs`; the
dispatcher is `components/tidb_query_expr/src/lib.rs`; function metadata types
are in `types/function.rs` and `types/expr_builder.rs`; the code generator is
`components/tidb_query_codegen/src/rpn_function.rs`.

Terms: a *signature* is a typed function identity (`ScalarFuncSig`), not a SQL
name. *Admission* is the decision to let a given expression tree run in the
engine. *Fallback* is a native evaluation performed instead of an engine one.
*Compiled program* is the immutable RPN form; *execution state* is the
per-call scratch the evaluator needs.


## Plan of Work


### Milestone A — the engine stops being per-execution and single-threaded

Make the compiled form `Send + Sync` and separate it from execution state.
On the TiKV side change the metadata signature from `Box<dyn Any + Send>` to
`Box<dyn Any + Send + Sync>` in `types/function.rs`, follow the compile errors
through the metadata constructors in the kernel files and through the 24
codegen references, and update `STANDALONE.md`. Then give the facade a
compiled form that can be evaluated through `&self`: either move the mutable
stack and output buffers into an explicit `ExecutionState` argument to
`eval_decoded`, or add a variant that allocates scratch per call. The stock
server path may keep its current shape; the standalone path is what must
become shareable.

On the TiDB side, cache the compiled engine program on the shared
`EvaluatorProgram` instead of in the per-suite `Mutex<ProjectionCache>`, so
parallel projection workers share one compilation with no lock on the hot
path. The cache key is the canonical expression hash plus the input column
schema. Report cache hits and misses in the coverage counters.

After this milestone a compiled program can be created once and evaluated from
several threads, and the feature-on suites still pass.

### Milestone B — one table decides admission, and silence fails

Replace the name-matching `admitted()`/`local_call()` logic with a table whose
rows are (signature, required argument eval types, shape constraints,
exclusion reason). Shape constraints cover the lazy rules and the
constant/leaf rules that exist today. Every SQL function name the Rust
rewriter can produce must appear either as an admitted row or as an excluded
row with a reason; generate that name list from the same registry the
rewriter uses, so a new function cannot appear without a decision.

Turn fallback into a gate. In test builds, when an expression compiles for the
engine but executes natively, record the reason and fail unless the reason is
an explicitly listed exclusion. The existing engine-row counters already
distinguish the cases; the work is to assert rather than to observe.

The inventory generator consumes the table so the CSV/JSON report shows
admitted, excluded and untested status per signature, not source-text
evidence.

### Milestone C — laziness

In TiKV, define the set of signatures that must see unevaluated children
(`if`, `ifnull`, `coalesce`, `case`, `and`, `or`, `xor`, `elt`, `field`,
`interval`, `greatest`, `least`, the `addtime*null` forms and `nulltimediff`).
Teach the evaluator to walk those with a lazy path: evaluate the selector
first, then evaluate only the children still required for the rows that still
need a value, and merge while preserving SQL three-valued logic. In the
row-oriented path this is an interpreter change; do not change `tipb`.

The TiDB side must not have to guess which signatures are lazy, because
compiling an eager kernel and a lazy kernel succeeds identically and an eager
one would silently produce the wrong semantics for a skipped branch. TiKV
therefore exposes a capability query (`is_lazy_signature(signature)`), and the
admission table's lazy shape rule consults it: a lazy shape is admitted only
when the engine reports that signature lazy. A test asserts the table's lazy
set and the engine's reported set agree, so adding or removing a lazy kernel
cannot drift silently. Until a signature is lazy, its non-leaf shapes stay
native exactly as today.

Provide the host capability trait at the same time, because laziness without
it still cannot express `IF(cond, getvar(...), 0)`:

    trait HostEval {
        fn current_time(&self, fsp: i8) -> Result<Time>;
        fn rand(&self) -> f64;
        fn user_var(&self, name: &str) -> Option<ScalarValue>;
        fn statement_value(&self, which: StatementValue) -> Result<ScalarValue>;
        fn advisory_lock(&self, op: LockOp) -> Result<i64>;
        fn max_allowed_packet(&self) -> usize;
        fn default_week_format(&self) -> i64;
    }

The standalone `Context` gains an optional implementation of this trait;
`None` means "capability absent" and the call returns a structured
`Unsupported` error rather than 0 or a panic.

On the TiDB side add the session variable and DAG flag from the issue, and let
the local adapter admit non-leaf lazy shapes only when the engine reports the
lazy path. TiDB's own vectorized short-circuit for `AND`/`OR` (selection
based) is part of this milestone because the two must agree.

### Milestone D — types the removal needs

Start from the native surface inventory produced in milestone B. The completed
inventory (`tikv-expression-type-gaps.md`) finds that **`Set` is the only value
type that must be added**; `Geometry` and arrays are not needed because the
native evaluator has no datum kind and no builtin for them. `Set` is an
input-only value: no builtin returns one and there is no wire `SetLiteral`, but
every implemented `ETString`/`ETInt` builtin accepts one, so all `Set`
expressions fall back today. TiKV already owns most of the scaffolding
(`EvalType::Set`, `ScalarValue::Set`, `VectorValue::Set`, `SetRef`, the
`*ForSet` aggregators, `cast_set_as_int`) but it is unreachable; the additions
are the `FieldTypeTp::Set` mapping, chunk/raw codecs whose name is the
comma-joined selected `elems`, the `ChunkedVecSet` element-name storage, the
`Int`/`Bytes` hybrid borrow arms, the standalone `Column::Set`, and registering
the existing cast. No new ordinary kernel signature is needed, because
extending the two hybrid carriers makes the existing string/int kernels accept
`Set` exactly as they accept `Enum` today. A latent `get_enum` var-length
indexing bug in the TiKV chunk codec is flagged while adding `get_set`.

### Milestone E — remove

Flip the default to the engine, delete the `tikv-expr` feature, delete the
fallback branches, then delete the native kernels and their source-port tests
whose subject is now the engine, keeping the Go-oracle corpora that still
validate results. Before deleting, run the full Rust suites, the enrolled
mysql replay, and the Go expression suites as the external oracle. Record the
remaining exclusions as explicit errors, not as silent native execution.


## Concrete Steps


One heavy command runs at a time across both repositories, through
`/home/agent/tidb/expression-reuse/tools/limited-run.py`, with
`CARGO_BUILD_JOBS=1`, `RUST_TEST_THREADS=1` and single-threaded Go. Use
`RUST_MIN_STACK=2097152 MALLOC_ARENA_MAX=2` whenever a suite spawns worker
pools, otherwise the address-space cap turns thread creation into `EAGAIN`.

TiKV:

    cd /home/agent/tidb/expression-reuse/tikv
    python3 /home/agent/tidb/expression-reuse/tools/limited-run.py -- \
      cargo test -p tidb_query_expr --lib -j1 -- --test-threads=1

TiDB Rust (engine on), with the local engine checkout patched in during
development:

    cd /home/agent/tidb/expression-reuse/tidb/rust
    python3 .../limited-run.py -- cargo test -p tidb-expr --features tikv-expr --lib --test all -j1 \
      --offline --config 'patch."https://github.com/YangKeao/tikv.git".tidb_query_expr.path="/home/agent/tidb/expression-reuse/tikv/components/tidb_query_expr"' \
      -- --test-threads=1

Expected: the feature-on counts are 1200 + 39 for `tidb-expr`, 353 for
`tidb-executor`, 1726 + 338 for `tidb-session`; feature-off is 1180 + 18,
329 and 1726 + 337. Any drop means a regression in the coexistence period.

The replay is the semantic gate:

    INTEGRATION_TIKV_BACKEND=copying python3 .../limited-run.py -- \
      cargo test -p difftest-result-tests --features tikv-expr --test integration_diff -j1 -- --test-threads=1

Its current state is 142 divergences out of 10,252 compared statements,
identical to the native baseline and pre-existing; `expr_diff` (2 cases),
`table_diff` (7 of 1,942) and `join_shape` (stale ratchet) are red with the
feature disabled too. Milestone E requires those to be resolved or explicitly
ratcheted with a reason.


## Validation and Acceptance


Milestone A: a token `PreparedExpression` (or its replacement) is `Send + Sync`
proven by a compile-time assertion, two threads evaluate one compiled program
concurrently producing identical output to a single-threaded run, and the
feature-on suites keep their counts.

Milestone B: every function name in the rewriter registry has an admission
row; a deliberately admitted-but-not-executing expression fails the test; the
inventory reports per-signature status.

Milestone C: `IF(0, <overflow>, 7)` executes in the engine and returns 7 with
no error and no warning; `IF(1, 1, <overflow>)` likewise; an unused branch
containing `GETVAR` does not run; TiDB and TiKV agree on `AND`/`OR` with NULL
operands under selection; the issue's switch exists in TiDB and is propagated.

Milestone D: each added type round-trips through the adapter and matches the
native evaluator on the same fixture; a type whose encoding cannot be matched
is refused with an explicit error.

Milestone E: `rg 'tikv-expr' rust/` returns no feature gate, the native
evaluator modules are deleted, and the full suites plus the replay pass.


## Idempotence and Recovery


Every milestone is additive until E. The native path stays default until the
last step, so a failed experiment can be reverted by dropping a commit rather
than by repairing a broken evaluator. Cache and admission-table changes are
pure functions of the expression tree and input schema, so re-running is safe.
Deleting native code is the only irreversible step and is gated on the
milestones before it.


## Artifacts and Notes


`components/tidb_query_expr/EXPRESSION_SEMANTIC_GAPS.md` (TiKV) is the running
list of kernel divergences. This plan, the coverage report and the inventory
generator under `rust/docs` and `rust/scripts` are updated as milestones land.
The earlier copying/borrowed benchmark artifacts remain untouched.


## Interfaces and Dependencies


TiKV keeps `PreparedExpression::compile(serialized_expr, serialized_schema,
Context)` and gains a `Sync` compiled form with an explicit execution-state
argument. It exposes `scalar_function_signature(name) -> Option<i32>` and the
host trait above, and its `Context` carries the optional trait object.
`tidb_query_datatype` gains only the types milestone D justifies.

TiDB's `tikv.rs` keeps `TikvExpression::compile` and `evaluate`/`evaluate_into`
signatures, gains a per-`EvaluatorProgram` cache, and consumes the admission
table instead of name matching. The `Backend` enum and counters stay until
milestone E. No new dependency on a second fork is introduced.
