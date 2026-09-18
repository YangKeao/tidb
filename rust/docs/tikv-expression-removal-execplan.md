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
- [ ] Milestone A (point 6): engine shareable and thread-safe.
- [ ] Milestone B (point 2): explicit admission table and silent-fallback gate.
      Partial: the gate mechanism landed — `FallbackReason` (`not-admitted`,
      `unrepresentable-input`) is reported through
      `Columns::record_tikv_expression_fallback`, `StmtContext` keeps two
      reason counters, and the SQL differential helper fails when an admitted
      projection records a decline or a native one records no reason. The
      admission table itself is in progress.
- [ ] Milestone C (point 1): lazy/short-circuit evaluation in TiKV, switch and
      vectorized short-circuit in TiDB.
- [ ] Milestone D (point 4): the type support the removal actually needs.
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

Start from the native surface inventory produced in milestone B. Add to
`tidb_query_datatype` only what that inventory requires, with round-trip
fixtures captured from the Rust native encoding: `Set` (bytes plus `elems`
metadata) and the array element form if the native surface exposes one.
`Geometry` is included only if the native evaluator implements any geometry
expression. Wire encoding must be byte-identical to the TiDB side or the
adapter must refuse the type.

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
