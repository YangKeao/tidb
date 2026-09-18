# Deletion checklist for the native expression evaluator

This is the working checklist for Milestone E of
`tikv-expression-removal-execplan.md`. It records what has to change before
TiDB Rust's own evaluator can be deleted, and what deleting it does and does
not mean. Numbers were measured on branch `feat/tikv-expression-coverage`.


## 0. What is deleted, and what is not

Deleted: the *evaluation kernels and their row/vector entry points* — the code
that turns an expression plus a row into a value.

Not deleted: the expression tree (`Expression`, `Column`, `Constant`,
`ScalarFunction`), type inference and field types, the rewriter/planner, the
chunk and datum types, the session variable machinery, and the parser. Those
are metadata and structure; the engine consumes them and does not replace them.

Therefore "the native implementation is gone" is a statement about
`ScalarFunction::eval`-style code only, and the checklist below is scoped to
that.


## 1. Measured inventory

| Item | Count | Where |
| --- | --- | --- |
| Native kernel modules | 11,353 lines in 5 files | `scalar_function.rs` 4,297; `ops.rs` 2,452; `string_fn.rs` 2,308; `builtin_compare.rs` 1,617; `arg_eval_type.rs` 679 |
| Temporal family | 8 files | `crates/tidb-expr/src/time_fn/` |
| JSON / extended builtins | 20 files | `crates/tidb-expr/src/builtin_ext/` |
| `.eval(` call sites in the workspace | 361 | 271 inside `tidb-expr/src` (the evaluator's own recursion, which dies with it) and 90 outside |
| `.eval(` sites outside the adapter that must be rerouted | **75** | 36 in a row loop or comparator, 31 against a single chunk row, 8 against `Row::empty()`; 13 of the 90 are not the evaluator and 2 are a planner test helper. Per-file breakdown in `tikv-expression-removal-native-sites.md` |
| Go-test source ports | 33 files, 413 `#[test]` | `crates/tidb-expr/src/tests/*_source.rs` |
| `tikv-expr` feature mentions in the workspace | 75 | `grep -rn tikv-expr --include=*.rs --include=*.toml --include=*.sh --include=*.py .` from `rust/`: crates, difftests, scripts, manifests |
| `cfg(not(feature = "tikv-expr"))` arms | 0 | — |

The last row matters: there is no "engine off" code to remove. The feature
only *adds* the engine, so flipping the default is a matter of making the
engine context mandatory rather than of deleting conditionals.


## 2. Consumers that must be rerouted

Every one of the 75 reroutable sites either moves to the engine or disappears.
`tikv-expression-removal-native-sites.md` has the by-file inventory and the
reproducible grep; the kinds are:

* **Projection** (`tidb-executor`): already goes through the engine when a
  statement context opts in. This is the only fully migrated path.
* **Planning-time constant folding and estimation** (`tidb-planner`, and
  `tidb-expr`'s own fold helpers): these evaluate with `NoColumns` or a
  static context that has no session. They need either an engine context that
  can answer "no host capability" structurally, or a compilation cache keyed
  by the constant subtree. `Columns::tikv_expression_required` plus the
  structured `ExternalEngine` error is the seam; the call sites are not yet
  converted.
* **Predicates and filters** (`tidb-executor` scan sources): the filter
  entry point uses the engine only when the context opts in; the non-engine
  filter path evaluates rows natively.
* **Aggregation and sorting helpers** (`tidb-executor`, `tidb-expr`): group
  keys and comparison keys still call native `eval`/`cmp` helpers.
* **Generated columns, defaults and CHECK constraints**: these evaluate
  during DML with a statement context, so they can be moved with projection,
  but they are separate call sites.

The first conversion is done and has evidence: the two
`ddl/table_partition_list.rs` sites now go through
`tidb_expr::evaluator::eval_constant_row`, and
`tidb-executor/tests/tikv_expression.rs` proves the engine answered
(`tikv_expression_rows() > 0`) for a `PARTITION BY LIST COLUMNS` table. That
helper is for the once-per-statement kind only; it compiles per call, so the
per-row kinds must move the evaluation out of their loop instead.
`tikv-expression-removal-native-sites.md` records which site was converted and
that the other 73 are not.

Each conversion needs the same guarantee the adapter already enforces: a
compilation refusal is decided before evaluation, and a runtime error is never
retried through another implementation.


## 3. Test corpora

The 33 `*_source.rs` files are transcriptions of Go's expression tests and are
the most valuable asset in the crate. They must not be deleted; they must be
**re-pointed** so that each case runs through the engine as well as natively
during the coexistence period, and through the engine only afterwards.

Disposition:

* Cases whose subject is a kernel the engine admits: run both ways and require
  equal values, metadata and warnings. This is now automatic for constant
  cases: `tests/mod.rs::chunk_e` -- the helper nearly every port uses --
  evaluates the same rewritten expression through the engine as well and
  requires agreement, skipping only expressions the adapter declines, and
  comparing errors by classification rather than wording. Its first run found
  seven real divergences (`tikv-expression-corpus-plan.md` and the TiKV gap
  list), all now either fixed at the admission boundary or recorded.
* Cases whose subject is a kernel the engine excludes on purpose
  (session-dependent, effectful, UUID parsing, temporal JSON): these become
  explicit "engine refuses, native answers" tests, and after deletion they
  become "engine refuses" tests. They are the list that Milestone E's error
  surface must cover.
* Cases that assert the native *implementation* rather than SQL behavior
  (internal helper signatures, coercion ladders) are deleted with their code.

413 tests is the size of that re-pointing job. It is deliberately not part of
the earlier milestones, because the corpora are also the oracle that proves the
engine is right.

Three gates now hold that re-pointing in place, all in
`crates/tidb-expr/tests/`:

* `tikv_ratchet.rs` pins the engine-only outcome in both directions: 17
  expressions that moved from native to engine, 59 that still decline, and both
  list lengths. A change that silently adds or removes a fallback fails.
* the same file measures what deletion does to the 59: 57 return the structured
  `ExternalEngine` error naming the refusal, and 2 are planning-time refusals
  (`(1, 2) = (1, 2, 3)`, `convert(... using cp866)`), pinned by name.
* it also measures which of the 59 the planner's construction-time fold removes
  before the adapter sees them: 40 fold to a `Constant`, so the observed
  production surface is 17. `tikv_column_shapes.rs` measures the other
  direction -- 28 of 48 column-bearing shapes run in the engine -- because
  folding cannot remove a shape that carries a column.

The three numbers answer three different questions (adapter surface on unfolded
constants 59, planner-reachable 17, column shapes 28/48), and
`tikv-expression-corpus-plan.md` sections 7.11-7.17 are the record.


## 4. Feature and flag removal

* 75 `tikv-expr` mentions: manifests (`crates/tidb-expr/Cargo.toml`,
  `tidb-executor`, `tidb-session`, `difftests/result-tests`), `#[cfg(feature =
  "tikv-expr")]` gates in production code and tests, and the bench
  targets. They become unconditional in dependency order: `tidb-expr` first,
  then `tidb-executor`, then `tidb-session`, then the harnesses.
* The `Backend::{Copying, Borrowed}` enum stays after deletion — it is the
  adapter choice, not the evaluator choice.
* The `FallbackReason` counters and `tikv_expression_fallback` hook stay: after
  deletion there is no fallback, so the gate changes meaning from "an admitted
  expression stayed native" to "an expression hit an unlisted exclusion", which
  is still worth failing on.
* `Columns::tikv_expression_required` becomes `true` for every production
  resolver, and the engine context becomes mandatory.
* The DAG-level switch from `pingcap/tidb#70156` is independent remote-pushdown
  work; the local adapter needs only the capability query described in the
  milestone C plan section.


## 5. Acceptance gates for deleting a single family

A family may have its native kernels deleted only when all of the following
hold, with recorded evidence:

1. Every signature the family can produce is either admitted by
   `tikv/admission.rs` with a lowering site, or excluded with a reason that is
   a deliberate SQL-level refusal.
2. Every admitted signature has differential evidence: native and engine agree
   on values, nullability, metadata and warnings for a fixture set that covers
   NULLs, empty/single/split batches, selected rows with duplicates, unsigned
   boundaries and the error cases.
3. No fixture records a `FallbackReason` when the engine ran.
4. The engine's lazy set covers the family's lazy signatures if it has any,
   verified through the capability query rather than assumed.
5. The family appears in a green run of the enrolled mysql replay with the
   engine executing, and its divergence set is no larger than the native
   baseline.
6. Source-port tests for the family are re-pointed as described in §3.


## 6. Family order (proposed)

Ordered by risk, not by size:

1. Arithmetic, comparison, bit operations, `IN` — already largely covered.
2. String and `LENGTH` — covered; the packet-limit family stays excluded.
3. Math — covered by the matrix; the extreme-digit guards are in the engine
   facade.
4. Temporal — needs the host capability for "current date" before the
   `AddTime*Null`/`Duration`-to-date shapes can move.
5. JSON — several excluded shapes need the NULL/deprecation/quote fixes from
   `EXPRESSION_SEMANTIC_GAPS.md`.
6. Control flow (`IF`/`CASE`/`COALESCE`/`AND`/`OR`) — Milestone C is done (the
   Tier-1/2/3 kernels are lazy), so what remains is the shapes: a skipped arm
   may only be a leaf, and a *condition* may take Go's own cast (7.17).
7. Session-dependent and effectful functions — need the `HostEval` trait; until
   then they are explicit refusals, which is acceptable, because the goal is
   one evaluator, not one evaluator that does everything.
8. `Set` input — Milestone D is done (the `Set` carriers, codecs, bridge and
   cast registration are in).


## 7. Open questions

* ~~Whether planning-time constant folding should compile through the engine
  per constant or keep a narrow literal-only fast path.~~ Measured in
  `tikv-expression-corpus-plan.md` 7.13: the planner folds with the live
  statement context before a plan exists, so 40 of the 59 corpus declines never
  reach the adapter at all. What is still unmeasured is the *compilation* cost
  of that fold, not whether it happens.
* Whether the differential surface can be extended to all 230 admitted names
  before deletion, or whether some families are deleted with a smaller fixture
  set and a recorded gap.
* Whether the pinned fork dependency is acceptable for the final state or must
  be upstreamed first (the packaging question from the earlier discussion).
