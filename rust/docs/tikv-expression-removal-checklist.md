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
| Native `eval` call sites outside the adapter | 361 | `tidb-expr` 274, `tidb-executor` 72, `tidb-planner` 15 |
| Go-test source ports | 33 files, 413 `#[test]` | `crates/tidb-expr/src/tests/*_source.rs` |
| `tikv-expr` feature mentions in the workspace | 65 | crates, difftests, scripts, manifests |
| `cfg(not(feature = "tikv-expr"))` arms | 0 | — |

The last row matters: there is no "engine off" code to remove. The feature
only *adds* the engine, so flipping the default is a matter of making the
engine context mandatory rather than of deleting conditionals.


## 2. Consumers that must be rerouted

Every one of the 361 call sites either moves to the engine or disappears:

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
  equal values, metadata and warnings (the differential helper in
  `tests/tikv_coverage.rs` is the pattern).
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


## 4. Feature and flag removal

* 65 `tikv-expr` mentions: manifests (`crates/tidb-expr/Cargo.toml`,
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
6. Control flow (`IF`/`CASE`/`COALESCE`/`AND`/`OR`) — needs Milestone C
   finished and the capability query wired.
7. Session-dependent and effectful functions — need the `HostEval` trait; until
   then they are explicit refusals, which is acceptable, because the goal is
   one evaluator, not one evaluator that does everything.
8. `Set` input — needs Milestone D.


## 7. Open questions

* Whether planning-time constant folding should compile through the engine per
  constant or keep a narrow literal-only fast path. The removal goal argues for
  the engine, but the compilation cost for tiny constants is unmeasured.
* Whether the differential surface can be extended to all 232 admitted names
  before deletion, or whether some families are deleted with a smaller fixture
  set and a recorded gap.
* Whether the pinned fork dependency is acceptable for the final state or must
  be upstreamed first (the packaging question from the earlier discussion).
