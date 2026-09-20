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

### The permanent exceptions

Some names cannot move into the engine at any point, because the pinned `tipb`
cannot name them. They are not "work to schedule": after the native evaluator
is deleted they must raise the classified "no engine" error rather than execute
natively. As of this round they are six, and the reason column says so:

| Name | Why it can never move |
| --- | --- |
| `translate`, `weight_string`, `load_file`, `json_schema_valid` | no `ScalarFuncSig` for the function in the pinned `tipb` |
| `localtime`, `localtimestamp` | no `ScalarFuncSig` whose name contains `LocalTime`, although the other clock spellings all have one |

Everything else that is excluded today is either a TiKV kernel to write
(`NO_ENGINE_KERNEL`), a cast spelling the local cast arm must not serve
(`EXPLICIT_CAST_SPELLING`), a host capability to add (the other clock names),
or untriaged (`NOT_TRIAGED`), and the table's reason column distinguishes
them.


## 1. Measured inventory

| Item | Count | Where |
| --- | --- | --- |
| Native kernel modules | 11,353 lines in 5 files | `scalar_function.rs` 4,297; `ops.rs` 2,452; `string_fn.rs` 2,308; `builtin_compare.rs` 1,617; `arg_eval_type.rs` 679 |
| Temporal family | 8 files | `crates/tidb-expr/src/time_fn/` |
| JSON / extended builtins | 20 files | `crates/tidb-expr/src/builtin_ext/` |
| `.eval(` call sites in the workspace | 354 | 273 inside `tidb-expr/src` (the evaluator's own recursion, which dies with it) and 81 outside |
| `.eval(` sites outside the adapter | **66** | 38 in a row loop or comparator, 20 against a single chunk row, 8 against `Row::empty()`; 15 of the 81 raw hits are not the evaluator (no-argument folding helpers, the planner's `metadata.eval`, the statement predicate's four-argument `eval`) |
| of those, that must actually be rerouted | **44 production** (22 test-only) | Test-only hits are re-pointed with the corpora instead. 10 documented sites are already converted, one of which keeps a documented sparse-column fallback. The seams are `evaluator::{eval_constant_row, eval_row_values, eval_chunk}`. Classified by `rust/scripts/classify-native-eval-sites.py`; per-file breakdown in `tikv-expression-removal-native-sites.md` |
| Go-test source ports | 33 files, 413 `#[test]` | `crates/tidb-expr/src/tests/*_source.rs` |
| `tikv-expr` feature mentions in the workspace | 75 | `grep -rn tikv-expr --include=*.rs --include=*.toml --include=*.sh --include=*.py .` from `rust/`: crates, difftests, scripts, manifests |
| `cfg(not(feature = "tikv-expr"))` arms | 0 | — |

The last row matters: there is no "engine off" code to remove. The feature
only *adds* the engine, so flipping the default is a matter of making the
engine context mandatory rather than of deleting conditionals.


## 2. Consumers that must be rerouted

Every one of the 44 production reroutable sites either moves to the engine or
disappears; the 22 test-only sites are re-pointed with their corpora.
`tikv-expression-removal-native-sites.md` has the by-file inventory and the
reproducible classifier; the kinds are:

* **Projection** (`tidb-executor`): already goes through the engine when a
  statement context opts in. This is the only fully migrated path.
* **Planning-time constant folding and estimation** (`tidb-planner`, and
  `tidb-expr`'s own fold helpers): these evaluate with `NoColumns` or a
  static context that has no session. Three DDL constants now use
  `eval_constant_row`; the remaining sites need either an engine context that
  can answer "no host capability" structurally or a compilation cache keyed by
  the constant subtree. `Columns::tikv_expression_required` plus the structured
  `ExternalEngine` error is the seam.
* **Predicates and filters** (`tidb-executor` scan sources): `selection.rs`
  still evaluates a filter natively. A projection can make an engine-row receipt
  for the same SQL statement, but that is not evidence that the predicate moved.
* **Aggregation and sorting helpers** (`tidb-executor`, `tidb-expr`):
  `VecGroupChecker` now batch-evaluates grouping keys with retained suites;
  the hash/stream aggregate and sort-key routes remain native.
* **Generated columns, defaults and CHECK constraints**: these evaluate
  during DML with a statement context, so they can be moved with projection,
  but they are separate call sites.

### Conversion receipt

Ten sites no longer call the native evaluator: three constant-row in `ddl/` and
four row-with-columns in `partition_pruning.rs` go through the engine helpers
(one of those four keeps its native call for the sparse-column shape, which the
helper refuses rather than guess), one row loop in `vec_group_checker.rs` now
evaluates each grouping item for the whole chunk, and two in `sort.rs`
(`compare_rows`'s out-of-range branch) were removed as provably error-only --
that branch's `Expression::eval` could only return `column index is outside the
input row`. Every further site that *returns a value* must arrive with the same
three receipts, because the first one alone is not enough:

1. **the context's static type** -- the helpers take `C: Columns` by value
   reference, so a `&dyn tidb_expr::Columns` cannot use them (`run` needs
   `C: Sized`; `C: ?Sized` compiles the bound but not its body). Two sites are
   in that position and keep their row evaluation deliberately.
2. **a green suite in both feature modes** -- necessary, not sufficient: the DDL
   and pruning suites run without an engine context and would pass through the
   fallback.
3. **an engine-execution assertion** -- `tikv_expression_rows() > 0` under
   `with_tikv_expression(true)`, plus the value agreeing with the native run.
   `tidb-executor/tests/tikv_expression.rs` has that shape for both DDL sites
   and `range_pruning_evaluates_through_the_engine` for one pruning site.

The per-row kinds (36 sites in a loop or comparator) are excluded from this
receipt because they must not be wrapped: they need the evaluation moved out of
the loop so the engine sees a batch, and no helper makes that safe.

The seven engine-helper sites now have evidence of conversion, in both input
shapes. The two
`ddl/table_partition_list.rs` sites plus the one `ddl/table_partition_range.rs`
site go through
`tidb_expr::evaluator::eval_constant_row` (no input columns), proven by
`tidb-executor/tests/tikv_expression.rs` asserting `tikv_expression_rows() > 0`
for a `PARTITION BY LIST COLUMNS` table. The four
`partition_pruning.rs` sites go through `eval_row_values` (one row with
columns), proven by `range_pruning_evaluates_through_the_engine` asserting the
same pruned ids *and* `tikv_expression_rows() > 0`. `eval_row_values` takes the
chunk's column types from the expression and refuses a sparse column set
(`Ok(None)`, caller keeps its row evaluation), because a datum-derived chunk
layout can disagree with the declared type the engine reads cells by. Both
helpers compile per call, so they are for once-per-statement sites. The free
`evaluator::eval_chunk` has the same one-off rule; a per-row operator retains an
`EvaluatorSuite` and calls `EvaluatorSuite::eval_chunk`, which keeps its
`EvaluatorProgram` and compiled-engine cache across chunks (the
`VecGroupChecker` conversion is the first receipt).
`tikv-expression-removal-native-sites.md` records all of it, including that 44
production sites remain unconverted (66 textual hits, 22 of them test-only).

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
4. Temporal — the non-clock shapes (`AddTime*Null`, `DATE`/`TIME` casts,
   `EXTRACT`) are in. The *clock* is two different problems, and the admission
   table now says which (`SESSION_CLOCK_NEEDS_HOST_CLOCK` vs
   `NO_WIRE_SIGNATURE`):
   * `now`, `current_timestamp`, `curdate`, `current_date`, `curtime`,
     `current_time`, `utc_date`, `utc_time`, `utc_timestamp`, `sysdate` — the
     pinned `tipb` *can* name all ten (`NowWithArg`/`NowWithoutArg`,
     `CurrentDate`, `CurrentTime0Arg`/`CurrentTime1Arg`, `UtcDate`,
     `UtcTimestamp*`, `UtcTime*`, `SysDate*`; the proto spells the UTC ones
     `UTCDate`/`UTCTimestamp*`/`UTCTime*`), but the engine dispatches only
     `SysDateWithoutFsp` and that kernel reads the host's own clock. One
     statement's rows must all read the statement's start time, under the
     session time zone for the UTC forms, so admitting any of them needs a
     TiKV kernel **and** a clock the facade's `Context` carries. This is the
     one family whose blocker really is a host capability.
   * `localtime`, `localtimestamp` — the pinned `tipb` has no variant whose
     name contains `LocalTime` at all, so no host clock could help: they are
     permanent native exceptions, in the same class as `translate`.
   The split is pinned by
   `admission::tests::clock_names_state_the_wire_and_host_clock_facts`.
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
