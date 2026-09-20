# The native evaluator's call sites outside projections

Milestone E deletes the native evaluator. Projections are already served by the
engine (`EvaluatorSuite` + `EvaluatorProgram`), so the remaining question is how
many call sites evaluate an expression *without* a projection, and what each
kind needs. This is the measurement, and the method is repeatable:

    cd rust/crates
    grep -rn "\.eval(" --include=*.rs . | grep -v '/tidb-expr/src/'

## What the raw hits are

The raw grep above returns 80 hits at the current branch state. They are
classified mechanically by `rust/scripts/classify-native-eval-sites.py` (its rule is its
docstring), which reads a hit plus the following eight lines so a call whose
arguments span lines is still classified by its argument list:

| Kind | All sites | Production only |
| --- | --- | --- |
| one row of a chunk (`get_row(0)`) | 20 | 11 |
| a row-loop variable or comparator | 37 | 26 |
| a constant with no input row (`Row::empty()`) | 8 | 6 |
| not the evaluator: no-argument `constant.eval()`/`column.eval()`, the planner's `metadata.eval(k)`, the statement predicate's own four-argument `eval(row, catalog, db, ctx)` | 15 | -- |

So the native evaluator surface outside projections is **65 sites**, and every
one of them is row-at-a-time: `Expression::eval` against a single `Row`. Of
those, **43 are production** and **22 only run under `cargo test`** (a file
under a `tests/` directory, a `src/*tests.rs` module, or any line below its
file's first `#[cfg(test)]`); the classifier reports both because test-only
calls are re-pointed with the corpora rather than converted, so 43 -- not 65 --
is the pre-deletion routing work. The earlier hand count in this file said 90
raw and 75 sites; the difference is the 10 raw hits the conversions removed (see
the conversion sections) and a classification that is now a script rather than
a reading. The driver's six-argument `UpdateExpression::eval` is deliberately
counted: it is a wrapper whose branches call `Expression::eval`
(`driver/dml/correlated.rs:134,137`), so it is a real dispatch site even though
the callee is the same evaluator.

One classifier caveat is now confirmed rather than hypothetical:
`tidb-executor/src/stream_agg.rs` has two textual evaluator calls but is an
unlinked duplicate. `lib.rs` exports the actual `StreamAggExec` and
`GroupedStreamAggExec` from `hash_agg.rs`, and `driver/physical_builder.rs`
imports those types; `cargo test --lib -- --list` contains none of
`stream_agg.rs`'s tests. The 65/43 figures remain the mechanical textual gate,
but only **41** of its 43 production-labelled sites are reachable routing work.
Do not convert the dead duplicate; migrate `hash_agg.rs`'s real aggregate paths
instead.

## Production sites by file

Generated from the classifier; the remaining 22 test-only sites are not listed
because they are re-pointed with the corpora, not converted.

| Sites | File |
| --- | --- |
| 7 | `tidb-executor/src/window.rs` |
| 6 | `tidb-executor/src/driver/dml.rs` |
| 4 | `tidb-executor/src/hash_agg.rs` |
| 3 | `tidb-executor/src/driver/dml/correlated.rs` |
| 3 | `tidb-planner/src/ranger/go_cases.rs` |
| 2 | `driver/multi_dml.rs`, `stream_agg.rs` |
| 1 each | `column_default.rs`, `driver/agg_build.rs`, `driver/physical_builder.rs`, `driver/subquery.rs`, `generated_column.rs`, `hash_agg/group_key.rs`, `hash_agg/input.rs`, `join.rs`, `joiner.rs`, `partition_pruning.rs` (the sparse fallback), `predicate_pushdown.rs`, `selection.rs`, `union_scan.rs`, `sort.rs`, `tidb-planner/src/physical/scan_ranges.rs`, `tidb-planner/src/ranger/points.rs` |


## What each kind needs for the removal

* **A row loop or comparator (26 production).** The surrounding loop already
  holds a chunk, so the engine's own strength applies: evaluate the expression
  for the whole chunk *before* the loop and index the result vector. That is now
  one call -- `tidb_expr::evaluator::eval_chunk(expression, ctx, chunk)` -- which
  runs the suite over the caller's chunk and returns one datum per row, so the
  loop does not pick an implementation cell by cell. See the
  `VecGroupChecker` and hash-shuffle conversions below.
* **One chunk row (11 production).** These are probes: partition pruning,
  access-cost estimates, column defaults, generated columns, `dual`, correlated
  subquery inputs. The input is a one-row chunk already, so a one-row engine call
  is sufficient; `eval_chunk` covers it (a chunk with one row), and
  `eval_row_values` covers the variant whose row is a `&[Datum]` instead.
* **A constant with no row (6 production).** `physical_builder` and `agg_build`
  evaluate a constant expression with `Row::empty()`. The engine handles this
  with a virtual one-row chunk (the corpus does it with
  `set_num_virtual_rows(1)`), which is `eval_constant_row`.

None of the 43 needs a new kernel; they need the call to move. What still needs
a *new* engine capability is a different list: the clock family's host clock
(checklist section 6), and the two wire-permanent names.

## The first conversion, with evidence

`tidb-executor/src/ddl/table_partition_list.rs` had two of the "one chunk row"
sites, both of the shape `expression.eval(ctx, dual.get_row(0))` over an
*empty* one-row chunk. They now call
`tidb_expr::evaluator::eval_constant_row(&rewritten, ctx)`, which builds exactly
that chunk and runs the suite, so the engine/fallback choice and the
post-removal structured error come from the same path a projection uses.

The helper deliberately does **not** cache the compiled program: it compiles per
call, which is right for a once-per-statement site and wrong for a per-row loop.
The per-row kinds above need the evaluation moved out of the loop instead.

Evidence that the engine -- not the fallback -- did the work:
`tidb-executor/tests/tikv_expression.rs::tikv_expression_partition_list_values_run_in_the_engine`
runs `CREATE TABLE ... PARTITION BY LIST COLUMNS (v) (...)` with a context built
by `with_tikv_expression(true)` and asserts `tikv_expression_rows() > 0`. The
11 partition-DDL unit tests and both feature modes stay green.

That is one of the 75, and the shape it proves is the smallest one.

## The second conversion: one row *with* columns

`tidb-executor/src/partition_pruning.rs` had four of the "one chunk row" sites,
all evaluating `spec.expr` against a row built from the ranger's bounds. They now
go through `tidb_expr::evaluator::eval_row_values(&expr, ctx, &values)`, which
takes the column values **indexed by the expression's own `Column::index`** and
builds the one-row chunk itself.

Two details are the point of this shape:

* the chunk's column **types** come from the expression
  (`Column::get_static_type`), not from the datums: a datum-derived layout can
  disagree with the declared type a partition expression was typed against, and
  the bridge reads cells by the declared type. The helper collects them with the
  adapter's own `remap_columns`, so the chunk and the wire schema agree by
  construction;
* a **sparse** column set (anything other than `0..values.len()`) returns
  `Ok(None)` rather than guessing a chunk layout, and the caller keeps its row
  evaluation. After the native evaluator is deleted that branch becomes the
  structured engine error.

Evidence, per call site: `range_pruning_evaluates_through_the_engine` and
`list_point_pruning_evaluates_through_the_engine` each run the same pruning as
their Go-derived sibling under `with_tikv_expression(true)`, assert the same
pruned ids (`Some(vec![102])` for the range spec, `Some(vec![201])` for the list
spec) and assert `tikv_expression_rows() > 0`. `hash_point_pruning_evaluates_through_the_engine` does the same for the HASH
point path (`Some(vec![103])`). and the endpoint path is covered by the same
test: a non-point interval (`8..9`) takes `evaluate_range_partition_endpoint`
rather than the point path, so the test asserts the row counter *grows* across
that second run. All four pruning call sites now carry the receipt, and the 23
pruning tests stay green.

`ddl/table_partition_range.rs` has a third constant-row site of the same shape
and is converted too, verified by the same 11 partition-DDL tests **and** by
`tikv_expression_range_partition_values_run_in_the_engine`, which asserts
`tikv_expression_rows() > 0` for a `PARTITION BY RANGE` table. Both DDL
conversions now carry the same strength of evidence: a green suite is not
enough, because the DDL tests run without an engine context and would pass
through the fallback.

So **11 of the 75** documented sites no longer call the native evaluator -- three
constant-row sites in `ddl/`, four pruning sites in `partition_pruning.rs`, two
error-only sites in `sort.rs`, and two retained-suite grouping loops
(`vec_group_checker.rs` and `shuffle.rs`) -- which is what takes the raw grep
from 90 hits to 80, the site count to 65 and the production count to 43. One
converted pruning site still contains a native call by design: the helper's
`Ok(None)` arm keeps its row evaluation for a sparse column set, which is why
the pruning file went from four hits to one rather than to zero. The remaining
43 production sites still need the evaluation moved out of their loop rather
than wrapped.

## The first row-loop conversion: `VecGroupChecker`

`tidb-executor/src/vec_group_checker.rs` evaluated its grouping items cell by
cell (`for row { for item { item.eval(ctx, row) } }`). Go does not:
`VecGroupChecker.SplitIntoGroups` calls `VecEval` once per item and indexes the
resulting temporary column per row, which is exactly the shape a projection has
and exactly what the engine is for. `new` retains one single-expression suite
per grouping item, so the loop is now

    let mut columns = Vec::with_capacity(self.group_by_suites.len());
    for suite in &self.group_by_suites {
        columns.push(suite.eval_chunk(ctx, chunk)?);
    }

and the keys are assembled from the columns.

`EvaluatorSuite::eval_chunk` is the seam these conversions use and the one the
other 25 row-loop sites need: it runs one retained suite over the caller's own chunk,
using `run_with_shared_input` (the evaluation half of `run`, without the
direct-column ownership transfer, which is why the input can stay behind `&`),
then returns one datum per row. It reads a chunk the caller already built, so it
costs no per-call chunk construction and works for an expression that references
only some of the chunk's columns -- unlike `eval_row_values`, whose dense-layout
rule refuses that case. Retaining the suite also retains its immutable
`EvaluatorProgram` and its compiled engine cache across chunks. The free
`evaluator::eval_chunk(expression, ctx, chunk)` is the one-off variant; it
constructs a fresh suite and is only for call sites that do not retain state.

One thing that helper is not allowed to be: an `impl From<EvaluatorError> for
EvalError`. That impl makes `EvaluatorError: Into<EvalError>`, and
`builtin_ext/compare2.rs:500` pins its closure's error type by exactly that
inference, so adding the impl breaks the crate with E0282 (the conversion is
`evaluator::into_eval_error` instead, with the reason in its doc comment).

Evidence that the engine -- not the fallback -- did the work:
`tidb-executor/src/tests_executor_internal_source.rs::vec_group_checker_evaluates_the_grouping_key_in_the_engine`
splits the same four-row chunk (keys `plus(col, 1)`) with a native context and
with `with_tikv_expression(true)`, asserts the same groups and ranges, asserts
the native context recorded zero engine rows, and asserts the engine context
recorded more than zero. `tikv_coverage.rs::eval_chunk_matches_native_row_by_row_and_leaves_the_input_alone`
covers the helper itself: three rows agree with the native answer, the engine
counts three rows, the input chunk is untouched, and a required-engine resolver
gets the structured `ExternalEngine` error for a declined expression.
`retained_chunk_suite_reuses_its_engine_program` runs the same retained suite on
two chunks and asserts four engine rows but exactly one engine compilation.
`shuffle.rs::hash_splitter_evaluates_partition_keys_in_the_engine` runs the
same three-row hash splitter with native and engine contexts, asserts identical
worker assignments, zero native engine rows, and exactly three engine rows.


## What this does not establish

* The inventory is not evidence that a conversion works; the per-site sections
  below are. The counts are textual: a call site that is dead code, or one that
  a later change adds, moves the number. The grep is the source of truth.
* The *aggregate* and *window* cases need the vectorized value to agree with the
  per-row value they replace; the engine's row-vs-vector equivalence is what the
  dual-run corpus tests for constants, and what `tikv_coverage.rs` tests for
  columns, but neither covers these aggregate/window call sites.

## The scalar bridge cannot take a `&dyn Columns` context

Converting the two `access_cost.rs` sites was attempted and reverted, and the
reason is worth knowing before converting more sites: their context is a
`&dyn tidb_expr::Columns` (`resolver.comparison_context()`), and
`EvaluatorSuite::run` needs `C: Sized`. Its native path calls
`Constant::eval_in(ctx)` and `Expression::eval(ctx, row)`, which take
`&dyn Columns`, so the coercion `&C -> &dyn Columns` is only provable when `C`
is sized. Writing `C: Columns + ?Sized` compiles the bound but not the body
(three `CoerceUnsized` errors at those two native calls).

So the two helpers work from a **sized** context -- a concrete `StmtContext`,
which is what the six converted sites use -- and not from a trait object. A
context stored as `&dyn Columns` keeps its row evaluation until either the
helpers grow a `&dyn`-shaped seam or the native entry points stop requiring
`Sized`. The check to do first at any remaining site is therefore: what is the
static type of this context?

### How big is the trait-object obstacle?

Small, which is why it is not worth a wrapper. Of the 90 `.eval(` hits outside
the adapter, exactly two pass a `&dyn tidb_expr::Columns` (the `access_cost.rs`
pair, whose context comes from `resolver.comparison_context()`); every other
site names a concrete type (`ctx`, `&self.ctx`, `context`). Delegating the
`Columns` trait's ~40 methods into a sized wrapper would buy two sites and
would risk silent behaviour changes wherever a delegation was missed -- a
missed `strict_sql_mode` or `handle_truncate` would change the *native*
fallback's answers, which is the one thing the coexistence period must not do.
Those two sites keep their row evaluation, and the check to do first at any
remaining site is the context's static type.

## The `sort.rs` comparator, converted for a reason the plan had wrong

The smallest of the 36 per-row sites is in `tidb-executor/src/sort.rs`: two
calls in one branch of the sort comparison --

    // sort.rs, in the fallback branch of the comparison
    let left = item.expr.eval(ctx, left)?;
    let right = item.expr.eval(ctx, right)?;

The plan was to batch those into `eval_sort_key`, which builds the same keys for
the merge path. Reading the branch instead of the shape shows that plan was
wrong, and the reason matters for the remaining 36: the branch is reached only
when the by-item names a column the row does not carry
(`!(column < left.len() && column < right.len())`), and in that state native
`Column::eval` cannot return a datum at all:

* a negative index is already excluded by the match guard
  (`if column.index >= 0`);
* a missing result type is already excluded, because the arm only matches when
  `compile_compare_funcs` handed back a `Some`, and that function requires
  `get_static_type()`;
* what is left is the third precondition of `Column::eval`
  (`crates/tidb-expr/src/column.rs:224`), which returns
  `EvalError::Unsupported("column index is outside the input row")`.

So both evaluations could only ever turn into that one error, and wrapping them
in the engine would compute nothing. They are replaced by the error itself
(`ExecError::Eval(EvalError::Unsupported("column index is outside the input
row"))`), which is the shape the post-removal code must have anyway; `ctx`
stays in the signature as `_ctx` so no caller changes, and `compare_rows`'s doc
comment records that it no longer reads a context at all.
`an_out_of_range_sort_key_column_is_the_error_the_native_eval_returned` pins
the classification. The equivalence is by precondition analysis, not by a
differential run: writing one would have added a native `.eval(` call to the
tree and inflated the count this file exists to measure. Receipts
(`expression-reuse/round52-final.log`): `cargo test -p tidb-executor` is
1337 + 355 + 6 + 2 with the feature and 1334 + 329 + 6 without -- one more test
per mode than before, the new one -- and `--test all -- tikv_expression`
(the engine-execution receipts for the DDL and pruning sites) is 21 passed.

Two lessons for the rest of the 36: a "per-row site" can be a *defensive* site
whose only outcome is an error, in which case deletion is not a conversion but a
simplification; and the shape of the expression (`Column` only, enforced by
`validate_by_items`) bounds what any wrapping could ever do. `eval_sort_key`'s
own site (one call, `Expression::Column` against a real row) is the remaining
`sort.rs` hit and is *not* of this kind: it returns a value, and `Expression::eval`
also applies the `ENUM_SET_AS_INT` rewrite to a column of that type, so a direct
cell read is not equivalent and the conversion has to go through a compiled
program.

The next candidate is therefore a *production* site that returns a value.
`column_default.rs::evaluate` (one site, line 847) is the best fit: a computed
`DEFAULT` evaluated once per inserted row, with a sized `&impl Columns` context
and a row that comes from the insert's own chunk, which is exactly the shape
`eval_row_values` was built for. The virtual-row probes are the other family:
`driver/agg_build.rs:158` and `driver/dml.rs:1503,1778` evaluate over a
one-row chunk the same way `eval_constant_row` does. The `stmt_context.rs`
probes (`SPACE(2000)`) are *not* candidates -- they are inside `#[cfg(test)]`,
which is why the classifier's test-only split matters here.


