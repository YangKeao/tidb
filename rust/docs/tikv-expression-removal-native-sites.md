# The native evaluator's call sites outside projections

Milestone E deletes the native evaluator. Projections are already served by the
engine (`EvaluatorSuite` + `EvaluatorProgram`), so the remaining question is how
many call sites evaluate an expression *without* a projection, and what each
kind needs. This is the measurement, and the method is repeatable:

    cd rust/crates
    grep -rn "\.eval(" --include=*.rs . | grep -v '/tidb-expr/src/'

## What the 90 raw hits are

| Kind | Count |
| --- | --- |
| one row of a chunk (`eval(ctx, chunk.get_row(n))`) | 31 |
| a row-loop variable or comparator (`eval(ctx, row)`) | 36 |
| a constant with no input row (`Row::empty()`) | 8 |
| not the evaluator: `constant.eval()` (folding helper) | 11 |
| not the evaluator: the statement predicate's own `eval(row, catalog, db, ctx)` | 2 |
| not the evaluator: the planner's test-only `metadata.eval(k)` | 2 |

So the native evaluator surface outside projections is **75 sites**, and every
one of them is row-at-a-time: `Expression::eval` against a single `Row`.

## By file

| Sites | File |
| --- | --- |
| 10 | `tidb-executor/src/hash_agg.rs` |
| 7 | `tidb-executor/src/window.rs` |
| 6 | `tidb-executor/src/driver/dml.rs` |
| 4 | `tidb-executor/src/join.rs` |
| 4 | `tidb-executor/src/partition_pruning.rs` |
| 3 | `tidb-executor/src/driver/dml/correlated.rs` |
| 3 | `tidb-executor/src/sort.rs` |
| 3 | `tidb-planner/src/ranger/go_cases.rs` |
| 3 | `tidb-planner/src/ranger/points.rs` |
| 2 each | `access_cost.rs`, `ddl/table_partition_list.rs`, `driver/multi_dml.rs`, `stream_agg.rs`, `stmt_context.rs`, `logical/rule_predicate_simplification.rs`, `tidb-expr/tests/info_metadata_source.rs` |
| 1 each | `access_path.rs`, `column_default.rs`, and the remaining files |

## What each kind needs for the removal

* **A row loop or comparator (36).** The surrounding loop already holds a chunk,
  so the engine's own strength applies: evaluate the expression for the whole
  chunk *before* the loop and index the result vector. This is the same shape
  `EvaluatorSuite` already implements for projections, so the work is moving the
  call out of the loop, not new engine capability.
* **One chunk row (31).** These are probes: partition pruning, access-cost
  estimates, column defaults, generated columns, `dual`, correlated subquery
  inputs. The input is a one-row chunk already, so a one-row engine call is
  sufficient; the corpus harness (`tests/mod.rs::engine_case`) is a working
  example of exactly that shape, including the empty-input case.
* **A constant with no row (8).** `physical_builder` and `agg_build` evaluate a
  constant expression with `Row::empty()`. The engine handles this with a
  virtual one-row chunk (the corpus does it with `set_num_virtual_rows(1)`).

None of the 75 needs a new kernel; they need the call to move.

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

Evidence: `range_pruning_evaluates_through_the_engine` runs the same pruning as
`range_pruning_evaluates_go_supported_partition_functions` under
`with_tikv_expression(true)`, asserts the same pruned ids (`Some(vec![102])`)
and asserts `tikv_expression_rows() > 0`. All 21 pruning tests stay green, and
the executor's 1333/1334 lib tests too.

`ddl/table_partition_range.rs` has a third constant-row site of the same shape
and is converted too, verified by the same 11 partition-DDL tests.

So **7 of the 75** are converted -- three constant-row sites in `ddl/` and four
pruning sites in `partition_pruning.rs`. The remaining 68 are still textually
unconverted, and the per-row kinds still need the evaluation moved out of their
loop rather than wrapped.

## What this does not establish

* No site has been converted. This is an inventory with a reproducible method,
  not evidence that a conversion works.
* The counts are textual: a call site that is dead code, or one that a later
  change adds, moves the number. The grep is the source of truth.
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

