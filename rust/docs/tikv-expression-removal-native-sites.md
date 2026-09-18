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

## What this does not establish

* No site has been converted. This is an inventory with a reproducible method,
  not evidence that a conversion works.
* The counts are textual: a call site that is dead code, or one that a later
  change adds, moves the number. The grep is the source of truth.
* The *aggregate* and *window* cases need the vectorized value to agree with the
  per-row value they replace; the engine's row-vs-vector equivalence is what the
  dual-run corpus tests for constants, and what `tikv_coverage.rs` tests for
  columns, but neither covers these aggregate/window call sites.
