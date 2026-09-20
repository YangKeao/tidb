# The native evaluator's call sites outside projections

Milestone E deletes the native evaluator. Projections are routed through
`EvaluatorSuite` + `EvaluatorProgram` (admission/native coexistence remains),
so the remaining question is how
many call sites evaluate an expression *without* a projection, and what each
kind needs. This is the measurement, and the method is repeatable:

    cd rust/crates
    grep -rn "\.eval(" --include=*.rs . | grep -v '/tidb-expr/src/'

## What the raw hits are

After Selection filter routing, the raw grep returns 43 hits.
`rust/scripts/classify-native-eval-sites.py` classifies call arguments from an
eight-line window. Its bounded test-scope scanner reports **15 production /
13 test-only** after the Join/lookup, aggregate and Selection migrations. It
previously reproduced the manual 26 / 13 audit (rather than the old 19 / 20).
Helper/field attributes no longer taint siblings, and inner attributes apply
only to their enclosing file/module. Delimiters inside comments and literals
are masked; uncertain cfg/item syntax stays production-labelled. This is still
a textual, per-line inventory using test-file naming conventions, not macro
expansion or a proof of reachability/deletion readiness.

From the repository root:

    PYTHONDONTWRITEBYTECODE=1 python3 rust/scripts/test_classify_native_eval_sites.py
    PYTHONDONTWRITEBYTECODE=1 python3 rust/scripts/classify-native-eval-sites.py --list

The initial 16-test regression suite failed with 16 subtest failures before
the fix; the final suite passes 18 tests, including additional test-attribute
and conservative unsupported-generic controls. That tooling-only validation
recorded 54 raw / 39 evaluator sites without running Rust/SQL/performance/lint.
The newer executor migrations below reran executor tests; current counts
are 43 raw / 28 evaluator sites.

| Kind | All sites | Production only |
| --- | --- | --- |
| one row of a chunk (`get_row(0)`) | 11 | 4 |
| a row-loop variable or comparator | 12 | 11 |
| a constant with no input row (`Row::empty()`) | 5 | 0 |
| not the evaluator: no-argument `constant.eval()`/`column.eval()`, the planner's `metadata.eval(k)`, the statement predicate's own four-argument `eval(row, catalog, db, ctx)` | 15 | -- |

The textual native evaluator surface outside projections is **28 sites**:
**15 production-scope** and **13 test-only**. Test-only
calls are re-pointed with the corpora rather than converted. This is a textual
inventory, not a proof of reachability. The driver's six-argument
`UpdateExpression::eval` is deliberately
counted: it is a wrapper whose branches call `Expression::eval`
(`driver/dml/correlated.rs:137,140`), so it is a real dispatch site even though
the callee is the same evaluator.

One classifier caveat is now confirmed rather than hypothetical:
`tidb-executor/src/stream_agg.rs` has two textual evaluator calls but is an
unlinked duplicate. `lib.rs` exports the actual `StreamAggExec` and
`GroupedStreamAggExec` from `hash_agg.rs`, and `driver/physical_builder.rs`
imports those types; `cargo test --lib -- --list` contains none of
`stream_agg.rs`'s tests. Excluding these two known dead calls leaves **13**
production-scope sites requiring routing/reachability review (not the earlier
heuristic's 17).
Do not convert the dead duplicate; migrate `hash_agg.rs`'s real aggregate paths
instead.

## Ordering-blocked native calls

Window partition/order key comparisons now retain one `EvaluatorSuite` per key
and evaluate a single-row selection at the original left/right demand points.
A mismatch skips later keys; errors restore the dense buffer before propagation.
Tests assert engine row receipts, one compilation across repeated calls, and
that unselected overflowing rows and skipped keys do not execute. Value
(FIRST/LAST/NTH) and Relative (LEAD/LAG) arguments/defaults now also retain suites
and use single-row selections. Emission-path tests compare native and engine
results, assert engine receipts, and verify absent targets skip arguments,
in-range targets skip defaults, and out-of-partition defaults use the current
row. RANGE now retains separate calculation/comparison suites per bound and
uses the same helper: current-row targets are evaluated in order, candidate rows
advance monotonically, and each comparison stops at the first unequal key.
Tests cover both sort directions, skipped overflowing candidates/keys, and
CURRENT ROW/UNBOUNDED paths that never demand expressions. All seven direct
native calls in `window.rs` are now routed through suites. This is not an
engine-only claim: suite admission and feature-gated native paths remain.

`joiner::eval_bool` now dispatches through `ConditionEvaluator`, preserving the
original condition order, ordinary NULL rejection, and NULL continuation for
IN-rewritten equality. It selects `Row.idx()` physically through the shared
input chunk, never applying `Chunk.sel()` twice. Semi-family joiners retain the
programs and clone them by `Arc`, so compilation is shared; tests assert cache
identity/count and actual engine rows through a semi join and its clone. The
public convenience `eval_bool` wrapper constructs temporary programs: other
hot callers still need retained caches. Existing join scratch-row copies were
not removed; no new copied row is introduced by the evaluator routing.

`JoinExec::matches` and `matches_index_pair` now retain full/residual programs.
Their ordinary matching path rejects NULL immediately rather than adopting
anti-semi's special continuation policy. Scalar index-hash task/worker output
clones share the corresponding program cache. Changing merge keys refreshes
the residual cache; tests cover the changed predicate result, not just cache
identity. `matches_chunk_rows` now routes through the same ordinary-match
program: serial chunk-backed probes and both specialized exact-key/general
parallel workers share the retained residual cache. Real Next-loop tests cover
native/engine contexts, multiple task windows, the actual dispatch selector,
exact engine row counts and one compilation. The helper's NULL short-circuit
still skips an overflowing later condition. Existing scratch-row copies remain;
outer-filter convenience calls are not yet fully cached.

Index-probe bounds now retain one shared program per bound and evaluate selected
physical rows from the existing `OuterBatch` chunks. Invalid keys skip bounds,
NULL bounds skip later terms, and evaluation remains before deduplication.
Tests reorder the demand cursor, leave an overflowing unselected row in a
backing chunk, reuse plans across batches, and recover after a demanded error.
Direct-column bounds must disable column-swap mode: the first regression caught
this adapter construction mistake before correction. There are no direct
`Expression::eval` calls left in `join.rs`; this is not an engine-only claim.

`IndexJoinLookupExec::row_passes_filters` also retains ordinary-match programs,
shared with `LookupForkTemplate` and rebuilt tasks. `set_filters` installs a new
program set without changing previously captured templates. Tests cover local
Next output/engine rows/one compilation, rebuilt-task cache reuse, replacement
isolation and NULL/FALSE/error demand order. Existing physical-row scratch
copying and remote-predicate handling are unchanged. The new rebuild test uses
the same constructor as `open`, but does not open a remote cursor.

Aggregate argument and order-key evaluation now uses retained per-expression
programs in `AggInputMode`, separate from mutable `AggFunc` descriptors and
per-group state. The existing typed dispatch is kept as `AggInputKind`;
plan clones and chunk bindings share the programs. Tests compare engine/native
results and pin multi-argument NULL short-circuit, extras-before-primary order
for AVG/JSON_OBJECTAGG, sort keys after a NULL GROUP_CONCAT argument (current
native order, not a Go-oracle claim), physical row selection, and FIRST_ROW
skipping later errors. Direct typed aggregate kernels remain unchanged and
are not counted as expression-engine executions. Window frame evaluators now
retain these input plans across frames while allocating fresh accumulator
state per frame. Real emission tests pin one compilation, overlapping/empty/
FIRST_ROW frames and delayed overflow demand; recomputation itself is unchanged.

Selection now retains `FilterProgram` across child chunks and no longer owns
separate NULL/string-IN predicate kernels. Row-mode matching uses retained
single-row suites; the batch/VecEvalBool facades route engine requests through
admission, including required-engine contexts without an engine. A regression
first exposed the old batch facade executing native getvar despite requiring
the engine. The native typed vector path remains for non-requesting contexts.
Tests pin engine receipts/cache reuse, physical-mask intersection after
filtering, NULL-from-IN continuation versus ordinary NULL, and side-effect/error
demand. Convenience filter APIs still construct temporary programs; callers
must retain `FilterProgram` for cross-call compilation reuse.

Remaining ordering-sensitive calls are not candidates for an eager whole-chunk
cache. For example, `union_scan.rs` evaluates generated columns into a `MutRow`,
where each write can feed the next expression. It needs an order-preserving
engine interface, not a new copied dense-row workaround. Window/Join demand
invariants and remaining input-copy limitations are also recorded in TiKV's
`EXPRESSION_SEMANTIC_GAPS.md`.

## Production sites by file

Manually audited and now reproduced by the classifier with `--list`. The old
heuristic's ten false test labels were: `access_cost.rs:1928,2209` (`condition_kind`,
`string_match_selectivity`); `access_path.rs:5532`
(`IndexJoinLookupExec::row_passes_filters`); `hash_agg.rs:3617,3626,3630,3647,3675,3694`
(`eval_agg_input`, reached from `hash_agg/input.rs:585`); and `join.rs:1826`
(`matches_chunk_rows`). Test helper/field attributes and a closed test module
had incorrectly tainted those later production scopes. Conversely,
`tidb-planner/src/ranger/go_cases.rs:155,174,208` are test-only under that file's
`#![cfg(test)]`. Line numbers describe the JoinExec-cache audit snapshot.
The remaining 13 actual test-only sites are not listed below.

| Sites | File |
| --- | --- |
| 3 | `tidb-executor/src/driver/dml.rs` |
| 2 | `tidb-executor/src/access_cost.rs` |
| 2 | `tidb-executor/src/driver/dml/correlated.rs` |
| 2 | `tidb-executor/src/stream_agg.rs` (unlinked duplicate) |
| 1 each | `column_default.rs`, `generated_column.rs`, `partition_pruning.rs`, `predicate_pushdown.rs`, `union_scan.rs`, `sort.rs` |


## What each kind needs for the removal

* **A row loop or comparator (11 production-scope).** Where eager
  evaluation preserves observable order, evaluate the expression for the whole
  chunk before the loop and index the result vector. Otherwise use selected
  rows at the original demand points, as the Window key path does. That is now
  one call -- `tidb_expr::evaluator::eval_chunk(expression, ctx, chunk)` -- which
  runs the suite over the caller's chunk and returns one datum per row, so the
  loop does not pick an implementation cell by cell. See the
  `VecGroupChecker` and hash-shuffle conversions below.
* **One chunk row (4 production-scope).** These are probes: partition pruning,
  access-cost estimates, column defaults, generated columns, `dual`, correlated
  subquery inputs. The input is a one-row chunk already, so a one-row engine call
  is sufficient; `eval_chunk` covers it (a chunk with one row), and
  `eval_row_values` covers the variant whose row is a `&[Datum]` instead.
* **A constant with no row (0 production-scope, 5 test-only).** This bucket
  includes the `ranger/go_cases.rs` calls previously mislabelled production.
  Re-point these with the test corpora; `eval_constant_row` supplies the virtual
  one-row input when an engine comparison is needed.

This inventory measures dispatch sites, not kernel coverage. Each migration
must establish admission, representation and ordering compatibility with tests;
a count alone cannot prove that all remaining sites need only a call rewrite.

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

So **12 of the 75** documented sites no longer call the native evaluator -- three
constant-row sites in `ddl/`, four pruning sites in `partition_pruning.rs`, two
error-only sites in `sort.rs`, and three retained-suite grouping routes
(`vec_group_checker.rs`, `shuffle.rs`, and `hash_agg.rs::GroupedStreamAggExec`)
-- which is what takes the raw grep from 90 hits to 76, the site count to 61 and
the production count to 39. One converted pruning site still contains a native
call by design: the helper's `Ok(None)` arm keeps its row evaluation for a sparse
column set, which is why the pruning file went from four hits to one rather than
to zero. The remaining 39 production-path textual sites (37 reachable) still
need the evaluation moved out of their loop rather than wrapped.

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


