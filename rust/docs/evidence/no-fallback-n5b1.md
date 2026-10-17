# N5b1: direct native columns need no computation wire conversion

TiDB parent f817cd99 plus this commit; TiKV unchanged49ead6d. Local signed
checkpoint, no push. Global fallback elimination remains active.

PreparedBatchSuite previously required check_type for direct columns, although
these outputs only transfer native Chunk storage after calculations succeed.
That rejected Float32/YEAR/BIT/ENUM/SET and unnecessarily routed the whole suite
to another evaluator. Remove only this computation-carrier admission check.
Bounds, complete schema metadata budget/snapshot/comparison, invocation owner,
selection, physical column length/layout and destination layout checks remain.
Calculated outputs retain their own full source closure and carrier checks.

One regression test covers six cases (Float32 negative zero, Double NaN payload,
YEAR zero, BIT, ENUM and SET), each with NULL and duplicate output aliases,
vector optimization enabled/disabled, unselected/empty/2050 selected occurrences.
Raw storage and NULL bits match native physical-row publication. Unselected
outputs retain shared alias identity; selected inputs retain their owners.
All36 scenarios prepare without fallback/refusal. Before the fix the test
failed at Float admission with route(0,1,1), establishing the coverage gap.

The first selected-reference fixture called the old direct-swap evaluator with
a selection; it correctly rejected that unsupported invocation. The final
reference publishes physical rows and explicitly indexes each requested
occurrence, keeping exact raw comparisons rather than relying on that route.

74 focused frontend tests pass. Full frontend1426 pass with exactly the same
4 inherited failures and95 ignored, compared by name against N5a2. make lint,
scoped rustfmt and git diff --check pass. Core and executor implementation are
unchanged; their N5a2 checks remain applicable. No new allocation or value kernel
is introduced. This does not expand calculated wire types.

Reproduce with pinned ../tools/cargo-tidb from TiDB root: RUSTFLAGS='',
CARGO_BUILD_JOBS=8, test --manifest-path rust/Cargo.toml --release --locked
--offline -j8 -p tidb-expr --lib evaluator::batch, then the fresh test binary
for the complete suite; make lint. Raw workspace evidence/no-fallback logs:
n5b-direct-before.log, n5b-direct-focused.log (selected reference fixture),
n5b-direct-focused-final.log, n5b-direct-full.log, n5b-direct-lint.log.

Next work is native root outputs and computed values, remaining kernel/context
coverage, effects and other callers before deleting alternate dispatch.
