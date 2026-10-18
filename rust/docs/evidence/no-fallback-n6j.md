# N6j: closed control roots keep their native row demand

This local signed-off checkpoint continues the no-fallback migration. It does
not retire alternate execution globally. TiKV code remains `cff06c7`.

## Regression and change

Before the fix, EQ(ABS(column0),ABS(column1)) on rows(1,MIN_INT) and(MIN_INT,1)
returned an SDK evaluation failure from the unified route. The actual native
owner instead returned DataOutOfRange(BIGINT,abs(t.c1)): row0's right operand is
demanded before row1's left operand. The regression ran before the production
change (`n6j-controls-red.log`).

The source compiler previously promoted control/comparison roots only when
PLUS/MINUS/MUL occurred somewhere below them. Standard-carrier ABS descendants
therefore remained in an eager typed graph despite the native row contract.
That descendant-name predicate is now removed. Every native-value-closed
control/comparison root selects its genuine row program through the same SDK
frame driver. Source closure/type/arity/budget checks remain in place; no Expr
callback, fake ready-child expression or alternate value evaluator is added.
The explicit old strict test-consumer compiler policy remains unchanged.

Native candidate eligibility and publication are still independent. N6i's
shared static candidate fact selects full-batch final-output staging where
native requires it; global row mode retains its native successful prefix.
This change corrects demanded operand order and original SQL errors before
any global fallback deletion.

## Validation

Evidence files are in workspace `evidence/no-fallback/`.

- The original native comparison now passes in both flag states on a reused
  prepared owner. A23-root ×7-selection ×4-flag matrix adds644 comparisons;
  with the original four,648 new native comparisons pass. Families include
  IF/IFNULL/COALESCE/CASE, AND/OR, nine boolean predicates, six comparisons,
  NULL-safe equality and NULLIF. Selections include empty, repeated,2050 safe
  occurrences and a late error at2049. Result/error objects, context traces,
  output NULLs/raw cells and absence of alternate routes are compared.
- Eight new actual rewritten SQL forms add96 native comparisons, including
  comparison, nullable selection, IF/CASE, AND/OR and IS NULL over ABS.
- Complete frontend:1491 passed / exact four inherited failures /95ignored,
  including all132 batch tests (`n6j-controls-front-full.log`). Failure names
  match N6i exactly. The full suite remains non-green.
- Production Projection25 passed. Its control matrix now mixes seven roots,
  adding ABS comparison, COALESCE and AND over actual columns; serial and
  two-worker execution match native with both flag states
  (`n6j-controls-projection.log`).
- `make lint`, all three changed Rust files' pinned format checks and
  `git diff --check` passed (`n6j-controls-lint.log`). All jobs are terminal.

SDK source/ABI did not change, so its existing N6h1058-pass/exact-six-failure/
1ignored baseline,14 window protocol tests and scoped clippy89warnings were
not repeated for this source-domain change.

## Reproduction

From TiDB root:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
    make lint

Use the pinned nightly2026-08-22 rustfmt, edition2021 and skip_children=true,
for batch.rs, batch_tests.rs and projection.rs; run `git diff --check`.

## Remaining scope

Typed output roots such as CONCAT still require a publication audit. Ordinary
typed operand phases, source/value bridges, deferred/parameter type-before-value
staging, other function families/callers, effectful multi-output ordering and
global alternate-driver retirement remain open. Output staging alone cannot
repair eager operand order or justify root tiling of a whole-selection program.
