# N6f: native arithmetic roots and Decimal priority

Closed plus/minus/multiply roots now execute through the canonical SDK frame
driver with genuine native row or bulk demand. This step does not retire the
global alternate evaluator. Integer ABS and Decimal/JSON CAST bulk operand
phases, standalone CAST priority, general control wrapping, dynamic sources,
ordered effectful projections and the remaining callers/functions are still open.

The source compiler checks complete tree/resource bounds before the recursive
native eligibility predicate. Per-edge views retain the original typed source
owner, including literal-once, column normalization and computed Int rules.
A genuinely bulk-eligible but incompletely covered operand family does not get
silently substituted with the row recipe. Immutable row/bulk programs share
the actual source owner/root and a combined checked node/metadata budget. The
native getter selects row or bulk at its original per-output point. Disabling
vectorization does not latch later bulk calls to a one-row tuning width;
whole-selection programs cannot be root-tiled by the tuning API.

Native Decimal priority occurs before that getter, including when its eventual
value is false. A separate immutable factory builds this semantic stage as
another program of the **same SDK frame driver**, not a second evaluator.
Inputs contain only strict literal outcomes, raw column facts or the exact
integer-column CAST shape; the latter shares a plain shape/value helper with
its original owner. Constant conversion precedes broadcasting even for empty
priority input. Its immutable empty-supported evidence is separate from the
generic EligibleBatch::eval_native empty guard, which skips all value demand
after the original getter/admission point.

The window provider requests complete left, then complete right, then computes
using the datatype crate's public checked-coefficient kernel. A typed left
Unsupported prevents a right request. All returned capabilities are validated
before any semantic Unsupported result is acted on; late wrong purposes cannot
be hidden by an earlier representation refusal. Final MyDecimal conversion
must succeed for the entire selection before output publication. Only the
original typed Unsupported/final-representation refusal selects the subsequent
canonical bulk/row phase. Any SDK, binding, resource or SQL error is terminal.
Successful priority skips the getter and generic return coercion. Runtime code
contains no Expr.eval callbacks, fake ready-child expressions, or old
EvaluatedArgs programs. The legacy fast adapter appears only as a test reference.

The SDK implementation is unchanged from 7ac5b37. Its maintenance guide is
updated in paired local guide-only commit93b9705 for the frontend caller/ownership contracts. Limits remain per SDK
invocation and separately metered frontend logical retention; this is not a
claim of a combined hard statement allocator-peak bound.

Validation from the TiDB root:

```sh
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
make lint
```

- Final frontend: **1478 passed**, exact same four inherited failures,
  95 ignored. Exact failure-name comparison against N6f0 passed. This is not
  a completely green suite. The frontend run includes all119 batch tests.
- **673** actual native root comparisons:325 base/profile/tuning cases,
  168 wide/nested/literal Decimal cases,144 rewritten SQL cases and36 vector
  row-domain cases. Checks include exact results/errors, raw output bytes,
  warning/context traces, true/false/true/false toggles, empty/reordered/repeated
  selections,2050-row phase order and no fallback/refusal for covered roots.
- **150** fast-source integer-column CAST stage cases compare the original
  native priority function's disposition and raw output; this validates that
  stage without claiming its generic CAST continuation is promoted.
- **99** actual fast window cases compare shared native coefficient results
  and whole-operand demands, with144 terminal resource invocations and72
  wrong-purpose invocations. NoContextCalls rejects context/Expr use.
- **23 Projection tests passed**. The new mixed-root test executes fast
  Decimal, integer and wide Decimal outputs together for3 operations, both
  vector flags and serial/two-worker execution (12 integration runs).
- The nine-node aggregate phase budget rejects8 and admits9; a fast SDK
  resource failure leaves output/context untouched and cannot query the flag
  or enter generic execution. Reusing the cleaned program then succeeds.
- Pinned frontend rustfmt, `make lint`, and both worktrees' `git diff --check`
  passed. SDK code/clippy suites were not rerun for this frontend-code/doc-only
  change; their N6e evidence remains the unchanged SDK baseline.

Red evidence retained: the initial source tests exposed Decimal-before-flag
ordering; the fast provider test caught a live RefCell borrow at publication;
a later wrong-purpose regression reproduced early Unsupported masking invalid
tail provenance before complete-vector validation. The old once-per-suite
mode-counter assertion was replaced with a direct native calculated-suite
counter comparison. The new Projection fixture initially requested3 rows from
TableDual, whose valid domain is0/1; the final fixture uses existing
NumberSource(3,3). No runtime guards were relaxed for any of these fixes.

Workspace evidence: `evidence/no-fallback/n6f-root-front-final.log`,
`n6f-root-front-full.log`, `n6f-root-projection-fixed.log`,
`n6f-root-lint.log`, `n6f-fast-batch-final.log`,
`n6f-fast-source-boundaries.log`, `n6f-fast-window-resource.log`,
`n6f-fast-window-capability-red.log`, `n6f-fast-all-batch.log` and the
initial/fixed source/provider logs. The unrelated untracked third-party
BUILD.bazel is excluded. Commits are local with Signed-off-by; no push or PR.
