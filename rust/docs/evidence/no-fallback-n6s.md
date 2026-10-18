# N6s — retire comparison argument-program adapters

Date:2026-10-18. Parent TiDB7e76f620 / TiKV54dbe2f; SDK production
remains6a36ba0. Local-only incremental checkpoint, not global completion.

## Change

Generic, typed integer/Decimal, actual NULL and NULL-safe comparison callers
now invoke the same genuine PreparedComparison/PreparedNullSafeComparison
compute methods as canonical providers. Removed comparison_into_evaluated,
comparison_null_args, prepare_comparison_int/decimal/real and prepare_comparison,
including their temporary argument programs and result unpacking. NULL-safe
presence uses the existing actual presence kernel; duration-column/constant
text still normalizes its computed equality with the existing IsTrue value
kernel, whereas original datetime conversion NULL stays NULL. No expression
evaluation callback or invented ready child was introduced.

The original promotion, collation, typed source facts, literal/column policy,
error/warning and getter ownership remains in comparison preparation. Invalid
ordinary operators are rejected before Decimal bridging or context access.
Actual predicates publish directly; direct LocalError failures retain their
outer resource/evaluation class and actual cause without an invented worker
phase. All callers supply the existing finite default allowance. SDK
production code and datatype did not change. Explicit legacy recipes and other
frontend ready-value families remain for their own consumers.

## Independent OLD proof before switch

n6s-comparison-original-witness.log passes all five tests while the original
production callers still use their argument programs. It compares actual OLD
results with direct values and detached facts:

-157,464 generic:27x27 value/type fixtures, six predicates, three collations,
  four constant/column profiles and three truncation policies;
-26,244 NULL-safe:the same fixture/collation/profile/policy matrix, including
  duration text and datetime conversion NULL, sentinel-before-NULL priority;
-1566 typed:1176 signed/unsigned integer boundary pairs,384 Decimal pairs
  (coefficient/scale/large magnitude boundaries) and six actual-NULL calls.

The contexts record actual truncate-level/precision reads and warnings. Old
fixed FNV-1a result/context traces are now asserted in tests:

- ordinary4e3678354b488fcd;
- NULL-safe93bb0d8b5b45573b;
- typed3d32f94b793fd8a9.

Never regenerate these constants from the new implementation. Agent workspace
evidence/no-fallback/n6s-original-comparison-baseline.patch is a test-only patch
against TiDB7e76f620 with SDK6a36ba0. Apply in an isolated checkout and run
ops::comparison::tests with --nocapture to reproduce original results. The
original full comparison source is also saved as
n6s-comparison-before-retirement.rs. These are historical evidence, not restart
scripts for the current tree.

## Verification

New direct-boundary checks cover all six predicates' actual Decimal comparison
success under zero work allowance (Grow Ord borrows existing words), genuine
SDK errors from deliberately corrupted JSON transport and unchanged outer
Evaluation classification, vector terminal ResourceLimit refusal, actual NULL
under zero work allowance, and invalid operator-before-invalid-Decimal/context
precedence. The initial full run's only new failure was a test assumption that
Decimal comparison allocates an arithmetic buffer. SDK source confirms no
scratch allocation; the test was corrected, with no production budget changes.
The initial log n6s-comparison-front-full.log is retained as evidence. Existing finite allowance,
NULL-safe temporal/presence and fixed additive/quotient witnesses remain active.
Final gates:

- n6s-comparison-front-full-fixed.log:1528 pass, exactly the four inherited
  failures/95 ignored, zero filtered. All159 batch checks, three new fixed
  comparison witnesses and existing fixed additive/quotient witnesses pass.
  n6s-comparison-inherited-failures.txt machine-compares exact names to N6r.
- n6s-comparison-projection.log:all25 production Projection tests pass,
  including the mixed28-root matrix under both flags and serial/two workers.
  Production code was final at this run; only the test assumption described
  above was subsequently corrected, so Projection was not repeated needlessly.
- n6s-comparison-lint.log:make lint passes. Pinned nightly formatting for both
  changed Rust files and both repository diff checks pass. All jobs closed.

Commands from TiDB root:

```sh
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib ops::comparison::tests -- --nocapture
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
make lint
```

SDK full/clippy/datatype were not rerun:source is unchanged and N6q0's1068 pass,
exact inherited six failures/one ignored and89 clippy warnings remain the SDK
baseline. Other adapters, builtin/source/composition/caller coverage, global
alternate dispatch retirement and inherited-failure/final SQL/server gates
remain outstanding. Goal/task4 stays active.
