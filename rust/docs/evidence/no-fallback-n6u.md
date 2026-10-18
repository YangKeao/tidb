# N6u — direct control selection and native ownership

Date:2026-10-18. Parent TiDB734d3745 / TiKV7e3b147; SDK production
remains6a36ba0. Local-only checkpoint; global no-fallback goal remains active.

## Change

IF/CASE/IFNULL/COALESCE/NULLIF frontend wrappers now use genuine shared
native_if_choose_branch/native_if_null_choose_first selectors and actual native
values. Removed their identity-frame encode/decode, head/finish reports,
secondary Ready programs, one-shot selected-context binding and CASE OnceCell
protocol. Original callback/context properties, conversion and warning sites,
selected-only demand and eager NULLIF comparison stay intact. Empty CASE still
initializes its base once; empty/all-NULL COALESCE and exhausted CASE use the
same public impl_compare::coalesce_bytes(&[]) policy as the old no-arg recipe.
There is no invented NULL operand or report buffer.

Selected owned values move after a logical retained-payload check. Borrowed
selected values admit source-plus-copy payload before clone. Both use the
existing finite default64MiB execution allowance. Payload means actual
coefficient/raw bytes/vector elements, not Decimal visible padding or obsolete
identity-frame buffers. Fixed-width raw metadata stays unchanged. These are
conservative logical ownership allowances, not whole-statement allocator-peak
bounds or recovery from every allocation failure. Observed source/resource
errors remain terminal and preserve their original capture; no other engine
is retried. Canonical source/store budgets remain unchanged.

Invalid NULLIF comparison output still accepts only actual NULL/Int0/Int1 and
retains its exact private ExpressionAdapterFailure Scope/Contract kind/reason.
Old identity codecs and explicit SDK recipes still used by other families were
not deleted or redirected. SDK production/datatype code did not change.

## Independent OLD evidence before deletion

Workspace evidence/no-fallback/:

- n6u-control-original-witness-all.log:7680 actual OLD result/representation/
  source-demand/config/warning comparisons, plus78 source-failure cases pass.
  Forty native Datum fixtures include full Float32 f64 bits, nonfinite vectors,
  opaque names/collation, raw duration/FSP, huge/invalid Decimal metadata and
  declared shapes. This is an identity/source proof, not an assertion that
  every malformed raw value is a valid SQL literal.
- Control profiles include all five families, no-cache/fresh/reused caches,
  zero-pair CASE/base initialization and ELSE/exhaustion, empty/all-NULL
  COALESCE,2050-NULL prefixes and eager NULLIF comparisons.
- The78 injected SQL/resource source failures compare actual opaque capture
  identity, exact prior warning/config/source prefix, no replay, and successful
  later control reuse after each failure.
- OLD fixed traces8addb0a0f0899f79 (7680 value/demand cases) and
  4576b55725e08f88 (78 failure cases) are now asserted. Never regenerate them
  from new wrappers. n6u-original-control-baseline.patch is test-only against
  TiDB734d3745/SDK6a36ba0 and reproduces the original_control filters.
- n6u-control-original-private-cause.log confirms the exact original private
  NULLIF comparator Scope/Contract kind/reason for six invalid outputs.
- n6u-control-old-worker-route-red.log:all five real worker/cache lookup traps
  fail against old wrappers; the same tests pass after direct migration.
- n6u-before-{if_control,case_control,if_null,coalesce,null_if}.rs preserve the
  original production wrappers. Historical artifacts are not restart scripts.

The initial fixture compilation exposed only Rust test-module visibility; it
was corrected to reuse the existing32-case fixture helper within cfg(test).
No production behavior was changed before the independent OLD proof.

## Final validation

n6u-control-front-full.log passes1540 with exactly inherited4/95 ignored after
switching. The final resource cohort n6u-control-front-full-resource.log passes
1541, exactly inherited4/95 ignored, zero filtered. All159 batch tests, five
worker-route regressions, both new fixed control traces and previous independent
predicate/comparison/arithmetic witnesses pass. Selected-owner budget tests
cover all40 raw representations, finite allowance rejection, actual payload
refusal, zero-work fixed values, and million-visible-scale Decimal movement/
copy with only one retained coefficient byte and no formatting/frame work.

n6u-control-projection.log:all25 production Projection tests pass, including
mixed28-root output under both flags and serial/two workers.
n6u-control-lint.log:make lint passes. Pinned nightly formatting for all ten
changed Rust files and both repository diff checks pass. All jobs are closed.
n6u-control-inherited-failures.txt machine-compares exact final frontend failure
names to N6t; no inherited failure was ignored, filtered or reclassified.
SDK full/clippy/datatype were not rerun because source is unchanged; N6q0's
1068 pass/exact inherited six failures/one ignored and89 clippy warnings remain
the recorded SDK baseline.

Commands from TiDB root:

```sh
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib original_control -- --nocapture
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
make lint
```

Other adapters and builtin/source/composition/caller closure, global alternate
dispatch retirement and inherited-failure/final SQL/server gates remain.
Goal/task4 is not complete. No push/PR/deployment is authorized.
