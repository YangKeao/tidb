# N6r — retire remaining additive argument-program adapters

Date:2026-10-18. Parent:TiDB223553c1 / SDK source6a36ba0
(TiKV head a705401 differs only in documentation). Local-only checkpoint;
global no-fallback migration remains active.

## Change

Generic and typed-integer ADD/SUBTRACT/MULTIPLY callers now invoke the actual
prepared native values' compute entry, already used by the canonical provider.
Removed PreparedArithmetic::into_evaluated, prepare_binary_arithmetic and
prepare_integer_arithmetic, both unsigned/null Cell side channels, and the
extra argument-program/result unpacking. Integer preparation still owns the
original subtraction-mode read and signedness. Inputs retain original domain
promotion, string warnings and NULL rules; full f64 payloads and native Decimal
and vector representations retain their existing kernels and checks. All calls
supply the existing finite default execution allowance. Quotients' genuine
zero-divisor warning handling stays in their own value path.

This deletes the temporary additive caller adapters. Explicit SDK recipes and
other frontend ready-value consumers remain compatibility surfaces for their
own callers; they were not silently redirected or deleted in this step.
Global fallback dispatch and remaining function families still need migration.
SDK production/datatype code did not change.

## Independent deletion-before-switch proof

The original generic caller still used its registered argument program when
73,728 generic cases were compared with the direct entry:32x32 Datum fixtures,
three operators, four unsigned-field pairs, two unsigned-subtraction modes and
Warn/Error/Ignore truncation policies. Exact values, errors, actual context
reads/warnings, f64 bits, Decimal coefficient/visible/storage scales and vector
f32 element bits matched. Another1176 typed-integer cases compared the actual
old typed caller against direct preparation across signedness, mode and
integer boundaries. Both runs passed before production mutation.

Fixed FNV-1a traces of the actual OLD results/context observations:

- generic:7246d6c6e2263fe1 (73,728 cases);
- typed integer:6e729cf695ab6ca5 (1176 cases).

Tests now assert those constants. They must never be regenerated from the new
implementation. Workspace evidence/no-fallback/ preserves:

- n6r-additive-original-witness.log:initial25,752 comparison run (warning-only
  generic trace, before expanding modes; not the final generic constant).
- n6r-additive-original-witness-modes.log:final74,904 OLD comparisons and
  constants, all three tests pass.
- n6r-additive-before-retirement.rs:original complete arithmetic implementation.
- n6r-original-additive-baseline.patch:test-only patch against TiDB223553c1,
  retaining genuine old production adapters. Apply in an isolated checkout
  with SDK source6a36ba0, then run ops::arithmetic::tests with --nocapture to
  reproduce the old witnesses. It is historical evidence, not a restart script
  for the current source tree.

## Validation

Workspace logs n6r-additive-front-full.log, n6r-additive-projection.log and
n6r-additive-lint.log record the final gates:

- Complete frontend1524 pass, exactly the four inherited failures/95 ignored,
  zero filtered; all159 batch checks, both new fixed additive witnesses and
  existing fixed quotient witnesses pass. The inherited failure list was
  machine-compared against N6q1 (n6r-additive-inherited-failures.txt).
- Production Projection25 pass, retaining the mixed28-output arithmetic,
  comparison/control/cast matrix under both flags and serial/two workers.
- make lint, pinned nightly rustfmt for all three changed Rust files and both
  git diff checks pass. All jobs are closed.

Existing additive resource tests continue to require
terminal finite-budget failures and a genuine NULL with no invented argument
or subtraction-mode read. SDK full/clippy and datatype are not rerun because
production code is unchanged; N6q0's1068 pass/same6/one ignored and89 clippy
warnings remain the recorded SDK baseline.

From TiDB root:

```sh
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib ops::arithmetic::tests -- --nocapture
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
make lint
```
