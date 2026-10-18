# N6i: native-value batch publication follows the actual candidate owner

This is a local, signed-off migration checkpoint. Global alternate-evaluator
retirement is still incomplete. TiKV production code remains at `cff06c7`.

## Evidence and behavior

The new native comparison for ABS(real_column * 2) over a successful row
followed by f64::MAX reproduced a publication mismatch: unified output retained
one successful row while native batch output remained empty
(`n6i-publication-red.log`). RootValue versus BulkRoot and control-function
spelling do not describe the publication contract. Operand demand and final
output publication are independent facts.

Native-value outputs now stage when the actual static native candidate is
eligible and the compiled program is not already a complete-selection program.
The original per-output global getter selects whether this batch publication
rule applies. The same canonical driver still performs all expression work.
Pending results retain N6h's checked capacity/payload ownership and amortized
allocation; SQL, binding and resource errors remain terminal. A preexisting
output prefix is preserved, and global row mode keeps its native successful
prefix after a later error.

The first generalized predicate used batch_supported alone. A source audit
showed this was weaker than native candidate eligibility: non-deferred
constants and columns are not candidates, although their types are supported.
The functional2050-occurrence,32KiB regression reproduced the resulting
unnecessary storage refusal (`n6i-publication-leaf-red.log`).

The final implementation shares `static_batch_target` with the native
candidate's static scalar-function arm. It checks the actual root class,
static return type and supported closure. The unified compiler consumes this
pure fact only after full source preflight. The deferred-root native arm and
its context-dependent type-before-value observation are unchanged; this does
not claim unified deferred-source coverage. Constants/columns retain their
native row/broadcast publication owner and do not acquire full-batch storage.

## Changes

`rust/crates/tidb-expr/src/scalar_function.rs` owns the shared static candidate
fact and uses it in the original native candidate arm.
`rust/crates/tidb-expr/src/evaluator/batch.rs` derives native-value staging from
that fact, output role and the checked whole-selection contract.
`batch_tests.rs` adds the original RED,112 root comparisons, four seeded-output
comparisons and two low-budget non-call root comparisons. Four extra rewritten
SQL forms add48 native comparisons. The existing production unary Projection
matrix now mixes five roots, adding Real/Decimal ABS over real column sources
in serial/two-worker execution with both global flag states.

An initial test fixture used the unsupported spelling cast_real; it was
corrected to the existing cast_double operator before final comparisons. No
admission or resource guard was relaxed.

## Validation

Evidence is under the agent workspace `evidence/no-fallback/`.

- The complete frontend:1489 passed / exact four inherited failures /95ignored,
  including all130 batch tests (`n6i-publication-front-final.log`). Failure names
  are matched exactly against N6h; the full suite is not green.
- The four new tests include122 actual root/native comparisons in total:
  four for the original error case,112 Real/Decimal ABS and CAST cases, four
  existing-output/row-prefix cases and two non-call low-budget cases. Selections
  include empty, repeated and2050-occurrence maps, plus a late error at2049.
- Rewritten SQL adds48 actual native comparisons. Flags switch true/false on
  a reused prepared owner. Results, raw output cells, errors and context traces
  are compared, with zero alternate routes/refusals.
- `make lint` passed (`n6i-publication-lint-final.log`).

Final production Projection:25 passed, including the extended five-root
serial/two-worker and global-flag matrix
(`n6i-publication-projection-final.log`). Pinned format checks for all four
changed Rust files and `git diff --check` passed. All build/test handles were
terminal before signing.
SDK code is unchanged from N6h, whose1058 passes / exact six inherited failures /
1ignored,14 window tests and scoped clippy89warnings remain the applicable SDK
baseline; those unchanged-code gates were not repeated for this frontend step.

## Reproduction

From TiDB root:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
    make lint

Format the four changed Rust files using the pinned nightly2026-08-22 rustfmt,
with edition2021 and skip_children=true. Run `git diff --check`.

## Remaining scope

Typed output roots and ordinary operand phases still require audit. Candidate
eligibility alone does not prove that a typed eager graph has native row demand,
and output staging cannot fix a phase order changed by tiling. In particular,
compare controls with ABS descendants even when there is no PLUS/MINUS/MUL,
and ordinary typed/host roots such as CONCAT. Deferred/type-before-value
sources, other function families/callers, effectful multi-output ordering and
global alternate-driver retirement remain open.
