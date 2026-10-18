# N6v0: borrowed typed IN values and shared membership state

This is a local value-layer checkpoint, not IN source closure or global fallback
completion. It removes frontend typed-IN identity encoding/Ready dispatch and
shares the real temporal/JSON membership calculation with the ordinary SDK wire
recipe. Legacy integer/bytes request loops remain unchanged. Three new source
admission regressions remain active, uncommitted WIP for the next source step.

## Contracts and independent evidence

`NativeInTypedValue` borrows already-cast Time/Duration/JSON values, maps other
actual non-NULL kinds to nonmatching `Other`, and reports arity below two.
Neither this iterator nor membership state evaluates expressions. Existing
callers must finish all source evaluation, then all casts, before membership.
Time uses actual native core ordering (packed type/FSP bits are ignored),
duration uses signed nanos and JSON uses the original binary primitive,
including its existing malformed-array behavior. Full wire identity validation
still runs before NULL or match short circuit. Frontend invalid arity retains
exact ExpressionAdapterFailure Scope/Contract and original private reason,
not RuntimeInputContract. No identity packet or worker lookup is needed.

`NativeInMembership` exposes the existing original match/NULL state and prepared
key probe. Its `observe` return is evidence, not authorization to skip source
reads. Generic/prepared callers still compare all required candidates. Prepared
coercion and late effective collation stay inside the original conditional and
the coerced buffer is released before dynamic comparisons. AST, ready-value and
SQL-function demand policies remain distinct.

Before changing either membership implementation, the actual old frame API ran
97,752 cases: 45 raw fixtures squared, four list layouts, four domains, three
cache profiles plus arity0/1. Fixed OLD hash **e128f6d8e515d162** covers results,
raw metadata and contract errors. Then every old frame result was independently
compared to the new borrowed helper before changing the wire body. The same
fixed hash is asserted after deletion; never regenerate it from new code.
Original test-only patch `n6v-original-typed-in-baseline.patch` targets TiDB
0ab81038/SDK6a36ba0 and passes cached apply-check using a separate Git index.
Snapshots n6v-before-{in_list,native_in,native_in_control}.rs and old test source
preserve the original code. Historical scripts/patches are not restart scripts.

Logs in the agent workspace `evidence/no-fallback/`:

- n6v-in-typed-original-witness.log: OLD97,752 pass.
- n6v-in-typed-old-vs-value.log: independent OLD-vs-value97,752 pass.
- n6v-in-typed-worker-red.log: actual old Ready lookup trap fails.
- n6v-in-typed-value-green.log: fixed OLD witness and lookup trap pass.
- n6v-in-value-core-full-fixed.log:1071 pass, exact inherited6 failures,
  one ignored, zero filtered.
- n6v-in-value-clippy.log: successful scoped clippy,89 warnings unchanged.
- n6v-in-value-front-existing.log:1544 pass, exact inherited4 failures,
  95 ignored, exactly3 filtered. Those are ONLY the three new uncommitted
  source-admission regressions; no inherited failure was filtered.
- n6v0-source-still-red.log: separate current-binary rerun, all3 fail ONLY the
  expected route assertion, actual refusal1/fallback1 vs required0/0. SQL values
  match before that assertion. Tests remain active and unignored in WIP.
- n6v0-inherited-failures.txt: machine-matched exact SDK/frontend failure names
  against N6q0/N6u, respectively.

The initial SDK full run exposed a NEW test's incorrect assumption that raw
Time0/1 differ. Actual core ordering masks packed metadata. Only that expectation
was corrected, using1<<32 for an actual differing core; production unchanged.

## Final checkpoint validation

From TiKV root:

```sh
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tikv test --release --locked --offline -j8 -p tidb_query_expr --lib
RUSTFLAGS='' TIKV_FRAME_POINTER=0 make run COMMAND='bash /home/agent/.slock/agents/1555103c-9f3e-46ca-ac2e-b964c650c2f1/evidence/no-fallback/n3-clippy-scoped.sh --lib --tests --release --locked --offline -j8'
```

From TiDB root:

```sh
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib -- --skip evaluator::batch::tests::native_in_generic_abs_operands_require_canonical_driver --skip evaluator::batch::tests::native_in_duration_columns_require_canonical_driver --skip evaluator::batch::tests::native_in_computed_json_requires_canonical_driver
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
make lint
```

Pinned nightly rustfmt checks all nine changed Rust files (including uncommitted
source regression file); both repository diff checks pass. make lint passes
(n6v0-lint.log). Production Projection passes all25 tests (n6v0-projection.log), including the
mixed28-root matrix under both flags and zero/two workers. All jobs are closed. No SQL server/real-TiKV or global
closure claim. SDK datatype unchanged; datatype suite not rerun. No push/PR/
deployment. Goal/task4 remains active.
