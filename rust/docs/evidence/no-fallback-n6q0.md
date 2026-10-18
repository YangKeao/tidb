# N6q0 — quotient row/window protocol preparation

Status: verified protocol preparation; quotient SQL source integration is still
in progress. This checkpoint adds explicit SDK scheduling identities, not new
SQL root coverage. No push or PR is authorized.

Three actual MOD/division/DIV roots over ABS(column0), rows7,MIN, fail the route
assertion before implementation: refusals1/fallback1, after matching native SQL
results and context behavior. `n6q-quotient-root-red.log` retains all three REDs.
The source regressions remain active local work; they are not disabled or
claimed fixed by this protocol step.

The SDK catalog has six distinct binary Bytes-capability identities: row and
whole-window MOD, division and integer DIV. Window identities select the
existing canonical vector callback protocol. Their row children use the same
SDK frame with original occurrence provenance; no nested evaluator or RPN
argument program is introduced. All six retain exact arity/carrier validation
and genuine operation identity. The frontend's exhaustive enum consumer covers
the new variants, while its source factory still admits only the previously
implemented arithmetic operations. Invalid/unconstructed descriptors do not
skip existing name, catalog or ownership checks.

Two protocol tests cover the six identities, arity0/1/3 refusal, wrong second
argument/output carrier, and three paired window/row programs. Each pair uses
2050 reordered/duplicate occurrences with Fresh0,Fresh1,Reuse0,Fresh0,Reuse1,
checks both whole child passes and the parent's whole RHS pass, zero-budget
refusal before reads, original primary input-error identity, cancellation,
clean later reuse and empty selections. The fixture is an explicit byte relay,
not a SQL math implementation. Actual quotient value kernels were verified in
N6o/N6p; source demand and SQL diagnostics still require their next integration.

Evidence in agent workspace evidence/no-fallback/:

- n6q-quotient-protocol-core-full.log —1068 pass, exactly the inherited six
  failures/one ignored; both new protocol tests pass. Failure names match N6o.
- n6q-quotient-protocol-front-existing.log —1515 pass, four inherited failures,
  95 ignored, three filtered out. This is an existing-consumer compatibility
  gate only: exactly the three known, newly added admission RED tests were
  excluded because their implementation is the next step. It does not prove
  quotient admission, and no fixed-root claim or completion follows from it.
- n6q-quotient-protocol-clippy.log — passes with89 inherited warnings.
- n6q-quotient-protocol-lint.log — make lint passes. Both pinned nightly
  format checks and both git diff checks pass; all jobs are closed.
- n6q-quotient-root-still-red.log — reruns the three exact pending root tests
  after the protocol bridge; all still fail only the same route assertions.
  These regressions stay uncommitted local source-integration work, without
  ignore attributes, and are not included in this protocol-only commit.

Commands from the corresponding repository roots:

```sh
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tikv test --release --locked --offline -j8 -p tidb_query_expr --lib
RUSTFLAGS='' TIKV_FRAME_POINTER=0 make run COMMAND='bash <agent-workspace>/evidence/no-fallback/n3-clippy-scoped.sh --lib --tests --release --locked --offline -j8'
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib -- --skip evaluator::batch::tests::native_modulo_abs_requires_canonical_driver --skip evaluator::batch::tests::native_division_abs_requires_canonical_driver --skip evaluator::batch::tests::native_integer_division_abs_requires_canonical_driver
make lint
```

No frontend SQL scheduling/value behavior changed in this protocol-only step;
Production Projection and datatype were not rerun. Later source integration
must fix and run all three admission REDs, prove authentic row/whole operand
demand, conditional Decimal DIV precision and exact diagnostic contents, then
run the affected production Projection gates. The global goal remains active.
