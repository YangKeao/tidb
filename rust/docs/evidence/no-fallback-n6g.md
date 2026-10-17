# N6g: whole-child ABS and CAST, including standalone Decimal priority

Three explicit unary-window operations join the existing canonical frame:
AbsIntWindowNative, CastDecimalWindowNative and CastJsonWindowNative. Each
requires exactly one Bytes argument/result; old binary contracts and entry
limits are unchanged. The same window cache, Fresh/Reuse protocol, task guard,
whole-selection root proof and cancellation ledger apply. No row retry or
second evaluator is introduced. The paired SDK commit is89a0ea5.

The actual frontend providers preserve distinct native owners:

- Integer ABS completes the typed Int vector, then applies signed ABS to the
  bit carrier. It does not substitute row UInt value dispatch. The source
  expression's bounded overflow diagnostic remains unchanged.
- Decimal CAST completes the Int vector, then uses the original source-or-
  target unsigned rule and per-non-NULL conversion/context reads. The pure
  integer-source Decimal conversion is now shared with the native reference.
- JSON CAST validates its source declaration before any input demand, obtains
  the complete source-typed vector, then converts JSON with the original flags.

`batch_window_values.rs` extracts arithmetic's existing source-vector owner for
reuse: literal-once broadcast, column normalization, computed Int normalization
and already typed integer windows remain distinct. Known conversion bounds also
include Decimal visible/storage padding before formatting, rather than just its
small coefficient payload. Statement-wide allocator-peak bounds are not claimed.

Source lowering now selects these genuine unary phases at roots and operand
edges; it replaces the previous three closure exclusions with implementation.
The authentic row companion is still selected at native's flag point, using the
same SDK driver. Standalone Decimal CAST priority also runs before that point,
even when vectorization is disabled: a genuine unary cast window finalizes the
strict column-padding import. Unsupported/non-column CAST shapes do not evaluate
the child in that priority stage, including for empty input. Metadata records
and validates the actual one/two argument ordinals. Only typed representation
refusal/final MyDecimal refusal can enter the subsequent native semantic phase;
SQL, resource and binding errors remain terminal. Runtime providers own values
and immutable facts, with no Expr callback or old EvaluatedArgs program.

Validation commands:

```sh
# TiKV root
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tikv test --release --locked --offline -j8 -p tidb_query_expr --lib
RUSTFLAGS='' TIKV_FRAME_POINTER=0 make run COMMAND='bash /home/agent/.slock/agents/1555103c-9f3e-46ca-ac2e-b964c650c2f1/evidence/no-fallback/n3-clippy-scoped.sh --lib --tests --release --locked --offline -j8'
# TiDB root
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
make lint
```

Final frontend run: **1483 passed**, exact same four inherited failures and
95 ignored. It includes all124 batch tests. Final core run: **1055 passed**,
exact same six inherited failures and1ignored. Failure-name equality against
N6f/N6e baselines was checked mechanically; neither full suite is completely
green. Scoped project clippy passed with89 inherited warnings. Pinned rustfmt
and both worktrees' diff checks passed; make lint passed.

The three-operation protocol test covers2050 rows, full-vector Fresh/Reuse,
wrong arity/carrier rejection, exact owned primary-cause identity, cancellation,
resource failure before input and cleaned-program reuse. All11 window tests
pass. Actual frontend unary-provider tests cover71 value/input-failure cases,
108 additional resource attempts, source admission before data, and a1M-place
Decimal refusal before context or formatting. These compare native values,
structured errors, context/warning traces, complete input maps and cleanup.

New root/nested tests add336 native comparisons across both flags and empty,
reordered/repeated/2050-row selections. Twelve more rewritten SQL forms add144
comparisons, giving1153 root comparisons including N6f's673. Tests include
UInt bit-pattern literals with signed declarations, narrow/unspecified/unsigned
Decimal targets, standalone priority, nested arithmetic/ABS/CAST/JSON, and child
failure before conversion. The previous source/capability/lazy-control tests
remain enabled. All24 Production Projection tests pass, including a new mixed
real-column ABS/Decimal/JSON test for serial/two-worker execution under both
vector flags (four new production integration runs).

Evidence lives in the agent workspace under `evidence/no-fallback/`:
`n6g-unary-window-core.log`, `n6g-unary-core-full.log`,
`n6g-unary-core-clippy.log`, `n6g-unary-provider-initial.log`,
`n6g-unary-provider-values.log`, `n6g-unary-provider-guards.log`,
`n6g-unary-source-initial.log`, `n6g-unary-roots.log`,
`n6g-unary-front-full.log`, `n6g-unary-projection.log`,
`n6g-unary-lint.log`.

The global alternate expression evaluator remains. General typed-control
wrapping, dynamic/deferred and extra sources, other function families/callers
and ordered effectful multi-output execution still require implementation and
retirement. The unrelated untracked third-party BUILD.bazel is excluded.
This checkpoint is local with Signed-off-by; no push or PR is authorized.
