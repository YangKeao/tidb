# N5b2: native leaf root outputs through shared frames

TiDB parent dd33c9e0 plus this commit; TiKV unchanged49ead6d. Local signed
checkpoint, no push. Global fallback remains; no new general function coverage
is claimed.

NativeView::RootValue selects lossless native transport for rich literal roots
and calculated column roots (when direct-column optimization is disabled).
Ordinary representable Int/UInt/finite Real/String/Bytes literals retain their
existing broadcast fast path. Selection uses only borrowed kind/declaration
inspection before preflight; complete source/schema/payload budget checks still
precede conversion and snapshots. Parameters, deferred/virtual/correlated and PB
sources remain outside this stage. Existing computation wire checks remain
mandatory in calculated trees without native transport.

Output handles use NativeValueRole::Output, distinct from CHAR argument handles.
The existing frame driver runs a native input leaf; there is no separate
expression evaluation. The output consumer checks Bytes shape, selection length,
retained vector/selection policy and every handle's namespace, window, role,
root node ordinal, occurrence and physical row before publishing any cell.
Wire NULL is invalid; native SQL NULL has its own validated value handle.
Publication borrows the actual Datum and uses the same Chunk::append_datum as
the native evaluator, preserving its output storage rules without a kernel-copy
allocation. The store remains alive until output consumption finishes. Source,
SDK and destination allocation policy remain outside the arena's logical
observer; this is not a hard statement memory tracker.

Validation:

- The literal regression failed before the change at Float32 with a fallback
  route. Final test covers 18 declarations / 17 actual native kinds including
  NULL, 216 scenarios across vector mode, empty/unselected/2050 repeated rows.
  Output raw bytes/NULL bits match native value publication, including NaN
  payload, negative zero, unsigned bits, Decimal/JSON, temporal and hybrid kinds.
  Internal range-only MinNotNull/MaxValue sentinels are not SQL output fixtures.
- Native column raw-storage tests now also disable direct-column optimization,
  exercising the new calculated-column path (72 total scenarios).
- Adversarial output tests reject a late wrong-role/wrong-occurrence/wire-NULL
  handle with no earlier result written, and reject retained-byte policy denial.
  Existing store tests continue to cover namespace/generation/forgery/resource
  checks. The first adversarial fixture used the test-only call-root helper;
  switching to the production calculation compiler tests the intended boundary.
- 76 focused tests pass. Full frontend1428 pass, exactly the same4 inherited
  failures by name,95 ignored. 12 production Projection tests pass, including
  serial/parallel Float32, NaN, Decimal and JSON roots with raw output parity
  and no new warnings; serial route counters confirm no fallback/refusal.
- make lint, scoped rustfmt and diff checks pass. Core implementation is
  unchanged; N5a2 clippy/full-core evidence remains applicable.

Use pinned ../tools/cargo-tidb from TiDB root, RUSTFLAGS='', CARGO_BUILD_JOBS=8:
 test --manifest-path rust/Cargo.toml --release --locked --offline -j8
 -p tidb-expr --lib evaluator::batch; use the fresh binary for full tests.
Use the same command with -p tidb-executor --lib projection:: for callers.
Raw workspace evidence/no-fallback: n5b-root-before.log, n5b-root-focused.log,
n5b-root-focused-final.log (call-root fixture failure),
n5b-root-focused-contract-final.log, n5b-root-full.log,
n5b-root-projection.log and n5b-root-lint.log.

Rich native leaf roots use per-occurrence checked handles, which carry overhead
relative to a direct raw broadcast. This checkpoint establishes correctness and
coverage; it makes no speedup claim. Next extend native computed results and
coercions using plain value kernels and preserve argument demand/context effects
before removing the global alternate evaluator.
