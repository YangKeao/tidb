# N6d: native arithmetic window provider

TiDB parent0fc94169 plus this signed local checkpoint; paired TiKVe9b0a92.
No push. The TiKV commit updates only the maintenance guide; SDK code remains
ec9cc67. The no-fallback goal remains active.

NativeBatchHosts now implements start_window/resume_window for the three
explicit arithmetic descriptors. Immutable metadata records actual source and
result FieldTypes, node/role identities, diagnostic subjects/render dispositions,
source-owner classes, generic numeric domain, integer bulk mode and requested
integer result handling. The ordinary row arithmetic descriptor explicitly has
no window mode, and row callbacks keep their original contract.

Window tasks own only the physical map and complete converted left values.
They share generation allocation, task installation and cancellation with the
existing row host provider. Every reply rechecks demand index, complete field,
row map, occurrence origin, count and carrier. Each native capability is checked
against its actual argument node, role and original occurrence/physical row.
SQL byte contents are never recursively interpreted as capabilities.

The provider first completes the typed source vector, including literal-once
typed normalization and broadcast, and only then completes its vectorized
numeric argument casts. Matching source/target domains retain exact values.
The left vector finishes before the right request; both vectors finish before
any arithmetic. Integer bulk mode uses strict bit-domain values and shared
integer preparation, then the original declared-signedness result repacking.
Generic mode retains signed-literal normalization, div_precision_increment
observation, generic preparation and original return coercion. Both compute
through PreparedArithmetic.compute directly and publish checked native handles.
No Expression, ScalarFunction, evaluator callback or evaluated-argument program
is stored or invoked by this runtime path.

Source/copy/conversion/diagnostic/output allowances precede corresponding work.
The task ledger includes map/vector heads and conservative actual payload
allowances. A literal broadcast reserves its normalized payload, which can be
larger than the raw literal. Diagnostic formatting remains demand-driven.
Native SQL errors retain their actual native value behind the existing fixed
SQL-host driver sentinel; provider logical ResourceLimit errors remain terminal
and cannot return cached success. These are logical retained/copy allowances,
not a hard statement allocator-peak tracker.

Validation on final code:

- 98 direct SDK window/native batch comparisons cover three operators, empty,
  dense and repeated/reordered selections, signed/unsigned combinations,
  NO_UNSIGNED_SUBTRACTION modes, NULL demand, real/Decimal/string operands,
  constants, exact diagnostic/context traces, native overflow and input failure
  before an earlier lane's eventual arithmetic overflow.
- 148 additional provider resource refusals cover both refusal before input and
  refusal after the complete left input phase but before conversion effects.
  They assert no right demand, no context effects, no native SQL substitution
  and no remaining tasks. Existing capability guards stay intact.
- Full frontend1466 pass, four exact inherited failures and95 ignored. Failure
  names match N6c; all107 batch checks pass in the full run.
- All22 Projection tests pass. Required make lint, pinned scoped rustfmt and
  git diff --check pass. Full suites are not green.
- No SDK code changed; N6b's core1051 pass with exact inherited six failures
  and one ignored remains the paired code evidence.
- Logs: workspace evidence/no-fallback/n6d-window-arithmetic-{initial-fixed,
  values-guards,front-full,projection,lint}.log. Initial test fixture corrections
  initialized the native store generation and compared the actual retained
  native error through its existing transport sentinel, without changing the
  production capability or error checks.

Primary focused command from the TiDB tree:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib arithmetic_window::tests

The freshly emitted frontend test binary was then run without a filter.
Projection, lint and formatting follow the commands recorded in N6c.

Current source lowering still emits row descriptors with batch=None. The new
provider is exercised through actual checked SDK programs and native values,
not yet selected by frontend arithmetic roots. Typed controls, bulk ABS/CAST
source phases, per-edge row/bulk lowering and selections beyond1024 remain to
be integrated. Before widening, replace repeated prefix-payload scans with a
linear ledger. Then retain whole-selection argument phases rather than using
the caller's current1024 root tiling, which can change error/effect order.
Other functions/effects, dynamic sources, remaining callers, multi-output and
final global migration-dispatch removal remain unfinished. No new frontend
root fallback is claimed closed at this checkpoint.
