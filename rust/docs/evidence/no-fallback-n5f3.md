# N5f3: native NULL-safe equality through shared comparison frames

TiDB parent4f6d67c1 plus this commit; paired TiKV30befc6. Signed local
checkpoint, no push. Global alternate execution remains.

NullSafeEqNative shares the existing fixed left-capability state, raw right
demand, complete provenance checks, borrowed preclone allowance and own result
publication with the six ordinary comparison hosts. Comparison metadata now
holds the closed SQL host operation; the catalog, operation and function name
must agree. Root/source recognition and native argument transport include the
new operation while retaining bounded complete-producer classification.

The original NULL-safe preparation was extracted into the pure comparison
module as PreparedNullSafeComparison. Existing callers adapt this shared
preparation back into their temporary argument program; the new host computes
the prepared value directly. No expression owner, evaluated-argument worker or
another program enters the new provider call chain.

This operation preserves its original generic policy, independently of the
ordinary typed integer shortcut. Range sentinels are rejected before actual
NULL handling. Actual NULL uses the other value's genuine presence and the
existing core is_null primitive. Non-NULL values use the original generic Eq
preparation and direct kernels. A duration-column/constant-text comparison
normalizes a conversion NULL through int_is_true<KeepNullOff>; a datetime/text
conversion NULL stays NULL, as in the original port. This preserves existing
behavior rather than broadening the normalization to every domain.

Finite allowance validation is shared by the direct comparison boundaries,
including the presence-only path. Original result coercion and checked output
identity remain unchanged. Source input NULL is still a native value behind a
valid handle, not a wire NULL or the absence of a capability.

Validation:

- RED: CAST(nulleq(...)) used migration dispatch before this recipe existed.
  The common matrix now runs21 pairs ×7 operators ×3 selections (441 cases),
  comparing raw result bytes, SQL NULL, exact native errors and full warnings.
  Cases include both/one NULL, JSON null, signed/unsigned bits, NaN, wide
  decimal/vector values, nested controls and selected right errors.
  Sentinel/NULL cases distinguish ordinary typed and generic NULL-safe policy.
- Seven new actual parser/rewriter SQL forms cover <=> with numeric strings,
  duration/datetime conversion errors, UInt, NULL with a demanded JSON cast,
  IF and nonfinite Real. Together the comparison SQL test executes22 forms
  ×6 selections (132 executions), with preparation and no fallback/refusal.
- A fixed-expectation pure test independently checks duration conversion
  failure=false, datetime conversion failure=NULL, both/one NULL presence,
  sentinel rejection before NULL and rejection of an unbounded allowance.
- 98 focused tests pass. Full frontend1453 pass with exactly the same4
  inherited failures by name,95 ignored. Core1043 pass with exactly the
  same6 inherited failures by name,1 ignored.
- Fresh19 production Projection tests pass. The expanded comparison matrix
  has63 roots ×serial/parallel (126 executions); outputs, errors and warning
  tuples match the original caller, including both NULL and selected right
  errors. Serial route counters exclude fallback/refusal.
- make lint, scoped core clippy, scoped rustfmt and diff checks pass. Static
  provider/arena and pure comparison production audit finds no expression
  evaluation, evaluated-argument worker or LocalProgram. The legacy adapter
  remains explicitly outside the pure module for migration callers.

Pinned reproduction with RUSTFLAGS='', CARGO_BUILD_JOBS=8: from TiDB root,
../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked
--offline -j8 -p tidb-expr --lib evaluator::batch, then its fresh binary
for full tests; -p tidb-executor --lib projection:: for production. From
TiKV root use ../tools/cargo-tikv test --release --locked --offline -j8
-p tidb_query_expr --lib.

Raw workspace evidence/no-fallback: n5f-nullsafe-before.log,
n5f-nullsafe-focused.log, n5f-nullsafe-front-full.log,
n5f-nullsafe-core-full.log, n5f-nullsafe-core-clippy.log,
n5f-nullsafe-projection.log and n5f-nullsafe-lint.log.

Budgets remain conservative logical component allowances, not a complete
statement/allocator peak tracker. No performance claim is made. NULLIF,
remaining arithmetic producers, effects, source forms and other callers still
need coverage before migration dispatch can be removed.
