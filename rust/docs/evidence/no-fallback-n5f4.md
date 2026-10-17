# N5f4: native NULLIF with original first-value identity

TiDB parent45318cd0 plus this commit; paired TiKV25624f7. Signed local
checkpoint, no push. Global alternate execution remains.

NULLIF reuses the checked two-operand comparison HostFrame, fixed left handle,
unconditional raw right demand, borrowed preclone allowance and own result
publication. The catalog, source operation and function name remain joined.
It retains original immutable source metadata, but its value comparison
deliberately ignores expression-level signedness and column classification.

The original func.rs value entry compares its fetched arguments through
eval_binary_in(Eq), using literal operand facts and DERIVATION_FREE_COLLATION.
The new plain prepare_literal_comparison retains that same policy. NULLIF
also bypasses the binary operator's signed bit-literal normalization: its
original function name is not a binary operator. This matters for actual
Int(-1) under an UNSIGNED declaration, high-bit literals, duration columns
and Decimal-column/constant-string comparisons.

After the actual Eq primitive returns a normalized nullable boolean, the
provider validates it with if_head_native_args_valid and uses the same public
native_if_choose_branch helper as the existing registered NULLIF selector.
Equal returns an owned SQL NULL; false/unknown fetches the original first
value through its already validated handle. Comparison temporaries are gone
before that selected copy, which fits the already checked two-input allowance.
The result passes through the original return coercion and receives the
NULLIF producer's own handle, never a forwarded child capability.

No identity packet, expression owner or evaluated-argument program is created
in this path. Both capabilities are checked before comparison/selection even
when equality will discard the first value. Native value errors,
resource and protocol failures remain terminal.

The context-observation audit also found a missing read in the preceding
native <=> boundary: its original ScalarFunction owner observes
div_precision_increment before dispatch. The new NULLIF literal boundary and
that NULL-safe boundary now preserve the original read after both raw operands
and before comparison coercion. The value does not affect Eq, but the context
accessor remains observable. An explicit trace test protects this ordering,
including no read when the right operand fails and warning prefixes on either
side of the read.

Validation:

- RED: the native NULLIF producer used migration dispatch. Final18 manual
  roots ×3 selections (54 cases) retain exact raw outputs, NULLs, native
  errors and complete warnings. They cover selected first kinds, actual
  Int/UInt flag disagreement, signed bit literals, result collation versus
  the fixed literal comparison policy, NaN, decimal/time/JSON/vector values,
  selected right errors and nested CAST/COALESCE/IF. Empty, repeated and
  2050-occurrence selections are included.
- Two column roots ×3 selections prove that NULLIF does not adopt ordinary
  comparison column classification. Duration-column/text adds no duration
  parse warning; the large Decimal-column/string case uses its original
  literal REAL comparison, including rounding equality.
- Source inspection confirms this Rust rewriter retains NULLIF as a builtin
  and infers its first argument's result type. The func.rs comment about an
  IF rewrite describes Go, not this Rust entry. Ten new actual SQL forms ×6
  selections include NULL result type, UInt, decimal/time columns, JSON errors,
  NaN and composition. The complete comparison SQL test now runs32 forms
  ×6 selections (192 executions), with no fallback/refusal.
- Two function names ×two right-operand outcomes compare the original and
  unified context/configuration/warning traces, including the corrected <=>
  observation. The shared hostile-input test now runs Eq and NULLIF modes,
  validates exact selected output kind/value under its own producer, and
  rejects stale/wrong-role/wrong-row/wrong-producer handles, excessive copy
  allowance and operation/descriptor drift without a native SQL error side
  channel.
- 101 focused tests pass. Full frontend1456 pass with exactly the same4
  inherited failures by name,95 ignored. Core1043 pass with exactly the
  same6 inherited failures by name,1 ignored.
- Fresh20 production Projection tests pass. New10 NULLIF roots ×serial/parallel
  retain exact values, errors and statement warning tuples; serial counters
  require preparation without fallback/refusal.
- make lint, scoped core clippy, scoped rustfmt, diff checks and static
  provider/arena audit pass. The new literal preparation and selected-value
  path contain no expression evaluation, old NULLIF worker or LocalProgram.

Pinned reproduction with RUSTFLAGS='', CARGO_BUILD_JOBS=8: from TiDB root,
../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked
--offline -j8 -p tidb-expr --lib evaluator::batch, then its fresh binary for
full tests; -p tidb-executor --lib projection:: for production. From TiKV root
use ../tools/cargo-tikv test --release --locked --offline -j8
-p tidb_query_expr --lib.

Raw workspace evidence/no-fallback: n5f-nullif-before.log,
n5f-nullif-focused.log, n5f-nullif-focused-policy.log,
n5f-nullif-focused-final.log, n5f-nullif-front-full.log,
n5f-nullif-core-full.log, n5f-nullif-core-clippy.log,
n5f-nullif-projection.log and n5f-nullif-lint.log.

Budgets remain conservative logical component allowances, not a complete
statement/allocator peak tracker. No performance claim is made. Arithmetic
producers, effects, source forms and additional callers still need coverage
before migration dispatch can be removed.
