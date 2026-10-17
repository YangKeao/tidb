# N5f2: native comparisons through shared frames

TiDB parent04337881 plus this commit; paired TiKV7766eeb. Signed local
checkpoint, no push. Global alternate execution remains.

The six Eq/Ne/Lt/Le/Gt/Ge native hosts use the same HostFrame and native value
store as CAST and controls. Metadata owns only the fixed operation/name,
original result type, two exact child ordinals, immutable operand facts,
collation and own output ordinal/role. The catalog operation, descriptor,
argument type presence and arity must agree before demand begins.

The first reply validates the left capability and retains its fixed40 bytes.
It does not convert/copy the payload or stop on SQL NULL; the right raw value
is always requested next. After both replies, both provenance/role/row joins
and the borrowed payload allowance are checked before copying either value.
Store borrows end before actual context conversion or warning publication.
This preserves a right operand error before a comparison coercion warning,
and retains earlier warnings produced by an explicitly cast left child.

After the original binary-literal normalization, a plain comparison preparation
wrapper selects the original typed integer input policy when both declarations
are Int; other inputs use N5f1's shared preparation. The first differential
test exposed a real policy difference: typed integer comparisons give genuine
NULL priority and use integer-specific coercion errors for range sentinels.
prepare_typed_integer_values now supplies that same pure rule to the original
integer_binary_typed owner and the new native wrapper. Its result is a NULL,
integer pair or declined typed classification, never an evaluator callback.

PreparedComparison::compute invokes the existing direct comparison helpers
under the remaining finite host allowance, including its vector copy gate.
Original result coercion and own-producer publication remain shared. No
evaluated-argument worker, expression owner or replacement program is created.

Preflight combines constant classification with its existing reverse subtree
join pass. It uses the same is_unfoldable_call predicate as folds_to_constant,
with no runtime constant evaluation or extra recursive classification walk.
CheckedNode's larger scaffolding is charged by size. Immutable facts preserve
unsigned/type distinctions, true duration-column identity and folded constant
subtrees. Native root preference recognizes non-Int comparisons with complete
producer closure; existing typed integer frames remain available.

Validation:

- RED before implementation: CAST(comparison) used the migration path. Final
  18 operand pairs ×6 operators ×3 selections (324 cases) compare exact raw
  results, NULL, native errors and complete warnings. Cases include UInt/Int
  declaration behavior, wide decimals, NaN, string collation, JSON, vectors,
  nested controls, selected right errors, and the typed integer sentinel/NULL
  priority in both operand orders. Both raw and cast-left warning prefixes
  are checked. Empty, duplicate and2050-occurrence selections are included.
- 15 actual parser/rewriter SQL forms ×6 selections (90 executions) cover
  Decimal-column/constant text, duration-column/constant and dynamic text,
  folded CAST constants, datetime parsing/warnings, UInt, nonfinite Real,
  JSON errors and nested IF/COALESCE/AND/NOT/CHAR. All routes require
  preparation without fallback/refusal.
- Provider checks reject stale, wrong-role, wrong-occurrence and wrong-producer
  handles and catalog/descriptor drift as terminal failures. An oversized left
  still requests the raw right, then fails the borrowed copy gate before
  conversion/warnings; no native SQL error side channel is fabricated.
  A valid result is checked under its own output producer and role.
- 98 focused tests pass. Full frontend1452 pass with exactly the same4
  inherited failures by name,95 ignored. Core1043 pass with exactly the
  same6 inherited failures by name,1 ignored.
- Fresh19 production Projection tests pass. New36 roots ×serial/parallel
  compare exact values, NULL, errors and full warning tuples, including a
  NULL left with a selected right error and raw/cast-left warning prefixes.
  Serial route counters require preparation without fallback/refusal.
- make lint, scoped core clippy, scoped rustfmt and diff checks pass. Static
  provider/arena audit finds no expression eval, evaluate_args_in,
  EvaluatedBytesWorker or LocalProgram. The new preparation call chain uses
  only plain typed-input policy, N5f1 value preparation and direct kernels.

Pinned reproduction with RUSTFLAGS='', CARGO_BUILD_JOBS=8: from TiDB root,
../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked
--offline -j8 -p tidb-expr --lib evaluator::batch, then its fresh binary
for full tests; -p tidb-executor --lib projection:: for production. From
TiKV root use ../tools/cargo-tikv test --release --locked --offline -j8
-p tidb_query_expr --lib.

Raw workspace evidence/no-fallback: n5f-native-before.log,
n5f-native-focused.log (typed integer diagnostic difference),
n5f-native-focused-sql.log, n5f-native-focused-final.log,
n5f-native-front-full.log, n5f-native-core-full.log,
n5f-native-core-clippy.log, n5f-native-projection-final.log and
n5f-native-lint.log.

Budgets remain conservative logical component allowances, not a complete
statement/allocator peak tracker. No performance claim is made. Null-safe
equality, NULLIF, remaining arithmetic producers, effects, sources and callers
still need coverage; old comparison adapters still serve those migration
callers. This checkpoint does not remove global fallback.
