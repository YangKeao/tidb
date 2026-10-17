# N5e1: native unary boolean predicate closure

TiDB parentbb161a81 plus this commit; paired TiKV6696e51. Signed local
checkpoint, no push. Global alternate execution remains.

Native NOT, IS NULL, IS TRUE/FALSE, their negations, and UNKNOWN aliases now
produce checked native handles in CAST/CHAR/control islands. The prior
IsTrueWithNullNative boundary shares its frame with these closed unary hosts.
Source metadata remains immutable names/types/producer ordinals; arity, exact
operation/name joins, native store availability, source roles and row identity
are checked before value conversion. Bounded root preference recognizes non-Int
predicate inputs while preserving existing complete typed trees.

IS NULL observes raw actual nullness without invoking truth conversion. UNKNOWN
aliases retain ScalarFunction's original truth-validation step, including
rejections that a raw nullness test would bypass. NOT preserves its separate
range-sentinel diagnostic. Other predicates use the original pure truthy_of;
none adds truncation warnings. A SQL rewriter's earlier CAST remains responsible
for its own warnings/errors. Borrowed payload allowance precedes conversion;
store borrows end before native error publication.

Finalization calls the existing public RPN int_is_true<KeepNullOff/On>,
int_is_false<KeepNullOff>, is_null and unary_not_int value kernels. Negated
predicates compose the same positive predicate and unary NOT used by their
existing registered programs. No eval_boolean_ready_in, EvaluatedArgs program,
Expression callback or fabricated source expression is used. The original
result coercion and own-producer handle publication remain shared.

An older test expected JSON IS NULL to fall back because its typed catalog
inserted CastJsonAsString. That route is now connected directly and the test
requires preparation with identical raw output, not a compatibility escape.

Validation:

- RED before implementation: CAST(NOT(NULL)) fell back. Final9 predicate names
  ×20 value fixtures ×3 selections (540 cases) cover all19 actual Datum kinds,
  genuine NULL, signed/unsigned bits, strings/bytes, NaN, JSON null/false,
  temporal/decimal/vector/hybrid/raw values and both sentinels. Exact native
  output, NULL state, errors and full warning tuples are compared. Additional
  fixtures intentionally disagree with their SQL declaration to check actual
  value transport. Empty, repeated and2050-occurrence selections are included.
- 12 parser/rewriter SQL forms ×6 selections (72 executions) cover every SQL
  predicate form, nested OR/NOT, IF/COALESCE, CHAR/JSON conversion, empty and
  duplicate rows, selected errors and complete warning prefixes. Routes must
  prepare without fallback/refusal.
- The existing predicate hostile-input test now checks IF and all9 unary
  recipes: oversized borrowed input and wrong-role handles remain terminal,
  without a native SQL error side channel.
- 95 focused tests pass. Full frontend1447 pass with exactly the same4
  inherited failures by name,95 ignored. Core1043 pass with exactly the same6
  inherited failures by name,1 ignored.
- 18 production Projection tests pass. New36-case serial/parallel matrix
  checks NULL, string truth without an added warning, special-value errors,
  outer CAST and preparation counters (72 executions).
- make lint, scoped core clippy, scoped rustfmt and diff checks pass. Static
  provider/arena audit finds no expression eval, evaluate_args_in,
  EvaluatedWorker or LocalProgram. truthy_of delegates to pure native_truthy.

Pinned reproduction with RUSTFLAGS='', CARGO_BUILD_JOBS=8: from TiDB root,
../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked
--offline -j8 -p tidb-expr --lib evaluator::batch, then the fresh binary for
full tests; -p tidb-executor --lib projection:: for production. From TiKV root
use ../tools/cargo-tikv test --release --locked --offline -j8
-p tidb_query_expr --lib.

Raw workspace evidence/no-fallback logs: n5e-boolean-before.log,
n5e-boolean-focused.log (new predicates pass; one retired route expectation),
n5e-boolean-focused-sql.log, n5e-boolean-front-full.log,
n5e-boolean-core-full.log, n5e-boolean-core-clippy.log,
n5e-boolean-projection.log and n5e-boolean-lint.log.

The conservative logical budget still excludes external source/context/output
owners and allocator peaks. No performance claim is made. Native comparison,
NULLIF, arithmetic producers, effects, source forms and other callers remain
necessary before removing migration dispatch.
