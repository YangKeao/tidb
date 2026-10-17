# N5f1: comparison values and metadata separated from argument programs

TiDB parent5420d3b3 plus this commit; paired TiKVb5243d4. Signed local
checkpoint, no push. This is shared comparison groundwork; the new comparison
HostFrame is the next step. Global fallback has not been removed.

The existing prepare_comparison body now lives in ops/comparison.rs and returns
an owned PreparedComparison: genuine conversion NULL, signed/unsigned integer
pair, shared decimal, raw IEEE real, collated bytes, JSON bytes, native vector,
time-core bits or duration nanoseconds. Domain selection, conversion order and
context diagnostics keep their original implementation. Existing callers use
the extracted preparation and a narrow adapter back to their current evaluated
argument boundary. That adapter remains outside the pure module and is a
migration dependency to remove when those callers move to the unified driver.

Comparison metadata has a query-only trait and an owned OperandFacts form
(eval type, unsigned flag, constant classification and real duration-column
classification). The existing Expr-backed operand implements the same queries
lazily, so extraction does not eagerly walk/fold every operand or evaluate an
expression. The future compiled host can supply the owned facts directly.
unsigned_operand now delegates to its extracted plain flag/value helper.

Each core fixed comparison profile also exposes a pure value entry generated
from its existing operation/predicate table. Both the registered RPN kernel and
the direct entry invoke the same implementation helper. No comparison algorithm
was copied. PreparedComparison::compute uses those direct calls, including raw
NaN/Inf transport, wide decimal coefficients and original collation policies;
it does not construct a worker or an argument program.

The direct boundary requires a finite allowance. Before native-vector byte
representation copies, it checks the conservative original-capacity plus copy
size, with checked arithmetic. Decimal passes the actual finite allowance to
its existing core primitive. These are logical boundary checks, not a complete
statement/allocator peak tracker; external owners and other SDK temporaries
remain outside that observer.

Validation:

- 18 value/type fixtures ×18 ×6 comparison operators ×4 column/constant
  classifications ×3 collations =23,328 comparisons. Raw results, NULL,
  exact preparation errors and full warning tuples match the original entry.
  Cases include actual Int(-1) under UNSIGNED metadata, UInt64 maximum, NaN,
  signed zero, wide decimal precision, truncating strings, duration-column
  versus constant text, datetime parsing, JSON null/array, range/raw errors,
  and native vectors including raw mutable NaN/Inf.
- A second test rejects zero vector-copy allowance before conversion, accepts
  the sufficient conservative allowance, and rejects an unbounded allowance.
- Both new tests pass. Existing95 batch tests pass. Full frontend1449 pass
  with exactly the same4 inherited failures by name,95 ignored. Core1043
  pass with exactly the same6 inherited failures by name,1 ignored.
- Existing18 production Projection tests pass after extraction of the original
  comparison preparation. The subsequent direct-only budget strengthening
  does not change a production caller yet; it is covered by the two new tests.
- make lint, scoped core clippy, scoped rustfmt and diff checks pass. The
  production portion of the pure comparison module contains no Expression,
  ScalarFunction, EvaluatedArgs, evaluate_args, LocalProgram or expression eval.
  Test-only expressions are independent reference metadata for the original
  caller. Transitive conversion helpers were audited for hidden argument
  programs; vector/datatype conversion, temporal parsing, numeric conversion
  and the decimal representation bridge remain value operations.

Pinned reproduction with RUSTFLAGS='', CARGO_BUILD_JOBS=8: from TiDB root,
../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked
--offline -j8 -p tidb-expr --lib ops::comparison, then the fresh binary for
full tests and evaluator::batch; -p tidb-executor --lib projection:: for
production. From TiKV root use ../tools/cargo-tikv test --release --locked
--offline -j8 -p tidb_query_expr --lib.

Raw workspace evidence/no-fallback: n5f-comparison-values.log (initial two
import paths corrected, then the first matrix passed),
n5f-comparison-values-budget.log, n5f-comparison-front-full.log,
n5f-comparison-batch.log, n5f-comparison-core-full.log,
n5f-comparison-core-clippy.log, n5f-comparison-projection.log and
n5f-comparison-lint.log.

Next: connect fixed native comparison hosts with exact child provenance,
immutable operand facts and original raw-operand demand order. Existing
comparison programs and fallback are still present. NULLIF, arithmetic
producers, effects, source forms and other callers remain outstanding.
