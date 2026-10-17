# N5a2: production native value inputs and CHAR

TiDB parent b2e8be32 plus this commit; paired TiKV 49ead6d. Local signed commits,
no push. This milestone does not complete global fallback elimination.

## Contract and behavior

NativeValueStore now owns demanded native literal/ordinary-column Datums in
production. Private 40-byte handles validate unique namespace, checked window
generation, role, original node ordinal and selected occurrence/physical row.
SQL bytes are values, never recursively decoded handles. Original SQL types and
row schema remain admission checks; only the effective transport view is Bytes.
Existing paths allocate no arena unless a native consumer needs one. Frames,
tasks and output consumption finish before resetting each window; errors drop
execution-local state. Computed native input expressions are not admitted yet.

The closed CastCharNative operation shares the existing host frame and receives
actual source type/charset and row provenance. It unwraps a value, releases the
store borrow, and calls the existing plain native cast helper. No Expression,
ScalarFunction eval, evaluation callback, EvaluatedBytesWorker or nested
LocalProgram is used. Shared value-admission checks preserve vector/range errors.
Exact native warning codes/messages are captured with pre/post reserve checks,
then published after result/resource validation. Resource denial remains
terminal; warning capture denial publishes no warnings or later argument work.

This also fixes our N4b1 signedness assumption: Datum::Int(-1) with an UNSIGNED
declaration produced 18446744073709551615. A failing native comparison was
recorded before the fix. Actual literal kind and the core's existing result-kind
registry now determine signedness through ANY_VALUE/ROUND and unsigned bitwise
results. Native UInt values unrepresentable by the signed wire lane use handles.

Borrowed source payload charging precedes copies. Arena charges record capacity
and a conservative 2x logical payload allowance; imports gate encoded cell bytes
before SDK decoding and actual payload afterward. SDK decoding/formatting
transients, source owners and allocator peaks are outside this observer. It is
not a statement memory tracker or a hard peak bound.

## Validation

- 73 focused frontend tests pass, including 19 actual native storage kinds,
  adversarial handles, resource denial, signedness propagation and exact errors.
- Native column cases cover Real NaN/Inf/-0, Float32, Decimal, JSON, temporal,
  YEAR zero/2026, ENUM/SET names+ordinals and BIT; duplicate selections and 2050
  selected rows cross execution windows without changing results.
- 36 parser/rewriter SQL executions cover Double/Float32/YEAR/JSON inputs in
  CAST, nested CONCAT and nested REGEXP with empty/duplicate/2050 selections.
  All match native results without fallback or prepare refusal.
- 11 production Projection tests pass. New serial/parallel cases compare exact
  native output and complete GBK decode/truncation warning tuples; serial
  counters verify preparation without fallback/refusal.
- Frontend full suite: 1425 pass, exactly the same 4 inherited failures,
  95 ignored. Core: 1043 pass, exactly the same 6 inherited failures, 1 ignored.
- make lint, project-scoped core clippy, scoped rustfmt and diff checks pass.
  Static provider/store audit finds no alternate evaluator calls.

The first Projection test fixture began with invalid GBK bytes; the native
reference correctly decoded an empty prefix, so no truncation warning followed.
Adding a valid ASCII prefix exercises both warning paths. This was a reference
fixture assertion failure, not a product regression. An earlier success-value
assertion also needed normalization through the target output Chunk, matching
production publication. All final assertions remain exact native comparisons.

Reproduce with pinned wrappers, RUSTFLAGS='', CARGO_BUILD_JOBS=8, release, locked,
offline and -j8. From TiDB root run ../tools/cargo-tidb test with
--manifest-path rust/Cargo.toml -p tidb-expr --lib evaluator::batch; run the same
command with -p tidb-executor --lib projection::. Core uses ../tools/cargo-tikv
from its root, -p tidb_query_expr --lib. Full suites use freshly built binaries.

Raw workspace evidence/no-fallback logs: n5a2-signedness-before.log,
n5a2-front-tests-sql.log, n5a2-front-full-final.log, n5a2-core-full.log,
n5a2-core-clippy.log, n5a2-lint-final.log and
n5a2-projection-native-prefix.log. Exact inherited failure names are recorded
in the full logs and previous milestone receipts.

Next: native direct transport/output, computed values, broader coercion/control
and function/context coverage, other callers, then delete global routing only
when previously supported SQL remains supported by the unified executor.
