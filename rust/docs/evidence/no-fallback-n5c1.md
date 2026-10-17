# N5c1: native computed CAST islands without a second evaluator

TiDB parent bb772dee plus this commit; paired TiKV9cc6b26. Signed local
checkpoint, no push. Global fallback remains active.

## Implementation

The closed core CastNative host consumes/returns one Bytes value handle. Native
source lowering joins a requested native view with a known one-argument CAST
name, original source/result types and exact source ordinals. Its immutable
descriptor contains CastType, name, types, argument/producer ordinal and output
role, with no Expression, ScalarFunction or evaluation callback. Metadata and
borrowed payload budgets complete before detached copies. Invalid PB/deferred/
virtual/correlated sources remain rejected by the existing source boundary.

The existing HostFrame demands the child, validates its native input role and
selected row, checks the borrowed host payload allowance before the kernel
clone, then invokes the existing plain cast::eval_cast helper. The arena also
checks the clone allowance. The result is passed through the unchanged result
coercion extracted from ScalarFunction into coerce_function_result(name,type,
value), checked against logical result/token/vector policy, and published as a
new native value handle bearing this producer's ordinal and requested role.
The source never traverses expressions at runtime. CHAR's host now also checks
its borrowed input allowance before cloning.

Known CAST targets cover signed, unsigned, unsigned-in-union, char, binary,
decimal, date, datetime, time, year, double, float and vector. Native root casts
(except the existing CHAR fast paths) and casts inside a native argument view
form computed value islands. An outer CHAR no longer picks its typed Int path
for these native producers, allowing nested casts to feed CONCAT and REGEXP.
These facts do not imply arbitrary typed arithmetic/control parents can consume
native handles. JSON CAST has a distinct pre-argument type check and source
evaluation conversion; it remains a separate outstanding task.

Generic CAST diagnostics go directly to the actual statement context, retaining
its own allocation policy. Resource errors after a kernel are terminal and do
not replay it or demand another expression. No diagnostic buffer is owned by
this generic host. The existing CHAR-specific buffered-warning policy remains.
SDK conversion/formatting temporaries, source/destination/context owners and
allocator peaks are outside the logical observers.

## Transitive audit correction

The first passing implementation still reached evaluate_args_in through the
old real-to-unsigned helper. That intermediate state was not committed. The
original algorithm is now a public plain f64 kernel in core native_cast.rs,
used by both the existing framed ABI and the ordinary native helper. The latter
checks actual Real/Float32 kind and calls the primitive directly, without
identity encoding, EvaluatedBytesOp, worker/cache or local program creation.
Existing independent boundary expectations cover ties-even, negative/upper
bounds, infinities and three NaN patterns; direct/framed results agree.
Overflow code1690 and original message rendering remain unchanged. Static audit
of all frontend cast*.rs helpers, provider and arena finds no evaluate_args_in,
EvaluatedWorker, LocalProgram or Expression eval calls.

## Validation

- RED proof: cast_signed root, including empty selection, previously routed
  to fallback. The final 22 root cases ×3 selections match native output raw
  bytes, NULLs, exact owned errors and complete warning vectors. They include
  nested casts, original YEAR source metadata, Decimal/FSP, vector errors,
  NULL, Real NaN/Inf/negative and original Float32 bits.
- 24 actual parser/rewriter SQL executions cover string columns, signed/
  unsigned/Decimal/Double, nested CAST, outer CHAR, CONCAT and REGEXP with
  empty/duplicate/2050 selected occurrences. All prepare without fallback or
  refusal and retain exact output and diagnostics.
- Provider tests reject wrong native role and input resource policy with no
  native error side channel; existing output tests check all handles before
  any result publication. The first extended test fixture reused an existing
  CastWarnings name; the duplicate declaration was corrected before validation.
- 79 focused tests pass. Full frontend1431 pass with exactly the same4 inherited
  failures by name,95 ignored. Full core1043 pass with exactly the same6
  inherited failures by name,1 ignored.
- 13 production Projection tests pass after the plain-unsigned change. New
  serial/parallel cases compare exact output and statement warnings for signed,
  nested CHAR/Decimal and YEAR-to-datetime; serial counters confirm preparation
  without fallback/refusal.
- make lint, project-scoped core clippy, scoped rustfmt and diff checks pass.

Pinned reproduction from TiDB root: RUSTFLAGS='', CARGO_BUILD_JOBS=8,
../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked
--offline -j8 -p tidb-expr --lib evaluator::batch; use its fresh binary for the
full suite. Same command with -p tidb-executor --lib projection:: for callers.
Core uses ../tools/cargo-tikv from its root with -p tidb_query_expr --lib.

Raw workspace evidence/no-fallback: n5c-cast-before.log, n5c-cast-focused.log,
n5c-cast-focused-sql.log (duplicate fixture), n5c-cast-focused-sql-final.log,
n5c-cast-focused-budget.log, n5c-cast-focused-plain.log,
n5c-front-full-plain.log, n5c-core-full-plain.log,
n5c-core-clippy-plain.log, n5c-cast-projection-plain.log and n5c-front-lint.log.

No speedup claim is made. Native handle/copy overhead and the outstanding source,
function, coercion/control/effect and caller coverage still need work before
removing alternate-evaluator dispatch.
