# N5d1: native COALESCE and IFNULL through the staged driver

TiDB parent adb952e0 plus this commit; paired TiKV6e5a497. Local signed
checkpoint, no push. Global fallback elimination remains active.

Closed CoalesceNative/IfNullNative host signatures transport actual native
value handles. Their immutable descriptor contains name, result type, direct
argument producer ordinals and output ordinal/role, with no expressions or
evaluation callbacks. The same HostFrame requests one argument at a time.
Lookup validates namespace/window/input role/exact producer/selected occurrence
and row, then classifies actual Datum::Null. A non-NULL Bytes handle itself
does not establish SQL non-NULL. The existing pure native_if_null_choose_first
selector decides whether another argument is needed.

A selected native value passes borrowed host payload policy before a kernel
clone, then the existing plain coerce_function_result boundary. This preserves
actual UInt/Int kind and COALESCE's temporal FSP stamping. Shared native result
publication checks logical result/token/vector allowances and mints a new handle
with this control's producer identity, including the all-NULL case. It does not
forward a child identity as the parent result. The same publisher is now used
by native CAST. Old coalesce/if_null evaluated-args wrappers are never called
from these hosts. Context warnings/errors and destination/SDK allocations
retain their existing external policies; this is not a hard allocator tracker.

Native preference uses bounded borrowed kind/metadata inspection. Already
prepared typed Int/String/control trees retain their path when the complete
child tree cannot yet produce native handles. An intermediate implementation
failed one existing COALESCE(IF(...)) route assertion; restricting native-root
preference to complete producer trees fixed this before commit. Native inputs
inside CAST/CHAR remain closed over the current native producer recipes; missing
producers retain N5c3 admission compatibility.

CheckedNode stores its subtree end. A reverse in-place pass computes child joins
in linear time; variable-arity descriptors advance through these ends. Its
record budget accounts for the added word, and ordinal arrays/result snapshots
are charged before allocation/copy. Classification stops the sibling iterator
at its node/depth bound. Arity/node and pending metadata checks now precede
argument declaration scans; a regression requires a node-policy error before
an argument declaration capability gap.

Validation:

- RED: nested CAST/COALESCE previously fell back. Final 12 root cases ×3
  selections compare exact raw output, NULLs, native errors and warning vectors.
  They cover skipped vector/JSON errors, all-NULL, JSON null as a non-NULL value,
  NaN bits, UInt64::MAX and Int(-1) declared unsigned under CHAR, and temporal
  precision without rounding the instant.
- 30 actual parser/rewriter SQL executions use two columns, documents, SQL/JSON
  NULL, selected versus skipped parse errors, nested nullable/CAST, CHAR, CONCAT
  and REGEXP, empty/duplicate/2050 occurrence selections. All prepare without
  fallback/refusal. Composite child ordinals are exercised directly.
- Provider tests keep selected-input resource and wrong-role failures terminal
  with no native SQL error side channel. Existing arena/output provenance and
  no-replay checks remain passing.
- 87 focused tests pass. Full frontend1439 pass with exactly the same4 inherited
  failure names,95 ignored. Core1043 pass with exactly the same6 inherited
  failure names,1 ignored.
- 15 production Projection tests pass. New serial/parallel cases cover nullable
  selection, SQL/JSON NULL, skipped/selected errors, complete warnings, actual
  signedness and time FSP; serial counters require no fallback/refusal.
- make lint, project-scoped core clippy, scoped rustfmt, static provider/arena
  audit and diff checks pass. No nested LocalProgram, EvaluatedWorker,
  evaluate_args_in or Expression eval call is present in these providers.

Reproduce with pinned ../tools/cargo-tidb from TiDB root, RUSTFLAGS='',
CARGO_BUILD_JOBS=8: test --manifest-path rust/Cargo.toml --release --locked
--offline -j8 -p tidb-expr --lib evaluator::batch, then its fresh binary for
full tests; -p tidb-executor --lib projection:: for production callers. Core
uses ../tools/cargo-tikv from its root, -p tidb_query_expr --lib.

Raw workspace evidence/no-fallback: n5d-nullable-before.log,
n5d-nullable-focused.log (existing route regression),
n5d-nullable-focused-closure.log, n5d-nullable-focused-sql.log,
n5d-nullable-focused-bounded.log, n5d-front-full-bounded.log,
n5d-core-full.log, n5d-core-clippy.log, n5d-nullable-projection-bounded.log,
n5d-lint.log. No speedup claim is made. Native IF/CASE/logical/NULLIF, further
producer kernels, effects, source forms and other callers still require coverage
before deleting alternate-evaluator dispatch.
