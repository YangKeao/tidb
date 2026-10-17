# N5d3: native logical demand and nullable truth

TiDB parent7dceb0b0 plus this commit; paired TiKV0b491ef. Signed local
checkpoint, no push. The global alternate evaluator remains.

AND/OR use the same HostFrame, native value store and Selection metadata as
other controls. Shared logical_needs_right keeps the original scalar caller
and the new host on the same demand rule: false AND / true OR stop immediately;
SQL NULL requires the right operand. The host validates source ordinal, role
and row, checks the borrowed payload allowance before copying, releases the
store borrow, then invokes the original plain logic_truthy with the actual
statement context. String/byte truncation warnings occur before any later
operand demand and survive a selected error. IF/CASE's distinct truth policy
remains unchanged.

The new boundary calls the public RPN impl_op::logical_and/logical_or value
kernels directly. It does not invoke eval_logical_ready_in, EvaluatedArgs,
LocalProgram, an Expression callback or another interpreter. An absorbing left
uses an explicitly irrelevant kernel representative for the undemanded right;
no right value is fetched or marked demanded. Nullable output passes through
the original result coercion and is minted under the parent's own identity.

Real SQL initially exposed an uncovered rewriter wrapper around non-Int
logical arguments: istrue_with_null(CAST(...)). Its closed native host now uses
plain truthy_of and the direct RPN int_is_true<KeepNullOn> kernel, preserving
SQL NULL and leaving truncation warnings to the preceding CAST. All three core
operation descriptors use the existing native capability profile and the same
metadata/arity/budget/ordinal checks as the other value hosts.

Validation:

- RED before implementation: native logical root refused preparation. Final
  17 manual roots ×3 selections compare native raw values, NULLs, exact errors
  and complete warning tuples. Four NULL-left cases explicitly use a CAST
  producer so they exercise this new host. Both-NULL, NaN, JSON truth, selected
  and skipped errors, two string warnings and outer CHAR/IF are included.
- Six actual parser/rewriter SQL forms ×6 selections (36 executions) cover
  string columns, numeric/JSON conversions, outer CHAR/IF/COALESCE, empty and
  repeated rows, selected errors, and2050 selected occurrences. Every route
  prepares without fallback/refusal and matches native diagnostics.
- Provider tests reject oversized borrowed inputs and wrong-role handles
  before executing the value kernel, with no native-error side channel.
- 93 focused tests pass. Full frontend1445 pass, exactly the same4 inherited
  failures by name,95 ignored. Fresh core1043 pass with exactly the same6
  inherited failures by name,1 ignored. No new suite failures.
- Fresh17 production Projection tests pass. The new10-case serial/parallel
  matrix includes nullable demand, warning prefixes before selected errors,
  two-string warnings and CHAR composition; serial route counters exclude
  fallback/refusal.
- make lint, scoped core clippy, scoped rustfmt and diff checks pass. Provider
  and arena static audit finds no Expression eval, evaluate_args_in,
  EvaluatedWorker or LocalProgram; logical string conversion dependencies
  were also checked for hidden evaluated-argument adapters.

Pinned reproduction with RUSTFLAGS='', CARGO_BUILD_JOBS=8: from TiDB root,
../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked
--offline -j8 -p tidb-expr --lib evaluator::batch, then its fresh binary for
full tests; -p tidb-executor --lib projection:: for production callers. From
TiKV root use ../tools/cargo-tikv test --release --locked --offline -j8
-p tidb_query_expr --lib.

Raw workspace evidence/no-fallback logs: n5d-logical-before.log,
n5d-logical-focused-wrapped.log, n5d-logical-front-full.log,
n5d-logical-core-full-wrapped.log, n5d-logical-core-clippy-wrapped.log,
n5d-logical-projection-wrapped.log and n5d-logical-lint.log.

Memory observations remain conservative logical allowances, not a hard
statement/allocator peak limit. No speedup claim is made. XOR, native NULLIF
and comparison, remaining arithmetic producers, effects, sources and callers
require further coverage before migration dispatch can be deleted.
