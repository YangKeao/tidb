# N5d2: native IF and CASE through shared frames

TiDB parent84b06b59 plus this commit; paired TiKVbf5b5e5. Local signed
checkpoint, no push. Global fallback remains active.

Closed IfNative/CaseNative hosts share the same HostFrame and native value
transport as nullable controls. Selection metadata remains values only: name,
result type, exact argument producer ordinals and own output ordinal/role.
The private metadata variant is now Selection to describe both operations.
No Expression, ScalarFunction or evaluation callback is stored or constructed.

IF demands its condition, then exactly one branch. CASE walks flattened
condition/result pairs and requests only a matching result or optional ELSE.
Predicate lookup validates the native input capability and row provenance and
checks its borrowed payload allowance before conversion. crate::truthy_of calls
the original plain core native_truthy conversion; native_if_choose_branch
selects the next operand. This keeps original string-prefix, JSON, NaN and SQL
NULL semantics. In particular it does not add WHERE's truncation warning policy
to IF/CASE. No old evaluated-args IF/CASE wrapper is invoked.

Selected arguments share the same borrowed preclone gate, native value copy,
coerce_function_result and producer-handle publication as nullable controls.
CASE without a match or ELSE publishes an owned NULL under its own producer
identity, not a predicate's identity. Source preference recognizes non-Int
predicate declarations in its bounded sibling/depth/node traversal; complete
native producer closure still protects existing typed ready paths. Original
source metadata, declaration budgets and ordinal subtree joins remain checked.

The N5c3 CAST(IF(column,...)) example now has a connected producer and requires
preparation without fallback. Only that case's route assertion changed; ABS and
PLUS cases remain explicit admission capability gaps until their native
producer recipes are connected. Resource, protocol and native execution errors
remain terminal and are never retried through another evaluator.

Validation:

- RED: native IF with nested CAST initially fell back. Final14 roots ×3
  selections compare exact raw output, NULLs, native errors and full warnings.
  Cases cover skipped and selected errors, CASE without ELSE, JSON null/false
  truth, SQL NULL, NaN conditions, string prefix with no new condition warning,
  actual Int(-1) under an UNSIGNED declaration beneath CHAR, and nested controls.
- 30 parser/rewriter SQL executions use three columns, IF/CASE, optional ELSE,
  JSON document errors, CHAR/CONCAT and COALESCE. Empty, duplicate, first/else
  errors and2050 selected occurrences retain native results/diagnostics with
  no fallback/refusal.
- Predicate provider tests reject oversized input allowance and wrong input
  role as terminal local failures without a native SQL error side channel.
- 90 focused tests pass. Full frontend1442 pass with exactly the same4
  inherited failures by name,95 ignored. Core1043 pass with exactly the same6
  inherited failures by name,1 ignored.
- 16 production Projection tests pass. New serial/parallel cases retain truth,
  selected/skipped errors, SQL NULL, nested nullable controls, actual signedness,
  exact output and complete statement warning tuples; serial counters confirm
  preparation without fallback/refusal on errors too.
- make lint, project-scoped core clippy, scoped rustfmt, diff checks and static
  provider audit pass. The provider/arena contains no Expression eval,
  evaluate_args_in, EvaluatedWorker or LocalProgram.

Pinned reproduction from TiDB root with RUSTFLAGS='', CARGO_BUILD_JOBS=8:
../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked
--offline -j8 -p tidb-expr --lib evaluator::batch, then its fresh binary for
full tests; -p tidb-executor --lib projection:: for production callers. Core
uses ../tools/cargo-tikv from its root with -p tidb_query_expr --lib.

Raw workspace evidence/no-fallback: n5d-conditional-before.log,
n5d-conditional-focused.log, n5d-conditional-focused-sql.log,
n5d-conditional-front-full.log, n5d-conditional-core-full.log,
n5d-conditional-core-clippy.log, n5d-conditional-projection.log and
n5d-conditional-lint.log.

SDK conversion temporaries, source/context/destination owners and allocator
peaks remain external to conservative logical observers. No speedup claim is
made. Native AND/OR, NULLIF/comparison, other producers/effects/source forms and
callers still need coverage before migration dispatch can be deleted.
