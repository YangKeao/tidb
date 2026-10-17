# N5g1: native ABS and bounded source diagnostics

TiDB parent d67f7011 plus this commit; paired TiKV29358e6. Signed local
checkpoint, no push. The global alternate evaluator remains; this is a
completed arithmetic-producer step within the active no-fallback migration.

ABS now uses the same staged HostFrame as the native value producers. It
requests its argument once, checks the capability's role, producer, window
and occurrence, checks the borrowed payload copy allowance, and drops the
store borrow before conversion or warning emission. Actual Datum kind, rather
than the declared unsigned flag, selects signed Int, UInt, raw IEEE, Decimal
or the existing plain numeric conversion. NULL reaches the nullable shared
primitive. Every result passes through shared return coercion and is published
with the ABS producer's own identity.

The signed, unsigned, raw IEEE and wide Decimal computations are public pure
entries also called by the original registered RPN wrappers. Signed overflow
is a typed failure; no error text parsing is used. Unsigned magnitude retains
all64 bits, IEEE ABS retains NaN payloads, and Decimal retains the original
coefficient/scale bridge and finite kernel/output allowance. The original
wrapper still owns its original structured overflow diagnostic.

The native provider cannot retain an expression callback to recover ABS's
source-shaped BIGINT overflow text. Instead the existing renderer now shares
a pure literal formatter; the compiler-only renderer borrows literal values
without evaluating constants. The original caller keeps its eval_in behavior
for sources not yet migrated. A reverse borrowed source pass computes whether
the complete diagnostic is renderable and its conservative byte bound. All
owned diagnostic copy groups are charged before literal conversions, detached
snapshots or formatting. If any child cannot render, the compiler skips the
formatter altogether and retains the original generic overflow carrier.
Runtime error-string copies are checked before allocation.

An ABS root chooses native values only for a complete native producer tree;
existing typed ready paths such as ABS(PLUS(...)) remain available during the
migration. Unknown producers under mandatory native parents remain admission
gaps. This checkpoint neither renames migration dispatch nor converts valid
SQL into a new unsupported operation.

Validation:

- 23 manual roots ×3 selection shapes compare exact output bytes/NULLs,
  typed errors and complete warning lists, with prepared routes and no
  fallback/refusal. Empty, repeated and2050-occurrence selections cover
  signed minimum, declaration/actual signedness disagreement, UInt upper
  half, raw NaN payload/negative zero/infinity, Float32, wide Decimal,
  prefix-string warnings, binary literals, enum/set, duration, vector and
  sentinel errors, nested control diagnostics and CAST composition.
- A tiny-coefficient Decimal with200000 visible fraction digits proves the
  source diagnostic budget rejects before formatting. The same skipped
  branch compiles without an ABS diagnostic; a later unrenderable string
  prevents all formatting, including the earlier huge Decimal. The test's
  32KiB source allowance admits the entire non-diagnostic tree.
- Seven additional actual rewritten SQL forms include string, UInt, Decimal,
  raw float columns and nested CAST/COALESCE/IF. The combined SQL test now
  covers39 forms ×6 selections =234 executions with exact diagnostics and
  prepared routes. The remaining PLUS producer admission case stays explicit.
- Nine new Projection roots compare serial and two-worker execution against
  the original evaluator for raw outputs, overflow errors and warnings.
  All21 production Projection tests pass.
- Focused batch103 pass. Full frontend1458 pass, four exact inherited failures,
  95 ignored. Full TiKV1044 pass, six exact inherited failures, one ignored;
  the added pure ABS test is registered and executes. Failure-name lists
  were compared with the N5f4 logs, not merely their counts.
- make lint, scoped core clippy, scoped pinned rustfmt and git diff --check
  pass. Logs: workspace evidence/no-fallback/n5g-abs-{focused-final,
  front-full,core-full-final,projection,clippy,lint}.log.

Transitive audit: finish_math calls the shared value primitives, plain
math_fn::numeric_arg, plain Decimal representation bridges, shared return
coercion and the checked native publisher. It does not call math_fn::abs,
ScalarFunction evaluation, dispatch_values, evaluate_args_in, or an
EvaluatedArgs program. The original remaining callers still use their old
adapters; their eventual removal is separate work.

Source and runtime allowances count logical owned copy groups. Context,
external input/output owners, SDK internal temporaries and allocator capacity
peaks are not a hard statement-memory guarantee. Other arithmetic functions,
other semantic families, effects, remaining sources/callers and final global
migration-dispatch removal remain outstanding.
