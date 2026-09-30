# Expression unification experiment

Checkpoint-ID: `raw-math-six-15` (previous: `inet-four-14`)

**42/245 families delegate to TiKV with their native evaluator algorithms removed; target 221.** This checkpoint adds ASIN, ACOS, SQRT, SIGN, RADIANS and DEGREES. Strict final-audited acceptance remains 0; this is not PR-ready.

## Paired repositories

- [YangKeao/tidb, expression-unification-demo](https://github.com/YangKeao/tidb/tree/expression-unification-demo)
- [YangKeao/tikv, expression-unification-demo](https://github.com/YangKeao/tikv/tree/expression-unification-demo)

Use sibling `tidb` and `tikv` checkouts. `checkpoint.json` pins the paired TiKV commit. Root Plans mirror `/home/agent/tidb/EXPRESSION_UNIFICATION_PLAN.md`. Validated steps push both branches without force-push or automatic PRs.

## Raw math ownership

Six mathematical primitives now live once in TiKV `impl_math.rs`, shared by its original Real wrappers and private native wrappers. An explicit nullable IEEE754-bits role carries all f64 patterns without weakening Real/NotNan. Internal Byte8 transport has strict role/length checks and owned-bit results; SIGN returns signed Int. Ordinary Bytes cannot impersonate this role.

Private IDs are rejected by the ordinary registry. Only closed factory recipes select fixed getters through existing common preparation, validation and metadata construction. The same synchronous driver and operation-keyed evaluator-instance pool remain; workers are not threads.

TiDB retains coercion and output policy. SIGN adapts integer and Decimal sign/zero classes without using rounded Display/to_f64 or calculating the final sign answer. Ordinary ASIN/ACOS map raw NaN to NULL; legacy consumers retain NaN for casts and total_cmp. SQRT preserves NaN, positive infinity and negative zero; RADIANS/DEGREES retain original finite-result diagnostics. NULL also invokes the actual kernel. No new SQL/PB admission or native fallback.

Legacy unistore requires a typed error chain across numeric, string, JSON, temporal and interval consumers. `LegacyEvaluator` preserves old SQL-error folding and demand order while propagating runtime/adapter failures. Public string errors remain at the old boundary. Its method grouping includes indentation changes; use `git diff -w` to review the substantive changes.

## Actual validation

- TiKV local: 200 passed/1 ignored; raw identity/role guard: 1 passed; original math tests: 46 passed.
- New native dispatch: 3 passed; session SQL/lifecycle: 33 passed; restored legacy regressions: 2 passed.
- Full expression: 1395 passed/4 unchanged failures/94 ignored, 1493 discovered. Complete failure blocks match checkpoint14 after only thread-ID normalization.
- Full unistore: 173 passed/1 failure/13 ignored, 187 discovered. The failing EqInt('abc',-1) fixture reproduces with only cophandler.rs replaced by its HEAD version; other math sources were unchanged. This is a path-specific comparison, not a full old-workspace rerun. The original Shared SQL-error behavior is retained.
- The first unistore compile failed at two old String errors needing explicit SQL classification; these were fixed before the above tests. A compile failure is not passing evidence.

SQL uses 36 cells of analytical constants, five Real result columns plus SIGN Int metadata, and 12 direct zero-slot refusals. Existing expected values were not changed. Exact commands, results and exclusions: `evidence/raw-math-six-checkpoint.md`, `logs/raw-math-six-summary.txt`.

## Open work

Next: PI with true NoArgs, and four IP predicates with private NULL propagation and IPv4-only leading-zero normalization. They are not credited yet. Ordinary SQL may legally fold PI; tests must not force the optimizer to retain it.

Go/std trigonometric differences, packet-aware strings, SHA2, compression and ORD retain explicit compatibility gaps. No partial-domain credit. Broad operation-scope guards, release performance, allocator remeasurement, physical peak/OOM safety, paired differential reruns, full workspace and make lint remain unverified. Kernel reuse is not a claim of complete Go-package transcreation.
