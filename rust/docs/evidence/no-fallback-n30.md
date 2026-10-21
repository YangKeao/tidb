# LIKE and ILIKE keep absent declarations and original lazy demand

The existing wildcard value continuation now retains actual optional escape and
return FieldTypes. Absence stays None. Text and pattern still use the original
SQL byte conversion, and the escape uses the original cast with its actual
optional source declaration, including unsignedness. Declared returns use the
same bounded coercion; undeclared returns keep the actual result.

A separate capability admits these real SQL calls with absent declarations.
It selects their existing raw-value continuation rather than the eager value
body. Preparation charges the selected invocation, real optional field copies
and argument ordinals. The existing cache handles, derived collation,
constantness checks, context identity timing and lazy operand demand are
unchanged. No SQL value computation, original scalar dispatch, argument policy,
other host body, typed bulk guard or SDK code changed.

Independent original capture and rerun preceded production changes:
92160/FFB7855F523B1F05. Source admission refusal was reproduced with the same
digest, then all observations passed without fallback. The matrix covers both
names, five real argument shapes, all 16 declaration masks, three result field
families, eight value triples, callback faults, both flags, selected duplicates
and emptiness, output prefixes and repeated calls. It checks deferred writes,
NULL skipping invalid later arguments, arbitrary escape bits, malformed bytes,
Decimal, NaN and truncation behavior.

All 149 actual values were subsequently placed in each of the three operands:
42912/65B1761CB7A9C6A1. Both digests are unconditional. Direct protocol coverage
crosses Some/None returns with string, absent and unsigned escape declarations.
It retains token authentication, stale/foreign/wrong role/node/row/occurrence
rejection, cancellation and replay rejection, real cache history and context
identity changes, callback panic cleanup, resource bounds and same-provider
reuse. The source resource-error test also recovers on NULL using the same suite
with absent return and escape declarations.

Actual Projection covers three prepared parameters, deferred variable writes in
each operand, all declaration masks, NULL and malformed input, skipped first
rows, both flags and worker settings, exact errors, serial output prefixes,
warnings and real session variable state.

Validation:

- Frontend: 2034 passed, exactly the inherited 3 failures, 95 ignored;
  all 493 batch tests passed, warnings unchanged at 346 (131 frontend).
- Targeted wildcard tests: 13 passed.
- Projection: 33 passed. Expand: 6 passed, original 64/B42FB8BE316E6C79 unchanged.
- Root lint, both supported rustfmt versions, diff checks and ownership checker
  passed. SDK ad73b11 is clean and unchanged; its N16 baseline remains valid.
- The fixed admission inventory reports 1704 refusals across 25 names in 83088
  scenarios. This is a local inventory, not a global completion percentage.

Captures, source snapshots, SHA256 sums and the checker are in the owner's
evidence/no-fallback/n30-* workspace files. Generic fallback, other public
execution entries, remaining metadata/PB gaps, full server correctness and final
performance validation remain unfinished. No push or release.
