# CONCAT batch publication keeps original row demands

Declared string CONCAT/CONCAT_WS calls with flat, genuinely declared string
parameters, literals and ordinary columns can now use the shared source. The
eligibility exception requires a real parameter marker, the original registered
bytes result signature, valid arity, and no PB origin, deferred expression,
virtual/correlated column, VALUES offset or grouping metadata. It reads only
borrowed metadata. Other contextual typed-root guards remain in place.

The original batch dispatcher evaluates these calls per row, interleaving
parameter reads and packet callbacks. It collects the complete invocation before
publishing output when batching is enabled. The implementation preserves those
row demands and extends the existing CAST_CHAR publication policy to this closed
template. It introduces no typed-once argument preparation, new driver, temporary
expression, default declaration, Ready worker or value computation. The initial
typed-once prototype failed parameter-count comparison and was discarded.

Independent original capture and rerun, followed by admission refusal checks,
preceded the production change. The same fixed observations now pass without
fallback, comparing actual values, exact outcomes, output prefixes, parameter
read counts and callback effects:

| Scope | Observations | Original digest |
| --- | ---: | --- |
| Three parameter/column/literal shapes and callback profiles | 19440 | 3CE163458ACA2747 |
| All 149 actual values at each argument position | 32184 | E1B29ADC97CAEEC9 |
| Arity 1 | 3240 | 9B21916B002CA553 |
| Arity 2 | 6480 | 8947CBC47EC5D18D |
| Arity 5 | 6480 | 9746E47C0EBFF03F |
| Arity 6 | 6480 | 3B7D87B0EA8F1545 |
| Arity 9 | 6480 | 7BA6E0F9E0044141 |

These 80784 observations cover both batch flags, ordinary/duplicate/empty
selections, packet limits, repeated markers in distinct nodes, changed values on
subsequent reads, parameter errors, callback errors/panic and same-context reuse.
The arity refusal wrapper stopped at its first refused arity; the later green
wrapper exercises all five arities, respecting CONCAT_WS minimum arity.

An additional 320-observation original witness, captured and independently
rerun after implementation, pins 11E1A2590FCF4497. It compares 0/1/3/1026/2051
selected rows, late parameter failure at read 1026, packet callback panic at call
3073, repeated calls after failure and alternating flags on the same owner and
context. No output normalization or fallback is used.

Actual Projection covers genuinely vectorizable pure-parameter calls with
serial and parallel workers, 0/3/2051 rows, both flags, NULL, invalid bytes,
unsigned actual values and packet overflow. Complete output columns and warnings
match the original suite. Production Projection code is unchanged.

Final validation: frontend 2047 passed, exactly the inherited 3 failures,
95 ignored, all 506 batch checks passed and warnings remained 346. Projection
35, Expand 6 with original 64/B42FB8BE316E6C79, lint, dual rustfmt and the
ownership checker passed. The preceding 221184/EE3A7F821B85B619 witness is
unchanged; its remaining 1152 guarded observations now run without fallback.
The fixed 83088-scenario inventory has 1352 refusals across 23 names; this is
a local count, not a global completion percentage. Source snapshots, original records,
failed prototype, final logs and ownership checker live in the owner's
evidence/no-fallback/n32-* files. SDK, original SQL computations, argument policy,
all existing witnesses, hosts, caches and driver code remain unchanged. Other
contextual batch shapes, optional metadata/PB gaps, public entries, generic
fallback removal, server correctness and final performance validation remain
unfinished. No push or release.
