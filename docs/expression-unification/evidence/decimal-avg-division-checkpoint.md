# Decimal AVG division

`decimal-avg-division-202` removes TiDB's finalization-only schoolbook decimal division. `Decimal::div_round` now constructs the positive COUNT divisor and delegates to shared MySQL decimal division, preserving visible and retained word precision; the local `digit_divmod` production body is deleted.

Compare, hash, codec, division-boundary and real session numeric aggregate filters are GREEN. A broader `tidb-exec --test all` filter remains compile-blocked by fifteen pre-existing `ConfiguredWritePlan` patterns lacking `warnings`; it is retained as a failure, not counted.

Broader M2 remains open for shared ownership of hash normalization, assignment fit/clamp, natural codec shape, and chunk fixed-cell fallback. [Receipts](../logs/decimal-avg-division-summary.txt).
