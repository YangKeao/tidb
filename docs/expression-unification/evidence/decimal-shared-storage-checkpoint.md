# Decimal shared value storage

`decimal-shared-storage-201` makes TiKV `NativeDecimalParseValue` the sole owner of Decimal sign, inline coefficient, visible scale, retained storage scale, and declared shape. It supplies raw transport, const metadata accessors, shape mutation, and exact inline-state inspection.

TiDB `Decimal` deletes all five fields and becomes a public shared-value wrapper. `DecimalDigits` remains only a local construction/math helper; TiDB-only JSON persistence, concrete errors, display and public arithmetic APIs remain adapters.

Six focused filters are GREEN. Two transient compile refusals during integration—const `SmallVec` deref and stale codec field access—were corrected before the recorded gates. [Receipts](../logs/decimal-shared-storage-summary.txt).
