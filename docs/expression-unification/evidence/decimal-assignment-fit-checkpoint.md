# Decimal assignment fitting

`decimal-assignment-fit-204` moves the complete `DECIMAL(M,D)` value policy to TiKV `NativeDecimalParseValue::fit_precision_scale`: round first, validate the integer budget, ignore insignificant leading zeros, and clamp overflow to the signed maximum. Invalid `M < D` remains explicit.

TiDB's public fit method is a thin projection. Datum conversion deletes double rounding, maximum-text formatting/reparse, and its local fit decision; it retains concrete diagnostics, events, unsigned policy, and declared-shape stamping.

Four focused gates are GREEN. The initial SDK test itself used a nonexistent accessor and failed compilation; the corrected rerun is GREEN and no semantic test failed. [Receipts](../logs/decimal-assignment-fit-summary.txt).

Broader M2 now has one identified Decimal blocker: chunk fixed-cell fallback.
