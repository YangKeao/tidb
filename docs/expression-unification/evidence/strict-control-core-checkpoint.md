# Strict control core

`strict-control-core-206` verifies the staged TiKV report-driven selectors for IF, IFNULL, COALESCE, NULLIF, and CASE. A new 1,024-pair CASE stress gate proves iterative condition demand under one selected scope and exactly one selected value evaluation; it does not recurse through one worker cursor per condition.

Four local demand gates and four real session SQL roots are GREEN. The existing TiKV wire depth stress is also GREEN, but its oracle demonstrates a remaining incompatibility with M3: `ENABLE_SHORT_CIRCUIT_EXPRESSION` falls back to regular eager `FnCall` for nested AND/OR beyond 32 levels. This is evidence of an explicit deferral, not a pass toward strict completion.

Strict complete-family count therefore remains zero and M3 is not claimed complete. [Receipts](../logs/strict-control-core-summary.txt).
