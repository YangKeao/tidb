# Expression unification experiment

Current paired checkpoint **decimal-chunk-projection-205**. Functional Demo: **240/245 (97.96%)**, strict **0**.

TiKV now directly projects Decimal coefficients into fixed nine-word chunk cells, preserving Go-compatible integer overflow, fraction truncation, visible scale and signed-zero behavior. TiDB's SQL-text fallback is deleted. Five focused gates are GREEN. [Evidence](evidence/decimal-chunk-projection-checkpoint.md) · [receipts](logs/decimal-chunk-projection-summary.txt).

Broader M2 is complete for the planned Demo scope. Strict family audit, M3/M6 final acceptance, and PR readiness remain open.
