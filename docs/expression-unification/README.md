# Expression unification experiment

Current paired checkpoint **decimal-assignment-fit-204**. Functional Demo: **240/245 (97.96%)**, strict **0**.

TiKV now owns Decimal assignment round-first fitting and signed maximum clamping. TiDB deletes double rounding and maximum-text reparse while retaining concrete diagnostics and declared-shape stamping. Four focused gates are GREEN. [Evidence](evidence/decimal-assignment-fit-checkpoint.md) · [receipts](logs/decimal-assignment-fit-summary.txt).

Broader M2 has one identified Decimal blocker left: chunk fixed-cell fallback. M6 final acceptance and PR readiness are not claimed.
