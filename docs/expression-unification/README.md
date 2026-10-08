# Expression unification experiment

Current paired checkpoint **decimal-word-parts-196**. Functional Demo: **240/245 (97.96%)**, strict **0**.

TiKV now uniquely owns Decimal raw-word-to-coefficient projection, including inline SmallVec storage and negative-zero scale semantics. TiDB retains a thin concrete `Decimal` constructor and its TiDB-only JSON persistence schema. Five focused filters are GREEN. [Evidence](evidence/decimal-word-parts-checkpoint.md) · [receipts](logs/decimal-word-parts-summary.txt).

Broader M2 and M6 final acceptance remain active. PR readiness is not claimed.
