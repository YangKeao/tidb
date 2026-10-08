# Expression unification experiment

Current paired checkpoint **charset-name-classifier-197**. Functional Demo: **240/245 (97.96%)**, strict **0**.

TiKV FieldType `Charset` now uniquely owns canonical names, allocation-free case-insensitive classification, and `utf8mb3`; its wire API preserves exact-name behavior. TiDB retains only public-enum projection. Three focused filters are GREEN. [Evidence](evidence/charset-name-classifier-checkpoint.md) · [receipts](logs/charset-name-classifier-summary.txt).

Broader M2 and M6 final acceptance remain active. PR readiness is not claimed.
