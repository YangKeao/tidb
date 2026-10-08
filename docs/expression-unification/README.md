# Expression unification experiment

Current paired checkpoint **core-time-fields-200**. Functional Demo: **240/245 (97.96%)**, strict **0**.

TiKV now uniquely owns all seven unchecked CoreTime field projections. TiDB deletes its remaining clock offset/mask algorithms and retains only the public `CoreTime` facade. Three focused filters are GREEN. [Evidence](evidence/core-time-fields-checkpoint.md) · [receipts](logs/core-time-fields-summary.txt).

Broader M2 and M6 final acceptance remain active. PR readiness is not claimed.
