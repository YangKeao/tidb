# Expression unification experiment

Current paired checkpoint **temporal-raw-storage-199**. Functional Demo: **240/245 (97.96%)**, strict **0**.

TiKV `NativeTemporalValue` now uniquely owns unchecked calendar bits plus independent kind/FSP metadata. TiDB's public `Time` is a thin shared-value wrapper while `CoreTime` remains its raw-bit facade. Three focused filters are GREEN. [Evidence](evidence/temporal-raw-storage-checkpoint.md) · [receipts](logs/temporal-raw-storage-summary.txt).

Broader M2 and M6 final acceptance remain active. PR readiness is not claimed.
