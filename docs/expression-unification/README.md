# Expression unification experiment

Current paired checkpoint **decimal-shared-storage-201**. Functional Demo: **240/245 (97.96%)**, strict **0**.

TiKV `NativeDecimalParseValue` now uniquely owns Decimal sign, inline coefficient, visible/storage scales, and declared shape. TiDB's public `Decimal` deletes those five fields and wraps the shared value while retaining TiDB-only JSON/error adapters. Six focused filters are GREEN. [Evidence](evidence/decimal-shared-storage-checkpoint.md) · [receipts](logs/decimal-shared-storage-summary.txt).

Broader M2 and M6 final acceptance remain active. PR readiness is not claimed.
