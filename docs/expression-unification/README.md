# Expression unification experiment

Current paired checkpoint **duration-raw-parts-198**. Functional Demo: **240/245 (97.96%)**, strict **0**.

TiKV `NativeDurationParts` now uniquely owns raw `{nanoseconds, fsp}` duration storage and metadata access. TiDB's public `MySqlDuration` is a thin shared-parts wrapper, and its TIME limits reference SDK constants. Four focused filters are GREEN. [Evidence](evidence/duration-raw-parts-checkpoint.md) · [receipts](logs/duration-raw-parts-summary.txt).

Broader M2 and M6 final acceptance remain active. PR readiness is not claimed.
