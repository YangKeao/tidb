# Expression unification experiment

Current paired checkpoint **window-param-marker-193**. Functional Demo: **240/245 (97.96%)**, strict **0**.

Prepared integer arguments for `NTH_VALUE`, `LEAD/LAG`, and `NTILE` now resolve through the existing statement `Columns::param_value` channel. A focused regression moved from retained RED to GREEN, the original Go table remains GREEN, and real `PREPARE NTILE(?)` SQL passes. [Evidence](evidence/wrapper-owner-matrix.md) · [receipt](logs/window-param-marker-summary.txt).

Planner null-rejection live-context threading remains open. Broader M2 and M6 final acceptance remain active. PR readiness is not claimed.
