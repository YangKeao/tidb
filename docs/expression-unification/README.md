# Expression unification experiment

Current paired checkpoint **wrapper-classification-192**. Functional Demo: **240/245 (97.96%)**, strict **0**.

Window runtime retains the live `StmtContext` for partition/order/range and function arguments; an appended zero-slot window SQL regression is GREEN. DDL defaults use live context, and aggregate literal metadata probes are intentionally ownerless. [Evidence](evidence/wrapper-owner-matrix.md) · [receipt](logs/wrapper-classification-summary.txt).

Open wrapper gaps are planner null-rejection context threading and prepared window integer arguments. Broader M2 and M6 final acceptance remain active. PR readiness is not claimed.
