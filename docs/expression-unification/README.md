# Expression unification experiment

Current paired checkpoint **null-rejection-context-194**. Functional Demo: **240/245 (97.96%)**, strict **0**.

Null-rejection proof now retains the caller's live `Columns` throughout recursive proof and nullified folding. Outer-to-inner and join simplification paths supply their existing statement/fold contexts; the context-free API remains a compatibility wrapper. A session-function regression moved from RED to GREEN and existing planner transformations remain GREEN. [Evidence](evidence/wrapper-owner-matrix.md) · [receipt](logs/null-rejection-context-summary.txt).

Known wrapper-context gaps are closed. Broader M2 and M6 final acceptance remain active. PR readiness is not claimed.
