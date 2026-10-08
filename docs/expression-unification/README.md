# Expression unification experiment

Current paired checkpoint **strict-control-core-206**. Functional Demo: **240/245 (97.96%)**, strict **0**.

IF, IFNULL, COALESCE, NULLIF and CASE local demand plus real SQL roots are GREEN. CASE now has a 1,024-pair iterative stress gate. The official wire short-circuit builder still falls back to eager AND/OR beyond 32 nested levels; this is explicitly retained as an M3 deferral rather than counted as strict completion. [Evidence](evidence/strict-control-core-checkpoint.md) · [receipts](logs/strict-control-core-summary.txt).

Broader M2 is complete. M3/M6 final acceptance and PR readiness remain open.
