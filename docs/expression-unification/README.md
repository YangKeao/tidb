# Expression unification experiment

Current paired checkpoint **strict-wire-depth-209**. Functional Demo: **240/245 (97.96%)**, strict **0**.

All production TiKV DAG expression constructors now use the strict wire builder: eligible nested AND/OR remains lazy beyond depth 32 and runs through the iterative frame driver. The old capped builder remains a compatibility API/oracle only. Deep small-stack differential, legacy stress, production compilation, and TiDB 1,024-pair gates are GREEN. The executor test target's existing cross-crate test-macro visibility RED is retained and excluded. [Evidence](evidence/strict-wire-depth-checkpoint.md) · [receipts](logs/strict-wire-depth-summary.txt).

Broader M2 and the planned Demo M3 scope are complete. Strict full-family audit remains zero by contract. M6 final acceptance, broad toolchain/test-only visibility, release/performance and PR readiness remain open.
