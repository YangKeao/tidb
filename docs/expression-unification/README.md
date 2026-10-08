# Expression unification experiment

Current paired checkpoint **m6-cost-record-214**. Functional Demo: **240/245 (97.96%)**, strict **0**.

Current TiDB `make lint`, full TiKV `make clippy`, direct production-crate compilation, Plan/count/link/remote/tracked-clean invariants, executor full lib **120/0/0**, aggregate full lib **40/0/0**, and focused production/deep-control gates are GREEN.

M6 now records the isolated direct-dependency compile cost, logical owned transport/retained bytes, width-one execution cost, and batch-safe 0/1/1,024/1,025 scope. These are diagnostics without a threshold or improvement claim. [Cost evidence](evidence/m6-cost-record-checkpoint.md) · [receipts](logs/m6-cost-record-summary.txt).

Broader M2, planned Demo M3 and M6 are complete; functional M4/M5 coverage exceeds the frozen target with five approved no-credit exceptions. Overall completion awaits the final current invariant and cost-aware independent readiness reruns; PR readiness remains outside this experiment and is not claimed.
