# Expression unification experiment

Current paired checkpoint **m6-full-clippy-213**. Functional Demo: **240/245 (97.96%)**, strict **0**.

Current TiDB `make lint`, full TiKV `make clippy`, changed production-crate compilation, Plan/count/link/remote/tracked-clean invariants, executor full lib **120/0/0**, aggregate full lib **40/0/0**, and focused production/deep-control gates are GREEN.

The compatible full-clippy environment uses GCC 14, a configure-only CMake 3.5 policy floor, and forced `cstdint` inclusion for old vendored native dependencies. Clippy also found and closed a real flate chain-budget bug with RED-timeout-to-GREEN regression evidence. [Evidence](evidence/m6-full-clippy-checkpoint.md) · [receipts](logs/m6-full-clippy-summary.txt).

Broader M2, planned Demo M3 and M6 are complete; functional M4/M5 coverage exceeds the frozen target with five approved no-credit exceptions. Overall completion awaits final invariant and independent readiness reruns; PR readiness remains outside this experiment and is not claimed.
