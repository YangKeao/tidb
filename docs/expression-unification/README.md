# Expression unification experiment

Current paired checkpoint **m6-macro-abi-210**. Functional Demo: **240/245 (97.96%)**, strict **0**.

The downstream `rpn_fn` macro ABI is restored through a minimal `doc(hidden)` construction surface. The previously RED executor test target now compiles: executor full lib is **120/0/0**, aggregate full lib is **40/0/0**, and focused production/deep-control gates are GREEN. The original RED receipt remains retained. [Evidence](evidence/m6-macro-abi-checkpoint.md) · [receipts](logs/m6-macro-abi-summary.txt).

Broader M2 and planned Demo M3 are complete. M6 still requires final TiDB lint/toolchain reruns; full TiKV clippy remains blocked by the recorded grpcio/Abseil C++ incompatibility. Release/performance/exhaustive validation and PR readiness remain open.
