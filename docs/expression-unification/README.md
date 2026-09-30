# Expression unification experiment

Checkpoint-ID: `session-runtime-lifetime-05` (previous: `native-capability-value-04`)

This is a verified intermediate checkpoint, not a completed migration or PR-ready tree. Fully audited families remain0/245, target221. The user requested faster functional progress: compile and focused semantic checks first, deeper audits and performance follow-ups later; no native fallback or hidden test failures.

## Paired repositories

- [YangKeao/tidb, expression-unification-demo](https://github.com/YangKeao/tidb/tree/expression-unification-demo)
- [YangKeao/tikv, expression-unification-demo](https://github.com/YangKeao/tikv/tree/expression-unification-demo)

Keep sibling checkouts named `tidb` and `tikv` for Rust path dependencies. Checkpoint metadata records the paired TiKV commit. Both root Plans are byte-identical publication mirrors of `/home/agent/tidb/EXPRESSION_UNIFICATION_PLAN.md`. Each core-validated step pushes both branches without force-push or automatic PR creation.

## Current result

Session now accepts an explicit pool policy once, preserves a stable root, passes one execution through context construction/COW and distinguishes nested calls from detached results. Results hold captured close authority; old Close/Drop cannot close a new epoch or reset its lexical marker. The synchronous evaluator is an ordinary object, not a thread. SQL ASCII remains native in this checkpoint; the next activation and shared-kernel batch are separate work.

Actual tests: session lifecycle14 before/28 after; complete executor context23; named native error conversion1. Full expression1364 passed/4 unchanged baseline failures/94 ignored; renderer9 passed/1 unchanged baseline failure. Complete old failure bodies were compared after only thread-ID normalization. Exact commands, tested paths and limitations: `evidence/session-runtime-lifetime-checkpoint.md`; ownership contract: `evidence/session-runtime-lifecycle-contract.md`.

No release performance, whole-workspace or make lint acceptance yet. Additional lifecycle/fault cases and business-operation scope propagation remain follow-ups. The last independent192-byte caller allocation-request measurement is checkpoint04 (`logs/native-capability-arc-final/`); no fresh measurement is claimed for this checkpoint's binary. That old observation is not portable ABI, allocator peak, whole-pool heap or physical-OOM proof.

## Reproduction

The local experiment uses separate Jan2026 TiKV and Aug2026 TiDB compiler/target profiles. Commands and baseline history are in `evidence/validation-baseline.md` and checkpoint-specific evidence. Logs are actual selected receipts; absolute paths/artifact hashes identify local runs. Binaries and caches are not published. Do not count zero matched tests as passing or relink against an arbitrary artifact. No complete upstream Go-package transcreation claim follows.
