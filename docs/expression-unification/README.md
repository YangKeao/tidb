# Expression unification experiment

Current working checkpoint **lane-cache-only-runtime-220** (paired commits recorded after validation/push). Accelerated M0–M6 Demo: **COMPLETE**.

Functional TiKV-only ownership/native deletion is **240/245 (97.96%)**; strict per-family final-audit count remains **0** and five approved no-credit exceptions remain explicit. Those exceptions use one closed host adapter and are absent from the generic native family dispatcher; JSON schema validation alone retains a dedicated lazy cache adapter. The previously recorded TiDB lint, TiKV clippy, production-crate compilation and targeted semantic/lifecycle gates remain the accelerated-Demo evidence.

The standalone report records architecture, interfaces/glue, staged replacement, affected modules, compatibility, and frozen/current performance. Its round218 statement-pool measurements are retained as historical evidence. Round220 deletes that pool, its owner/execution/scope and slot/lease state machine, and makes each executor evaluation lane directly own an operation→worker `ReadyValueCache`; unbound calls use a stack-local one-shot cache.

The historical default ownerless AST/value microbenchmark regressed by **1.35×–66.33×** across six successful workloads; its explicit pooled candidate improved but remained **1.22×–37.50×** slower. Those numbers predate the lane-cache-only runtime and are not current performance claims. The recorded release MD5 Prepare/`InvalidSpecification` failure likewise remains unresolved, so `pr_ready=false` and no release-compatibility/performance claim is made.

[Standalone report](ARCHITECTURE_MIGRATION_AND_EVALUATION_REPORT.md) · [runtime-cleanup evidence](evidence/ready-value-statement-lifecycle-checkpoint.md) · [runtime-cleanup receipt](logs/round224-runtime-cleanup-summary.txt) · [performance receipt](logs/performance-before-after-summary.txt) · [worker construction receipt](logs/worker-prepare-probe-summary.txt) · [Host-adapter evidence](evidence/five-host-adapters-checkpoint.md) · [Final M0–M6 evidence](evidence/m0-m6-accelerated-complete-checkpoint.md).

This completion is scoped to the accelerated experiment. Exhaustive release/TiFlash/FIPS-environment validation, allocator peak/OOM thresholds, strict audit of every family, production server performance, and complete Go-package transcreation are not claimed.
