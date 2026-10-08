# Expression unification experiment

Current paired checkpoint **architecture-performance-report-217**. Accelerated M0–M6 Demo: **COMPLETE**.

Functional TiKV-only ownership/native deletion is **240/245 (97.96%)**; strict per-family final-audit count remains **0** and five approved no-credit exceptions remain explicit. Those exceptions use one closed host adapter and are absent from the generic native family dispatcher; JSON schema validation alone retains a dedicated lazy cache adapter. The previously recorded TiDB lint, TiKV clippy, production-crate compilation and targeted semantic/lifecycle gates remain the accelerated-Demo evidence.

The new standalone report records architecture, interfaces/glue, staged replacement, affected modules, compatibility, and frozen/current performance. The default ownerless AST/value microbenchmark regresses by **1.35×–66.33×** across six successful workloads; explicit pooled execution improves but remains **1.22×–37.50×** slower. The current release MD5 path fails at Prepare/`InvalidSpecification` although the same named debug test passes. These findings do not revoke the scoped functional Demo checkpoint, but they reinforce `pr_ready=false` and block a release-compatibility/performance claim.

[Standalone report](ARCHITECTURE_MIGRATION_AND_EVALUATION_REPORT.md) · [report evidence](evidence/architecture-performance-report-checkpoint.md) · [performance receipt](logs/performance-before-after-summary.txt) · [Host-adapter evidence](evidence/five-host-adapters-checkpoint.md) · [Final M0–M6 evidence](evidence/m0-m6-accelerated-complete-checkpoint.md).

This completion is scoped to the accelerated experiment. Exhaustive release/TiFlash/FIPS-environment validation, allocator peak/OOM thresholds, strict audit of every family, production server performance, and complete Go-package transcreation are not claimed.
