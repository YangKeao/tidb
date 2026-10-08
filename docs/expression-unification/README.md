# Expression unification experiment

Current paired checkpoint **five-host-adapters-216**. Accelerated M0–M6 Demo: **COMPLETE**.

Functional TiKV-only ownership/native deletion is **240/245 (97.96%)**; strict per-family final-audit count remains **0** and five approved no-credit exceptions remain explicit. Those exceptions now use one closed host adapter and are absent from the generic native family dispatcher; JSON schema validation alone retains a dedicated lazy cache adapter. Current TiDB `make lint`, full TiKV `make clippy`, direct production-crate compilation, core semantic/lifecycle gates, M6 cost records, final invariants and two independent readiness reviews are GREEN/COMPLETE.

[Host-adapter evidence](evidence/five-host-adapters-checkpoint.md) · [summary](logs/five-host-adapters-summary.txt) · [Final M0–M6 evidence](evidence/m0-m6-accelerated-complete-checkpoint.md) · [M6 cost evidence](evidence/m6-cost-record-checkpoint.md).

This completion is scoped to the accelerated experiment. `pr_ready` remains false; exhaustive release/TiFlash/FIPS-environment validation, allocator peak/OOM thresholds, strict audit of every family, and complete Go-package transcreation are not claimed.
