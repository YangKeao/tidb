# Expression unification experiment

Current paired checkpoint **m0-m6-accelerated-complete-215**. Accelerated M0–M6 Demo: **COMPLETE**.

Functional TiKV-only ownership/native deletion is **240/245 (97.96%)**; strict per-family final-audit count remains **0** and five approved no-credit exceptions remain explicit. Current TiDB `make lint`, full TiKV `make clippy`, direct production-crate compilation, core semantic/lifecycle gates, M6 cost records, final invariants and two independent readiness reviews are GREEN/COMPLETE.

[Final evidence](evidence/m0-m6-accelerated-complete-checkpoint.md) · [summary](logs/m0-m6-accelerated-complete-summary.txt) · [M6 cost evidence](evidence/m6-cost-record-checkpoint.md).

This completion is scoped to the accelerated experiment. `pr_ready` remains false; exhaustive release/TiFlash/FIPS-environment validation, allocator peak/OOM thresholds, strict audit of every family, and complete Go-package transcreation are not claimed.
