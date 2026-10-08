# M6 accelerated final gates

`m6-final-gates-211` closes the validation/reporting scope required by the accelerated Demo.

The current TiDB tree passes the repository-mandated `make lint`. The changed TiKV production crates compile together with locked dependencies; their full executor and aggregate libraries passed 120 and 40 tests at the preceding checkpoint. The final invariant audit verifies all three Plan copies, checkpoint SHA, 240/245 functional coverage, zero strict-final claims, five explicit no-credit deferrals, relative evidence links, pushed remote heads, and tracked cleanliness.

## Acceptance

- Functional migration/native deletion: **240/245 (97.96%)**, above the frozen 90% target.
- Planned M2 and M3 Demo scopes: complete.
- Five remaining families: explicit approved no-credit deferrals with future ownership conditions.
- Strict complete-family audit: deliberately **0**; no stricter claim is made.
- TiDB lint and core production compile/lifecycle/SQL gates: GREEN.
- Duplicate algorithm owners for credited families: deleted or reduced to documented adapters; no feature-off native backend is accepted.

## Deferred, not passed

The user-directed accelerated policy makes exhaustive differential/release/TiFlash sweeps, allocator peak/OOM measurement and performance thresholds non-blocking. No performance improvement or allocation threshold is claimed. Width-one local execution and owned transport remain bounded by the documented execution budgets; deep wire controls were exercised at 33/256 levels and local CASE at 1,024 pairs, but these are correctness gates rather than benchmarks.

Full TiKV `make clippy` remains blocked after its non-build policy gates by the retained grpcio-sys vendored-Abseil/current-C++ incompatibility. Historical broad `tidb-expr` and unistore failures remain disclosed. Independent final review therefore leaves the canonical goal **ACTIVE** until final performance/workspace gates are closed or explicitly waived; the functional Demo is deliverable, but neither M6 completion nor PR readiness is claimed. [Receipts](../logs/m6-final-gates-summary.txt).
