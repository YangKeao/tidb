# Three logical families — logical-three-13

Functional delegation/native-deletion progress: 32/245; final acceptance: 0/245. AND, OR and XOR are three complete functional families on their existing admitted surfaces. The cut covers eight TiDB caller files plus one SQL-test file and five TiKV files, not completion of the overall unification target.

## Changes and demand contracts

The public `LogicalFunction`/`LogicalArgs` boundary distinguishes `Both` from `UndemandedRight`. The latter is valid only for AND with false left or OR with true left. Invalid `UndemandedRight` markers, including NULL-left and XOR cases, return ScopeContract before any factory attempt or dispatch. Its representative `Some(false)` is transport for an undemanded RHS, not the original RHS data or a claim that it was evaluated.

The AND/OR control-tag exception is confined to closed evaluated-ready Int2 recipes: strict signature, tag and retained-argument order `[0, 1]` checks precede emission of the prepared official eager FnCall. TiKV's existing row/wire AND/OR control path stays lazy. Results are computed by the kernel rather than returned as native constants, including short-circuit outcomes. The driver and factory limits are unchanged.

Native AST/helpers and legacy unistore retain eager evaluation and their original channels; typed/PB AND/OR retain lazy RHS demand. The ops path still runs both warning probes, ignores their Err results and then applies `truthy`, rather than adopting typed/PB `numeric_arg` error propagation. XOR gains no PB or unistore admission. BETWEEN's existing `logic_and` helper now also reaches the real kernel; this does not earn a separate BETWEEN family.

## Actual validation

Parent ran the pinned January TiKV and August TiDB wrappers. [Commands and exact result lines](../logs/logical-three-summary.txt) are retained separately.

- TiKV `local::`: 194 passed/1 ignored/468 filtered.
- TiKV Int2 kernel-identity/wrong-kind guard: 1 passed/662 filtered.
- TiDB `logical_dispatch_`: 3 passed/1485 filtered.
- Session lifecycle/SQL filter: 29 passed/2078 filtered.
- Unistore `legacy_logical_operators_keep_eager_rhs_and_null_channel`: 1 passed/184 filtered.
- Full expression suite: 1390 passed/4 existing failures/94 ignored; 1488 discovered, exit101. Parent compared all four complete failure blocks against hash-two-12: identical after only thread-ID normalization. The full suite remains non-green, with no new failures.

New SQL coverage checks all 27 cells of the three-valued truth tables and nine direct zero-slot refusals; typed signed-result metadata is covered. PB regression uses existing `PbBuiltin`/`from_pb` construction, not new wire-decode validation.

The old NOT BETWEEN test's instrumentation expectation changes from one facade entry to two (AND + NOT). Invocation snapshots from different workers are not subtracted. Its SQL Datum0 and Go-baseline result expectations stay unchanged; this is deliberately not a claim that all test expectations were unchanged.

## Review and not verified

Parent reviewed the TiKV compile/mod changes and native ops/core demand-marker diff. Documentation only maps entrypoints/counts; no new policy, Go, Bazel or PR-metadata changes are claimed.

Release performance, complete business-wrapper operation scopes/guards, allocator remeasurement, physical peak/OOM guarantees, full-workspace validation, make lint and final acceptance remain open. Targeted tests and previous allocation receipts do not establish these gates or PR readiness.
