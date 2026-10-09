# Repair the audited expression SQL compatibility failures

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at the TiDB repository root; this plan is maintained according to it.

## Purpose / Big Picture

The Rust TiDB server currently rejects several ordinary expressions with `ERROR 1105 (HY000): Expression runtime specification failure`, even when all arguments are constants. After this work, the highest-impact audited families should execute with the same rows, errors, and warnings as Go TiDB against the same real PD/TiKV data. Every fix must have a focused regression that fails before the fix and passes after it. Independent defects that cannot be safely fixed in this pass remain documented with an exact reproducer rather than being hidden by fallback.

## Progress

- [x] (2026-10-09 06:42Z) Claimed Raft task #2 and acknowledged the correctness-first scope.
- [x] (2026-10-09 06:45Z) Read repository policies, testing flow, and nearest-test placement guidance.
- [x] (2026-10-09 07:03Z) Reproduced the release-only 1105 family in a table-driven prepare test: `IsNull` failed before the fix because Rust function-pointer addresses differed across optimized codegen units.
- [x] (2026-10-09 07:07Z) Removed function addresses from the private kernel identity checks while retaining operation, canonical function name, types, arity, and typed metadata; the release regression then passed for all 18 audited operations.
- [x] (2026-10-09 07:28Z) Diagnosed and fixed qualified JOIN ordering: select-field reuse compared only the last column name, so `ORDER BY u.id` reused `t.id`; a focused descending-order regression failed before and passed after the qualifier-aware fix.
- [x] (2026-10-09 07:43Z) Fixed invalid-day `STR_TO_DATE` warning payloads by retaining and rendering the parsed invalid date instead of reporting zero datetime.
- [x] (2026-10-09 08:01Z) Ran focused regressions, file-scoped rustfmt checks, TiDB `make lint`, targeted TiKV clippy, release server builds, and three successive real PD/TiKV differentials.
- [x] (2026-10-09 08:03Z) Self-reviewed both repository diffs and recorded the sole residual semantic disagreement (`REGEXP_INSTR` on empty input) as a Go vector/scalar inconsistency rather than regressing Rust to the erroneous vector behavior.

## Surprises & Discoveries

- Observation: The audit failures span null predicates, string/byte operations, bit operations, network functions, and hashes, yet all collapse to the same public InvalidSpecification class. This suggests one or a small number of transport/shape contract defects rather than independent algorithm failures.
  Evidence: `artifacts/expression-unification-e2e-final2-20261009/summary.tsv` in the agent workspace; constant-only probes fail for every affected family.
- Observation: Rust function-pointer equality is not a valid stable identity across optimized codegen units. The common compiler had already selected a checked `FunctionRef`; re-reading the same function through a second codegen unit could yield a different address and reject the legal recipe only in release builds.
  Evidence: the new `audited_sql_operations_prepare_in_optimized_builds` test failed before the fix at `IsNull` with `InvalidSpec("evaluated Bytes preparation changed its selected arity/canonical/unit-metadata kernel")` and passed after removing only the address comparison.
- Observation: All six JOIN output differences were one planner bug, not join execution bugs. `find_in_select_fields` matched `t.id` and `u.id` by their last component only, so a qualified `ORDER BY u.id` bound to the first projected `id`.
  Evidence: pre-fix `EXPLAIN` rendered only `Sort ... expr_audit.t.id` even for `ORDER BY u.id`; the descending duplicate-name regression failed before and passed after qualifier-aware matching.
- Observation: `REGEXP_INSTR('', 'a', 1, ...)` is inconsistent inside Go TiDB itself. The current vector SQL path returns error 1139 because it rejects position 1 when the rune count is zero, while existing scalar tests explicitly accept the empty string (including `'^$'` returning 1). Rust returns the scalar/intended result and should not be changed to reproduce that vector-only defect.
  Evidence: Go `pkg/expression/builtin_regexp.go` performs the vector range check; Go `builtin_regexp_test.go` contains the empty-string scalar expectations. The final differential's two remaining names, `regexp_core` and `regex_instr`, are the same input and failure.

## Decision Log

- Decision: Repair correctness before making any performance changes.
  Rationale: Performance refactors would obscure failure attribution and could accidentally create a fallback path, while the user asked to address the poor compatibility first.
  Date/Author: 2026-10-09 / TiDBEngineer.
- Decision: Extend the nearest existing Rust tests instead of adding a new Go top-level test or integration fixture unless SQL-only behavior cannot be reached otherwise.
  Rationale: This keeps the diff small and avoids unnecessary Bazel regeneration; the final real-cluster harness still provides SQL-level acceptance.
  Date/Author: 2026-10-09 / TiDBEngineer.
- Decision: Treat the closed operation plus canonical name, SQL types, arity, and typed metadata as the private prepared-kernel identity; do not compare Rust function addresses.
  Rationale: The program is constructed only by the checked common compiler, its fields are private, and every stable semantic dimension remains checked. Function addresses add release-only false negatives without authenticating anything the compiler contract does not already own.
  Date/Author: 2026-10-09 / TiDBEngineer.
- Decision: Leave the `REGEXP_INSTR` empty-string behavior unchanged in Rust.
  Rationale: Rust agrees with Go's scalar tests and the natural position-1-on-empty convention; copying the current Go vector error would knowingly introduce a semantic regression. The proper follow-up is to reconcile Go's vector implementation with its scalar contract.
  Date/Author: 2026-10-09 / TiDBEngineer.

## Outcomes & Retrospective

The release differential improved from 34/75 exact cases to 73/75. Every audited 1105 preparation failure is gone: null predicates, null-safe equality, byte/string length, replace/hex/unhex, bit operations, network conversion, MD5/SHA1, and expression use in JOIN conditions now execute normally. Qualified duplicate-name JOIN ordering and the invalid-day `STR_TO_DATE` warning payload also match Go.

The two non-exact case names are duplicate coverage of one residual `REGEXP_INSTR` empty-string disagreement. Rust follows Go's scalar tests; current Go vector execution raises 1139. No release blocker remains in Rust for that case, but Go should reconcile those two paths.

Validation evidence:

* Release prepare regression: `audited_sql_operations_prepare_in_optimized_builds` — 1 passed.
* Qualified JOIN-order regression: `join_order_by_duplicate_column_names` — 1 passed.
* Native temporal-warning regression: `ordinary_stages_preserve_unpacked_day_warning_choice_and_late_mode_demand` — 1 passed.
* File-scoped rustfmt checks passed in both repositories; TiDB `make lint` passed; targeted `cargo clippy -p tidb_query_expr --all-targets` completed with only pre-existing workspace warnings.
* Final real-cluster output: `/home/agent/.slock/agents/1555103c-9f3e-46ca-ac2e-b964c650c2f1/artifacts/expression-unification-e2e-fix3-20261009/summary.tsv` (73 exact, 2 copies of the known regexp disagreement).

## Context and Orientation

The relevant TiDB repository is `/home/agent/tidb/expression-unification/tidb` at branch `expression-unification-demo`; its paired TiKV checkout is `/home/agent/tidb/expression-unification/tikv`. TiDB's Rust expression runtime lives in `rust/crates/tidb-expr`. Operations are lowered to TiKV-local prepared workers in `src/tikv/ready_value.rs`; TiKV validates and evaluates those operations under `components/tidb_query_expr/src/local` and `src/types/expr_eval.rs`. `InvalidSpecification` means a private TiKV local-expression contract rejected the supplied operation/program/metadata; `src/tikv/runtime_failure.rs` intentionally redacts its private reason at the SQL boundary.

The independent audit harness is `/home/agent/.slock/agents/1555103c-9f3e-46ca-ac2e-b964c650c2f1/expression-unification-e2e-audit.sh`. Its final baseline evidence is under the sibling `artifacts/expression-unification-e2e-final2-20261009` directory. It starts one PD/TiKV, provisions data through Go TiDB, then runs the same statements through the Rust server.

The TiDB worktree already contains one unrelated untracked file, `rust/third_party/tikv-client-rs/tests/client_go_differential/BUILD.bazel`. Preserve it and do not include or alter it.

## Plan of Work

First, trace one constant failure (`LENGTH('abc')`) and one presence-only failure (`NULL IS NULL`) through `EvaluatedBytesOp`, worker preparation, evaluated-argument matching, expression shape validation, and output materialization. Add focused tests beside existing `ready_value`/local-expression tests that expose the private rejection reason and prove the pre-fix failure.

Second, determine whether the other failing families share that contract defect. Fix the contract at the narrowest owning boundary, preserving fail-closed behavior for genuinely invalid operation/arity/carrier combinations. Extend a table-driven regression over every operation repaired by the same change. Do not introduce native replay.

Third, diagnose the non-1105 defects independently. For join ordering, compare plan/executor ordering metadata and output chunk construction. For `REGEXP_INSTR`, compare error propagation and empty-string/position handling with Go semantics. For invalid temporal warnings, locate the point where the original invalid input is replaced by a zero datetime. Only make changes whose ownership and regression contract are clear in this pass; retain exact reproducers for the rest.

Finally, run the smallest unit tests proving each modified boundary, then rebuild the Rust server and rerun the SQL differential against real PD/TiKV. Review all product diffs and run formatting/lint required by the modified Rust scope.

## Concrete Steps

Work from `/home/agent/tidb/expression-unification/tidb` unless a command explicitly names TiKV.

Inspect the failing paths and nearest tests:

    rg -n "InvalidSpec|evaluated_bytes_shape|EvaluatedBytesOp::Length|EvaluatedBytesOp::IsNull" rust/crates/tidb-expr/src ../tikv/components/tidb_query_expr/src
    rg --files rust/crates/tidb-expr -g '*test*.rs'

Run the focused Rust regressions using the repository's pinned/offline build environment already used by this project. Record exact environment and test selectors in this plan once the owning test modules are selected.

Rebuild the release Rust server without default features, because the default feature set has a known allocator conflict:

    cargo build --manifest-path rust/Cargo.toml -p tidb-server --release --no-default-features

Run the real-cluster differential from the agent workspace with a new output directory. Expect every repaired atomic case to have identical exit status, stdout, and stderr; compare the new `summary.tsv` with the retained baseline.

## Validation and Acceptance

For every source fix, a focused regression must be observed failing before the change and passing after it. Repaired constant-only and column-driven SQL cases must both match Go TiDB. No case may be made green through native fallback or by weakening invalid-spec validation globally.

At minimum, acceptance includes the atomic cases for `IS NULL`, `<=>`, byte/string length, replace/hex, bit operations, network conversion, MD5/SHA1, and any non-1105 defect changed in this pass. Existing known-good arithmetic, lazy control flow, warning, aggregate, and window cases must remain exact in the final differential.

The task is complete only after targeted unit tests, formatting/lint for the changed scope, a rebuilt release server, and a cleaned real PD/TiKV run. Any omitted broad validation must be explicitly reported.

## Idempotence and Recovery

All inspection, unit-test, build, and differential commands are safe to rerun. The e2e harness creates a distinct artifact directory and installs cleanup traps for child processes. If interrupted, verify ports 2379, 20160, 4400, and 4401 are unused before retrying. Never reset or delete the existing unrelated untracked BUILD file. Use normal Git diffs to isolate and revert only edits made by this task if a prototype is discarded.

## Artifacts and Notes

Baseline audit report: `/home/agent/.slock/agents/1555103c-9f3e-46ca-ac2e-b964c650c2f1/expression-unification-audit-20261009.md`.

Baseline summary hash:

    f695668b21b9b5613ee3946f9d819cfe7fa76846e819456f742624333fe7ee2a  summary.tsv

Final summary hash:

    10a589fd9f9a87c51dc7796948fe27a8c1d77212f73d14a30e088ff8d8b0f9f8  summary.tsv

## Interfaces and Dependencies

Preserve `tidb_expr::tikv::EvaluatedBytesOp` as the operation identifier and TiKV's local prepared-worker boundary. Any contract change must keep arity, carrier, SQL type, and error/warning behavior explicit. The Rust server must continue to fail closed when an operation cannot be represented; neither Go nor Rust native expression replay may be added.

Revision note: initial plan created on 2026-10-09 from the completed SQL/performance audit and repository policy.
