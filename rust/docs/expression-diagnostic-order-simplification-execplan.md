# Simplify expression unification without diagnostic ordering

This ExecPlan is a living document maintained according to repository-root `PLANS.md`. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` current.

## Purpose / Big Picture

The expression-unification prototype used a second diagnostic-provenance system to remember the exact input or arithmetic node that failed and to reconstruct ordered warning/error observations. The product path immediately discarded that provenance and returned the original local error. After this change there is one evaluation path: it returns the original `LocalError`, leaves warnings in `EvalContext`, and does not promise diagnostic order or source ordinal. SQL values, warning/error content, NULL behavior, short-circuit demand, and observable host/user-variable/sequence effects remain unchanged.

The simplification spans TiDB at `/home/agent/tidb/expression-unification/tidb` and its path dependency TiKV at `/home/agent/tidb/expression-unification/tikv`. It is observable in the deletion of the reported-failure facade and in focused tests that compare raw error kinds and warning membership rather than diagnostic positions.

## Progress

- [x] (2026-10-09 08:08Z) Claimed Raft task #3 and mapped warning/error ordering machinery across TiDB and TiKV.
- [x] (2026-10-09 08:12Z) Confirmed TiDB's `ordinary_diagnostics` layer was private prototype/test-only, while production numeric batch converted TiKV's reported failure back to the original raw error.
- [x] (2026-10-09 08:31Z) Removed TiDB's unused PLUS diagnostic renderer/endpoints and source-site tests.
- [x] (2026-10-09 08:36Z) Routed numeric batch directly through raw `LocalError`, removing TiDB's reported-failure wrapper and provenance-only assertions.
- [x] (2026-10-09 08:49Z) Removed TiKV's failure recorder/reported facade and its optional recorder plumbing, retaining one ordinary evaluator.
- [x] (2026-10-09 09:01Z) Changed retained warning assertions to compare content/presence without relying on diagnostic order; retained ordered callback assertions only where they specify evaluation demand.
- [x] (2026-10-09 09:07Z) Passed TiDB numeric-batch tests (18), TiKV numeric-batch tests (28), TiKV lineage tests (23), TiDB `make lint`, and TiKV all-target clippy with pre-existing warnings.
- [x] (2026-10-09 09:27Z) Built release TiKV and TiDB (`--no-default-features`), then reran the real PD/TiKV differential: 73/75 exact, with only the two known duplicate `REGEXP_INSTR` cases differing.

## Surprises & Discoveries

- Observation: the TiDB diagnostic renderer was 1,520 lines of private implementation/tests with no production consumer.
  Evidence: `ordinary_diagnostics.rs` and `ordinary_diagnostics_tests.rs` were reachable only from `tikv/ordinary.rs` and their own tests.
- Observation: TiKV's 162-line failure type caused recorder parameters to be threaded through binding, row, frame, leaf, input, and kernel functions, plus 1,016 lines of dedicated tests.
  Evidence: deleting `local/diagnostic.rs`, `local/diagnostic_tests.rs`, and the recorder plumbing removes more than 1,500 TiKV lines while focused behavior tests remain green.
- Observation: the full TiKV library suite has six existing failures unrelated to this change. Five assert function-address kernel identity that the preceding compatibility fix intentionally removed; one expects the old `STR_TO_DATE` warning payload that the preceding fix corrected. The changed numeric/lineage cohorts pass independently.
  Evidence: full serial result was 1,019 passed, 6 failed, 1 ignored; none of the six failing test bodies or their ready-value/`STR_TO_DATE` implementations are changed by this plan.
- Observation: TiDB's default release feature still conflicts with TiKV's global allocator.
  Evidence: the default build reports `#[global_allocator] ... conflicts with ... tikv_alloc`; the established server build uses `--no-default-features`.

## Decision Log

- Decision: diagnostic order, exact failing source ordinal, and reconstructed native overflow text are not compatibility requirements.
  Rationale: the user explicitly removed ordering from the contract, and the active caller discarded the provenance before producing its product error.
  Date/Author: 2026-10-09 / TiDBEngineer.
- Decision: preserve evaluation demand and non-diagnostic side-effect order.
  Rationale: warning/error ordering can be relaxed without changing IF/CASE/COALESCE demand, host callbacks, user variables, sequences, or input reads.
  Date/Author: 2026-10-09 / TiDBEngineer.
- Decision: delete the compatibility facade rather than leave a raw method plus a reported alias.
  Rationale: one evaluator and one error type materially reduce branches, metadata, parameters, and test duplication.
  Date/Author: 2026-10-09 / TiDBEngineer.
- Decision: keep `OrdinaryCallSite` compile facts.
  Rationale: those facts authenticate the admitted profile and source tree; they are not the removed runtime diagnostic-order receipt.
  Date/Author: 2026-10-09 / TiDBEngineer.

## Milestones

Milestone 1 removes TiDB-only reconstruction. Delete `rust/crates/tidb-expr/src/tikv/ordinary_diagnostics.rs` and its tests, remove the module declaration, and simplify `evaluator/numeric_batch.rs` so its failure enum carries `LocalError` directly. Acceptance is the 18-test numeric-batch cohort passing.

Milestone 2 removes TiKV runtime provenance. Delete `components/tidb_query_expr/src/local/diagnostic.rs` and its tests; remove `FailureRecorder`, `ReportedLocalFailure`, the reported public methods, and every optional recorder parameter from `local/batch.rs` and `types/expr_eval.rs`. Convert retained tests to raw error kinds and unordered warning membership. Acceptance is the numeric-batch and lineage cohorts passing.

Milestone 3 validates the integrated product. From TiKV run the focused tests and clippy with `RUSTC_BOOTSTRAP=1`, GCC/G++ 14, `CXXFLAGS='-include cstdint'`, and `CMAKE_POLICY_VERSION_MINIMUM=3.5`. From TiDB run the focused `tidb-expr` tests and `make lint`. Build TiKV release and TiDB release with `--no-default-features`, then run `expression-unification-e2e-audit.sh` against real PD/TiKV. Acceptance is the retained 73/75 exact differential, with only the known duplicate `REGEXP_INSTR` disagreement.

## Validation

The focused commands are:

    cargo test -p tidb_query_expr local::profile_tests::numeric_batch --lib
    cargo test -p tidb_query_expr local::lineage_tests --lib
    cargo clippy -p tidb_query_expr --all-targets
    cargo test --manifest-path rust/Cargo.toml -p tidb-expr evaluator::numeric_batch::tests --lib
    make lint

All Cargo commands use the toolchain environment described in Milestone 3. File-scoped `rustfmt --edition 2021` and `git diff --check` must pass in both repositories. Historical evidence documents are receipts and are not rewritten; `docs/agents/architecture-index.md` is updated because it describes current source.

## Outcomes & Retrospective

Implementation is complete and has removed the duplicate diagnostic path while retaining one raw evaluator. Excluding this new 70-line execution plan, the source/test change adds 188 lines and deletes 3,266, a net reduction of 3,078 lines; the complete staged change is a net reduction of 3,008 lines. File-scoped rustfmt and `git diff --check` passed in both repositories; the TiDB 18-test numeric cohort, TiKV numeric cohort, TiKV 23-test lineage cohort, TiDB lint, TiKV all-target clippy, and both release builds passed. The real PD/TiKV audit retained 73/75 exact results; only the two duplicate `REGEXP_INSTR` cases differ, and `summary.tsv` has the same SHA-256 (`10a589fd9f9a87c51dc7796948fe27a8c1d77212f73d14a30e088ff8d8b0f9f8`) as the preceding compatibility baseline. The full TiKV library suite remains at its pre-existing 1,019 passed, 6 failed, 1 ignored state described above. The TiKV change is commit `608bbfc`; the TiDB change is the commit containing this plan.
