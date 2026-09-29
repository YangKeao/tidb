# TiDB Architecture Index for Agents

This file is a navigation index for quick subsystem discovery.
Hard requirements remain in the repository root `AGENTS.md`.

## How to Use This Index
1. Map the task to one primary subsystem.
2. Find existing tests covering the same behavior.
3. Reuse existing fixtures/testdata before creating new shapes.
4. Expand to adjacent subsystems only when cross-module effects are clear.

## Core Subsystems

### Planner and optimization
- `pkg/planner/`
- `pkg/planner/core/base/`
- `pkg/planner/core/operator/logicalop/`
- `pkg/planner/core/operator/physicalop/`
- Typical changes: rule matching, plan shape, cost-based choices.
- First tests to inspect:
  - `pkg/planner/core/casetest/`
  - `pkg/planner/core/casetest/rule/testdata/`

### Execution and expressions
- `pkg/executor/`
- `pkg/expression/`
- Typical changes: runtime operator semantics, builtin behavior, evaluation edge cases.
- First tests to inspect:
  - package unit tests under the same path
  - SQL integration tests for user-visible behavior

### Rust shared-expression experiment
- `rust/crates/tidb-datatype/src/collation.rs` retains the SQL-facing collation facade; migrated General/UCA key and LIKE kernels are owned by the sibling TiKV checkout's `components/tidb_query_datatype/src/codec/collation/`.
- `rust/crates/tidb-datatype/src/tikv_compat/` is the checked shared-value boundary. Complete SQL `FieldType` metadata remains TiDB-owned; the initial bridge admits Int/Real/Bytes and NULL, not full Decimal/temporal/JSON unification.
- `rust/crates/tidb-expr/src/like.rs` and `rust/crates/tidb-util/src/stringutil.rs` delegate matching to that shared kernel. TiDB's general expression evaluator has not yet been replaced by this foundation checkpoint.
- Private `rust/crates/tidb-expr/src/tikv/` contains an explicit, crate-internal signed-BIGINT leaf/control seed: `lower.rs` retains SQL/PB metadata, `context.rs` imports demanded physical row occurrences, and `batch.rs` owns each worker's official TiKV RPN program. It is not a global evaluator hook or native fallback. `distsql_builtin.rs` retains shallow wire provenance before conversion loses presence/signed-ID information.
- `rust/crates/tidb-datatype/src/field_type/memory.rs` provides `checked_snapshot_payload_bytes` for pre-copy logical metadata budgeting. It borrows visible elements, includes the independent private binary-marker length and checks arithmetic without allocating snapshots. It does not measure allocator capacity or make observation/copy atomic: the owner keeps aliases stable during that interval. FieldType equality/hash can allocate element snapshots, so new bounded adapters apply this byte gate first. Its eight inline tests use `cargo test --locked -p tidb-datatype --lib field_type::memory::tests::snapshot_payload_` from `rust/`, not the generated integration target.
- PB provenance keeps its detached effective type private: `FieldType::clone` shares mutable Go-style element backing, so immutable specifications cannot expose that baseline through shallow clones. The lowerer compares it through a predicate; inspection snapshots detach the backing.
- `rust/crates/tidb-expr/src/rewriter/preparation.rs` supplies a bounded, explicit StructuralOnly boundary through the purpose-aware builders in `rewriter.rs` and `new_function.rs`. Existing public wrappers remain SqlBuild. Structural trees retain declared metadata, not value-refined nullability/precision; explicit signed CAST can be prepared but is not admitted for execution by the private seed. No constant/value hook, native retry or public SQL route is added.
- Private `rust/crates/tidb-expr/src/tikv/ordinary.rs` provides the separate signed-PLUS row seed. Typed trees contain no PB state; PB trees are checked occurrence-for-occurrence against the trusted retained original wire during lowering, then release that borrow. Binding checks native Int/NULL before transport can erase UInt identity; each computed node owns its result metadata. Its raw-runtime entry preserves local errors and does not activate a public route. `rust/crates/tidb-expr/src/tikv/ordinary_diagnostics.rs` adds an optional private, own-spec diagnostic plan and checked native overflow view: exact kernel/site/domain joins precede typed1690, own PLUS rendering is distinct from nested PB display names, and the raw cause remains recoverable. Input/validation/resource errors are not reclassified as arithmetic; count/length warning endpoints are observations, not a warning sink or context/severity handoff.
- Private `rust/crates/tidb-expr/src/tikv/lineage.rs` adds the separate native Datum SQLTypedRow control boundary. Its all-node no-PB type proof, sparse producer IDs and own program/table preserve selected Int/UInt and String/Bytes/collation identity; predicates reject transitively possible UInt. Complete source and incoming-schema payload gates precede allocating snapshots/equality. The demanded read checks kind/collation before transport erasure; materialization accounts for the retained core values, IDs and native output together, without promising a hard allocation peak. Inspection returns detached FieldType snapshots. It does not compose ordinary calls, the private overflow view, native batch, or public SQL routes. Its focused command from `rust/` is `cargo test --locked -p tidb-expr --lib tikv::lineage::tests:: -- --test-threads=1`.
- Private `rust/crates/tidb-expr/src/evaluator/numeric_batch.rs` owns the closed signed-203 native-batch caller. `evaluator.rs` shares its actual dispatch with a mandatory private consumer while public `run` remains native; `scalar_function.rs` separates unchanged eligibility from the native value worker. The invocation token is minted after Decimal priority and one current vectorization-flag read, never by executing a worker as a probe. It joins its own program/calculated slot/source tree, checks full incoming metadata and chunk layout before effects, copies the actual selection once and checks native Int/NULL on the demanded read. Only one calculated tree and no ColumnSwap/leaf/row/PB/parameter routes are admitted. Source, selection and fixed native Int output coexistence have separate retained checks; aliases remain owner-stable and there is no hard-peak claim. Raw/reported failures do not use the row diagnostic or control-lineage domains. From `rust/`, run `cargo test --locked -p tidb-expr --lib evaluator::numeric_batch::tests:: -- --test-threads=1`.
- `rust/Cargo.toml` records the experimental sibling TiKV checkout relationship and caller-side dependency patches. Kernel reuse is in-process and does not itself require a TiKV/PD service.
- First tests: datatype `collation` unit tests, `shared_collation_contract` and `tikv_value_bridge_source` in the declared `--test all` target, expression `like::tests`/`tikv::tests`, and utility `stringutil` unit tests. Check each crate's manifest: `tidb-util` uses `--lib`, not an `all` integration target. From `rust/` with the pinned caller toolchain, run `cargo test --locked -p tidb-expr --lib tikv::tests::` for the explicit seed and `cargo test --locked -p tidb-expr --lib rewriter::preparation_tests::` for the preparation boundary. The preparation suite also checks paired legacy build behavior. Run `cargo test --locked -p tidb-expr --lib tikv::ordinary::tests::` for the explicit PLUS row boundary. Use `cargo test --locked -p tidb-expr --lib tikv::ordinary::diagnostics::tests::` for the private overflow view. These suites do not prove general expression-family migration, general SQL diagnostics, or public activation.

### Session, variables, protocol
- `pkg/session/`
- `pkg/sessionctx/`
- `pkg/sessionctx/variable/`
- `pkg/server/`
- Typical changes: session lifecycle, statement context behavior, protocol-level behavior.

### DDL and metadata
- `pkg/ddl/`
- `pkg/infoschema/`
- `pkg/meta/`
- `pkg/meta/autoid/`
- Typical changes: schema evolution, metadata persistence, ID generation.

### Storage and distributed execution
- `pkg/kv/`
- `pkg/store/`
- `pkg/distsql/`
- `pkg/tablecodec/`
- Typical changes: KV semantics, storage integration, distributed query paths.

### Domain and statistics
- `pkg/domain/`
- `pkg/statistics/`
- Typical changes: schema/statistics lifecycle, cardinality/estimation behavior.

### Parser and AST
- `pkg/parser/`
- Typical changes: SQL grammar, AST nodes, parser behavior.

## Test Surfaces
- Unit tests: package-local tests under `pkg/**`.
- Integration tests:
  - inputs: `tests/integrationtest/t/`
  - expected outputs: `tests/integrationtest/r/`
- RealTiKV tests:
  - `tests/realtikvtest/`
  - use when behavior depends on real TiKV/PD interaction.

## Practical Search Workflow
1. Start from symptom:
   - SQL keyword
   - error message
   - variable name
2. Find implementation entrypoint:
   - planner/executor/expression/session/ddl/store
3. Find existing tests around the same behavior.
4. Confirm neighboring modules only if call chain crosses boundaries.

## Common Cross-Module Paths
- Planner -> Executor -> Expression for query semantics.
- Session/Variables -> Executor for user-visible runtime behavior.
- DDL -> Infoschema/Meta -> Domain for schema lifecycle.
- Store/KV/DistSQL -> Executor for distributed execution behavior.

## Notes and Runbooks
- Planner notes: `docs/agents/planner/rule/rule_ai_notes.md`
- Notes guide: `docs/agents/notes-guide.md`
- Testing runbook: `docs/agents/testing-flow.md`
- AGENTS review guide: `docs/agents/agents-review-guide.md`
- Root execution contract: `AGENTS.md`
