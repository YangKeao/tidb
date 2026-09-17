# Reuse TiKV expression evaluation in the Rust SQL node


This ExecPlan is a living document maintained according to the repository-root `PLANS.md`. Keep Progress, Surprises & Discoveries, Decision Log, and Outcomes & Retrospective current. This is an explicitly bounded integration experiment, not a claim that a Go package has been transcreated or that TiDB and TiKV expression semantics are fully interchangeable.


## Purpose / Big Picture


An explicitly enabled Rust TiDB projection will evaluate a documented subset of typed SQL expressions using TiKV's existing `tidb_query_expr` RPN evaluator in the same process. RPN is TiKV's compiled, typed sequence of expression operations. The first implementation deliberately copies and converts columns. Users can compare native TiDB evaluation with this path using tests and a repeatable benchmark that includes input conversion, TiKV evaluation, and output conversion. Existing behavior stays the default, and unsupported expressions stay native without retrying runtime errors through a different evaluator.

All remote changes must be pushed only to `YangKeao/tidb` and `YangKeao/tikv`. The independent checkouts have their upstream remote push URL disabled. Do not open upstream PRs or push to any `pingcap` or `tikv` organization repository.


## Progress


- [x] Create isolated TiDB and TiKV checkouts and feature branches; verify authenticated YangKeao fork write permissions.
- [x] Install the TiDB-pinned nightly toolchain into workspace-local Cargo/Rustup homes.
- [x] Inspect both column representations, projection evaluator boundaries, and repository development instructions.
- [x] Add a checked, copy-based embedding API around TiKV's existing expression builder and evaluator; complete TiKV expression library tests pass (420 tests, including 12 embedding suites). TiKV fork commit `ff315c7d001573166595702af6a36b6f69d2f93e` is published; its nonfinite-input safety fix has before/after failing/passing regression evidence.
- [x] Add an optional TiDB dependency and typed-expression/column/context/error adapters.
- [x] Connect an explicit opt-in projection path and prove SQL/executor integration plus safe native fallback. New adapter and SQL tests assert actual TiKV execution; feature-enabled and feature-disabled native regression suites pass.
- [x] Run targeted unit/integration tests, formatting/lint, and release-mode conversion-inclusive benchmarks. Final receipts and both raw benchmark runs are in `tikv-expression-reuse.md` and its linked CSV files.
- [x] Pin the published TiKV fork commit and review/prepare the TiDB branch for fork-only publication. TiDB's branch URL is recorded in the report; the final push must use the checked `fork` remote and be verified with `git ls-remote`.


## Surprises & Discoveries


The installed system compiler is stable 1.97.1 and no rustup was initially present. The workspace-local toolchain is nightly-2026-08-22 (rustc 1.100.0-nightly), matching TiDB's pin. TiKV itself pins an older nightly, so compatibility must be checked rather than assumed.

TiDB uses prost-generated TiPB types, whereas TiKV uses rust-protobuf. Serialized expression/type messages form a small compile-time bridge; batches themselves are never serialized or sent over RPC.

The TiDB branch contains a documented pre-existing `tidb-exec` integration target migration failure in `hash_join_v2_source.rs`. Do not silently remove or weaken it to make broad validation green.

The existing global pushdown catalog does not lower every desired local signature (including Length, Abs, and several numeric operators). The adapter therefore uses existing lowering for leaves and a private, homogeneous-type signature map for local scalar nodes; it preserves every node's inferred FieldType and does not alter distributed pushdown admission. Five missing wire enum constants were copied from the pinned upstream TiPB source into the checked protocol input, with generated code rebuilt by Cargo.

Automatic Decimal admission is deferred: the copying facade's decimal text cannot carry hidden storage precision independently of resultFrac for arbitrary intermediate values. Nonfinite REAL inputs are also left native before evaluation. TiKV's standalone API still exercises Decimal with bounded direct tests.

A fresh consumer does not inherit TiKV's Cargo.lock or root patches. TiKV's TiPB/KVProto workspace dependencies were pinned to their baseline revisions, and the TiDB root applies matching rust-protobuf and raft patches. The optional engine currently forces flate2 to TiKV's existing 1.0.11 version; native-path regression tests cover the combined resolution.

Host toolchain compatibility required CMAKE_POLICY_VERSION_MINIMUM=3.5 and CXXFLAGS='-include cstdint -std=c++17' for legacy gRPC/abseil under CMake 4/GCC 16. TiDB's configured -Zthreads=8 multiplied by 12 Cargo jobs exhausted the host's thread budget; validation uses RUSTFLAGS='' to disable that optional compiler frontend parallelism. No dependency source was patched for these environment issues.

The required fresh-checkout `make bazel_prepare` was attempted and failed immediately because bazel is not installed. No Go or Bazel source was modified. `make lint` completed successfully. This is a recorded gate limitation, not a claim that Bazel validation passed.


## Decision Log


- Decision: keep two independent repositories under `expression-reuse/{tidb,tikv}` and disable upstream pushing in their local config. Rationale: protect the user's other checkouts and enforce fork-only publication. Date: 2026-09-17.
- Decision: preserve existing TiKV kernels and add a small checked embedding surface, rather than copying function implementations into TiDB. Rationale: the experiment must measure actual engine reuse. Date: 2026-09-17.
- Decision: accept column copies and value conversions, retain native evaluation as the default, and use a conservative admission set. Rationale: avoid bundling column unification, lazy-control-flow redesign, and full SQL parity into the first proof. Date: 2026-09-17.
- Decision: use serialized TiPB expression/type metadata only during compilation. Rationale: bridge independent protobuf generators without introducing row serialization or a service boundary. Date: 2026-09-17.


## Outcomes & Retrospective


The opt-in copying integration runs real SQL projections with TiKV's original RPN engine. Final validation passes 420 TiKV expression tests; 1180 TiDB expression unit tests plus 25 integration tests; and 340 executor integration tests, including eleven SQL/adapter cases. Existing ignored tests remain ignored (99 expression, 184 executor). Feature-disabled regressions, the MySQL error mapper, feature-enabled tidb-exec compilation, affected-target Clippy, owned formatting and make lint also pass. Bazel preparation remains unavailable because the host has no bazel binary.

The final 1024-row dense microbenchmarks, including all copies, show approximately 4.34× integer-add, 7.46× integer-chain, 4.65× DOUBLE-arithmetic and 1.60× byte-length speedups against this branch's native Rust evaluator. A longer repeat confirms the broad pattern, while single-row calls are roughly 2–4.7× slower. These are warm expression measurements, not cluster/SQL speedups; per-chunk parallel compilation and dependency weight remain important limitations. Complete parity, production readiness, zero-copy sharing and TiFlash integration are not claimed.

Review discovered a public-boundary infinity panic not covered by the initial nullable/ordinary-value tests. Red tests reproduced it in both layers; the final pinned engine rejects nonfinite inputs before kernel entry and all new regressions pass. This confirms why strict value-domain admission is needed in addition to a signature whitelist.


## Context and Orientation


TiDB baseline is `ceaaa790da06562dbaa0aff48a7fd6914b6375f7` on `hparser-integration`. TiKV baseline is `51b411a728f7c5b12f919fd4dac00a664145b751` on master. Development branches are `feat/tikv-expression-reuse-poc` and `feat/standalone-expression-poc` respectively.

In TiDB, `rust/crates/tidb-expr/src/evaluator.rs` owns projection programs and their execution instances. `pushdown_catalog.rs` lowers typed expression trees to TiPB, retaining explicit type conversions and field metadata. `context.rs::Columns` supplies statement settings and warning callbacks. `rust/crates/tidb-executor/src/projection.rs` invokes the evaluator; `stmt_context.rs` owns statement-scoped execution settings. `tidb-chunk` owns the input/output column buffers.

In TiKV, `components/tidb_query_expr` contains the existing builder, RPN evaluator, and builtins; `components/tidb_query_datatype` supplies typed vectors and evaluation context. `eval_decoded` evaluates in-memory columns without storage access. Some RPN helpers assume batches of at most 1024; the embedding surface must split safely and handle empty input before calling them. Conditional expression children are eager in the generic RPN path, so control-flow expressions are excluded from initial automatic admission.


## Plan of Work


First, add `tidb_query_expr::standalone` with a prepared expression, owned Int/Real/Bytes/Decimal column carriers, explicit context, and errors/warnings carrying MySQL codes. It must call the existing builder and evaluator, validate shapes and admitted signatures before evaluation, normalize selection to dense input, and split oversized batches. Keep changes to existing kernels out of this milestone.

Second, add an optional TiDB backend that compiles admitted expressions through existing TiPB lowering. Copy only referenced input columns, materialize logical rows in selection order, and append typed output to the existing Chunk. Keep compiled TiKV state execution-local rather than putting non-Sync RPN metadata into the shared plan. Context changes must not reuse stale compilation/evaluation policy. Unsupported expressions are rejected before evaluation; a runtime error must never cause native re-evaluation.

Third, expose an explicit statement/execution opt-in and wire it through the projection path, with tests that prove the shared evaluator actually ran. Keep lazy expressions, session effects, unsupported types, and unverified signatures on native evaluation. Add a benchmark using identical typed expressions and input chunks; report native and TiKV paths, and distinguish compilation from steady-state conversion-inclusive evaluation.

Finally, test, format, lint, and review both changes. Commit TiKV with DCO sign-off and publish only to its YangKeao fork. Replace any temporary local dependency in TiDB with the exact published fork revision before final validation and publication. No absolute local dependency path may remain in committed manifests.


## Concrete Steps


The workspace-local build environment is:

    export CARGO_HOME=/home/agent/tidb/expression-reuse/cargo-home
    export RUSTUP_HOME=/home/agent/tidb/expression-reuse/rustup-home
    export PATH="$CARGO_HOME/bin:$PATH"
    export RUSTUP_TOOLCHAIN=nightly-2026-08-22

Use separate target directories for TiDB and TiKV. Initial TiKV feasibility check from its checkout is:

    CARGO_TARGET_DIR=../target-tikv cargo check -p tidb_query_expr --lib

The exact feature-enabled TiDB test, lint and benchmark commands will be recorded when their executable targets exist. Run TiDB Cargo commands serially as required by `rust/docs/operations/validation.md`. For repository gates, inspect `make bazel_prepare` (required by the fresh-checkout policy) and run `make lint`; preserve unrelated generated differences separately rather than including noise in this feature.

Before each push, inspect the exact URL and use the explicit fork remote with a fully specified branch refspec. Do not use force-push.


## Validation and Acceptance


Tests must demonstrate ordinary and nullable arithmetic, selected/reordered rows, empty and multi-batch inputs, type/shape validation, warning propagation, error code preservation, and native fallback for unsupported/conditional expressions. At least one test must run a typed SQL projection through TiDB's executor path, not just call a standalone TiKV example.

Benchmarks must verify equal results outside the timed loop and consume timed outputs so the compiler cannot eliminate work. Include at least numeric and variable-length data workloads, small/normal/large batches, and all per-call input/output conversions. Report actual observations without assuming that Rust or vectorization makes the bridge faster. Compilation/setup timing must be clearly separated or explicitly included in a separate cold-path result.

A successful outcome is passing bounded tests and reproducible measurements from committed fork revisions. Full Rust TiDB/TiKV production integration, full cluster tests, all signatures, zero-copy columns, TiFlash, and changes to network/storage formats are out of scope.


## Idempotence and Recovery


Both repositories are independent fresh clones, so this work does not change the user's other working trees. Cargo caches and toolchains are under the session workspace. Existing test or dependency failures are investigated and recorded, not suppressed through deleted assertions. If a proposed signature fails parity, remove it from automatic admission and retain a regression for rejection before execution. The default native path remains available throughout.


## Artifacts and Notes


Forks and access were verified using GitHub API under the authenticated YangKeao account. Upstream push URLs in both clones are `DISABLED-UPSTREAM-PUSH`. See `tikv-expression-reuse.md` for exact validation/reproduction commands, fork references, complete scope/limitations, and links to both final benchmark CSV files.


## Interfaces and Dependencies


TiKV embedding API is `PreparedExpression::compile(expr_bytes, schema_bytes, Context)` and `eval(&mut self, columns, row_count, selection)`. `Column` owns nullable Int, Real, Bytes or Decimal values. `EvalOutput` contains the dense result column, retained warnings, and total warning count; failures preserve numeric MySQL error identity and message. These are a deliberate copy-based integration boundary, not a new shared column format or C ABI.

TiDB depends optionally on the exact published TiKV fork revision. Feature-disabled builds must not compile the TiKV dependency, and normal execution remains native unless explicitly enabled. The production data path must use TiKV's existing `RpnExpressionBuilder` and `eval_decoded` rather than reimplementing their algorithms.
