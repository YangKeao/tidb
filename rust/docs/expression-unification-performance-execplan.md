# Make unified expression execution observable and fast

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at the repository root. Maintain this plan according to that file.

## Purpose / Big Picture

The experimental Rust expression-unification path is correct on the current targeted SQL corpus, but its prepared signed-integer `PLUS` route still costs about 260.5 ns per selected row at batch size 1,024, roughly 12.3–13.3 times the frozen native evaluator. After this work, operators can see which execution route actually handled every eligible projection batch, and the common prepared runtime avoids repeating immutable expression/schema validation and avoidable lane-local allocations. Later milestones turn that same common substrate into a whole-selection batch driver and remove transport copies; `PLUS` is only the first proof, never a permanent bespoke fast path.

The observable first milestone is a release-profile benchmark which prints a live route snapshot, allocation/capacity evidence, and before/after timings for batch sizes 1, 16, 256, and 1,024 across dense, sparse, reverse, duplicate, NULL, and error cases. Correctness remains at least the existing 73/75 exact real PD/TiKV SQL differential baseline. The final milestone is not complete until temporary migration gates and equivalent fallback implementations have been removed.

## Progress

- [x] (2026-10-15) Replaced per-lane singleton kernel calls with one vector call, removed singleton views/SmallVec dependency, and added aggregate work-budget boundary tests. TiKV profile 48/48 and runtime budget/storage 6/6 pass.
- [x] (2026-10-16) Measured three interleaved release pairs: Int 52.792→17.370 ns/row (3.05x), Decimal 536.500→288.082 (1.86x), LOWER 104.976→37.813 (2.78x). All nine workloads improve, 1,890 records / 135 checksum groups agree. Frozen native was separately refreshed (Int21.196). Report/evidence retained under `docs/expression-unification/logs/performance-m9-vector-kernel/`.
- [ ] Reduce typed transport overhead while preserving Chunk selection/alias behavior; measure separately.
- [ ] Add a reproducible per-function/signature planning inventory and expand shared capability coverage with semantic tests.

- [x] (2026-10-15) Publication review: refreshed TiDB batch 25/25, TiKV profile 48/48, TiDB lint and scoped formatting/diff checks; retained original timing and prior full-suite/build limitations. Published benchmark evidence and historical probes alongside the report.

- [x] (2026-10-10 01:24Z) Received authorization to execute the agreed performance roadmap and moved Raft task #4 to In Progress.
- [x] (2026-10-10 02:05Z) Re-read repository policies, the TiKV coprocessor maintenance guide, the existing performance report, and the current TiDB/TiKV local-expression contracts.
- [x] (2026-10-10 02:20Z) Created this self-contained ExecPlan and recorded migration-boundary deletion rules.
- [x] (2026-10-09 17:29Z) Captured the frozen same-machine release baseline from binary SHA-256 `d7c33c2b60ff10b587ec896bbe5e98c6c1e830d8d7a0389affc9b2b322b4d7b2`: batch-1,024 dense signed `PLUS` median 285.179 ns/row across seven samples (284.410–285.575 ns/row); the frozen binary predates live telemetry, so route identity remains its retained benchmark assertion and the paired after run must supply the live snapshot.
- [x] (2026-10-09 17:40Z) Added allocation-free prepared/fallback/refusal telemetry, direct benchmark consumption of that telemetry, and focused tests for one-time refusal plus live call counts.
- [x] (2026-10-09 17:40Z) Deep-detached alias-backed `FieldType` metadata once at `EvaluatorProgram` ownership, sealed numeric workers by owner plus pinned root identity, retained bounded live row-schema/layout checks, and reused bounded selection scratch.
- [x] (2026-10-09 17:43Z) Completed milestone 1: the release benchmark now uses live route counters, covers 1/16/256/1,024 rows plus dense/NULL/sparse/reverse/duplicate selections, and reports retained scratch capacities.
- [x] (2026-10-09 17:43Z) Completed the measured numeric portion of milestone 2. At batch 1,024 every measured call was prepared with zero fallback/refusal; medians were dense 279.861, NULL 283.471, sparse 296.480, reverse 281.755, and duplicate 281.452 ns/row. Selection/decoded capacities stabilized at 1,024/2,048 elements. The ready-value `HashMap` is not on this route, so no unmeasured dense-slot rewrite was made.
- [x] (2026-10-09 17:43Z) Closed the optional ready-value portion of milestone 2 without a code change: the dedicated numeric benchmark proved that cache is not on this workload, so changing it would be unmeasured complexity rather than part of the accepted optimization.
- [x] (2026-10-09 17:52Z) Completed milestone 3 without a TiKV source change: production now invokes TiKV's existing generic `LocalBatchProgram::eval_with_bindings` whole-selection substrate. Deleted the parallel decoded/raw evaluator and its route flag rather than retaining a migration fallback.
- [x] (2026-10-09 17:57Z) Re-ran the focused TiDB suites, complete `tidb-expr` library, release no-run build, `make lint`, and the full release benchmark matrix. The four complete-library failures are the same independently reproducible failures recorded below; all modified-path tests pass.
- [x] (2026-10-09 18:12Z) Completed milestone 4: the admitted fixed-width path reads through the checked caller-Chunk binding and writes directly into the destination Chunk. The old decoded input, intermediate `Vec<Datum>`, and per-row `append_datum` path were deleted.
- [x] (2026-10-09 18:17Z) Completed milestone 5's first shared family: `PLUS`, `MINUS`, `MUL`, and unary `ABS` use one arity-aware sealed/batched/direct-output substrate and the same TiKV integer dispatcher. No function-named execution route was added.
- [x] (2026-10-09 18:26Z) Rebuilt the release server and reran the real PD/TiKV differential: 73/75 exact, with only the same two names representing one Go vector/scalar `REGEXP_INSTR` inconsistency.
- [x] (2026-10-09 18:28Z) Validated the TiKV changes with 44/44 local profile tests and a targeted `tidb_query_expr` clippy run. Full-workspace policy clippy remains independently blocked by a fixed-nightly compiler ICE in unchanged `components/file_system`; the first non-ICE workspace error is also outside this patch in `tikv_alloc`'s jemalloc intrinsic configuration.
- [x] (2026-10-10 00:28Z) Removed the 1,024-row fallback boundary. Larger selections now stream consecutive 1,024-row windows through the same prepared program; 1,025/2,048-row and 2,049-row reverse/duplicate tests prove ordering and cross-window behavior.
- [x] (2026-10-10 00:51Z) Unified the default server and benchmark allocator on TiKV's `tikv_alloc`, removed the second global allocator, aligned allocator-stat ABI versions, and fixed the pinned-nightly jemalloc intrinsic call. Default debug and release server builds now pass.
- [x] (2026-10-10 01:04Z) Fixed the stats background panic by excluding synthetic non-positive physical IDs from persisted-stats targets; the focused regression test covers a negative system-table ID.
- [x] (2026-10-10 01:19Z) Completed milestone 6 real-server validation against PD/TiKV at concurrency 1/4/16, with clean Rust-server logs and graceful shutdown. Completed the default 75-case release differential, FIPS TiKV check, TiFlash-facing DistSQL/pushdown tests, and final source-boundary audit.
- [x] (2026-10-10 01:24Z) Deleted the last migration names and gates. `BatchLane` is the permanent semantic-class state, not a function-specific or transitional route; source search finds no old numeric-vector program, compile mode, lane, or evaluator symbol.
- [x] (2026-10-10 02:31Z) Generalized the admitted carrier boundary from signed Int to Int/Bytes and proved both physical directions on the same whole-selection driver: `CHAR_LENGTH` is Bytes-to-fixed and `LOWER`/`UPPER` are Bytes-to-varlen. NULL, sparse, reverse/duplicate selection, and 2,050-row windowing stay prepared.
- [x] (2026-10-10 02:41Z) Renamed the production route and TiKV program/compile surface from `NumericBatch`/`numeric_batch`/`tikv_numeric` to semantic `Batch` names. Production-source searches for the obsolete names and tags return zero.
- [x] (2026-10-10 02:58Z) Centralized eager capability in TiKV's `batch_signature` registry and made preparation, native eligibility, and execution consume it. The frame driver now accepts heterogeneous inputs and arbitrary registered arity; `SUBSTRING(Bytes, Int, Int) -> Bytes`, `EQ/GT(Int, Int) -> Int`, and Real `PLUS/MINUS/MUL/ABS` use the same route. TiDB focused tests pass 23/23 and TiKV local profile tests pass 46/46.
- [x] (2026-10-10 03:01Z) Added a paired release `REAL PLUS` workload. Batch-1,024 dense median is 60.679 ns/row versus 1,178.884 ns/row for the same binary's current fallback ablation (19.43x); every selection scenario reports prepared only and zero fallback/refusal. This is not the frozen original-native baseline.
- [x] (2026-10-10 03:18Z) Added exact Decimal `PLUS/MINUS/MUL/ABS` through the same registry and driver without text conversion. Focused suites pass 24/24 in TiDB and 47/47 in TiKV. Paired batch-1,024 Decimal `PLUS` dense median is 563.610 ns/row versus 1,506.392 ns/row for the same binary's current fallback ablation (2.67x), not the frozen original-native evaluator.
- [x] (2026-10-10 03:35Z) Rebuilt the release server and reran complete-library, FIPS, DistSQL/TiFlash-facing, source-boundary, and real PD/TiKV checks. The final broader tree keeps the 73/75 exact SQL baseline; complete-library failures remain the known four TiDB and six TiKV cases.
- [x] (2026-10-12 01:58Z) Generalized the owned typed carrier boundary to all native TiDB EvalTypes. Date/Datetime/Timestamp use an exact raw time word, Duration preserves nanos plus FSP, JSON preserves type code plus binary payload, and VectorFloat32 preserves raw float bits. Datetime and Timestamp share TiKV's DateTime carrier. ENUM/SET/BIT/ARRAY remain excluded because they are hybrid/array encodings rather than independent native EvalTypes.
- [x] (2026-10-12 02:10Z) Added a three-mode same-binary ablation for the current fallback, current TiKV code forced to one-row windows, and optimized TiKV batch. Five samples over five selection/NULL scenarios for one representative workload per TiDB EvalType produced 675 timing records, 45 cross-mode checksum groups with zero mismatches, and prepared-only TiKV routes with zero fallback/refusal. The first two modes were initially mislabeled as historical native/original TiKV and are not used for cross-version claims.
- [x] (2026-10-12 02:22Z) Corrected the benchmark cohort after review. A disposable probe built the actual frozen original-native TiDB `364aef2b` with paired TiKV `548812e` and measured the same nine dense batch-1,024 workloads. The historical pre-optimization TiKV pair `f388b337`/`608bbfc9` was rebuilt separately; only signed Int PLUS had a TiKV batch route at that revision, so the other eight historical TiKV cells are explicitly N/A rather than simulated with current code.

## Surprises & Discoveries

- Observation: before milestone 1, `EvaluatorSuite::run_with_tikv_batch` cached only `Prepared` versus `Unsupported`; the benchmark inferred the route through a `cfg(test)` boolean, so a live server could not report why it fell back.
  Evidence: the implementation now exposes the permanent prepared/fallback/refusal counters and uses `BatchLane` for the semantic-class state; the old test-only inference hook is gone.

- Observation: a prepared numeric invocation re-walks the complete expression and compares every detached `FieldType`, then allocates a fresh selection vector and decoded columns on every call.
  Evidence: `PreparedBatch::validate_invocation` calls `suite_shape`, `preflight`, full node/schema equality, and creates `Vec<usize>`; `decoded_columns` creates a new `Vec<VectorValue>`.

- Observation: the ready-value lane cache removes a worker from a `HashMap` for exclusive invocation and reinserts it afterward. TiKV also performs deep `evaluated_bytes_shape` and retained-metadata checks through `retained_storage` before and after calls.
  Evidence: `rust/crates/tidb-expr/src/tikv/ready_value.rs::ReadyValueCache::run_args` and `CacheInvocation::finish`; `components/tidb_query_expr/src/local/batch.rs::EvaluatedBytesWorker::observe_storage`.

- Observation: the existing round228 benchmark already hard-asserts numeric versus production fallback and covers several selection shapes, but it uses batch sizes 1/8/64/256/1024 and exposes neither refusal reasons nor reusable-capacity counters.
  Evidence: `rust/crates/tidb-expr/src/tests/lane_cache_batch_performance.rs` and `docs/expression-unification/LANE_CACHE_PERFORMANCE_REPORT.md`.

- Observation: the repeated source-tree check could not simply be deleted: `FieldType::clone` intentionally shares Go-style mutable ENUM/SET backing, so an external builder alias could change metadata after preparation. The safe seal requires detaching that backing at the immutable program ownership boundary first. Owner identity alone was also insufficient for private fault-injection tests; a pinned root-identity tag closes that seam without a tree walk.
  Evidence: `program_metadata_detachment_and_incoming_caps_precede_equality_and_effects` and `foreign_suite_decimal_or_control_cannot_do_work_before_private_refusal` failed under the naive owner-only change, then pass with ownership detachment plus the root tag.

- Observation: the required generic whole-selection machinery was already present in the paired TiKV baseline. `LocalBatchProgram::eval_with_bindings`, `RpnExpression::eval_with_input_batch`, `EvalExecution::SqlBatch`, and `OrdinaryFrame::finish_batch` already preserve selection occurrence order while evaluating the expression program once. The TiDB production route had continued to use a separate decoded/raw vector entry.
  Evidence: switching only the TiDB adapter to that existing entry reduced the final batch-1,024 dense median to 149.129 ns/row. No TiKV file changed, and source search now finds no `run_batch_raw`, `decoded_columns`, `decoded_capacity`, `run_batch_with_bindings`, or numeric route boolean.

- Observation: once direct destination writes were added, the old TiKV vector-specialized program kind and its parallel compile/evaluation entry became pure migration duplication.
  Evidence: the TiKV patch removes that program entry, compile mode, compiled-program member, and local decoded batch entry. The remaining `RpnExpression::eval_decoded` is the generic public RPN API, not the deleted local numeric adapter.

- Observation: extending the first family did not require another driver. Arity plus signature is sufficient metadata for signed fixed-width `PLUS`, `MINUS`, `MUL`, and `ABS`; all four lower to `NodeKind::IntCall` and use the same frame driver and math dispatcher.
  Evidence: every family benchmark scenario reports prepared calls only with zero fallback/refusal, and source search finds no function-named batch entry.

- Observation: real-server validation exposed two production blockers unrelated to the arithmetic kernel: TiDB linked a second jemalloc ABI and the stats follower admitted negative synthetic table IDs into persisted-stats bookkeeping.
  Evidence: the allocator now comes only from TiKV `tikv_alloc`; default release linking succeeds. `stats_targets` filters `physical_id > 0`, its regression test passes, and the final server run has an empty Rust problem log.

- Observation: a selected set larger than 1,024 is not semantically unsupported; it is a transport windowing concern.
  Evidence: the final adapter streams each bounded window through the same sealed prepared program, and tests cover 1,025, 2,048, and reverse/duplicate 2,049-row selections without entering scalar fallback.

- Observation: fixed versus variable width changes storage import/output construction, not eager batch admission. The current cross-repository seam imports selected Bytes into one compact TiKV data/offset/null arena; it avoids per-row objects and interpreter dispatch, but it is not a literal zero-copy borrow of TiDB's internal Chunk allocation.
  Evidence: the same driver executes Bytes-to-Int, Bytes-to-Bytes, mixed Bytes/Int three-argument calls, and fixed-width Real calls. The Bytes adapter performs one bounded arena copy while the Int/Real adapters copy fixed cells; all are accounted under the same retained-memory policy.

- Observation: duplicating function allowlists between TiDB's eligibility pass and TiKV's compiler caused a newly admitted comparison to prepare but miss the mandatory consumer entry.
  Evidence: the first `EQ/GT` test exposed that split. `batch_signature` is now the one capability registry, and both the TiDB eligibility pass and TiKV preparation read the same resolved signature/carrier/arity fact.

- Observation: broadening the registry also broadened the existing native-reference eligibility helper; that helper still assumed every Int result was binary arithmetic, so the first full SQL rerun exposed a panic on unsigned `GT`.
  Evidence: the failed first carrier audit produced 72/75 and a panic at the old arithmetic-only assertion. The helper now leaves registered non-arithmetic calls on their existing row evaluator; a native-vs-batch `GT/EQ` regression passes, and the rebuilt audit returns to 73/75 with no panic.

## Decision Log

- Decision: optimize one common substrate; use signed LongLong `PLUS` only as the first end-to-end proof.
  Rationale: invariant sealing, route telemetry, dense slots, and buffer reuse are operation-independent. Carrier-specific batch adapters must be shared by all pure strict functions in that carrier.
  Date/Author: 2026-10-10 / TiDBEngineer and YangKeao.

- Decision: keep TiKV changes inside `components/tidb_query_expr::{local,types::expr_eval}` and its maintenance documentation; do not alter existing wire/RPN/coprocessor entry behavior.
  Rationale: the experimental route is an in-process library consumer. A change requiring per-builtin signature rewrites or coprocessor request changes is a design-stop signal.
  Date/Author: 2026-10-10 / TiDBEngineer.

- Decision: preparation performs deep validation once and publishes a compact immutable seal. Invocation checks only owner identity, the seal, mutable input/output layout, selection bounds, and live worker health.
  Rationale: `EvaluatorProgram::new` now first deep-detaches Go-shared `FieldType` slice backing. The resulting private tree is pinned by `Arc`; owner plus root identity authenticates it, while caller-owned row schema remains size-bounded and exactly compared on every invocation. Repeating the owned tree walk is then measurable overhead rather than a safety boundary.
  Date/Author: 2026-10-10 / TiDBEngineer.

- Decision: temporary gates are deletion-scoped scaffolding. Each gate introduced below names its deletion milestone, and at most canonical scalar plus target batch modes may coexist.
  Rationale: YangKeao explicitly requires the completed design not to accumulate fallback routes.
  Date/Author: 2026-10-10 / YangKeao and TiDBEngineer.

- Decision: variable width is a storage-adapter concern only. Pure, strict, eager calls are admitted by one signature registry; their input/output carriers may be fixed, variable, or heterogeneous, and arity does not select a different driver.
  Rationale: batching still removes frame construction, dispatch, validation, and call overhead when values are represented by offsets/slices. Only lazy/effectful semantics require another canonical execution mode.
  Date/Author: 2026-10-10 / YangKeao and TiDBEngineer.

- Decision: owned typed transport is sufficient for batch admission; zero-copy is an independent optimization.
  Rationale: Go TiDB VecEval demonstrates that removing per-row tree traversal and intermediate Datum construction is valuable even when the Chunk boundary copies typed values. Every native EvalType therefore has an owned adapter now; only hybrid/array physical encodings and order-sensitive semantics remain outside this eager mode.
  Date/Author: 2026-10-12 / YangKeao and TiDBEngineer.

## Outcomes & Retrospective

The six initial milestones below describe the earlier signed fixed-width checkpoint, not the storage path of the final all-carrier implementation. The latter uses owned typed import/output and still invokes a singleton kernel per lane; full-vector kernels and zero-copy/direct output remain future work. See the current [benchmark report](../../docs/expression-unification/LANE_CACHE_PERFORMANCE_REPORT.md).

At the earlier checkpoint, The first sealing/scratch-only checkpoint moved the batch-1,024 dense median from 285.179 to 279.861 ns/row. After whole-selection execution and direct destination output, the final seven-sample `PLUS` medians are dense 131.879 ns/row (-53.8% from the frozen baseline), NULL 133.786, sparse 136.628, reverse 133.789, and duplicate 132.553. The final test binary SHA-256 is `111d05ffae43f824ee1c339b4de1e9faca8340df7e05941af22fb1176edfbc38`; CPU calibration was 1.063 before and 1.079 ns/iteration after. All live route snapshots showed prepared calls only with zero fallback/refusal, and retained scratch is only the selection buffer (capacity 1,024, or 256 for the sparse case).

The same final binary proves horizontal reuse: dense medians were 131.278 ns/row for `MINUS`, 127.335 for `MUL`, and 81.483 for `ABS`; dense, NULL, sparse, reverse, and duplicate scenarios all stayed on the prepared route with zero fallback/refusal. These shorter three-sample family probes establish route and order-of-magnitude behavior; the seven-sample `PLUS` matrix remains the primary paired measurement.

The carrier-generalized final binary adds a much broader current-code ablation. At batch 1,024, paired same-binary current-fallback-to-batch dense medians were 860.351 to 63.384 ns/row for `CHAR_LENGTH` (13.57x), 1,104.693 to 130.713 for `LOWER` (8.45x), 1,553.316 to 127.950 for three-argument UTF-8 `SUBSTRING` (12.14x), 1,178.884 to 60.679 for Real `PLUS` (19.43x), and 1,506.392 to 563.610 for Decimal `PLUS` (2.67x). These ratios isolate the current route change; they do not compare against the frozen original native implementation. Every current-production dense/NULL/sparse/reverse/duplicate sample reported prepared calls only and zero fallback/refusal. Integer comparison, mixed-carrier/three-argument execution, and exact Decimal precision/scale transport are covered by the shared-registry tests.

The corrected historical comparison uses one representative workload for each TiDB EvalType. Dense batch-1,024 medians for `original native / historical TiKV batch / optimized TiKV batch` in ns/row are: Int 21.324/253.812/55.491, Real 111.344/N/A/56.781, Decimal 73.998/N/A/548.599, String 150.011/N/A/113.659, Datetime 119.524/N/A/43.268, Timestamp 119.799/N/A/42.469, Duration 121.732/N/A/49.928, JSON 140.874/N/A/220.669, and VectorFloat32 155.959/N/A/181.934. Historical TiKV has N/A for eight carriers because its only admitted batch route was signed Int PLUS. Relative to true original native, optimized batch is faster for Real (1.96x), String (1.32x), Datetime (2.76x), Timestamp (2.82x), and Duration (2.44x), but slower for Int (2.60x), Decimal (7.41x), JSON (1.57x), and VectorFloat32 (1.17x). For the one true three-way cell, Int, optimized batch is 4.58x faster than historical TiKV but remains 2.60x slower than original native.

The retained 675-sample same-binary matrix remains useful only as a current-code ablation: its first mode is the current migrated fallback, and its one-row TiKV mode uses current lowering/adapters/kernels. Binary calls construct three typed vectors per optimized 1,024-row call versus 3,072 in that current row-window ablation; unary identity calls construct two versus 2,048. These counters describe evaluator vector construction, not allocator-hook events. Complete raw evidence and the correction are retained in-repository at `docs/expression-unification/logs/performance-m8-all-carriers/`.

The final real-server workload queried all four operations over 1,024 rows per request. Concurrency 1/4/16 delivered respectively 94.516/272.469/255.464 QPS and 96,784/279,008/261,596 rows/s. Corresponding p50/p95/p99 latencies were 10.582/11.086/11.323 ms, 14.193/20.960/23.732 ms, and 62.412/86.400/99.241 ms. Observed RSS ranges were 141,380–143,088 KiB, 154,948–159,464 KiB, and 203,860–206,568 KiB. The Rust problem log is empty and shutdown is clean.

The final all-native-EvalType focused suites pass 25/25 in TiDB and 48/48 in TiKV. Complete `tidb-expr` is 1,374 passed with the same four known failures; complete `tidb_query_expr` is 1,023 passed with the same six known failures. The release-server, TiKV FIPS, 256-case DistSQL, and 73/75 PD/TiKV results above belong to the preceding Int/Bytes/Real/Decimal checkpoint and were not rerun for this carrier-only extension. The known RegionCache unique-owner shutdown error therefore remains outside this expression optimization, and this evidence does not claim release readiness.

The final source audit finds no vector-specialized lane/program/compile-mode names, no numeric-specific decoded evaluator, no feature gate, and no function-named executor. Unsupported semantic classes still use the one canonical native evaluator; this is an intentional semantic fallback, not an equivalent migration implementation.

## Context and Orientation

The TiDB checkout is the directory containing this plan. The paired TiKV checkout is `../tikv` relative to the TiDB repository directory. TiDB owns SQL expression lowering, `Chunk` input/output, the projection lane, and lane-local worker caches. TiKV supplies `tidb_query_expr`, the shared RPN kernels and the closed local evaluation APIs linked into the Rust TiDB process; this route is not an RPC to a TiKV server.

`rust/crates/tidb-expr/src/evaluator.rs` owns `EvaluatorSuite` and `BatchLane`. `run_with_tikv_batch` is the production projection entry: it prepares the eager batch worker once, otherwise calls the canonical native row evaluator. `rust/crates/tidb-expr/src/evaluator/batch.rs` lowers a TiDB expression into a TiKV `LocalBatchProgram`, validates each invocation, exposes checked demand reads from the caller Chunk to the generic batch program, and imports owned typed inputs and appends typed results to the destination Chunk. This avoids intermediate Datum values but still copies input cells and uses a TiKV result vector. `rust/crates/tidb-expr/src/tikv/ready_value.rs` owns a different lane-local cache for closed already-evaluated argument operations.

In TiKV, `components/tidb_query_expr/src/local/batch.rs` defines `EvaluatedBytesWorker`, `LocalBatchProgram`, and their closed argument/batch contracts. `components/tidb_query_expr/src/types/expr_eval.rs` owns the common frame driver and fixed-recipe shape validation. The applicable maintenance contract is `doc/maintenance-guides/src/coprocessor.md`, especially “In-process expression-library consumers” and the closed-ready change-impact entry.

A “route” is the execution mode actually used by one `run_with_tikv_batch` call. `prepared` means the sealed TiKV eager batch program ran. `fallback` means the suite was already classified unsupported and the canonical native evaluator ran. `refusal` is the one-time reason the attempted preparation was rejected before values or warnings were observed. A “seal” is a compact identity created only after full prepare-time validation; it is not caller-provided metadata and cannot authorize a foreign expression or schema.

## Plan of Work

Milestone 1 adds a public, read-only per-suite route snapshot in `evaluator.rs`. Preparation records a bounded refusal category rather than discarding the error. Every production entry increments exactly one prepared or fallback invocation count; the first failed preparation increments one refusal count. The manual benchmark reads this same live snapshot before and after samples, rather than inferring the route from a test-only boolean. The snapshot must not allocate, log per row, or register unbounded labels. This telemetry is permanent observability, not migration scaffolding.

Milestone 2 first captures the current release baseline. In `batch.rs`, preparation retains a compact source/schema seal and invocation stops reconstructing and deeply comparing owned immutable metadata. It continues to check `Arc` owner identity, input/output physical shape, selection bounds, and row counts. Selection and decoded Int columns become lane-owned scratch with a hard retained-capacity ceiling of 1,024 selected occurrences and the existing 8 MiB materialization limit. In `ready_value.rs`, replace the operation-to-worker `HashMap` only if a stable microbenchmark shows a repeatable benefit; the representation must be a bounded dense slot index supplied by TiKV, not an unsafe enum cast because comparison variants carry payloads. TiKV adds a compact recipe slot/seal API only in the closed local module. Public deep storage observation remains available for audits, while the hot reuse check verifies the sealed immutable identity and only mutable health/binding/capacity facts.

Milestone 3 reuses the one generic whole-selection driver already present in TiKV local evaluation. Capability is bounded by carrier and semantics: fixed-width Int, pure, strict, eager, and batch-capable. Signed LongLong `PLUS` is the first admitted expression; the driver preserves selection order and duplicates, NULLs, first error, resource bounds, and existing non-diagnostic demand/effect semantics. TiDB's separate decoded/raw entry and route boolean have been deleted. The final `BatchLane` is permanent per-suite state for this semantic class, not a migration gate; all admitted fixed-width Int arithmetic uses it, while unsupported semantic classes enter the sole canonical native evaluator.

Milestone 4 removes the owned decoded-column adapter in favor of the checked demand binding over the borrowed caller Chunk and writes the common typed batch result directly to the destination Chunk. Dense/sparse/reverse/duplicate/NULL/error tests and the real SQL corpus use this one path. Layouts that cannot prove the borrowed contract belong to the canonical scalar semantic mode; do not create operation-specific fallback ladders.

Milestone 5 registers `MINUS`, `MUL`, and `ABS` through signature/arity metadata on the same fixed-width integer substrate. Later families are chosen from live route/workload telemetry. A function adds only capability metadata and compatibility tests; any need to clone a driver or add a function-named route stops the work for redesign.

Milestone 6 measures server QPS and p50/p95/p99, allocator/scheduler behavior, retained capacity under concurrency, OOM handling, FIPS, and TiFlash compatibility. Then delete temporary feature flags, old cache/worker routes, duplicate validation/materialization, migration-only reason codes, and obsolete tests. Search for every symbol listed in the milestone artifact and require zero remaining references before declaring completion.

Milestone 7 generalizes the proven execution mode across carriers without cloning it. TiKV owns one `batch_signature` capability registry containing exact input carriers, result carrier, and signature; TiDB resolves SQL calls and consumes that registry rather than owning another allowlist. The same frame driver accepts heterogeneous arguments and any registered arity. Int/Bytes/Real/Decimal/DateTime/Duration/JSON/VectorFloat32 use shared typed adapters; TiDB Datetime and Timestamp both map losslessly onto DateTime. Lazy/effectful functions remain outside this eager semantic mode rather than accumulating per-function fallback routes.

## Concrete Steps

Run all commands from the TiDB repository unless a step explicitly changes directory. Preserve the unrelated untracked file `rust/third_party/tikv-client-rs/tests/client_go_differential/BUILD.bazel`.

Inspect state and capture exact revisions:

    git status --short --branch
    git rev-parse HEAD
    git -C ../tikv status --short --branch
    git -C ../tikv rev-parse HEAD

Build and run the focused TiDB release benchmark with the repository’s pinned toolchain. The known environment for this branch is:

    export RUSTC_BOOTSTRAP=1 CC=gcc-14 CXX=g++-14 CXXFLAGS='-include cstdint' CMAKE_POLICY_VERSION_MINIMUM=3.5
    CARGO_TARGET_DIR=/home/agent/tidb/expression-unification/target-tidb cargo test --manifest-path rust/Cargo.toml --release --locked -p tidb-expr --lib --no-run
    EXPR_BATCH_PERF_WORKLOAD=integer_add EXPR_BATCH_PERF_SAMPLES=7 EXPR_BATCH_PERF_TARGET_NS=75000000 taskset -c 2 <tidb-expr-test-binary> tests::lane_cache_batch_performance::lane_cache_batch_release_performance_probe --ignored --exact --nocapture --test-threads=1

Save the exact binary hash, command, environment, raw output, and median summary under `docs/expression-unification/logs/`; do not overwrite round228 evidence. Use identical commands for before and after measurements. The benchmark must print live route counters and retained scratch capacity.

During milestones 1–2, run focused tests after each edit:

    cargo test --manifest-path rust/Cargo.toml -p tidb-expr evaluator::batch::tests --lib
    cargo test --manifest-path rust/Cargo.toml -p tidb-expr tikv::ready_value --lib
    cd ../tikv
    cargo test -p tidb_query_expr --lib local::profile_tests
    cargo test -p tidb_query_expr --lib evaluated_ascii

Before review, return to TiDB and run:

    make lint
    cd ../tikv
    make clippy

Then build the release TiDB/TiKV binaries with the already established separate target directories and rerun `/home/agent/.slock/agents/1555103c-9f3e-46ca-ac2e-b964c650c2f1/expression-unification-e2e-audit.sh`. Record exact pass counts and any known baseline failures; do not label unrelated pre-existing failures as new.

## Validation and Acceptance

Milestone 1 is accepted when the production `run_with_tikv_batch` entry exposes allocation-free per-suite counts for prepared, fallback, and one-time refusal category; tests prove a prepared suite increments only prepared, an unsupported suite records exactly one refusal and subsequent fallbacks, and benchmark output reads these counters directly.

Milestone 2 is accepted when focused tests preserve expression results and error/NULL/selection semantics, deep validation still rejects malformed construction, live invocation no longer performs full expression/`FieldType` equality for immutable owned state, and repeated calls retain bounded selection/decoded capacities. A same-machine paired release run must show a repeatable improvement; a complexity-only change without measurable benefit is reverted or redesigned. The real SQL differential must remain at least 73/75 exact.

Milestone 3 is accepted: one generic driver invocation handles the whole selection for signed LongLong `PLUS`, and no `PLUS`-named executor fast path or parallel decoded/raw entry remains. Admission still names the seed operation while the next fixed-width pure/strict operations are not yet registered; milestone 5 must prove that extension requires only shared metadata/admission changes and tests.

Milestone 4 is accepted: owned input copies and intermediate `Vec<Datum>` materialization are removed from the admitted fixed-width path, while selection/NULL/error/resource tests remain exact.

Milestone 5's first family is accepted: `PLUS`, `MINUS`, `MUL`, and `ABS` share the same arity-aware driver and direct-output path, and every benchmark scenario reports the prepared route with zero fallback/refusal.

Milestone 6 is accepted: default and FIPS builds pass; real PD/TiKV server load covers concurrency and tail latency with bounded observed RSS and clean shutdown; TiFlash-facing request/pushdown boundaries pass without protobuf or coprocessor changes; selections larger than 1,024 stay prepared; allocator and stats blockers are closed; and every declared temporary route symbol is absent.

The initial milestone checkpoint passed its scoped performance and production checks and deleted its declared temporary boundaries. This does not establish native parity or completion of the remaining full-vector/storage optimizations. The remaining native fallback is the one canonical execution mode for semantic classes that are not safe for eager batching; it is neither a parallel implementation of the admitted family nor a migration ladder.

The carrier-generalization slice is accepted: all native TiDB EvalTypes have an owned typed input/literal/output adapter and use the same registry/driver; Bytes-to-fixed, Bytes-to-varlen, mixed-carrier three-argument, integer comparison, Real arithmetic, and exact Decimal arithmetic remain covered. TiDB's focused batch suite passes 25/25 and TiKV's registry/profile suite passes 48/48. The three-mode release probe remains prepared across every native carrier and selection scenario, obsolete numeric-specific program/route names are absent, and no storage-width condition selects scalar execution. Hybrid ENUM/SET/BIT, ARRAY, and lazy/effectful calls remain explicit semantic exclusions rather than type-specific fallback ladders. The server/build/FIPS/DistSQL/differential matrix belongs to the preceding broader-tree validation and was not rerun for this carrier-only slice; the known RegionCache shutdown blocker remains outside this expression optimization.

## Idempotence and Recovery

All inspection, test, build, and benchmark commands are safe to rerun. Evidence uses new round-specific filenames so prior results remain immutable. If a benchmark is interrupted, discard only that incomplete new raw-output directory and rerun the identical command; do not alter the frozen round228 inputs.

Use small signed commits per milestone. Do not use destructive reset or checkout commands because the worktree contains an unrelated untracked file. If an optimization regresses correctness or does not produce a stable gain, revert only that milestone’s owned patch with a new commit, retain the evidence in this plan, and redesign from the last passing commit.

## Artifacts and Notes

Current baseline revisions are TiDB `f388b337dfad156233904d5407230f8439e0150e` and TiKV `608bbfc9c63e8b50fa2be757533eba572ef9b874`. The retained round228 report is `docs/expression-unification/LANE_CACHE_PERFORMANCE_REPORT.md`; its raw evidence and parsers are under `docs/expression-unification/evidence/` and `docs/expression-unification/logs/`.

Final microbenchmark evidence remains in the retained expression-unification logs. Real-server concurrency evidence is `/home/agent/.slock/agents/1555103c-9f3e-46ca-ac2e-b964c650c2f1/evidence/performance-m6-server-v4`; its JSON files contain the 1/4/16-worker measurements and its Rust problem log is empty. Final default-release differential evidence is `/home/agent/.slock/agents/1555103c-9f3e-46ca-ac2e-b964c650c2f1/evidence/performance-m6-final-e2e`; `summary.tsv` contains 75 cases and 73 exact vector/scalar matches.

At each milestone append: the before/after commit IDs, test commands and summaries, benchmark medians with dispersion, route snapshots, retained capacities, profile deltas, and a source-search list for temporary symbols plus their deletion status.

## Interfaces and Dependencies

Milestone 1 should define in `rust/crates/tidb-expr/src/evaluator.rs` a copyable route snapshot and a bounded refusal enum, exposed through a non-mutating `EvaluatorSuite` method. The exact names may be refined during implementation, but the interface must represent prepared calls, fallback calls, refusal count, and refusal category without strings or heap allocation.

Milestone 2 changes the ready-value representation only if a dedicated workload proves that cache material. The current numeric path never reaches that map, so this slice deliberately made no ready-value cache change. `PreparedBatch` owns only reusable selection scratch and exposes its retained capacity to the benchmark; the decoded-column scratch, intermediate result vector, and parallel TiKV numeric-vector evaluator were deleted by milestones 3–4.

Do not add a TiKV dependency on TiDB `Chunk`, change protobuf/wire signatures, change the coprocessor request path, or introduce a second arithmetic implementation. Reuse the existing `tidb_query_expr` kernels and `tidb_query_datatype::{LazyBatchColumnVec, VectorValue}` storage.

Change note (2026-10-12): initial plan created from the approved roadmap, current code inspection, repository policies, and the retained round228 evidence. Later updates record all six initial milestones, production evidence, deletion of temporary boundaries, carrier-generalization through every native EvalType, heterogeneous three-argument execution, the final broader-tree validation matrix, and the three-mode all-carrier benchmark.

Change note (2026-10-15): publication requested by YangKeao. Added the corrected historical comparison and immutable current-code ablation evidence to the repository benchmark report; clarified the remaining singleton-kernel and owned materialization costs. Historical benchmark numbers were not remeasured for publication.


## Continued optimization (authorized 2026-10-15)

YangKeao requested continued measured improvements, with commits and pushes at
useful checkpoints, until stopped or no clear cost-effective direction remains.
First replace `OrdinaryFrame::finish_batch` singleton calls with one generated
kernel call over all operands. Charge all lane work before invoking the pure
kernel; check retained output before publication or the next expression effect.
This budget is retained storage, not a hard allocator peak limit. Then reduce
input/output transport costs and build a per-signature coverage inventory rather
than equating carrier support with all-function optimization. Lazy/effectful
coverage requires an explicit semantic design and must not be silently admitted.

Validation uses focused TiDB batch and TiKV profile/resource tests, library
regressions, release benchmark with the same current workload definitions, and
updated comparisons with retained historical baselines. Each useful slice must
remove its superseded implementation and record remaining gaps. Large redesigns
should stop with a concrete cost/benefit assessment rather than accumulating
unmeasured routes.
