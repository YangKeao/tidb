# Remove adapter payload copies and measure the difference


This living ExecPlan follows repository-root `PLANS.md`. This is a follow-up to the copying PoC, not a completed Go-package port or a promise of end-to-end zero-copy execution.


## Purpose / Big Picture


The user wants a separate branch from the working expression-reuse PoC, removal of avoidable copying, and measured improvement against that PoC. Preserve the native and copying backends as controls. Add an explicit borrowed-input/direct-output backend that reads existing TiDB column payloads without constructing intermediate owned input columns, invokes TiKV's existing kernels, and appends results without constructing an owned facade result and a Vec<Datum>. New output and intermediate expression values still need storage; do not call these unavoidable result writes zero-copy.

The checkouts are `expression-reuse/tidb` and `expression-reuse/tikv` under the session workspace. TiDB follow-up branch is `feat/tikv-expression-borrowed-poc`, starting at `38870eba83fcf6b4939063e01c42c383f5d512b2`; TiKV is `feat/borrowed-expression-poc`, starting at `ff315c7d001573166595702af6a36b6f69d2f93e`. Preserve the original branches. Only YangKeao forks may be pushed; upstream push URLs remain disabled.


## Progress


- [x] Verify clean baselines and create both follow-up branches.
- [x] Identify input/result materialization stages and read-only column lifetime requirements.
- [x] Add safe borrow-scoped TiDB column views and their tests (275 library tests passed, 4 existing ignored; 2 compile-fail doctests passed).
- [x] Extend TiKV RPN argument loading to accept borrowed packed inputs while retaining original kernels (429 expression and 21 codegen tests passed).
- [x] Wire explicit borrowed execution and direct output into TiDB, preserving copying/native controls (full expression 1180+34 and executor 345 suites initially passed against the published engine revision).
- [x] Verify SQL parity, NULL/selection/alias handling, nonfinite safety, and no input payload materialization.
- [x] Finish append-compatibility RED/GREEN regression: full expression tests now pass 1180+35; existing suite appends via copying while the direct API retains its empty-output contract.
- [x] Rerun both complete 80-case three-backend benchmarks on source commit bdc7597; record all gains and the sparse-byte regression.
- [x] Run scoped gates, review, pin published TiKV 521ac733, and publish both follow-up source branches only to YangKeao forks; remote source SHAs verified. Report/CSV documentation accompanies delivery.


## Surprises & Discoveries


TiDB numeric column bytes are native-endian, not universally little-endian. They may be unaligned (including frozen slices of coprocessor buffers), so casting them to &[i64] or &[f64] is not valid. Safe stack-local scalar decoding with from_ne_bytes is acceptable; materializing an entire decoded input column is what this change removes.

TiDB `SharedBytes` has owned, frozen, and synchronized shared storage. Borrowing shared data requires a read guard. Moreover, two chunks can share an entire Column owner, even though the Chunk values differ. Holding an input read guard and acquiring a write guard on an aliased output can deadlock. Check owner identity and shared output storage before acquiring input guards; use the existing copying path for unsupported alias cases, with counters that do not misreport it as borrowed execution.

Existing TiKV typed arguments expose references to values in owned ChunkedVec storage. Borrowed unaligned packed inputs cannot satisfy those references without either copying whole columns or extending argument loading. The intended extension decodes only the current numeric scalar into stack-local storage and borrows byte slices, then invokes existing scalar kernels. Generated intermediate vectors remain owned.


## Decision Log


- Decision: keep native/copying/borrowed as distinct runtime choices in one feature-enabled binary. Rationale: isolate the copy-removal delta from unrelated compiler/dependency changes, and preserve rollback. Date: 2026-09-17.
- Decision: no unsafe typed casts in TiDB or unchecked assumptions about buffer alignment. Rationale: Chunk supports frozen/unaligned/shared payloads and the workspace forbids unsafe code. Date: 2026-09-17.
- Decision: preserve the existing admission whitelist and original TiKV kernels. Rationale: copy removal should not also broaden SQL semantics or create handwritten arithmetic replacements. Date: 2026-09-17.
- Decision: retain mandatory final output writes and owned RPN intermediates, but remove adapter input payload materialization and redundant result containers. Rationale: report actual eliminated copies rather than rebrand unavoidable computation storage. Date: 2026-09-17.


## Outcomes & Retrospective


Implementation and validation are complete. TiDB source/benchmark commit is bdc7597cf916a259061c2785e7a2f5b002b4c39e; the pinned/published TiKV commit is 521ac7330208b7f74f2b182bc373882687967280. `tikv-expression-borrowed.md` records exact commands, gates, caveats and both complete 80-case CSVs. At 1024 dense rows, the longer repeat reduced copying-relative time by 29.0% (integer add), 11.7% (integer chain), 8.4% (DOUBLE arithmetic), and 74.2% (short-byte LENGTH). Sparse LENGTH (1 of 4096 rows) instead became 4.07x slower due to whole-offset validation; short-data single-row cases still favor native. Large-string LENGTH avoids payload reads, so its much larger factors are not general kernel speedups. The adapter remains explicit/experimental, with native/copying controls and owned intermediates/final output writes preserved.

An additional integration review found nonempty calculated-output append semantics, outside the new direct API's empty-output contract. A RED test proved the regression; copying fallback before borrowed execution restored all three modes' append behavior. A resource-limited run also exposed the existing allocator stress test's 100 internal threads: lowering its thread stacks and malloc arena reservations, not raising limits, restored the full 275-test pass. The maximum sampled development/validation group RSS was 2263.7 MiB; final benchmark processes sampled 42.5/43.5 MiB.


## Context and Orientation


The preserved copying path in `rust/crates/tidb-expr/src/tikv.rs` gathers selected Chunk rows into owned nullable facade columns. TiKV `components/tidb_query_expr/src/standalone.rs` copies those into decoded VectorValue columns, evaluates existing RPN, then copies the result into another owned facade column. TiDB converts that to Vec<Datum> and appends to the output Chunk. `EvaluatorSuite` in `tidb-expr/src/evaluator.rs` caches compiled programs per execution; `StmtContext` provides explicit opt-in, settings and successful-row counters.

A borrow-scoped view means slices are valid only while their read guard is alive. The new `tidb-chunk::column::ColumnReadView` holds that guard and expose data, validity bits (one means valid), i64 offsets, row count and optional fixed width. Offsets/validity and selection maps stay borrowed; no compaction is required simply to express reordered or duplicate logical rows.


## Plan of Work


First, add and test the minimal read-only Column view API. Prove returned payload/metadata addresses are those of existing storage, including owned, frozen and shared backing. Expose a read-only shared-storage predicate for safe fallback decisions, not a mutable representation escape hatch.

Second, extend TiKV argument loading and the standalone facade with a borrowed column view and a result sink. The sink is a callback used while an engine result is alive; it receives nullable primitive values or borrowed byte slices and writes them directly into TiDB's output column. Validate buffer shapes, offsets, selection bounds and finite selected REAL values before kernel entry. Keep the existing copying evaluator unchanged and test borrowed/copying equivalence across empty, scalar, selected, duplicate and split batches. Avoid introducing a new hand-maintained builtin implementation.

Third, add explicit backend selection to Columns/StmtContext and a borrowed-row counter in addition to the existing total TiKV-row counter. Change the projection adapter to write directly into its destination column. Detect potentially aliased output before input guards are held; preserve safe copying fallback. Any runtime error must propagate without replay; partial output must be reset/discarded and never presented as success.

Fourth, add the separate `benches/tikv_expression_borrowed.rs` target, leaving the original benchmark untouched, to compare the three backends on exactly the same expression/input/settings/output contract. Rotate all six execution orders over at least six samples. Verify output equality and positive backend-specific counters before timing, and exact counter deltas after each timed sample. Keep compile/setup separate from warm timing. Include original batch sizes and payloads plus larger byte strings so removed payload copies are observable, rather than assuming numeric results generalize to strings.

Finally, run required gates, publish/revision-pin the TiKV dependency to the YangKeao fork, revalidate against that exact commit, record measurements and limitations, and deliver the follow-up TiDB branch without rewriting the original branches.


## Concrete Steps


Use the installed isolated homes and separate target directories. Following the user's explicit memory-safety request after a suspected OOM/SSH interruption, run only ONE heavy command across both repositories at a time. Cargo, native compilation and test execution use one worker initially (`-j 1`, `CARGO_BUILD_JOBS=1`, `CMAKE_BUILD_PARALLEL_LEVEL=1`, `NUM_JOBS=1`, `RUST_TEST_THREADS=1`). This overrides the earlier repo default of twelve jobs. Wrap heavy commands with the workspace helper `expression-reuse/tools/limited-run.py`: process-group RSS limit 6144 MiB, per-process virtual address-space limit 8192 MiB, and abort if host available memory falls below 8192 MiB. Benchmark inputs remain bounded and use a smaller 1024 MiB RSS budget. Do not retry a memory-limit failure without first reducing memory consumption; never silently rerun unbounded.

The SSH interruption alone did not prove OOM: current host memory showed about 40 GiB available, kernel dmesg access was denied, and neither implementation agent had started a build. The helper samples inherited process-group RSS conservatively (shared pages can be counted more than once) and terminates only its own child group. A small-allocation smoke test passed. The host-specific environment is:

    export CARGO_HOME=/home/agent/tidb/expression-reuse/cargo-home
    export RUSTUP_HOME=/home/agent/tidb/expression-reuse/rustup-home
    export PATH="$CARGO_HOME/bin:$PATH"
    export RUSTUP_TOOLCHAIN=nightly-2026-08-22
    export RUSTFLAGS=''
    export CARGO_BUILD_JOBS=1 CMAKE_BUILD_PARALLEL_LEVEL=1 NUM_JOBS=1 RUST_TEST_THREADS=1
    export CARGO_TARGET_DIR=/home/agent/tidb/expression-reuse/target-tidb
    # Use /home/agent/tidb/expression-reuse/target-tikv for TiKV commands.
    export CMAKE_POLICY_VERSION_MINIMUM=3.5
    export CXXFLAGS='-include cstdint -std=c++17'

Exact final target/filter names and logs are recorded in the accompanying report. Relevant commands from TiDB `rust/` include:

    cargo test -p tidb-expr --features tikv-expr --lib --test all -j 1 --locked -- --test-threads=1
    cargo test -p tidb-executor --features tikv-expr --test all -j 1 --locked -- --test-threads=1
    cargo bench -p tidb-expr --features tikv-expr --bench tikv_expression_borrowed -j 1 --locked --no-run

After builds finish, run the printed benchmark binary pinned to CPU 2, with no competing build. Run `make lint` from TiDB repository root. No new clone or Go/Bazel change is planned; the previous fresh-clone Bazel preparation failed for missing bazel and that limitation remains recorded.


## Validation and Acceptance


Acceptance requires actual SQL projection on the new backend with a counter proving it did not silently use copying. Results and errors must match the previous bounded contract; diagnostic wording remains TiKV's own, not exact native text parity. Test selected duplicate rows, NULLs, zero/large batches, unaligned/frozen storage, finite-input enforcement, context isolation and alias fallback without hanging. Backend callback lifetimes must not permit references to outlive read guards.

Pointer-identity tests and source-level absence of payload materialization must support any no-input-copy claim. Benchmarks must include all remaining per-call validation, scalar loads, output reset/materialization, and counters. Report copying/borrowed and native/borrowed ratios, sample spread and setup costs. Do not discard slower cases or claim a whole-SQL/cluster speedup.


## Idempotence and Recovery


Do not reset or force-push either baseline branch. If borrowed loading proves unsafe or semantically incompatible, retain copying as the supported fallback and record the exact limitation. Never retry execution errors natively. Commit only relevant code/docs/results; keep compiler caches/logs outside tracked source. Verify fork push URLs and resulting remote branch SHAs before reporting publication.


## Interfaces and Dependencies


TiDB view API is `Column::read_view() -> ColumnReadView<'_>` with `data`, `null_bitmap`, `offsets`, `rows`, `fixed_len` methods, and `Column::has_shared_mutable_storage()`. Final engine API: `ColumnRef::{Int { values, validity }, Real { values, validity }, Bytes { values, offsets, validity }}`, `PreparedExpression::supports_borrowed()`, and `eval_borrowed(columns, physical_row_count, selection, sink) -> Result<Diagnostics, Error>`. The sink is `for<'a> FnMut(ScalarRef<'a>) -> Result<(), Error>`, with `ScalarRef::{Null, Int, Real, Bytes}`; borrowed references never enter the cached program. TiDB exposes `TikvExpression::evaluate_into(context, input, output, output_index)` and `Backend::{Copying, Borrowed}` through Columns/StmtContext, plus a distinct successful borrowed-row counter.

Revision note: initial plan records the clean forked baselines, actual packed-byte/guard constraints, the three-way comparison requirement, and the distinction between eliminated adapter copies and remaining engine/output storage.
