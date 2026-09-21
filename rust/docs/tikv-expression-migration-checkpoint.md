# TiKV expression migration: verified checkpoint, not completion

## Revisions and decision

Validated TiDB `982bad99e44c9d5d0bb4e802fdd50866c0bc877f` and TiKV documentation
HEAD `afc1525e11ad50b7314afdee5b10d5b6b7f52e7f`. Both worktrees were clean
before this documentation update. TiDB actually builds the pinned engine
`db9c7f08d954fef02519b86a55c2d72cf72ef697`, not arbitrary TiKV HEAD.
Only YangKeao personal forks have been used for publication.

**Do not switch the default yet. Physical deletion is now deliberately in
progress.** The first tranche removed the complete native math-kernel directory
and made unverified math families explicitly unsupported; this invalidates the
older policy of retaining every native family until compatibility was complete.
The earlier refusal-intolerant corpus still exposed 59 distinct first-refused
expressions, so the migration objective remains active/incomplete.

## Acceptance status for the six requested items

| Item | Established foundation | Remaining acceptance work |
| --- | --- | --- |
| 1. Lazy/short-circuit | Signature-driven engine lazy kernels, unchanged wire format, TiDB lazy tests and full-tree risk metadata. Hash/Merge filter owners retain programs. | Complete TiDB vectorized short-circuit and default-switch validation. Scalar cache reuse is not vectorization; requested Borrowed mode does not prove borrowed-lazy execution. |
| 2. Capabilities/admission/gates | Explicit signature/shape admission, structured refusal, static inventory checks, CI-callable runtime receipt checker. | Wire and validate hosted CI; extend engine-required execution beyond current fixtures, remaining kernel/host capabilities and all native-supported shapes. Current compatibility mode still admits native execution. |
| 3. Semantic gap tracking | `components/tidb_query_expr/EXPRESSION_SEMANTIC_GAPS.md` records divergences, guards and the resolved embedder probe error boundary. | Resolve or explicitly account for remaining gaps before deletion; do not silently widen admission. |
| 4. Required datatypes | Set transport/codecs and adapter bridge, bounded literal/text casts and temporal profiles have tests. | Root/lazy literal provenance, implicit binary coercions, remaining numeric profiles, named-zone/DST and restrictive zero-date transport. Set support alone does not establish native-removal completeness. |
| 5. Error classification/no replay | Differential helper compares error versus success; engine errors are not replayed natively. Probe-child errors now stop later filter execution. | Enforce the same contract at every remaining entrypoint/corpus. Successful compatibility tests are not proof of zero native fallback. No retry-after-error guarantee was established. |
| 6. Sharing/cache/context | Sync metadata, compiled/execution split, EvaluatorProgram cache, structured required-engine errors; retained Hash/Merge condition programs with reopen tests. | Remaining live entrypoints, partition retention, statement-owned generated/default descriptors and their invalidation contracts; avoid caches on publicly mutable descriptors. |

The living implementation record is `tikv-expression-removal-execplan.md`.
Historical milestone checkboxes there describe implemented foundations, not
completion of all six requirements. This checkpoint corrects stale admission
counts and overbroad lazy/datatype milestone wording.

## Fresh validation results

All heavy commands ran serially with one worker and the memory guard below.
Logs are in `/home/agent/tidb/expression-reuse/`.

| Log | Result | Scope/qualification |
| --- | --- | --- |
| `checkpoint-executor-native.log` | 1346 library + 329 integration + 6 tests passed; 184 integration ignored | Feature-off executor compatibility, not engine coverage. |
| `native-math-delete-full-lib-tests.log` + `native-math-delete-integration-tests.log` | 1216 library + 77 integration passed; 99 library ignored | Feature enabled after physical math deletion; contracted families assert explicit refusal. |
| `native-math-runtime-gate-green.log` | 30 tests, 334 fixture receipts, 2150 engine rows, 160 observed borrowed rows | Reviewed baseline after removing 34 now-contracted math receipts; zero fallback in receipted engine contexts. |
| current static gate | Self-check and check pass | 384 declaration rows, 223 admitted / 161 excluded, zero missing registry/synthesized names. Static candidates are not execution coverage. |
| `checkpoint-engine-only.log` | **1163 passed / 61 failed / 99 ignored**, exit 101 | All 61 failed sections report adapter refusal; 59 distinct first-refused expressions. This is an incomplete cutover gate. |

Sampled peak for these fresh runs: 2210.3 MiB. Very short cached runs can be
below the guard's sampling interval; their near-zero samples are not actual
zero-memory claims. Feature-on executor previously passed 1402/355/6/2 with
184 integration ignored in `probe-error-executor.log` at this code revision.
Repository `make -j1 lint` passed in `probe-error-lint.log`; this is Go
revive/dashboard checking, not Rust clippy or SQL equivalence.

### Important meaning of the engine-only environment variable

`TIKV_EXPR_ENGINE_ONLY=1` affects `tidb-expr/src/tests/mod.rs::chunk_e`:
that helper still computes the native oracle first and compares it with the
engine. The variable turns adapter refusal into a panic instead of skipping the
engine comparison. It does **not** remove native code or force every unit test
through the engine. Therefore neither the 1163 passing tests nor the difference
between passing-suite counts is an engine execution count.

Each failing test stops at its first refused expression. The 59 distinct strings
are a lower bound on missing corpus shapes, not a complete inventory. Examples
include `benchmark(-3, 1)` and `cot(1)`; additional families include temporal and
implicit casts, binary literals, collation-sensitive lookup, regexp and JSON.
Keep refusal/error/value-divergence classifications separate when fixing these.

## Exact reproduction

From `expression-reuse/tidb/rust`:

```bash
export CARGO_HOME=/home/agent/tidb/expression-reuse/cargo-home
export RUSTUP_HOME=/home/agent/tidb/expression-reuse/rustup-home
export RUSTUP_TOOLCHAIN=nightly-2026-08-22
export PATH="$CARGO_HOME/bin:$PATH"
export CARGO_BUILD_JOBS=1 MALLOC_ARENA_MAX=2 RUST_MIN_STACK=4194304
export CMAKE_POLICY_VERSION_MINIMUM=3.5
export CXXFLAGS='-w -std=gnu++14 -include cstdint' CFLAGS='-w' RUSTFLAGS='-Awarnings'
# Run each command serially; preserve each exit status.
python3 ../../tools/limited-run.py --rss-mib 8192 --as-mib 16384 -- cargo test -q -p tidb-executor --locked --offline -j1 -- --test-threads=1
python3 ../../tools/limited-run.py --rss-mib 8192 --as-mib 16384 -- cargo test -q -p tidb-expr --features tikv-expr --locked --offline -j1 -- --test-threads=1
python3 ../../tools/limited-run.py --rss-mib 8192 --as-mib 16384 -- python3 scripts/tikv_expression_runtime_gate.py
python3 scripts/tikv_expression_coverage.py --self-check --check
# Expected to fail until remaining admitted-shape gaps are fixed:
TIKV_EXPR_ENGINE_ONLY=1 python3 ../../tools/limited-run.py --rss-mib 8192 --as-mib 16384 -- cargo test -q -p tidb-expr --features tikv-expr --lib --locked --offline -j1 -- --test-threads=1
```

Next priority: fix one refusal family at a time using a failing engine-required
fixture, engine-side semantics where needed, and narrow signature/shape admission;
rerun the refusal-intolerant corpus after each change. Do not lower the gate or
relabel native fallback as coverage. Hosted CI, fresh Go/mysql replay, broad
performance comparison, package-transcreation completion and native deletion
remain unverified/not achieved.
