# Share native host argument collection and remove redundant wrappers

This living ExecPlan follows repository-root PLANS.md. The implementation and final measurement are complete; human review remains pending. See [the result and review guide](expression-shared-host-arguments-results.zh.md) and [machine-readable evidence](expression-shared-host-arguments-results/).

## Purpose / Big Picture


Reduce repeated per-function state machines, validation and empty ownership/coercion wrappers while preserving SQL results, child demand, errors, warnings and cancellation. YangKeao requested one consolidated simplification round, checkpoint commits, and final performance measurement. The latest review direction authorizes Git pushes to the existing YangKeao/tidb and YangKeao/tikv expression-unification-demo branches. Source ZIP delivery is superseded; no PR or deployment was requested.

## Progress


- [x] Snapshot the retained source and protect the unrelated BUILD file, SDK and frozen original repository.
- [x] Consolidate eighteen compatible host families and eleven repeated child-list construction walks.
- [x] Remove output-free profile walks, ignored private allowance arguments, infallible assignment/value wrappers and provably redundant reads.
- [x] Preserve callback order, NULL stopping, real conversion and allocation failures, reply provenance and cancellation.
- [x] Save and push seven implementation checkpoints ending at 43ba91c201769b0d7d9930e2da3488870e95ca6f.
- [x] Complete cumulative unit/service builds, 995 scoped checks, edition2021 formatting, root make lint and source-isolation checks.
- [x] Complete the final fourteen-scene, four-label comparison: 224 cells and 140244 full queries; audit all observations and clean owned services/data.
- [x] Prepare the Git review report, raw timing and test evidence, and a standalone verifier.
- [ ] Human review and final acceptance. The broader project's path-unification and performance goals remain incomplete.

## Surprises & Discoveries


The early sixteen-family candidate omitted immediate completion for zero-argument temporal functions; nine existing tests caught it and the branch was restored. Its later functional result was 981 passes and one known failure. Its performance comparison had adverse changes and was not accepted. A compact follow-up reduced the descriptor stride from712 to632 bytes and passed functional checks but was never SQL-timed before the consolidated-round directive superseded it.

The cumulative build after later plumbing cleanup caught one remaining profile_shape call in the Decimal fast source. Checkpoint43ba91c201 removes it; failed build101/10.633s and successful retry0/287.931s are both preserved.

The first final-round SQL preflight failed before timing because JSON_SUM_CRC32(doc) is not legal parser syntax. The correct ARRAY syntax is parsed but rejected by the existing rewriter in A/C/F. The derived fixture also omits stored JSON columns. Preserve these failed probes; use supported computed JSON construction/extraction as scene14. JSON checksum value semantics have unit coverage, but its SQL performance is explicitly unmeasured. No parser or ARRAY feature was added to enable a benchmark.

## Decision Log


- Reuse the SDK HostFrame protocol, native value store and static kernel dispatch. Do not add an executor, plugin layer or dependency.
- Share argument collection, task consumption and result publication only across compatible families. Keep kernel-specific coercion, PASSWORD warning phases, TIMESTAMPDIFF NULL stopping and zero-argument completion explicit.
- Keep probe values only inside the five kernels that need them. The final descriptor indexing stride632 and entry stack reservation3016 match the compact version; these observations do not establish performance causality.
- Remove same-state duplicate authentication only without intervening callbacks, and remove fresh-store checks only before any external effect. Preserve public compatibility limit fields, provenance and real allocation failures.
- Call the existing scalar result converter directly. Moving/cloning selected values cannot produce the deleted Result errors; CASE/COALESCE exhaustion directly returns NULL. Existing opaque-value fixtures now exercise IF/COALESCE/ANY_VALUE consumers.
- Follow the latest request to retain this consolidated simplification for review despite adverse performance measurements. Do not declare universal no-regression or attribute all variation to environment noise.
- Deliver source and evidence through Git. The earlier ZIP artifacts remain historical evidence only.

## Outcomes & Retrospective


Relative to e94b3c53dec9a80395e640e03171549217285918, production Rust is 1813 additions/4395 deletions (2582 fewer lines), tests398 additions/335 deletions (63 more). Blank lines count; docs are excluded. These are incremental reductions, not the full project's difference from its original base.

The final995 scoped checks completed994pass/1same known baseline failure in1035.028s. The2441-name inventory retains all original2440 tests plus one new shared-state test. The real service build completed0/607.112s; lint and format passed. SDK tests are explicitly inherited from identical1574 inputs (1159pass/1known/1ignored), not rerun.

The final valid measurement completed0/997.769s. Strict56 preflight observations and448 before/after records (672 full observations) pass the audit. Decimal and JSON CPU are lower in all four rounds against both identical-binary controls; crypto, timestamp difference and4096-row deep expressions are higher in all four. The report includes every scene, metric, observed range and original-Rust reference. No source withdrawal follows from performance alone. All owned services and temporary database directories were removed.

## Context and Orientation


Main repository: /home/agent/tidb/expression-unification/tidb. SDK sibling: /home/agent/tidb/expression-unification/tikv, unchanged f7672bf980143f2938fd0f900650a4c29091e70f. Whole-project original bases are Main364aef2bab5cc633ecb76a775ae8f36f86a6687d and SDK548812e1ef57aef077a2062a9cc356640a6347f5. Complete-base diffs include historical branch changes beyond this round.

Main rust/crates/tidb-expr/src/evaluator/batch.rs lowers source expressions. batch_hosts.rs owns task lifecycle; batch_arguments.rs now collects arguments for eighteen function families. Value kernels live under src/tikv and call shared SDK algorithms. Host replies carry authenticated native value handles. Short-circuit demand and eager value conversion remain distinct semantic stages.

## Plan of Work


Implementation is complete. Review the result guide in ownership order: SDK compiler/frame protocol, Main lowering/host integration, source-specific demand, and value kernels/tests. Review the seven checkpoint commits independently if needed. Address new review findings with a new checkpoint and relevant verification; do not relabel the fixed43ba91c201 evidence as testing later production edits.

## Concrete Steps


Actual builds ran from the Main root with empty RUSTFLAGS and CARGO_BUILD_JOBS=1:

    ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j1 -p tidb-expr --lib --no-run --message-format=json
    ../tools/cargo-tidb build --manifest-path rust/Cargo.toml --release --locked --offline -j1 -p tidb-server --bin tidb-server --no-default-features
    make lint

The result directory records the exact rustfmt file list and995 test names, terminal outputs, binary hashes and build commands. Independently verify the delivered evidence from the repository root without launching services:

    python3 rust/docs/expression-shared-host-arguments-results/verify-results.py

## Validation and Acceptance


The scoped tests cover values, NULL stopping, first errors, warnings, ignored NAME_CONST coercion, typed contexts, stale/wrong-row handles, duplicate replies, cancellation and callback mutation/unwind. Two added PB matrices each assert26,101,440 cases. Preserve test outcomes and sample data, not obsolete helper call syntax.

One known optional-common original-observation test still fails with signature1713/657792/D19F5082E4AF34C5. Full2441 execution, new SDK/datatype/Go sweeps and allocation-failure injection were not performed. JSON checksum SQL/ARRAY conversion and stored-JSON reads were not benchmarked.

A/B are identical retained14 ELF, C is the final candidate, F is the frozen original Rust service. Use whole-SQL execute plus full fetch, measured process CPU, actual chunk/vector settings, four balanced orders, and at least4seconds AND30queries per cell. Observed ranges are not confidence intervals. This evidence does not prove universal production performance.

## Idempotence and Recovery


Do not rerun historical mutation/build/timing scripts in sealed evidence directories. Use new output paths for another experiment and preserve initial failures. Restore only scoped owned changes if a later decision requires it; never reset/clean shared work. The unrelated rust/third_party/tikv-client-rs/tests/client_go_differential/BUILD.bazel and frozen original repository remain untouched.

Clean only recorded PID/birth-matched services and matching data directories. This round is already cleaned. The local full evidence is in the agent workspace evidence/path-unification/shared-host-arguments/overnight. Do not run its final_gates or storage setup again against completed artifacts.

## Artifacts and Notes


The report links full-base and incremental Git comparisons. The result directory includes every timed latency, per-test terminal output, raw preflight/semantic evidence, source inventories, actual receipts and cleanup records. Repeated observations use a lossless JSON dictionary; the verifier reconstructs the exact log SHA256. Binary ELF and complete compiler/service logs remain local and are not included in Git. Historical first/compact candidate evidence is separate from the final source.

## Interfaces and Dependencies


Private NativeHostMetadata, SqlDemand, NativeTask and NativeArguments changed. HostInvocation/HostArgReply/HostStep, native-store provenance, public source views, value semantics and SDK interfaces remain compatible. No dependency or public SDK API changed. Existing fixed arities determine inline argument storage; no new SQL arity limit was imposed.
