# Share native host argument collection and task state

This ExecPlan follows repository-root PLANS.md. Keep Progress, Surprises & Discoveries, Decision Log and Outcomes & Retrospective current.

## Purpose / Big Picture

Reduce the repeated per-function state machines and validation around TiDB native values while keeping SQL results, demand order, errors, warnings and cancellation behavior. YangKeao authorized this experiment on November 12 in message52a009d5 and requested a way to review the entire project before a PR. Deliver both original-base patches with a reading guide and a separate patch for this experiment. No push or PR is required.

## Progress

- [x] Read current source, repository rules and prior performance evidence; snapshot retained inputs and all batch modules before edits.
- [x] Consolidate compatible function families around common argument/task metadata and collection; preserve distinct value-conversion phases.
- [x] Exercise ordinary eager, NULL-stop and warning-before-conversion cases, malformed replies, cancellation and callback behavior.
- [ ] Complete targeted expression/consumer checks, formatting, root lint, and actual SQL performance comparisons.
- [ ] Decide retention from results, report net production/test changes separately, and deliver whole-project plus incremental review artifacts.

## Surprises & Discoveries

The first temporal test run found nine failures caused by omitted niladic immediate completion. The shared zero-argument start branch is restored; expected results remain unchanged. Initial compiler failures and their fixes are preserved in the evidence directory.

Previous tiny check-removal experiments were withdrawn after performance regressions. This experiment changes the duplicated state representation and complete family scaffolding rather than replaying those patches. Many families are eager in expression demand but short-circuit during value conversion; those stages must stay distinct.

## Decision Log

- Use the existing SDK HostFrame protocol and native value store. Share compatible TiDB task and metadata structure with static dispatch; do not introduce a second executor, dynamic plugin layer or new dependency. November12 / ExpressionReuseEngineer.
- Preserve validations at external frame replies and after callbacks. Eliminate repeated internal lookups only where the state cannot have changed. Preserve all real allocation failures and observation contracts. November12 / ExpressionReuseEngineer.
- Target net2000 fewer production lines across compatible families, but report actual results even if this target is not achievable without semantic or performance loss. Do not cut tests or functionality to manufacture the count. November12 / ExpressionReuseEngineer.

## Outcomes & Retrospective

Sixteen families now share one argument state and lowering branch. The complete candidate unit binary built successfully; 982 scoped consumer tests completed: 981 passed and one existing failure retained the exact baseline signature. All nine earlier niladic failures now pass. The actual server build completed successfully; root lint, edition2021 formatting and source isolation checks passed. SQL performance is pending. Production diff is 1444 additions / 3379 deletions (1935 net fewer), tests 236 additions / 116 deletions (120 net more). No deletion or performance result is accepted yet.

## Context and Orientation

Main /home/agent/tidb/expression-unification/tidb at e94b3c53dec9a80395e640e03171549217285918; SDK sibling tikv at f7672bf980143f2938fd0f900650a4c29091e70f. Main rust/crates/tidb-expr/src/evaluator/batch.rs lowers expressions to NativeHostMetadata; batch_hosts.rs creates and resumes NativeTask; batch_<family>.rs repeats argument collection then invokes value kernels under src/tikv. Host replies carry authenticated native value handles, not serialized SQL values. Short-circuit means requesting only required child programs; eager means requesting every child before value conversion.

Evidence directory /home/agent/.slock/agents/02fef0a9-76ce-4afe-9bce-73eebe82ff7d/evidence/path-unification/shared-host-arguments holds baseline-input.json and byte copies of original batch modules. Prior archives are immutable. Retained server is evidence/remove-demo-policy/performance/tidb-server-after-round14, never mutable target-tidb/release/tidb-server, which still contains a rejected candidate before this build.

## Plan of Work

Start with eager fixed-arity identity, byte, crypto, misc, grouping and vector families. Give them one metadata envelope and argument cursor, keeping kernel-specific payload and body conversion. Share collection, stage consumption and result publication; preserve warning callbacks without live store borrows. Extend the same representation only to families whose argument order and NULL behavior can be represented without a new interpreter. Remove superseded module declarations and code, update all lowering sites and direct-construction tests together. NULL-stopping families retain explicit demand policy. Diagnostic metadata readers must continue exposing actual source arguments and output roles.

## Concrete Steps

From the main repository, build a release unit binary with:

    ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j1 -p tidb-expr --lib --no-run --message-format=json

Use an empty RUSTFLAGS and one compiler. Save actual child terminal receipts and binary SHA256 in the evidence directory. Run existing affected batch tests and consumer tests from that binary with one thread, comparing any preexisting failure to the retained baseline. Run make lint and rustfmt checks on changed files, then build the real tidb-server release binary with the existing cargo-tidb wrapper. Record exact executed commands and selections in evidence job descriptions rather than relying on this planned text.

## Validation and Acceptance

Verify normal values, NULL stopping, first-error and warning order, PASSWORD warning before conversion, NAME_CONST ignored name coercion, stale/wrong-row handles, repeated or canceled replies, and callback mutation/unwind using existing tests plus focused shared-state coverage. Test cases and independent expected values must remain. Root lint applies to Rust changes; no Go/Bazel metadata edits are planned. SDK tests may be inherited only if its actual inputs remain unchanged and inheritance is explicitly reported.

Before SQL timing read current resource directives and actual machine load. Compare the candidate against two runs of saved retained14 using identical data, full results/fields/warnings/EXPLAIN and effective session configuration. Cover scan/control, integer, decimal, text, nested/deep/batch expressions and directly migrated function families. Save initial failures without normalization; no concurrent builds or tests during timing. Report CPU/query, latency and throughput, paired variability and controls; do not claim universal no-loss from noisy measurements. Under the latest human direction843fd7fa, complete one consolidated simplification round before timing. Report adverse measurements and environmental variation; a performance-only failure does not automatically withdraw the simplification. Functional correctness remains required.

## Idempotence and Recovery

Only edit owned files after verifying baseline bytes; preserve unrelated rust/third_party/tikv-client-rs/tests/client_go_differential/BUILD.bazel, SDK and frozen original repository. Restore only this experiment's changes if rejected, never git reset/clean shared work. Use fresh output paths for every trial and preserve failures. Clean only PID/birth-verified owned services and matching temporary data directories.

## Artifacts and Notes

Review delivery includes two original-base patches (TiDB364aef2bab5cc633ecb76a775ae8f36f86a6687d and TiKV548812e1ef57aef077a2062a9cc356640a6347f5), a separate pre-experiment-to-final patch, file inventories and a guide ordered by algorithm ownership, shared compiler/frame engine, host integration and tests. Mark unrelated branch changes and distinguish raw full diff from project-only attribution. A local reviewable artifact is sufficient; do not open or push a PR.

## Interfaces and Dependencies

NativeHostMetadata, SqlDemand, NativeTask and family value bodies are the main private interfaces. Keep HostInvocation/HostArgReply/HostStep, native store provenance, source views and SQL value kernels semantically compatible. New common task state uses bounded inline storage only for functions whose actual signature has that bound; never impose a new SQL arity limit. Dependencies and public SDK interfaces remain unchanged unless a documented finding requires a separately justified change.

## Compact probe follow-up

The first candidate completed 192 SQL cells / 131884 queries with strict semantic and configuration checks, but regressed CPU in migrated families (calendar +4.480% versus retained A and +4.255% versus B). It is not accepted. All owned services and data were removed. Disassembly shows metadata indexing stride increased from 616 to 712 bytes, while the common resume stack reservation is 3016 bytes (baseline crypto760/calendar1112). These are measured layout differences, not proven full causes. The follow-up moves optional probe data into only the five kernels that use it, preserving shared task/argument state without adding heap allocations. Rebuild, existing tests, actual layout and fresh SQL comparison are required before deciding retention.

Compact-probe validation completed: unit build0/297.385s, 982 consumers981pass/1same known failure/454.989s, server build0/610.524s, rootlint and edition2021 format0. Actual service metadata stride632 (baseline616/firstcandidate712), local stack reservation3016 unchanged. Candidate2 net1891 fewer production lines/tests123 more. Two fresh load probes failed (targetcore~5.8%/6.9% plus browser/media activity); no variant2 SQL storage or timing started. Raft reminder e2250a00 will recheck at2026-11-12T10:44:05.372Z, then resume the preserved binaries when actual load permits. Task29 remains in progress and neither candidate is accepted.

## Consolidated simplification round (November13)

Human843fd7fa supersedes the pending compact-only timing: finish plausible low-cost structural reductions together, then run one final functional and SQL performance round. The compact-only source and binaries remain preserved, with no SQL timing performed. Reminder e2250a00 was acknowledged. Audit remaining eager family wrappers, duplicate typed result publication, repeated authentication without intervening callbacks, and wrappers left after removed accounting. Preserve lazy demand, callbacks and externally reachable validations. Keep existing outputs and failures immutable in shared-host-arguments/consolidated/before-source and previous evidence directories.
