# Share native host argument collection and remove redundant wrappers

This living ExecPlan follows repository-root PLANS.md. The first consolidated round and its measurement are fixed at product commit43ba91c201 and report commite5e15d1d27. Autonomous cleanup continues under YangKeao’s November13 10:00 Asia/Shanghai deadline; human acceptance remains pending. See [the result and review guide](expression-shared-host-arguments-results.zh.md) and [machine-readable evidence](expression-shared-host-arguments-results/).

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
- [x] Fix the offline verifier’s Python 3.9 compatibility separately at0623f21305; raw measurements remain unchanged.
- [x] Remove28 compiler-confirmed unused private functions/methods after checking references and configuration; no fixture values or test functions changed. This checkpoint removes323 net production lines.
- [x] Remove seven unused test-only forwarding helpers; retain every test function and fixture value, including unused fixture generators.
- [x] Compile the28-private/seven-test-wrapper cleanup atae4b0b5965 (actual0/280.571s); no runtime checks attributed to this interim build.
- [x] Remove infallible Result contracts from numeric-window assignment, identity-name probing and WEIGHT_STRING padding; retain formatting panic order and token checks after conversion. Remove the newly unused private JSON re-export.
- [x] Save and push the unused-helper/control-result checkpoints throughffb508eeae; retain316 actual test function names across54 changed Rust files.
- [x] Complete the cumulative1013 selection (1012pass/1sameknown), both release builds,54-file format check, root lint and unchanged source/isolation proof.
- [x] Complete the next14-scene224-cell comparison against pinned43ba91c201 and the frozen original; preserve the initial estRows-only preflight failure, one restart and strict successful retry; clean owned services/data.
- [x] Prepare the second result report, complete raw evidence and independently runnable Python3.9 verifier.
- [x] Apply the separately reviewed six-file dispatch/direct-call cleanup:44 fewer production lines, no test or fixture changes.
- [x] Build, validate and measure the six-file checkpoint separately against pinnedffb508eeae; publish the complete result and independently verifiable evidence.
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


The first measured round is complete. Continue by removing unused crate-private entrypoints in JSON dispatch, scalar/wildcard evaluation, regular-expression adapters, coercion and temporal helpers. Confirm each candidate against compiler diagnostics and whole-crate references; preserve exported APIs, active kernels, test cases and fixture values. Update documentation links to the actual shared helpers when a forwarding wrapper disappears. Use a new evidence directory, evidence/path-unification/unused-private-helpers, leaving the prior overnight directory sealed. The next A/B baseline is the pinned43ba91c201 service SHA256783723c09121c6e64512586a2fbe43b8fa9aa30a1d7983e7ce15973fb1048629; F remains the frozen original. After meaningful cumulative cleanup, build the library tests and service, retain every baseline test name, run relevant checks and repeat the same14-scene measurement. Review the result guide in ownership order: SDK compiler/frame protocol, Main lowering/host integration, source-specific demand, and value kernels/tests. Review the seven checkpoint commits independently if needed. Address new review findings with a new checkpoint and relevant verification; do not relabel the fixed43ba91c201 evidence as testing later production edits.

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

Revision November13 02:00 Asia/Shanghai: continue the authorized overnight simplification after fixing the first measured result in Git; preserve its evidence and use it as the next comparison baseline.


## Continuation: remove empty control work and inspect hidden dead code


The 85da8c2a85 checkpoint completed995 scoped checks (994 pass, one unchanged known failure),16 additional existing AST/control/diagnostic checks, both builds, format and lint. Its service and source proof are preserved in evidence/path-unification/unused-private-helpers. It was not SQL benchmarked: the next comparison will use the final cumulative source.

First remove empty type branches and infallible private result wrappers in AST demand, diagnostics, native view selection and Decimal fast inputs. COALESCE can move its already-owned selected value directly. Preserve child demand, formatting panics, unsupported outcomes and all handle checks after callbacks. Next remove immediately invoked closures that only return their expression; keep closures that contain early returns or question-mark propagation needed before callback revalidation.

Restore dead-code diagnostics in tikv/mod.rs and rewriter/preparation.rs, which currently suppress warnings across whole modules. Compile before deciding whether newly exposed private remnants can be removed; preserve public APIs, existing test cases and fixtures. Save each independent source checkpoint and push for review. Use evidence/path-unification/control-result-cleanup for new receipts, never overwrite the prior measured round.

The final validation selection is the previous995 plus16 existing AST/control/diagnostic tests and two arithmetic rewriter tests (1013 total), retaining the2441-name inventory. Repeat the release offline library/service commands above and make lint. Then run the same14-scene matrix once on cumulative source, with pinned43ba91c201 SHA783723c0 as identical A/B controls and the original frozen service as F. A later checkpoint does not inherit the85da runtime results.

Revision November13 02:33 Asia/Shanghai: record intermediate functional completion and next cumulative cleanup before applying it.


Revision November13 02:38 Asia/Shanghai: the f39e38fbd3 intermediate library build passed (278.866s). Restored warnings identified four unused private production helpers, three cfg(test) helpers and two unconstructed private regexp selectors; active legacy regexp dispatch is retained. The compiler also proves a repeated Decimal coercion arm and two closed-type fallback arms unreachable. Remove those arms, a dead outer Ok and one unused private limits argument. Both added arithmetic rewriter checks passed on the pinned85 baseline. Run1013 scoped checks on the final cumulative build; all existing test functions and fixture values remain.


## Cumulative control-result cleanup outcome and next scope


At 2026-11-13 03:23 Asia/Shanghai, the cumulative sourceffb508eeae is validated and measured. Relative to the preceding measured43ba91c201 source it removes501 production and86 test-helper lines; every test function and sample remains. The final1013 checks complete1012pass/1sameknown failure; unit build280.273s and service build605.013s both exit0. No new SDK run is claimed; exact1574-input inheritance remains unchanged.

The first preflight stopped before timing:39 comparisons differed only in114 estRows fields, including the identical A/B binary. Preserve the failing receipt and field-by-field proof. After one same-owned-data restart, the unchanged strict56 preflight passes. The successful measurement exits0 in998.168s with224 cells and140160 full queries;448 before/after records and672 observations pass audit. Both owned temporary database directories and all owned services are removed. See [the control-result report](expression-control-result-cleanup-results.zh.md) and [its data](expression-control-result-cleanup-results/) for all favorable/adverse results. These records do not establish a universal performance improvement.

Next, use evidence/path-unification/resume-dispatch-cleanup and preserve this completed round. WorkMacOperator's read-only review identifies two small safe changes: reuse the task returned by validate_row_reply in resume_numeric_parent, and replace the niladic start's repeated task reads with one match. Both numeric parent callers retain reply validation; the ImmediateGuard still cancels on every return or unwind. Remove seven remaining immediately-invoked single-call closures in batch.rs, CONCAT, eager/user-variable and WEIGHT_STRING adapters; keep every closure that sequences fallible work before revalidation. No public interface, callback order, post-callback authentication, fixture or test function changes. Do not introduce carrying structures or broad parameter churn just to remove one validation from the21 single-entry adapters.

Save and push this small source checkpoint, run the same1013 selection on its own pinned unit binary and build its own service. Preserve the2441-name inventory, root lint, edition2021 formatting and protected SDK/frozen/BUILD snapshots. If those gates complete, use this round's pinnedffb508eeae service SHA2567e2b08d36cbf55adcc6701a08315135dad8fd0f813f5eb694ab31eb9df76d4b2 as identical A/B controls for the unchanged14-scene matrix. Earlier results cannot be relabeled as validation of later edits. Human acceptance remains pending.


## Six-file dispatch cleanup outcome


Source6765b4efa267025ab916d05f9040fab2ffe09b1c changes six production files,48 additions/92 deletions (44 fewer lines), with no test or fixture edits. Relative to43ba91c201, the post-first-round cleanup totals545 fewer production lines and86 fewer test-helper lines. WorkMacOperator independently reviewed the niladic dispatch, validation reuse and seven direct calls; the runtime evidence below belongs to this exact source.

The release unit build completed0/280.175s, the1013 selection completed1012pass/1sameknown failure in1038.210s, and the service completed0/603.842s. The2441-name inventory, six-file formatting, root lint and source/isolation checks pass. No new SDK/full2441/Go/datatype or allocation-failure-injection claim is made.

The final comparison completes224 cells/139985 full queries; strict56 preflight and448 persisted before/after records pass audit. All owned services and both temporary database directories are removed. [The final dispatch result](expression-resume-dispatch-results.zh.md) records every scene, favorable/adverse direction, absolute metric and paired range; [the evidence](expression-resume-dispatch-results/) includes a standard-library verifier. The prior28b19112e3 result remains separately fixed. Retain the simplification as requested, without inferring universal no-regression or completed project-wide unification. Human review remains pending.

The remaining21 single-entry adapter checks were not traded for new state-carrying structures or extensive parameter changes. Multi-entry adapters and callback-related checks still protect real boundaries; do not delete them using only a single caller's assumptions. This closes the reviewed cleanup scope, leaving broader API/ownership redesign for a separately specified change.


## Final remaining-logic follow-up


WorkMacOperator independently verified the2897e0289d report and requested another bounded review. Two remaining numeric resume closures contain only a single call and an immediate return of its result. Replace them with direct calls in batch_numeric_parent.rs and batch_numeric_window.rs. Preserve the authentication after each call and propagate the result only afterward. This changes no API, state branch, callback, test, or fixture. Remaining multi-step closures retain their local error domains and early returns.

- [x] Remove the two single-call wrappers.
- [ ] Build, validate the same1013 selection and separately measure the final source against pinned6765; preserve all earlier evidence.

Evidence belongs in a new numeric-direct-call-cleanup directory. The earlier2897 report remains an immutable completed result; this follow-up is not yet validated.
