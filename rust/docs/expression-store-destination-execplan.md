# Evaluate shared destination construction for native value entries

This completed ExecPlan follows root PLANS.md. Progress, Surprises & Discoveries, Decision Log, and Outcomes & Retrospective record the actual outcome, including a failed initial direction.

## Purpose / Big Picture

The original goal is to unify expression execution paths without losing performance. This trial tested whether filling a final value slot directly removes a whole-entry temporary and memory copy in the existing shared store. It was withdrawn at the actual-service artifact gate. No new retained optimization or source reduction resulted.

## Progress

- [x] Read directive443ecd76; claim task21 and declare mechanism/falsification ef0e4877.
- [x] Preserve original9521 source/configuration inputs and original protected-file hash.
- [x] Identify absent reuse-branch samples in old annotation; terminate reuse-only build with actual exit-15 after81.689613seconds, before second build.
- [x] Declare corrected append+reuse scope568fb065; fix candidate2 and independent source review da47a706.
- [x] Build actual service17, exit0 after558.327177seconds, preserve ELF and raw disassembly.
- [x] Falsify whole-entry elimination: observed append path still copies112-byte vacant Entry, then overwrites80-byte real payload.
- [x] Restore owned source and exact original9521 inputs, HEAD/status/protected file; no rebuild after identical restoration.
- [x] Candidate unit/runtime/lint/format/performance intentionally not run because artifact gate failed; prepared scripts are not execution evidence.
- [x] Prepare complete results and document-only submission. Report/archive delivery and actual local commit identifiers are tracked in separate delivery/postcommit receipts, outside the immutable archive.

## Context and Orientation

Repository /home/agent/tidb/expression-unification/tidb started at91c51fceeab4a241c1b3455f9a1b944ceafac42b. Sibling SDK tikv is clean atf7672bf980143f2938fd0f900650a4c29091e70f. The retained actual service14 has SHA2d435bce2ba0ea2c153df91b5e6dbd82a5200d78c20bb09ee3dac8c993d75fea. Mutable target-tidb/release/tidb-server is now rejected17 and must not be used as the retained baseline.

In rust/crates/tidb-expr/src/evaluator/batch_values.rs, NativeValueStore::insert_with validates storage liveness, slot width/version and new capacity, invokes a private value supplier, then registers Entry and returns an authenticated handle. Entry is112 bytes in the actual service and common Datum transport80 bytes. The five suppliers are cloned/owned SQL, Decimal fast outcome, diagnostic unavailable and diagnostic text; no user-context callback is introduced.

## Plan of Work and Concrete Steps

Candidate2 changed only the Entry construction block. It called make_value after the unchanged checks and try_reserve, before any store mutation. New slots received a fully initialized vacant Entry in already reserved space; append and reuse then shared field writes. No inline attribute, ownership-consumption rule, ABI, kernel, authentication or SDK change occurred. Candidate13add/9delete/net+4 production lines, tests0; the full-file inverse was byte identical.

From agent workspace /home/agent/.slock/agents/02fef0a9-76ce-4afe-9bce-73eebe82ff7d:

    python3 evidence/remove-demo-policy/resume-nov05/job_runner.py evidence/path-unification/store-entry-destination/server-build2.json
    python3 evidence/path-unification/store-entry-destination/pin_service.py
    python3 evidence/path-unification/store-entry-destination/inspect_store_codegen.py

The first command ran ../tools/cargo-tidb build --manifest-path rust/Cargo.toml --release --locked --offline -j1 -p tidb-server --bin tidb-server --no-default-features from repository root, empty RUSTFLAGS/CARGO_BUILD_JOBS1. Actual child receipt is complete/0/558.327177s. Saved service17 SHA6dd7d6903643dbbac5a720f20b41d327b66782468b7d7e93343f79b6dc4f5dfe. Inspection preserved raw code of actual retained and candidate functions; symbols were matched exactly and instruction rows counted by mnemonic column.

Do not rerun these commands against the restored current source and call the new binary the same experiment. Candidate source, snapshots and saved actual ELF remain evidence; guarded restoration is final.

## Validation and Acceptance

The declared first gate required the observed append path to eliminate whole Entry staging/copy. In service17 at0x45cbba7 the None marker is written to stack0x60;0x45cbbe3..0x45cbc33 copies seven16-byte pieces from stack0x60..0xc0 to Entry offsets0..0x60. Then0x45cbc9b..0x45cbcc8 writes five16-byte actual-value pieces. Thus the hypothesis fails. Reuse removes the aggregate temporary locally, but that is insufficient for the observed hot append path.

insert_owned nevertheless shrank1933 to1709 bytes,370 to345 mnemonic instructions and344 to312 stack-reservation bytes. Entire .text shrank78390112 to78389280 bytes. Caller remained6286 bytes with1896-byte stack reservation. These are static properties, not measured execution gain or regression.

Had the gate passed, the declared minimum was original973 selected consumers including11 existing store/lease tests, unchanged2440-name inventory, exact inherited failure comparison, root make lint, two pinned Rust formats and fixed-input checks; then only five SQL scenes deep16/integer/Decimal/text/scan, A/Bsame14,C17,F99a, four balanced orders,20 strict observations/80cells, legal configGET and fresh16-second actual quiet. These were not executed for this candidate. No new full2440, SDK, numeric5.58M matrix, profiler or allocation-failure injection is claimed.

## Surprises & Discoveries

First reuse-only scope lacked observed hot-path support: retained insert-owned annotation had zero nonzero rows in0x45cbc09..0x45cbd00, while append addresses had samples. Zero samples do not prove zero cost. The first build was deliberately terminated, never reported as successful. The common destination revision made some real static improvements, but preserved a112-byte vacant-entry copy and an additional final value write.

Source-only independent review confirmed capacity, ownership and error ordering, original tests and all9521 inputs. It did not execute builds, tests or performance. The stale reuse-only prose was corrected before sealing; both original failed direction and actual final scope are recorded.

## Decision Log

Decision: withdraw at the predeclared actual-artifact gate; do not run a broad matrix to seek benefit from a mechanism that failed its first requirement.
Rationale: actual append path retains the target aggregate copy despite local static shrink.
Date/Author:2026-11-10 ExpressionReuseEngineer.

Decision: preserve first-build signal-15 and both source versions, but accept only actual second-build exit0 as compiled candidate.
Rationale: truthful provenance and an early correction to the sampled hotspot.
Date/Author:2026-11-10 ExpressionReuseEngineer.

## Idempotence and Recovery

Original file batch_values.before.rs was restored only after current bytes matched candidate2.rs. Temporarily remove only this owned plan to prove snapshot equality to initial-input.json, including git status, then recreate this result document. All9521 source/configuration files, SDK/native HEAD/status and protected BUILD remained original. No recompilation after identical restoration. No trial services, fixtures or database directories were created. Never modify prior archives or rust/third_party/tikv-client-rs/tests/client_go_differential/BUILD.bazel.

## Outcomes & Retrospective

New retained production/test reduction0/0; candidate performance is unmeasured, not0percent. Cumulative retained production2134added/7293deleted/net5159fewer; tests5492added/5684deleted/net192fewer. The broader unified-path/no-loss goal remains incomplete. Saved actual14 remains the next retained baseline. Report and raw evidence precede an authorized local document-only DCO; no push/PR/deployment.

## Artifacts and Notes

Evidence directory evidence/path-unification/store-entry-destination contains initial/candidate input hashes, source proof, both actual build receipts, raw function bytes/disassembly, branch annotation, independent reviews, restoration and decision. REPORT.zh.md explains scope in Chinese. Prepared functional/measurement scripts are explicitly unexecuted; no generated success records stand in for actual terminals.

## Interfaces and Dependencies

No signatures, handle ABI, storage representation, dependency or SDK change survives. FnOnce()->StoredValue and all original authentication and callback/lease rules remain unchanged.

Revision note: completed actual-artifact rejection and guarded restoration; historical reuse-only scope was superseded before candidate2 compilation and never retained.

Final independent actual-artifact/restoration review867ec416 PASS confirmed saved14/17 ELF,112-byte placeholder plus80-byte final writes, both actual build terminals and9521-input restoration. Raw review is preserved; it did not rebuild, test or time. Report/statistics delivered in f93e1962 before local submission.
