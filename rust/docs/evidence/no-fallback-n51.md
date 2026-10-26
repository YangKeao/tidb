# N51: original Constant Datum/error pairs through the shared frame

Based on local5a30922d56. Overall migration is unfinished and public interpreter entries have not switched. The common host contains the opt-in error-value channel; its production Projection consumer defaults to disabled capture, while the genuine borrowed scalar consumer and enabling/draining methods remain cfg(test) until public caller contracts are complete.

## Behavior and ownership

A parameter-read or deferred-child failure returns the original Constant cached planning Datum. A deferred-conversion failure returns the actual unconverted parameter/child Datum. The host now retains that actual root conversion value after existing token validation, using validated task metadata, node0, Output role and the real invocation row. The consumer takes it atomically with the original native error; foreign captured-value origins cannot consume the pair, and a pending failure blocks another callback. Read/child failures copy only the genuine borrowed Constant.value. No child is evaluated twice, and no substitute expression, Suite, Chunk or SQL FieldType is constructed.

An explicit capture request bounds additional conversion/error-Datum copies before the original converter can clone an unconverted large payload. A cached planning snapshot is not copied by source preparation or ordinary error consumption. The ordinary disabled consumer preserves its existing SQL error. Error payload, task and typed-cache retention keep the existing limits; this is a conservative logical policy, not allocator peak accounting.

Changed Rust files are evaluator/batch_hosts.rs, batch_context_value.rs, batch_scalar_source.rs, batch_public_source_tests.rs (one new test-module declaration only) and the new batch_public_error_tests.rs. Original Expression, ScalarFunction, Constant, Column, context value algorithms, SourcePlan compiler and comparison bodies remain unchanged.

## Independent original evidence

Thirteen files were archived at5a30922d56 before observers in workspace evidence/no-fallback/n51-before/manifest.json. The original error-value corpus3240/FB8415D6D51EBFBD was captured, independently rerun and pinned before changing the channel. Exact original observer-prefix SHA256 is c89b4b78a158c8ef81016393664c51fcfe2144fad5e8b207015667e75b723d19. It distinguishes outer and inner cached values, live parameters, actual physical Int10/NULL cells, negative/wrong parameter orders, missing result declarations, nested child failures and actual conversion errors.

The real existing ordinary shared consumer reproduced the lost-Datum RED at record0: NULL instead of the actual cached planning string, with the original error and callbacks unchanged. Both that3240 corpus and the earlier6720/9DB38EEAA92DDE88 public error-pair corpus now match the shared frame unchanged:9960 fixed error-pair observations. The prior ordinary6720/E96DA50FA24BFFC5 and original direct-function5880/2550BA35CB389B63 contracts also remain unchanged; direct function roots are not yet migrated.

Additional controls verify atomic cause/value consumption, wrong node/occurrence/physical-row rejection without consuming the captured pair, same-row failure/NULL/failure recovery, preservation of the original opaque error cause identity, and one compiled plan recovering after a callback unwind. Default-disabled versus explicit-capture tests distinguish SQL failure from bounded2MiB payload refusal at the same64KiB limit. Those large-payload cases are finite-policy evidence, not uncapped success/error-Datum equivalence and not exemptions from either fixed corpus.

## Validation

From the TiDB worktree:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib public_source_tests -- --nocapture
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
    make lint

Public15/0, aggregate346warnings unchanged. Current frontend test binary runs43 existing tests individually with --exact --nocapture --test-threads=1: dynamic_leaf_tests, typed_dynamic_tests, batch typed_context_tests and the affected dynamic-leaf/dynamic-diagnostic/deferred-typed/char-cast-context groups. All43 pass; exact names/status/durations are retained in workspace evidence/no-fallback/n51-context-focused.json. This focuses on actual unchanged production-default and changed explicit error-capture paths.

Projection52/0,28.32seconds, terminal0. Current cached executor expand::tests --nocapture --test-threads=1 gives6/0 plus unchanged64/B42FB8BE316E6C79. Current cached frontend exact evaluator::batch::tests::stateful_tests::remaining_optional_admission_inventory_keeps_original_behavior gives83088cases/0refused/0names, a bounded witness rather than global coverage. Fresh lint terminal0. Both pinned nightly-2026-01-30 and nightly-2026-08-22 rustfmt --check --edition 2021 --config skip_children=true pass for all five Rust files.

The whole frontend was not rerun for this opt-in channel. The preceding signed N50 baseline remains2246pass/exact inherited3fail/95ignore/all705batch/346warnings; it is prior evidence only. All affected current public/context/executor tests above were rerun. SDKad73b11 remains clean/unchanged and its documented N16 baseline1148/exact inherited1/1ignore/17warnings and clippy171/89 is reused without claiming a new SDK run.

Workspace n51-final-check.py --gates verifies both exact original observer prefixes, all original archives, unchanged oracle/compiler bodies, the explicit-only capture control, five reviewed source hashes, protected untracked BUILD and SDK ownership, and these focused gates. No source limits/digests/inherited failures were rebased. No push, PR or release.

## Remaining work

Next compile a genuine borrowed ScalarFunction root without wrapping it in a fabricated Expression, preserving its absence of the enclosing Expression enum/set integer view. Public entry replacement still needs the full actual caller contract, then AST/filter/join/Unistore integration, generic fallback deletion, fresh global/service verification and final unified performance. The test-only scalar prototype is not a final cache design or performance result.
