# N50: shared source plan, real scalar-row proof and comparison phases

This is a local refactor checkpoint based on8536971ff7. The overall migration and final performance work remain unfinished. No public scalar interpreter entry has switched.

Production changes are in tidb-expr/src/evaluator/batch.rs, batch_hosts.rs, batch_decimal_fast_source.rs and ops.rs. SourcePlan owns common detached compiler metadata; BatchSource retains the actual Projection program Arc/root/slot/output/schema seal. The same leaf primitive serves native Chunk execution and the cfg(test) borrowed scalar proof in batch_scalar_source.rs. No expression/Suite/Chunk adapter or SQL input/output declaration is fabricated. PhysicalRow width is distinct from a complete SQL schema. The byte FieldType is only the existing authenticated handle transport.

The production comparison task now uses the original integer_binary_typed worker for the initial all-Int-declared probe, then requests both operands fresh if it declines. The actual generic ladder no longer repeats that probe. Only ordinary Eq/Ne/Lt/Le/Gt/Ge roots with two actual Int declarations and a LongLong result, without PB provenance, gain the additional context profile. Raw operand demand, selected physical rows, window boundaries, errors and row-prefix versus atomic publication are preserved.

## Independent original evidence

Thirteen actual baseline files were archived before observers/factory changes in workspace evidence/no-fallback/n50-before/manifest.json. Original public contracts were captured, independently rerun and pinned before production changes:

| Actual original caller | Records | Fixed digest |
| --- | ---: | --- |
| Expression Datum/error pair |6720|9DB38EEAA92DDE88|
| Direct ScalarFunction |5880|2550BA35CB389B63|
| Ordinary Expression |6720|E96DA50FA24BFFC5|

The exact original observer prefix has SHA256 d773c63e0d0d315bf0d8511a5b38fc68327f60ddd0269591198cac77bd53aa45 and remains unchanged. The actual borrowed-source ordinary corpus matches all6720records. Original Expression/ScalarFunction/Constant/Column and other baseline oracle bodies remain unchanged. Direct function roots and error Datums are original evidence, not yet routed through the new proof consumer.

Regression logs retain the first ordinary record3528 mismatch (old parameter reads2/new1), the actual Suite admission/fallback RED, the late sentinel error mismatch, and the empty-Chunk scalar cursor RED. The fresh-value test uses Real1.5 followed by Int7 and checks the changed equality result1, all six comparisons, both parameter sides and vector flags. The publication test includes0/3/2051rows, duplicate/reordered selection and a final fresh-demand error across windows, without filtering cases or normalizing errors.

The ops baseline was separately retrieved and byte-verified against8536971ff7 immediately before its phase fix; it is not claimed as one of the initial thirteen captures. SHA256994fc9404c083f982b348a18eee385ac711593f78594bdde31813e93f46ea709; the final checker reconstructs its one intended edit exactly.

## Validation

From the TiDB worktree, run:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib public_source_tests -- --nocapture
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
    make lint

Targeted8/0. Whole frontend2246pass/exact inherited3fail/95ignore, all705batch tests pass, aggregate346warnings unchanged,836.65seconds, terminal101 solely for the inherited failures. Exact inherited failures remain:

- pushdown_catalog::tests::ifnull_string_column_literal_uses_go_signature_and_column_collation
- tests::builtin_info_json_math_source::exp
- tests::builtin_math_misc_op_source::vectorized_builtin_op_func

Projection52/0,78.53seconds, terminal0. Current cached executor expand::tests --nocapture --test-threads=1 gives6/0 plus unchanged64/B42FB8BE316E6C79. Current cached frontend exact evaluator::batch::tests::stateful_tests::remaining_optional_admission_inventory_keeps_original_behavior gives83088cases/0refused/0names; this bounded inventory is not global coverage. Fresh lint terminal0. Both pinned nightly-2026-01-30 and nightly-2026-08-22 rustfmt --check --edition 2021 --config skip_children=true pass on all seven Rust files. Git diff --check passes.

Workspace evidence/no-fallback/n50-*.log stores complete results. n50-reviewed-source-SHA256SUMS pins all seven reviewed Rust files; n50-final-check.py --gates verifies original archives, exact observer prefix and unchanged original tests, the isolated ops edit, genuine source and original seals, protected BUILD, SDK ownership and final gates.

SDKad73b11 is clean and unchanged; its documented N16 baseline is reused, not rerun:1148pass/exact inherited1fail/1ignore/17warnings and clippy171total/89expression warnings. The unrelated untracked BUILD.bazel remains protected. No branch push, PR or release.

## Limits and next work

The borrowed scalar consumer remains cfg(test) until every actual public caller contract is ready. It verifies context/row reuse, optional return declarations, real physical selection, empty-Chunk undemanded branches, width authentication and64KiB resource refusal followed by same-plan recovery. The2MiB source refusal is finite-policy evidence, not uncapped native success equivalence. Arbitrary invalid physical cursors and usizeMAX coordinate overflow are not certified. No caps, original digests or inherited failures were raised or rebased.

Next preserve the actual Constant cached-versus-unconverted error Datum and accept a genuine borrowed ScalarFunction root without a fabricated Expression. Then switch public entries and complete AST, filter/join, Unistore, generic fallback deletion, full service verification and final unified performance. Per-call prototype compilation is not a performance result or a final cache design.
