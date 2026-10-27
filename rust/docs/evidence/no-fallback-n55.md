# N55: reuse genuine scalar preparation in actual filters

Validated local checkpoint from main4a7bfdba9d; SDK3ee7511 remains clean and unchanged. Overall task4 is still in progress.

The actual row-based filter and the general scalar branch of VecEvalBool now prepare a genuine immutable expression once, on its first demanded physical row, and reuse the existing SDK cursor program for the remaining live rows. Each row still supplies its actual Row coordinate and current Columns; each Datum is converted to truth immediately before evaluating the next row. Filter-major order, NULL bookkeeping, IN-derived equality behavior and final input-selection application remain structurally unchanged. Empty inputs and filters suppressed by earlier predicates do no preparation. Existing typed-vector, constant-once and correlated-once phases are unchanged.

PreparedScalarExpression borrows the real root, retaining neither a context nor an input Chunk. Its constructor factors the existing public scalar preparation; ordinary and error-Datum public wrappers use that same constructor and retain their Prepare/Invoke error mapping. Direct ScalarFunction behavior and the existing scalar source compiler/execution loop are unchanged. Compilation limits and the public64MiB SDK execution/materialization policy are unchanged. Resource failures remain terminal, with no retry through the old interpreter. The independent old filter reference is available only under the existing explicit cfg(test) oracle scope.

Before any N55 source edits,399 source/plan files were archived. Five executed source/test files were archived again immediately before production promotion. The independent original corpus retains25920/9C47E8782A22C4DF, covering15 real filter shapes, live/deferred parameters, assignment side effects, seeded RAND, missing declarations, errors and exact panic payloads; all four public filter modes,0/1/7 physical rows, six values, three context profiles, sequential parameters and four selection forms are included. Out-of-range selection retains its original panic and prior effects. The early draft recorded only a generic panic marker; the final exact-payload corpus was captured and frozen before changing production.

The original and current per-row shared implementation passed the complete independent comparison before promotion. Three additional controls then reproduced repeated compilation (8vs1,8vs1,7vs1). An initial new control assumed eight parameter reads for eight rows; the actual original probe/general-demand behavior reads sixteen, so that new assumption was corrected before production. No reads were removed. After promotion all five semantic/control tests pass, including resource/no-replay, callback panic recovery and root edits between calls. A sixth test compares actual filtering with public per-row execution using the same current SDK compiler, values and caps, and verifies rows-versus-one preparation counts for32/1024/8192 rows.

Fresh full frontend:2283pass/exact inherited3/95ignore in918.74seconds; all733 tests under evaluator::batch pass. The inherited failures are exactly pushdown_catalog::tests::ifnull_string_column_literal_uses_go_signature_and_column_collation, tests::builtin_info_json_math_source::exp and tests::builtin_math_misc_op_source::vectorized_builtin_op_func. The individual warning-title Counter is unchanged at346 against N54. The same complete run covers all50 public-filtered test names,43 established context tests and the fixed83088/0/0 optional admission inventory; these are extracted same-run coverage, not additional focused executions.

Actual Projection52/0 in215.30seconds, Expand6/0 with the existing64/B42FB8BE316E6C79 fixture, production-only check, lint and both pinned nightly formatters over all five touched Rust files pass. The reconstruction checker verifies all399 baseline archives, pre-promotion executed sources, the exact filter scheduling substitutions, unchanged scalar compiler/SDK frame/limits, frozen fixtures, source hashes and the unrelated protected BUILD file. It passes before the local DCO commit and is rerun afterward. SDK N53's1150pass/exact inherited1/1ignore/17warning Counter and clippy171/89 remain prior unchanged-SDK evidence, not a new SDK run.

Isolated timing was collected only after every build/test and the delegated long-test child exited, pinned to CPU8 with one test thread. Seven alternating repeats compare current public per-row preparation with the optimized actual filter, both using the same current shared compiler/SDK, NoColumns, genuine input rows and plus(column,2). It is a release crate observation including test counters, not an old-binary or SQL service benchmark; no speed threshold determines test pass.

| Physical rows | Per-row public median ms | Prepared filter median ms | Time saved |
|---:|---:|---:|---:|
|32|0.26031|0.09566|63.3%|
|1024|6.46940|2.26408|65.0%|
|8192|51.96752|18.02313|65.3%|

Raw seven-repeat arrays are in workspace evidence/no-fallback/n55-isolated-timing.json; timing and every measurement retain their original logs and exit files. Reproduce from this TiDB checkout:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib evaluator::filter_prepared_tests::
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb check --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr
    make lint
    taskset -c 8 ../target-tidb/release/build/tidb-expr/f60c45dba483c7a1/out/tidb_expr-f60c45dba483c7a1 --exact evaluator::filter_prepared_tests::filter_reuse_matches_public_per_row_execution_and_reports_timing --nocapture --test-threads=1

This reuses a borrowed plan within one filter invocation; it does not cache a mutable public AST globally or hold contexts/rows across calls. Actual typed-vector behavior is unchanged. Owned-root Unistore reuse, other public/AST/typed execution, generic Admission fallback removal and final service/performance gates remain open. Task4 is not complete.
