# N1: Preserve terminal expression preparation errors

Baseline TiDB bf4723283aadba5fbde116a7572c2aac18a887ba / TiKV d32cebb3ebc353748cb225fe254d6893e62c8bdc.

A 1MiB source metadata element made the old public batch entry succeed through generic evaluation. The new regression fails on that baseline. The fixed entry returns the original structured ResourceLimit/Prepare failure on both invocations, retaining the same cause ownership; no fallback, input lookup, output write, or second preparation occurs. Other non-admission errors also become cached terminal failures. Execution errors remain non-replayed.

Existing IS NULL(JSON) used a catalog selector requiring JSON-to-String conversion but did not insert a cast. Validate exact shared carrier types before compilation, classifying this unfinished capability as admission. Do not reinterpret arbitrary InvalidSpec failures as unsupported SQL.

Validation from TiDB root, using RUSTC_BOOTSTRAP=1 RUSTFLAGS='' CC=gcc-14 CXX=g++-14 CXXFLAGS='-include cstdint' CMAKE_POLICY_VERSION_MINIMUM=3.5 CARGO_HOME=/home/agent/tidb/expression-unification/cargo-home CARGO_TARGET_DIR=/home/agent/tidb/expression-unification/target-tidb:

    cargo test --manifest-path rust/Cargo.toml --release --locked --offline -j2 -p tidb-expr --lib preparation_resource_failure_is_cached_without_entering_another_evaluator
    cargo test --manifest-path rust/Cargo.toml --release --locked --offline -j2 -p tidb-expr --lib evaluator::batch::tests
    /home/agent/tidb/expression-unification/target-tidb/release/deps/tidb_expr-a6e9181eda8c8093
    make lint
    rustfmt --edition 2021 --check rust/crates/tidb-expr/src/evaluator.rs rust/crates/tidb-expr/src/evaluator/batch.rs rust/crates/tidb-expr/src/evaluator/batch_tests.rs
    git diff --check

Batch35 pass. Full1387 passed/4 failed/95ignored; exact same inherited failure identities as M30: pushdown_catalog::tests::ifnull_string_column_literal_uses_go_signature_and_column_collation, tests::builtin_info_json_math_source::exp, tests::builtin_math_misc_op_source::vectorized_builtin_op_func, time_fn::tests::str_to_date_partial_formats_follow_no_zero_date. No fresh server/PD/TiKV/benchmark claim for this narrowly scoped error-routing change.

Raw logs (including first candidate's JSON regression) retained in agent workspace evidence/no-fallback/. Ordinary unsupported-expression fallback has NOT yet been removed; this is a first runnable checkpoint, not completion of the elimination goal.
