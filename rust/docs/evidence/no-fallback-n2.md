# N2: Compile complete pure projections

TiDB parent e0770c68 / unchanged TiKV d32cebb3ebc353748cb225fe254d6893e62c8bdc.

PreparedBatchSuite retains the immutable projection owner and one shared RPN program per computed output. Preparation covers all outputs, including literal and column roots; each execution validates every calculated destination before kernels and moves direct column owners only after all computed outputs succeed. The existing shared execute_chunk loop was extracted, not duplicated. No nested native EvaluatorSuite or expression-value callback is used. Disabling vectorized expression execution chooses one-row windows in the same shared model.

Selected direct columns use Chunk's raw physical-cell copy, preserving reverse/repeated selection and NULL backing. Input-owner aliases merge into a single copied destination, whose other output slots then share its owner, including across reset/reuse. Indices/layouts/live source and destination aliases are checked before the copy. The existing stable column-alias topology precondition remains. Suite-wide source-node and metadata bounds prevent multiplying a per-root allowance; retained caller materialization caps are divided among workers. These budgets do not claim to measure allocator peaks or external Chunk storage.

New expression cases independently specify six mixed outputs (Int arithmetic, LOWER, CHAR_LENGTH, literal, repeated direct column), both vector settings, dense/empty/2050 reversed/repeated selections and repeated invocations; a later overflow preserves source owners and never retries, malformed later output fails before kernels, leaf roots/direct-only projections stay compiled, and two roots cannot each consume a one-node suite budget. Two Chunk tests verify alias copy/reuse/nulls and failure without writes.

Validation:

- Release expression batch40 pass; full1392 pass/4 inherited failures/95ignored. Exact failure names match N1/M30; no new failure.
- Release Chunk275 pass/4ignored.
- Pinned-nightly production Projection caller tests9 pass (worker concurrency/order, direct columns, empty virtual rows, fresh execution state). This also compiles non-test expression/chunk libraries.
- make lint, scoped rustfmt and git diff --check pass. No fresh server/PD/TiKV/FIPS or timing claim.

Expression/Chunk commands from TiDB root used RUSTC_BOOTSTRAP=1 RUSTFLAGS='' CC=gcc-14 CXX=g++-14 CXXFLAGS='-include cstdint' CMAKE_POLICY_VERSION_MINIMUM=3.5 CARGO_HOME=/home/agent/tidb/expression-unification/cargo-home CARGO_TARGET_DIR=/home/agent/tidb/expression-unification/target-tidb with:

    cargo test --manifest-path rust/Cargo.toml --release --locked --offline -j2 -p tidb-expr --lib evaluator::batch::tests
    cargo test --manifest-path rust/Cargo.toml --release --locked --offline -j2 -p tidb-chunk --lib
    /home/agent/tidb/expression-unification/target-tidb/release/deps/tidb_expr-a6e9181eda8c8093

Those pure-crate tests additionally work on system rustc1.97.1. Full executor tests require pinned Rust1.100 nightly (the jemalloc intrinsic has a different signature on the system compiler). No allocator code was modified. From rust/:

    RUSTFLAGS='' CARGO_BUILD_JOBS=12 /home/agent/tidb/expression-unification/tools/cargo-tidb test --release --locked --offline -j12 -p tidb-executor --lib projection::tests

The initial offline attempt lacked locked paste1.0.15; an online --locked fetch populated the cache. The system-compiler attempt then failed in unchanged tikv_alloc; the existing pinned compiler wrapper resolved it. All attempts and successful logs are retained in agent workspace evidence/no-fallback/n2-*.log.

This checkpoint removes structural projection fallbacks, not the six unsupported-function benchmark fallbacks. The migration Unsupported branch and other legacy callers still exist and must be eliminated in later milestones. Compiled-route counters include pure direct-column plans; actual shared-program windows are separately observable.
