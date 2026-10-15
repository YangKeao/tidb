M14 validation, from paired checkout repositories.

Build/test environment (empty RUSTFLAGS is intentional):

    RUSTC_BOOTSTRAP=1 RUSTFLAGS='' CC=gcc-14 CXX=g++-14 CXXFLAGS='-include cstdint' CMAKE_POLICY_VERSION_MINIMUM=3.5 CARGO_HOME=/home/agent/tidb/expression-unification/cargo-home CARGO_TARGET_DIR=/home/agent/tidb/expression-unification/target-tidb cargo test --release --locked -p tidb-expr --lib real_decimal_comparisons -- --nocapture

Run from TiDB rust/. Copy the resulting binary into `current-bin` before any next build. The retained focused.log is the final passing invocation; compilation-only API mistakes during test authoring preceded it.

    RUST_MIN_STACK=33554432 ./current-bin evaluator::batch::tests --test-threads=1
    RUST_MIN_STACK=33554432 ./current-bin --test-threads=1

`tidb-batch.log`: 27 passed. `tidb-full.log`: 1378 passed, same four baseline failures. No new failures.

From TiKV root, the same environment with CARGO_TARGET_DIR=/home/agent/tidb/expression-unification/target-tikv:

    cargo test --release --locked -p tidb_query_expr --lib local::profile_tests

`make lint` from TiDB root and rustfmt edition 2021 skip_children checks on the modified Rust files are retained separately. `git diff --check` run in both repositories. No Go/Bazel input changes, so no bazel_prepare required. No fresh server, FIPS, real PD/TiKV differential or workspace clippy success claimed; prior workspace clippy had an unchanged file_system compiler ICE.

Frozen native build: original production TiDB364aef2bab5cc633ecb76a775ae8f36f86a6687d + TiKV548812e1ef57aef077a2062a9cc356640a6347f5. Only the archived test-only probe and its tests/mod.rs module registration are modified. Build from /home/agent/tidb/bench-original-native/tidb/rust with above environment but CARGO_TARGET_DIR=/home/agent/tidb/bench-original-native/target and `cargo test --release --locked -p tidb-expr --lib --no-run`. Expanded probe adds second Decimal column and fourteen comparisons; no production changes. This has a new binary hash and is not the old M13 native binary.

TiKV: 51 focused tests pass, full library 1027 pass / same six known failures. Full lib command: RUST_MIN_STACK=33554432 /home/agent/tidb/expression-unification/target-tikv/release/deps/tidb_query_expr-7e6179f0aa3abd5b --test-threads=1. Full failure names retained in tikv-full.log; TiDB in tidb-full.log. No new failure.
