# Validation

TiDB, from rust/, with this exact build environment:

    RUSTC_BOOTSTRAP=1 RUSTFLAGS='' CC=gcc-14 CXX=g++-14 CXXFLAGS='-include cstdint' CMAKE_POLICY_VERSION_MINIMUM=3.5 CARGO_HOME=/home/agent/tidb/expression-unification/cargo-home CARGO_TARGET_DIR=/home/agent/tidb/expression-unification/target-tidb cargo test --release --locked -p tidb-expr --lib evaluator::batch::tests -- --nocapture

Final tidb-focused.log: 28 pass. Copied release binary as current-bin; full-library command:

    RUST_MIN_STACK=33554432 ./current-bin --test-threads=1

1379 pass, same four baseline failures (names in tidb-full.log). New test covers 112 operator/signedness/type combinations at each width's valid extrema plus four signed PLUS cases, including a result beyond the input width, NULL and 2050 reverse/duplicate selected occurrences.

TiKV, from its root, same build environment except target-tikv and debug test:

    cargo test --locked -p tidb_query_expr --lib local::profile_tests
    RUST_MIN_STACK=33554432 /home/agent/tidb/expression-unification/target-tikv/debug/deps/tidb_query_expr-b51fd3e71a71d50f --test-threads=1

Final tikv-focused-final.log: 52 pass. Full library 1028 pass / same six baseline failures. A prior release test invocation retained the obsolete Tiny/Long-must-refuse expectation; its one failure in tikv-focused.log predates replacing those negative cases with Year/BIT. The final debug suite checks the changed admission contract and all previous cases. The older signed LongLong row-only profile remains restricted and its Tiny/Long rejection tests remain unchanged.

TiDB make lint (lint.log), scoped rustfmt edition2021 skip_children checks in both repos (fmt logs) and git diff --check pass. Rust-only changes do not require bazel_prepare. No compilation during final timing; no fresh server, PD/TiKV, FIPS or full workspace clippy pass claimed. The prior unrelated compiler ICE is not resolved by this slice.

Frozen production pair: TiDB364aef2bab5cc633ecb76a775ae8f36f86a6687d / TiKV548812e1ef57aef077a2062a9cc356640a6347f5. Only the archived test probe and its tests/mod.rs registration differ. Build from frozen TiDB rust/, same release environment with CARGO_TARGET_DIR=/home/agent/tidb/bench-original-native/target and cargo test --release --locked -p tidb-expr --lib --no-run. Probe source is ../../evidence/original-native-narrow-integer-performance-m15.rs, SHA256 325197d64b4059be2981a1c1e08b51114c0da4f143c4edfb01d64fe952abc93d. It changes only benchmark schema via EXPR_BATCH_PERF_INT_TYPE. Binaries/hashes are distinct from M14; binary-hashes.txt records this cohort.
