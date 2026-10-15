# Validation and cohort

TiDB from rust/, using:

    RUSTC_BOOTSTRAP=1 RUSTFLAGS='' CC=gcc-14 CXX=g++-14 CXXFLAGS='-include cstdint' CMAKE_POLICY_VERSION_MINIMUM=3.5 CARGO_HOME=/home/agent/tidb/expression-unification/cargo-home CARGO_TARGET_DIR=/home/agent/tidb/expression-unification/target-tidb cargo test --locked -p tidb-datatype --lib direct_cell_projection -- --nocapture

197056 independent old/new projections agree on every logical field and initialized word. All integer/fraction count pairs0..81 fitting9words; visible result scales0,1,8,9,10,30,81,127; both signs; all-zero, max-word padding, deterministic random and leading-zero patterns; invalid inactive words ignored. Every result also passes its exact per-owner word budget and rejects one byte below it. Visible scales are targeted boundary representatives, not every possible header value.

Full TiDB datatype:

    RUST_MIN_STACK=33554432 /home/agent/tidb/expression-unification/target-tidb/debug/deps/tidb_datatype-e28221b4478b7fe2 --test-threads=1

514 pass. Shared TiKV datatype from its root, same environment except CARGO_TARGET_DIR=.../target-tikv:

    cargo test --locked -p tidb_query_datatype --lib

564 pass. Core existing arithmetic/physical-codec functions are unchanged; the new method is consumed by TiDB. TiKV expression library's1028/same6 result is the preceding M15 check, not a fresh M16 claim.

TiDB release from rust/, same environment:

    cargo test --release --locked -p tidb-expr --lib --no-run

Copy resulting libtest to after-bin, then:

    RUST_MIN_STACK=33554432 ./after-bin evaluator::batch::tests --test-threads=1
    RUST_MIN_STACK=33554432 ./after-bin --test-threads=1

Batch28 pass; full1379 pass/same4 baseline failures (names in tidb-full.log). make lint and scoped rustfmt edition2021 skip_children checks pass; git diff --check in both repos passes. No Go/Bazel inputs change. No fresh server, PD/TiKV, FIPS or full workspace clippy success claimed.

Before binary: M15 production pair TiDB6e46687c0dc1771ca4084f44c935534099b83868 / TiKV8b210d8330eb7afecf973f6ccee0df059e76eb03. After binary: report's commit plus its paired TiKV commit. Both use batch_tikv, same SQL/data/schema/defaultLongLong, three alternating before/after pairs. This is a real code change comparison, unlike the separately labeled current-fallback ablations in M13–M15.

Frozen native: original production pair TiDB364aef2bab5cc633ecb76a775ae8f36f86a6687d / TiKV548812e1ef57aef077a2062a9cc356640a6347f5, with M15 test-only probe and tests/mod.rs registration only. No production edits. Reuses the exact M15 frozen binary; fresh timing with default LongLong schema. Archived source ../../evidence/original-native-narrow-integer-performance-m15.rs SHA256325197d64b4059be2981a1c1e08b51114c0da4f143c4edfb01d64fe952abc93d. Binary hashes in binary-hashes.txt; reproducible build command/environment in M15 validation.md.
