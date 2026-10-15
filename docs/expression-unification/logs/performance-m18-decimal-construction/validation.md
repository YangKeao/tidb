# Validation and cohort

Only shared Decimal fresh construction and constant-zero initialization change.
No arithmetic, physical/logical transport, mutating resize, registry or driver
semantics change. Existing tests cover resource preflights, wide hidden scales,
status metadata, cloning/ownership and initialized NULL backing; no new test
mirroring the constructor implementation is added.

Build environment, TiDB rust/:

    RUSTC_BOOTSTRAP=1 RUSTFLAGS='' CC=gcc-14 CXX=g++-14 CXXFLAGS='-include cstdint' CMAKE_POLICY_VERSION_MINIMUM=3.5 CARGO_HOME=/home/agent/tidb/expression-unification/cargo-home CARGO_TARGET_DIR=/home/agent/tidb/expression-unification/target-tidb cargo test --locked -p tidb-datatype --lib
    RUSTC_BOOTSTRAP=1 RUSTFLAGS='' CC=gcc-14 CXX=g++-14 CXXFLAGS='-include cstdint' CMAKE_POLICY_VERSION_MINIMUM=3.5 CARGO_HOME=/home/agent/tidb/expression-unification/cargo-home CARGO_TARGET_DIR=/home/agent/tidb/expression-unification/target-tidb cargo test --release --locked -p tidb-expr --lib --no-run

Use the same environment for both commands. TiDB datatype515 pass, including
197056 direct-cell normalization/export checks and130236 strict/wide exports at
six limits. Copy release libtest to after-bin:

    RUST_MIN_STACK=33554432 ./after-bin evaluator::batch::tests --test-threads=1
    RUST_MIN_STACK=33554432 ./after-bin --test-threads=1

28 batch pass; full1379 pass/same4 baseline failures. Exact names in tidb-full.log.
TiKV root, same environment except target-tikv:

    cargo test --locked -p tidb_query_datatype --lib

565 pass. In particular, test_private_grow_hidden_scales_and_resource_checks
checks usize/u32 count errors and hidden result/storage behavior; existing wide
comparison/hash/clone and physical/logical role tests remain passing. make lint,
scoped rustfmt edition2021 skip_children and both diff checks pass. Rust-only
inputs; no bazel_prepare required. No fresh server, PD/TiKV, FIPS, full workspace
clippy or TiKV full expression claim (M15 expression baseline1028/same6 reused).

Primary before: M17 TiDB016e8e230b8351eefe31fb44c5b9b2a83e0f09dd /
TiKV44befe011baf3082d18398b27428f47030a29be2. After: report's TiDB commit and
paired TiKV recorded below. Both use actual batch execution over identical data.
The separate int-reference-bin is actual M15 production TiDB6e46687c0dc1771ca4084f44c935534099b83868 /
TiKV8b210d8330eb7afecf973f6ccee0df059e76eb03; it measures four controls so earlier
Int/Time/Duration/LOWER regressions are not hidden by moving baseline. Seven75ms
samples, three alternating pairs, all five shapes in both primary and reference.

Frozen true original-native production TiDB364aef2bab5cc633ecb76a775ae8f36f86a6687d /
TiKV548812e1ef57aef077a2062a9cc356640a6347f5, with only M15 test probe/module
registration changed. Exact reused binary91f9c56c4a52ba68fd51976ceb287559ed2929f056a12ba570c5764720dcebe9,
freshly timed. Archived probe ../../evidence/original-native-narrow-integer-performance-m15.rs
SHA256325197d64b4059be2981a1c1e08b51114c0da4f143c4edfb01d64fe952abc93d.
Default LongLong schema matches current/probe inputs. Binary hashes in
binary-hashes.txt; machine/toolchain receipts retained. No compilation during timing.

## Decision: rejected and reverted

The candidate does not establish a clear gain. Decimal PLUS is156.558→158.373ns/row (paired ratio about0.99x), LT100.104→102.923 and NULL-safe equality99.938→102.966 (about0.97x). Other controls are flat or slower. M15 reference regressions remain visible. All correctness checks pass, but they do not justify keeping a performance-only source expansion.

The exact candidate.patch is retained for reproduction against TiKV44befe011baf3082d18398b27428f47030a29be2, paired with TiDB016e8e230b8351eefe31fb44c5b9b2a83e0f09dd production code. The patch was reverse-applied after measurement; TiKV git diff/status are clean at that original commit. No candidate production change is pushed. This report-only TiDB commit keeps runtime at the M17 code. Existing M17 validation remains applicable to the exact restored source; no redundant rebuild is claimed. The next small profile-guided experiment targets the generic vector append call, not arithmetic semantics.
