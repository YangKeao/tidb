# Validation and cohorts

Build environment, from TiDB rust/:

    RUSTC_BOOTSTRAP=1 RUSTFLAGS='' CC=gcc-14 CXX=g++-14 CXXFLAGS='-include cstdint' CMAKE_POLICY_VERSION_MINIMUM=3.5 CARGO_HOME=/home/agent/tidb/expression-unification/cargo-home CARGO_TARGET_DIR=/home/agent/tidb/expression-unification/target-tidb cargo test --locked -p tidb-datatype --lib direct_ -- --nocapture

Two focused tests pass. The preceding197056 native-cell corpus now also compares exact old/new output cell bytes. A separate130236 strict/bounded/wide value corpus checks six logical byte-span limits each, comparing successful raw bytes and NativeDecimalError discriminants against the old Decimal::try_from_shared_math→to_my_decimal bridge. It covers independent result scale, inactive words, leading integer zeros, signed zero, wide zero prefixes normalizing into bounded output, retained wide fractions that cannot fit, and resource-boundary rejection. These are targeted corpora, not every possible owner state.

Full TiDB datatype:

    RUST_MIN_STACK=33554432 /home/agent/tidb/expression-unification/target-tidb/debug/deps/tidb_datatype-e28221b4478b7fe2 --test-threads=1

515 passed. TiKV datatype from its root, same environment with target-tikv:

    cargo test --locked -p tidb_query_datatype --lib

564 passed. No existing arithmetic/physical-codec function changes; new transport method is consumed by TiDB. TiKV expression1028/same6 is reused from M15, not a fresh M17 full-expression run.

TiDB release, same build environment:

    cargo test --release --locked -p tidb-expr --lib --no-run

Copy release libtest as after-bin, then:

    RUST_MIN_STACK=33554432 ./after-bin evaluator::batch::tests --test-threads=1
    RUST_MIN_STACK=33554432 ./after-bin --test-threads=1

28 batch pass, full1379 pass/same4 known failures; names in tidb-full.log. make lint, scoped rustfmt edition2021 skip_children, both git diff --check pass. Rust-only changes, no bazel_prepare required. No fresh real-cluster/server/FIPS or full workspace clippy success is claimed.

Primary before: TiDB01d879349f13a6c9b862b619283a1182ce4b9034 / TiKVd38538fbce0e7c97de0d98e598589e24c7529126 (M16). After: report's TiDB commit / TiKV44befe011baf3082d18398b27428f47030a29be2. Separate M15 Int/Datetime control: TiDB6e46687c0dc1771ca4084f44c935534099b83868 / TiKV8b210d8330eb7afecf973f6ccee0df059e76eb03. The reference-control scripts explicitly use int-reference-bin=M15, not the primary before-bin=M16, so the earlier regression cannot disappear by moving baseline. Each mode uses actual batch execution.

Frozen true native: production TiDB364aef2bab5cc633ecb76a775ae8f36f86a6687d / TiKV548812e1ef57aef077a2062a9cc356640a6347f5; only M15 test probe/module registration differs. Exact reused binary91f9c56c4a52ba68fd51976ceb287559ed2929f056a12ba570c5764720dcebe9, fresh timing. Probe ../../evidence/original-native-narrow-integer-performance-m15.rs SHA256325197d64b4059be2981a1c1e08b51114c0da4f143c4edfb01d64fe952abc93d. Default LongLong input schema and data match all current/reference probes. Full environment/build commands are in M15 validation.md; all current/reference/frozen hashes in binary-hashes.txt.

Final TiKV datatype565 passed after adding one cfg(test)-only guard for physical-cell admission versus strict logical export. It checks noncanonical head/tail padding, empty active prefix and visible-scale>storage rejection, plus one valid cell. This test-only addition followed timing and does not change the production library called by the measured TiDB binary. tikv-datatype-final.log is the final validation; earlier564 log is retained.

A separate post-measurement profile (no concurrent compilation/timing):

    EXPR_BATCH_PERF_WORKLOAD=decimal_add EXPR_BATCH_PERF_BATCH_SIZE=1024 EXPR_BATCH_PERF_SAMPLES=7 EXPR_BATCH_PERF_TARGET_NS=200000000 RUST_MIN_STACK=33554432 perf record -F 499 --call-graph dwarf -o decimal-after.perf.data -- taskset -c 2 ./after-bin tests::lane_cache_batch_performance::lane_cache_batch_release_performance_probe --ignored --exact --nocapture --test-threads=1
    perf report --stdio --no-children --sort symbol --percent-limit 0.7 -i decimal-after.perf.data

3395 samples, no lost samples. Report/text retained; binary perf.data remains only in the agent evidence directory. This is a cycle profile across all five probe shapes, not an allocation count or a dense-only profile. Input try_from_native_cell26.30% self, ChunkedVecSized<Decimal>::push_data9.89%, output try_to_native_cell9.20%, Decimal::try_new9.06%, native from_raw_bytes6.57%, read_batch5.99%, do_add_with_limit4.88%, vector_storage_bytes4.32%. These identify remaining constructor/transport cost, not proof of the cause of control regressions.
