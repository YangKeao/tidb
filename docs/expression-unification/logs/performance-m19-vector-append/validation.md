# Validation and cohort

The candidate changes only ChunkedVecSized<T>::push_data from inline to
inline(always). Value ownership, bitmap/data append order, NULL backing and
all algorithms/interfaces remain identical. The M18 constructor experiment
was fully reverted before this candidate; its binary is not the baseline.

From TiDB rust/, use this exact environment for each cargo command:

    export RUSTC_BOOTSTRAP=1 RUSTFLAGS='' CC=gcc-14 CXX=g++-14 CXXFLAGS='-include cstdint' CMAKE_POLICY_VERSION_MINIMUM=3.5 CARGO_HOME=/home/agent/tidb/expression-unification/cargo-home CARGO_TARGET_DIR=/home/agent/tidb/expression-unification/target-tidb
    cargo test --locked -p tidb-datatype --lib
    cargo test --release --locked -p tidb-expr --lib --no-run

Copy final release libtest as after-bin:

    RUST_MIN_STACK=33554432 ./after-bin evaluator::batch::tests --test-threads=1
    RUST_MIN_STACK=33554432 ./after-bin --test-threads=1

TiKV from its root, same environment except target-tikv, fresh generic-vector
consumer checks:

    cargo test --locked -p tidb_query_datatype --lib
    cargo test --locked -p tidb_query_expr --lib

Full-library failure names/counts in logs; existing owner/drop/NULL/payload
and resource tests cover the unchanged append contract. No implementation-
mirroring test is added for an inline hint. make lint, scoped Rust formatting
and both git diff checks run; no Go/Bazel inputs change. No fresh server,
PD/TiKV, FIPS or full workspace clippy success is claimed.

Baseline production: TiDB016e8e230b8351eefe31fb44c5b9b2a83e0f09dd /
TiKV44befe011baf3082d18398b27428f47030a29be2 (M17). TiDB24c34cdea72b1d2ba611b31cd006c7325c2836c3
is the subsequent report-only commit with identical production code. Baseline
binaryd29d409f280e55e71e38b36f4c0b8c765342cf2f2e15510efc0b08b2bae958d0.
The exact candidate patch and final keep/revert decision are retained below.

The independent M15 reference is TiDB6e46687c0dc1771ca4084f44c935534099b83868 /
TiKV8b210d8330eb7afecf973f6ccee0df059e76eb03, binary025aa8b47be1fb0a540662e576c589cb800ed0d2178a873f3e4c28f3c461399d.
The true frozen-native pair is TiDB364aef2bab5cc633ecb76a775ae8f36f86a6687d /
TiKV548812e1ef57aef077a2062a9cc356640a6347f5, with only M15 test probe/module
registration. Exact binary91f9c56c4a52ba68fd51976ceb287559ed2929f056a12ba570c5764720dcebe9
is freshly timed with the same defaultLongLong schema. Archived probe
../../evidence/original-native-narrow-integer-performance-m15.rs,
SHA256325197d64b4059be2981a1c1e08b51114c0da4f143c4edfb01d64fe952abc93d.
All hashes in binary-hashes.txt; machine/toolchain receipts retained. No
compilation during final timing.

Final candidate validation: TiDB datatype515, TiKV datatype565 pass. FRESH TiKV
expression1028 pass/same6 failure names verified against M15; release TiDB
batch28 pass and full1379 pass/same4 failure names verified against M17. lint
and scoped format/diff checks pass. Candidate hash9f0910ee69f166a6c46a58a2b24c9fbf68fd0e3e12a48797eff246669cb8167f.

Symbol/code-size inspection (separate from timing):

    nm -C before-bin | rg 'ChunkedVecSized<tidb_query_datatype::codec::mysql::decimal::Decimal>.*push_data'
    nm -C after-bin | rg 'ChunkedVecSized<tidb_query_datatype::codec::mysql::decimal::Decimal>.*push_data'
    size before-bin after-bin

Four baseline symbols, zero candidate symbols. text69430989→69476641 bytes,
data4405544 unchanged; this is a code-generation receipt, not a cache or
throughput claim. Text files preserve matching symbol lists.

## Decision: retained with measured tradeoffs

Three cohorts consistently improve Decimal PLUS158.749→149.122ns/row (1.07x), LT102.791→94.233 and NULL-safe equality103.245→94.419 (1.09x). Duration15.878→14.919 and JSON131.806→127.864 also improve. The candidate is retained as the one-line append hint, paired with TiKV366723653e14e12b7f3bf2a6f0d40f5e3eb5919d and the TiDB commit containing this report. No new driver, API or semantic mode is introduced.

This is not a universal gain: Real13.319→13.989, Datetime10.406→10.761 and Timestamp10.427→10.731 are slower; Vector is about1% slower and LOWER flat. M15 reference controls still show remaining Int/Time/Duration/LOWER regressions, all retained above. The source change helps the measured Decimal append cost; no established explanation or application-level gain is claimed for all control differences. Decimal PLUS remains about twice true native, so input transport remains an explicit optimization direction.
