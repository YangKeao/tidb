# Three-way implementation selection

All variants execute the same current batch driver over identical SQL/data:
M20 unchanged, full reciprocal candidate, and one-line canonical-head bound.
The full candidate's patch, independent integer proof/1M comparisons, initial
2310-record measurement and516/566 validation are retained in the sibling
performance-m21-decimal-word-powers receipt. Its after-bin is full-bin here.
The head-only candidate starts from M20 and adds just word>=TEN_POW[head]
before the existing remainder; for words already below the divisor, remainder
is the original word. Standard division and fractional checks remain unchanged.
No alternate production mode, helper or value domain is kept alongside a choice.

Head candidate tests, from TiDB rust/:

    export RUSTC_BOOTSTRAP=1 RUSTFLAGS='' CC=gcc-14 CXX=g++-14 CXXFLAGS='-include cstdint' CMAKE_POLICY_VERSION_MINIMUM=3.5 CARGO_HOME=/home/agent/tidb/expression-unification/cargo-home CARGO_TARGET_DIR=/home/agent/tidb/expression-unification/target-tidb
    cargo test --locked -p tidb-datatype --lib
    cargo test --release --locked -p tidb-expr --lib --no-run

516 datatype pass, including197056 independent physical/logical cell oracles,
130236 strict/wide exports at six limits and malformed physical admission.
TiKV root, same environment except target-tikv:

    cargo test --locked -p tidb_query_datatype --lib

565 pass. Full reciprocal-only helper/test/table are absent from this source.
Head release binary:

    RUST_MIN_STACK=33554432 ./head-bin evaluator::batch::tests --test-threads=1
    RUST_MIN_STACK=33554432 ./head-bin --test-threads=1

28 batch pass; full1379 pass/same4 names verified against M20. Scoped rustfmt
and both diff checks pass. The M21 full-candidate make lint result is reused
for unchanged Go/lint inputs; only the same Rust implementation/doc changes.
No Go/Bazel input changes, no bazel_prepare. No fresh server, PD/TiKV, FIPS,
full workspace clippy or TiKV full-expression result claimed; M19 expression
1028/same6 remains explicitly reused.

Three-mode measurement: modes m20/full/head rotate positions across three
cohorts (ABC/BCA/CAB); seven75ms samples, five shapes, eleven workloads. Every
mode is a distinct real binary in batch_tikv mode, NOT a same-binary ablation.
Frozen native runs after each cohort. All five shape samples, mode-process
medians and within-cohort paired ratios are retained. The independent M15
reference compares the actual head candidate against four earlier controls;
after-bin is an identical copy of head-bin only for that script's filename.
No compilation during timing. Hashes in binary-hashes.txt.

M20 source: TiDB835daf31153006a41d5d7f9ae1e7755e18db1f4a /
TiKV7c427c84ec6f341e04ed516c84b4333333d65b3d. Full candidate: same source plus
sibling full-candidate.patch. Head candidate: same source plus head-candidate.patch.
M15 reference: TiDB6e46687c0dc1771ca4084f44c935534099b83868 /
TiKV8b210d8330eb7afecf973f6ccee0df059e76eb03, exact binary025aa8b47be1fb0a540662e576c589cb800ed0d2178a873f3e4c28f3c461399d.
True frozen-native production: TiDB364aef2bab5cc633ecb76a775ae8f36f86a6687d /
TiKV548812e1ef57aef077a2062a9cc356640a6347f5, only M15 test probe/registration
changed; exact binary91f9c56c4a52ba68fd51976ceb287559ed2929f056a12ba570c5764720dcebe9
freshly timed with defaultLongLong schema. Probe
../../evidence/original-native-narrow-integer-performance-m15.rs,
SHA256325197d64b4059be2981a1c1e08b51114c0da4f143c4edfb01d64fe952abc93d.
Final decision: retain only the head-bound predicate, TiKV 8d143e47eba26e2a33562961a98b4693f1e2f22a. Full reciprocal source is absent; exact experimental patch/proof retained in sibling receipt.
