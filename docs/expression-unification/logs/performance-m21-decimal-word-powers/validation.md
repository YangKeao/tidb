# Validation and cohorts

Only native partial-word normalization/validation changes; Decimal arithmetic,
rounding, Fixed/Grow dispositions, byte decoding, word budget and driver stay
unchanged. Reciprocals are computed at compile time from existing TEN_POW values.
For B=2^64,d in10..1e9 and n:u32, M=ceil(B/d) gives:

    n/d <= n*M/B < n/d+n/B
    n*d < B, so n/B < 1/d

Writing n=q*d+r with0<=r<d bounds the upper fraction below q+1; high64(n*M)
is therefore exactly floor(n/d). n*M fits u128; power0 directly returns n.
Quotient*divisor<=n prevents u32 overflow in remainder/truncation. The physical
and strict logical admission gates are not widened.

From TiDB rust/:

    export RUSTC_BOOTSTRAP=1 RUSTFLAGS='' CC=gcc-14 CXX=g++-14 CXXFLAGS='-include cstdint' CMAKE_POLICY_VERSION_MINIMUM=3.5 CARGO_HOME=/home/agent/tidb/expression-unification/cargo-home CARGO_TARGET_DIR=/home/agent/tidb/expression-unification/target-tidb
    cargo test --locked -p tidb-datatype --lib
    cargo test --release --locked -p tidb-expr --lib --no-run

516 datatype tests pass, including197056 independent old-value/direct-cell
projections with exact word budgets,130236 strict/wide exports at six limits,
and malformed physical admission. New core test uses native integer division
as the independent oracle for powers0..9, quotient-boundary±1/full-u32 edges
and100000 deterministic random u32s×10 powers (1M random comparisons).

TiKV root, same environment except target-tikv:

    cargo test --locked -p tidb_query_datatype --lib

566 pass. M19 full TiKV expression1028/same6 remains explicitly reused; this
helper changes only native cell projection, not existing expression arithmetic.
Copy final current release libtest as after-bin:

    RUST_MIN_STACK=33554432 ./after-bin evaluator::batch::tests --test-threads=1
    RUST_MIN_STACK=33554432 ./after-bin --test-threads=1

Batch28 pass, full1379 pass/same4 failure names verified against M20. make lint,
scoped rustfmt and both git diff checks pass. Rust-only inputs; no bazel_prepare.
No fresh server, PD/TiKV, FIPS or full workspace clippy success is claimed.

Before: M20 TiDB835daf31153006a41d5d7f9ae1e7755e18db1f4a /
TiKV7c427c84ec6f341e04ed516c84b4333333d65b3d, binary95264c1c925ceaca9d113ef42092102a0d45d92e921adc1d43240ef14dddb5b9.
Candidate source/final keep decision recorded below; all hashes in binary-hashes.txt.
M15 reference: TiDB6e46687c0dc1771ca4084f44c935534099b83868 /
TiKV8b210d8330eb7afecf973f6ccee0df059e76eb03, binary025aa8b47be1fb0a540662e576c589cb800ed0d2178a873f3e4c28f3c461399d.
True frozen production: TiDB364aef2bab5cc633ecb76a775ae8f36f86a6687d /
TiKV548812e1ef57aef077a2062a9cc356640a6347f5, only M15 test probe/registration
changed; exact binary91f9c56c4a52ba68fd51976ceb287559ed2929f056a12ba570c5764720dcebe9
freshly timed. Probe ../../evidence/original-native-narrow-integer-performance-m15.rs,
SHA256325197d64b4059be2981a1c1e08b51114c0da4f143c4edfb01d64fe952abc93d.
Identical defaultLongLong schema/data; primary11 workloads plus independent
M15 four controls, three alternating pairs, seven75ms samples/five shapes.
No compilation during timing; machine/toolchain receipts retained.

Static code-generation audit, separate from timing: nm -S -C and objdump -d
--no-show-raw-insn over try_from_native_cell_bytes and try_to_native_cell.
M20 had2 input/1 output hardware divl; candidate has0/0. division-audit.txt and
function assembly receipts retained. This does not by itself establish a speedup.

Final disposition: full reciprocal candidate not selected. Production retains only the one-line head bound, TiKV8d143e47eba26e2a33562961a98b4693f1e2f22a. See sibling head-bound receipt for the actual three-mode comparison.
