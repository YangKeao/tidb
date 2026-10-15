# Validation and cohorts

The typed NativeMyDecimal input adapter and MyDecimal facade input method are
replaced, not retained. The shared core now consumes native cell bytes through
the EXISTING NativeMyDecimal::from_raw_bytes checked decoder once, then applies
the unchanged logical normalization and word budget. It does not clone a decoder
or widen physical/logical admission. Decimal batch input borrows one raw-cell
view per column window and skips NULL bytes before decoding, as before.

From TiDB rust/:

    export RUSTC_BOOTSTRAP=1 RUSTFLAGS='' CC=gcc-14 CXX=g++-14 CXXFLAGS='-include cstdint' CMAKE_POLICY_VERSION_MINIMUM=3.5 CARGO_HOME=/home/agent/tidb/expression-unification/cargo-home CARGO_TARGET_DIR=/home/agent/tidb/expression-unification/target-tidb
    cargo test --locked -p tidb-datatype --lib direct_ -- --nocapture
    cargo test --release --locked -p tidb-expr --lib --no-run

Three focused tests pass:197056 valid cell old-value/direct-byte projections with
all fields/initialized words and exact resource boundaries;130236 strict/wide
output values×six limits; valid plus seven malformed physical cases agree with
the existing checked decoder. Independent source oracle remains the old native
value normalization, not the removed typed adapter. A full direct libtest run:

    RUST_MIN_STACK=33554432 /home/agent/tidb/expression-unification/target-tidb/debug/deps/tidb_datatype-e28221b4478b7fe2 --test-threads=1

516 pass. TiKV root, same environment with target-tikv:

    cargo test --locked -p tidb_query_datatype --lib

565 pass. Fresh TiKV expression1028/same6 is M19 evidence, reused here because
this input adapter is consumed by TiDB and no TiKV expression function changes.

Copy final current release libtest into after-bin:

    RUST_MIN_STACK=33554432 ./after-bin evaluator::batch::tests --test-threads=1
    RUST_MIN_STACK=33554432 ./after-bin --test-threads=1

Batch28 pass; full1379 pass/same4 baseline failures, names compared against M19.
make lint, scoped rustfmt edition2021 skip_children and both diff checks pass.
No Go/Bazel inputs change; no bazel_prepare required. No fresh server, PD/TiKV,
FIPS or full workspace clippy success is claimed.

Source deletion audit (production source only; historical reports stay immutable):

    rg -n 'try_from_native_cell\(' ../tikv/components rust/crates
    rg -n 'cell.try_to_shared_math|pub fn try_to_shared_math' rust/crates/tidb-datatype/src/mydecimal.rs

No matches. The unrelated VALUE Decimal::try_to_shared_math and OUTPUT
MyDecimal::try_from_shared_math remain required and unchanged. New core entry
try_from_native_cell_bytes is the single batch input projection boundary.

Before: M19 TiDB47225cd1297ad04dcc69ab38e83d323d4a9dc200 /
TiKV366723653e14e12b7f3bf2a6f0d40f5e3eb5919d. Current candidate source checkpoint
and keep/revert decision recorded below; binary hashes in binary-hashes.txt.
Separate M15 reference: TiDB6e46687c0dc1771ca4084f44c935534099b83868 /
TiKV8b210d8330eb7afecf973f6ccee0df059e76eb03, binary025aa8b47be1fb0a540662e576c589cb800ed0d2178a873f3e4c28f3c461399d.
It tracks Int/Datetime/Duration/LOWER against the earlier production cohort.

True frozen native production: TiDB364aef2bab5cc633ecb76a775ae8f36f86a6687d /
TiKV548812e1ef57aef077a2062a9cc356640a6347f5, only M15 test probe/registration
changed. Exact binary91f9c56c4a52ba68fd51976ceb287559ed2929f056a12ba570c5764720dcebe9
is freshly timed using identical defaultLongLong schema/data. Source
../../evidence/original-native-narrow-integer-performance-m15.rs,
SHA256325197d64b4059be2981a1c1e08b51114c0da4f143c4edfb01d64fe952abc93d.
Three alternating current pairs and separate M15 references, seven75ms samples
per shape; no compilation during timing. machine/toolchain receipts retained.

## Decision: retained, obsolete adapters removed

Three cohorts improve Decimal PLUS149.513→132.998ns/row (1.12x), LT94.040→78.179 (1.20x) and NULL-safe equality94.095→77.907 (1.21x). Other primary controls are flat or improve, apart from small LOWER/Int differences; all values remain visible. M15 references still show old Int/Time/Duration/LOWER regressions and are not reset away. No universal or application-level gain is claimed.

Retained source: report's TiDB commit / TiKV7c427c84ec6f341e04ed516c84b4333333d65b3d. Production searches find no old typed input entry or facade caller. Historical report/profile names remain immutable evidence. The factory reuses the original checked decoder; no new physical parser or semantic mode remains alongside it.
