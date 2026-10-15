# M28 source, validation and reproducibility

Before production TiDB7f0daa9135330e5d64aef22a2f2819e41fd90c68 /
TiKVd5817d0bfa15ccfad1fe93bea82e779dadf5e4c9. Candidate is exactly one
inline(always) on Duration::from_nanos in candidate.patch. Its check_fsp and
checked_round body is unchanged; no value/range/rounding/resource policy is
relaxed. No new tests mirroring an inline hint. M26 profiles identified15–17%
self there; FSP is shared across an input window so inlining could expose
invariant work or remove a return copy, but retention depends on measurement.

Build environment from TiDB rust/:

    export RUSTC_BOOTSTRAP=1 RUSTFLAGS='' CC=gcc-14 CXX=g++-14 CXXFLAGS='-include cstdint' CMAKE_POLICY_VERSION_MINIMUM=3.5 CARGO_HOME=/home/agent/tidb/expression-unification/cargo-home CARGO_TARGET_DIR=/home/agent/tidb/expression-unification/target-tidb
    cargo test --release --locked -p tidb-expr --lib --no-run
    cargo test --locked -p tidb-datatype --lib

Datatype516 pass, including existing native Duration and Decimal transport/
resource tests. Copy release libtest into after-bin, then:

    RUST_MIN_STACK=33554432 ./after-bin evaluator::batch::tests --test-threads=1
    RUST_MIN_STACK=33554432 ./after-bin --test-threads=1

Batch33pass/full1385pass/same4 names verified against M27:
pushdown_catalog::tests::ifnull_string_column_literal_uses_go_signature_and_column_collation;
tests::builtin_info_json_math_source::exp;
tests::builtin_math_misc_op_source::vectorized_builtin_op_func;
time_fn::tests::str_to_date_partial_formats_follow_no_zero_date.

TiKV root, same environment with target-tikv:

    cargo test --locked -p tidb_query_datatype --lib
    cargo test --locked -p tidb_query_expr --lib

Datatype565pass/full1031pass/same6 exact names verified against M26. Raw full
logs record those names; no failure waived. M27Chunk273 reused explicitly:
Chunk source is unchanged. make lint/scoped rustfmt edition2021 skip_children
and diff checks pass. No Go/Bazel input changes, no bazel_prepare. No fresh
server/PD-TiKV/FIPS/workspace clippy claim; existing unrelated compiler ICE.
No guide update needed for an inline hint with unchanged contracts.

Symbol audit using nm -S -C: baseline Duration::from_nanos is579bytes,
candidate has no standalone symbol. Matching symbol lists retained. This is
code-generation evidence, not proof of speedup. All binary hashes recorded:
M27 before1416106e4a5cd16cc1d919f88934819a23694343f45d4053a80caeff26cf8d50;
candidatea3b5545f58875a490ae734563c0b1ffd6ec32486f04e7e6199e45073226cf8d3.
Binaries stay outside git; final decision/source in decision.md.

True original-native production TiDB364aef2bab5cc633ecb76a775ae8f36f86a6687d /
TiKV548812e1ef57aef077a2062a9cc356640a6347f5. Only archived test registration/
probe differs. Same actual M26 frozen binary reused4fb092925c1ff864aa41b8de1a0860afe9ec83c1555a0f8552ca4e3a318178f5,
freshly TIMED after each pair. Exact probe
../../evidence/original-native-integer-identities-performance-m26.rs,
SHA256dffd65f6bc59b4c43277f9f6000b73071af3643658732d9f732a6a023a20349f.
M15 actual source pair6e46687c0dc1771ca4084f44c935534099b83868 /
8b210d8330eb7afecf973f6ccee0df059e76eb03,
binary025aa8b47be1fb0a540662e576c589cb800ed0d2178a873f3e4c28f3c461399d.

All14workloads compare DISTINCT actual M27/candidate batch binaries, no current
fallback ablation. UInt identity schema flag matches both/current/native for
the same2queries; old controls use original signed fields. LongLong explicit,
Real input-domain selector cleared. No new columns/payloads/schema widths.
collect.py:3alternating pairs×14workloads×5shapes×7samples75ms×2=2940timings/
210checksum groups including42fresh native/current dense groups. All modes
prepared-only,zero fallback/refusal. Independent M15/current reference-control.py
adds840records/60groups. Parsers assert all expected counts, labels and checksums.
No compilation/tests overlap timing. CPU2 pinning does not isolate host/SMT/boost.

Run collect.py,reference-control.py,summarize.py,summarize-reference.py,
make-report.py with exact binaries and decision.md. No universal/QPS claim;
all slow cells and process ranges stay visible. Machine/toolchain same-host
snapshot reused. Signature metadata67/131 is unchanged, not SQL coverage percent.
