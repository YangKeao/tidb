# M27 fixed-output experiment: contracts and reproduction

Before production TiDB9ba9a2240281e425c4f44a92d938a88f72f7b07d /
TiKVd5817d0bfa15ccfad1fe93bea82e779dadf5e4c9. Only TiDB's existing
Column::append_fixed_cells owned-buffer loop changes; exact candidate.patch
includes source and independent scalar-byte tests. TiKV/core datatypes are
unchanged. Final retention/source in decision.md.

The loop reserves/resizes bytes once and fills chunks rather than extending
Vec length each row. NULL backing retains the existing last scratch element,
including after reset. Actual written count trims an oversized positive hint;
remaining cells preserve an undersized positive hint or zero-byte rows. The
cloned iterator is fused so its first None terminates permanently, as the old
for loop did. Shared/frozen buffers keep the unchanged scalar copy-on-write
path. No unsafe code, new execution mode, unchecked parser or resource-policy
change. Value conversion/output prevalidation remain outside the mutation.

M26 independent profiles identify fixed-output append as a major self-time cost;
they did not prove the cause of M26 regressions. This candidate must win actual
measurement, not only reduce visible source operations.

TiDB rust/ build environment:

    export RUSTC_BOOTSTRAP=1 RUSTFLAGS='' CC=gcc-14 CXX=g++-14 CXXFLAGS='-include cstdint' CMAKE_POLICY_VERSION_MINIMUM=3.5 CARGO_HOME=/home/agent/tidb/expression-unification/cargo-home CARGO_TARGET_DIR=/home/agent/tidb/expression-unification/target-tidb
    cargo test --locked -p tidb-chunk --lib
    cargo test --release --locked -p tidb-expr --lib --no-run

Chunk273pass/4ignored. New independent scalar appends verify raw bytes, NULL
bitmap, scratch state and row count for0/4/8/16/40byte cells, with/without reset,
correct/short/long positive hints and a nonfused iterator resuming after None.
Existing frozen/shared alias and nullable storage tests pass. No source algorithm
is duplicated as the test oracle. Copy release libtest as after-bin:

    RUST_MIN_STACK=33554432 ./after-bin evaluator::batch::tests --test-threads=1
    RUST_MIN_STACK=33554432 ./after-bin --test-threads=1

Batch33pass/full1385pass/same4 exact names verified against M26:
pushdown_catalog::tests::ifnull_string_column_literal_uses_go_signature_and_column_collation;
tests::builtin_info_json_math_source::exp;
tests::builtin_math_misc_op_source::vectorized_builtin_op_func;
time_fn::tests::str_to_date_partial_formats_follow_no_zero_date.

make lint, scoped rustfmt edition2021 skip_children/diff checks pass. No Go/Bazel
inputs change, no bazel_prepare. TiKV M26 full1031/same6 and datatype516/565 are
reused explicitly because that source is unchanged. No fresh server,PD-TiKV,
FIPS or workspace clippy pass claimed; existing unrelated compiler ICE unchanged.

Before actual M26 binary1ca445c94e9aee991466fa753986bbbd4fd130a8ede0f40a772864892ff80917.
Candidate1416106e4a5cd16cc1d919f88934819a23694343f45d4053a80caeff26cf8d50.
Original-native production TiDB364aef2bab5cc633ecb76a775ae8f36f86a6687d /
TiKV548812e1ef57aef077a2062a9cc356640a6347f5, only archived test registration/
probe changed. Reuse M26 binary4fb092925c1ff864aa41b8de1a0860afe9ec83c1555a0f8552ca4e3a318178f5;
it is freshly TIMED for every cohort, not freshly rebuilt. Exact test source
../../evidence/original-native-integer-identities-performance-m26.rs, SHA256
dffd65f6bc59b4c43277f9f6000b73071af3643658732d9f732a6a023a20349f.
M15 reference actual source pair6e46687c0dc1771ca4084f44c935534099b83868 /
8b210d8330eb7afecf973f6ccee0df059e76eb03, actual binary
025aa8b47be1fb0a540662e576c589cb800ed0d2178a873f3e4c28f3c461399d.
All hashes in binary-hashes.txt. Binaries remain outside git.

All14workloads compare DISTINCT actual M26/current batch binaries. There is NO
current fallback ablation in this checkpoint. For UInt identity workloads both
before/after/native use identical EXPR_BATCH_PERF_INT_UNSIGNED=1 schema choices;
all other controls keep original signed data. LongLong is explicit. The Real
domain flag is cleared. No columns/payloads/schema widths change.

collect.py executes3alternating pairs×14workloads×5shapes×7samples75ms×2=
2940timings/210checksum groups, including42fresh native/current dense groups.
All routes prepared-only/zero fallback/refusal. Native runs after each pair over
the same data/flags. reference-control.py independently compares M15/current
four controls,840records/60groups; old losses remain visible. Parsers assert
complete sample counts and checksums. No compilation/tests overlap timing.
CPU2 pinning does not isolate host/SMT/boost effects; no QPS claim.

Reproduce with before/after/frozen/int-reference bins, then collect.py,
reference-control.py,summarize.py,summarize-reference.py and make-report.py.
Decision.md pins keep/revert; all raw samples/dispersion remain. Machine/toolchain
snapshot reused same host. Static coverage stays67/131 metadata signatures;
this buffer experiment introduces no signature or SQL coverage claim.
