# M29 common fixed-input experiment: exact value policy and reproduction

Before documents TiDB548ce9f1aa496e70b8c99c4744840355a6554870, adopted runtime
still M27 TiDB7f0daa9135330e5d64aef22a2f2819e41fd90c68 /
TiKVd5817d0bfa15ccfad1fe93bea82e779dadf5e4c9. M28 inline hint is fully reverted;
its experimental binary is NOT a baseline. Final disposition/source in decision.md.
Exact candidate.patch includes only native batch transport and its test.
The private outlined read_fixed_native helper owns one raw-view/NULL/decode/push
loop for Int, Real, Time and Duration. Typed closures retain their existing
integer-bit, NaN admission, temporal normalization and FSP checks. No new driver
or runtime implementation selector exists. The direct-only candidate is an
archived alternative, not an additional production mode.

Time/Duration now use one raw-cell read view per window, as existing Int/Real.
On the declared fixed8byte, alias-stable native input layout, native-endian
word/nanos reads match scalar fixed_elem/get_duration. Producer owner/layout
preconditions remain those in evaluator/batch.rs, not atomic snapshots.
Time::from_go_raw then go_raw is preserved exactly before the shared carrier.
That native constructor normalizes FSP, not SQL calendar validity: packed
TimestampFSP7 maps to6. An initial test wrongly expected rejection; source
review corrected the fixture, not the production normalizer. New test covers
all16nibbles and2050reverse/duplicate selected rows for DateTime/Timestamp,
including NULL with arbitrary hidden payload. No blind raw-word shortcut.

Duration's old get_duration is only MySqlDuration::from_raw_parts(nanos,fillFSP)
with verbatim fields. Direct reads remove that temporary wrapper, while keeping
i8::try_from(FSP) and the same TikvDuration::from_nanos call inside NON-NULL.
No FSP/range/rounding check is moved or removed. Scalar getters remain used by
other consumers; no new decoder, function-specific driver or migration route.

Build from TiDB rust/:

    export RUSTC_BOOTSTRAP=1 RUSTFLAGS='' CC=gcc-14 CXX=g++-14 CXXFLAGS='-include cstdint' CMAKE_POLICY_VERSION_MINIMUM=3.5 CARGO_HOME=/home/agent/tidb/expression-unification/cargo-home CARGO_TARGET_DIR=/home/agent/tidb/expression-unification/target-tidb
    cargo test --locked -p tidb-expr --lib time_raw_batch_binding
    cargo test --release --locked -p tidb-expr --lib --no-run

Copy release libtest into after-bin:

    RUST_MIN_STACK=33554432 ./after-bin evaluator::batch::tests --test-threads=1
    RUST_MIN_STACK=33554432 ./after-bin --test-threads=1

Batch34pass; full1386pass/same4 exact names verified against M27:
pushdown_catalog::tests::ifnull_string_column_literal_uses_go_signature_and_column_collation;
tests::builtin_info_json_math_source::exp;
tests::builtin_math_misc_op_source::vectorized_builtin_op_func;
time_fn::tests::str_to_date_partial_formats_follow_no_zero_date.
make lint/scoped rustfmt edition2021 skip_children/diff checks pass. Core/TiKV
source unchanged after the M28 revert; datatype516/565,TiKV1031same6 and M27
Chunk273 reused explicitly. No fresh server/PD-TiKV/FIPS/workspace clippy claim.
No Go/Bazel inputs, no bazel_prepare; existing unrelated compiler ICE unchanged.

Before actual M27 binary1416106e4a5cd16cc1d919f88934819a23694343f45d4053a80caeff26cf8d50.
Direct candidate86f1ea1a6c2a9c71165516033478ee8b187866ebf35ed2088d67d735ac283ea4.
Common helper candidate13ce06f7cc3c50eeaf6a7f64970060123ae2ee1ee37583d7a598af74b83fea87.
fixed-bin and after-bin are identical; after is only the historical-control script alias.
Original-native production TiDB364aef2bab5cc633ecb76a775ae8f36f86a6687d /
TiKV548812e1ef57aef077a2062a9cc356640a6347f5, test registration/probe only.
Reuse M26 frozen binary4fb092925c1ff864aa41b8de1a0860afe9ec83c1555a0f8552ca4e3a318178f5,
freshly timed after every pair. Exact probe
../../evidence/original-native-integer-identities-performance-m26.rs,
SHA256dffd65f6bc59b4c43277f9f6000b73071af3643658732d9f732a6a023a20349f.
Actual M15 source pair6e46687c0dc1771ca4084f44c935534099b83868 /
8b210d8330eb7afecf973f6ccee0df059e76eb03,
reference binary025aa8b47be1fb0a540662e576c589cb800ed0d2178a873f3e4c28f3c461399d.
All hashes in binary-hashes.txt; binaries stay outside git.

All14workloads compare distinct ACTUAL M27/direct/common batch code, no fallback
ablation. Same2UInt identity queries use identical unsigned schema flags for
baseline/direct/fixed/native; old controls retain signed LongLong and original payloads.
Real domain flag cleared. No new column/type/data shape. collect.py executes
3rotating cohorts ABC/BCA/CAB×14workloads×5shapes×7samples75ms×3=4410records/210checksum
groups, including42fresh native/current dense groups. All routes prepared-only,
zero fallback/refusal. Independent M15/current reference adds840records/60groups.
Parsers assert complete sample counts/route labels/checksums. No compilation or
correctness tests overlap timing; CPU2 pinning is not CPU isolation.

Run collect.py/reference-control.py/summarize.py/summarize-reference.py/
make-report.py with exact baseline/direct/fixed/frozen/int-reference binaries and decision.md. Machine/toolchain reused
same-host snapshot. Coverage stays67/131metadata signatures, not SQL percent.
All slower cells/process ranges remain; no universal or application QPS claim.
