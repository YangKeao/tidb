# M22 validation and reproducibility

Source before: TiDB02c3676b94f849db74e3019a5e27233629a96e2e /
TiKV8d143e47eba26e2a33562961a98b4693f1e2f22a. Candidate is exactly
candidate.patch (one inline(always) on Decimal::try_new); no algorithm or check
is removed. No new mirror test for a code-generation hint. Existing wide/raw
owners, resource failures, NULL backings and arithmetic corpora apply.

TiDB rust/ environment for builds:

    export RUSTC_BOOTSTRAP=1 RUSTFLAGS='' CC=gcc-14 CXX=g++-14 CXXFLAGS='-include cstdint' CMAKE_POLICY_VERSION_MINIMUM=3.5 CARGO_HOME=/home/agent/tidb/expression-unification/cargo-home CARGO_TARGET_DIR=/home/agent/tidb/expression-unification/target-tidb
    cargo test --locked -p tidb-datatype --lib
    cargo test --release --locked -p tidb-expr --lib --no-run

Datatype516 pass, including197056 independent cell projections and130236strict/
wide exports×six limits. TiKV root with same environment and target-tikv:

    cargo test --locked -p tidb_query_datatype --lib
    cargo test --locked -p tidb_query_expr --lib

565 pass; full expression1028pass/same6 failures. Exact unchanged names:
local::str_to_date_worker_tests::str_to_date_workers_pass_whole_states_keep_wide_days_and_read_typed_mode_late;
types::expr_eval::tests::{test_evaluated_ascii_requires_official_recipe_without_restricting_legacy_rpn,
test_evaluated_bytes_rejects_same_carrier_operation_and_kernel_drift,
test_evaluated_composite_requires_exact_call_chain,
test_evaluated_int2_rejects_kernel_identity_and_wrong_kind,
test_evaluated_raw_rejects_role_length_and_kernel_drift}.

Copy final TiDB release libtest into after-bin. Direct tests:

    RUST_MIN_STACK=33554432 ./after-bin evaluator::batch::tests --test-threads=1
    RUST_MIN_STACK=33554432 ./after-bin --test-threads=1

Expected batch28/full1379pass/same4 baseline names:
pushdown_catalog::tests::ifnull_string_column_literal_uses_go_signature_and_column_collation;
tests::builtin_info_json_math_source::exp;
tests::builtin_math_misc_op_source::vectorized_builtin_op_func;
time_fn::tests::str_to_date_partial_formats_follow_no_zero_date.
Actual outcomes logged beside this file. make lint and scoped rustfmt edition2021
skip_children pass, diff checks pass. No Go/Bazel inputs changed; no
bazel_prepare needed. No fresh server, PD/TiKV, FIPS or workspace clippy pass.
Known unrelated file_system compiler ICE remains; no new full-clippy claim.

Original native production TiDB364aef2bab5cc633ecb76a775ae8f36f86a6687d /
TiKV548812e1ef57aef077a2062a9cc356640a6347f5, only test probe/registration
changed. Exact probe ../../evidence/original-native-narrow-integer-performance-m15.rs,
SHA256325197d64b4059be2981a1c1e08b51114c0da4f143c4edfb01d64fe952abc93d.
Native binary91f9c56c4a52ba68fd51976ceb287559ed2929f056a12ba570c5764720dcebe9.
M15 reference actual TiDB6e46687c0dc1771ca4084f44c935534099b83868 /
TiKV8b210d8330eb7afecf973f6ccee0df059e76eb03,
binary025aa8b47be1fb0a540662e576c589cb800ed0d2178a873f3e4c28f3c461399d.
No simulated historical route. DefaultLongLong schema/data identical across pairs.

collect.py: three alternating before/after pairs,11workloads×5shapes×7samples75ms,
2310current timing records/165checksum groups plus33fresh native/current dense
groups; native runs after each pair. reference-control.py separately compares
candidate with M15 fourcontrols×5shapes×7samples×3pairs=840records/60groups.
All timing on CPU2 with compilation/test processes finished first. Scripts
assert complete data and cross-version checksums. Binaries outside git; hashes
recorded in binary-hashes.txt. Reproduce with the corresponding exact binaries,
then run collect.py/reference-control.py/summarize.py/summarize-reference.py/
make-report.py. Final retention decision and source in decision.md.
