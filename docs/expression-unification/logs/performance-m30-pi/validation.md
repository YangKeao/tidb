# M30 pure niladic PI: contracts, validation and reproduction

Before documents TiDB0012cfbea1c895355880ed1cac58c85cf574eb0b;
adopted runtime is M27 TiDB7f0daa9135330e5d64aef22a2f2819e41fd90c68 /
TiKVd5817d0bfa15ccfad1fe93bea82e779dadf5e4c9. Rejected M28/M29 binaries are
not baselines. M30 changes only the shared capability registry (Pi exactarity0,
Real result) and removes the TiDB blanket zero-arity rejection. Existing
catalog/PB lowering and prepared-kernel row count already support this call.
No separate execution driver or constant-result cache is introduced.

Source equivalence: TiKV impl_math.rs pi returns Real::new(f64::consts::PI);
pi_raw delegates to pi and encodes the identical bits. Native uses PiRaw, wire
Pi uses pi_fn_meta. TiDB catalog already declares signature Pi args[]/Real.
This admits one pure deterministic niladic signature, not all zero-argument
calls. RAND stays absent from the capability registry and native preparation
refuses it without evaluating RNG. No session/time/random state is read by PI.

Tests: TiDB PI on2050virtual rows with no columns, empty selection and2050
reverse/duplicate occurrences of17physical rows; direct and nested Real PLUS
output are non-NULL and bit-identical to literal3.141592653589793. A zero source
node budget refuses; TiKV tests exact arity/type, repeated selected-row output
without reads, zero work and zero retained-byte limits. Actual SQL pi() smoke
is prepared, with a separate literal-bit output oracle in the benchmark.

Build from respective TiDB rust/ or TiKV root (change target-dir as indicated):

    export RUSTC_BOOTSTRAP=1 RUSTFLAGS='' CC=gcc-14 CXX=g++-14 CXXFLAGS='-include cstdint' CMAKE_POLICY_VERSION_MINIMUM=3.5 CARGO_HOME=/home/agent/tidb/expression-unification/cargo-home
    CARGO_TARGET_DIR=/home/agent/tidb/expression-unification/target-tidb cargo test --locked -p tidb-expr --lib pure_niladic_pi
    CARGO_TARGET_DIR=/home/agent/tidb/expression-unification/target-tikv cargo test --locked -p tidb_query_expr --lib batch_niladic_pi
    CARGO_TARGET_DIR=/home/agent/tidb/expression-unification/target-tidb cargo test --release --locked -p tidb-expr --lib --no-run

Copy the final release libtest as after-bin, debug TiKV libtest as appropriate:

    RUST_MIN_STACK=33554432 ./after-bin evaluator::batch::tests --test-threads=1
    RUST_MIN_STACK=33554432 ./after-bin --test-threads=1
    RUST_MIN_STACK=33554432 /home/agent/tidb/expression-unification/target-tikv/debug/deps/tidb_query_expr-b51fd3e71a71d50f --test-threads=1

Focused1each/batch34pass, fullTiDB1386pass/same4failures and TiKV1032pass/same6.
TiDB: ifnull_string_column_literal_uses_go_signature_and_column_collation;
builtin_info_json_math_source::exp; builtin_math_misc_op_source::vectorized_builtin_op_func;
str_to_date_partial_formats_follow_no_zero_date. TiKV: str_to_date_workers_pass_whole_states_keep_wide_days_and_read_typed_mode_late;
test_evaluated_ascii_requires_official_recipe_without_restricting_legacy_rpn;
test_evaluated_bytes_rejects_same_carrier_operation_and_kernel_drift;
test_evaluated_composite_requires_exact_call_chain;
test_evaluated_int2_rejects_kernel_identity_and_wrong_kind;
test_evaluated_raw_rejects_role_length_and_kernel_drift.
make lint and scoped rustfmt edition2021 skip_children/diff checks pass.
Unchanged datatype516/565 and Chunk273 are reused, not rerun. No fresh server,
PD-TiKV/FIPS/workspace clippy claim; known file_system compiler ICE unchanged.
No Go/Bazel input changes, so no bazel_prepare. Guide updated with niladic purity.

Actual original-native source TiDB364aef2bab5cc633ecb76a775ae8f36f86a6687d /
TiKV548812e1ef57aef077a2062a9cc356640a6347f5; TiDB only test registration/probe,
TiKV clean. Exact50workload probe ../../evidence/original-native-pi-performance-m30.rs
SHA256ef2fd79adbf4e3753c8ae3da47a5a936af5aa222576138c938412a44f94aac01.
Fresh rebuilt frozen binaryd9928689626d481ce07f926c4794a072091ff5992a4a6c6581125894ad3545a3.
Current4cab9cb8e5b0bd66a207fe4f89a3711a3de207cd51a2c61283e0a534a5bd2a75;
actualM27before1416106e4a5cd16cc1d919f88934819a23694343f45d4053a80caeff26cf8d50.
M15reference025aa8b47be1fb0a540662e576c589cb800ed0d2178a873f3e4c28f3c461399d
(source TiDB6e46687c0dc1771ca4084f44c935534099b83868 /
TiKV8b210d8330eb7afecf973f6ccee0df059e76eb03).
All exact hashes in binary-hashes.txt; binaries outside git.

Fifteen workload pairs: new pure_pi BEFORE is same-current fallback ablation,
explicitly not native. Old14controls BEFORE is distinct actualM27batch binary;
AFTER is finalM30batch. Actual original-native dense15workloads freshly timed
after each of3alternating pairs.7samples75ms/5shapes/1024rows produce3150current
records/225checksumgroups incl45freshnative/current dense groups; independent
M15/current4control set adds840records/60groups. Two UIntidentity workloads
use flag1 on every corresponding binary/native; all other data/schema unchanged,
LongLong signed and original Real domain. Parsers verify counts/routes/checksums.

Run collect.py,reference-control.py,summarize.py,summarize-reference.py and
make-report.py with pinned binaries. No compilers/tests overlap timing; CPU2
pinning is not isolation. All slower cells and process ranges are retained.
Coverage68/131 is static-signature metadata only, not SQL coverage or QPS.
