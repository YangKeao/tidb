# M26 result policy, identity proof and reproducibility

Before production TiDBd6e63358384717aac377a739f9638f9e198c6fd1 /
TiKV2b289f9eaa2f4c2620b4e128ec3f5a2b2e08be02. Final pair in decision.md.
The private registry replaces its result Boolean with ResultSignedness:
Signed/Unsigned/FirstArgument, independently of unsigned input permission.
Existing bitwise/comparison contracts are preserved; RoundInt and IntAnyValue
permit UInt and require the result flag to match the first argument. Public
batch_signature input/output/arity API is unchanged. No driver or algorithm.

Source proof: impl_math.rs round_int returns arg.to_owned(); round_int_native
calls that same function. Native TiDB math_fn/mod.rs int_round_is_identity
selects RoundIntNative only for one-argument integer ROUND. The two-argument
form selects RoundIntWithScaleNative and performs a real f64 round-trip. Native
UInt restores the original result tag after transporting the same i64 bits.
IntAnyValue's existing generic kernel clones the first argument. The existing
catalog already declares RoundInt result FirstArgument; ANY_VALUE's existing
typed frontend clones the source type. No PB catalog/decoder changes needed.

TiDB rust/ builds:

    export RUSTC_BOOTSTRAP=1 RUSTFLAGS='' CC=gcc-14 CXX=g++-14 CXXFLAGS='-include cstdint' CMAKE_POLICY_VERSION_MINIMUM=3.5 CARGO_HOME=/home/agent/tidb/expression-unification/cargo-home CARGO_TARGET_DIR=/home/agent/tidb/expression-unification/target-tidb
    cargo test --locked -p tidb-expr --lib integer_identities
    cargo test --release --locked -p tidb-expr --lib --no-run

Copy target-tidb/release/deps/tidb_expr-a6e9181eda8c8093 as after-bin:

    RUST_MIN_STACK=33554432 ./after-bin integer_identities --test-threads=1
    RUST_MIN_STACK=33554432 ./after-bin evaluator::batch::tests --test-threads=1
    RUST_MIN_STACK=33554432 ./after-bin --test-threads=1

Focused1/batch33pass, full1385pass/same4 names verified against M25. New goldens
coverNULL/0/±1/i64MIN/MAX/MAX-1/2^53+1 with either signedness, constants, nested
ROUND(ANY_VALUE(...)) and2050reverse/duplicate selected rows. Every output bit
and declared result flag remains correct. Two-argument ROUND(MAX-1,0) explicitly
stays native fallback and yields MAX, proving it was not misclassified as an
identity. Actual SQL smoke3prepared with1sample1ms; smoke timing is not reported.

Full unchanged TiDB failures:
pushdown_catalog::tests::ifnull_string_column_literal_uses_go_signature_and_column_collation;
tests::builtin_info_json_math_source::exp;
tests::builtin_math_misc_op_source::vectorized_builtin_op_func;
time_fn::tests::str_to_date_partial_formats_follow_no_zero_date.

TiKV root, same build environment with target-tikv:

    cargo test --locked -p tidb_query_expr --lib integer_identity_result
    RUST_MIN_STACK=33554432 /home/agent/tidb/expression-unification/target-tikv/debug/deps/tidb_query_expr-b51fd3e71a71d50f --test-threads=1

Focused1pass, all input/result flag combinations admit iff equal for both
identities. Full1031pass/same6 names verified against M25; actual tikv-full.log
records every name and sibling M25/M24 receipts preserve their baseline. No
existing suite failure is newly waived. Datatype code unchanged; adopted-core
516/565 results reused. make lint/scoped rustfmt edition2021 skip_children and
diff checks pass. No Go/Bazel inputs, no bazel_prepare. No fresh server,
PD-TiKV/FIPS/workspace clippy claim; existing unrelated compiler ICE unchanged.

Original-native production TiDB364aef2bab5cc633ecb76a775ae8f36f86a6687d /
TiKV548812e1ef57aef077a2062a9cc356640a6347f5. Only test registration/probe changes.
Exact49workload source at
../../evidence/original-native-integer-identities-performance-m26.rs,
SHA256dffd65f6bc59b4c43277f9f6000b73071af3643658732d9f732a6a023a20349f.
Frozen build: same environment, CARGO_TARGET_DIR=/home/agent/tidb/bench-original-native/target,
from frozen TiDB rust/:

    cargo test --release --locked -p tidb-expr --lib --no-run

Frozen binary4fb092925c1ff864aa41b8de1a0860afe9ec83c1555a0f8552ca4e3a318178f5.
Candidate1ca445c94e9aee991466fa753986bbbd4fd130a8ede0f40a772864892ff80917.
Before actual M25e6d751a72c26d5475e1a7554d2593193c1c77b00270ed4b2c38192b302d30098.
M15 actual reference production TiDB6e46687c0dc1771ca4084f44c935534099b83868 /
TiKV8b210d8330eb7afecf973f6ccee0df059e76eb03,
binary025aa8b47be1fb0a540662e576c589cb800ed0d2178a873f3e4c28f3c461399d.
All hashes in binary-hashes.txt; binaries stay outside git.

For new round_int_unsigned/any_value_int_unsigned only, candidate and frozen
schema set EXPR_BATCH_PERF_INT_UNSIGNED=1. The original i/j raw8byte cells are
unchanged and read with identical UInt flags on both sides. Negative raw i cells
therefore denote large valid u64 values. Signed round_int_signed and11old
controls keep original signed fields. No new columns/payload patterns; old
controls and reference runs explicitly clear unsigned/Real-domain selectors.
EXPR_BATCH_PERF_INT_TYPE=longlong is explicit. INPUT_SIGN probe headers record
the selected schema; this is test data metadata, not a production route/gate.

collect.py: NEW3 before cells are SAME candidate forced current_fallback_ablation,
explicitly NOT original native/historical TiKV. OLD11 before cells are actual
M25 batch code. All after cells use M26 batch. Three alternating pairs,
14workloads×5shapes×7samples75ms×2×3=2940timings/210checksum groups, including
42fresh native/current dense groups. Actual native runs separately after each
pair with the same unsigned choice. Independent reference-control.py compares
M15/M26 four old controls,840records/60groups, retaining older regressions.
Parsers assert7samples/490records per mode/cohort and every checksum/route label.
No compilation/tests overlap timing. CPU2 pinning does not isolate host/SMT/boost.

Run collect.py/reference-control.py/summarize.py/summarize-reference.py/
make-report.py with exact binaries, then record decision.md. Machine/toolchain
snapshot reused same-host M22. Regenerate static inventories:

    python3 docs/expression-unification/evidence/audit-batch-function-coverage.py --output docs/expression-unification/evidence/function-inventory.csv

67registry-admitted/131static signature names. This is not SQL coverage percent.

Additional regression confirmation is separate from the main matrix:

    python3 regression-control.py
    python3 summarize-regression.py

Actual M25/M26 bins, reverse initial order,3pairs/3controls/5shapes/7samples150ms:
630records/45checksum groups, all agree. The longer samples confirm repeatable
old-workload regressions, not merely one short cohort; they do not establish
causality. No replacement or discarded main sample.

Separate Duration profiles (after all formal timings), for mode before/after:

    env -u EXPR_PERF_REAL_DOMAIN EXPR_BATCH_PERF_INT_TYPE=longlong EXPR_BATCH_PERF_INT_UNSIGNED=0 EXPR_BATCH_PERF_WORKLOAD=any_value_duration EXPR_BATCH_PERF_BATCH_SIZE=1024 EXPR_BATCH_PERF_SAMPLES=1 EXPR_BATCH_PERF_TARGET_NS=1000000000 EXPR_BATCH_PERF_MODE=batch_tikv RUST_MIN_STACK=33554432 perf record -F 499 --call-graph dwarf,8192 -o duration-MODE-perf.data -- taskset -c 2 ./MODE-bin tests::lane_cache_batch_performance::lane_cache_batch_release_performance_probe --ignored --exact --nocapture --test-threads=1
    perf report -i duration-MODE-perf.data --stdio --no-children --percent-limit 0.5

Before2472samples, after2557samples; both0lost. Reports/run logs retained, raw
perf.data outside git. Profiles identify Duration::from_nanos (~15–17%self),
input access and fixed-output append as major costs. The registry is not a
leading self-time entry; no causal attribution is inferred from that absence.
Sampled profile-run latency is not used as formal benchmark data. The next
small experiment can target the generic fixed-output loop before attempting a
borrowed-carrier ABI or removing constructor normalization checks.
