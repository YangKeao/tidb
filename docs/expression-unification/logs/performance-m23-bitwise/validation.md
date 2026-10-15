# M23 validation and reproduction

Before production: TiDBba09a2b45d2d03240426992790e4a67d9af79de0 /
TiKV8d143e47eba26e2a33562961a98b4693f1e2f22a; runtime/before-bin is M21
TiDB02c3676b94f849db74e3019a5e27233629a96e2e with unchanged TiKV.
M22 hint was fully reverted and is not a baseline. Final retained pair is in
decision.md. New production source has one catalog result-sign enum, one
capability registry result flag and the existing kernel/driver. No temporary
execution route or migration fallback is introduced.

TiDB rust/ environment:

    export RUSTC_BOOTSTRAP=1 RUSTFLAGS='' CC=gcc-14 CXX=g++-14 CXXFLAGS='-include cstdint' CMAKE_POLICY_VERSION_MINIMUM=3.5 CARGO_HOME=/home/agent/tidb/expression-unification/cargo-home CARGO_TARGET_DIR=/home/agent/tidb/expression-unification/target-tidb
    cargo test --release --locked -p tidb-expr --lib --no-run

Copy target-tidb/release/deps/tidb_expr-a6e9181eda8c8093 as after-bin. Direct:

    RUST_MIN_STACK=33554432 ./after-bin bitwise --test-threads=1
    RUST_MIN_STACK=33554432 ./after-bin evaluator::batch::tests --test-threads=1
    RUST_MIN_STACK=33554432 ./after-bin --test-threads=1

Focused5 pass/1ignored (includes existing bitwise tests), batch29 pass, full1381
pass/same4 failures. Golden bit answers cover sign bit, shifts63/64/65/-1, both
operand flag combinations and NULL over2050reverse/duplicate selected rows. PB
literal encode/decode/evaluate checks Always unsigned results even for signed
operands and NULL. The initial test incorrectly read output through the old
signed helper; corrected to values_for(actual,unsigned_field) before final build.
This was a test decoding error, not a production raw-bit conversion change.

Full TiDB unchanged failures:
pushdown_catalog::tests::ifnull_string_column_literal_uses_go_signature_and_column_collation;
tests::builtin_info_json_math_source::exp;
tests::builtin_math_misc_op_source::vectorized_builtin_op_func;
time_fn::tests::str_to_date_partial_formats_follow_no_zero_date.

TiKV root, same environment with target-tikv:

    cargo test --locked -p tidb_query_expr --lib bitwise
    RUST_MIN_STACK=33554432 /home/agent/tidb/expression-unification/target-tikv/debug/deps/tidb_query_expr-b51fd3e71a71d50f --test-threads=1

Focused1 pass, all operand flags and required unsigned-result declarations;
signed bitwise results and unsigned comparison results refused. Full1029pass/
same6 failures, names compared to M22. Exact unchanged names:
local::str_to_date_worker_tests::str_to_date_workers_pass_whole_states_keep_wide_days_and_read_typed_mode_late;
types::expr_eval::tests::{test_evaluated_ascii_requires_official_recipe_without_restricting_legacy_rpn,
test_evaluated_bytes_rejects_same_carrier_operation_and_kernel_drift,
test_evaluated_composite_requires_exact_call_chain,
test_evaluated_int2_rejects_kernel_identity_and_wrong_kind,
test_evaluated_raw_rejects_role_length_and_kernel_drift}.
Datatype516/565 are M22 results reused explicitly: no datatype code changes.
make lint, scoped rustfmt edition2021 skip_children checks and diff checks pass.
No Go/Bazel input changes, no bazel_prepare. No fresh server/PD-TiKV/FIPS/full
workspace clippy pass. Existing unrelated file_system compiler ICE is unchanged.

SQL smoke uses existing production rewrite/run_with_tikv_batch for i&j/i|j/i^j/
i<<j/i>>j. All five admit batch, unsigned result schema/checksums, all5shapes.
Smoke uses1sample1ms only to verify routes; no performance claims use that log.
Formal collect.py uses7samples75ms/5shapes/1024rows on CPU2 and3alternating
pairs. For NEW bitwise workloads before is the current after-bin forced to
current_fallback_ablation, explicitly labeled NOT original native. For11old
controls before is the actual M21 before-bin in batch_tikv mode. All after cells
use after-bin batch_tikv.3360timings/240checksum groups incl48native/current
dense groups. reference-control.py independently compares actual M15 with M23,
840records/60groups, preserving old regressions. All groups and expected sample
counts asserted by parsers. No compilation or correctness tests overlap timing.

True native production: frozen lab TiDB364aef2bab5cc633ecb76a775ae8f36f86a6687d /
TiKV548812e1ef57aef077a2062a9cc356640a6347f5. Fresh git audit: TiKV clean; TiDB
only tests/mod.rs registration + new test-only probe. No production changes.
Exact probe ../../evidence/original-native-bitwise-performance-m23.rs,
SHA256fdf2cbc7f72b0cdd1c6354ed16d6d9d5a937c56cc417b64cd6d44ce40a64f95f.
Same build environment, CARGO_TARGET_DIR=/home/agent/tidb/bench-original-native/target,
cargo test --release --locked -p tidb-expr --lib --no-run; frozen-build.log.
Binary0e206b9109716e96c1f685f34bed318a18a03913145a0ee4f8ace70b8cf379ef.
35test workloads vs30previous, including five new bitwise SQL inputs. Dense
schema/data/output FNV identical current/frozen and verified per cohort.

M15 reference production TiDB6e46687c0dc1771ca4084f44c935534099b83868 /
TiKV8b210d8330eb7afecf973f6ccee0df059e76eb03; actual binary
025aa8b47be1fb0a540662e576c589cb800ed0d2178a873f3e4c28f3c461399d.
All binary hashes in binary-hashes.txt. Machine/toolchain snapshot reused from
same-host M22. Binaries stay outside git. Reproduction scripts/logs and decision
are colocated; read summary.md for labels, limits and all slower controls.

Coverage metadata regenerated, not hand-edited:

    python3 docs/expression-unification/evidence/audit-batch-function-coverage.py --output docs/expression-unification/evidence/function-inventory.csv

131unique static signature names,56registry admitted; not SQL coverage percent.
