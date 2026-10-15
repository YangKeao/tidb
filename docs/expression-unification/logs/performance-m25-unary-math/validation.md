# M25 source equivalence and reproduction

Before actual production TiDB806cb7b7b05ff37c3a6a673a0c46cfcd686ed469 /
TiKVf983e547a148b8bccdf99141b8e32c6de1f503c6. The candidate only adds three
registry capabilities, no algorithm or driver. Final pair in decision.md.

Source proof: TiKV impl_math.rs asin_f64 and acos_f64 call f64.asin/acos;
the ordinary Real kernels and private asin_raw/acos_raw workers call these same
helpers. Wire Real::new(...).ok() filters NaN output to NULL. Native TiDB
math_fn/mod.rs pack_inverse_trig filters NaN to NULL after the raw worker.
Both retain signed zero. By contrast native SIN/COS/ATAN use exact Go kernels,
while their wire counterparts use libm; ROUND also differs. Those remain
excluded. NaN input is outside the existing Real carrier, not newly admitted.
UnaryNotInt uses zero/nonzero/NULL and is valid for either Int signedness;
result remains signed. No internal istrue_with_null SQL implementation claim.

TiDB rust/ environment:

    export RUSTC_BOOTSTRAP=1 RUSTFLAGS='' CC=gcc-14 CXX=g++-14 CXXFLAGS='-include cstdint' CMAKE_POLICY_VERSION_MINIMUM=3.5 CARGO_HOME=/home/agent/tidb/expression-unification/cargo-home CARGO_TARGET_DIR=/home/agent/tidb/expression-unification/target-tidb
    cargo test --locked -p tidb-expr --lib inverse_trig_batch
    cargo test --release --locked -p tidb-expr --lib --no-run

Copy target-tidb/release/deps/tidb_expr-a6e9181eda8c8093 to after-bin:

    RUST_MIN_STACK=33554432 ./after-bin evaluator::batch::tests --test-threads=1
    RUST_MIN_STACK=33554432 ./after-bin --test-threads=1

Batch32pass, full1384pass/same4 failures, names compared to M24. The two new
tests use2050reverse/duplicate selections. Native-suite bit comparisons cover
NULL,±Inf,±1.5,immediately outside±1,±1,±.5,±0, plus independent domain-NULL
and signed-zero answers. NOT golden values include UIntMAX/i64MIN/MAX/±1/0/NULL.
Full unchanged TiDB failures:
pushdown_catalog::tests::ifnull_string_column_literal_uses_go_signature_and_column_collation;
tests::builtin_info_json_math_source::exp;
tests::builtin_math_misc_op_source::vectorized_builtin_op_func;
time_fn::tests::str_to_date_partial_formats_follow_no_zero_date.

TiKV root, same environment with target-tikv:

    cargo test --locked -p tidb_query_expr --lib local::profile_tests
    RUST_MIN_STACK=33554432 /home/agent/tidb/expression-unification/target-tikv/debug/deps/tidb_query_expr-b51fd3e71a71d50f --test-threads=1

Profile54pass, full1030pass/same6 names verified against M24; exact failure names
are in actual tikv-full.log and sibling M24/M23 receipts. Datatype code unchanged,
adopted-core516/565 results reused. Scoped rustfmt edition2021 skip_children,
make lint and diff checks pass. No Go/Bazel input changes, no bazel_prepare.
No fresh server/PD-TiKV/FIPS/workspace clippy claim; existing unrelated compiler
ICE is unchanged. Smoke3actualSQLprepared with1sample1ms checks only routes;
smoke times are not used for performance claims.

Frozen actual native production TiDB364aef2bab5cc633ecb76a775ae8f36f86a6687d /
TiKV548812e1ef57aef077a2062a9cc356640a6347f5. Only test registration/probe
changes; exact46workload probe at
../../evidence/original-native-unary-math-performance-m25.rs, SHA256
d5c97bdb0ac3188b7f007cab482e46aef9c22faeb3ef4f22b1722f5d9179ced8.
Same environment, CARGO_TARGET_DIR=/home/agent/tidb/bench-original-native/target:

    cargo test --release --locked -p tidb-expr --lib --no-run

Frozen binary9bfad291d02c13c5028d6b4efd9e6ac7d2bde5765e337d7b6b54599a95c0637a.
Candidate binarye6d751a72c26d5475e1a7554d2593193c1c77b00270ed4b2c38192b302d30098.
Before actual M24 binaryba74ff39c141fc48aad186b13d9653b6e4b3eb69cb13e21cc4f4991ef3ca02fa.
M15 actual reference TiDB6e46687c0dc1771ca4084f44c935534099b83868 /
TiKV8b210d8330eb7afecf973f6ccee0df059e76eb03,
binary025aa8b47be1fb0a540662e576c589cb800ed0d2178a873f3e4c28f3c461399d.
All hashes in binary-hashes.txt; binaries stay outside git.

For real_asin/real_acos only, current/frozen data setup reads the test-only
EXPR_PERF_REAL_DOMAIN=inverse_trig. The repeated r values are
[-1,-.9999,-.5,-0,+0,.5,.9999,1,-1.5,1.5],8valid/2outside values per10.
NULL-every10 masks follow the same existing pattern. Setup is outside timing.
Raw headers print EXPR_BATCH_INPUT_DOMAIN / ORIGINAL_NATIVE_INPUT_DOMAIN.
All other columns/schema remain identical. Old controls use the original dataset
with the domain flag unset, so the actual M24 baseline matches those inputs.
This is benchmark data, not a production execution route or acceptance gate.

collect.py:3new workloads BEFORE=explicit current after-bin fallback ablation,
AFTER=current batch.11old workloads BEFORE=actual M24 batch code,AFTER=M25.
The ablation is NOT original native or historical TiKV. Three alternating
pairs×14workloads×5shapes×7samples75ms×2=2940timings/210checksum groups,
including42fresh native/current dense groups. Native runs separately after each
pair with the same data domain as current. reference-control.py independently
compares M15/M25 four old controls,840records/60groups. Parsers assert each7
samples/490records per mode/cohort/all checksum groups and route labels. No
compilation/tests overlap timing. CPU2 pinning does not isolate host/SMT/boost.

Run collect.py, reference-control.py, summarize.py, summarize-reference.py and
make-report.py with exact binaries; decision.md records retention. Machine and
toolchain receipt reused same-host M22. Regenerate static inventories:

    python3 docs/expression-unification/evidence/audit-batch-function-coverage.py --output docs/expression-unification/evidence/function-inventory.csv

66registry-admitted/131static signature names, not SQL coverage percent.
