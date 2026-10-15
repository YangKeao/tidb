# M24 correctness, provenance and reproduction

Before production TiDB5a67a7cc3d3eae961442413a22a067426bf38826 /
TiKV57d3d13d48b425007ed26e0c995b85d5e50101cd. Final source pair is in decision.md.
Only the shared registry gains seven IS NULL signatures; the existing generated
nullable kernels and ordinary eager driver execute them. JSON is deliberately
excluded: Go catalog currently inserts CastJsonAsString then StringIsNull. No
JsonIsNull override, per-function driver, nullness-only shortcut or new execution
mode is introduced. The PB catalog and decoder are unchanged.

From TiDB rust/, builds use:

    export RUSTC_BOOTSTRAP=1 RUSTFLAGS='' CC=gcc-14 CXX=g++-14 CXXFLAGS='-include cstdint' CMAKE_POLICY_VERSION_MINIMUM=3.5 CARGO_HOME=/home/agent/tidb/expression-unification/cargo-home CARGO_TARGET_DIR=/home/agent/tidb/expression-unification/target-tidb
    cargo test --locked -p tidb-expr --lib is_null_batch
    cargo test --release --locked -p tidb-expr --lib --no-run

Copy target-tidb/release/deps/tidb_expr-a6e9181eda8c8093 to after-bin:

    RUST_MIN_STACK=33554432 ./after-bin is_null_batch --test-threads=1
    RUST_MIN_STACK=33554432 ./after-bin evaluator::batch::tests --test-threads=1
    RUST_MIN_STACK=33554432 ./after-bin --test-threads=1

Focused1/batch30 pass, full1382pass/same4 failures. New independent golden answers
use NULL versus zero/empty/nonzero for Int/UInt/Real/Decimal/String/Datetime/
Timestamp/Duration/Vector and NULL literals across2050reverse/duplicate selected
rows. All answers are non-NULL0/1; column and constant JSON cases verify existing
native fallback instead of claiming prepared coverage. Existing full catalog PB
tests independently pin every IS NULL signature, result flags and JSON cast.
Actual SQL smoke covers all8new benchmark cases,1sample1ms only for route proof;
those smoke times are not used for performance claims.

Full TiDB unchanged failures:
pushdown_catalog::tests::ifnull_string_column_literal_uses_go_signature_and_column_collation;
tests::builtin_info_json_math_source::exp;
tests::builtin_math_misc_op_source::vectorized_builtin_op_func;
time_fn::tests::str_to_date_partial_formats_follow_no_zero_date.

TiKV root, same environment with target-tikv:

    cargo test --locked -p tidb_query_expr --lib is_null_batch
    RUST_MIN_STACK=33554432 /home/agent/tidb/expression-unification/target-tikv/debug/deps/tidb_query_expr-b51fd3e71a71d50f --test-threads=1

Focused1pass; full1030pass/same6 names verified against M23. New admission test
checks exact7carriers, Int unsigned permission, rejection of non-Int unsigned,
and exclusion of direct wire JsonIsNull. Common registry signed-result guard is
unchanged. Full unchanged failure names are recorded in the sibling M23 receipt
and actual local tikv-full.log; no new failure is hidden. Datatype code is exactly
the adopted M21 code (516/565 results reused), no fresh datatype run needed.
make lint, scoped rustfmt edition2021 skip_children and both diff checks pass.
No Go/Bazel input changes, no bazel_prepare. No fresh server, PD/TiKV, FIPS or
workspace clippy success. Existing unrelated file_system compiler ICE unchanged.

Frozen original-native production is TiDB364aef2bab5cc633ecb76a775ae8f36f86a6687d /
TiKV548812e1ef57aef077a2062a9cc356640a6347f5. Only test registration/probe is
changed. New probe43workloads, including8IS NULL cases; exact source archived at
../../evidence/original-native-is-null-performance-m24.rs, SHA256
62d13501c9bb8ba36fa80664978c78f54d29259383dc89748b18c7f1e80f8625.
Build from frozen TiDB rust/ with the same environment and
CARGO_TARGET_DIR=/home/agent/tidb/bench-original-native/target:

    cargo test --release --locked -p tidb-expr --lib --no-run

Copy its release libtest to frozen-bin, SHA256
03606aa12b7fd328a188876eae2928871280fcc8f8fbcd85d0023fe65a450572.
Current candidate after-bin SHA256
ba74ff39c141fc48aad186b13d9653b6e4b3eb69cb13e21cc4f4991ef3ca02fa.
Before-bin actual M23 SHA256
07fa20034ed76989771d14307d7974b86976b2a4368a127bfe7ccd32a1729b64.
M15 actual reference production TiDB6e46687c0dc1771ca4084f44c935534099b83868 /
TiKV8b210d8330eb7afecf973f6ccee0df059e76eb03, binary025aa8b47be1fb0a540662e576c589cb800ed0d2178a873f3e4c28f3c461399d.
All hashes in binary-hashes.txt. Binaries are not committed.

collect.py executes3alternating pairs on CPU2,7samples75ms per workload/shape.
NEW8isnull_* BEFORE cells use the SAME candidate after-bin with explicit
current_fallback_ablation; this is NOT original native/historical TiKV. OLD11
controls use actual M23 before-bin batch code; AFTER all use M24 batch. All8new
queries use native nullable columns i/r/d/s/dt/ts/dur/v; source data/schema/NULL
patterns match original native exactly. Native dense results run separately
after every pair.3990current timings/285checksum groups including57fresh native
cross-groups. reference-control.py separately compares actual M15 versus M24
for4old controls,3pairs×5shapes×7samples=840records/60checksum groups. Parsers
assert sample lengths/counts, exact route labels and checksums. No compilation
or correctness tests overlap formal timing; CPU pinning is not CPU isolation.

Run collect.py, reference-control.py, summarize.py, summarize-reference.py and
make-report.py with the exact binary names above; decision.md pins retention.
Machine/toolchain snapshot reused same-host M22. Rebuilds get new hashes and
new measurements; do not relabel current fallback as historical native.

Regenerate coverage metadata from TiDB root:

    python3 docs/expression-unification/evidence/audit-batch-function-coverage.py --output docs/expression-unification/evidence/function-inventory.csv

63admitted/131static signature names. This is metadata, not SQL coverage percent.
