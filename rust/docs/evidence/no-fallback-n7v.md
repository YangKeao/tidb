# N7v: actual UUID and hash operands

SDK signed local checkpoint: c15a1f1 (DCO); no push.

This local checkpoint replaces eight ordinary Ready handoffs for IS_UUID,
UUID_VERSION, UUID_TIMESTAMP, UUID_TO_BIN, BIN_TO_UUID, TIDB_SHARD and VITESS_HASH.
The SDK actual-value APIs call the same original eight pure owners (visibility
changes only); existing wire workers keep their ABI and algorithms. The frontend
retains strict versus byte-preserving coercion, typed errors, exact six-place
signed Decimal timestamps and unsigned hash bits.

## Independent proof and ownership

Before production candidates, n7v-original-capture.log recorded34713 direct/
2E9C5CB462D67999 and381843 scalar/1EE864887CAE1C77 observations. Both were frozen
and rerun in n7v-original-frozen.log. Ready/source guards genuinely failed on
all seven original names. While the old implementation still existed,
n7v-independent-pairs.log compared all416556 real old/new outcomes, including
warning/getter events and panic payloads. Executed tests/front/scalar/SDK snapshots
are preserved as n7v-independent-{tests,front,scalar,sdk}.rs in the persistent
agent evidence directory. Fixed final witnesses do not renew independent proof.

MiscNative uses authenticated actual operands in the same existing SDK frame.
Its original stored first-Constant probe runs before argument0; all children
remain eager before body conversion. Consuming demand advances before any
fallible body, callback or panic. UUID_TO_BIN parses before optional flag
conversion and may suppress it. BIN_TO_UUID handles String/Bytes flag warnings
before testing even a NULL payload. Ordinary return coercion runs once. Exact
existing typed SDK recipes retain their original same-frame handling.

A real1MiB Vector flag exposed an unnecessary BIN_TO_UUID copy: the new source
returned Resource where the old body succeeded under128KiB. A separate hash
case reproduced Resource versus the original UInt167. The source now detaches
only callback-capable flag ownership; quiet flags borrow actual values. Raw and
Vector hash casts have empty original warning stages: after finite work checks,
the real zone callback runs without a store borrow, then the genuine operand is
reauthenticated and passed to its original pure cast/hash. No fake operand or
precomputed answer is supplied. Logs n7v-expanded-boundaries-compile-fixed.log
and n7v-hash-unused-payload-red.log preserve the real failures; all seven focused
checks pass in n7v-focus-borrow-fixed.log after the fix.

Expanded checks cover17 source declarations,11 returns, all optional flag
kinds, three context profiles, three truncation levels, four execution flag
runs, duplicate selected positions, eager errors and original output prefixes.
Provider checks cover both output roles, nine valid name/arity shapes,176 bad
capability cases, exclusive store callbacks, panic/replay/cancel/recovery and
large unused raw/vector/hybrid payloads. Five resource cases refuse terminally
without fallback; the SAME prepared source subsequently handles NULL. The new
19-root Projection cohort is appended after existing clock setup, and exercises
both execution flags and workers0/2.

## Original clock witness audit

The first complete N7v run exposed a pre-existing N7u checksum dependency on the
UTC calendar day: Duration-to-Date/Datetime return conversion reads session_now,
which directly reads Utc::now. The N7u original capture occurred2026-10-19
17:18:44UTC; after midnight, the live unchanged signed TiDB80aaa96d plus
TiKVda0bf55 pair independently reproduced231102/E060F864CFA86ED1 in
n7v-n7u-signed-pinned-sdk-clock-audit.log. This is not a migration result delta.

The frontend datatype wrapper now offers an explicit default-OFF development
feature test-conversion-clock, enabled only by the expression dev-dependency.
The scope is thread-local and restores nesting and unwinding. Default production
builds retain the original direct UTC clock read. No process clock, timezone,
SQL result, datum or historical expected checksum is rewritten. The original
N7u scalar corpus still executes its real algorithms and original count/hash
assertions, with its original captured instant as explicit test input.

Exactly the same clock harness and N7u witness were applied to the detached
signed old pair and current sources; n7v-clock-source-proof.log verifies identical
files and the old checkout pins all four SDK manifest paths. The old pair passes
nine focused checks and restores231102/E5E2EFBE8F3E3F49. The current controlled
replay also restores that hash; the final full run checks the unchanged assertion.
A new test exercises3960 genuine Duration Date/Datetime conversions across two
explicit calendar days. Wrapper tests verify nesting, unwind restoration, thread
isolation and timezone projection; the default-OFF production check verifies the
normal library graph. Test-fixture count/import compile mistakes are preserved
in initial logs and are not called production regressions.

## Validation and remaining work

All builds reached terminal status. Final full frontend43601 passed1765 with
exactly the original3 failures and95ignored; all281 batch tests passed. SDK38415
passed1131 with exactly the original1 failure and1ignored. Projection88626
passed26; the extended19-root cohort matched original serial results with both
flags and workers0/2. Final frontend datatype1065 passed515 with precisely the
original two failures from the514-pass baseline; the new clock guard passed.
Production feature-OFF check51602 completed successfully. Old controlled clock
replay85651 passed9; current original scalar checksum is preserved in
n7v-current-clock-controlled.log and its assertions pass in the final full run.

Frontend346 and SDK17 warning-title Counters exactly match N7u; scoped SDK
clippy38129 completed with89 expression warnings and171 overall, Counter-identical
to N7u. Final lint41694 passed. Both pinned formatters checked18 touched Rust
files; both immutable STR_TO_DATE fixtures and git diff --check pass. Read-only
n7v-final-check.py checks exact counts/failures/warnings/hashes and independent
pair/clock evidence. SDK datatype production code remains unchanged since the
signed565/exact2 baseline; this is distinct from the frontend wrapper suite.

Inherited frontend failures are
pushdown_catalog::tests::ifnull_string_column_literal_uses_go_signature_and_column_collation,
tests::builtin_info_json_math_source::exp and
tests::builtin_math_misc_op_source::vectorized_builtin_op_func. SDK failure remains
local::str_to_date_worker_tests::str_to_date_workers_pass_whole_states_keep_wide_days_and_read_typed_mode_late.
Frontend wrapper failures remain
convert::tests::shared_decimal_uint_facade_preserves_typed_errors_subjects_and_panic_order
and field_type::tests::shared_field_decimal_meta_keeps_limits_updates_and_partial_mutation_order.
No inherited failure was removed, filtered or relabeled.

Evidence resides in the persistent agent workspace evidence/no-fallback:
n7v-frontend-final.log, n7v-sdk-full.log, n7v-projection-final.log,
n7v-frontend-datatype-clock-guards-final.log, n7v-clock-feature-off.log,
n7v-clippy.log, n7v-lint-final.log and n7v-final-check.log. Executed clock harness
and manifest snapshots plus SHA256 are retained in n7v-clock-harness/ and
n7v-clock-harness-sha256.log. Initial fixture/compile mistakes and real production
REDs are distinguished above.

No Go or Bazel source changed; make bazel_prepare was not triggered. Logical work
allowances do not certify physical allocator failure or whole-statement peak
memory. Remaining Ready families, generic admission fallback, unistore traversal,
unsupported/PB/malformed/deferred sources, typed NAME_CONST parents, inherited
failures, independent Go calendar/UNIX precision audits and full-server SQL gates
remain open. No complete upstream Go package transcreation is claimed.


## Reproduction

From the frontend repository root:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib misc_native_ -- --nocapture
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib native_misc_sources_do_not_fallback
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-datatype --lib
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb check --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-datatype --lib --no-default-features
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
    make lint

From the SDK repository root:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tikv test --release --locked --offline -j8 -p tidb_query_expr --lib
    RUSTFLAGS='' TIKV_FRAME_POINTER=0 make run COMMAND='bash /home/agent/.slock/agents/1555103c-9f3e-46ca-ac2e-b964c650c2f1/evidence/no-fallback/n3-clippy-scoped.sh --lib --tests --release --locked --offline -j8'

From the persistent agent workspace, run
`python3 evidence/no-fallback/n7v-final-check.py`. Historical original-candidate
pair scripts are applied artifacts and must not be rerun against the retired
implementation; their executed snapshots and logs preserve that proof. The old
clock audit checkout uses the same expression filter misc_identity_ with
--nocapture, against its explicitly pinned old SDK.
