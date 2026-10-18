# N7s: actual GROUPING metadata and typed temporal literals

Local expression-unification-demo checkpoint; no push, PR, deployment or complete
Go-package port. Global goal/task4 remains active. Previous signed checkpoint:
TiDB33ab96c4/TiKV6c08491. SDK companion DCO commit:
`4d3a51507c609e754b37f93b63cfbb4e8d05c86e`.

## Ownership and demand

GROUPING's three modes and NULL route, plus DATE/TIMESTAMP literal construction,
no longer enter Ready. The unused frontend prepare_grouping_args forwarder is
removed. Explicit SDK wire workers, validators, packet builders and parsers are
unchanged; generic compatibility routing for other families remains active.

SDK native_grouping_value.rs accepts an actual optional u64 id and borrowed
validated GroupingMetadata. NULL precedes metadata work; non-NULL absent metadata
retains the original uninitialized failure. GroupingMetadata::eval remains the
sole BitAnd/NumericCmp/NumericSet algorithm, including empty sets, arbitrary group
counts and wrapping UInt bits. No native mark set or temporal identity is encoded
for a second argument interpreter, and no new codec/dependency is introduced.

ScalarFunction now stores immutable Arc metadata. The public getter still returns
&GroupingMetadata, validated replacement creates a new snapshot, and failed
replacement still clears metadata/hash caches. Source compilation shares the
existing header without scanning or cloning unused huge marks, and accepts absent
metadata until a non-NULL runtime id demands it. Existing mark storage belongs
to the expression; logical copied ownership accounting does not claim a bound
on whole-statement peak memory.

Dedicated GroupingNative catalog/metadata/demand uses the same existing SDK
HostFrame and native capability ABI. Runtime authenticates the eager child,
consumes demand, then uses the original strict Int/UInt reader with no invented
cast. NULL suppresses metadata, but argument errors precede missing metadata.
The signature returns UInt bits; the outer ScalarFunction::eval ordinary
return-family conversion still follows once. Integer representation policy stays
original; no generic temporal-result parse is added. Pure stages borrow storage
without any Columns callback crossing that borrow. Output provenance is retained.

Keep hashing contracts distinct: HashCode includes GROUPING mode/marks, while
Equals and Hash64 intentionally exclude planner metadata. These production
contracts are unchanged. Source host slots keep independent snapshots even when
ordinary structural hashes compare equal; mixed outputs exercise distinct modes.

Typed literals use native_temporal_literal_value over the unchanged private
SDK date_value/timestamp_value policies. Actual parsed values or owned hard errors
cross directly. The builder supplies its already-captured zone and three boolean
modes; execution Columns provide no second setting read. Regex-before-parser
order, DATE zero-mode order, unpadded diagnostics, hard1292/1525 codes, explicit
offsets, named-zone DST carry, raw FSP and complete Date/Datetime FieldType remain
original. Literals remain construction-time typed constants, rather than casts.

## Independent proof and actual corrections

Original84009 captured GROUPING **10416/A5CDD369E6A88378** and literal
**12960/80D479F72878FD19** observations before candidate edits. Six Ready routes
were RED; a separate cached run reproduced GROUPING source fallback before edits
because that guard's name did not match the first metadata_ filter. Each case
uses a fresh original context/cache under three profiles, without a reused-cache
poison-history claim. Literals cover six explicit zones, including named DST and
invalid fixed offsets, all eight modes, wide fractional/zero/calendar/Unicode/
syntax inputs, exact raw temporal values, FieldTypes, errors and context.
Grouping covers31 metadata states including missing metadata, all three modes,
0..127 groups, empty/single/multiple keys, every original native input kind and
strict id/NULL/error ordering. No parent process TZ was changed.

True40193 ran both exact independent witness tests while old callers remained.
Every10416 grouping pair and12960 literal pair matched with both original hashes
before retirement. Persistent workspace evidence/no-fallback retains
n7s-before-{grouping,literal,scalar,sdk-grouping,sdk-literal}.rs,
n7s-original-witness.rs, n7s-final-independent-pairs.rs, n7s-original-red.log,
n7s-source-red.log and n7s-independent-pair.log. Permanent fixed witnesses call
new owners and are guards, not renewed independent proof. Never regenerate
hashes or rerun applied /tmp/n7s-host.py or /tmp/n7s-tests.py.

Session51060 then exposed two genuine failures in n7s-resource-red-focus.log.
First,3000 empty NumericSet groups under500000 work incorrectly returned Resource
instead of UIntMAX. Initial uniform4096-per-group work assumed maximum tree
height even for empty sets and validated one-key modes. Finite work now admits a
bounded128-per-group header pass before iterating set lengths, then adds a
64-times-bit-length bound only for actual NumericSet searches, checked before the
original evaluation. The test is GREEN, large empty sets no longer pay maximum
search cost, and both original SQL hashes remain unchanged. This explicit work
correction occurred after proof; no witness was rewritten.

Second, a Varchar result declaration exposed native fixed-column panic versus old
success. I had mistaken the GROUPING signature's early UInt return for the whole
public Eval boundary. The outer Eval still performs ordinary return conversion;
the source host now preserves that once. The complete matrix passes afterward,
including contradictory declarations and physical output prefixes. A third
failure in51060 was a wrong new test assumption that Equals/Hash64 include
metadata; the test now asserts the existing distinct contracts. Production
hashing policy was not changed.

Private LocalError/control_value import compilation failures are archived
separately in n7s-independent-compile.log and n7s-focus-compile.log; they are not
business REDs. SDK's first full-test compile also revealed two chrono_tz versions;
new test-zone construction now uses the existing inferred named-zone parse form.
Only test portability changed; that log is n7s-sdk-compile.log.

## Final validation

All actual jobs are terminal. No production behavior changed after these runs.

| Check | Result |
| --- | --- |
| Frontend45163, n7s-front-full.log |1739 pass, exact3 inherited failures,95 ignored,0 filtered; all268 batch pass |
| SDK85633, n7s-sdk-full.log |1127 pass, exact1 inherited failure,1 ignored,0 filtered; both new actual API tests pass |
| Projection49196, n7s-projection.log |26 pass; new13-root group under both flags/workers0 and2 |
| Focus69250, n7s-focus-final.log |31 pass; frozen hashes/Ready/source/matrix/provider/hash/rewriter/resource checks |
| Scoped clippy82046, n7s-clippy.log |89 expression/171 all warning-title multiset exactly N7r |
| Frontend warning-title multiset |346 exactly N7r |
| Lint39422, n7s-lint.log |pass |
| Dual pinned formatting |all16 changed Rust files pass |
| Immutable fixtures and both diffs |original lengths/SHA256 and git diff --check pass |

The GROUPING source matrix retains14 declared input families,9 returns,31 metadata
states,4 seeds/3 levels/4 reused flag transitions, empty/full/reordered/duplicate
selections, CHAR parents and eager ABS errors. It compares caught outcomes,
complete physical columns/row counts and exact context. NewDate cannot occupy an
old fixed-time input column; unchanged cases remain computed/literal Time values
over supported integer selections. No alias or nonfinite case was dropped.
Representation parity does not certify malformed metadata or physical NewDate
storage. GROUPING PB shapes were already unsupported by this frontend decoder;
this checkpoint adds ordinary canonical sources, not a full PB coverage claim.

Provider checks cover both output roles and five metadata states: missing,
three one-group modes, and4096 empty groups. Every input capability is tested for
corrupt/stale/wire NULL/wrong role/node/row/occurrence. Huge unsupported bytes and
vectors preserve strict id errors without copying. At128KiB, demanded4096-group
work refuses Resource while the independent ordinary Eval at its default budget
returns UIntMAX; NULL still skips it. Consumed success/failure cannot replay even
before cancellation; cancellation is idempotent and later invocations recover.
A full source with100000 empty groups refuses non-NULL work without fallback,
then the same prepared suite succeeds on a NULL selection. A separate huge-mark
NULL source and Arc pointer checks verify sharing and no unused mark traversal.

Clone/hash replacement tests preserve cached HashCode invalidation, original
Equals/Hash64 policy and failed replacement to uninitialized metadata. Literal
rewriter tests count zone/modes/context capture exactly once and panic on any
execution Columns temporal/Ready access. Original DST/type/precision tests remain.
Projection mixes three modes, missing-metadata NULL, two folded temporal literals
and six CHAR parents with an older arithmetic output; earlier groups and clock
context remain intact.

Exact inherited frontend failures remain:

- pushdown_catalog::tests::ifnull_string_column_literal_uses_go_signature_and_column_collation
- tests::builtin_info_json_math_source::exp
- tests::builtin_math_misc_op_source::vectorized_builtin_op_func

SDK failure remains
local::str_to_date_worker_tests::str_to_date_workers_pass_whole_states_keep_wide_days_and_read_typed_mode_late.
No inherited failure was filtered, renamed, removed or reclassified.

Commands from TiDB root:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib -- metadata_ native_grouping_ --nocapture
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
    make lint

True independent40193 used:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib -- --exact tikv::metadata_value_tests::metadata_grouping_original_observations tikv::metadata_value_tests::metadata_literal_original_observations --nocapture

That historical comparison is archived; current exact tests are fixed witnesses.
The genuine work RED is metadata_grouping_empty_sets_do_not_pay_maximum_tree_search_work;
complete source parity is native_grouping_sources_keep_strict_id_metadata_demand_raw_returns_and_context.
Commands from TiKV root:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tikv test --release --locked --offline -j8 -p tidb_query_expr --lib
    RUSTFLAGS='' TIKV_FRAME_POINTER=0 make run COMMAND='bash /home/agent/.slock/agents/1555103c-9f3e-46ca-ac2e-b964c650c2f1/evidence/no-fallback/n3-clippy-scoped.sh --lib --tests --release --locked --offline -j8'

Both rustfmt binaries under
/home/agent/tidb/expression-reuse/rustup-home/toolchains/nightly-{2026-01-30,2026-08-22}-x86_64-unknown-linux-gnu/bin/rustfmt
ran with --check --edition 2021 --config skip_children=true on all16 files in
workspace evidence/no-fallback/n7s-final-check.py. The read-only script verifies
exact failures/title Counters/counts, both hashes in capture/pair/focus logs,
passing fixed witnesses in the full harness (successful stderr is captured),
immutable STR_TO_DATE fixtures and both diffs; output n7s-final-check.log.

SDK guide and all four living ExecPlan sections are current. No Go/Bazel, datatype
or unistore production file changed; no bazel_prepare gate was triggered.
Datatype/unistore suites were not rerun; prior datatype failures remain open.
The unrelated untracked client differential BUILD.bazel is untouched. Remaining
Ready families, generic admission fallback, old unistore traversal/eight child-free
DurationDatetime IDs, unsupported/malformed sources, inherited failures, Go
semantic audits and full-server SQL gates remain open. This signed local
checkpoint does not complete the global objective.
