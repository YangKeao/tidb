# N7q: actual date and quantity presentation sources

Local expression-unification-demo checkpoint; no push, PR, deployment or complete
Go-package port. The global objective remains active. Prior signed checkpoint:
TiDB4caa1ec0/TiKV4f63cf8. SDK companion DCO commit:
`b5c60ae6c11086a9c85385484dbc582da02401ac`.

## Scope and ownership

DATE_FORMAT, FORMAT_BYTES and FORMAT_NANO_TIME now execute as canonical native
sources through the existing single SDK frame. The two metric Ready callers in
builtin_ext/info.rs are retired. DATE_FORMAT's direct actual body was already
implemented in N7j and remains unchanged; this checkpoint integrates its source.
Generic compatibility dispatch for other families remains unchanged.

SDK impl_miscellaneous.rs exposes native_metric_format_value over actual nullable
f64 with finite work. It uses the sole existing unit tables and unchanged
format_scaled/fixed/scientific primitives. NULL requires no formatter work;
non-NULL requires1024 logical work. Explicit wire workers retain their nullable
raw8-byte little-endian IEEE754 decoder and transport planners, then delegate
that same formatter. No new dependency, codec, validity witness or argument
interpreter is introduced.

The frontend retains builtin_ext/info.rs::real_arg as the sole numeric conversion
owner. Only actual String/Bytes conversion invokes truncation callbacks; pure
actual kinds borrow authenticated values, including unsupported vectors and
Enum/Set values with unused names. Decimal float conversion still renders its
visible precision; Time/Duration and JSON keep their original conversions. The
source host releases store borrows before callbacks and copies only demanded
callback-bearing input after checked ownership admission. All eager capabilities
authenticate and demand closes before body work, including original panic.

DATE_FORMAT first performs the original ETDatetime cast with full source FieldType
and YEAR metadata. It then delegates existing epoch_value::date_format_values_bounded,
which strictly coerces both date and layout before nullable formatting, even if
the first is NULL. Earlier cast/UTF8 errors keep priority. No context callback
spans the borrowed layout. These three policies add no generic temporal-result
parse; ordinary return-family coercion follows the body exactly once.

## Independent original proof and explicit resource correction

Before candidate edits, session27209 captured56016 original result/error/panic
and exact-context observations in a fresh isolated UTC child. Each original call
uses fresh context/cache; no reused-cache history checksum or parent TZ mutation
is claimed. Original checksum: **02F463312450CC05**. The same capture reproduced
two metric Ready routes and three canonical source refusals/fallbacks as RED.

Independent session83067 compared all56016 old/candidate observations while the
old metric callers/workers remained, including original panic payloads and
numeric-prefix warnings. Every pair and checksum matched before retirement.
Persistent workspace evidence/no-fallback contains n7q-before-{info,epoch,sdk-misc}.rs,
n7q-original-witness.rs, n7q-final-independent-pairs.rs, n7q-original-red.log and
n7q-independent-pair.log. The current permanent checksum witness calls the new
owners and is a guard, not renewed independent proof. Never regenerate its hash
or rerun applied /tmp/n7q-*.py scripts.

Original behavior includes positive-zero text for negative zero, NaN bytes/ns,
all unit thresholds, IEEE754 rounding and scientific exponent spelling. Both
infinities retain the original scientific formatter panic:
“Rust scientific format always contains an exponent”. No finite-value filter,
soft NULL or changed SQL policy was added. Source tests catch both reference and
candidate panic, compare exact context plus full partial physical output columns
and row count, and reuse the suite across flags. Consumed host demand cannot
replay after panic; subsequent invocations recover without obsolete Ready-cache
poison.

After independent proof, a genuine resource regression in session45219 showed
that a1MiB nonzero BinaryLiteral under8KiB returned Resource instead of16.00EiB.
The original converter scans leading zero bytes and tests remaining width; a long
nonzero suffix needs no scan. Metric-specific metric_numeric_work now bounds that
actual prefix scan before calling unchanged real_arg. A100-zero prefix followed
by a huge nonzero suffix succeeds; an unresolved all-zero1MiB prefix still fails
before unbounded scanning. Generic numeric_work for previous operations is
unchanged. This explicit finite-work correction happened after proof; the fixed
56016 SQL witness remains unchanged. The earlier helper compilation failure
(private Result alias) is recorded separately as n7q-literal-resource-compile.log
and is not the business RED. Logical work and ownership allowances do not certify
allocator failures or whole-statement peak allocation.

## Final validation

All builds have terminated. No production behavior changed after these runs.

| Check | Result |
| --- | --- |
| Frontend28037, n7q-front-full.log |1722 pass, exact3 inherited failures,95 ignored,0 filtered; all260 batch pass |
| SDK17062, n7q-sdk-full.log |1124 pass, exact1 inherited failure,1 ignored,0 filtered; new actual formatter API test passes |
| Projection97164, n7q-projection.log |26 pass, new7-root group under both flags/workers0 and2 |
| Focus58858, n7q-focus.log |9 pass, original hash, Ready/source/matrix, finite and prefix-work guards |
| Provider, n7q-provider.log |pass;84 operation/fact combinations and both output roles |
| Scoped clippy, n7q-clippy.log |89 expression/171 all warning-title multiset exactly N7p |
| Frontend warning-title multiset |346 exactly N7p |
| Lint51969, n7q-lint.log |pass |
| Dual pinned formatting |all11 changed Rust files pass |
| Immutable fixtures and diff checks |original lengths/SHA256 and both git diff --check pass |

The source matrix spans all argument positions,13 declared families including
Timestamp/NewDate aliases,8 return families,4 context seeds,3 error levels,
4 flag transitions, empty/full/reordered/duplicate selections, CHAR parents,
eager ABS errors and2050 warnings. Full physical columns are compared, including
contradictory return metadata and partial output after original infinity panic.
NewDate has no fixed-time physical input column in the old Chunk; unchanged
alias cases use real computed/literal Time values over supported integer input
selections. No alias case or infinity was dropped. Representation parity does
not certify malformed source validity or physical NewDate input storage.

Provider checks extend the existing suite to84 operation/fact combinations and
both output roles. Every eager capability is tested for stale/wire NULL/wrong
node/row/occurrence/role, with cancellation, expired tasks and exclusive mutable
store access inside callbacks. New cases retain huge Vector unsupported errors
without pre-copy, long nonzero Bit conversion, original infinity panic followed
by replay denial and a later successful call, and DATE_FORMAT first-cast terminal
errors before a huge unused layout. SDK tests cover all6 byte and6 time unit
thresholds, NaN, negative zero, maximum f64, finite work and panic recovery.
Projection compares3 source outputs plus their CHAR parents and an older
arithmetic output; metric "12abc" retains exact truncation warning order.

Exact inherited frontend failures remain:

- pushdown_catalog::tests::ifnull_string_column_literal_uses_go_signature_and_column_collation
- tests::builtin_info_json_math_source::exp
- tests::builtin_math_misc_op_source::vectorized_builtin_op_func

SDK failure remains
local::str_to_date_worker_tests::str_to_date_workers_pass_whole_states_keep_wide_days_and_read_typed_mode_late.
No failure was filtered, renamed, dropped or reclassified.

Commands from TiDB root:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib presentation
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib native_duration_construction_capabilities_resources_panic_cleanup_and_exclusive_context
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
    make lint

The actual provider check used the already-built release binary with --exact on
evaluator::batch::tests::native_duration_construction_capabilities_resources_panic_cleanup_and_exclusive_context;
its log records the passing exact test. Historical independent proof used the
first command with presentation_original_observations before retirement; that
historical comparison is archived. Real resource RED and GREEN used
presentation_metric_literal_budget_uses_only_demanded_prefix.

Commands from TiKV root:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tikv test --release --locked --offline -j8 -p tidb_query_expr --lib
    RUSTFLAGS='' TIKV_FRAME_POINTER=0 make run COMMAND='bash /home/agent/.slock/agents/1555103c-9f3e-46ca-ac2e-b964c650c2f1/evidence/no-fallback/n3-clippy-scoped.sh --lib --tests --release --locked --offline -j8'

Both rustfmt binaries under
/home/agent/tidb/expression-reuse/rustup-home/toolchains/nightly-{2026-01-30,2026-08-22}-x86_64-unknown-linux-gnu/bin/rustfmt
ran with --check --edition 2021 --config skip_children=true on all11 changed Rust
files enumerated in workspace evidence/no-fallback/n7q-final-check.py. That
read-only script also checks exact failure/title Counters, runtime counts,
original hashes, immutable STR_TO_DATE fixtures and both diffs; output is
n7q-final-check.log.

SDK maintenance guide and living ExecPlan are updated in the same checkpoint.
No Go/Bazel, datatype or unistore production file changed; no bazel_prepare gate
was triggered. Datatype/unistore suites were not rerun, and prior datatype
failures remain open. The unrelated untracked client differential BUILD.bazel
is untouched. Remaining direct Ready families, generic admission fallback, old
unistore traversal/eight child-free DurationDatetime IDs, unsupported/malformed
shapes, inherited failures, Go semantic audits and full-server SQL gates remain
open. This signed local checkpoint does not complete the global objective.
