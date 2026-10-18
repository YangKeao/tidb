# N7p: remaining native time arithmetic uses actual values

Local expression-unification-demo checkpoint; no push, PR, deployment or full
Go-package transcreation. The global no-fallback goal remains active. Previous
signed checkpoint: TiDB318b48d3/TiKV9668157. SDK companion DCO commit: `4f63cf8`.

## Scope and ownership

ADDTIME, SUBTIME, TIMESTAMP with one/two arguments, and TIMESTAMPADD now use actual
SDK values and canonical native sources through the existing single SDK frame.
All remaining Ready calls in frontend time_fn/add_sub.rs are gone: time-add
metadata/ADD/SUB, timestamp NULL/one/base/add, timestamp-add prefix NULL/ordinary
profiles. The unused dependent-stage `evaluate_prepared_args_scoped_in` helper
and re-export are also removed. The active generic compatibility router for other
families is unchanged.

SDK native_time_add.rs/native_timestamp_add.rs return actual owned text and
warning dispositions. NativeTimestampBaseValue owns the real parsed timestamp
and consumes itself during RHS finishing, including on error. Original temporal
parsers, duration arithmetic, timestamp base addition, month policy and formatters
remain authoritative. Explicit wire workers retain their validators, planners,
packet bytes and error causes while encoding those same actual results. Native
calls never serialize a native time identity for another argument program. No
new expression interpreter, synthetic NULL/validity operand, dependency or wire
codec is introduced.

Canonical lowering shares the original extracted FieldType kind classifier for
ADDTIME/SUBTIME and carries actual literal-only row-path facts. Runtime
BinaryLiteral/Bit status still comes from the real RHS Datum. All eager source
capabilities authenticate and demand closes before body conversion/callbacks.
Store borrows never span callbacks; callback-bearing operands use checked owned
copies, and pure stages borrow authenticated values. Result publication retains
original output provenance.

## Preserved demand and result boundaries

ADDTIME/SUBTIME with static DATETIME RHS suppress both body coercions, after all
child expressions and capabilities were evaluated. Other signatures coerce left
then right even with NULL left; earlier errors stop later conversion. Datetime/
date signatures parse RHS first, duration parses LHS first, and the default arm
keeps row-only ADDTIME's date-dash guard before LHS parsing. Native finite parser
work follows these demanded points. Warnings append1292 even in strict mode,
with the original capped truncation subjects/full other diagnostics and source
selection. Row selection depends on original Expression::Constant variants,
independently of the vectorization setting. The original declared Datetime and
Duration return transformations remain distinct.

TIMESTAMP reads the zone only after non-NULL first text. Actual Int/UInt/Decimal/
Real/Float32 kind selects the original numeric-string parser, using first-dot
precision. Parse warnings append1292 and suppress RHS conversion. A valid parsed
year-zero base still demands frontend RHS text conversion before rejecting;
invalid RHS UTF8 therefore still errors. The actual arithmetic then rejects zero
year before scanning/parsing the already-coerced RHS. TIMESTAMP's source result
always parses DateTime with no supplied result FSP, even for a string result
declaration. Ordinary return-family conversion follows once.

TIMESTAMPADD converts unit and its original full-float amount even after NULL
unit; it does not use numeric-prefix warning conversion. Prefix NULL suppresses
the direct signature's third text conversion. Canonical sources first perform
the original third-argument ETDatetime cast with complete FieldType/YEAR metadata
before converting either prefix, including with NULL prefix. A terminal third
cast error therefore precedes copying a huge unit. Date validity precedes unit
classification/uppercase work. IEEE754 rounding, month-only clamping, overflow
NULL and computed-field diagnostics remain original. No generic temporal result
parse is added; the ordinary return-family boundary remains.

## Independent original proof and resource correction

Original session9853, before candidate edits, captured1345032 observations under
an isolated UTC child: original FNV checksum **D12EB9991F65CDD5**. Each call gets a
fresh original cache/context so obsolete cache poison cannot hide later SQL
behavior. No parent TZ mutation or reused-cache history digest is involved.
Coverage includes every4x4 static kind pair, both row paths, ADD/SUB, extra sign
normalization inputs, real Datum families in both positions, timestamp first/RHS
pairs, timestamp-add units/IEEE/date/NULL-prefix combinations and exact context
traces. Six representative Ready guards and all five canonical source refusals/
fallbacks were reproduced as RED before retirement.

Actual independent session56346 compared all1345032 candidate outcomes and
result/error/panic observations with still-present original callers/controllers.
Every pair and the original checksum passed before retirement. Its authoritative
log is `n7p-actual-independent-pair.log`; the earlier misleadingly named
`n7p-independent-pair.log` is only an original-witness compile smoke, not paired
proof. Pair source and original bodies are archived in persistent workspace
`evidence/no-fallback/n7p-final-independent-pairs.rs`,
`n7p-original-witness.rs` and `n7p-before-{front,time-add,timestamp,timestamp-add}.rs`.
The permanent test retains the fixed original checksum; do not regenerate it from
new code or rerun applied `/tmp/n7p-*.py` scripts.

A subsequent real resource regression in the new actual-state wrapper failed in
session14838: a year-zero base and already-coerced1MiB borrowed RHS at0 work
returned Resource, although original arithmetic never scans that RHS. Finishing
now performs finite admission and the original zero-year/NULL gate before charging
RHS parsing. Frontend RHS coercion stays before this gate. The regression and SDK
actual-value test pass after the fix, and the fixed original SQL checksum remains
unchanged. This is an explicit finite-work correction after independent proof,
not a silent SQL-witness rewrite. Logical work/ownership allowances do not certify
allocator failures or whole-statement peak memory.

## Final validation

All real jobs are closed. No production code changed after these final runs.

| Check | Result |
| --- | --- |
| Frontend61878, n7p-front-final.log |1716 pass, exact3 inherited failures,95 ignored,0 filtered; all258 batch pass |
| SDK87346, n7p-sdk-full.log |1123 pass, exact1 inherited failure,1 ignored,0 filtered;3 new actual API tests pass |
| Projection62568, n7p-projection-final.log |26 pass; new13-root group under both flags/workers0 and2 |
| Focus7617, n7p-focus-final.log |6 pass, original checksum/Ready/source/matrix/finite/zero-base guards |
| Provider35652, n7p-provider-final.log |pass; all81 operation/fact combinations and both output roles |
| Scoped clippy83654, n7p-clippy.log |89 expression/171 all warning-title multiset exactly N7o |
| Final frontend warning titles |346 exactly N7o |
| Lint51077, n7p-lint.log |pass |
| Dual pinned formatting |14 changed Rust files pass |
| Immutable fixtures and both diff checks |original lengths/SHA256 and git diff --check pass |

The source matrix spans five policies/all argument positions,13 declared input
families including Timestamp/NewDate aliases, eight return families, four seeds,
three error levels, four flag transitions, empty/full/reordered/duplicate
selections, CHAR parents, eager ABS errors and2050 warning occurrences. Six-by-six
static alias/type combinations additionally compare both ADD/SUB and literal/
nonconstant paths across all return families. Output comparisons include complete
physical columns and exact callbacks/warnings. NewDate cannot be appended as a
fixed-time physical input column in the old Chunk; these unchanged cases use real
typed computed/literal values over valid integer-backed selections. No alias case
was dropped, and physical NewDate storage is not certified by this checkpoint.
Contradictory metadata representation parity is not malformed-source validity.

Provider tests cover all eager argument capabilities: corrupt/stale/wire NULL,
wrong role/node/row/occurrence, both output roles, cancellation/expired tasks,
exclusive mutable access from callbacks, ignored huge time-add operands under
DATETIME-RHS signatures, ignored huge timestamp RHS after first NULL, callback
unwind/replay rejection/later recovery, and third-cast error priority over a huge
TIMESTAMPADD unit. For false-row time-add facts, the comparison uses the original
typed value helper: an IF added only to simulate row classification had introduced
its own DATE normalization before the already-evaluated provider boundary. Complete
computed expression normalization remains independently compared to ScalarFunction
in the source matrix. That adjustment changed tests, not production semantics.

Initial full frontend17757 and Projection67964 had the same runtime passes, but
compiler output exposed the now-unused scoped Ready import. The helper/re-export
were deleted, and final full runs above confirm original warning-title parity.
The earlier premature warning-parity update was corrected in thread83e58115.
No warning suppression was introduced.

Exact inherited frontend failures remain:

- `pushdown_catalog::tests::ifnull_string_column_literal_uses_go_signature_and_column_collation`
- `tests::builtin_info_json_math_source::exp`
- `tests::builtin_math_misc_op_source::vectorized_builtin_op_func`

SDK failure remains
`local::str_to_date_worker_tests::str_to_date_workers_pass_whole_states_keep_wide_days_and_read_typed_mode_late`.
No inherited failure was filtered, renamed, dropped or reclassified.

Commands from TiDB root:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib time_arithmetic
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib native_duration_construction_capabilities_resources_panic_cleanup_and_exclusive_context
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
    make lint

The original independent proof used the first command with filter
`time_arithmetic_original_observations` before retirement. The current test is the
fixed original-checksum witness; historical independent pair source is archived.
The real resource RED used filter
`time_arithmetic_zero_base_does_not_charge_unread_rhs`; the same test is now GREEN.
Commands from TiKV root:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tikv test --release --locked --offline -j8 -p tidb_query_expr --lib
    RUSTFLAGS='' TIKV_FRAME_POINTER=0 make run COMMAND='bash /home/agent/.slock/agents/1555103c-9f3e-46ca-ac2e-b964c650c2f1/evidence/no-fallback/n3-clippy-scoped.sh --lib --tests --release --locked --offline -j8'

Both rustfmt binaries under
`/home/agent/tidb/expression-reuse/rustup-home/toolchains/nightly-{2026-01-30,2026-08-22}-x86_64-unknown-linux-gnu/bin/rustfmt`
run with `--check --edition 2021 --config skip_children=true` on the14 files in
workspace `evidence/no-fallback/n7p-final-check.py`. The script also verifies all
counts, exact inherited failures/warning Counters, original checksums, immutable
STR_TO_DATE fixtures and both diffs; final log is `n7p-final-check.log`.

No Go/Bazel, datatype or unistore production file changed. No bazel_prepare gate
was triggered; datatype/unistore suites were not rerun. Prior datatype failures
remain open. The unrelated untracked tikv-client-rs BUILD.bazel is untouched.
Remaining format-source families, generic admission fallback, old unistore
traversal/eight child-free DurationDatetime IDs, unsupported/malformed shapes,
inherited failures, Go semantic audits and full-server SQL gates remain open.
This signed local checkpoint does not complete the global objective.
