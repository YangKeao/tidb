# N7j: actual timestamp conversion and complete source return boundaries

Signed local SDK commit `baea038` is paired with this TiDB implementation and
receipt; neither is pushed. Final source, SDK and consumer validation is complete.
This controller/source checkpoint does not complete the global migration.

## Actual state and ownership

Ordinary `UNIX_TIMESTAMP` and `FROM_UNIXTIME` no longer encode identities or
execute Ready parse/value/local programs. `native_epoch_value` owns actual
numeric coefficients, parsed time and accepted epoch state. Parsed-time finish
and local epoch rendering consume their actual state. Explicit compatibility
workers retain their original validators, codecs, report layouts, output plans
and zone-owner rules and delegate to those same functions. The private optional
work policy is absent only for compatibility under its existing transport
planner; public actual APIs require finite allowances. No fabricated large limit
replaces compatibility policy, and no runtime limit was raised.

UNIX_TIMESTAMP retains ordinary i64 clock arithmetic and full-u32 nanos, native
input-derived precision and range, all-zero NULL versus partial-zero numeric
zero, unconditional bad-date warning1292, and a fresh second zone getter only for
an accepted parsed time. The first parse is not repeated. Its Naive DST-gap
adjustment and raw fixed offsets remain distinct from legacy typed Time's strict
`native_core_to_datetime(false)` conversion. Legacy typed Time accepts original
kind/FSP metadata, does not clear Date clocks, and returns exact integer seconds,
Decimal zero-scale0 or valid micros-scale6. Nullable legacy input remains NULL.

FROM_UNIXTIME keeps Int/UInt FSP0, visible-Decimal precision capped6 and original
Float32-as-f64/Real decimal conversion with FSP6. Text parsing keeps first-nine
fraction digits, original half-up rounding, negative-zero fraction behavior,
integer-prefix diagnostics and the integral range check before a possible carry.
A truncate outcome owns the same accepted zero epoch: HandleTruncate precedes
zone access, and an error skips zone/local/format work. Legacy Decimal retains
its visible text to f64 conversion, upper f64 range literal (which rounds to
MAX+1), extra ordinary u32 nanos*1000 behavior, DateTime FSP0 and raw micros.
Those legacy quirks are not replaced by the ordinary native policy.

Text and raw-core DATE_FORMAT call the existing shared datatype formatting
primitives directly. Original conversions still run in their old order; core
presence/NULL/missing-child behavior and text/core trailing-percent, invalid
month and dirty-clock differences remain intact. Legacy FROM_UNIXTIME's pack
callback receives its original context; no selected alternate execution must
stay alive through its formatter callback.

Canonical hosts cover unix_timestamp0/1 and from_unixtime1/2 with original eager
source capabilities and actual result capabilities. This preserves polymorphic
Int/Decimal and String/Time values through the existing root/operand publication
model. Every token's node/role/window/row occurrence is authenticated before SQL
callbacks. Clock0 finishes immediately without a fabricated child. All sources
have already evaluated, even when a format's later conversion is suppressed.
After a valid local result, its original format capability is checked again and
copied within that same frame-owned source window; no store borrow survives a
mode, clock, zone, truncate or warning callback. NULL, out-of-range/nonfinite
inputs and truncation errors do not copy an unused huge format. The original
FROM_UNIXTIME temporal result policy runs before ScalarFunction return-family
conversion. Generic PB-origin admission and unistore's old walker are not
credited as migrated by these ordinary source hosts.

## Independent old observations before retirement

At TiDBb2d95e19/TiKV1017778, original callers and independent old controllers
produced **384264** observations with fixed digest **14B4FDA02CC55EED**
(`n7j-original-and-red.log`,96492 CLOSED101). Twelve forbidden-Ready hooks and
four canonical-source refusal/fallback counterexamples genuinely failed. The
initial count/hash-zero assertions were capture assertions, not semantic REDs.
There was no incidental invariant panic in the observation capture.

The domain includes3 cache profiles,12 dynamic clock/zone seeds and3 truncate
levels; all native datum kinds, raw/hidden Decimal metadata, nonfinite numbers,
invalid UTF-8, zero/partial dates, DST gaps/folds, eight explicit/dynamically read
UTC/named/raw fixed zones, three Time kinds and raw FSP255, nullable/invalid format
arguments, bad arities and legacy raw/format pack callbacks. It records errors,
raw result identities, NULLs, warning/zone/clock/mode/truncate traces and the pack
context identity.

New SDK functions and frontend candidates were initially added beside untouched
old callers/controller bodies. `n7j-original-paired.log` (53693 CLOSED0) matched
**every384264 observation** before any old caller/controller was removed. Only
old observations feed the digest. Pure existing parser/rounding/formatting and
strict-zone helpers were shared unchanged; no old control body delegated to the
candidate during this comparison. Only afterward were old control bodies
retired and new/new paired instrumentation removed. The fixed digest now checks
the actual producer and remains unchanged in the final full frontend run.

Workspace evidence retains `n7j-independent-paired-test.rs` and
`n7j-before-{frontend-session-tz,frontend-unix,frontend-from-unix,sdk-unix,sdk-from-unix}.rs`
for replay from the signed old heads. No historical digest or calendar oracle
was regenerated from replacement outputs.

## Three verified return-boundary defects

The new source-context matrix32118 found a real omission: the timestamp source
host skipped ordinary `ScalarFunction::coerce_to_ret_type`. An integer result
under a Decimal declaration was published as integer bits, while the reference
returned the correctly converted Decimal. The kernel's value and context traces
already matched; this was the function return boundary, not new timestamp math.
The original `coerce_function_result` now runs once after the kernel and any
FROM_UNIXTIME temporal policy, before typed/native publication.

The same audit found that earlier EXTRACT and DATE_ADD source hosts also omitted
this boundary. `n7j-existing-return-boundary-red.log` (55144 CLOSED101) reproduces
both: Extract Double expected f64(2.0), got integer2 bits; DateAdd Timestamp
expected Time bits, got ISO string bytes. Their original scalar reference already
converted those values. Both now use the same bounded original ordinary-family
policy as timestamps. Unit-suffixed extract/date arithmetic names have the same
non-COALESCE policy; no new expression selector or evaluator was introduced.
Both REDs and their CHAR-parent/Decimal variants pass in the final full run.

An initial coercion preflight incorrectly treated arbitrary VARCHAR width as
allocation. `n7j-unused-return-width-red.log` (72957 CLOSED101) shows the original
conversion returns String42 for VARCHAR(i64::MAX), while the candidate wrongly
failed a16384 allowance. The shared `control_value::coerce_result_bounded` now
bounds actual target growth: Decimal fraction scale (using the original shared
target-shape helper), fixed binary String padding and the bounded integer-to-Bit
output. VARCHAR/ordinary CHAR merely truncate; their huge declared widths do not
request corresponding padding. Unsupported/UTF-8 conversion dispositions remain
owned by the original conversion, including its existing fallback. Same-family
and NULL paths retain original behavior. This RED also passes in the final run.

Finite formatter/parser/diagnostic, detached-copy and publication checks precede
the work they guard. Raw Decimal invariant panics remain panics with sufficient
allowance; there is no production catch or normalization. These are conservative
logical work/retained allowances, not a whole-statement allocator peak promise.

## Validation

`n7j-front-full-final.log` (30633 CLOSED101) has **1676 passes, exact3 inherited
failures,95 ignored,0 filtered**; all **240 evaluator::batch tests pass**. The
unchanged failures are:

- `pushdown_catalog::tests::ifnull_string_column_literal_uses_go_signature_and_column_collation`
- `tests::builtin_info_json_math_source::exp`
- `tests::builtin_math_misc_op_source::vectorized_builtin_op_func`

`n7j-sdk-full-final.log` (cached command CLOSED101) has **1111 passes, exact1
inherited failure,1 ignored,0 filtered**. The remaining failure is exactly
`local::str_to_date_worker_tests::str_to_date_workers_pass_whole_states_keep_wide_days_and_read_typed_mode_late`.
All four new actual SDK tests pass: late-zone/strict legacy policies, source
precision/carry/zero epoch/legacy fraction, finite work/panic order/consuming
states and raw/text/nullable/expanding DATE_FORMAT. Twelve existing SDK timestamp
worker/codec tests also pass in4535.

Source matrices compare results/raw bytes/NULLs, errors, exact callback traces
and zero refusal/fallback under12 seeds,3 levels, four repeated vector flags,
CHAR parents, empty/full/2050 duplicate selections, heterogeneous source types,
string/date/Datetime3/Duration6 result declarations, and0/1/2 argument forms.
They cover eager ABS(MIN) errors after a NULL epoch and suppressed invalid-UTF8
format conversion. The final full suite passes all of them.

Provider checks cover both root/operand output roles and all four call shapes,
every eager prefix, stale/wrong-node/role/occurrence capabilities, repeated
cancellation/reuse, immediate-clock failure cleanup, clone versus parser errors,
and huge authenticated format suppression after NULL/out-of-range/NaN/truncate
errors. Clock/zone/truncate callbacks can exclusively borrow the store. The
Datetime3 extension also executes the result-mode callback with that exclusive
borrow; `n7j-provider-final.log` (28212 CLOSED0) and final full30633 both pass.
Its only earlier failure was a new test expecting `Conversion` instead of the
original `TruncatedWrongValue` variant; the test now checks the exact original
variant/message, without changing production behavior.

`n7j-projection-final.log` (95218 CLOSED0) passes all **26 Projection tests**.
The new six-root group combines an integer, nullable Unix Decimal, From Datetime3,
formatted From String and CHAR parents under both flags/workers0/2, matching raw
bytes, NULLs and warnings. All older heterogeneous groups remain intact. An
initial new-test moved-variable compile error was corrected with a fresh field
definition in this new group; it is not semantic RED evidence.

Four existing unistore consumers pass: two timestamp/session-zone tests
(`n7j-consumer-unix.log`,3792 CLOSED0), and text/raw DATE_FORMAT tests
(`n7j-consumer-format.log`,cached CLOSED0), including dirty raw clocks, invalid
month, trailing-percent differences and distinct presence. No unistore code or
its unrelated pre-existing formatting was changed. No datatype production
changed or package rerun is claimed; prior564 passes/exact2 inherited datatype
failures remain prior baseline evidence.

Scoped clippy24644 CLOSED0 retains **89 expression warnings** and exactly the
same **171 all-crate warning-title multiplicities** as N7i. The full frontend
warning-title multiset is also identical. Lint65515 CLOSED0 passes. Both pinned
nightly2026-01-30/2026-08-22 rustfmt parsers pass all17 frontend/6 SDK changed/new
Rust files after the final provider extension; git whitespace checks pass.
All build/test handles are closed.

The15188-byte historical STR_TO_DATE gzip retains SHA256
`fd9ed6080649e8987dd86b64bd761998c7db2bf66cd8aff52fdbd4a9a9464571`;
the32388-byte Go partial-calendar JSON retains
`b39e3c61207f877917bea98ff68b87f2c01901b04070f644e2a70eb40ef84a8f`.
Both still decode as JSON. `n7j-baseline-check.log` and
`n7j-format-fixtures.log` record exact comparisons.

Commands from the respective repository root:

```sh
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib unix
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-unistore --lib unix
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-unistore --lib date_format
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tikv test --release --locked --offline -j8 -p tidb_query_expr --lib
RUSTFLAGS='' TIKV_FRAME_POINTER=0 make run COMMAND='bash /home/agent/.slock/agents/1555103c-9f3e-46ca-ac2e-b964c650c2f1/evidence/no-fallback/n3-clippy-scoped.sh --lib --tests --release --locked --offline -j8'
make lint
```

## Remaining scope

TSO and other monolithic time Ready callers, generic admission fallback,
unistore's old SimpleExpr walker/unsupported sources, the Go `%j` semantic gap,
inherited frontend/SDK/datatype failures and full server/SQL gates remain open.
A future Go-signature audit must also compare declared Decimal precision with
this native generic helper's historical input-derived FSP; this checkpoint
restores the original return-family boundary without silently changing that
historical policy. No complete Go-package transcreation or broad PB-source
coverage is claimed. Global task4 remains active after the signed checkpoint.
