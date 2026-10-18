# N7i: actual EXTRACT values and canonical sources

Signed local SDK commit `1017778` is paired with this TiDB implementation and
receipt; neither is pushed. Final validation is complete. This EXTRACT
controller/source checkpoint does not complete the global migration.

## Behavior and ownership

Ordinary EXTRACT selects its original datetime, duration or mixed-string cast
using the actual value and original declared source FieldType. Known named
Timestamp/Date/Datetime types retain their identity; Unknown lookalike codes do
not become temporal. Actual Time storage also contributes to selection. Existing
argument wrappers alone own string/datetime/duration conversion, timezone, clock,
date modes, truncation disposition and warnings. No extra result cast is added.
The scalar owner still eagerly evaluates both children before nullable-unit
handling. Units retain original byte coercion, lossy UTF-8 and ASCII uppercase.

The SDK actual mixed state borrows the original unit/text and retains parsed
nanoseconds/FSP. A successful duration parse precedes a separately demanded
second allow-invalid-dates flag; finish consumes the state. It parses datetime
from the original text and original get-time-FSP logic, and compares actual
hour/minute/second against the retained duration. No continuation report is
encoded, and the first parse is not replayed. Invalid/truncated/overflowing mixed
duration text remains a hard SQL1292 error irrespective of truncate level.

The eleven named one-argument composite functions preserve their different broad
policy: right-aligned duration groups, unrestricted u32 hours, six-digit fraction
truncation, Rust trimming, bad time suffix defaulting to zero clock, unknown unit
zero, and unconditional invalid-date warning1292 plus NULL. Existing signed
arithmetic remains unchanged, including release wrapping/debug overflow behavior.
Raw calendar bits and duration storage are not normalized through display text.

`native_extract_value` owns actual selection/control/results. Explicit wire
compatibility retains validation, output planning, codecs, tags and the original
25-byte mixed-state header; its adapters delegate to these same actual functions.
There is no retained alternate controller/math implementation. The broad pure
arithmetic helpers are shared unchanged.

Canonical hosts demand two scalar capabilities or one composite capability,
authenticate each node/role/row occurrence and window epoch, account for detached
payload clones and drop store borrows before casts or any context callback. A
NULL unit does not suppress the right source's error or authentication. Original
source metadata and immutable composite keywords are included in compile-time
metadata accounting. Integer and native-value output modes preserve original
NULL/raw results and parent-expression semantics.

Finite allowances precede unit normalization, actual-route byte/formatter work,
cast/parser/diagnostic work and output publication. Byte-unit and duration-display
preflights deliberately preserve their originally accepted invalid UTF-8 routes;
Bit/Raw mixed casts keep their original post-cast string error identity. YEAR
integer fast paths differ from overflowing unsigned formatting routes. Raw
Duration passthrough does not format huge FSP. No original invariant panic is
caught or translated. These conservative logical work/retained allowances are
not a whole-statement allocator peak promise, and no runtime limit was raised.

## Original observations and independent comparison

At TiDB6052b81b/TiKVcba2532, the original callers and old independent wire
controllers produced **1,107,720** observations with fixed old digest
**9EC51A59F6C44C40** (`n7i-original-and-red-final.log`, 62568 CLOSED101).
Five Ready entry-route panic hooks and two canonical-source refusal/fallback
counterexamples actually failed in that run. Initial zero-count/hash assertions
were capture assertions only, not evidence of a semantic defect. A preceding
compile-only run60712 used an incorrect FieldTypeCode spelling and is not RED
semantic evidence.

The matrix includes46 actual values,12 source fields,25 date/clock/composite/
unknown units,3 cache profiles,8 dynamic date-mode seeds and3 truncate levels:
993600 direct calls,82800 broad composites,3600 bad arities,72 NULL-unit controls
and27648 complete scalar-wrapper unit-normalization calls. Observations preserve
exact errors, raw result identity, NULL, warning text/order and mode/zone/clock/
truncate callback traces. No incidental raw-invariant panic occurred in capture.

New actual candidates were added beside untouched old callers and independent
wire controllers. `n7i-original-paired.log` (73850 CLOSED0) matched all1,107,720
observations before retirement, including full scalar normalization. NULL-unit
controls compare actual direct NULL, with no arithmetic. The old observations
alone feed the fixed digest. Only then were old controller/caller bodies and
paired new/new instrumentation removed. The fixed old count/digest now test the
actual producer; neither was regenerated from replacement outputs.

Workspace evidence retains `n7i-before-frontend-extract.rs`,
`n7i-before-sdk-extract.rs` and `n7i-independent-paired-test.rs` for replay with the
signed original heads. Historical STR_TO_DATE fixtures are unchanged; this
checkpoint changes neither their parser semantics nor their oracles.

## NULL-unit resource regression

Self-review reproduced a new source-integration defect in
`n7i-null-ignored-clone-red.log` (87053 CLOSED101): an authenticated16KiB right
value after a NULL unit failed the8KiB host allowance because the host copied it
before the actual helper's NULL suppression. Both original source expressions
had already run, and the right token was valid. The direct scalar helper already
returned NULL without value conversion/copy at zero work allowance.

The source host now authenticates both original capabilities first and publishes
NULL without detaching the ignored right payload. No source demand, origin check,
warning/error conversion or publication budget is bypassed. The same protocol
regression checks both typed/native outputs and retains all forged/stale right
prefix failures after NULL left. The final full frontend run60057 passes this
regression in both output forms. No limit was raised.

## Validation

The full SDK run (`n7i-sdk-full-final.log`,7571 CLOSED101) has1107 passes,
1 ignored and zero filtered. Its sole failure is exactly the inherited
`local::str_to_date_worker_tests::str_to_date_workers_pass_whole_states_keep_wide_days_and_read_typed_mode_late`.
All four new actual SDK tests pass: named/unknown/actual-storage selection,
separately supplied mixed flags and retained parsed FSP, broad arithmetic and
finite work, and byte/duration preflight routing. An earlier new-test assertion
incorrectly expected FSP0 for a date-prefixed zero clock; the original parser
retains6. Only that test literal was corrected, not production behavior.

Scoped clippy (`n7i-clippy-final.log`,12609 CLOSED0) retains89 expression warnings;
all171 all-crate warning-title multiplicities exactly equal N7h, without adding
suppression. `make lint` (60091 CLOSED0) passes. Final frontend
`n7i-front-full-final.log` (60057 CLOSED101) has1664 passes,95 ignored and zero
filtered. All231 `evaluator::batch` tests pass, including the NULL-unit clone
regression, direct work/context boundaries, source provenance and dynamic traces.
The exact three inherited failures remain:

- `pushdown_catalog::tests::ifnull_string_column_literal_uses_go_signature_and_column_collation`
- `tests::builtin_info_json_math_source::exp`
- `tests::builtin_math_misc_op_source::vectorized_builtin_op_func`

The full frontend warning-title multiset also matches N7h exactly. Fixed old
EXTRACT count/digest and the existing calendar fixtures pass in this full run.
Pinned nightly2026-01-30 and2026-08-22 rustfmt checks pass for all9 changed/new
frontend Rust files and6 SDK files; both git diffs pass whitespace checks.
The immutable15188-byte old STR_TO_DATE gzip retains SHA256
`fd9ed6080649e8987dd86b64bd761998c7db2bf66cd8aff52fdbd4a9a9464571`;
the32388-byte Go partial-calendar JSON retains
`b39e3c61207f877917bea98ff68b87f2c01901b04070f644e2a70eb40ef84a8f`.
Both also decode as JSON. Projection after the last host fix
(`n7i-projection-final.log`,71301 CLOSED0) passes all26 tests. The extended mixed
five-root suite combines an integer expression, scalar EXTRACT over nullable
strings, scalar duration EXTRACT, named broad composite and CHAR parent, checking
both vector flags and workers0/2 against original raw bytes, NULLs and warnings.
All previous heterogeneous groups are retained. All build handles are closed.
No datatype production changed; its prior564 passes/exact2 inherited failures are
prior baseline evidence only, not a rerun in this checkpoint.

Source-context checks already compare dynamic mode/zone/clock/truncate/warning
traces across8 seeds and3 levels, four repeated vector flags, CHAR parents,
empty/full/2050 duplicate selections, String/Time/Duration/YEAR/Unknown metadata,
computed CHAR children and all11 composites. NULL units retain right-side
ABS(MIN) source errors and suppress later invalid-UTF8 value conversion.
Provider checks cover typed/native outputs for scalar/composite calls, every
argument prefix, stale/role/node/occurrence forgery, clone versus actual-parser
budget errors, cancellation and reuse. Context callbacks take an exclusive
store borrow to ensure no borrow survives into them.

Commands run from the respective repository root:

```sh
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib extract
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tikv test --release --locked --offline -j8 -p tidb_query_expr --lib
RUSTFLAGS='' TIKV_FRAME_POINTER=0 make run COMMAND='bash /home/agent/.slock/agents/1555103c-9f3e-46ca-ac2e-b964c650c2f1/evidence/no-fallback/n3-clippy-scoped.sh --lib --tests --release --locked --offline -j8'
make lint
```

## Remaining scope

Other Ready timestamp callers, generic admission fallback, the unistore old
SimpleExpr walker and unsupported sources, the Go `%j` semantic difference,
inherited frontend/SDK/datatype failures and full server/SQL gates remain open.
This is neither complete PB-source coverage nor a complete Go-package
transcreation claim. Global task4 remains active after this signed checkpoint.
