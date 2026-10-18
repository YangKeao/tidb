# N7n: actual duration, construction, format and difference execution

Intermediate local no-fallback checkpoint. The global task remains active; no
push, PR, deployment, complete Go-package port or full-server claim is included.
Validation is recorded below; final local DCO signatures identify the checkpoint.

SDK companion: local DCO commit `62db99a` on expression-unification-demo.

## Scope and original contracts

MICROSECOND, TIME, TIMEDIFF, TIME_TO_SEC, SEC_TO_TIME, TIME_FORMAT, MAKEDATE,
MAKETIME, FROM_DAYS, PERIOD_ADD, PERIOD_DIFF, GET_FORMAT and DATEDIFF now use actual
SDK values. Legacy raw DATEDIFF, nanosecond MICROSECOND, raw DATE predicate and
GET_FORMAT's grammar-selector adapter also stop constructing Ready invocations:
13 ordinary sources and 17 direct/legacy/grammar entries.

SDK native_duration_construction.rs owns the original actual-operand kernels and
seconds formatter, and native_time_diff.rs owns a consuming parsed-left state.
Existing datatype parsers/getters/formatters remain authorities. Wire workers
retain their decoding, NULL/metadata validation, typed period causes and output
bytes while delegating these kernels. No fabricated validity marker, expression
callback, byte-encoded native value or second argument program is introduced.

TIME returns actual text and a truncation disposition rather than a status byte;
its context still reports the original capped warning and zero-duration fallback.
MICROSECOND suppresses that parser error. TIME_TO_SEC/TIME_FORMAT deliberately use
their distinct parser: nonnumeric text such as `bad` means zero there, while
invalid colon fields and hours above838 reject. TIMEDIFF retains matching
calendar/duration kinds, written precision and clamping. TIME_FORMAT consumes
actual original text before late layout demand, sharing the datatype formatter.

MAKETIME converts hour, minute and second in original order, including after NULL
hour; numeric conversion of second still warns before validity. It computes
actual signed f64 seconds with original unsigned-hour provenance before demanding
precision. SEC_TO_TIME keeps original numeric/FSP and IEEE754 behavior. Decimal
float conversion really renders visible precision; integer projection reads
storage without unused display scale. Late precision and actual output padding
remain bounded without substituting normalized 0..6 metadata. FROM_DAYS preserves
its typed zero date, narrow NULL band and original signed conversion/panic domain.
Period failures retain exact1210 messages and both eager integer conversions.
Raw legacy core/nanosecond domains do not acquire SQL text validation.

Canonical DurationConstructionNative entries use the existing one-frame SDK host
protocol with original1..3 argument nodes, complete FieldTypes, result node and
role. All eager source capabilities authenticate before any conversion/callback.
Original DATEDIFF casts both ETDatetime arguments before signature text conversion;
GET_FORMAT casts both ETString arguments even if its first is NULL. Its direct
values reader separately preserves first-NULL suppression, and its grammar
adapter preserves strict location coercion. Identity string casts borrow actual
storage, so a huge ignored right operand is not copied. Callback-free stages
borrow authenticated actual values; TIME, SEC_TO_TIME and MAKETIME's numeric
warning stage use checked owned copies. No value-store guard crosses callbacks.

Consuming Prepared states own actual integers/text/parsed operands. The host
closes argument demand before a conversion can error or panic, so caught panic
cannot replay the invocation into NULL. Later invocations recover normally.
Only the original TIMEDIFF/SEC_TO_TIME/MAKEDATE/MAKETIME/FROM_DAYS names run generic
temporal result finishing; TIME deliberately does not. Every source retains the
ordinary return-family boundary once afterward. Finite bounds cover logical
work and ownership, not hard whole-statement peak heap or allocation recovery.

## Independent evidence and explicit retired-cache difference

Before candidate production edits, old cached call history produced5839272
observations, FNV1a **AA540CDA49D3B88D**, in n7n-original-red.log (session91188).
All17 Ready and all13 canonical-source refusal counterexamples reproduced. The
zero-placeholder assertion was capture machinery, not a SQL semantic RED.

The first independent candidate comparison exposed old ReadyValueCache poisoning:
FROM_DAYS's original conversion panic makes later unrelated cached calls return
ExpressionAdapterFailure ScopePoisoned. Copying that retired-cache artifact into
actual kernels would retain a separate backend state. The original panic remains;
subsequent actual SQL calls recover. This is an explicit historical change, not a
rewritten old hash or silently dropped sample.

Final proof36113 retained both old callers and worker bodies. For every sample it
executed (1) actual old reused-cache history, (2) a fresh unpoisoned old call with
identical context settings, and (3) the actual candidate. All **5839272** clean-old
SQL/candidate pairs matched. Old history still hashed **AA540CDA49D3B88D**. The
independently captured clean-old SQL hash is **E07682675B3A9EFD**; it is folded from
old calls, not candidate output. Exactly **1282080** historical differences each
asserted the exact old ScopePoisoned/Scope cause and empty event trace. No other
historical difference was accepted. This proof passed before retirement; old
history and independent tests/logs are archived in the agent evidence workspace.

The permanent5839272/**E07682675B3A9EFD** witness exercises actual SQL after
retirement. Historical poisoned-cache tests are archived, not newly ignored or
claimed as post-retirement comparisons. The corpus includes ordinary Datum
fixtures, every two-argument Cartesian pair, every three-argument position and a
compact1000-case constructor Cartesian set; raw temporal cores/kinds/FSP/nanos;
arity errors, malformed bytes, NULLs, constructor/date/period/precision boundaries,
legacy inputs and grammar selectors; no cache and two independently reused caches
(not three different policies), eight contexts and three error levels. Observed
results include temporal identity, exact errors, panic payload and getter/warning
traces. Original code, capture and proof artifacts remain under
agent-workspace evidence/no-fallback/n7n-*.

## Validation

Completed:

- Focus36122:6 passes, including frozen SQL witness,17 Ready/13 source regressions,
  dynamic source matrix, provenance/resources/context and original panic/reuse.
- Matrix85852:all13 names,11 input declaration families, all argument positions,
  eight result declarations, four context seeds, three error levels, four flag
  transitions, empty/full/reordered/duplicate rows, CHAR parents,2050 TIME warning
  occurrences and eager ABS errors. Exact errors and callback/warning traces match.
- Matrix deliberately retains contradictory return declarations. TIME_FORMAT's
  numeric layouts declared DATE can leave the original fallback String in
  fixed-width storage; indexing those malformed cells panics in the old column
  reader. Complete ColumnRead equality instead verifies exact physical bytes,
  offsets, NULL bitmap and layout, preserving all cases. This is representation
  parity, not certification that contradictory declarations are valid schemas;
  malformed-shape gates remain global work.
- Provider covers both output roles/all13 operations/each original1..3 arity and
  every argument's wireNULL, corrupt prefix, stale window, wrong role/node/
  occurrence/inputrow. No callback occurs before complete eager authentication.
  Normal/NULL/malformed/invalid-UTF8 SQL outcomes and traces match ScalarFunction;
  ignored1MiB right values, repeated cancel/expired-task replay and exclusive
  mode/zone/clock/truncate/warning access pass. FROM_DAYS's retained raw panic
  rejects replay and a later vector conversion succeeds. Its128KiB allowance
  accommodates the shared conservative Time-to-string return formatter, while
  remaining far below the ignored1MiB copy. The initial16KiB test allowance was
  insufficient for that existing formatter policy; production limits were not
  changed to make the test pass.
- Direct bounded tests distinguish original first-integer overflow from unused
  huge third input, genuinely demanded million-digit Decimal float formatting
  from storage-only period mode, late raw-Date/FSP255 output work from invalid
  MAKETIME-part suppression, unconditional numeric warning despite NULL hour,
  and GET_FORMAT's source casts versus direct first-NULL reader.

Final runtime checks after the GET_FORMAT resource fix:

- Frontend15293 n7n-front-work-final.log: **1705 pass /exact3 inherited failures /
  95 ignored /0 filtered**, including **all254 batch tests**. Frontend warning-title
  multiset exactly matches N7m.
- SDK6267 n7n-sdk-work-final.log: **1119 pass /exact1 inherited failure /1 ignored /
  0 filtered**. The four actual-value API tests pass after correcting the new
  expectation to the original nonnumeric TIME_FORMAT zero policy.
- Focus55358 n7n-focus-work-final.log: **7 pass**, including the genuine8KiB/1MiB
  GET_FORMAT budget RED→GREEN and unchanged5839272/E07682675B3A9EFD witness.
- Clippy78852 n7n-clippy-final.log: **89 expression /171 all warning-title multiset
  exactly N7m**. The one additive items-after-test-module warning is removed by
  placing actual TIMEDIFF value items before tests.
- Projection54121 n7n-projection-final.log: **26 pass**, including the new27-root
  group with13 operations/13 CHAR parents/one older arithmetic root, original
  typed first-argument IF(column0,value,NULL), both flags/workers0/2. Previous
  groups and contexts remain unchanged.
- Final lint58803 passes. Both pinned formatters verify all17 changed/new Rust
  files (edition2021/skip_children=true). Both immutable STR_TO_DATE fixtures keep
  their original length/SHA256. Both git diff checks pass. One wire error helper's
  signature was formatted after runtime verification; no logic changed afterward.
  Local DCO signatures are recorded in the commits and agent memory.

Exact inherited frontend failures:
`pushdown_catalog::tests::ifnull_string_column_literal_uses_go_signature_and_column_collation`,
`tests::builtin_info_json_math_source::exp`,
`tests::builtin_math_misc_op_source::vectorized_builtin_op_func`.
SDK inherited failure:
`local::str_to_date_worker_tests::str_to_date_workers_pass_whole_states_keep_wide_days_and_read_typed_mode_late`.
No failure was filtered, dropped, reclassified or newly ignored.

GET_FORMAT's fixed table compares against fixed-length literals; a longer byte
view fails a length check without scan or copy. The initial implementation wrongly
charged both full input lengths despite borrowing them. A genuine regression
failed in14629 before correction. Shared lookup now budgets only its fixed work
and small output; direct values borrow both reader views too, eliminating an
unnecessary owned-kind stage. Source still authenticates both eager capabilities
and performs both actual ETString cast actions before its first-NULL boundary.
Demanded numeric formatting/unsupported errors remain demanded. Source/direct/SDK
large non-NULL unknown-byte examples pass at8KiB with1MiB inputs. This changes
finite resource admission, not the independently captured ample-budget SQL result.
Full frontend/SDK/Projection/clippy are rerun after this production fix.

Exact commands from respective repository roots:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib duration_construction_ -- --nocapture
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tikv test --release --locked --offline -j8 -p tidb_query_expr --lib
    RUSTFLAGS='' TIKV_FRAME_POINTER=0 make run COMMAND='bash /home/agent/.slock/agents/1555103c-9f3e-46ca-ac2e-b964c650c2f1/evidence/no-fallback/n3-clippy-scoped.sh --lib --tests --release --locked --offline -j8'
    make lint

No datatype/unistore production edit or rerun is included. Their prior failures
are prior evidence only. No Go/Bazel edits trigger bazel_prepare. The unrelated
untracked client differential BUILD.bazel remains excluded. Generic admission
fallback, other time/format entries, the unistore old walker/unsupported sources,
malformed-shape and Go semantic audits, inherited failures and full-server/SQL
gates remain open. This checkpoint does not redefine completion of the global goal.
