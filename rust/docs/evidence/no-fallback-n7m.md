# N7m: actual calendar values and canonical sources

This is an intermediate local checkpoint of the still-active no-fallback task.
No push, PR, deployment, full-server validation or complete Go-package port is
claimed. Validation is recorded below; local DCO signatures identify this checkpoint.

SDK companion: local DCO commit `4b20565` on expression-unification-demo.

## Scope and contracts

The native entries DATE, YEAR, MONTH, DAY/DAYOFMONTH, QUARTER, HOUR, MINUTE,
SECOND, DAYOFWEEK, DAYOFYEAR, WEEKDAY, MONTHNAME, DAYNAME, WEEKOFYEAR, WEEK,
YEARWEEK, LAST_DAY, TO_DAYS and TO_SECONDS no longer construct Ready invocations.
Legacy MONTH/WEEK raw core and three signed-nanosecond HMS entries use actual
SDK operands too: 20 names, 19 policies and five legacy routes.

SDK `native_calendar.rs` retains the existing datatype parsers and getters. The
original worker calendar projections, month-end arithmetic and week-year sentinel
now have one actual-value owner. Wire adapters retain their UTF-8/raw-core/mode
validation and output kinds while delegating the same value functions. Their
already-materialized text supplies a checked finite parser extent; native callers
supply their own independent finite work allowance. No encoded actual-value
carrier, fake validity witness or host-side answer is introduced.

WEEK captures an actual parsed YMD before optional mode conversion and consumes
that state once. Explicit NULL mode stays NULL until the existing mode-zero
policy. Invalid dates suppress mode conversion; all eager source expressions and
capabilities still run and authenticate first. WEEK's session default getter
remains eager after its original ETDatetime cast, including explicit modes and
NULL dates. YEARWEEK never reads that getter and defaults to zero. Legacy WEEK
remains a nonvalidating raw-core mode-zero operation.

DATE captures its own modes once, retains the original warning/error order, and
uses the same captured flags after callbacks. It projects the complete raw core,
including hidden clock bits of Date-kind operands, to Date/FSP0. Accepted dates
never format unused fractional precision; rejected dates preflight diagnostics
but preserve the original ample-budget raw-FSP255 panic.

Canonical sources retain original source/result FieldTypes, in particular YEAR
source metadata. All except HMS apply the same ETDatetime argument boundary;
HMS retains Display-to-text parsing. Only LAST_DAY has the original generic
result temporal finishing. Every source then runs the ordinary return-family
boundary once. Borrowed value-store guards never cross context callbacks. Mode
conversion borrows an authenticated actual second operand only after date
validity, without cloning large ignored values or charging unused Decimal display
precision. Actual parser/conversion/copy/output work has finite bounds; these are
logical allowances, not whole-statement peak-heap claims.

## Independent original evidence

Before candidate production edits, the old callers/workers produced **246096**
observations with FNV1a **E72FB95D39CD0172**. The corpus includes no cache and two
independently created reuse caches (not three distinct policies), eight dynamic
mode/default/zone/clock seeds, all three truncate levels, every entry's arity
errors, ordinary Datum fixtures, zero/invalid/leap/year/week/text boundaries,
raw core/kind/FSP combinations, explicit NULL/malformed/overflow modes, and
legacy NULL/full-width raw operands. Results include raw temporal identity,
errors and exact context/warning events.

An additive candidate was compared independently against all **246096** old
observations before replacing callers or worker bodies. An additional **24**
seed/kind comparisons checked raw-FSP255 DATE panic/nonpanic and context order.
Both tests passed in `n7m-pairs-final.log` before retirement. Archives in the
agent workspace `evidence/no-fallback/` preserve original time modules, SDK
workers, the pairing tests and capture/proof logs. The fixed old hash was never
regenerated from the candidate. Archived pairing filters now select no tests;
they are historical proof, not a new post-retirement comparison.

The initial prohibition checks reproduced all 25 native/legacy Ready routes and
all 20 canonical-source refusals. YEAR/HMS's original direct entry is in
`time_fn::calendar`, not `time_fn::dispatch`; the capture helper was corrected
before recording the frozen old hash. The zero-placeholder hash assertion was a
capture mechanism, not a semantic regression counterexample.

## Validation

Completed validation:

- Frontend full66745: **1698 pass / exact3 inherited failures /95 ignored /
  0 filtered**, including **all251 batch tests**. Warning-title multiset exactly
  matches N7l. No inherited failure was filtered or reclassified.
- SDK full66478: **1115 pass / exact1 inherited failure /1 ignored /0 filtered**,
  including the new actual core/text/week/finite-work test and old wire workers.
- Calendar focus68499: **10 pass**, frozen246096/E72FB95D39CD0172 unchanged;
  all25 Ready/all20 source regressions green, dynamic source matrix, provider and
  bounded-work checks green. Four existing calendar filters also pass.
- Clippy37764: **89 expression /171 all warning-title multiset exactly N7l**.
  Root lint13799 passes. Both pinned formatters check all15 changed/new Rust
  files, edition2021/skip_children=true. Both immutable STR_TO_DATE fixture
  byte lengths and SHA256 hashes remain unchanged. Both diff checks pass.

Projection37396: **26 pass**. New41-root heterogeneous group includes all20
calendar names, their20 CHAR parents and one older arithmetic root, with both
flags and workers0/2. Previous contexts/groups remain unchanged. Only a misplaced
SDK clock documentation comment was relocated after runtime verification; no
production behavior changed afterward. Final DCO signatures are recorded in
the local commits and agent memory.

Source matrix checks six static input families including YEAR metadata, raw
DateTime and Duration, numeric Decimal and original strings. It compares actual
row/chunk bytes, NULLs, results/errors and getter/warning traces under four flag
transitions, dynamic context seeds, all three truncate levels, result declaration
families, explicit/NULL WEEK modes, CHAR parents and duplicate/reordered rows.
A2050-duplicate WEEK selection and NULL-head eager ABS failure also pass.

Provider checks both output roles, all19 operations and accepted1/2 arities.
Each argument rejects wireNULL, corrupt prefix, stale window, wrong role/node/
occurrence/inputrow before any callback. NULL first still eagerly authenticates
the mode, while a genuine ignored1MiB mode is never cloned or converted. Huge
first-operand work, repeated cancellation/expired-task replay, later reuse and
exclusive mode/zone/clock/truncate/warning callback access pass. Direct checks
retain storage-only Decimal mode conversion despite unused million-digit display
precision and resource-before-render versus ample-budget original FSP255 panic.

Exact inherited frontend failures:
`pushdown_catalog::tests::ifnull_string_column_literal_uses_go_signature_and_column_collation`,
`tests::builtin_info_json_math_source::exp`,
`tests::builtin_math_misc_op_source::vectorized_builtin_op_func`.
SDK inherited failure:
`local::str_to_date_worker_tests::str_to_date_workers_pass_whole_states_keep_wide_days_and_read_typed_mode_late`.

Logs remain under the agent workspace evidence/no-fallback/n7m-*; the numeric
handles above are actual completed execution sessions, not test result counts.

Exact commands from their respective repository roots:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib calendar_ -- --nocapture
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tikv test --release --locked --offline -j8 -p tidb_query_expr --lib
    RUSTFLAGS='' TIKV_FRAME_POINTER=0 make run COMMAND='bash /home/agent/.slock/agents/1555103c-9f3e-46ca-ac2e-b964c650c2f1/evidence/no-fallback/n3-clippy-scoped.sh --lib --tests --release --locked --offline -j8'
    make lint

No datatype/unistore production change is included. Their existing failures and
remaining walker migration stay open, as do generic admission fallback, other
monolithic time functions, malformed source shapes, Go semantic audits and
full-server/SQL gates. No Go/Bazel change triggers bazel_prepare in this scope.
The unrelated untracked client differential BUILD.bazel is excluded.
