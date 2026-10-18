# N7h: actual legacy DATE_ADD/SUB demand and values

Local signed SDK commit `cba2532` is paired with this TiDB implementation and
receipt; neither is pushed. Final validation is complete. This is a local
controller-retirement checkpoint, not completion of the global migration.

## Implementation and ownership

The frontend legacy text/time/duration entries no longer encode requested child
values or execute Ready head/step/zone-bound parse programs. Their actual SDK
`NativeLegacyDateArithmeticValues` state owns only demanded unit/date/Decimal
scratch, parsed raw temporal values and phase. Typed nullable source views retain
the original byte, folded i128, Real, raw Decimal, Time and Duration channels.
The owner alone obtains a requested source using its original reader; an original
callback error E remains E. The state never evaluates an expression or builds a
secondary signature program.

Explicit wire compatibility retains its metadata, codec, validation, output
planning, report tags and raw pending date identity. It delegates head/step/parse
control to the same actual state. The old arithmetic/control bodies are removed;
the native temporal primitives still perform the original arithmetic. The private
compatibility constructor keeps the existing transport planner's policy rather
than raising a runtime limit or changing its accepted replies.

The frontend MyDecimal facade already delegates directly to the shared datatype
`NativeMyDecimal`. The actual state calls that exact from_string, optional HalfUp
round-to-zero and byte rendering primitive; no decimal text/report round trip is
needed by ordinary native callers. Wire compatibility still exposes the original
DecimalText request and consumes the original owner's supplied reply bytes.
This is not a new claim of complete Go-package transcreation.

## Observable contract

Text-returning domains demand unit, date, explicit-zone parse, then interval.
Typed Time/Duration demand date, unit, then interval; zero typed Time still reads
both later sources. NULL stops at the actually requested channel, including a
zero-budget NULL source. Out-of-i64 folded date integers become NULL before parse,
while interval integers retain full i128 formatting. Date parsing uses the
explicit legacy timezone and fixed flags0, never a fresh statement timezone or
date-mode getter. Unit and text UTF-8 normalization remains the original lossy
normalization, separate from ordinary DATE_ADD's string coercion.

String SECOND intervals use generic MyDecimal parse/render without rounding.
Non-composite Decimal intervals use HalfUp to zero. Date/duration arithmetic
preserves duration-before-calendar order, original plain signed products,
ignored ParsedInterval truncation, checked duration addition and max(source,delta)
FSP. Typed calendar bits/kind/FSP are not normalized. Presence is separately
produced by the SDK from the same execution: even a zero-duration result has
presence1. It is not a host ToBool conversion.

Finite work checks precede demanded lossy text/coefficient copies, visible Decimal
formatting, date/interval parsing, generic decimal work and output rendering.
Borrowed original source storage and the external legacy source reader remain
owned by the caller. These are conservative actual-controller work/retained
allowances, not a hard whole-statement peak allocation promise.

## Independent original observations before retirement

At TiDBdc69eb6f/TiKV3e94811, unmodified public legacy entries produced **1,188,864**
observations with fixed digest **F0099F79736B34C1** in
`n7h-original-and-red.log` (69542 CLOSED101). The same run reproduces prohibited
Ready hook panics in all three entry domains. Its initial zero-count/hash
assertions were capture assertions only, not semantic defect evidence.

The domain includes all six date kinds, four interval kinds, two signs, UTC/LA/
fixed+08 zones and three cache profiles; nullable typed values, raw malformed
text, out-of-i64 date/int128 interval boundaries, nonfinite floats, raw duration
FSP, nullable/unknown/lowercase/invalid-UTF8 units and four callback-error positions.
The observations preserve exact requested source index/channel, original context
identity, source warning side effects, original E errors, raw result identity
and separate presence.

The new actual SDK state and frontend candidate were initially added beside
untouched independent old wire controllers and public callers.
`n7h-original-paired.log` (98843 CLOSED0) matches all **1,188,864** cases exactly.
A second matrix adds the remaining12 clock/composite units, covering all20 valid
units across the two matrices. `n7h-original-all-units-paired.log` (10524 CLOSED101)
matches all **1,188,864** additional old/new cases before its final capture-only
zero-count assertion; its independently captured digest is **1F5BF8B3E75AD934**.
The first matrix passes again in that run. Thus **2,377,728 independent old/new
comparisons finish before old controller/caller retirement**. Both old digests
were fixed before removal, not regenerated from the replacement implementation.

Only then were old frontend/wire control bodies and paired new/new instrumentation
removed. Both fixed original digests now check the actual producer. The complete
paired test and original source files remain in workspace evidence as
`n7h-independent-paired-test.rs` and `n7h-before-{frontend,sdk}-legacy-date-arithmetic.rs`
for replay with the signed original heads.

## Actual-state unwind regression

Self-review identified a WIP public API affinity gap beyond ordinary Err paths.
`n7h-invariant-unwind-red.log` (68787 CLOSED101) supplies an invalid raw Decimal
coefficient to the requested interval channel. The original ASCII-invariant
formatter panic occurs as expected; after catching it, the same continuation
incorrectly accepts Decimal(None) and returns SQL NULL. This is a raw actual API
lifecycle defect, not a newly claimed SQL semantic bug.

Each public advance now marks itself closed before running and reopens only on
success. Returned errors and invariant unwind both prevent replay; no production
catch_unwind, panic normalization or fake SQL sentinel is introduced. The pending
text getter also rejects a closed continuation, while the internal generic
primitive uses its private accessor during the active attempt. The regression
preserves the original panic and demands InvalidResponse for the subsequent NULL.
The regression is green in the final complete SDK run87869; the original
formatter panic is still observed inside its test, followed by InvalidResponse.

## Validation and remaining boundary

- Frontend81306: all3 original/no-Ready tests pass after retirement; final complete
 3640 (`n7h-front-full-final.log`):1656 pass, exact same3 inherited failures,95 ignored,0 filtered; all226
 batch-module tests pass. The fourth new frontend test verifies typed resource
 failure after unit/date before interval, original callback E before work,
 zero-budget NULL and wrong-domain contract failure before any source.
- SDK51234: all5 focused tests pass, including all6 date x4 interval x3 source
 prefix wrong-channel/NULL/terminal cases, zero-time demand, i128 date boundary,
 zero-duration presence, demanded copy/visible-scale/date-parser/MyDecimal work
 refusal, failure replay rejection and fresh-owner reuse. Final SDK87869
 (`n7h-sdk-full-final.log`):1103 pass, exact same1 inherited failure,1 ignored,
 0 filtered, including the fourth new actual-state invariant-unwind regression.
- Unistore98991 and final60787: all4 date_add consumer tests pass. New test covers six date
 sources x four interval sources x two signs, explicit LA zone against forbidden
 statement zone/date-mode getters, raw results, bare presence and present zero
 duration. Its actual prepared Ready worker count remains0. The legacy evaluator
 still binds its existing lane scope; that legitimate query is not mistaken for
 actual report execution.
- Final clippy4787 (`n7h-clippy-final.log`):89 expression warnings / exact171-entry
 all-crate warning-title multiset match N7g; make lint76769 passes. The final
 SDK-only unwind guard is covered by final clippy/core/frontend/consumer runs;
 no TiDB production source changed after lint.
- Six changed/new Rust files pass both pinned whole-file rustfmt parsers. The
 unistore change is only141 added test lines and no deletion: the isolated added
 test module passes both parsers, leaving pre-existing whole-file/trailing blank
 formatting untouched. Both git diff checks and original STR_TO_DATE gzip/Go
 JSON SHA256/length checks pass; see `n7h-final-baseline-format-fixtures.log`.

No executor/batch-source production body or datatype source changed. N7g's26
Projection tests and prior datatype564-pass/exact-two-failure result are retained
as prior baselines, not claimed as rerun. The unchanged frontend failures are
`pushdown_catalog::tests::ifnull_string_column_literal_uses_go_signature_and_column_collation`,
`tests::builtin_info_json_math_source::exp`, and
`tests::builtin_math_misc_op_source::vectorized_builtin_op_func`; the SDK failure is
`local::str_to_date_worker_tests::str_to_date_workers_pass_whole_states_keep_wide_days_and_read_typed_mode_late`.

The broader unistore SimpleExpr walker, its other unsupported source families and
eight child-free Duration-to-Datetime ids remain global work. This checkpoint does
not replace them with ordinary scalar DATE_ADD semantics or claim canonical legacy
source migration. Other remaining Ready callers include extract, unix_timestamp,
from_unixtime and the monolithic wrapper, and generic admission fallback remains.
The Go `%j`/native wide-day semantic difference and inherited failures remain open.
No push, full server/SQL release, benchmark or complete package-port claim.

## Reproduction

From TiDB root:

```sh
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib legacy_date_arithmetic
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-unistore --lib cophandler::tests::date_add_
make lint
```

From SDK root:

```sh
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tikv test --release --locked --offline -j8 -p tidb_query_expr --lib legacy_date_arithmetic
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tikv test --release --locked --offline -j8 -p tidb_query_expr --lib
RUSTFLAGS='' TIKV_FRAME_POINTER=0 make run COMMAND='bash /home/agent/.slock/agents/1555103c-9f3e-46ca-ac2e-b964c650c2f1/evidence/no-fallback/n3-clippy-scoped.sh --lib --tests --release --locked --offline -j8'
```

Use both pinned nightly2026-01-30/2026-08-22 parsers for changed Rust code and
`git diff --check` in both repositories. Format the inserted unistore test as an
isolated test module to preserve unrelated existing formatting. No Go/Bazel
inputs changed, and the unrelated untracked BUILD.bazel remains untouched.
