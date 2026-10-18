# N7g: ordinary date arithmetic actual values and canonical sources

Local signed SDK commit `3e94811` is paired with this TiDB implementation and
receipt; neither is pushed. Final validation is complete.
Legacy PB arithmetic and the global no-fallback migration remain open.

## Implementation and preserved contract

The frontend ordinary DATE_ADD/SUB calendar, AST-default and typed-duration
entries use the SDK's opaque `NativeDateArithmeticValues` continuation. Actual
borrowed native inputs, typed metadata, owned demanded text and prepared numeric
values replace encoded Ready reports. The continuation requests only original
pure datatype conversions; it never evaluates a child expression. Calendar and
duration arithmetic still use `native_date_arithmetic_helpers` unchanged.
Explicit wire compatibility keeps its codec, validation, output planning and
original report layout, delegating the controller to the same actual state.

Composite calendar intervals prepare amount before date, including a NULL date.
Duration arithmetic checks the actual date and unit first. String amount
truncation warnings are unconditional; calendar overflow reads the statement's
truncate disposition only after actual overflow. A pending warning can be
replaced by later internal work before the next external boundary; accepted wire
continuations preserve this behavior. That contract is not claimed to reproduce
a normally reachable ordinary SQL warning-replacement case. AST mode retains
`NoColumns` semantic policy, irrespective of its caller's warning settings.
Distinct Real/Float32 rounding, raw Decimal coefficients, SECOND parsing and
amount-declared precision are preserved.

The scalar function and canonical source host share one frontend pre/post
policy: infer result FSP before evaluating both eager operands; retain the
original Duration result branch, Duration-to-Datetime argument cast and computed
Date/Datetime result conversion. The source compiler snapshots original argument
fields, result fields, static unit and sign within finite metadata bounds. The
host authenticates both input capabilities and occurrence, bounds owned clones,
and drops value-store borrows before conversions, warnings, date modes, timezone,
clock and overflow-policy callbacks. Bytes, DateTime, Duration and native-value
publication retain checked ownership and original raw representation.

## Independent original observations and actual failures

At original TiDB49333e87/TiKVe212b91, `n7g-original-and-red.log` (93887 CLOSED101)
records **2,325,024 observations / FEEA99CD98022F82**: 46 date operands, 39 amount
operands, 16 units, nine calendar/default/duration configurations, three cache
profiles and three truncation policies. Result/debug errors, raw identity bytes,
warnings and context reads enter the fixed digest. The initial zero digest was a
capture assertion only. All nine entry variants independently hit the prohibited
Ready hook; the canonical source also reproduced refusal/fallback(1,1).

New actual controllers were initially introduced beside untouched old public
callers and wire controller bodies. `n7g-original-paired-final.log` (60826 CLOSED0)
compares **all 2,325,024** independent old/new observations exactly, before old
retirement. The earlier 79127 run was a compile-only Option<String> correction,
not semantic evidence. After comparison, old caller/controller bodies and paired
new/new instrumentation were removed; the literal original count/digest remains
checked against the actual producer. A test-only duration wrapper now calls the
production shared scalar helper after the obsolete private wrapper was removed.
The original capture test and pre-change files are retained in workspace evidence
for replay with the signed original heads; neither the digest nor old STR_TO_DATE
fixtures is regenerated from modified production code.

The WIP continuation had an independently reproduced error-state defect:
`n7g-value-resource-red.log` (11110 CLOSED101) returned SQL NULL after rejecting
an oversized coercion response. All failed advances now permanently invalidate
the state; subsequent NULL/correct-value replays fail the contract. Focused SDK
23664 and later 45827 both pass this regression.

A broader source test initially prohibited the reference suite's scope-binding
cache query. `EvaluatorSuite::run` always calls `ReadyValueCache::with_columns`
before evaluating any expression. The reference context alone now returns None
for that query; canonical source and direct-entry contexts still panic on every
Ready lookup. This test-scope correction is not a production fallback exemption.

## Resource boundary follow-up

`n7g-incoming-capacity-red-final.log` (93651 CLOSED101) reproduces a second WIP
API defect: a received String with capacity6000 but text"bad" under a6000-byte
allowance could still format a warning. The incoming owned capacity is now
charged while live and transferred exactly once into retained date/amount text
or cleared after drop. The regression passes in the final full frontend run,
as does invalid NULL replay after this refusal. The first99444 attempt was an
import compilation error, not behavioral evidence.

A separate formatter probe (`n7g-formatter-boundary.log`) confirms that valid
precision65535 does not expand NaN/inf text, but precision65536 and above panics
in Rust's format-parameter handling. The initial finite-work test39158 reached
that panic under a small allowance. The new bounded API now precharges invalid
precision work; ample work still reaches the original panic, checked by the SDK
regression. Explicit old wire planning already ignores nonfinite precision, so
its original panic is deliberately retained. Valid nonfinite text continues to
contribute zero under the original composite interval parser; an initial new
NULL expectation was wrong and corrected without altering that arithmetic.
These are bounded API resource checks, not newly claimed SQL semantic fixes.

## Final validation

- Frontend45587 (`n7g-front-full.log`) CLOSED101: **1652 passed / exact same
  three inherited failures /95 ignored /0 filtered**; all **226** tests in
  evaluator::batch modules pass. All eight new date arithmetic tests pass,
  including the fixed2325024/FEEA99CD98022F82 original witness, all nine direct
  no-Ready entries, source/context/provider and finite resource regressions.
- SDK58621 (`n7g-sdk-full.log`) CLOSED101: **1099 passed / exact same one
  inherited failure /1 ignored /0 filtered**. All three new actual-state tests
  pass, alongside unchanged ordinary/legacy wire and worker lifecycle checks.
- Clippy60088 (`n7g-clippy.log`) CLOSED0: **89 expression warnings**; the whole
  **171-entry warning-title multiset is exactly unchanged** from N7f, excluding
  only generated-count and future-incompat summaries.
- Source context matrix covers eight changing date-mode seeds, three truncation
  levels, alternating timezone, original clock, all four result domains, typed
  parents, empty/full/2050-duplicate selections and repeated vector flags.
  Warning1292 can change the later overflow disposition to Error; NULL left
  still evaluates right ABS overflow sources in simple and composite intervals.
- All four provider outputs and both argument prefixes cover stale/wrong
  role/node/occurrence capabilities, NULL prefixes, resource refusal, repeated
  cancellation, valid reuse and exclusive store borrowing in callbacks.
- Production Projection83008 (`n7g-projection.log`) CLOSED0: all **26** pass.
  Its new mixed group contains integer, DATE_ADD string/datetime, DATE_SUB
  duration and CHAR parent roots, both vector flags and workers0/2; compares
  raw bytes, NULL, warning order and prepared/fallback/refusal counters without
  replacing earlier groups.
- `make lint`33610 and final-source6348 both pass. Both pinned parsers
  accept all15 changed/new Rust files; both `git diff --check` pass.
  `n7g-baseline-and-fixture-check.log` verifies exact inherited failure lists,
  warning multiset and unchanged original gzip/Go JSON fixture SHA256/lengths.
  No datatype source changed, so the prior564-pass/exact-two-failure datatype
  result is retained as a prior baseline rather than claimed as rerun.

The remaining frontend failures are
`pushdown_catalog::tests::ifnull_string_column_literal_uses_go_signature_and_column_collation`,
`tests::builtin_info_json_math_source::exp`, and
`tests::builtin_math_misc_op_source::vectorized_builtin_op_func`.
The remaining SDK failure is
`local::str_to_date_worker_tests::str_to_date_workers_pass_whole_states_keep_wide_days_and_read_typed_mode_late`.
These names are compared exactly against N7f, not hidden by filters or ignored
annotations. The Go `%j`/native wide-day difference remains pending and neither
calendar oracle is rewritten by this checkpoint.

## Reproduction

From TiDB root:

```sh
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib date_arithmetic
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
make lint
```

From SDK root:

```sh
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tikv test --release --locked --offline -j8 -p tidb_query_expr --lib
RUSTFLAGS='' TIKV_FRAME_POINTER=0 make run COMMAND='bash /home/agent/.slock/agents/1555103c-9f3e-46ca-ac2e-b964c650c2f1/evidence/no-fallback/n3-clippy-scoped.sh --lib --tests --release --locked --offline -j8'
```

Use the same pinned rustfmt toolchains as N7f, all changed Rust files, and
`git diff --check` in both repositories. Workspace evidence is under
`evidence/no-fallback/n7g-*.log`. No datatype production source, original calendar
oracle/STR_TO_DATE fixture, Go/Bazel input or runtime resource maximum is changed.
No push, full server/SQL release, benchmark or complete package-port claim.
