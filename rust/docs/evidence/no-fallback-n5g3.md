# N5g3: additive producers inside native row-demand boundaries

TiDB parentf3cb86f7 plus this commit; paired TiKV973af06. Signed local
checkpoint, no push. The no-fallback goal remains active.

PLUS, MINUS and MUL now produce native values beneath the existing native
CAST, conditional and other row-demand boundaries. This closes the explicit
CAST(PLUS(...)) migration gap. The common HostFrame requests each actual
operand; the provider owns immutable metadata and demanded values, never an
Expression, ScalarFunction or evaluation callback.

Native arithmetic metadata records original source declarations, actual
constant/binary-literal classification, argument and result identities,
typed/generic input modes and bounded diagnostic information. The new plain
numeric operand boundary shares typed_constant_value, nonconstant numeric
normalization and the original numeric-argument cast policy with existing
callers. It preserves UInt bit carriers, hybrid conversion, real/Decimal
conversion diagnostics and the original context access points.

The row protocol preserves left conversion before right demand. Typed integer
NULL and numeric MINUS/MUL NULL stop before the right operand; numeric REAL
PLUS still reads both sides. A failed typed integer fast probe requests fresh
arguments through the same HostFrame for its original generic pass. The
subtraction-mode read stays in integer preparation. Generic arithmetic retains
its div_precision_increment observation after both values and before coercion.
Prepared values call the N5g2 shared kernels directly, pass through original
return coercion and gain the arithmetic producer's own checked result identity.
Borrowed payload and diagnostic-copy allowances precede clones and conversions;
the task's retained left value is included in its live byte allowance.

Preserving batch phases is a separate requirement. Existing complete compiled
arithmetic reads the entire left batch, then the entire right batch, then
computes answers. The first candidate's automatic native root selection
changed that behavior and failed13 existing phase/contract checks. Native
arithmetic roots therefore retain their existing typed source selection. ABS
containing a newly native arithmetic descendant also retains a proved existing
typed closure. The phase, global-flag, strict-source, failure-prefix and exact
source-metadata tests remain intact. New arithmetic value nodes extend the
row-demand islands; they do not silently redefine a bulk root as row execution.
Bulk-aware native orchestration remains necessary for root profiles whose
original compiled closure is missing.

Numeric source rendering now has a shared borrowed literal formatter. Existing
owners keep eval_in; compiler rendering reads literal_value only. A reverse
source pass bounds both diagnostic forms and needed real-to-Decimal subjects
before conversion, capture or formatting. Bounds cover long Rust float display,
wide Decimal padding and full JSON formatting work before its64-byte diagnostic
truncation. Known unavailable source shapes skip formatting completely.

A new regression reproduced premature formatting of a nonfinite raw JSON
literal in an unselected branch. These literals have a native Display failure,
which originally occurs only when a diagnostic actually needs that value.
The compiler now retains its exact fixed eight-byte literal without formatting
it. An actual diagnostic demand calls the original native formatter. The first
unavailable/failing child state preserves left-to-right rendering precedence;
no explicit panic or substitute SQL error is invented. This is immutable
literal metadata, not an expression callback or an evaluated-argument program.

Validation:

- 28 arithmetic roots ×3 operators ×2 subtraction modes ×3 selections =504
  executions inside an explicit native row boundary. Exact raw values, NULLs,
  native errors, complete warnings and mode/configuration reads match the
  original row evaluator. Empty, repeated and2050-occurrence selections cover
  signedness disagreements, integer bounds, NULL/right-error demand,
  nonfinite IEEE/F32, wide Decimal, prefix strings, binary/enum/set/temporal/JSON
  values, vectors and sentinels. A warning-producing mismatched computed Int
  proves that a failed fast probe really demands its child twice.
- Eighteen additional rewritten SQL forms cover all three operators over
  signed, UInt, Decimal, raw float and string columns, plus conditional NULL
  demand. The combined SQL test runs57 forms ×6 selections =342 executions
  with exact errors/warnings and no fallback/refusal.
- Source-format counters prove large Decimal/JSON work fails before formatting,
  unavailable shapes skip formatting and minimum-subnormal float diagnostics
  admit the complete long display. Six demand/order cases retain nonfinite
  JSON's actual formatting failure, skip it in dead branches and retain generic
  overflow when an earlier unavailable source prevents that formatting demand.
  RED evidence: n5g-native-arithmetic-format-demand-red.log.
- Thirty-nine new production Projection roots compare serial and two-worker
  execution. All22 Projection tests pass on the final source.
- Focused batch106 pass. Full frontend1464 pass, four exact inherited failures,
  95 ignored. Core1044 pass, six exact inherited failures and one ignored.
  Both inherited failure-name lists were compared with N5g2.
- make lint, scoped core clippy, scoped pinned rustfmt and git diff --check pass.
  Logs: workspace evidence/no-fallback/n5g-native-arithmetic-{final-values,
  front-full-final,core-full,projection-final,lint-final,clippy}.log.

Primary commands from their respective tree roots:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib evaluator::batch
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tikv test --release --locked --offline -j8 -p tidb_query_expr --lib
    make lint

The fresh frontend test binary emitted by the focused build was also run
without a filter. Full suites are not claimed green.

The transitive arithmetic runtime path contains only plain numeric conversion,
prepared values, shared core kernels, original return coercion and checked
publication. Only the original remaining callers use into_evaluated. Logical
copy allowances do not constitute a hard statement allocator-peak tracker.

Remaining work includes bulk-aware native argument phases, remaining root and
caller profiles, other functions/effects, dynamic and additional sources,
multi-output coverage and final global migration-dispatch removal. Retirement
must audit every caller's row/bulk demand contract; retaining those contracts
is part of completing the full goal.
