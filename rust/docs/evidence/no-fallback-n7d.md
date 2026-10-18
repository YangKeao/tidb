# N7d: correct shared partial-calendar validation with preserved history

Base: TiDB64bce47f / TiKV8e3bf36, expression-unification-demo. SDK companion
signed locally as TiKVf325f8b, not pushed. Local verified
checkpoint under task4; no push, new evaluator, admission expansion, ignored
test or resource-limit increase. This is not a complete Go package port or a
claim of complete STR_TO_DATE scanner parity.

## Correction

The SDK calendar finisher previously rejected month0 before its NO_ZERO_DATE
check, and unconditionally rejected a fullzero calendar afterward. Removing
those two checks makes partial/fullzero calendars actual values when
NO_ZERO_DATE is off. Its explicit NO_ZERO_DATE rejection, invalid/wide-day
handling, parser, callback demand, resource gates and result publication remain.
Ordinary wire/native/source owners share that same actual policy.

Original Go SELECT sets IgnoreZeroInDate(true) in pkg/executor/select.go1163;
pkg/types/time.go2146/2179 permits zero month/day with that context, including
fullzero, while builtin_time.go2053/2089 separately applies NO_ZERO_DATE. Six
integration examples at expression/issues.result354–400 keep NO_ZERO_IN_DATE
but drop NO_ZERO_DATE and return partial dates. The independent Go probe
confirmed them plus four fullzero cases. The previous suspicion of another
NoZeroInDate rejection during typed result conversion was disproven: that
String parser already allows zero components, and typed-parent tests pass.

## Historical and independent evidence

Before production edits, capture39668 closed0 independently observed ALL844344
original results/identity bytes/errors/context events at the base, matching the
immutable N7b hash d7dd9f9ee5b55431. Three cache profiles were explicitly compared
and identical, allowing a281448-record profile to be stored using534 strings
and indices. The15,188-byte original gzip has SHA256
fd9ed6080649e8987dd86b64bd761998c7db2bf66cd8aff52fdbd4a9a9464571.

The temporary test generator also captured3906 original parsed head calendar
states AFTER the official whole-report decoder validated them; wide u32 fields
were not packed/truncated. Its replayable test-only patch cached-applies at
64bce47f. The generator is removed from the tree and retained in workspace
n7d-original-capture.patch/n7d-original-capture-test.rs. Original production
policy was still active during capture.

An independently compiled Go program imported original production pkg/types;
build80601 closed0. It validated168 representable zero-month calendar states
with Time.Check under SELECT flags, both invalid-date settings, verified every
packed field round-trips exactly, and rendered accepted text. It also called
original Time.StrToDate on the ten direct partial/fullzero fixtures and three
audit cases. Go JSON SHA256:
b39e3c61207f877917bea98ff68b87f2c01901b04070f644e2a70eb40ef84a8f.
The standalone Go source and reproduction command are included in the fixture
README; source/binary/results/provenance also remain in workspace evidence/no-fallback.
This was a standalone production-code probe, not a Go package unit-test run;
no failpoint source transform, Go-file repository edit or Bazel change occurred.

Current tests require826200 observations to remain EXACTLY original and18144
relaxed-mode records to match explicit Go-calendar accepted text, identity and
mode-only events. They clearly replay the historical digest from captured OLD
observations; they do not pretend corrected production still generates it.
Both fixture streams have fixed fingerprints and metadata/coverage checks.
Original N7b snapshots, patch and digest remain historical evidence.

## RED, fix and verification

- n7d-partial-calendar-red.log5998 closed101: new historical/delta and ten Go
  fixture tests plus inherited partial-date test fail on old code;14 other date
  tests pass. Earlier86170 only had a test-only &String constructor compile
  error, corrected before this behavioral RED.
- n7d-source-calendar-red.log67817 closed101: typed source Go-value assertion
  fails on old NULL. Earlier16346 only had test FSP type mismatch, corrected
  before this RED. n7d-sdk-calendar-red.log1502 separately fails actual SDK
  partial/fullzero validation BEFORE the production fix.
- Only after REDs closed were the two unconditional checks removed.
  n7d-partial-calendar-focused.log30841 closed0: all18date checks pass, including
  every historical/delta observation and the original inherited regression.
- n7d-front-full.log18207 closed101:1637pass, EXACT remaining3 failures,
  95ignored/0filtered/all218batch pass. Machine comparison proves the ONLY
  removed failure is time_fn::tests::str_to_date_partial_formats_follow_no_zero_date.
- New source matrix checks8modes/four flag runs; String/Date/Datetime FSP0/3/6;
  CHAR and MONTH parents; empty,10row and2050duplicate selections; independent
  Go values, exact NULL/raw bytes, no warning when relaxed, original scalar
  context effects and no fallback/refusal.
- n7d-projection.log45395 closed0: all26pass. The new production test checks four
  mixed partial/fullzero outputs with NO_ZERO_IN_DATE kept on, both NO_ZERO_DATE
  settings, both vectorization flags and workers0/2, explicit values and no
  relaxed-mode warnings. Existing bounded matrices and limits remain.
- Initial SDK89821 had inherited6 plus an existing ordinary-stages assertion
  expecting the superseded relaxed2020/%Y warning. It now requires EXACT
  Value2020-00-00 and separately retains the strict NO_ZERO_DATE1411/input
  warning control. Only this cfgtest expectation changed after final frontend/
  Projection; no production body changed.
- n7d-sdk-full-final.log48699 closed101:1091pass, EXACT inherited6 failures,
  1ignored/0filtered; new calendar and corrected ordinary-stage tests explicitly
  present and PASS. Failure list machine-matches N7c.
- n7d-clippy-final.log25766 closed0:89warnings, exact diagnostic-title multiset
  machine-matches N7c. Lint99982 closed0; pinned formatter checks five Rust files,
  JSON/gzip validity and both diffs pass in n7d-final-static.log.
- Datatype was not rerun: no datatype source changed; N7c564pass/exact inherited2
  remains its baseline. Go package-wide tests, server/shared-library and complete
  SQL cohorts were not run in this narrow calendar checkpoint.

## Reproduction and remaining work

From TiDB root:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib str_to_date -- --nocapture
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
    make lint

From TiKV root:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tikv test --release --locked --offline -j8 -p tidb_query_expr --lib
    RUSTFLAGS='' TIKV_FRAME_POINTER=0 make run COMMAND='bash /home/agent/.slock/agents/1555103c-9f3e-46ca-ac2e-b964c650c2f1/evidence/no-fallback/n3-clippy-scoped.sh --lib --tests --release --locked --offline -j8'

The Go oracle confirmed a REAL remaining scanner difference: its `%j` is
currently ignored after consumption, while native scanning stores wide day999.
It also showed invalid2021-02-30 retains actual parsed fields for diagnostics,
where one inherited worker expectation assumes fullzero. The complete legacy
worker oracle/scanner policy needs separate correction; these six SDK failures
and three frontend/two datatype failures remain real pending work, not excused
or reclassified. Other native Ready callers, PB/source families, global fallback
removal and full release validation remain open. This checkpoint does not
complete the user's goal.
