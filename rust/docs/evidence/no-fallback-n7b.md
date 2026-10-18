# N7b: actual STR_TO_DATE states and canonical temporal sources

The native STR_TO_DATE caller no longer constructs head/date/typed-finish Ready
programs, encoded state reports or scoped context rebinding. Actual SDK owned
preparation, parsed calendar state and clock state drive conditional statement
mode reads and actual values/warnings. Canonical sources use the same SDK frame
and original temporal result conversion. Remaining date families, global fallback
removal and inherited semantic failures are incomplete. No push/PR/deployment.

## Semantics and ownership

Both original children are eager, including after a NULL first child. Both
capabilities authenticate before the actual-NULL prepass suppresses coercion of
both ready values. Otherwise input string coercion precedes format coercion.
Byte-kind UTF8 errors and range-sentinel errors precede later formatting work.
Named Datetime is distinct from Unknown(12); Timestamp and other declarations
retain original head behavior. The unchanged scanner computes actual parsed
fields, including wide day-of-year, and alone decides whether date_modes is
needed. Clock-only typed Datetime has its own later NO_ZERO_DATE decision.
Warnings retain exact code/message and getter order.

NativeStrToDatePrepared owns actual coerced input/format strings. Date state
contains opaque actual ParsedDateTime fields and a borrow of that owned input;
clock state owns actual clock text. No encoded report, host-selected class or
computed warning is supplied as a kernel operand. Native API budgets remain
finite; the explicit ordinary wire owner uses private None allowance with its
unchanged validators, whole-state protocol and input-length-plus64 report bounds.
Wire head/date/typed-finish now share actual policy and the original scanner.
Typed wire finish borrows its existing clock instead of introducing another copy.

Canonical StrToDateNative snapshots complete result fields, detached producer
ordinals and publication carrier. It reads two sources with Fresh requests,
reauthenticates the retained first source before work, then prepares owned
strings under the store borrow. That borrow ends BEFORE date-mode/warning
callbacks. Prepared strings drop before the original special temporal finishing:
Datetime/Date parse_computed_time with original kind/FSP, Duration
parse_computed_duration, then generic scalar-function return coercion and outer
ENUM_SET_AS_INT presentation. The exact former temporal match is extracted as a
value-only shared helper and still used by the original scalar-function family.
It contains no expression callback or alternate evaluation program.

Native output remains an authenticated value token. Existing typed parents can
receive the same computed bytes, DateTime or Duration carrier. Parent testing
found MONTH outside the eager source registry. The existing pure typed MONTH
kernel now has an exact DateTime→signedInt source fact. Its legacy native
eligibility remains unchanged, explicitly regression-checked; native CHAR's
legacy eligibility and CompiledSource closure stay separate.

The next actual failure was ResourceLimit/Invoke: host_copy_precharge treated
DateTime/Duration as usizeMAX, and compact host copies only implemented Int/Bytes.
The shared scalar/window host-copy code now measures temporal minimum storage,
copies actual values and checks actual retained capacities under unchanged
limits. Raw calendar/type/FSP fields and duration nanoseconds/FSP are preserved,
including NULL and scalar broadcast. Unsupported output-size sentinels no longer
reject these legitimate carriers. No budget increase masks the gap.

## Finite resources and effects

Native work checks demanded copies, Decimal visible/storage padding, f64/JSON/
vector formatting, two Vec<char> scanner capacities, warning/text buffers and
String-to-Arc publication overlap before the corresponding work. Empty scanning
has no fake frame charge; an empty-input warning can fit its actual exact text
budget. Actual buffer capacities are checked; warning buffers reserve fallibly.
The default64MiB is a conservative logical allowance, not whole-process OOM
recovery or a bound on arbitrary external diagnostic collectors.

Errors remain terminal: binding failures precede coercion/formatting, actual
resource causes retain ResourceLimit with phaseNone, and original early SQL
errors retain their value. No retry/replay/fallback. Repeated cancellation and
subsequent NULL/success reuse preserve task cleanup and output provenance.

NULL suppression applies to STR_TO_DATE preparation/parser demands. The original
computed-Duration result adapter still evaluates zone() BEFORE its generic NULL
guard; this required one timezone callback even for a NULL Duration result.
Direct tests first incorrectly expected zero total callbacks, then verified the
original native_cast_duration source and corrected ONLY that test expectation.
Production behavior and full source/reference effect comparisons stayed intact.
Every date-mode/warning/zone/clock callback in the direct provider obtains
store.borrow_mut(), proving no store borrow survives across it. A rejecting
Ready-cache callback also proves no retired program is consulted.

## Independent original evidence

At TiDBfec0abbf/SDK8c6e58a BEFORE production body changes, OLD844344 observations
fix hash d7dd9f9ee5b55431. They cover62inputs×63formats×9target identities×8mode
bits×3cache modes, plus648 malformed-arity observations. All32 Datum fixtures
occur on both sides, plus real date/clock/partial/Unicode/%j and invalid cases.
Exact Debug, identity bytes, LocalError cause/phase, date-mode/zone/warning events
are observed. No hash is regenerated from new code.

Agent workspace evidence/no-fallback/ preserves:

- n7b-original-date-capture.log68316 closed101: intentional hash0 capture;
- n7b-original-date-fixed-and-source-red.log8996: fixed witness PASS, five genuine
  text/Datetime/Date/Duration/NULL-with-late-error source REDs each match SQL/
  context then fail1refusal/fallback, actual rejecting-cache RED, and unchanged
  inherited partial-format failure;
- n7b-original-date-baseline.patch: fixed original test/registration only,
  separate-index cached apply-check atfec0abbf passed with SDK8c6e58a;
- original caller/SDK/test snapshots and n7b-old-vs-new-str_to_date_value_tests.rs;
- n7b-old-vs-date-values-actual.log54980 closed0: ALL844344 independently compare
  OLD active caller/wire against new actual SDK state APIs BEFORE body switches,
  then verify the immutable original hash. Earlier12888 only compiled the
  prototype plus OLD witness after an insertion-script error; it is explicitly
  NOT independent comparison evidence.

Only after actual independent proof were caller/wire bodies replaced. Post-switch
67074 fixed/cache checks2pass and33203 old-wire/new-value checks3pass. Original
fixed witness remains active and passes in the final complete frontend suite.

## Verification

- Original five source REDs green32619. New parent RED2707 identified MONTH
  admission;80928 exposed real temporal-copy resource refusal. Temporary cause
  diagnostic19756 reported retained evaluation storage exceeded. Diagnostics are
  removed. 18288 all11 parent/mode matrices pass after actual carrier support.
- 18412 all7 source/context/result checks pass with empty, ordinary,2050duplicate,
  late-invalid and genuinely fatal UTF8 error selections, four flag runs and
  eight dynamic mode seeds. Datetime/Date/Duration result FSP-1/0/3/6 and constant
  relaxed typed clock/MONTH0 checks pass. Exact NULL/raw output, partial prefixes,
  errors and complete context events match the original expression path.
- n7b-date-direct-providers-final.log26998 closed0: all8 source/provider checks
  pass. All4 publication carriers reject stale/wrong-role/producer/occurrence
  inputs, even after NULL; retained epoch failure beats huge target formatting.
  Huge Decimal/source/format/text work refuses before callbacks, while invalid
  UTF8/range sentinel retain earlier SQL errors. NULL suppresses huge format
  work and still reads the original Duration zone. Double cancel, repeated exact
  NULL/success reuse, callback exclusive borrow, no Ready lookup and native
  output provenance pass. Non-NULL zero-date clock fields/FSP and Duration nanos/
  FSP are checked directly. Initial direct tests had test-only Time accessor and
  temporary-slice lifetime compile mistakes, corrected before this final pass.
- n7b-front-full.log18723 closed101:1623pass, EXACT inherited4failures,
  95ignored/0filtered, all209 batch tests pass; failure list machine-matches N7a.
- n7b-sdk-full.log95169 closed101:1087pass, EXACT inherited6failures,
  1ignored/0filtered. New actual-state/resource tests and compact temporal host
  copy1/3row raw/FSP/NULL regression pass. Failure list machine-matches N7a.
- n7b-clippy-final.log89562 closed0:89warnings, exact title multiset matches N7a.
  Initial91 included only an unused test import appended after the test module;
  that import was removed. No SDK runtime body changed after the complete SDK
  test; final clippy compiled both library and all test code.
- n7b-lint.log88401 closed0; pinned fmt17 Rust files/both diff checks pass in
  n7b-final-static.log.
- n7b-projection.log54048 closed0: all25 Projection tests pass. Previous
  bounded31/8/9/5/5 groups remain intact;
  new4/5 groups cover dynamic IF text/NULL into string/date/Datetime/Duration,
  typed MONTH, concat and duration cast, both flags and workers0/2. No source
  metadata or resource allowance has been relaxed.

The four frontend/six SDK failures remain real pending work. In particular the
partial-format month-zero behavior is preserved here as original evidence,
without claiming it is correct against Go. This checkpoint is not global task
completion. All checkpoint gates are closed; signed local change only. Remaining
date semantics, source families and fallback removal continue under task4.
