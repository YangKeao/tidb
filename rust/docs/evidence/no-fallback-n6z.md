# N6z: actual charset bytes and canonical source composition

The three native charset callers and actual-NULL helper no longer build Ready
programs, identity frames or report decoders. Borrowed SDK byte services compute
actual ToBinary/FromBinary/ConvertUsing results; canonical sources use the same
SDK frame, with original demand and context phases. Other families/source closure
and global fallback removal remain incomplete. No push/PR/deployment.

## Actual behavior and resource boundaries

Exact source spelling and effective canonical field charset are distinct inputs.
Exact unknown/alias/case variants use original binary lookup fallback, while
FieldType canonicalization retains aliases and collation fallback. Supported
ConvertUsing targets are seven exact lower-case names. Binary source decode
errors compute NULL; strict To/From errors retain their original SQL error.
Typed ETString reader None computes empty bytes; only actual source NULL skips
later source demands and uses the distinct SDK NULL primitive.

Original direct API unknown-target validation precedes the ETString reader.
Canonical source order is source, actual-NULL decision, target coercion, source
value cast, unconditional connection-charset getter, metadata selection, codec,
then late default-collation publication. Target NULL means an empty unknown target,
not source NULL. Source ETString still reads the connection getter even when the
metadata selector ignores it. Explicit coercibility, complete source/result fields
and producer ordinals are detached factory snapshots. NativeEligibility remains
separate from CompiledSource closure.

Borrowed readers preserve the exact original accepted variants without invented
casts. Binary/Latin1 copies precharge length and check actual allocated capacity;
GB codec work precharges checked8*n, and every transform checks output capacity.
The frontend bounds target and actual source copies/formatting, demanded Decimal
visible padding, chosen metadata clones, string retag overlap and final publication.
Shared immutable String/Enum/Set owners incur no fabricated source-payload copy.
Raw values do have actual coercion copies and are charged. Skipped target/source
work is not charged. Default64MiB is a finite conservative logical allowance,
not a whole-process OOM recovery guarantee.

Capabilities authenticate role, producer, occurrence and epoch; retained first
source is reauthenticated BEFORE target work. Store borrows end before connection
callbacks and publication. Source SQL errors, host resource refusal, and actual
codec resource causes remain distinct terminal errors; no retry/replay/fallback.
Original result coercion and outer ENUM_SET_AS_INT presentation remain intact.

Parent testing found a real missing typed outlet under CONCAT: admission refused
and fell back despite root charset support. CharsetNative now always consumes
native source capabilities and can publish its SAME computed result as ETString
for an existing typed string parent. CONCAT keeps its original demand phases;
no extra cast, Expr callback or evaluation program is added. Native output parents
continue receiving authenticated value tokens.

## Independent original proof

At TiDB6e005bde/SDK1e3efd8 BEFORE production changes, fixed OLD observations:

- 4200 binary reader/name/cache cases, hash d7142611e2de991d;
- 88200 exact/effective/target/reader/cache cases, hash e3a22bd8fcc8c70f;
- 1604 isolated live GB default/source NULL cases, hash7d8948d00184e03d.

All94004 inputs independently matched new byte APIs BEFORE either caller or wire
replacement (n6z-old-vs-charset-values.log,36578 closed0). Observations include
exact Debug/identity bytes, LocalError cause/phase and context effects. Every
cohort executes in an isolated single-test child to avoid global-mode races.
Fixed hashes are NEVER regenerated from new implementations.

n6z-original-charset-baseline.patch is a test-only reproduction against6e005bde
with SDK1e3efd8; separate-index cached apply-check passed. Original production,
fixed-test, source-RED and independent comparison snapshots remain in agent
workspace evidence/no-fallback/n6z-*. Initial capture97573 closed101 intentionally
used zero hashes; fixed50828 reproduced six genuine SQL/context-matching source
refusal1/fallback1 failures and three actual rejecting-Ready-cache failures.
28246 post-retirement fixed/cache checks all6 pass;29406 all6source REDs green.
Parent59819/17026 exposed the new CONCAT gap;90114 proved the actual typed bridge.

Explicit ordinary wire profiles retain original complete validation, nullable
report protocol, cache policy and checked8*n+1 retained bound. They delegate only
actual byte computation to the shared services. No new native PB/wire admission.

## Verification

Artifacts in agent workspace evidence/no-fallback/:

- n6z-charset-providers.log:95939 closed0, all11 source tests pass. Root/CONCAT/
  IFNULL/outer-binary parents, dynamic getter ordering, four flag runs, empty/
  normal/2050duplicate/late-error selections, exact bytes/NULL/partial output and
  zero refusal/fallback. Six exact source-name variants and target NULL retained.
- Direct providers: all3 operations reject stale/role/node/occurrence capabilities;
  retained source expiry beats enormous target formatting. Huge target/source
  Decimal work is refused before formatting/getters; target sentinel SQL refusal
  precedes huge source work. Huge shared String plus unknown target retains the
  original SQL error after the getter, without invented copy refusal. Codec
  resource cause retains phaseNone. Double cancel and repeated NULL/success reuse
  preserve exact output provenance.
- Isolated late-GB provider callback obtains store.borrow_mut(), proving no shared
  borrow crosses it, flips global mode false→true, and output gets the current GB
  default after conversion. Actual NULL skips callback/target entirely.
- n6z-front-full.log:79252 closed101,1606pass, EXACT inherited4failures,
  95ignored/0filtered; all196 batch tests pass, fixed OLD witnesses included.
- n6z-sdk-full.log:1082pass, EXACT inherited6failures,1ignored/0filtered.
  Three new SDK tests cover actual outcomes, finite/zero/skipped copy bounds and
  actual source-NULL demand. Failure lists machine-compared to N6y, identical.
- n6z-clippy.log:25520 closed0,89 warnings unchanged. n6z-lint.log:21878 closed0.
- n6z-final-static.log: pinned rustfmt14 Rust files and both git diff --check pass.
- n6z-projection-fixed.log:77779 closed0, all25 Projection tests pass. Four
  bounded heterogeneous groups preserve previous31/8/9 matrices and add5
  arithmetic/charset/CONCAT outputs, both flags and workers0/2. No metadata budget
  was relaxed. Initial44107 closed101 failed in the reference path because the
  new test used unsupported cast_string; corrected TEST ONLY to existing
  cast_char. No production change after full frontend/SDK validation.

Original four frontend/six SDK failures remain real pending migration/audit work;
this checkpoint does not claim a globally green SQL server or completed goal.
