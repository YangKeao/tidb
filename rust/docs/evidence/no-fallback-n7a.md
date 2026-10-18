# N7a: actual JSON_SUM_CRC32 documents and canonical sources

The native JSON_SUM_CRC32 caller no longer serializes an already-parsed document
into a separate Ready program, reparses it, or decodes a framed report. A shared
SDK actual-document reducer borrows string members, formats demanded numbers,
and computes CRC accumulation. Original ordinary document admission/parsing is
already an SDK service and is now called directly by the bounded value API.
Other families/source closure and global fallback removal remain incomplete.
No push/PR/deployment.

## Behavior and resources

SQL NULL is distinct from parsed JSON null. Root documents must be arrays;
ordered iteration preserves the first non-scalar or homogeneous-domain error.
Strings are borrowed, including arbitrary decoded JSON string bytes. Integer,
u64 and finite-f64 numbers retain original Go-g spelling and IEEE CRC; totals
use wrapping i64 addition. The fixed checksum/result enum is an actual value,
not a host-computed verdict, serialized header, checksum input or partial sum.

Native parsing/display work has finite logical precharges. Invalid UTF-8 is
checked before parser allocation; unsupported Datum kinds retain their original
admission error before any copy. Decimal visible/storage scale is bounded before
display. A parsed document's string/empty/first-error cases need no allocation
or retired report tax; numeric formatting is checked only when actually reached,
using its finite formatting-work bound and actual formatted capacity.
Integers precharge128; float conversion precharges4096 for the original fixed
Display intermediate (up to327characters), second digits String and final Go-g
output. Merely bounding the short final spelling would miss actual temporaries. Later
members after a semantic error are not formatted. Default64MiB and parser work
bounds are conservative logical allowances, not whole-process OOM recovery.

The ordinary wire worker retains its existing transport admission, serde decoder,
NULL0/9byte report bounds and full protocol checks. Only its actual document
reducer is shared, under the original ordinary owner without imposing a new
native-value allowance. The native caller does not use this wire path.

Canonical JsonSumCrc32Native consumes an actual authenticated source capability
in the same SDK frame. Producer/role/row occurrence/epoch validation precedes
parsing or formatting. Complete result declarations and producer ordinals are
detached factory metadata. Store borrows end before publication. Native output
is an authenticated value token; an existing typed Int parent receives the same
computed Int result, with the catalog recording the output carrier. Neither
parent gains a new evaluator or cast. Original result coercion and outer
ENUM_SET_AS_INT view remain. NativeEligibility admission is separate from
CompiledSource closure. Errors remain terminal without retry/replay/fallback.

## Original independent evidence

At TiDB6263a88b/SDK93d7375 BEFORE production replacement, OLD10926 observations
fix hash e762b2ff1936744f. Three cache modes (none/reused/fresh), two replays,
32 full-kind fixtures and all8-atom arrays of lengths0..3 include strings,
numbers, NULL, booleans and nested arrays/objects. Extra non-array/numeric edge
texts, invalid text/UTF8, and String/Bytes/actual BinaryJSON representations
exercise number spelling, first errors, SQL NULL and JSON null.

n7a-original-jsonsum-capture-fixed.log15907 closed101 intentionally used hash0
and exposed three genuine source REDs: text/JSON/numeric each matched SQL/context
then refused/fell back once. An actual rejecting Ready-cache callback also failed.
Initial16873 was a test-only &String constructor type error, corrected to &str.
The immutable witness fixes10926/e762b2ff1936744f; never regenerate from new code.

n7a-old-vs-jsonsum-values-fixed.log66501 closed0: ALL10926 independently matched
actual new SDK input/value services while BOTH OLD caller and wire implementation
were still unchanged, then verified the fixed OLD digest. Exact Debug, identity
bytes, LocalError cause/phase and context effects were compared. First58323 only
had a frontend LocalError import-path compile mistake, corrected before this proof.
Only after that independent pass were caller/report/serialization code removed
and the ordinary wire reducer delegated. SDK targeted89445 closed0 all3 tests.

Agent workspace evidence/no-fallback/ retains original production/tests and
independent comparison snapshots. n7a-original-jsonsum-baseline.patch contains
the fixed original test and registration only; separate-index cached apply-check
at6263a88b passed, SDK93d7375. Source-RED tests remain active, not excluded.

## Verification

Artifacts in agent workspace evidence/no-fallback/:

- n7a-jsonsum-source-and-values-fixed.log17707 closed0, all9 checks pass.
  Three original source REDs and actual rejecting-cache trap are green; the
  immutable OLD digest passes. Parent root/arithmetic/IFNULL checks compare
  exact values, partial output and context order under four flag runs with
  empty/ordinary/2050duplicate/late-error selections and zero refusal/fallback.
- Direct providers test both native-token and typed-Int output catalogs. Stale,
  wrong-role/producer/occurrence capabilities beat huge Decimal formatting;
  huge Decimal and valid huge text refuse work before parsing/display. Invalid
  UTF8 and unsupported huge Raw retain their earlier SQL admission errors.
  Actual resource causes preserve phaseNone; double cancellation and repeated
  NULL/success reuse retain exact output provenance and no context callbacks.
  Initial26769 was a test-only missing reference in ScalarValueRef::Int,
  corrected before these checks; no runtime change was needed.
- n7a-front-full.log21498 closed101:1613pass, EXACT inherited4failures,
  95ignored/0filtered; all201 batch tests pass. Failure list machine-matches N6z.
- n7a-lint.log80410 closed0. n7a-final-static.log: pinned fmt15 Rust files and
  both git diff --check pass.
- Initial SDK full95528:1084pass EXACT inherited6/1ignored/0filtered;
  Projection52542 closed0 all25pass. Final review found that float Display
  intermediates exceed a128 final-text allowance. Before signing, corrected
  demanded float work to4096 and added MAX/minimum subnormal positive/negative
  probes: actual Display length exceeds128, allowance128 refuses,4096 succeeds.
  Integers retain128. This changes only the finite work boundary, not codec or
  checksum semantics; no OLD digest was changed.
- FINAL n7a-front-full-float-bound.log40383 closed101:1613pass EXACT inherited4,
  95ignored/0filtered/all201 batch; n7a-sdk-full-float-bound.log13443 closed101:
  1084pass EXACT inherited6/1ignored/0filtered. Failure lists machine-match N6z.
- FINAL n7a-projection-float-bound.log49375 closed0 all25pass. Preserves prior
  31/8/9/5 bounded mixed groups and adds5 outputs with dynamic IF-generated JSON
  text/NULL, checksum/arithmetic/IFNULL/cast_char, both flags and workers0/2.
  No metadata/resource allowance was relaxed.
- n7a-clippy.log35853 closed0:89warnings, exact diagnostic-title multiset
  machine-matches N6z with no additions/removals. Final pinned fmt15 Rust files
  and both git diff --check pass (n7a-final-static-float-bound.log). The sole
  frontend change after initial full checks was an explanatory carrier comment;
  final full checks and Projection include the final source and corrected SDK.

The four frontend/six SDK inherited failures remain pending work and are not
ignored, reclassified or claimed globally green. All checkpoint gates are closed; signed local checkpoint only, global goal
remains active.
