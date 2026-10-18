# N7c: actual TIMESTAMPDIFF values and canonical source demand

Base: TiDB996b942d / TiKV611b076, branch expression-unification-demo.
Validated SDK companion signed locally as TiKV8e3bf36; no push.
This is a local migration checkpoint under task4, not global completion or a Go
package transcreation claim. No push, PR, deployment or resource-limit increase.

## Behavior and ownership

Native text, raw-core and observed-NULL callers no longer prepare Ready argument
programs. SDK `native_timestamp_diff_value.rs` consumes actual native values;
`time/native_timestamp_diff.rs` owns opaque parsed endpoints, an actual computed
signed delta, late unit folding and the original calendar arithmetic. Text keeps
strict wide-year civil days and case-folded units after delta computation. Core
keeps optional complete raw bits, full-raw-zero suppression, exact bytes and
unknown-unit zero before arithmetic. The legacy caller no longer packs LE64.
Ordinary wire validators, optional8LE decoding, metadata and invalid-time policy
retain their original owner and share only actual arithmetic.

The same source frame now authenticates three retained native capabilities,
including epoch/role/producer/occurrence, before conversion or further demand.
Generic ScalarFunction first observes ALL sources, detaches bounded ownership,
then applies the ORIGINAL left/right datetime argument casts using complete
FieldTypes; it does so even after an actual NULL unit. Store borrows are released
before mode, zone, clock or warning callbacks. Exact PB TimestampDiff instead
stops at its first actual NULL without coercing earlier values and skips the
generic cast layer. Original result coercion, unsigned PB bit interpretation,
ENUM/SET view and typed/native output ownership are preserved.

PB origin admission is narrowly scoped to actual TimestampDiff and descendants.
Current and original effective FieldTypes are separately byte-gated before
provenance equality. Other PB function signatures still refuse; this does not
claim complete PB source coverage. Original ordinary native eligibility remains
separate from the larger compiled source closure.

The preceding STR_TO_DATE string work gate is extracted into shared bounded
coercion, preserving its original resource diagnostics. Demanded source copies,
Decimal visible/storage padding, float formatting intermediates, parser scratch
and late unit folding are bounded before their operations. The calendar cast's
work classifier lives beside the actual SDK cast and shares `year_source_value`.
It distinguishes UInt outside i64, which needs formatting, from actual YEAR
construction/errors and Time/NULL passthrough, which need no text allocation.
These are conservative logical work/retention allowances, not a whole-process
allocator peak guarantee.

## Original evidence and regressions

At the original base,392556 records produced immutable hash b4faecb69db19986:
48units x52left x52right x3cache profiles =389376 text records;13units x9cores
x9cores x3 =3159core records;21 arity/NULL-witness boundaries. Representations,
identity bytes, native error/cause/phase and context observations are captured.
The test-only `n7c-original-timestamp-diff-baseline.patch` cached-applies to996b942d;
original caller, arithmetic and tests are retained in workspace evidence.

Before caller/wire replacement, `n7c-independent-diff.log`70960 compared ALL392535
text/core pairs against new actual SDK APIs and verified the immutable whole
hash. The21 boundary records were protected by that original hash, not separate
prototype comparisons. Datatype arithmetic extraction retained the original
body and the fixed hash. Three genuine source regressions and a Ready-lookup
trap were still RED at this point. No hash is regenerated from the new code.

First source integration18021 made PB-prefix NULL and the Ready trap green but
exposed missing generic endpoint casts through context-event equality. Those
casts now share the existing plain SDK conversion owner.95925 then passed all11
targeted checks. Expanded tests cover true decoded PB constants/columns, unsigned
negative results, stale original metadata and unrelated PB-signature refusal;
six typed parents, exact output bytes/NULL/partial prefixes,2050duplicate
selections, repeated flag toggles, fatal UTF8 errors, dynamic eight-mode/zone
sequences, YEAR metadata, Duration missing-clock and actual statement-clock
conversion, all four native/typed and eager/PB provider combinations, invalid
capabilities before huge work, retained epoch invalidation, terminal native
resource class/phaseNone, double cancel and success reuse. Context callbacks
borrow the source store mutably to prove no immutable borrow survives.

Self-review found the UIntMAX YEAR work gap. The real zero-budget regression
`n7c-year-resource-red.log`48257 failed BEFORE its fix by entering date_modes.
The correction shares the actual SDK year route. The final regression requires
resource refusal before callbacks for UIntMAX, but zero-budget success for
actual YEAR construction and the original unsupported error for negative/i64MAX
YEAR values. No error API visibility was widened.

## Validation

The final frontend `n7c-front-full-final.log`95104 closed101:1634pass, EXACT
inherited4failures,95ignored/0filtered; all217 batch tests and the real YEAR
resource regression pass. Its failure list machine-matches N7b.

Final SDK `n7c-sdk-full-checked.log`98646 closed101:1090pass, EXACT inherited6,
1ignored/0filtered. The new calendar-work classifier test is explicitly present
and PASS. The initial final run9495 had1089pass because that new test was
accidentally nested inside another test function; clippy11044 caught both
cannot-test-inner-items/dead-code warnings. Only test placement was corrected
after the final frontend and Projection runs; no production body changed.

Final Projection `n7c-projection-final.log`86279 closed0: all25pass. Previous
bounded31/8/9/5/5/4/5 groups remain intact; the new4-output group mixes existing
arithmetic with IF text/NULL into TIMESTAMPDIFF, its arithmetic parent and CHAR
cast. Both flags and workers0/2 pass under unchanged limits.

Final clippy `n7c-clippy-checked.log`93873 closed0:89warnings, exact diagnostic-title
multiset machine-matches N7b after the test-placement correction. It compiles the
final library and tests. Final lint13889 closed0; pinned format19 Rust files and
both diff checks pass in `n7c-final-static.log`.

Datatype `n7c-datatype-full.log`88482 closed101:564pass and EXACT inherited2,
0filtered. No datatype runtime code changed afterward, only pinned formatting.
Earlier SDK32044, frontend46659 and Projection50168 results preceded the final
YEAR correction and are retained as intermediate evidence, not final claims.
All checkpoint jobs are closed. No ignored test or resource limit was added.

Commands, from TiDB root unless otherwise stated:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
    make lint

From TiKV root:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tikv test --release --locked --offline -j8 -p tidb_query_expr --lib
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tikv test --release --locked --offline -j8 -p tidb_query_datatype --lib
    RUSTFLAGS='' TIKV_FRAME_POINTER=0 make run COMMAND='bash /home/agent/.slock/agents/1555103c-9f3e-46ca-ac2e-b964c650c2f1/evidence/no-fallback/n3-clippy-scoped.sh --lib --tests --release --locked --offline -j8'

Complete shared-library/server/SQL cohorts and remaining date/source families
remain open under the global goal. The four frontend, six SDK and two datatype
failures remain real work, including partial STR_TO_DATE behavior; they are not
ignored, excused or reclassified by this checkpoint.
