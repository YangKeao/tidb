# N62: retain preparation failures without replay

This is a locally validated migration checkpoint, not overall completion.
The generic Projection admission-to-native retry is removed. Preparation errors
are retained once and returned on subsequent calls. The historical route counter
remains available, with its execution route and increment writer removed.

A remaining genuine context profile is explicitly open: an independent linked
public API probe shows `CHAR_LENGTH(?)`, with a declared Varchar parameter whose
value is UTF-8 `éx`, succeeds in native `EvaluatorSuite::run` but is still refused
by Projection preparation as Admission. The old contextual typed-root guard must
be closed with genuine native source profiles next. The current full unit suite
does not cover this combination. No server deployment, push or PR is authorized.

## Production and source contracts

Only `evaluator.rs` and `evaluator/batch.rs` change production Rust. The native
scheduler and every arithmetic, conversion and value kernel remain unchanged.
SDK source remains clean and byte-identical to `391243a9b45cbff43635f07b53ef75404f0a68aa`.
There are no increased limits, invented SQL declarations or weakened PB owner,
incoming schema, column or selected-row checks.

Selected PB identity determines diagnostic operands: a displayed arithmetic name
does not invent operands for another actual PB implementation. The obsolete PB
bulk guard is removed. Existing invalid owner/schema/column checks retain their
conditions and messages while classifying errors as terminal binding contracts.
Seven stale negative expectations change only Admission to Contract.

Genuine native String eligibility includes nested and deferred arguments.
Eligible ordinary Boolean integer results retain scalar argument demand and
atomic enabled-batch publication. Contextual native row collectors bind their
original cache before flags; already shared row profiles retain their earlier
query timing, and pure value/physical roots retain their no-Ready path.

## Independent original observations

The signed N61 pre-observer archive authenticates 624 files. All previously
captured original hashes remain fixed; no test is ignored and no golden is rebased.

| Domain | Records | Fixed digest |
| --- | ---: | --- |
| Legal Projection decoded PB owners | 127008 | C738E3D0F524444E |
| Legal Projection schema overlays | 432 | E60EF3ABFBDAA21B |
| Optional conditions and native row profiles | 388800 | ED53E168732558ED |
| Native Suite scope API, retained separately | 672 | 2B6BC730362FA53D |
| Actual pre-change Projection scope API | 672 | EA60639F6C88EF85 |

The first mutated-after-decode PB fixture violated the existing owner seal.
Its 52920/06658A0B1D129916 records remain historical invalid-owner evidence.
Broader native schema2940/406BABFF1C0C2EB9 and sealed PB158760/739DB1419FCC0361
also remain distinct because native coordinates and decoder histories have a
different contract from mandatory Projection bindings.

Native Suite scope initialization is not a valid universal oracle for the
already shared Projection API. Preserve native672 unchanged. Capture actual
Projection672 by temporarily restoring exact signed N61 production `evaluator.rs`
and `batch.rs`, running the real `run_with_tikv_batch` four times, authenticating
source hashes and byte-identical records, then restoring current production2
exactly by saved SHA. The restore-state receipt is inactive. The immutable
`batch_projection_scope_original.json` fixture enforces every original record.
Its collector body is unchanged; the capture-only file writer is retired.

## Reproduced failures and fixes

Legal PB comparison originally found 4704 differences,4944 refused records and240
fallback records. All are zero after selected-owner diagnostic/profile closure.
The unchanged 432 schema controls also retain zero differences/refusals/retries.
Invalid bindings terminate without SQL callbacks; repeated calls retain exactly
one preparation refusal and no output or replay.

The first complete frontend exposed nested String/Boolean row admissions and
seven stale binding assertions. Successive focused controls preserved all388800
original observations. An initially overbroad cache binding queried retired Ready
for a pure ANY_VALUE literal; its standalone RED and later GREEN are preserved.
The second frontend was explicitly canceled after this known regression and is
not reported as a completed baseline. An additional384 scope differences against
the actual pre-change Projection API were reproduced and eliminated by retaining
binding only for contextual native batch collectors. Both scope API hashes stay fixed.

## Final validation of this checkpoint

| Gate | Result |
| --- | --- |
| Complete frontend | 2354 passed; exact inherited3 failed;95 ignored;992.49s |
| Routing and original protocols | 10 passed |
| Optional matrix | 388800 strict observations; retained profiles0 |
| Existing admission inventory | 83088 cases;0 refused;0 names |
| Identity/no-Ready regression | passed |
| Existing binding negative controls | 7 passed |
| String resource/recovery/template controls | passed |
| Projection | 52 passed;105.72s |
| Expand | 6 passed; fresh Projection binary SHA recorded |
| Unistore | 195 passed; exact inherited3 failed;13 ignored;2.03s |
| Actual Session SHOW | 31 passed; exact inherited2 failed;0 ignored;0.07s |
| Production, make lint, both12-file formatter checks | passed |
| Source/original/restore/protected-file audit | passed |

The complete frontend's inherited failures remain IFNULL pushdown catalog,
EXP overflow and vectorized operator duration FSP. Unistore's inherited failures
remain the two old worker-counter assertions and strict DECIMAL selection.
SHOW retains the exact original auto-increment and warning Datum mismatches.
Warning counters match N61 exactly: frontend346 and normal expression library149.
The unchanged SDK reuses authenticated N60 full1153/exact inherited1/1ignore and
scoped clippy171total/89expression warnings. No new SDK validation claim is made.

Reproduce from the TiDB repository with the recorded wrapper environment:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-unistore --lib
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-session --test all show_ -- --test-threads=8
    make lint

Workspace evidence uses `n62-final3-*`, `n62-final4-fmt.json`,
`n62-original-audit.py` and `n62-final-check.py`. Older canceled/failed/setup and
cross-API fixture attempts remain explicitly recorded, not selected as final gates.
The independent remaining-gap proof is `n62-extra-context-probe.receipt.json`.

## Remaining work

Capture and close the remaining genuine contextual native profiles, beginning
with the confirmed CHAR_LENGTH parameter case; preserve real eligibility,
consumer phases, scope timing and publication in the same SDK frame. Then retire
actual public Unistore SimpleExpr/typed traversals, validate full local services
and measure final unified performance. Unit durations and the separate cgo
experiment do not certify final performance. Task #4 remains in progress.
