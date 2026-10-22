# CAST and UNION prefixes share their original fresh value phases

Unary source-specific UNION calls and unknown ordinary CAST targets now use a
small consuming demand state in the existing EagerValuesNative SDK frame.
There is no new driver, SDK operation, expression callback or Ready expression
worker. The original scalar UNION and non-JSON CAST value boundaries are shared
as pure helpers. Ordinary target recognition is moved unchanged into the same
module. Original common-body computation, common eligibility, argument policies,
other scalar branches, caches and SDK are unchanged.

The first UNION value can return NULL or a source-specific result. An unresolved
UNION branch requests the same live argument again with Fresh mode; the original
CAST branch or common eager tail consumes that second value. It never reuses the
first value as a replacement for the second evaluation. The original private
UNION dispatcher retains its intrinsic absent-source contract, including unsigned
negative-decimal behavior. An earlier native ordinary CAST path had introduced
an extra1292 warning and changed a sentinel panic; preceding comparisons expose
both differences and the shared original union entry now preserves them.

The same change carries actual optional source/result fields in the existing
DateArithmetic host. Original date unit/scale facts, eager operand demands and
bounded date value computation remain intact. Value output can preserve absent
fields; typed output still requires real source and result declarations. No
SQL field is fabricated from the caller schema or actual Datum.

Independent original capture, rerun and source failures preceded production.
All3669080 strict observations retain their original counts and digests:

| Witness | Observations | Original digest |
| --- | ---: | --- |
| Unknown prefixes, changed live reads, effects and late failures | 288000 | 0F79E3203A9C6199 |
| Initial prefixes, eight declared domains and149 actual values | 500640 | 34F5F154574439E5 |
| Seven real UNION aliases and other unknown spellings, phases | 691200 | 5AD410F03E154979 |
| Extended aliases, domains and all actual parameter positions | 1001280 | 0883DF2991A572B5 |
| Five valid date units, optional metadata and phases | 288000 | A62BA14E9B59AC03 |
| Valid date units, eight domains and actual values | 715200 | DC98E2D526F3CF37 |
| Raw Datum frames, literal positions and actual declarations | 184760 | D58D2A1C0604554C |

Every actual path passes without fallback; no original observation was removed
or rebaselined and no resource exception was added. The matrices preserve exact
outcomes/panics, raw kinds, column bytes/prefixes, warnings, variable writes,
parameter demand counts, both flags, ordinary/duplicate/empty selections and
same-owner/context reuse. Changed-read profiles distinguish NULL on the first
read from NULL on later reads. Date conversion time is explicit in the existing
dev-only clock scope; the production clock remains unchanged.

New direct protocol checks reject stale, wrong-role/node/row and cancelled
capabilities, enforce the actual Fresh second request and reject completed-task
replies. They refuse a large value at8KiB and recover using the same descriptor
and limit. Warning callbacks acquire the value arena, invalidate its epoch or
panic; the host authenticates again after callbacks and releases work scope on
unwind. Their context panics on any Ready worker or expression lookup. A separate
five-family check refuses32KiB under16KiB and recovers with NULL on the same
owner, context and cap. Another300 cases span0/3/2051 selected rows, late panic,
duplicate physical rows and four flag rounds. Typed date claims with absent
fields are rejected while genuine optional value claims remain allowed.

Real Projection covers13 prefix/date names, declared/absent fields, five actual
value pairs, real prepared parameters and deferred variable writes, query/strict
contexts, both flags,0/3/2051 rows and serial/parallel workers. For erased result
metadata, the caller supplies a physically valid column for the original raw
result: calendar text uses a string column, while passthrough UNION uses its
actual sample kind. The expression retains its missing declaration. Comparison
copies raw column ranges; decoding and re-encoding through a mismatched declared
slot would change the very data being checked. Neither correction changes
production value computation or an original recorded witness.

The first full run exposed an eligibility-layer mistake: including the new
prefix protocol in common-body supports expanded an old dynamic witness from
1713 to1716 profiles. The source now registers prefix capability separately;
common supports and prefers_common_body remain byte-equivalent to the baseline.
The original1713/657792/A94C463994D2D98D witness passes unchanged. No old selector,
expected count, digest, fixture or assertion was modified.

Final validation:17 new checks pass. Frontend2087 passed, exactly the inherited
three failures,95 ignored; all546 batch checks pass and warnings remain346.
Projection39, Expand6 with64/B42FB8BE316E6C79, root lint, both supported rustfmt
versions on ten changed Rust files and ownership reconstruction pass. The
checker reconstructs original scalar_function.rs and the date host body,
verifies unchanged eager computation/classification, original policies/observers
and clean SDKad73b11. Its N16 full/clippy baseline remains applicable. The fixed
83088-case inventory now reports344 refusals across8 names, a bounded fixture
count rather than global completion coverage.

From the TiDB root, with RUSTFLAGS='' and CARGO_BUILD_JOBS=8:

    ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib optional_reserved_prefix_ -- --nocapture
    ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
    ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
    make lint

Final dedicated sources, old common witness, inventory and Expand were rerun
from the newly compiled binaries. Expand used --nocapture --test-threads=1.
The owner's evidence/no-fallback/n36-* retains executed source snapshots,
independent captures/reruns, preceding failures, final logs and the checker.

Remaining regular-expression/WEIGHT_STRING/whole-parameter profiles, public
scalar/AST/filter/unistore entries, generic fallback removal, full-service
correctness and final performance remain open. No push or release.
