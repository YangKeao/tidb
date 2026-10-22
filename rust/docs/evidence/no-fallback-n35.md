# Membership, interval and extrema retain optional declarations

IN, INTERVAL, GREATEST and LEAST now carry genuine optional source/result
metadata in their existing native hosts. No replacement SQL type, expression,
value computation, cache implementation or driver is introduced. Scalar code,
argument policies, original value adapters and the SDK are unchanged.

IN selects its original typed barrier only from an actual declared first
argument; otherwise it keeps exhaustive generic comparisons and the existing
prepared string cache. Typed casts receive the real optional field for each
list item. Operand facts retain absent eval type and unsigned=false. INTERVAL's
existing SDK search receives optional field metadata and keeps the original
probe order. Extrema retain gl_signature(None) and UNSPECIFIED_LENGTH for an
undeclared argument's scale, exactly as the original owner does. Each family
uses optional result coercion and the enclosing expression's original view.
Metadata accounting includes only fields that are actually present.

Independent capture, rerun and admission failures preceded production changes.
All original observations and their expected digests remain fixed:

| Witness | Observations | Original digest |
| --- | ---: | --- |
| Optional declarations, lazy/eager demands, effects and late failures | 138240 | 6D23E22B6F5BBE49 |
| Eight declared domains, arities2/4, all149 values in either parameter | 1144320 | 0926C1DF1FCFFBE1 |
| Direct actual Datum frames and real prepared IN cache | 29800 | 0BBCF87024DEBD9F |

The phase witness covers all declaration masks, both flags, three output
families, ordinary/duplicate/empty selections, deferred writes, failing children,
physical columns, nested controls and same-context reuse. The expanded witness
adds signed/unsigned integers, string, real, decimal, datetime, duration and JSON
source/result declarations. Original scalar owners remain independent of the
new source and retain their exact values, errors/panics, prefixes and effects.
The direct protocol uses the actual promoted source and constant-prefix probes,
matching the real suite; omitting those probes in an initial test harness caused
an invalid comparison and was corrected without changing original records.

All138240 phase comparisons pass without fallback. The expanded witness has
1141600 strict comparisons and2720 explicit terminal resource refusals; the raw
witness has29721 strict comparisons and79 resource refusals. These2799 cases
all contain the exact malformed Decimal sample with huge scale declarations.
The original1312360 observations remain in their fixed digests; refusals are
not counted as successful native execution or value equivalence. Existing work
limits remain unchanged, and an all-declarations-present source already exposed
these finite refusals before the optional-metadata implementation. The tests
check the actual resource class, no interpreter fallback and unchanged output
prefix, rather than suppressing a discrepancy as a successful value result.

A separate four-family check rejects a32KiB actual parameter under the existing
16KiB retained limit, then succeeds with NULL using the same owner/context and
limit after switching the batch flag. Another240 cases use0/3/2051 selected rows,
late child failure, duplicate physical rows and four alternating flag rounds,
then recover using the same cached source and context. Real Projection covers
arities2/4, declaration masks, genuine variable writes, query/strict modes,
0/3/2051 rows, both flags and serial/parallel workers.

Final validation: all eight new checks pass. Frontend2070 passed with exactly
the inherited three failures,95 ignored; all529 batch checks pass and warnings
remain346. Projection38, Expand6 with64/B42FB8BE316E6C79, root lint, both supported
rustfmt versions on eight changed Rust files and the ownership checker pass.
The checker reconstructs the three original family host modules and verifies
unchanged original scalar/value computation, classifications, old observers and
SDKad73b11. Its N16 full/clippy baseline remains applicable. The fixed83088-case
inventory reports512 refusals across13 names; this is a local fixture count.

From the TiDB root, with RUSTFLAGS='' and CARGO_BUILD_JOBS=8:

    ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib optional_membership_ -- --nocapture
    ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
    ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
    make lint

Final dedicated source checks ran from the newly compiled frontend binary;
Expand used the new executor binary with --nocapture --test-threads=1. The
owner's evidence/no-fallback/n35-* retains source snapshots, captures/reruns,
admission failures, resource counts, final logs and the reconstruction checker.

Remaining aliases/regular expressions/WEIGHT_STRING/whole-parameter profiles,
public scalar/AST/filter/unistore entries, generic fallback removal, full-service
correctness and final performance remain open. No push or release.
