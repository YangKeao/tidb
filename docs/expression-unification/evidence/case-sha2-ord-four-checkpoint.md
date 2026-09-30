# Case conversion, SHA2 and ORD — case-sha2-ord-four-18

Functional delegation/native-deletion progress: 55/245; final acceptance: 0/245. LOWER, UPPER, SHA2 and ORD are four complete functional families on their existing admitted surfaces, covered by six runtime variants. The source cut covers ten TiDB and nine TiKV files, not completion of the overall unification target.

## Changes and domain coverage

Binary LOWER/UPPER perform real wire no-op calls. The two UTF8 ClosedPrivate variants bind the existing official EncodingUtf8Mb4 getter: the empty-charset descriptor cannot use the wire UTF8 selector, and its zero-heap representation is not changed. Native preparation retains Go's per-malformed-byte U+FFFD normalization, not a case algorithm or copied tables. Aliases and existing binary/UTF8 PB and NULL behavior remain.

SHA2 has one TiKV selector/hex core. Invalid wire lengths produce NULL plus warning1583 once; the private native path produces silent NULL without clearing warnings. The prior count enum becomes ReadyIntArg; BytesIntReady has its own role. NULL-left Undemanded is checked against the operation and NULL input before becoming an irrelevant physical Some(0); an actual NULL length remains Value(None).

ORD keeps only original argument-charset first-character preparation in native code, at most four bytes; TiKV owns the sole numeric fold. All three entry guards reject larger prepared payloads before dispatch. Wire decoding and NULL-to-zero remain, while native NULL remains None; the typed special entry and ETString preparation order are preserved. On a latin1-typed column holding UTF8 bytes for 'é', the existing byte-preserving alias makes ORD return195, not233; this cut does not silently repair that policy.

SQL coverage checks five rows with twelve result columns, aliases, binary cases and a raw-storage precheck; SHA256 expectations use fixed public vectors, and invalid selector123 remains warning-free. Eight direct zero-slot refusals are covered. Pre-existing expected values are not changed.

## Actual validation

Parent ran the pinned January TiKV and August TiDB wrappers. [Commands and exact result lines](../logs/case-sha2-ord-four-summary.txt) retain all seven receipts, including the compile-only failure.

- TiKV `local::`: 211 passed/1 ignored/469 filtered.
- TiKV official string tests: 63 passed/618 filtered.
- TiKV official encryption tests: 8 passed/673 filtered.
- Initial TiDB dispatch attempt: compile exit101, three E0433 errors, no tests run.
- TiDB dispatch retry: 3 passed/1498 filtered.
- Session lifecycle/SQL filter: 39 passed/2078 filtered.
- Full expression suite: 1403 passed/4 existing failures/94 ignored; 1501 discovered, exit101. Parent compared all four complete failure blocks against packet-string-four-17: identical after only thread-ID normalization. The full suite remains non-green.

## New test-path correction

The initial chain stopped at dispatch compilation and did not continue. Three new-test references incorrectly named `crate::Expression`; parent changed only those paths to `crate::expression::Expression`. The retry, SQL and full-expression receipts are subsequent runs, not tests executed by the failed initial attempt. This repair did not change runtime behavior or old expected values.

## Review and not verified

Parent reviewed the shared cores, native algorithm deletions and PB gates. No SHA2/ORD PB or unistore admission was added; unistore was not rerun in18. Driver, pool and limits were not changed, and no case/charset tables were copied. Complete business-wrapper operation scopes/guards, allocator remeasurement, physical peak/OOM guarantees, release performance, full-workspace validation, make lint and final acceptance remain open. These receipts do not establish PR readiness.
