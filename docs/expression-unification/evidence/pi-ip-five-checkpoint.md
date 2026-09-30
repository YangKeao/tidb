# PI and four IP predicates — pi-ip-five-16

Functional delegation/native-deletion progress: 47/245; final acceptance: 0/245. PI, IS_IPV4, IS_IPV6, IS_IPV4_COMPAT and IS_IPV4_MAPPED are five complete functional families on their existing admitted surfaces. The source cut covers eight TiDB and nine TiKV files, not completion of the overall unification target.

## Changes and domain coverage

PI uses an independent NoArgs role: empty schema, no input column, one function call with args0 and FrameRows1, without a dummy operand. The original `pi()` is the sole constant source. The public PI wrapper remains minimal and no-argument; existing AST/typed/PB and legacy routes delegate without expanding other admission. PrivateRawMath becomes ClosedPrivate, but NoArgs, IEEE754 and value roles remain distinct. Driver, pool, limits and NotNan are not expanded.

Four private nullable wrappers preserve native NULL and directly call the official algorithms for non-NULL inputs; the existing wire NULL-to-zero behavior remains unchanged. IPv4 preparation only removes redundant leading zeros within each original dot-separated segment: it neither validates ranges nor computes an answer, and leaves empty segments, dots and other characters unchanged. The old native parser, redundant IPv6 guards/parser and mapped/compat checks are deleted; `coerce_str?`, raw conversion and metadata remain.

The legacy value fixture adds exact PI-bit checks, and the existing infrastructure regression adds PI cross-type refusals. Existing expected values are not rewritten. SQL covers seven rows times four predicates (28 cells), PI value/metadata and eight direct zero-slot refusals for the predicates. PI may legally fold: its SQL checks do not claim a runtime kernel call or require zero-slot rejection. Helper/core/legacy tests exercise real PI dispatch separately.

## Actual validation

Parent ran the pinned January TiKV and August TiDB wrappers. [Commands and exact result lines](../logs/pi-ip-five-summary.txt) retain the eight current receipts.

- TiKV `local::`: 203 passed/1 ignored/469 filtered.
- TiKV raw role/length/kernel-drift guard: 1 passed/672 filtered.
- TiDB IP predicate dispatch: 1 passed/1494 filtered.
- TiDB PI dispatch: 1 passed/1494 filtered.
- Session lifecycle/SQL filter: 35 passed/2078 filtered.
- Unistore legacy inverse-trig filter, including PI infrastructure coverage: 2 passed/185 filtered.
- Unistore binary64 math fixture: 1 passed/186 filtered.
- Full expression suite: 1397 passed/4 existing failures/94 ignored; 1495 discovered, exit101. Parent compared all four complete failure blocks against raw-math-six-15: identical after only thread-ID normalization. The full suite remains non-green.

Full unistore was not rerun in16. The 173 passed/1 failed/13 ignored result and single-file HEAD reproduction belong to [checkpoint15](raw-math-six-checkpoint.md), not a fresh full-suite validation of this cut.

## Review and not verified

Parent reviewed the compare2/math/unistore delegation and deletion changes. Complete business-wrapper operation scopes/guards, allocator remeasurement, physical peak/OOM guarantees, release performance, full-workspace validation, make lint and final acceptance remain open. These targeted receipts do not establish PR readiness.
