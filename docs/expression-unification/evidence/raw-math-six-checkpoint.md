# Six raw-math families — raw-math-six-15

Functional delegation/native-deletion progress: 42/245; final acceptance: 0/245. ASIN, ACOS, SQRT, SIGN, RADIANS and DEGREES are six complete functional families on their existing admitted surfaces. The source cut covers seven TiDB and eight TiKV files, not completion of the overall unification target.

## Changes and domain coverage

A role-tagged IEEE754 input uses strict Byte8 transport with owned bits and the existing common prepare path. Six private local IDs are denied by the ordinary registry. Each algorithm has one f64 helper shared with its original Real wrapper; this does not expand the NotNan domain or introduce a second evaluator. Native SIGN retains coefficient-based, 15-digit coercion classes, including hidden precision and precision/scale 400 cases, without computing the answer natively.

Ordinary ASIN/ACOS preserve NaN-to-NULL behavior; legacy inverse-trig evaluation retains raw NaN for downstream cast-to-zero and `total_cmp` behavior. PB NULL also reaches real C4 execution. LegacyEvaluator distinguishes Sql, Infrastructure and InvalidResult: original SQL folding and evaluation order remain, while fatal failures propagate recursively through all nine channels. This uses neither TLS nor string tags to classify failures.

SQL tests check 36 cells against analytical constants/endpoints, not recorded engine outputs, plus 12 direct zero-slot refusals. Original metadata remains five Real columns and signed Int for SIGN. Existing expected values are not changed in this cut.

## Actual validation

Parent ran the pinned January TiKV and August TiDB wrappers. [Commands and exact result lines](../logs/raw-math-six-summary.txt) include unsuccessful attempts as well as passing targeted tests.

- TiKV `local::`: 200 passed/1 ignored/469 filtered.
- TiKV raw role/length/kernel-drift guard: 1 passed/669 filtered.
- TiKV official math tests: 46 passed/624 filtered.
- TiDB `math_dispatch_`: 3 passed/1490 filtered.
- Session lifecycle/SQL filter: 33 passed/2078 filtered.
- Unistore new legacy inverse-trig tests: 2 passed/185 filtered.
- Full expression suite: 1395 passed/4 existing failures/94 ignored; 1493 discovered, exit101. Parent compared all four complete failure blocks against inet-four-14: identical after only thread-ID normalization. The full suite remains non-green.

## Unistore compile repair and scoped comparison

The first unistore-full attempt exited101 during compilation: two JSON MEMBER OF branches still returned String instead of the new typed error. No tests ran. Parent wrapped both in their original Sql class, then reran the suite: 173 passed/1 failed/13 ignored, 187 discovered, exit101.

The sole failing test is `closure_executor_selection_over_point_range_answers_zero_rows`, exercising EqInt('abc', -1). Parent temporarily restored only `cophandler.rs` to HEAD with a path-scoped stash, leaving all other checkpoint-15 changes present. The same selected fixture failed again: 0 passed/1 failed/184 filtered, with `TruncatedWrongValue("Truncated incorrect DECIMAL value: 'abc'")`. The stash was restored. D verified that the fixture contains no math and its top-level Shared path has no folding difference against HEAD. This is a single-file scoped comparison, not a whole-tree HEAD baseline or a green unistore suite.

## Review and not verified

Moving unistore methods into `impl LegacyEvaluator` causes indentation churn; whitespace-insensitive review is needed to see the substantive error propagation changes. This is not described as a tiny one-line adapter. Release performance, complete business-wrapper operation scopes/guards, allocator remeasurement, physical peak/OOM guarantees, full-workspace validation, make lint and final acceptance remain open. These receipts do not establish PR readiness.
