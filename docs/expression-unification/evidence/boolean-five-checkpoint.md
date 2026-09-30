# Five boolean/predicate families — boolean-five-11

Functional delegation/native-deletion progress: 27/245; final acceptance: 0/245. NOT, ISNULL, ISTRUE, ISFALSE and ISTRUE_WITH_NULL are five complete functional families on their existing admitted surfaces. Negated spellings do not earn additional families. This checkpoint does not complete the overall unification target.

## Changes and domain coverage

TiKV uses fixed single- or two-FnCall recipes, including three composed negated predicates, with the native caller's factory limits of depth3/nodes4. Int truth/presence is transport after the original frontend conversions, not a narrowing of the SQL input domain. The public native `BooleanFunction`/`eval_boolean_ready_in` boundary checks that computed results are0/1/NULL.

Existing PB admission remains the seven IsNull signatures, UnaryNotInt and IntIsTrueWithNull. The legacy unistore integer-child and Datum channels keep their original differences. Ordinary no-warning behavior and PB1292 diagnostics remain distinct; typed UNKNOWN validation is not replaced by AST presence checking. Syntactic NOT wrappers negate the predicate result without changing the underlying predicate or repeating its child conversion.

Native vector NOT/ISNULL fast calculations are removed. These paths decline before evaluating children and use the existing row route with the original context. SQL coverage includes13 predicate spellings over three states and28 direct zero-slot refusals, including two WHERE cases. A planner may legitimately eliminate NOT(compare); this is not evidence of forcing a vector comparison fast path. Ordinary SQL did not previously admit ISTRUE_WITH_NULL, so no new SQL admission or fabricated SQL coverage is claimed; its existing PB/native paths are covered.

## Actual validation

Parent ran the pinned January TiKV and August TiDB wrappers. [Commands and exact result lines](../logs/boolean-five-summary.txt) are retained separately.

- TiKV `local::`:192 passed/1 ignored/468 filtered.
- TiKV exact composite-call-chain guard:1 passed/660 filtered.
- TiDB `boolean_dispatch_`:3 passed/1480 filtered.
- Session lifecycle/SQL filter:25 passed/2078 filtered.
- Unistore `is_null_preserves_integer_and_datum_channels`:1 passed/183 filtered.
- Full expression suite:1385 passed/4 existing failures/94 ignored;1483 discovered. Parent compared all four complete failure blocks against integer-seven-10: equal after only thread-ID normalization. The existing vectorized_builtin_op_func failure remains the duration-FSP fixture panic, not a newly failing boolean assertion.

No old expected values were changed and no new failures were introduced. Successful and NULL results, original conversion/diagnostic distinctions, fixed recipe identity and same-root refusal are covered by these targeted receipts; the full expression suite is not reported as green.

## Not verified

Release performance, complete business-wrapper operation scopes/guards, allocator remeasurement, physical peak/OOM guarantees, full-workspace validation, make lint and final acceptance remain open. Targeted tests and earlier allocation receipts do not close those gates.
