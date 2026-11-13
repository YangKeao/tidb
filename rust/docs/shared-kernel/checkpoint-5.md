# Shared kernel checkpoint 5 — root demand, state and repetition

The preceding fixed commits are TiDB b21d5a9b2a214c5b1653b8bc2b04771e145469a0
and TiKV038b1aa3ed86a1ff50fc8fa7f9f6eb443b5d62bf. M1 remains incomplete.

## Corrected root contract

Checkpoint4's outer-Vector assertion was wrong: the original numeric admission
recursively rejects the entire root when it contains IF. Its test checked the
candidate error without comparing the complete original root. The new regression
first confirms original batch rejection and native NULL, then reproduced the
candidate overflow. Compilation now selects a scalar region around the whole
fallback root. For the currently admitted signatures only pure signed plus/mul
trees enter numeric Vector; IF/JSON/regexp/MD5/state/repeat descendants make the
whole root use the original row fallback. Per-IF regions were removed. This
classification must be extended with actual native admission as signatures grow.

`NULL + JSON_LENGTH('{')` now skips its right child. Pure numeric
`NULL + (MAX_INT * 2)` still overflows in Vector and returns NULL in Scalar.
The earlier checkpoint4 claim about restoring an outer native Vector path is
superseded here; that path was not admitted for the whole original expression.
Invalid UTF-8 JSON now emits native unknown-error1105 and the original text,
through a typed Custom error at the parser boundary, without text-based code guessing.

## State and shared traversal

Input adapters expose concrete session RNG/variable capabilities, never expression
callbacks. Native RAND nodes execute inside the shared loop; constant integer
seeds use one stable frozen-node identity with the session's per-occurrence RNG,
while dynamic integer seeds use the existing SDK MySqlRng freshly per occurrence.
The existing PB RAND lowering and RNG implementations remain unchanged.

The SDK group entry executes column-major RAND roots and row-major variable
projections through the same evaluator. Native integration uses its original
variable-sensitive projection-order predicate. String SETVAR/GETVAR are admitted;
NULL assignment returns NULL without overwriting the previous session value.
Non-string stored variables and other typed variable coercions remain explicitly
unsupported; this is a minimum state proof, not complete user-variable coverage.
RAND seed admission currently accepts signed integer seeds only.

Flat Repeat/RepeatEnd instructions implement BENCHMARK inside the same loop.
NULL/negative counts skip body type checks; zero checks type but skips execution;
positive counts rerun the child, including its effects. Nested repeats and guarded
unselected repeats are covered. An unsupported Vector body is represented by its
runtime type check without compiling/evaluating that child's value.

## Validation

Raw logs are in Engineer workspace `evidence/shared-kernel-refactor/`.
`root-fallback-before.log`:actual1 failure after whole-root native baseline passed;
`root-fallback-after.log`:15 passed. `json-utf8-before.log`:actual1 failure,10000
versus1105. `state-tests-1.log`:18 passed; `repeat-tests-1.log`:19 passed.
`checkpoint5-native.log`:19 passed,0 failed,1323 filtered; final additions include
illegal repeat programs and an unselected stateful repeat. Native EvaluatorSuite
supplies the RAND/variable group baselines; constant-seed sequences and dynamic
seeds are compared with native calls and the original MysqlRng.
`checkpoint5-sdk.log`:34 passed,0 failed,395 filtered. `checkpoint5-clippy.log`:
expression/executor library checks finish with warnings. Root lint and both diff
checks pass before submission. Official make clippy remains unfulfilled as
previously recorded; these scoped checks do not replace it.

## Remaining gates and size

Full-domain Real/Decimal arithmetic, warning prefixes and remaining native error
adaptation (including regexp) still require work. Default integration, broad
compatibility, full SDK validation and SQL performance are not claimed. Historical
candidates and measurements are untouched. Known22–34K planning subtotal remains
provisional; bound additional algorithm/compatibility/retirement work before M2.

| Repo / reference | Production + / - | Tests + / - | Build + / - | Docs + / - |
| --- | --- | --- | --- | --- |
| TiDB / original |927 /0|1369 /0|1866 /91|8033 /0|
| TiKV / original |1276 /48|47 /4|0 /0|12 /0|
| TiDB / checkpoint4 |270 /22|473 /4|0 /0|92 /0|
| TiKV / checkpoint4 |397 /15|0 /0|0 /0|0 /0|

Combined original source+tests: +3619/-52, net3567, total3671.
Combined all files: +13530/-143, net13387, total13673. Increment: +1232/-41, net1191, total1273.
