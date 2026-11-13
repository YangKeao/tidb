# Shared kernel checkpoint 4 — invocation and JSON diagnostics

This remains an opt-in M1 checkpoint, not completion of the shared evaluator.
The preceding fixed commits are TiDB 4ce93983a71da469d9070499d919374fd56e913c
and TiKV 0a5cda977bd9a6707b4a56971c23f0e77e7a7322.

## Behavior

Scalar invocation is explicit and accepts one selected row. A width-one Vector
invocation remains Vector. Signed integer plus/multiply use original TiKV
kernels; the shared loop's NULL guard skips a scalar right operand after a NULL
left operand. Native `NULL + (MAX_INT * 2)` is NULL in Scalar and overflows in
Vector. Both original routes are asserted in the regression.

An IF subtree needs its original scalar child-demand contract even when invoked
from Vector. `IF(1, NULL + (MAX_INT * 2), 0)` is NULL in the original row fallback,
but the candidate initially overflowed. The shared loop now executes the native
IF region under Scalar, preserving duplicate selected occurrences, and restores
the caller mode at the region end. No frontend calls the evaluator per row.
The outer Vector arithmetic still evaluates its right operand after an IF NULL.
The checked program validates region boundaries and stack/result shapes.

JSON document coercion uses the existing SDK parser with native SQL diagnostic
adaptation. Empty/whitespace documents and malformed documents preserve code3140
and their respective native messages. The native evaluation API now carries an
optional SQL code and message instead of flattening every failure to a string.
Adapter validation failures have no SQL code; remaining kernel diagnostics are
transported without claiming complete native error equivalence.

The review initially required strict first-fatal ordering for two reachable
errors. The actual candidate difference was captured, but that requirement was
withdrawn after rereading the user's explicit permission to reorder warnings
and errors (Designer06e8cdf0). The regression now accepts either reachable
error. Region execution is justified by the independently reproduced NULL
child-demand difference, not by restoring diagnostic order. No error-order
recorder or host-resume protocol was introduced.

## Validation and evidence

Logs reside in the Engineer workspace `evidence/shared-kernel-refactor/`.
`json-diagnostic-before.log`:1 actual failed regression, after native baseline
assertions; `json-diagnostic-after.log`:14 passed.
`if-scalar-demand-before.log`:1 actual failed regression, after original numeric
batch rejection and native NULL assertions; `if-scalar-demand-after-1.log`:15 passed.
`order-regression-before.log`:12 passed/1 failed against the subsequently withdrawn
strict-first-error requirement; retained as historical difference evidence.
`checkpoint4-native.log`:15 passed,0 failed,1323 filtered; includes nested control,
512-depth, duplicate selection, mode restoration, float/Decimal identity.
`checkpoint4-sdk.log`:34 passed,0 failed,395 filtered, including existing PB
short-circuit/warning tests and eager-IF regression. PB lowering is unchanged.
`checkpoint4-clippy.log`:expression+executor libraries finish successfully with
warnings. This remains scoped clippy, not the previously failed official make
clippy workflow. Root make lint and both diff checks pass before submission.

## Scope and size

Full-domain arithmetic, RAND/variables/BENCHMARK, multi-root ordering and warning
prefix coverage remain M1 gates. No defaults, historical candidates or sealed
measurements changed; no SQL performance run or universal compatibility claim.
The known22–34K planning subtotal remains provisional; algorithm migration,
additional semantic work and retirement must be bounded before M2.

| Repo / reference | Production + / - | Tests + / - | Build + / - | Docs + / - |
| --- | --- | --- | --- | --- |
| TiDB / original |679 /0|900 /0|1866 /91|7941 /0|
| TiKV / original |891 /45|47 /4|0 /0|12 /0|
| TiDB / checkpoint3 |132 /15|216 /0|0 /0|87 /0|
| TiKV / checkpoint3 |233 /7|0 /0|0 /0|0 /0|

Combined original source+tests: +2517/-49, net2468, total2566.
Combined all files: +12336/-140, net12196, total12476. Increment: +668/-22, net646, total690.
