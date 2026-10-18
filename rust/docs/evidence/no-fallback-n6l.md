# N6l — bitwise native values and operand demand

Status: all scoped gates completed; ready for signed local commits. Overall
no-fallback goal remains active. No push or PR is authorized.

## Reproduced defect and implementation

The actual native comparison of BITXOR(ABS(column0),ABS(column1)) over
(1,MIN_INT),(MIN_INT,1) failed before the change: the unified typed graph
returned Evaluation/Invoke instead of DataOutOfRange(BIGINT,abs(t.c1)). It
completed a left operand window before the first row's right operand.
`n6l-bitwise-red.log` records this RED; the same regression now passes.

Six closed `BitwiseNative(NativeBitwiseOp)` operations carry raw native-value
capabilities through the existing SDK frame: binary AND/OR/XOR/shifts demand
left then right even after a left NULL, and unary NOT demands one value.
Each capability retains its actual producer, occurrence, physical row,
namespace, generation and role. Binary conversion reads the original
`div_precision_increment` getter after both raw values. Unary conversion does
not read it. Immutable unsigned declaration flags survive recursive coercion;
other expression metadata is not consulted by these restricted value paths.

The existing value conversion ladder is shared, including JSON stringification,
temporal numeric conversion, integer-prefix warnings, real ties-to-even and
Decimal warning/error order. It is not reimplemented as another cast table.
`impl_op::native_bitwise_value` now owns final nullable bit computation for
both the original RPN workers and frontend value callers. Frontend bitwise
values no longer build/run ready-argument programs. No callable Expression,
fake ready-child tree, runtime graph compilation or second evaluator enters
the new provider.

Source lowering closes the native-value subtree and promotes complete bitwise
roots into their actual row domain. Outer explicit whole-selection arithmetic
and integer ABS/CAST keep their existing complete operand phases; row scopes
preserve original occurrence identity below them. Strict legacy test-consumer
contracts remain unchanged. Ordinary typed parents outside this closed subtree
still require further migration; this checkpoint does not claim global closure.

Before copies/conversion/effects, the provider checks native payload owners,
Decimal visible/storage-scale work and conservative JSON expansion allowances.
Resource and capability errors remain terminal; owned SQL conversion errors
retain their original type. These are logical/retained allowances, not a hard
whole-statement allocator-peak guarantee.

## Evidence

Logs are retained under the agent workspace `evidence/no-fallback/`.

- `n6l-bitwise-matrix-final.log`: all four focused tests pass. The native-owner
  comparisons comprise13,012 invocations:27 roots ×7 selections ×4 flag states,
  23 literal carrier cases ×41 roots ×3 selections ×4 states,13 actual column
  carrier cases ×6 operations ×3 selections ×4 states, and the four original
  RED comparisons. Empty, duplicate, reordered and2050-row selections are
  included. Output bytes/NULL layout, errors, context trace and zero fallback
  are compared. The same prepared owner is reused across flag toggles.
- The direct provider test adds42 cases across the six operations: large Bytes
  and million-digit Decimal work refusal before context effects, wrong role,
  expired generation, wrong occurrence, strict warning rejection retaining its
  owned SQL error, and successful NULL reuse after cancellation. Binary NULL
  still demands the actual right capability.
- `n6l-bitwise-front-full.log`:1500 pass, exactly the inherited four failures,
  95 ignored; all141 batch tests pass. Failure names were compared mechanically
  against N6k, not inferred from the count.
- `n6l-bitwise-lint.log`: `make lint` passes.
- Both pinned nightly format checks pass for the changed files; both git diff
  checks pass.
- `n6l-bitwise-core-full.log`:1059 pass, exactly the inherited six failures,
  one ignored. Failure names mechanically match N6k. All15 window protocol
  tests pass, including the new five-operation2050-row bitwise scope test.
  The six existing bitwise RPN value tests also pass against their original
  expected-result tables.
- `n6l-bitwise-core-clippy.log`: scoped clippy passes with89 inherited warnings.
- `n6l-bitwise-projection.log`: all25 tests pass. The existing unary/native
  matrix now contains15 output roots, including all six bitwise operations
  and an outer whole-child arithmetic composition, under both flags and
  serial/two-worker production execution.

No build/test handles remain active. Datatype code did not change in N6l;
its N6k parent-proven two failures were not rerun for this checkpoint.

The initial implementation runs exposed a private method visibility error, a
missing opaque-handle catalog case, and a test-only Decimal import. Each was
corrected before the successful focused/full frontend verification. Failed
build logs are retained; they are not counted as semantic test successes.
