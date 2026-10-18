# N6m — logical XOR through the canonical driver

Status: all scoped gates pass subject to the exact inherited failures below.
The overall migration remains active. No push or PR is authorized.

## Actual RED and resulting behavior

XOR(ABS(column0),ABS(column1)) over(1,MIN_INT),(MIN_INT,1) previously refused
unified compilation and incremented both refusal and fallback counters. The
legacy evaluator returned the native SQL error, so the RED is the observed
fallback, not an invented error mismatch. `n6m-logical-xor-red.log` records it;
`n6m-logical-xor-green.log` records the same regression with zero fallback.

`XorNative` is a genuine, closed operation with two raw-value handle arguments
and an actual native result capability. It shares transport, capability
validation and copy/ownership checks with the previous bitwise provider in
`batch_raw_values.rs`; a distinct semantic variant carries logical XOR. Only
bitwise operations carry their real unsigned declaration flags. No callable
Expression, fake ready child, runtime graph compilation or ready-arguments
program enters the new XOR path.

The original binary caller demands both raw operands, including after NULL,
then reads div precision and rejects range sentinels. It probes string numeric
warnings on both sides, intentionally discarding probe conversion errors,
before attempting left and right truth conversion. The shared original value
helper retains this order. Actual input/truth SQL errors and resource errors
remain terminal. Final nullable XOR computation is shared with the existing
RPN worker; the frontend XOR helper no longer invokes a ready-value program.

XOR truth tests Decimal coefficient zero directly. Its work bound therefore
counts actual payload copies and truth/warning work, not unperformed Decimal
scale expansion or bitwise JSON text casts. The direct provider test proves
that a million-scale Decimal succeeds under a small budget while the genuine
bitwise conversion still refuses before effects. These are conservative
logical/retained allowances, not a hard whole-statement allocator peak.

Original static native eligibility stays unchanged. A unified XOR root owns a
row domain and does not acquire vectorization eligibility/configuration reads
merely because its new host is supported. Parent comparisons preserve the
actual native eligibility and phase rules. The SDK separately validates raw
row operands inside an explicit whole-selection parent, with original
occurrence identity; this is not a claim that every SQL XOR parent is eligible
for native batch execution. Ordinary typed parents and other missing function
families remain work for the overall goal.

## Validation

Logs are retained in the agent workspace `evidence/no-fallback/`.

- `n6m-logical-xor-batch.log`:145 batch tests pass, including existing bitwise
  tests after the shared-provider refactor. New XOR/native-owner comparisons
  cover23 literal carriers,13 actual column cases, NULL, repeated/reordered
  selections,2050 rows, arithmetic/control parents and strict warning probes.
  There are3324 comparisons:8 roots ×7 selections ×4 flag states;23 literal
  cases ×10 roots ×3 selections ×4 states;13 column cases ×2 orientations ×3
  selections ×4 states;24 strict-probe comparisons; and4 original RED cases.
- The shared direct-provider matrix now has49 cases across six bitwise
  operations and logical XOR. It includes resource refusal before context
  effects, wrong role/occurrence/generation, real SQL rejection, intentionally
  ignored XOR warning-probe errors, million-scale Decimal truth and clean
  reuse after cancellation. NULL binary operands still demand the right value.
- `n6m-logical-xor-front-full.log`:1504 pass, exactly the inherited four
  failures,95 ignored; all145 batch tests pass. Failure names mechanically
  match the N6l baseline.
- `n6m-logical-xor-core-full.log`:1060 pass, exactly the inherited six
  failures, one ignored; failure names mechanically match N6l. All16 window
  protocol/catalog tests pass, including six raw binary operations beneath
  a2050-row whole parent and XOR arity/carrier/operation-identity rejection.
  The original RPN logical-XOR expected-result test also passes.
- `n6m-logical-xor-projection.log`: all25 production Projection tests pass;
  the mixed output matrix now has18 roots, including XOR, an arithmetic parent
  and NULL XOR with a warning-producing right operand, under both flags and
  serial/two-worker execution.
- `n6m-logical-xor-lint.log`: `make lint` passes. Both pinned nightly format
  checks pass. Scoped SDK clippy passes with89 inherited warnings; both final
  git diff checks pass.

Datatype code did not change; its N6k parent-proven two failures were not rerun
for this checkpoint. These recorded inherited failures do not establish that
they are outside the eventual completion audit.

All build/test handles are closed. This checkpoint is ready for signed local
commits; global no-fallback coverage and retirement remain incomplete.
