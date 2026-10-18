# N6n — unary signs and original native signature eligibility

Status: verified local checkpoint. The global no-fallback migration remains
active. No push or PR is authorized.

## REDs and implementation

The independent unary-plus/minus roots over ABS(column0), rows1,MIN_INT, each
refused compilation and fell back. `n6n-unary-sign-red.log` records the two
route failures, and `n6n-unary-sign-green-fixed.log` records both with zero
fallback. This was an admission defect, not an invented SQL error mismatch.

`UnarySignNative(NativeUnarySign)` has one raw capability argument. Immutable
unsigned and folded-constant facts accompany the actual value. The new public
SDK value kernels are shared by the existing11 RPN recipes and frontend
callers; the frontend unary sign helper and integer-minus helper no longer
execute ready-argument programs. There is no Expression callback, fake ready
child, recursive SDK program or runtime compilation in the new provider.

The existing checked integer leaf preserves computed overflow and
constant-only Decimal promotion. Its typed cause retains the original bits
and signedness, and one shared frontend mapper renders the existing
DataOutOfRange(BIGINT, minus-original-bits), including signed MIN's double
minus. Real/Float32 use full f64 bits. Bytes/String kind and collation survive
the value kernel. Decimal plus restores its declared header; minus clears it;
storage and result scales continue through the existing exact Decimal workers.
The provider bounds raw copies and genuine dense Decimal conversion/clone work
before allocating the SDK word representation. Limits are conservative
logical/retained byte allowances, not SQL precision caps or hard allocator peaks.

The larger matrix exposed a real missing layer: ScalarFunction.Eval normalizes
the kernel's computed value to the declared result family. Bypassing it made
an unsigned-bit Int constant's promoted result leak as -18446744073709551615
instead of the original declared-Int boundary -9223372036854775808. Restoring
the existing result normalizer resolves that RED without weakening the fixture.

A separate qualification RED showed that using the growing compiler value
closure to select native CHAR signatures could add native batch eligibility
and a vectorization getter. Native eligibility now uses its existing raw-value
families independently from compiled capability transport. Ten signed,
unsigned, Real, Decimal and String qualification assertions cover both signs.
The same canonical driver executes the resulting programs; these are signature
facts, not a second runtime evaluation mode. Existing row/batch publication
and parent demand rules stay unchanged.

## Evidence

Logs are retained under the agent workspace `evidence/no-fallback/`.

- The first enlarged batch run exposed the result-family bug above; its failed
  output and subsequent diagnostic fixture run are retained in
  `n6n-unary-sign-batch.log` and `n6n-unary-sign-kind-debug-fixed.log`. Temporary
  fixture prints were removed. An initial debug-only moved-value compile error
  and a missing SDK error-type import were corrected before final validation.
- `n6n-unary-sign-qualification-red.log` records149 passes and the native CHAR
  qualification failure. `n6n-unary-sign-batch-fixed.log` records150 passes;
  final full frontend also includes the expanded ten-type/sign assertions.
- 2176 new native-owner comparisons cover31 literal cases,21 column cases,
  both signs, empty/repeated/reordered/2050-row selections, arithmetic/control
  parents and folded-constant facts:992 literal +672 column +504 parent +8
  original RED comparisons. Typed CHAR roots use the existing typed-root
  comparison helper; native roots retain the strict native-role helper.
- The shared direct-provider matrix has63 cases across nine operations. The
  new sign cases cover resource refusal before effects, wrong role/generation/
  occurrence, strict string-minus rejection, string-plus identity under the
  declared public result type, genuine NULL and clean reuse after cancellation.
- `n6n-unary-sign-result-flags-full.log`:1510 pass, exactly the inherited four
  failures,95 ignored; all151 batch tests pass. Failure names mechanically
  match N6m. The earlier pre-flag run had1509 passes.
- `n6n-unary-sign-core-full.log`:1062 pass, exactly the inherited six failures,
  one ignored. Failure names mechanically match N6m. All18 window/catalog
  tests and the three existing unary expected-result tests pass, covering
  integer boundaries, constant promotion, raw bits, Decimal scales and causes.
- `n6n-unary-sign-core-clippy.log`: scoped clippy passes with89 inherited
  warnings. `n6n-unary-sign-result-flags-lint.log`: `make lint` passes. Both pinned nightly
  format checks pass.
- `n6n-unary-sign-result-flags-projection.log`: all25 production Projection tests pass.
  The mixed output matrix now has22 roots, adding integer plus/minus,
  constant UInt-to-Decimal promotion and Decimal-plus header behavior under
  both flags and serial/two-worker execution.
- Both final git diff checks pass. All build/test handles are closed.

Datatype code is unchanged and was not rerun. Previously recorded inherited
failures are not presumed outside the eventual completion audit. Other source
families, compositions, metadata flags, dynamic/deferred sources, multi-output
effects and other callers still need the overall migration and final audit.

The additional valid-SET regression exposed a generic result-view omission:
ENUM_SET_AS_INT was applied by public Expression.Eval but missing from native
leaf transport and native function results. `n6n-unary-sign-enum-view-red.log`
records -9007199254740992 instead of exact -9007199254740993 for SET ordinal
2^53+1. One extracted, shared declaration/value helper now applies the same
view at literal/column imports and final SQL capability publication. Unchanged
literals remain borrowed until the copy-budget check; function result metadata
uses its existing O(1) host slot, with no expression callback or schema copy.
All four literal, column, IF and COALESCE paths pass under both execution flags
(16 added comparisons). `n6n-unary-sign-result-flags-batch.log` records151 passes.
The full frontend, Projection and lint gates were rerun after this correction;
SDK code did not change after its recorded core and clippy runs.
