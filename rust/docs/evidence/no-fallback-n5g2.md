# N5g2: shared add/subtract/multiply value preparation

TiDB parent1a80508d plus this commit; paired TiKV4fb4283. Signed local
checkpoint, no push. Arithmetic source HostFrames are still the next step;
this checkpoint does not claim to remove their migration dispatch.

The new ops/arithmetic.rs prepares owned values for the three operations
without retaining an expression owner or argument program. Both original
callers now use this preparation: the generic Datum ladder and the typed
integer arithmetic path. PreparedArithmetic retains actual NULL, integer
signedness and subtraction mode, raw float values, shared Decimal owners or
native vector owners. A temporary into_evaluated adapter lets those existing
callers keep their original worker contracts during migration.

The preparation preserves the original ladder and its ordering: sentinels,
unsigned reinterpretation, vector selection/NULL and casts, JSON/raw errors,
temporal promotion, left/right string conversions, real/decimal promotion,
and integral conversion. NO_UNSIGNED_SUBTRACTION is observed at the original
integer preparation point. The shared integer recipe retains the original
non-mathematical subtraction edge policy; it does not substitute an i128
range check. Remainder and division keep their distinct existing preparation.

PreparedArithmetic.compute executes directly through pure shared core
entries. Original RPN wrappers call these same integer, nullable raw IEEE,
wide-decimal, actual-NULL witness and vector value helpers. The new compute
path does not create or execute an EvaluatedArgs program. Real native finite
checks remain separate from the existing legacy IEEE policy. Vector value
methods retain their actual typed error and original arithmetic behavior.

Direct arithmetic overflow mapping inspects the actual retained typed core
cause and produces the original IntOverflow/FloatOverflow/DecimalOverflow.
It neither parses messages nor constructs an old worker failure receipt.
Other SDK failures retain their original opaque cause and unattributed phase.
A finite allowance is mandatory. Vector capacities plus a conservative full
output owner are checked before the native method allocates; Decimal retains
its finite core and output bridge allowances.

Validation:

- 32 actual-value fixtures ×32 partners ×3 operations ×4 unsigned metadata
  combinations ×2 subtraction modes =24576 comparisons. Direct computation
  matches the original registered argument program's values, typed errors,
  complete warnings and subtraction-mode observations. Extra checks retain
  raw float/vector bits and Decimal coefficient, visible scale and storage
  scale. Fixtures include integer bounds, declaration/actual disagreements,
  nonfinite floats, wide decimals, strings with prefix warnings, bit/binary
  literals, enum/set, temporal values, JSON/raw/sentinels, and empty/mismatched
  vectors.
- Observer budget failures remain terminal ResourceLimit errors. An actually
  observed NULL invokes the same NULL primitive without an invented partner
  or subtraction-mode read. A separate boundary test invokes a real Decimal
  kernel with no allowance and verifies the retained NativeDecimalError::Resource
  cause. Its outer runtime class is Evaluation, matching the SDK's original
  LocalError discriminant; it is never mapped to SQL arithmetic overflow.
- Three focused tests pass. Full frontend1461 pass, four exact inherited
  failures and95 ignored. All103 batch tests are included in that full run.
  Full core1044 pass, six exact inherited failures and one ignored. Both
  failure-name lists match N5g1. All21 production Projection tests pass.
- make lint, scoped core clippy, scoped pinned rustfmt and git diff --check
  pass. Logs: workspace evidence/no-fallback/n5g-arithmetic-{front-focused,
  front-full,core-full,projection,clippy,lint}.log.

Exact primary validation commands, from their respective tree roots:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib additive
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tikv test --release --locked --offline -j8 -p tidb_query_expr --lib
    make lint

The fresh frontend test binary emitted by the focused build was also run
without a test filter for the reported full-suite result.

Remaining work: the native arithmetic HostFrame must extract the original
numeric operand conversion and source diagnostic metadata. It must preserve
left conversion before right demand, signature-specific NULL demand, binary
literal normalization, return coercion, and context observations. The current
production callers still use the temporary argument adapter; no new native
source coverage is claimed here. Other arithmetic operations, other functions,
effects, sources/callers and final global dispatch removal remain outstanding.
