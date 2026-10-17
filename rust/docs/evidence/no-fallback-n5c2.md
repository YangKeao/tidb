# N5c2: typed JSON CAST in native value islands

TiDB parent5e431c96 plus this commit; TiKV unchanged9cc6b26. Signed local
checkpoint, no push. This is not completion of global fallback elimination.

JSON CAST differs from ordinary CAST: it validates its declared source before
demanding the argument, then reads that argument in its typed domain and calls
the prepared JSON conversion with source metadata and target flags. The generic
untyped cast_as_json route would lose these distinctions.

The CastNative host now admits the closed cast_json name. Its immutable
descriptor adds only an input_is_literal bit. At start it invokes the existing
source validator before returning an argument request; errors retain the exact
native error owner. After demand it selects the same plain typed source
conversion as the native caller, then builtin_ext::json::cast_json_prepared and
the shared result boundary. Native source/target charset, unsigned/hybrid flags
and PARSE_TO_JSON policy remain separate from the actual value. All existing
source metadata/payload, handle provenance, clone and output checks remain.

Constant::eval_typed_as_on_row retains GetType and raw parameter/deferred/value
fetch order, then delegates its original conversion body to typed_constant_value
(field,context,target,value). It does not call generic Constant::Eval, which
would alter decimal shape and diagnostics. Nonconstant eval_numeric_row now
delegates its original hybrid conversion and carrier normalization to
normalize_numeric_row_value(value,source,target,context); Constant/correlated
typed calls and their final carrier normalization remain unchanged. Both
helpers are plain value boundaries with no expression or evaluation callback.
The provider constructs neither Constant nor ScalarFunction objects. Its
conversion and prepared JSON helpers contain no nested evaluated-args program.

Validation:

- RED proof: an Int-column JSON cast with empty selection previously fell back.
  156 final scenarios compare 13 sources ×literal/column ×parse-document flag
  ×empty/duplicate/2050 selections. They cover signed/unsigned, Float32/NaN,
  Decimal, valid/invalid text, BINARY bytes, ENUM/BIT, JSON and vector/NULL.
  Raw output, NULLs, exact native errors and full warning vectors match.
- A poisoned source for vector-typed NULL asserts zero reads and no output.
  The exact native vector error must occur before even requesting that argument.
- 24 parser/rewriter SQL executions cover documents, errors, nested JSON/signed
  casts, outer CHAR, CONCAT and REGEXP. Each prepares without fallback/refusal
  and matches native output/error/diagnostics for selected occurrences.
- 82 focused frontend tests pass. Full frontend1434 pass with exactly the same
  4 inherited failures by name and95 ignored.
- 14 production Projection tests pass. New serial/parallel cases compare exact
  outputs, errors and statement warnings for valid/bad documents, UInt64::MAX,
  Float32, vector-NULL rejection and a nested warning-producing signed cast.
  Serial counters confirm preparation without fallback/refusal on error too.
- make lint, scoped rustfmt and diff checks pass. Static audit of the extracted
  value helpers confirms no Expression eval, constructed ScalarFunction,
  evaluate_args_in or LocalProgram. Core is unchanged; N5c1 full/clippy remain
  applicable. The initial test fixture's flag API was corrected to a raw zero;
  those compile errors did not exercise production behavior.

Reproduce with pinned ../tools/cargo-tidb from TiDB root, RUSTFLAGS='',
CARGO_BUILD_JOBS=8: test --manifest-path rust/Cargo.toml --release --locked
--offline -j8 -p tidb-expr --lib evaluator::batch, then the fresh binary for
the full suite; -p tidb-executor --lib projection:: for production callers.

Raw workspace evidence/no-fallback: n5c-json-before-final.log,
n5c-json-focused.log, n5c-json-focused-sql.log, n5c-json-full.log,
n5c-json-projection.log and n5c-json-lint.log.

Typed conversion/SDK temporaries and actual context/destination allocations
remain outside the conservative arena/host logical policy. No speedup or hard
statement memory bound is claimed. General computed parents, lazy controls,
effects, remaining functions/source forms and other callers still need coverage
before alternate-evaluator dispatch can be removed.
