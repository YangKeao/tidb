# N6c: shared numeric batch value interfaces

TiDB parentdf9335d8 plus this signed local checkpoint; TiKVec9cc67 unchanged.
No push. The global no-fallback goal remains active.

The existing native source owner and row arithmetic normalization now share
plain field/binary-literal source-domain selection. The original integer batch
owner shares strict NULL/Int/UInt bit extraction and declared-signedness
repacking. Outside-domain values retain the original Unsupported error; these
interfaces do not coerce arbitrary values into integer answers.

Numeric batch cast completion now has a value-only seam receiving the actual
source-typed Datum, immutable field/source/target facts and optional diagnostic
text or the exact retained native-format failure bits. It shares the original
vectorized cast implementation with the existing batch owner. Matching domains
return the exact value without casting, context reads or subject rendering.
Legacy dynamic source text remains lazy in the old owner; the new runtime-facing
seam has no Expression, ScalarFunction or evaluator callback.

Source evaluation and source normalization remain a separate complete phase.
In particular, literal typed conversion happens once before its batch broadcast.
This checkpoint does not move that phase into the new provider or claim that
per-row typed normalization reproduces it. Borrowed string-column specialization
also keeps its original direct cast path.

The audit additionally distinguishes eval_integer_batch's optimized Int bulk
loop from both the generic eval_arithmetic_batch ladder and the row typed
arithmetic probe. Its operands are strict bit batches; it performs its own
unsigned interpretation, integer overflow mapping and output-bit handling.
Upcoming window lowering must preserve this mode explicitly, along with the
generic path's configuration reads and return coercion. Bulk ABS/CAST source
phases also need audit before admitting those sources beneath new roots.

Validation:

- Six numeric-argument tests pass. The new test compares120 actual value/error
  results and complete diagnostic/context traces against the original
  vectorized cast consumer, plus matching-domain skip, binary/hybrid source
  selection and signed/unsigned strict-bit round trips and refusal cases.
- A same-domain Float32 value stays Float32 even with a retained nonfinite JSON
  formatter failure in diagnostic metadata; no context read or rendering occurs.
- Full frontend1465 pass, four exact inherited failures and95 ignored. Names
  match N6b exactly; all106 existing batch tests pass inside the same full run.
- All22 Projection tests pass. Required make lint, pinned scoped rustfmt and
  git diff --check pass. Full frontend is not claimed green.
- TiKV source is unchanged; no redundant SDK rebuild was run. N6b's core1051
  pass with the exact inherited six failures and one ignored remains the paired
  SDK evidence.
- Logs: workspace evidence/no-fallback/n6c-numeric-batch-values-final.log,
  n6c-numeric-batch-front-full.log, n6c-numeric-batch-projection.log and
  n6c-numeric-batch-lint.log.

Focused command from the TiDB tree:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib scalar_function::numeric_argument_tests

The freshly emitted frontend test binary was then run without a filter.
Projection and lint use the commands recorded in N6b. Formatting uses the
pinned frontend toolchain with skip_children=true on the two changed Rust files.

Next connect the window arithmetic provider to these values and to direct
PreparedArithmetic.compute, add source normalization/broadcast ownership, and
lower actual bulk edges independently of row boundaries. Window task ownership
and diagnostics must be bounded before conversion/formatting. Preserve complete
selection phases beyond1024 before promoting root/caller coverage. Other
functions/effects, dynamic sources, multi-output and global migration-dispatch
retirement remain unfinished. This checkpoint closes no frontend root fallback.
