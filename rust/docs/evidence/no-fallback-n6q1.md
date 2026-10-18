# N6q1 — genuine quotient SQL source integration

Date:2026-10-18. Parent local heads:TiDB0a761ef8 / TiKV6a36ba0.
This is a validated incremental source integration, not global completion.
All commits remain local and signed; no push, PR or deployment is authorized.

## Result and boundaries

MOD, ordinary division and integer DIV now lower into their genuine row or
whole-window operations in the canonical SDK driver. Original three
ABS-operand regressions changed from exactly one refusal/one fallback to zero
of each. No expression evaluator callback, ready-child tree or extra SDK
execution driver was added. Source collection and implicit cast phases retain
the original whole-column order; row Int/Real MOD still demands RHS after
left NULL. Quotients do not borrow the additive-only typed fast probe.

Actual integer signedness, generic precision reads and conditional Decimal DIV
probe/fallback reads retain their original owners. Row decimal overflow uses
source argument/CAST text with strict-source precision refinement; whole
Decimal DIV uses actual converted decimals, and integer DIV reports actual
integer operands. A detached diagnostic DTO contains only text and flags.
Original diagnostic cast formatting is shared with the source owner.
Raw DIV promotion is one pure fetched-value policy used by the old tree
caller and canonical provider; real-to-Decimal casts use captured fields and
diagnostic-only text suppliers. The obsolete fake literal Expression in that
cast wrapper was removed. Terminal resource failures and original native
causes retain the existing error boundary.

The original static native-CHAR eligibility families are unchanged even though
the compiled source closure grew. Original ScalarFunction return coercion and
Expression enum/set integer-view publication remain shared. Resource admission
bounds logical/retained work, not whole-statement allocator peak.

## Evidence

Agent workspace evidence/no-fallback/ contains:

- n6q-quotient-root-red.log and root-still-red.log:three actual admission
  regressions before source integration, including after the protocol bridge.
- n6q-quotient-root-green-fixed.log:all three pass after genuine integration.
- n6q-quotient-batch-expanded.log:159 batch tests pass, including all151 old
  batch tests. New source/value/context matrices contain19,500 comparisons:
  10,440 literal,1368 column,6084 column-pair,1092 parent/control/typed-CHAR,
  12 original root profiles,504 changing precision/strict diagnostic profiles.
  Selections include empty, reordered duplicates and2050 occurrences.
  The direct window fixture covers105 SQL/source-fault cases plus180 terminal
  resource checks; invalid source errors remain primary and no retry fallback
  is taken.
- n6q-quotient-front-full.log:1523 pass, exactly four inherited failures,
  95 ignored and zero filtered. All eight new tests and the N6p fixed
  pre-retirement quotient witnesses pass. n6q-quotient-inherited-failures.txt
  records a machine comparison against N6p's exact failure list.
- n6q-quotient-projection.log:all25 production Projection tests pass. The
  existing mixed-output matrix now has28 roots, including six new quotient
  outputs, under both vectorization settings and serial/two-worker execution.
- n6q-quotient-lint.log:make lint passes.

An extra resource probe was rejected rather than counted as a production RED:
its raw Decimal had one million visible fractional digits with one stored
digit. The initial physical-column fixture fails Chunk encoding; the literal
fixture fails original SQL diagnostic formatting before the operation. Logs
n6q-quotient-source-work-red.log and source-work-red-fixed.log preserve those
setup failures. No source budget rule was weakened on this premise, and that
invalid fixture was removed. N6p's genuine borrowed-kernel fast/zero/resource
checks remain active in the full suite.

The first expanded matrix run also caught a test-construction error: builtin
inference correctly rejects vector operands. The fixture now explicitly
asserts that vector-only reason before declaring the already-invalid generic
call to compare its runtime error. No carrier was skipped and no production
admission default was added. Initial DTO closure type inference was corrected
before successful builds; no semantic mismatch was observed in the final
matrices.

## Reproduction

From the TiDB root:

```sh
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
make lint
```

Pinned frontend nightly rustfmt checks cover all12 changed Rust files; both
repositories pass git diff --check. SDK production code and datatype are
unchanged from N6q0, so SDK full1068/six inherited failures/one ignored and
clippy89 warnings are reused, not claimed as rerun here.

Global alternate execution paths, other builtin families/callers, inherited
failure audit and final SQL/shared-library/server gates remain outstanding.
The active task/goal is not complete at this checkpoint.
