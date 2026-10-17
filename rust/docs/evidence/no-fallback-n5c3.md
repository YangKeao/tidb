# N5c3: preserve SQL at an incomplete native producer boundary

TiDB parent e88fdb5d plus this signed local commit; core unchanged9cc6b26.
No push; global elimination remains active.

The N5c native argument recursion exposed an incorrect old guard: when a CAST
argument is a valid calculation without a native value recipe, preflight
reported a binding-contract failure. CAST(ABS(column) AS SIGNED), for example,
then failed with InputContract/Prepare although the existing native evaluator
can execute it. This was introduced by our expansion of the source closure.

Classify only this missing producer recipe as an admission capability gap.
Existing temporary migration dispatch preserves the SQL until that producer is
connected. No alternative evaluator code or retry is added. Namespace/role/row/
source binding checks, schema checks, resource policy and native execution
errors retain their existing terminal paths.

The regression fails before the fix with InputContract/Prepare. Afterward four
selected-row cases (CAST of ABS into signed/JSON, nested CHAR/signed/PLUS, and
signed/IF) match native results and expose the precise admission reason. This
test records migration state, not a permanent fallback contract: update its
route expectation when these producer recipes are connected.

83 focused tests pass, including existing terminal resource/protocol/no-replay
checks. Full frontend1435 pass with exactly the same4 inherited failure names,
95 ignored. make lint, scoped rustfmt and diff checks pass. Core and value
kernels are unchanged; N5c1 core and N5c2 production caller checks remain
applicable. No new prepared coverage or speedup is claimed.

Reproduce with pinned ../tools/cargo-tidb from TiDB root, RUSTFLAGS='',
CARGO_BUILD_JOBS=8, test --manifest-path rust/Cargo.toml --release --locked
--offline -j8 -p tidb-expr --lib evaluator::batch, then the fresh binary for full
tests; make lint. Raw workspace evidence/no-fallback: n5c-gap-before.log,
n5c-gap-focused.log, n5c-gap-full.log and n5c-gap-lint.log.

Next connect additional native producers and nullable controls, preserving
previously valid SQL as the unified closure expands. The final goal still
requires deleting migration dispatch after complete coverage.
