# User-variable effects and ordered outputs in the shared driver

Local checkpoint after TiDB `1ad03fd3`; TiKV remains unchanged at `7df75c9`. This is one coherent step toward the overall no-fallback task, which remains in progress. Nothing is pushed or released.

The original scalar reads and assignments now share `scalar_function/user_variable.rs` with the native source host. The selectors contain immutable names/kinds and actual values, never expression children or evaluator callbacks. `LAST_INSERT_ID(value)` shares the original cast/store/value owner, including its signed return bits and full argument declaration.

The existing `EagerValuesNative` SDK frame selects an immediate stored-name read or demands only argument 1 for a stored-name assignment. A failed stored-name format follows the original common eager body in that same frame. `identity_actual::probe_name` retains the original formatter exactly once; its existing discard wrapper preserves other functions. NULL assignments do not write, unsupported variable kinds retain the original error/NULL behavior, and a raw assignment still precedes a failed or panicking return conversion.

`PreparedBatchSuite` preserves the original `program.vectorizable` scheduling decision. Ordered suites run selected rows, then outputs, using singleton selection views sharing the physical column owners. They execute no bulk Decimal priority stage or vectorization property getter. The caller selection is never changed, including on error/unwind. Shared-view header storage is reserved before dividing the finite materialization allowance among workers. Direct-column transfer remains after successful calculations; selected duplicates continue sharing their output owner. Other suites retain their previous column-major scheduling, including LAST_INSERT_ID-only programs.

## Independent evidence before retirement

The old scalar and old ordered evaluator were still active for capture and independent comparison:

- Scalar values, exact diagnostics/panics and context effects: **57,057 / DF2895EE82065D7E**. Nine variable kinds, nine stored-name headers, nineteen raw value carriers, eleven return declarations and three context profiles, plus LAST_INSERT_ID(value).
- Ordered output/state/getter order: **432 / 695ADB35A928374D**. Nine programs, three contexts, nullable/non-null input, unselected/duplicate/reordered/empty selection, and both vector flags. Context profile 2 changes the flag on writes to verify its original observation point.
- Both observed tuples were frozen and rerun unchanged before candidate comparison.
- **57,057 independent old-scalar/candidate pairs passed before retirement.** All four new source guards still showed actual Admission/fallback at that point. Executed scalar, candidate, tests, evaluator, batch and probe sources were archived before removing the old effects branches.

The selected direct-alias reference uses the original working general row evaluator because the old optimized swap rejects non-nil selections. Its frozen witness records SQL output/state/order. Native alias identity has a separate actual-owner assertion.

Evidence directory in the agent workspace: `evidence/no-fallback/`. The main historical logs are `n12-original-capture-fixed2.log`, `n12-original-rerun.log` and `n12-independent-scalar-pairs.log`; executed sources are `n12-independent-*.rs`. Final helper regression is explicitly labelled shared/frozen rather than a new independent comparison.

## Final validation

`n12-resource-and-final-focus.log`: **11 passed**, including two existing user-variable tests. New checks cover all **57,057 actual source cases with both flags**, duplicate selection `[2,0,2]` and output prefixes, exact state and complete output storage; all **432 ordered source cases**; **22** rejected credentials/indices; both output roles; **128** real getter callback panics with task cleanup; setter panic, generation change, consumed replies, NULL non-write and recovery. Callbacks exclusively borrow the value store, proving no storage Ref crosses them. One-megabyte values fail a 64-KiB worker policy as terminal Resource errors and recover on the same worker with NULL. An ordered setter panic preserves input selection and effects; the same source recovers and selected direct aliases share their owner.

The scalar source fixture initially decoded permissive original return storage as fixed-width cells and panicked outside protected execution. It now compares whole Column storage, matching the established session-read test. No production behavior was changed to accommodate that fixture error.

`n12-front-full.log`: **1,831 passed, exactly the same three failures, 95 ignored; all 320 batch tests passed**. The warning-title multiset remains exactly **346**. Existing failures are:

- `pushdown_catalog::tests::ifnull_string_column_literal_uses_go_signature_and_column_collation`
- `tests::builtin_info_json_math_source::exp`
- `tests::builtin_math_misc_op_source::vectorized_builtin_op_func`

`n12-projection.log`: **26 passed**. The appended heterogeneous suite includes variable assignment/read/nested increment/read, LAST_INSERT_ID writer/getter and CHAR parent, plus a direct column, with both vector flags and worker counts 0/2. Actual final variable and identifier state also match. Previous suites, including the session-read and clock configurations, remain unchanged.

`n12-lint.log`: `make lint` passed. `n12-final-check.py` passes exact original witness/retirement/shared-owner reconstruction, unchanged eager algorithm aside from the proven ID selector, inherited failure/warning sets, unchanged fixtures/clock policy, clean SDK, both pinned formatters on eleven Rust files, diff whitespace and unrelated BUILD exclusion.

Commands, from `/home/agent/tidb/expression-unification/tidb`:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib user_variable_
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
    make lint

The original capture and pre-retirement comparison used the same focused command with `-- --nocapture`. The final checker runs from the agent workspace: `python3 evidence/no-fallback/n12-final-check.py`. Both pinned rustfmt toolchains (`nightly-2026-01-30`, `nightly-2026-08-22`) use `--check --edition 2021 --config skip_children=true`.

The SDK did not change. Its N9 full result (1,141 passed, one inherited failure, one ignored) and clippy baseline (89 expression/171 total warnings) remain the applicable SDK evidence; they were not rerun or reclassified here. No new Go files, Bazel changes or module dependency changes require bazel_prepare.

## Remaining overall work

Early typed string and cached functions, other stateful functions, deferred/parameter/correlated and PB/type seams, public AST/unistore/filter consumers and the generic Admission-to-old-evaluator route still need closure. This checkpoint does not establish their support or overall completion. Continue directly with the next original-demand gap and preserve independent observations before retiring its old body.
