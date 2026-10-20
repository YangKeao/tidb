# Generic live context values in the shared frame

Local work after TiDB `4e9248b1`; SDK unchanged at `7df75c9`. The overall no-fallback task remains in progress. No push or release.

`context_value.rs` holds parameter order, deferred-conversion facts, optional original declaration and a live correlated binding. Its metadata contains no executable expression or callback. Ordinary Constant conversion and CorrelatedColumn reads share the original pure owners after independent comparison. The existing EagerValuesNative frame handles immediate context reads or one genuinely demanded deferred child. Parameter markers still win over deferred children, while deferred presence still controls conversion.

The source records a real context boundary at its original constant/correlated occurrence, without inventing an SQL function or PB signature. Deferred capabilities are authenticated, consumed before callbacks and checked again afterward. Context callbacks hold no NativeValueStore borrow. Copies, retained root state and output are bounded. No nested evaluator or SDK driver is invoked.

## Original evidence

Five original corpora were captured, frozen and rerun before retirement:

| Corpus | Cases | Hash |
| --- | ---: | --- |
| Values, saved/error Datums, callbacks | 16,416 | E7DC6852B12EF799 |
| Projection phases and live bindings | 640 | 702A47B62B6F8FC5 |
| Empty-row error precedence | 48 | F3F48F4224A0ECED |
| 2,050 selected occurrences | 40 | 9EFE344B4321EE4F |
| Hybrid generic/root consumer differences | 1,824 | 53D70540CA7421FF |

Before helper retirement, **16,416 independent value pairs** and **1,368 additional nonliteral hybrid pairs** passed against the actual old helpers with separate context and binding owners. All nine actual source Admission gaps still reproduced. The original hybrid corpus also reproduced an existing literal-root enum/set mismatch before its fix. Executed original and candidate sources are archived in the agent workspace as `evidence/no-fallback/n14-{before,independent,hybrid-independent}-*.rs`. Logs include `n14-independent-values.log`, `n14-original-hybrid-frozen-rerun.log` and `n14-hybrid-independent.log`.

Final shared-owner witnesses retain the exact frozen tuples; they do not claim independence after helper retirement.

## Consumer phases and reproduced corrections

Unordered root Constants without deferred expressions preserve raw Enum/Set values and read a parameter once across the whole selected chunk, including all driver windows. A bounded per-invocation cache resets on every execution. Generic expression consumers still apply the integer view. Ordered suites read root parameters per row, as before. Generic deferred/correlated results publish per row, preserving effects before a malformed output layout panics.

For deferred roots whose final scalar cannot use an original typed batch path, compiled facts retain the original parameter type probe after the vector flag read and before rows, even for empty input. Typed dynamic roots and mathematical diagnostics that re-read constants remain explicit Admission gaps pending their distinct phase implementation.

Expanded actual-source tests caught the shared scalar publication helper demanding ordinary scalar declaration metadata from ContextValue results. Context nodes now publish their own already-finished consumer view, preserving raw root shortcuts. The initial failure is recorded in `n14-expanded-actual.log`.

A finite Decimal scale-growth fixture reproduced unbounded work under a 64 KiB scope in `n14-actual-hosts-and-work-red2.log`. `Decimal::try_round_to_scale` now exposes the same shared native rounding kernel with its existing finite allowance; ordinary unbounded calls retain the same kernel and panic behavior. Deferred conversion uses that allowance only within a value-work scope and preserves a terminal Resource cause. Real parameter/correlated megabyte values and demanded scale growth all reject without fallback; the same prepared worker subsequently accepts NULL.

## Validation

`n14-context-final.log`: **14 passed**, including actual declared **15,048 x two flags**, hybrid **1,824 x two flags**, and **544** generic phase/long-selection cases. Both vector flags reuse the canonical owner across callback/output panics while each legacy reference is fresh. Complete output-column storage, exact callback/state snapshots, duplicate selections and output prefixes match. The phase matrix explicitly excludes the two still-unproved typed/optional programs rather than counting their fallback as native.

Direct host checks cover **22** invalid capabilities, foreign/stale generations, role/node/occurrence/physical-row mismatch, wire NULL, wrong demand index, **128** immediate callback panics, deferred conversion panic/generation mutation, consumed-task replay rejection and recovery. The immediate guard and finite work scope are released after unwinds.

Final gates are terminal. `n14-front-full.log`: **1,853 passed, exactly the prior three failures, 95 ignored; all 333 batch tests passed**, with the exact prior **346** warning-title multiset. The inherited failures remain `ifnull_string_column_literal_uses_go_signature_and_column_collation`, `builtin_info_json_math_source::exp` and `builtin_math_misc_op_source::vectorized_builtin_op_func`.

`n14-projection.log`: **26 passed**, including two appended heterogeneous parameter/deferred-column/correlated cohorts under unordered and ordered assignment consumers, both vector flags and workers 0/2. Prior configurations and their budgets remain intact. `n14-datatype-decimal.log`: **39 passed**. `n14-lint.log`: `make lint` passed. The final checker verifies original archives, independent tuples, actual-source regressions, exact prior failures/warnings, shared helper preservation except the finite bound, unchanged public scalar/expression/suite owners, unchanged SDK/fixtures and both pinned formatters on thirteen Rust files.

Commands from the TiDB worktree:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib dynamic_leaf_ -- --nocapture
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-datatype --lib decimal::
    make lint

The agent workspace checker is `python3 evidence/no-fallback/n14-final-check.py`. SDK full/clippy baseline is reused only because SDK sources remain unchanged: N9 1,141 passed/one inherited failure/one ignored, clippy89 expression/171 total warnings. Default clock and existing fixtures are unchanged. The unrelated untracked differential BUILD remains excluded.

## Remaining overall work

Typed dynamic operand/root admission, diagnostic re-demand, optional declarations/PB, FIND_IN_SET serialized initialization and empty-row misses, remaining stateful functions and public AST/unistore/filter/Admission fallback retirement remain open. Continue directly after this local checkpoint; it does not establish overall completion.
