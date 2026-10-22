# N47: actual selected PB Boolean and logical owners

Baseline TiDB `df469f81`; SDK `ad73b11` unchanged. Continuous task #4 remains open for remaining PB lazy/cast owners, malformed metadata, actual public/AST/filter/join/Unistore callers, generic fallback removal, service correctness and final performance.

## Independent original evidence

Fifteen baseline files were archived before adding observers. Original capture, independent rerun and the actual source RED agreed on all seven hashes. The RED reproduced admission/fallback without semantic differences. All seven assertions were fixed and archived in `n47-before/pinned-tests.rs` **before production edits**. The pure extraction then preserved every assertion.

| Original cohort | Observations | Hash |
| --- | ---: | --- |
| Arity, values, declarations, display aliases and execution phases | 8,268,480 | `422ED1E676E8C1D7` |
| Full 149-value corpus in each standard argument | 557,856 | `B32D19558ECCC945` |
| Long selections, truth transitions and context reuse | 36,960 | `3437114FD0BF3C6D` |
| Real physical columns and nested PB producers | 126,720 | `5A7FAB439B4E7FBD` |
| Actual binary literals and independent unsigned flags | 152,064 | `3C9D98FD0ED6EFE5` |
| Ten actual source declaration families | 47,520 | `AB6F94138BD915C5` |
| Actual absent argument/return declarations | 561,600 | `7EA0B2B3F1A97B05` |
| Total | **9,751,200** | Seven immutable assertions |

The absent-declaration cohort uses actual selected-PB constructors and constants without wire origins. An actual JSON coalesce caller declares its own output type and observes the untyped child; no type is assigned to that child. All strict observations compare output, errors/panics and context operations and require prepared execution with zero refusal/fallback. There are **no eligibility skips, resource exceptions, changed caps or rebased hashes** in these cohorts.

## Actual contracts and implementation

The eleven selected signatures comprise LogicalAnd/LogicalOr, seven IsNull signatures, UnaryNotInt and IntIsTrueWithNull. Their original body has no general arity header. Unary bodies demand only argument0; logical bodies demand argument1 only when actual left truth requires it. Missing arguments fail at the demanded position, and suffixes are ignored. IsNull observes raw NULL status without truth conversion; the others retain original context-sensitive string truth conversion and warnings.

An immutable `PbBooleanValues` fact exposes the actual selected signature, raw presence/truth policy, demand rule and existing closed value functions. The old PB facade calls these same extracted boundaries. The selected table, unrelated PB bodies, original ordinary truth/logical host bodies, scalar evaluator/native eligibility and operator algorithms remain unchanged.

The new raw capability continuation runs inside the same SDK driver. It authenticates all consumed producer/row/role/window handles, bounds demanded cloning, consumes its phase before context callbacks, audits work and reauthenticates every held token after callbacks. It stores the actual converted left truth, issues only the original Fresh demand and uses the actual `LogicalArgs::UndemandedRight` or `Both` marker. No synthetic right datum, function name, field type or expression evaluator selects a branch. Original result interpretation, return conversion and actual generic integer-batch presentation are preserved.

View-specific data arity retains real legacy unary/arithmetic bulk consumers while raw PB bodies compile only their consumed prefix. A fixed differential witness found optional missing-return bodies displayed as ABS receiving unrelated mathematical diagnostic arguments during preparation. That preparation now follows the actual owner/consumer.

## Additional protocol and caller checks

Four tests cover direct legacy consumers; unchanged64KiB refusal/recovery after an8,193-entry selection; stale/wrong-role/wrong-node/wrong-row capabilities in both logical phases; truncation callbacks that reset the value window or panic; consumed-state retry/cancel recovery; and66 stale declaration/provenance cases.

A direct host check uses a256KiB authenticated value store and a16KiB host work allowance to verify that IsNull observes32KiB non-NULL/invalid-byte payloads without cloning or truth callbacks. This is a host-boundary check, not a claim that an entire source can materialize that input within16KiB. A separate genuine suite with an unsupported expression in an ignored suffix must run without evaluating or compiling that suffix. The suite is constructed after the real expression tree is complete.

The new mixed Projection test covers all11 signatures, three return declarations, NULL/invalid-byte samples, both vectorization flags,0/3/2,051 rows and0/2 workers. Serial warning order is exact; parallel comparison preserves every warning level/code/message and multiplicity under the existing shared warning collector. All49 prior Projection tests and production Projection code remain unchanged.

## Validation

All jobs reached terminal states. Source/protocol **18/18** passed, preserving all seven immutable assertions and all9,751,200 strict observations without exemptions. Whole frontend: **2,196 passed, exactly the three inherited failures,95 ignored**; all **655** batch tests passed and aggregate compiler warnings remain **346**. Projection **50/50** passed. Expand **6/6** plus unchanged `64/B42FB8BE316E6C79` passed. The existing optional-admission inventory remains `83,088 / 0 refused / 0 names`; this is not a global caller-completion claim.

Fresh `make lint`, both pinned rustfmt versions over eight Rust files, complete source/facade reconstruction and preservation of the ordinary truth/logical controllers, previous witnesses and reviewed SHA manifest all passed. Evidence checker: `python3 evidence/no-fallback/n47-final-check.py --gates` from the agent workspace. Logs under workspace `evidence/no-fallback/`: `n47-source-fifth.log`, `n47-front-final.log`, `n47-projection-first.log`, `n47-expand-final.log`, `n47-inventory-final.log`, `n47-lint-final.log`, `n47-final-gates.log`.

Exact inherited frontend failures, neither filtered nor rebased:

- `pushdown_catalog::tests::ifnull_string_column_literal_uses_go_signature_and_column_collation`
- `tests::builtin_info_json_math_source::exp`
- `tests::builtin_math_misc_op_source::vectorized_builtin_op_func`

The optional-declaration cohort covers actual constants, parameters and deferred producers. It does not claim all malformed physical-column metadata or remaining PB lazy/cast bodies are closed.

Commands from the TiDB worktree:

```sh
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib pb_boolean_source_ -- --nocapture
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
make lint
```

SDK validation is reused solely because its entire worktree and HEAD remain unchanged: N16 full suite1,148 passed, exact inherited one failure, one ignored,17 warnings; clippy171 total/89 expression warnings. No service deployment, push, PR, release or final performance claim is included.
