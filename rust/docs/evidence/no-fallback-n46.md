# N46: actual selected protobuf binary owners

Baseline TiDB `37c97648`, SDK `ad73b11` unchanged. Continuous task #4 remains open for the public/AST/filter/join/Unistore callers, removal of generic admission fallback, full service correctness and final performance. This receipt covers the selected PB binary family, not the overall goal.

## Original evidence fixed before production edits

Thirteen baseline snapshots are recorded in `n46-before/SHA256SUMS`. Original capture and independent rerun both passed. The actual source RED retained the same six hashes and reproduced admission/fallback. All six count/hash assertions were inserted and archived in `n46-before/pinned-tests.rs` before extracting or integrating production code. The pure extraction then passed all six hashes.

| Original cohort | Observations | Hash |
| --- | ---: | --- |
| Signature/arity/NULL/source/header phases | 6,697,728 | `12FD6A34F8AA79A0` |
| Complete 149-value corpus in either operand | 3,261,312 | `894929E5F5B23495` |
| Long selections and repeated invocation | 26,880 | `B52C473E03962635` |
| Actual physical columns and nested producers | 437,760 | `0FEDC8DF23344ED5` |
| Actual binary literals and independent unsigned flags | 525,312 | `6BA31E029AD1908D` |
| All ten declared source families | 164,160 | `C5CC039A29704005` |
| Total | **11,113,152** | Six immutable assertions |

Every strict comparison checks output, error/panic, context observations and the absence of source refusals/fallback. There are no eligibility skips, resource exceptions, cap changes or rebased hashes in these six cohorts. BinaryLiteral cases use actual `ScalarFunction::from_pb` constructors; the wire decoder has no supported BinaryLiteral tag and none was invented.

## Implementation and preserved boundaries

The original 34 `Kernel::Binary` signatures and four `Kernel::IntegerMod` signatures expose their actual selected operation/domain/NULL/signedness facts. Their original value dispatch, baked MOD operand conversion and left-NULL result boundary are shared. The selected table, unrelated PB branches, ordinary operator algorithms, integer DIV casts and original scalar diagnostic algorithms remain unchanged.

Typed binary owners use the existing numeric continuation, source normalization and diagnostic plans. Metadata captures real operand facts: evaluation type, unsigned flag, constant status and duration-column status. Comparisons do not take the ordinary typed-integer probe. Input-domain selection is separate from diagnostic rendering through the actual display name. No synthetic expression, function name or field type selects a branch.

Binary arity errors occur before any child. Baked integer MOD reads only its original raw operands 0 and 1, casts each with signature-owned signedness, demands RHS even after a left NULL, reports missing arguments at the original position, ignores suffix children, and reads division precision at the original late boundary. Its boxed raw continuation authenticates every held capability before and after callbacks. Typed owners retain the existing consuming numeric controller. Both use original return and actual generic integer-batch presentation boundaries.

The ordinary SDK driver and closed value kernels are reused. No expression evaluator is called from a production host. The SDK remains clean at `ad73b11`.

## Additional regressions and resource policy

Four additional tests cover direct legacy consumers, stale/wrong-role/wrong-node/wrong-row capabilities, original overflow diagnostic re-demands, late precision callbacks, window-generation changes, panic/cancel consumption and recovery. The six strict corpus checks and these four tests pass together (16 tests).

The fixed materialization cap stays **64 KiB**: an 8,193-entry selection refuses without publication, and subsequent NULL/small invocations reuse the same owner. Non-NULL finite refusals are accepted only when the existing return guard independently refuses the real original pre-return PB value under the full same cap. A test-only accessor calls the actual original PB body for this control. For example, PlusInt can yield a Decimal intermediate before final integer conversion; observing only final `ScalarFunction::eval` misses the bounded return formatter. These explicit finite-policy dispositions are not counted as native semantic equivalence and do not alter any strict observer.

The source RED also exposed masked malformed-arity headers and a post-preflight display-name diagnostic walk over children absent from the actual data program. Header precedence and preparation now follow the actual selected body; original differential tests retain the failing witnesses.

The new mixed Projection test covers all 38 signatures, three return declarations, NULL/invalid-byte samples, both vectorization flags, 0/3/2,051 rows and 0/2 workers. Serial warning order is exact. Parallel workers retain the original shared `StmtContext` warning mutex, so warning sequences can interleave across workers; the parallel assertion preserves every level/code/message and multiplicity. Exact per-invocation ordering remains in the strict source observations. Existing Projection tests and production Projection code are unchanged.

## Validation

All jobs reached terminal states. Whole frontend: **2,178 passed, exactly the three inherited failures, 95 ignored**; all **637** batch tests passed and aggregate compiler warnings remain **346**. Projection **49/49** passed. Expand **6/6** plus the unchanged original `64/B42FB8BE316E6C79` observation passed. The existing optional-admission inventory remains `83,088 / 0 refused / 0 names`; it is not a claim that all public callers are unified.

Fresh `make lint`, both pinned rustfmt versions over all 13 Rust files, complete facade/source reconstruction, preservation of original operator/numeric computation spans and witnesses, and the reviewed SHA manifest passed. Evidence checker: `python3 evidence/no-fallback/n46-final-check.py --gates` from the agent workspace. Final logs are `n46-source-fourth.log`, `n46-front-final.log`, `n46-projection-final.log`, `n46-expand-final.log`, `n46-inventory-final.log`, `n46-lint-final.log` and `n46-final-gates.log` under workspace `evidence/no-fallback/`.

Exact inherited frontend failures, neither filtered nor rebased:

- `pushdown_catalog::tests::ifnull_string_column_literal_uses_go_signature_and_column_collation`
- `tests::builtin_info_json_math_source::exp`
- `tests::builtin_math_misc_op_source::vectorized_builtin_op_func`

The covered numeric source families have actual declarations. Absent/malformed PB source metadata and the remaining PB lazy/cast owners still need independent closure evidence alongside the global caller work.

Commands run from the TiDB worktree:

```sh
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib pb_binary_ -- --nocapture
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
make lint
```

SDK validation is reused only because its HEAD and entire worktree are unchanged: N16 full suite 1,148 passed, the exact inherited one failure, one ignored, 17 warnings; clippy 171 total/89 expression warnings. No new SDK claim is made.

No push, PR, release, service deployment or final performance claim is included.
