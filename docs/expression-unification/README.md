# Expression unification experiment

Checkpoint-ID: `unary-two-46` (previous: `regexp-worker-45`).
**157/245 functional families, target 221; strict final-audited acceptance 0.** New families: unaryplus/unaryminus. Incomplete, not PR-ready.
Paired branches: [TiDB](https://github.com/YangKeao/tidb/tree/expression-unification-demo) · [TiKV](https://github.com/YangKeao/tikv/tree/expression-unification-demo). Each checkpoint includes the Plan; no force-push or automatic PR.

## Shared implementation
- Shared datatype `NativeDecimalOp::Negate` calls the existing wire `Neg` leaf and native finish; native public `Decimal::negate()` is a thin bridge. Native wide scales, canonical zero and original shape clearing remain distinct from wire raw-zero policy.
- Eleven unit recipes reuse existing scalar/IEEE/Decimal-with-budget/NULL roles. No new carrier, metadata binding, driver or factory limit. Existing finite-budget and typed-cause helpers are shared, not copied.
- TiKV owns checked integer negation and constant promotion. Constant recipes return a real Decimal with the existing worker-computed checked integer view; column overflow uses an authenticated actual typed cause. Native code does not negate or test magnitude thresholds, and resource failures are not SQL overflow.
- Runtime plus copies actual worker inputs while preserving original other-to-decimal coercion. SQL's plus-elision rule is unchanged. UInt, Float32(f64), String collation and plus-Decimal declared column shape are original packing metadata, not a bypass returning the original operand. Coercion, warnings, child demand, NOT/BitNeg and PB/legacy admission remain unchanged.

## Validation
| Final gate | Result |
|---|---|
| Shared datatype / native decimal | 1 / 22 passed |
| TiKV unary / local | 17 / 281 passed, local 1 ignored |
| Native unary / SQL unary | 19 / 1 passed |
| Full expression | **1483 passed, 4 old failures, 94 ignored; exit101** |
| Full unistore | **197 passed, 1 old failure, 13 ignored; exit101** |

Eleven test-Cargo attempts: nine nonzero-test runs and two test-compilation failures. Six final focused green, one existing instrumentation RED and two final known-baseline non-green full runs; three failed-target retries. No zero-match or launch failure. Not all first-pass.
Compilation fixes added an existing trait import and selected the real warning slice API, retaining expected values. The old `FROM_DAYS(-140)` AST trace now expects separate unary-minus/FROM_DAYS workers; its zero-Date value is unchanged. Complete final failure sections match the previous checkpoint after numeric thread IDs only, without address mapping.
Fifteen Rust sources (TiKV9/native6), no manifest/dependency/lock changes. Final pinned formatter/diff checks and five entire original test-module byte proofs pass. Nine new test functions; no fixture recording or production changes to satisfy test guesses.
Exact commands, all eleven raw-log SHA256 receipts and source-tool incidents: [summary](logs/unary-two-summary.txt), [evidence](evidence/unary-two-checkpoint.md), `checkpoint.json`.

## Remaining work
Continue binary arithmetic or the documented complete JSON_UNQUOTE/PRETTY renderer closure; remaining families are not migrated. FORMAT/DATE/MICROSECOND, production default-NoColumns request-root integration, physical allocation/peak/OOM, differential testing, M6, TiFlash, release/profile, whole workspace and lint remain unverified. Historical parser-all E0061 and full-suite failures remain unresolved. No whole Go-package/type-domain or final performance completion claim.
