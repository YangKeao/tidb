# Expression unification experiment

Checkpoint-ID: `binary-three-47` (previous: `unary-two-46`).
**160/245 functional families, target221; strict final-audited acceptance0.** New families: plus/minus/mul. Incomplete, not PR-ready.
Paired branches: [TiDB](https://github.com/YangKeao/tidb/tree/expression-unification-demo) · [TiKV](https://github.com/YangKeao/tikv/tree/expression-unification-demo). Each checkpoint includes the Plan; no force-push or automatic PR.

## Shared implementation
- Public native Decimal add/mul and three MySql methods, coefficient add/sub/mul helpers and word projection now use TiKV implementations. Exact, MySql and signed batch-fast policies remain explicit; wire behavior is not silently replaced by native behavior.
- 43 unit recipes cover integer, real, Decimal, vector, genuine NULL/missing, full-i128 legacy profiles and real fast outcomes. Only two input roles and one computed result kind are added; no new driver, runtime, metadata binding or factory bound.
- Fast results distinguish `Unsupported` from actual nullable coefficient/scale values through one TiKV decoder. The batch retains whole-left/whole-right demand and atomic output; unsupported values continue through the ordinary TiKV row route, never a native arithmetic fallback. Nullable integer batch cells also reach a worker.
- Native unsigned multiply, zero-minus-MIN, subtraction mode and legacy quirks remain distinct. Actual typed causes plus recipe/domain and dispatch evidence authenticate SQL errors; bridge/resource failures are not SQL overflow. Vector results reuse the existing serialized LE decoder; copying is not zero-copy.

## Validation
| Final gate | Result |
|---|---|
| Shared datatype / native decimal | 86 / 23 passed |
| TiKV arithmetic / local | 30 / 284 passed; local1 ignored |
| Native arithmetic | 79 passed, 19 ignored |
| SQL / legacy arithmetic | 1 / 1 passed |
| Full expression | **1486 passed, 4 old failures, 94 ignored; exit101** |
| Full unistore | **198 passed, 1 old failure, 13 ignored; exit101** |

16 test-Cargo attempts: 14 nonzero-test runs, two compile failures. Seven final focused green gates plus an earlier green run, four measured non-baseline red runs and two known-baseline non-green full runs. Four recovery retries, one diagnostic replay and one final successful refresh; no zero-match or launch failure. Not all first-pass.
Measured and fixed: Grow multiplication's second carry reduction, missing six closed IEEE admissions, and the integer batch NULL bypass. Compile fixes used the actual existing APIs. No test expected value changed or fixture was recorded. Full failure sections match the previous checkpoint after numeric panic-thread IDs only.
25 Rust sources (TiKV11/native14), no dependency/manifest/lock changes. Final pinned formatter/diff checks and five original test-module byte proofs pass; one formatter-check retry. Fifteen new focused tests.
Exact commands, all16 whole-log hashes and source-tool incidents: [summary](logs/binary-three-summary.txt), [evidence](evidence/binary-three-checkpoint.md), `checkpoint.json`.

## Remaining work
DIV/IntDIV/MOD and 85 remaining eligible families are not migrated. JSON_UNQUOTE/PRETTY renderer closure, FORMAT/DATE/MICROSECOND, request-root integration for default NoColumns, physical heap/peak/OOM, differential tests, M6, TiFlash, release, whole workspace and lint remain unverified. Gigabyte-scale release Decimal scale-wrap shapes rejected by the checked bridge are an explicit compatibility exception, not SQL overflow. Historical parser-all E0061 and full-suite failures remain unresolved. No whole Go-package/type-domain or final performance completion claim.
