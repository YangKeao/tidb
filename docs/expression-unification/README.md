# Expression unification experiment

Checkpoint-ID: `regexp-worker-45` (previous: `regexp-foundation-44`).
**155/245 functional families, target 221; strict final-audited acceptance 0.** REGEXP_LIKE/SUBSTR/INSTR/REPLACE now delegate to TiKV workers; REGEXP/RLIKE are aliases, not extra credit. Incomplete, not PR-ready.
Paired branches: [TiDB](https://github.com/YangKeao/tidb/tree/expression-unification-demo) · [TiKV](https://github.com/YangKeao/tikv/tree/expression-unification-demo). Each checkpoint includes the Plan; no force-push or automatic PR.

## Shared implementation
- Shared context caches and native compiler/error types live in TiKV `native_regexp.rs`, above the shared regexp policy leaves. Native cache owners reset on Clone; explicit invocation handles share actual state, including miss writeback and memoized compile failures. Initialization remains lazy at original demand points.
- Four typed recipes use real 3/5/6/6 SQL slots. Metadata binds only during an invocation; RAII unbinds and clears observations before pool return, and unwinding cannot heal poison. Only these exact operation/role/kind combinations permit non-unit metadata. The common driver and ordinary PB admission are unchanged.
- Native coercion, collation and error/NULL order remain. Invalid INSTR return option authorizes undemanded flags, not dummy NULLs. Two NULL witnesses, two legacy case policies and one genuine missing-child NoArgs recipe complete nine operations. Legacy available-child evaluation and third-child non-demand are preserved.
- Native evaluator algorithms are deleted. Statistics retain a pure TiKV Option SDK helper, explicitly outside pooled SQL scope guarantees. Typed causes distinguish SQL policy failures from binding, count and resource failures.
- Known holder/cache structures and actual parts/error-buffer capacities are checked. Regex engine/TLS, transient uncached engines, Arc/allocator headers and peaks are not fully measured; M6/OOM/performance remain deferred.

## Validation
| Final gate | Result |
|---|---|
| TiKV regexp / local | 16 passed / 279 passed, 1 ignored |
| Native original regexp / cache / new dispatch | 7 / 1 / 2 passed |
| SQL regexp / legacy regexp | 4 / 2 passed after retries |
| Full expression | **1482 passed, 4 old failures, 94 ignored; exit 101** |
| Full unistore | **197 passed, 1 old failure, 13 ignored; exit 101** |

Twelve actual nonzero-test Cargo runs: seven final focused green; two new-fixture reds; one existing instrumentation mismatch; two final known-baseline non-green full runs. Three failed-target retries, zero compilation/zero-match/launch failures. Not all first-pass.
The new SQL metadata expectation was corrected from the original collation derivation, and the PB fixture from the original Shared-first decoder. One old NOT REGEXP trace now expects two independent workers, while its SQL value stays unchanged. No provider output recording or production change to satisfy test guesses. Complete final failure sections match foundation after numeric thread IDs only; no address remapping.
Twenty-two Rust sources (TiKV10/native12), no manifest/dependency/lock changes. Final pinned formatter/diff checks and three entire original regexp test-module byte proofs pass; one first formatter check required reformatting. Source edit-match failures and a restored test-name typo are disclosed. Fifteen new test functions cover cache/lifecycle, actual worker reuse, policy, SQL metadata and resource/error precedence.
Exact commands, twelve raw-log SHA256 receipts and all corrections: [summary](logs/regexp-worker-summary.txt), [evidence](evidence/regexp-worker-checkpoint.md), `checkpoint.json`.

## Remaining work
Continue shared-type-first arithmetic/JSON batches; FORMAT locale/numeric closure, DATE/MICROSECOND and remaining eligible families are not complete. Production default-NoColumns request-root integration, physical allocation/peak/OOM, 150-row differential, M6, TiFlash, release/profile, whole workspace and lint remain unverified. Historical parser-all E0061 and the full-suite failures remain unresolved. No whole Go-package or final performance completion claim.
