# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **intdiv-integer-68**, following **intdiv-sdk-67**.

## Progress

Functional migration remains **208/245**; strict final-audited count **0**. Target221 needs13 more families;37 eligible families remain. Latest whole-family migration: tso-timediff-66.

This partial evaluator step takes over native integer DIV's four signedness combinations and legacy full-width i128 DIV, removes their duplicate quotient bodies, and connects actual scalar/vector NULL paths to the existing worker. Five fixed profiles use existing carriers/results and the existing arithmetic cause, with a distinct IntDivide operation identity. Actual zero divisors reach computation before native warning replay; legacy MIN/-1 still panics, with existing poisoned-worker retirement. Original coercion, getter and child-demand order remain.

**No INTDIV family credit yet:** non-NULL native bounded Decimal, legacy exact Decimal and public exact-div/rem SDK still require closure. Their implementation blocks are byte-exact, not silently replaced by integer policy. No new PB/parser/wire admission, binding, driver, carrier, result kind or cause type.

## Validation and evidence

[Evidence](evidence/intdiv-integer-checkpoint.md), [exact commands/hashes](logs/intdiv-integer-summary.txt), [manifest](checkpoint.json), [cumulative ledger](migration-progress.json).

Eight agents, exclusive files;12 Rust files changed,11 new tests passed on first gates. Eight Cargo launches: six green gates plus two unchanged old full-suite REDs; no compile failure/new RED/Cargo retry/zero-match. CPP integer4/local319+1ignored; native expression5/NULL1/legacy1/SQL2 pass (overlapping filters).

SQL:10 fixed integer results in both modes=20 observations, eight direct zero-slot probes, one warning-before-overflow check. Plain typed columns, no masking wrapper. This proves the integer slice, not Decimal or whole INTDIV. Full expression **1558/4old/94ignored**, unistore **209/1old/13ignored** retain identical failure sections after thread-ID normalization. Original test bodies/oracles remain unchanged. One source-audit endpoint typo was corrected after grep/read; no production or test repair.

No dependency/generated/Go/Bazel changes. Whole workspace/lint/dev/bazel_prepare, release/performance/zero-copy/physical heap/peak/OOM, exhaustive differential/TiFlash/FIPS, M6/default-NoColumns and prior JSON_KEYS/AST/SQL metadata/parser/GB/vector/Decimal gaps remain deferred. This is neither a whole-package transcreation nor a PR-readiness claim.

Three Plans agree; manifest pins their hash and paired TiKV commit. TiKV publishes first, then TiDB, without force push or PR. Old untracked client-differential BUILD.bazel stays excluded. Overall goal continues.
