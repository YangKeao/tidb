# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **intdiv-decimal-69**, following **intdiv-integer-68**.

## Progress

Functional migration: **209/245**; strict final-audited count **0**. INTDIV adds one family; target221 needs12 more, with36 eligible families remaining.

Native bounded Decimal and legacy exact Decimal DIV now use dedicated TiKV workers; public exact div/rem reuses the existing shared division core. This closes the remaining evaluator policies after the previous integer and SDK checkpoints. Native captures actual0/1/2 precision reads and replays a computed warning-before-integer-outcome report, without quotient/truncation/conversion algorithms. Legacy raw frames preserve zero RHS before invalid LHS, returning a nullable signed integer. Three fixed profiles reuse carriers and finite budgets; no new binding, driver, result kind or wire admission.

**Explicit compatibility limitation:** raw-empty coefficient lhs divided by1 previously yielded exact zero; the shared math bridge rejects it (infallible SDK panic, evaluated infrastructure failure). Arbitrary invalid-raw mathematical parity is not claimed. Zero RHS before invalid LHS remains preserved and tested; no native fallback hides this gap.

## Validation and evidence

[Evidence](evidence/intdiv-decimal-checkpoint.md), [exact commands/hashes](logs/intdiv-decimal-summary.txt), [manifest](checkpoint.json), [cumulative ledger](migration-progress.json).

Eight agents;17 Rust files, one new core module,16 added tests ultimately pass. Fourteen Cargo launches:9 green,2 retained new REDs,1 compile failure and2 unchanged old full-suite REDs. Corrections: new-test storage-versus-display expectation, missing type qualification, and the native factory's six-node allowance for two new five-operand profiles. The latter failed existing/new tests before the production fix and passed afterward with unchanged oracles.

CPP SDK2/core2/wrappers2/local321+1ignored; native datatype99/expression4/legacy1/Decimal SQL2/prior integer SQL2 pass (overlapping filters). Decimal SQL:8 fixed results in both modes=16 observations and8 direct zero-slot probes. Full expression **1561/4old/94ignored**, unistore **210/1old/13ignored** retain identical failure sections/list after thread-ID normalization. Old test bodies/oracles are unchanged; all17 sources pass pinned formatting and both diff checks.

No dependency/generated/Go/Bazel changes. Whole workspace/lint/dev/bazel_prepare, release/performance/zero-copy/physical heap/peak/OOM, exhaustive differential/TiFlash/FIPS, M6/default-NoColumns and prior JSON_KEYS/AST/SQL metadata/parser/GB/vector/Decimal gaps remain deferred. This is neither a whole-package transcreation nor a PR-readiness claim.

Three Plans agree; manifest pins their hash and paired TiKV commit. TiKV publishes first, then TiDB, without force push or PR. Old untracked client-differential BUILD.bazel stays excluded. Overall goal continues.
