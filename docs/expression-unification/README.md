# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **add-sub-time-71**, following **time-microsecond-70**.

## Progress

Functional migration: **213/245**; strict final-audited count **0**. ADDTIME and SUBTIME add two families; target221 needs8 more, with32 eligible families remaining.

The existing shared duration/datetime DTOs now own arithmetic, truncating formatting and predicates; native private DTOs are aliases. Day-number and bounded calendar-year helpers reuse existing cores. TiKV owns all ADDTIME/SUBTIME signature, constant-row and binary-literal policies. Two fixed-sign text profiles and one metadata-only static-NULL profile preserve original coercion/parse/warning order without fabricated NULL inputs or host-computed answers.

Computed reports carry actual text or warning disposition. Native only replays original diagnostics; typed postcasts and context demand remain unchanged. Constant-row selection is not the session vectorized flag. No new PB/catalog/legacy admission, carrier, binding, driver, result kind or factory allowance. TIMESTAMP and TIMESTAMPADD are still uncredited.

**Retained compatibility limitation:** prior INTDIV raw-empty coefficient lhs divided by1 used to yield exact zero; the shared math bridge rejects it (infallible SDK panic, evaluated infrastructure failure). No native fallback conceals it; arbitrary invalid-raw mathematical parity is unclaimed.

## Validation and evidence

[Evidence](evidence/add-sub-time-checkpoint.md), [exact commands/hashes](logs/add-sub-time-summary.txt), [manifest](checkpoint.json), [cumulative ledger](migration-progress.json).

Seven exclusive write owners plus an independent bounded reviewer;13 Rust files, one new core module,7 added tests. Eleven Cargo launches:10 nonzero green and1 unchanged old full-expression failure. No new RED, compile failure, zero-match or retry.

CPP SDK2/core-and-wrappers2/local323+1ignored; native root1/source52+6ignored/captured1/SDK2/calendars22/TIMESTAMP consumers1/SQL2 pass (overlapping filters). All five original ADDTIME/SUBTIME source tables ran. SQL covers32 typed results,4 constant-row control results, metadata/warnings and8 direct zero-slot probes. Full expression **1566/4old/94ignored** retains identical failure section/list after thread-ID normalization. Unistore was unchanged and not rerun.

CPP182/native352 original test bodies and ten neighboring temporal functions are byte-identical; all13 sources pass pinned formatting and both diff checks. No dependency/generated/Go/Bazel edits or fixture recording. Full temporal parsing/type migration, whole workspace/lint/dev/bazel_prepare, release/performance/zero-copy/physical heap/peak/OOM, exhaustive differential/TiFlash/FIPS, M6/default-NoColumns and prior JSON_KEYS/AST/SQL metadata/parser/GB/vector/Decimal gaps remain deferred. This is neither whole-package transcreation nor PR readiness.

Three Plans agree; manifest pins their hash and paired TiKV commit. TiKV publishes first, then TiDB, without force push or PR. Old untracked client-differential BUILD.bazel stays excluded. TIMESTAMPADD has a bounded next-candidate closure; TIMESTAMP still needs broader generic parsing and timezone handling. Overall goal continues.
