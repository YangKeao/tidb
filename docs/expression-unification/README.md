# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **time-microsecond-70**, following **intdiv-decimal-69**.

## Progress

Functional migration: **211/245**; strict final-audited count **0**. TIME and MICROSECOND add two families; target221 needs10 more, with34 eligible families remaining.

The original duration grammar and wide/compact UTC datetime fallback now live in TiKV. Public compact parsing, non-Timestamp validation and exact byte-fraction parsing share their implementations; native duplicate bodies are removed. Native FspError aliases the shared type with unchanged variants/data/Display. The different wire fraction policy is untouched.

Three fixed unary workers preserve native TIME's computed text plus warning status, native MICROSECOND's silent parse-failure NULL, and legacy MICROSECOND's full raw-i64 nanosecond projection. PB context/observed-NULL demand, true-NULL roots, typed Duration postcast and original SQL FSP metadata remain intact. CastTimeAsDuration is not SQL TIME and is unchanged. No new carrier, binding, driver, result kind or admission.

**Retained compatibility limitation:** prior INTDIV raw-empty coefficient lhs divided by1 used to yield exact zero; the shared math bridge rejects it (infallible SDK panic, evaluated infrastructure failure). No native fallback conceals it, and arbitrary invalid-raw mathematical parity is unclaimed.

## Validation and evidence

[Evidence](evidence/time-microsecond-checkpoint.md), [exact commands/hashes](logs/time-microsecond-summary.txt), [manifest](checkpoint.json), [cumulative ledger](migration-progress.json).

Eight agents;21 Rust files, two new parser modules,13 added tests. Sixteen Cargo launches:13 nonzero green,2 unchanged old full-suite failures and1 retained zero-match SQL target mistake—not a pass. Correcting only `--test all` to `--lib` ran both lifecycle tests successfully.

CPP datatype1/parser3/workers2/local322+1ignored; native datatype time90/FSP14/expression7/parser1/calendar22/warning1/values1/legacy1/SQL2 pass (overlapping filters). SQL covers32 function values across8 inputs and two modes, plus8 direct zero-slot probes. Full expression **1565/4old/94ignored**, unistore **211/1old/13ignored** retain identical failure sections/list after thread-ID normalization. CPP228/native512 original test bodies and the native Timestamp validation branch are byte-identical; all21 sources pass pinned formatting and both diff checks.

No dependency/generated/Go/Bazel changes or fixture recording. Full temporal parsing/type migration, whole workspace/lint/dev/bazel_prepare, release/performance/zero-copy/physical heap/peak/OOM, exhaustive differential/TiFlash/FIPS, M6/default-NoColumns and prior JSON_KEYS/AST/SQL metadata/parser/GB/vector/Decimal gaps remain deferred. This is neither a whole-package transcreation nor a PR-readiness claim.

Three Plans agree; manifest pins their hash and paired TiKV commit. TiKV publishes first, then TiDB, without force push or PR. Old untracked client-differential BUILD.bazel stays excluded. Next bounded temporal candidates may reuse this parser foundation; they receive no credit before their own evaluator closure. Overall goal continues.
