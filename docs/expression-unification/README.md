# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **unix-timestamp-79**, following **timestamp-78**.

## Progress

Functional migration is **220/245**, strict final-audited count **0**. New family: **unix_timestamp**. All219 prior family objects are unchanged. Target221 needs1 more, with25 eligible families remaining; the overall goal continues.

TiKV `native_unix_timestamp.rs` owns ordinary parsing/epoch/result shaping and strict-Time legacy policies. Six closed profiles cover real clock input, nullable absence, parsing, fresh-zone continuation and legacy Int/Decimal. Ordinary valid civil input demands two distinct actual zone reads; parse failure/all-zero/partial-zero paths stop before the second read. The existing same-scope callback carries the actual SDK base unchanged. Native no longer computes epoch, formats Decimal or reconstructs a parsed base.

Legacy adapters consume typed Time and the original borrowed request zone, never Columns.time_zone. All raw kinds/FSP survive; strict gaps yield zero, valid Decimal has scale6 and zero has scale0. Native PB retains its ordinary route and unchanged outer declared-family coercion; observed NULL now reaches a worker. Existing wire DST/return-field policies remain distinct, sharing only the pure range predicate.

TemporalValue adds a closed unary role using existing zone ownership/accounting. No pool, budget, configuration, resource cause or ordinary signature admission is widened. FROM_UNIXTIME remains unchanged and uncredited.

## Validation and evidence

[Evidence](evidence/unix-timestamp-checkpoint.md), [exact commands/hashes](logs/unix-timestamp-summary.txt), [manifest](checkpoint.json), [cumulative ledger](migration-progress.json).

Ten locked launches: CPPcore2/wire2/local332+1ignored, native Unix8/gateway196+1ignored, legacy2 and SQL1 pass. SQL44 SELECTs cover20normal,20real-column zero-slot refusals,2filters and2controlled clocks; actual decimal scale and declared metadata are distinguished.

One NEW PB test initially omitted unchanged outer return-family conversion. Its two expected cases were corrected against unchanged source, not provider output; retry passes. No production or original test change. Full expression **1578/4old/94ignored** and unistore **212/1old/13ignored** retain exact normalized failure sections and remain RED. No compile failure, interruption or zero-match run.

CPP203/native471 original test bodies are byte-identical.18Rust files, two new modules, ten new tests; pinned format/diff checks pass. Dependencies/locks, Go/Bazel/generated/fixtures unchanged.

M6/default-NoColumns propagation, remaining evaluator closure, old planner mode forwarding/CAST warning/INTDIV raw-empty and other compatibility gaps remain open. Whole workspace/lint/dev/bazel_prepare/release, exhaustive differential/TiFlash/FIPS, performance/allocator/physical heap/peak/OOM/zero-copy/dual-timezone footprint are deferred. No whole-package transcreation or PR-readiness claim.

Three Plans agree; manifest pins their hash and paired TiKV commit. Publish TiKV then TiDB without force push or PR. Old untracked client-differential BUILD.bazel stays excluded.
