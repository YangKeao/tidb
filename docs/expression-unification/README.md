# Expression unification experiment

Paired branch: `expression-unification-demo` in YangKeao/tidb and YangKeao/tikv.
Current checkpoint: **in-typed-legacy-105**, after **mydecimal-core-104**.

Functional **237/245 (96.73%)**, strict **0**, remaining **8**—unchanged. This is a partial IN takeover, not whole-family completion. Overall goal remains active.

## Runnable typed and legacy IN

SDK `native_in.rs` owns typed Datetime/Timestamp/Duration/JSON membership after original evaluation/casts, plus the two actual legacy InInt/InString loops. Native `tikv/in_list.rs` actuates requested values and late effective collator resolution; SDK owns matching, NULL results and legacy traversal. Full i128 and distinct error-folding rules are retained.

AST scalar/row, ordinary typed/string-cache and ready-value IN remain pending. The old three-facade NOT IN test is unchanged and passes. Shared PB admits no IN signature; its preexisting recursive refusal of UnaryNotInt(IN) is explicitly pinned, not fixed.

## Evidence

[Checkpoint](evidence/in-typed-legacy-checkpoint.md), [commands/counts/hashes](logs/in-typed-legacy-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Nine launches: seven nonzero passing receipts, one corrected SDK import compile failure, one corrected zero-match gateway filter. Both original logs remain. Five new tests;154 SDK and526 native original test bodies unchanged. SQL adds11 SELECTs plus EXPLAIN, including four domains, scalar/vector execution, JSON distinctions, late warnings and two typed-root refusals.

## Still open

[Remaining acceptance](evidence/remaining-acceptance.md): full IN, CAST/M2, six complex candidates and broader request-root/default-NoColumns/liveDAG/final acceptance.

Full expression/unistore were not rerun; historical4+1 failures remain. Workspace/lint/dev/bazel/release/exhaustive/TiFlash/FIPS/performance/physical memory/OOM/allocator/dual-tzdata/whole-Go-package/PR readiness remain unverified. The manifest pins paired TiKV and three identical Plans. No force push or PR; unrelated BUILD excluded.
