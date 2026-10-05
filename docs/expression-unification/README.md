# Expression unification experiment

Paired branch: `expression-unification-demo` in YangKeao/tidb and YangKeao/tikv.
Current checkpoint: **in-control-106**, after **in-typed-legacy-105**.

Functional **238/245 (97.14%)**, strict **0**, remaining **7**. Only `in` is added; all237 earlier family objects and partial/type histories are retained. Overall goal remains active.

## IN control and existing comparison workers

SDK `native_in_control.rs` now owns AST scalar/row, ready-value, ordinary typed and prepared-cache traversal/reduction. It also builds and probes the actual string cache. Native nodes retain value/getter adapters, original casts, cache storage/invalidation and original comparison/NOT worker calls.

This is an explicit pure SDK control service, not an extra RPN profile or hidden Head. The unchanged AST NOT IN test still observes Eq, Eq, NOT—three facade calls. Empty-list and pure-cache paths keep their original comparison count and admission behavior. R108's typed temporal/JSON and two legacy mappings remain unchanged.

## Evidence

[Checkpoint](evidence/in-control-checkpoint.md), [exact commands/counts/hashes](logs/in-control-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Twelve nonzero green receipts; no failed, interrupted or zero-match run. Three new tests;233 original native test bodies in changed files remain byte-identical. Existing cache length/capacity, prepared rebinding, row and three-facade assertions pass. New SQL has8 direct SELECTs,3 executions of one prepared statement and EXPLAIN. R108 typed SQL and real legacy mappings also pass.

SQL row probes cover rewriting, not AST row dispatch; direct tests cover that control service. Prepared reuse does not prove a physical plan-cache hit. Shared PB still admits no IN signature and retains its recursive UnaryNotInt(IN) refusal.

## Still open

[Remaining acceptance](evidence/remaining-acceptance.md): broader CAST/M2, six complex candidates, general request-root/default-NoColumns/liveDAG and final acceptance.

Full expression/unistore were not rerun; historical4+1 failures remain. Workspace/lint/dev/bazel/release/exhaustive/TiFlash/FIPS/performance/physical memory/OOM/allocator/dual-tzdata/whole-Go-package/PR readiness are unverified. The manifest pins paired TiKV and three identical Plans. No force push or PR; unrelated BUILD excluded.
