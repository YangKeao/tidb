# Expression unification experiment

Paired branch: `expression-unification-demo` in YangKeao/tidb and YangKeao/tikv.
Current checkpoint: **date-arithmetic-102**, after **interval-runtime-101**.

Functional **235/245 (95.92%)**, strict **0**, remaining10—unchanged this round. All235 family objects are preserved. Overall goal remains active.

## Ordinary DATE_ADD/SUB takeover

Four SDK profiles now own ordinary calendar and typed-duration arithmetic through `tikv/date_arithmetic.rs`; native duplicate bodies are deleted. The necessary strict interval datatype parser and ParsedInterval carrier are shared too. AST retains historical NoColumns semantics with actual execution authority; typed callers retain their context, cast order and raw duration FSP.

This is partial:48 calculating legacy signatures and CoreTime arithmetic remain pending. Eight original Duration→Datetime refusals stay child-free. No new PB/legacy admission or whole-family credit.

## Evidence

[Checkpoint](evidence/date-arithmetic-checkpoint.md), [commands/counts/hashes](logs/date-arithmetic-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Twelve locked serial launches: ten nonzero passing runs, one retained zero-match filter, one retained bridge-constructor compile failure. Corrected filter and constructor retries pass. Six new tests;205 TiKV/435 native old test bodies unchanged.

New SQL52SELECT covers domains/FSP/warnings/NULL and both vector settings. Refusals are conservatively24 direct Head plus two pre-cast-route probes. Two unchanged SQL tests validate ordinary arithmetic and statement overflow policy.

## Still open

[Remaining acceptance](evidence/remaining-acceptance.md):2core,2ordinary,6complex candidates—not blanket exceptions—plus request-root/default-NoColumns/liveDAG/final acceptance.

Full expression/unistore were not rerun; historical4+1 failures remain. Workspace/lint/dev/bazel/release/exhaustive/TiFlash/FIPS/performance/physical memory/OOM/allocator/dual-tzdata/whole Go-package/PR readiness remain unverified. Manifest pins paired TiKV and three identical Plans. No force push or PR; unrelated BUILD excluded.
