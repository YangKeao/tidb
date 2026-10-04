# Expression unification experiment

Paired branch: `expression-unification-demo` in YangKeao/tidb and YangKeao/tikv.
Current checkpoint: **interval-runtime-101**, after **extrema-runtime-100**.

Functional **235/245 (95.92%)**, strict **0**, remaining10. Only `interval` is added;234 previous family objects are unchanged. Overall goal remains active.

## INTERVAL takeover

Three SDK profiles own eager and lazy classification/search. `compare2.rs` now delegates through `tikv/interval.rs`; duplicate native loops are removed.

Eager sentinel-before-NULL and complete real conversion remain distinct from lazy metadata-driven child demand. Exact signed/unsigned comparison, NaN predicate differences and original warning/cast context are preserved. Lazy Head now admits before target callback, so resource refusal may precede a child error. No new PB/legacy admission, carrier or vector kernel.

## Evidence

[Checkpoint](evidence/interval-runtime-checkpoint.md), [commands/counts/hashes](logs/interval-runtime-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Six locked serial launches pass on first execution. New SQL32SELECT has16 Head refusals and16 positive results across both vector settings; warning demand changes with real nullability metadata. Old constant-fold/unreachable-warning and cross-tier regressions pass.139CPP/390native old test bodies remain identical.

## Still open

[Remaining acceptance](evidence/remaining-acceptance.md):2core,2ordinary,6complex candidates—not blanket exceptions—plus request-root/default-NoColumns/liveDAG/final acceptance.

Full expression/unistore were not rerun; historical4+1 failures remain. Workspace/lint/dev/bazel/release/exhaustive/TiFlash/FIPS/performance/physical memory/OOM/allocator/dual-tzdata/whole Go-package/PR readiness remain unverified. Manifest pins paired TiKV and three identical Plans. No force push or PR; unrelated BUILD excluded.
