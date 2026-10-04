# Expression unification experiment

Paired branch: `expression-unification-demo` in YangKeao/tidb and YangKeao/tikv.
Current checkpoint: **extract-runtime-98**, after **extract-types-97**.

Functional **232/245 (94.69%) over frozen implemented domains**, strict final count **0**. Only `extract` is added; all231 previous family objects are unchanged. Overall goal remains active.

## EXTRACT runtime takeover

Six SDK profiles now own main signature selection, datetime/duration extraction, staged mixed parsing and distinct broad calendar compatibility. Duplicate native algorithms are removed. Native bridges retain complete source metadata for original casts, two independently demanded mode reads, real NULL witnesses and original error/warning delivery.

Five profiles use Values; Datetime keeps the existing TimeCoreBitsBytes role. No new carrier, PB/legacy admission or specialized vector kernel is added. R100's type services are reused unchanged.

## Evidence

[Checkpoint](evidence/extract-runtime-checkpoint.md), [exact commands/counts/hashes](logs/extract-runtime-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Six core gates pass: CPP core1/local351+1ignored; native entry13/gateway196+1ignored; two SQL gates each1. Five new tests pass first execution. New SQL28SELECT includes14 isolated Select-root zero-slot refusals, real values/metadata and original hard-error warnings; later worker roots have separate direct tests.

Full expression/unistore were not rerun; R100's4+1 failures remain historical. All137CPP/415native old test bodies are unchanged. SevenCPP/eightnative Rust files, two new modules. Formatting, receipt checks and focused source review completed. No Cargo/lock/Go/Bazel edits.

## Still open

[Remaining acceptance](evidence/remaining-acceptance.md):5core,2ordinary,6complex candidates—not blanket exceptions—plus request-root/default-NoColumns/liveDAG/final acceptance and earlier domain gaps.

Workspace/lint/dev/bazel_prepare/release/exhaustive/TiFlash/FIPS/performance/physical memory/OOM/allocator/dual-tzdata/whole Go-package/PR-readiness remain unverified. Manifest pins the paired TiKV commit and three identical Plans. No force push or PR; unrelated untracked BUILD excluded.
