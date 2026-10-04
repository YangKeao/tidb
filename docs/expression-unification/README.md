# Expression unification experiment

Paired branch: `expression-unification-demo` in YangKeao/tidb and YangKeao/tikv.
Current checkpoint: **extract-types-97**, after **json-sum-crc32-96**.

Functional **231/245 (94.29%) over frozen implemented domains**, strict final count **0**. All231 family objects are unchanged; this M0 prerequisite adds no family credit. Overall goal remains active.

## Duration/extraction foundation

SDK `duration/native_parser.rs` now owns the original byte duration parser, full timezone-aware datetime fallback, errors/events, endpoint clamp and raw Time conversion. `time/native_extract.rs` owns unit classification and raw temporal extraction. Native datatype adapters retain original public result shapes, Debug names and source policy; duplicate algorithm bodies are removed.

**EXTRACT runtime is not yet closed.** Its main selector, two lazy mode reads and independent calendar compatibility path remain native. The Plan records the next six-profile contract; no new carrier, worker admission or PB/legacy/vector kernel is claimed here.

## Evidence

[Checkpoint](evidence/extract-types-checkpoint.md), [exact commands/counts/hashes](logs/extract-types-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Nine compiled, nonzero-test launches: seven green, two known RED. CPP datatype464/local350+1ignored; native datatype461; four SQL gates each1 pass. Four new tests pass first execution. New SQL has four stored-field SELECTs across both vector settings, proving type consumers only.

Full expression remains1608pass/4fail/94ignored; unistore220pass/1fail/13ignored. All five failure diagnostics match R98, excluding process ids and shifted source lines. No silent repairs or original oracle changes. FourCPP/fournative Rust files, two new modules;68CPP/235native old test bodies unchanged. Formatting, receipt checks and focused source review completed. No Cargo/lock/Go/Bazel edits.

## Still open

[Remaining acceptance](evidence/remaining-acceptance.md):5core,3ordinary,6complex candidates—not blanket exceptions—plus request-root/default-NoColumns/liveDAG/final acceptance. JSON_SUM_CRC32 SQL ARRAY target conversion also remains baseline-unimplemented; prior helper-domain credit is not full SQL compatibility.

Workspace/lint/dev/bazel_prepare/release/exhaustive/TiFlash/FIPS/performance/physical memory/OOM/allocator/dual-tzdata/whole Go-package/PR-readiness remain unverified. The manifest pins the paired TiKV commit and three identical Plans. No force push or PR; unrelated untracked BUILD excluded.
