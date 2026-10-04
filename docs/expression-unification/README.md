# Expression unification experiment

Paired branch: `expression-unification-demo` in YangKeao/tidb and YangKeao/tikv.
Current checkpoint: **json-sum-crc32-96**, after **str-to-date-runtime-95**.

Functional **231/245 (94.29%) over frozen implemented domains**, strict final count **0**. All230 previous family objects are unchanged; only `json_sum_crc32` is added. Overall goal remains active.

## Implemented checksum domain now shared

One SDK worker owns JSON_SUM_CRC32's existing scalar-array classification, numeric spelling, IEEE CRC and wrapping sum. Native duplicate business code is removed; original Datum preparation and error projection remain. Shared datatype formatting retains Rust Display digits and reuses Go-g layout; CRC uses the existing IEEE service.

**This is not SQL ARRAY-target support.** The existing successful entries are helper/manual AST/manual ScalarFunction. Normal SQL `AS type ARRAY` still rejects before its child; no target conversion, registry/PB/legacy admission or SQL checksum success is added. Four SQL probes prove that refusal only.

## Evidence

[Checkpoint](evidence/json-sum-crc32-checkpoint.md), [exact commands/counts/hashes](logs/json-sum-crc32-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Six final core gates pass: CPP datatype1/core1/local350+1ignored, native checksum4/gateway196+1ignored, SQL refusal1. Six new tests finally pass. Retained failures: one new-test index-type compile error and one new-test scope temporary holding a single worker across both assertion operands. Only test construction/lifetime changed; production and value expectations did not.

Full expression/unistore were not rerun; R98's4+1 failures are historical, not current receipts. Scope8CPP/7native Rust files, two new modules;233CPP/374native original test bodies unchanged. Formatting, receipt checks and independent source review pass. No Cargo/lock/Go/Bazel edits.

## Still open

[Remaining acceptance](evidence/remaining-acceptance.md):5core,3ordinary,6complex candidates—not blanket exceptions—plus request-root/default-NoColumns/liveDAG/final acceptance and the explicit baseline-unimplemented SQL ARRAY conversion gap.

Workspace/lint/dev/bazel_prepare/release/exhaustive/TiFlash/FIPS/performance/physical memory/OOM/allocator/dual-tzdata/complete Go-package/PR-readiness remain unverified. The manifest pins the paired TiKV commit and three identical Plans. No force push or PR; unrelated untracked BUILD excluded.
