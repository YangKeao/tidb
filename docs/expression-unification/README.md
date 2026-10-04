# Expression unification experiment

Paired branch: `expression-unification-demo` in YangKeao/tidb and YangKeao/tikv.
Current checkpoint: **extrema-policy-99**, after **extract-runtime-98**.

Functional **232/245 (94.69%)**, strict **0**, remaining13 unchanged. All232 family objects are unchanged; this step adds no family or C4 credit.

## Shared extrema policy

SDK `native_extremum_policy.rs` now owns GREATEST/LEAST head/domain selection, numeric winner cursor and post-comparison promotion/scale. `compare2.rs` removes those duplicate native decision bodies and actuates the original casts/comparisons.

Four other reducers and string-as-time conversion remain native. Historical NoColumns numeric comparison and planner metadata remain unchanged. No new profile/carrier or PB/legacy admission. Existing shared primitives already support the next complete staged-runtime step; no extra primitive migration is required.

## Evidence

[Checkpoint](evidence/extrema-policy-checkpoint.md), [commands/counts/hashes](logs/extrema-policy-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Six launches:5green and1new-test failure retained. Final core1/native4/original cross-tier1/new SQL1/original SQL6 pass. The failure was a new typed fixture overlooking existing LongLong return conversion; only that new expectation was corrected from source. Production and old fixtures were not changed for green results.

New SQL4SELECT/18calls pins winner scale, constant scale, signed/unsigned and Real promotion, NULL and metadata across two vector settings. No SQLNaN, zero-slot or new-root claim.189 old native test bodies remain identical. TwoCPP/two native Rust files, one new module.

## Still open

[Remaining acceptance](evidence/remaining-acceptance.md):5core,2ordinary,6complex candidates—not blanket exceptions—plus request-root/default-NoColumns/liveDAG/final acceptance.

Full expression/unistore were not rerun; historical4+1 failures remain. Workspace/lint/dev/bazel/release/exhaustive/TiFlash/FIPS/performance/physical memory/OOM/allocator/dual-tzdata/whole Go-package/PR readiness remain unverified. Manifest pins paired TiKV and three identical Plans. No force push or PR; unrelated BUILD excluded.
