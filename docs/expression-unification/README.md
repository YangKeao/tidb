# Expression unification experiment

Paired branch: `expression-unification-demo` in YangKeao/tidb and YangKeao/tikv.
Current checkpoint: **extrema-runtime-100**, after **extrema-policy-99**.

Functional **234/245 (95.51%)**, strict **0**, remaining11. Only `greatest`/`least` are added;232 previous family objects are unchanged. Overall goal remains active.

## GREATEST/LEAST takeover

Eight SDK profiles now own all five native domains: numeric, temporal, vector, direct string and string-as-time. Duplicate reducers and temporal-text conversion are removed from `compare2.rs`; `tikv/extremum.rs` performs SDK-requested original preparation and materialization.

Numeric NoColumns semantics retain selected execution authority. Other casts/getters use the original context. Complete actual identities, effective collator mode, per-item mode/timezone order, global NULL, first ties and Decimal scale are preserved. No new carrier, PB/legacy admission or specialized vector kernel.

## Evidence

[Checkpoint](evidence/extrema-runtime-checkpoint.md), [commands/counts/hashes](logs/extrema-runtime-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Ten launches:9green/1retained product-regression run. Native testing caught a vector identity/storage-prefix mix-up missed by new direct fixtures. The consumer and only new fixture preparation were corrected from source; old native tests/oracles were unchanged. All final gates pass.

New SQL28SELECT includes14 isolated Head refusals across both vector settings, all five domains, quiet invalid-time fallback and NULL. Prior four-SELECT policy test and six original SQL tests also pass.151CPP/388native old test bodies remain identical. NineCPP/six native Rust files, two new modules.

## Still open

[Remaining acceptance](evidence/remaining-acceptance.md):3core,2ordinary,6complex candidates—not blanket exceptions—plus request-root/default-NoColumns/liveDAG/final acceptance.

Full expression/unistore were not rerun; historical4+1 failures remain. Workspace/lint/dev/bazel/release/exhaustive/TiFlash/FIPS/performance/physical memory/OOM/allocator/dual-tzdata/whole Go-package/PR readiness remain unverified. Manifest pins paired TiKV and three identical Plans. No force push or PR; unrelated BUILD excluded.
