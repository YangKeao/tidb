# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **clock-three-62**, following **json-merge-pair-61**.

## Progress

Functional delegation plus native algorithm deletion: **201/245**. Target221 needs20 more;44 eligible families remain. Strict final-audited acceptance stays **0**; overall goal remains active.

NOW (including timestamp/localtime aliases), CURDATE/CURRENT_DATE and SYSDATE now use three fixed actual-clock workers. NOW truncates; live SYSDATE rounds and uses its captured instant plus the frozen statement offset. Its true flag shares NOW preparation without nested guards. Native offset/formatting work is removed.

The distinct public GetTimeValue raw-sentinel helpers also share their calendar/truncation/date projection through `native_typed_clock.rs`. Their getter order, markers, non-sentinels and pure SDK surface remain unchanged. Native Time/TimeType/SessionTimeZone are not wire aliases: the unchanged native checked constructor is the final representation/bit-width codec. Date clearing still follows complete time validation.

## Evidence and limits

[Evidence](evidence/clock-three-checkpoint.md), [exact commands and hashes](logs/clock-three-summary.txt), [manifest](checkpoint.json), [cumulative ledger](migration-progress.json).

Eight exclusive writers;15 Rust files;one new source;14 permanent tests pass. Ten Cargo runs:seven green clock gates, two known full-suite RED runs and one failed exploratory CRC32 SQL probe. No compile failure, interruption, zero-match or retry. No clock production/test repair after its first gate.

TiKV clock13/local311+1ignored; native clock19/helper5/sysdate7; SQL2+original1 pass. SQL includes14 fixed results and13 direct zero-slot probes. Full expression **1536/4old/94ignored**, unistore **207/1old/13ignored** retain complete prior failure sections after only thread-ID normalization. Original oracles are unchanged; prior extra JSON_KEYS aggregate mismatch remains unresolved and was not rerun.

Quoted CRC32 SQL was rejected as a UDF (`FunctionNotExists`); the temporary test alone was removed and its RED receipt retained. No CRC32 production change, admission expansion or credit.

M6, broader default-NoColumns roots, workspace/lint/dev/bazel_prepare, release/performance/zero-copy/physical heap/peak/OOM, exhaustive differential/TiFlash/FIPS and prior parser/GB/vector/Decimal/deep-JSON exceptions remain deferred. No package-transcreation, PR-readiness or performance-neutrality claim.

Both tracked Plans equal the root Plan; the manifest pins their hash and exact paired TiKV commit. TiKV publishes first, then TiDB; no force push or automatic PR. The old untracked client-differential BUILD.bazel stays excluded. Next read-only candidates: DATE, paired TIME/MICROSECOND parser closure, and literal-time families with separate text-parser prerequisites; none receives advance credit.
