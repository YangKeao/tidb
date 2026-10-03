# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **tso-timediff-66**, following **identity-65**.

## Progress

Functional delegation plus native algorithm deletion: **208/245**. Target221 needs13 more;37 eligible families remain. Strict final-audited acceptance stays **0**; overall goal remains active.

TIDB_PARSE_TSO and TIMEDIFF now use two fixed TiKV profiles. Original TSO plus actual conditional offset produces a real Time identity frame; actual nullable TIMEDIFF texts produce the computed difference text. Native calculation/parser/formatter bodies are removed. Public CoreTime bit packing and the existing GoDuration formatter also share their single implementation. No new carrier, driver, result kind, binding or PB/legacy/parser admission.

## Evidence and limits

[Evidence](evidence/tso-timediff-checkpoint.md), [exact commands and hashes](logs/tso-timediff-summary.txt), [manifest](checkpoint.json), [cumulative ledger](migration-progress.json).

Eight exclusive owners;15 Rust files;one new source;12 new tests finally pass. Eleven Cargo launches include eight green gates, one corrected new-test RED and two unchanged old full-suite REDs. No compile failures or runtime production repairs. Original test bodies/oracles remain byte-exact; the first RED log is retained.

CPP datatype1/TSO5/TIMEDIFF7/local317+1ignored; native datatype1/TSO8/TIMEDIFF2/SQL2 pass (filters overlap). SQL pins13 values with original metadata and10 direct zero-slot roots. Full expression **1554/4old/94ignored**, unistore **208/1old/13ignored** preserve complete prior failure sections after only thread-ID normalization.

The corrected test initially used a partial values table instead of the actual temporal dispatcher; only that new call changed, not production or expected values. TSO keeps full raw i32 offsets, positive-only timezone demand and value FSP6 versus SQL metadata FSP0. TIMEDIFF keeps eager children but conditional right coercion, distinct fraction grammars and its original typed Duration post-cast, including NULL timezone demand. TSO frame-encoding errors use the existing RPN Evaluation channel, not the host codec's ResourceLimit category.

Prior JSON_KEYS mismatch and AST/SQL caller gaps remain. M6/default-NoColumns whole roots, workspace/lint/dev/bazel_prepare, release/performance/zero-copy/physical heap/peak/OOM, exhaustive differential/TiFlash/FIPS and previous parser/GB/vector/Decimal exceptions are deferred. Repeated parsing/UTC lookup and framing copies are not claimed performance-neutral. Both manifests/locks/generated tables stay unchanged. No package-transcreation or PR-readiness claim.

Three Plans agree; manifest pins their hash and paired TiKV commit. TiKV publishes first, then TiDB, without force push or PR. Old untracked client-differential BUILD.bazel stays excluded. Next RO: INTDIV can reuse existing exact division but needs four policies and original precision-getter demand; temporal literals need a substantial shared parser/timezone SDK plus fold-time guard, not ready host values. No advance credit.
