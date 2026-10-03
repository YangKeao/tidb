# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **identity-65**, following **weight-format-64**.

## Progress

Functional delegation plus native algorithm deletion: **206/245**. Target221 needs15 more;39 eligible families remain. Strict final-audited acceptance stays **0**; overall goal remains active.

ANY_VALUE and NAME_CONST now reuse TiKV's unchanged nullable byte-identity leaf through two fixed profiles. All19 Datum kinds reconstruct actual worker-returned payload and metadata, without an original-value cache. Native answer clones are deleted. A single shared representation codec and narrow raw Time/Decimal constructors preserve reserved bits, raw FSP/coefficient/shape, Float32's f64 bits, raw JSON, arbitrary bytes and vector bits/dimensions. No new VM, driver, carrier, result kind, binding or PB/legacy/parser admission.

## Evidence and limits

[Evidence](evidence/identity-checkpoint.md), [exact commands and hashes](logs/identity-summary.txt), [manifest](checkpoint.json), [cumulative ledger](migration-progress.json).

Eight exclusive owners;16 Rust files;two new sources;11 new tests finally pass. Eleven Cargo launches include six green gates, three corrected new-test REDs and two unchanged old full-suite REDs. No compile failures or runtime production repairs. Original test bodies/oracles remain byte-exact; RED logs retained.

CPP identity12/local316+1ignored; native raw Time1/raw Decimal1/identity19+1ignored/SQL2 pass (filters overlap). SDK covers19 kinds+four edge fixtures through both profiles and46 zero-slot roots. SQL pins14 ANY_VALUE and six NAME_CONST values, metadata/labels, three1210 cases and16 direct zero-slot roots. Full expression **1550/4old/94ignored**, unistore **208/1old/13ignored** preserve complete prior failure sections after only thread-ID normalization.

The three corrected assumptions are documented, not silently repaired in production: FSP7 is clamped to6 by the old constructor; old AST uppercase arity lookup misses its lowercase registry; old SQL post-derivation overwrites copied string metadata with connection collation, and chunk materialization attaches Decimal shape. SDK metadata identity is **not** a claim of Go SQL whole-FieldType parity. All values/cases remain; only new source-backed assertions/fields/comments changed.

Prior JSON_KEYS mismatch and the newly documented caller gaps remain. M6/default-NoColumns whole roots, workspace/lint/dev/bazel_prepare, release/performance/zero-copy/physical heap/peak/OOM, exhaustive differential/TiFlash/FIPS and previous parser/GB/vector/Decimal/deep-JSON exceptions are deferred. Both manifests/locks/generated tables stay unchanged. No package-transcreation or PR-readiness claim.

Three Plans agree; manifest pins their hash and paired TiKV commit. TiKV publishes first, then TiDB, without force push or PR. Old untracked client-differential BUILD.bazel stays excluded. Next RO preferences: smaller TIDB_PARSE_TSO/TIMEDIFF closures, then INTDIV's four policies; TIME/MICROSECOND require parser prerequisites, plan decoders/digest substantial SDK closure. No advance credit.
