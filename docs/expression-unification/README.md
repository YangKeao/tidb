# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **weight-format-64**, following **date-core-63**.

## Progress

Functional delegation plus native algorithm deletion: **204/245**. Target221 needs17 more;41 eligible families remain. Strict final-audited acceptance stays **0**; overall goal remains active.

WEIGHT_STRING now delegates actual bytes/padding/conditional metadata to fixed workers using existing shared collation keys. Numeric NULL consumes actual type metadata without demanding a skipped typed value. Original AST/typed AS BINARY differences, warning/getter order and raw-byte behavior remain.

FORMAT delegates clamping/rounding/grouping, preserving coercion and locale-warning phases. Public locale APIs are shared too, with one generated Nd authority and existing shared simple lowercase. Native duplicate padding/rounding/grouping/table bodies are removed. No PB/legacy/parser/wire admission expands.

## Evidence and limits

[Evidence](evidence/weight-format-checkpoint.md), [exact commands and hashes](logs/weight-format-summary.txt), [manifest](checkpoint.json), [cumulative ledger](migration-progress.json).

Eight exclusive owners;23 Rust files;four new CPP sources;13 new tests finally pass. Twelve Cargo launches include eight green gates, two unchanged old full-suite REDs, one corrected new expectation RED and one corrected new SQL-test accessor compile failure. Original RED logs remain; production needed no runtime repair and old tests/oracles are unchanged.

CPP datatype1/weight3/native_format3/local315+1ignored; native public locale2/SDK2/FORMAT roots2/SQL2 pass (filters overlap). SQL covers12 weight byte pins,8 format values/warnings and14 direct zero-slot cases. Full expression **1546/4old/94ignored**, unistore **208/1old/13ignored** preserve complete prior failure sections after only thread-ID normalization.

The generator's post-format stale-header failure was repaired in its template, not by hand-editing generated data. Pinned Go1.26.0 and source hashes remain; original64 Nd ranges and711-range graph are exact. One existing shared dependency edge was added without version updates.

Prior JSON_KEYS aggregate mismatch remains unresolved/not rerun. M6, default-NoColumns whole roots, workspace/lint/dev/bazel_prepare, release/performance/zero-copy/physical heap/peak/OOM, concurrent global-mode mutation, exhaustive differential/TiFlash/FIPS and previous parser/GB/vector/Decimal/deep-JSON exceptions remain deferred. No package-transcreation, PR-readiness or allocation-timing/performance-neutrality claim.

Three Plans agree; manifest pins their hash and paired TiKV commit. TiKV publishes first, then TiDB, without force push or PR. Old untracked client-differential BUILD.bazel stays excluded. Next RO preference: ANY_VALUE+NAME_CONST with all19 actual payloads; INTDIV needs a separate multi-policy/SDK closure. No advance credit.
