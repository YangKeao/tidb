# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **json-search-73**, following **timestamp-add-72**.

## Progress

Functional migration: **215/245**; strict final-audited count **0**. JSON_SEARCH adds one family; target221 needs6 more, with30 eligible families remaining.

TiKV owns path selection, string-leaf walking, ordered global deduplication and formatted JSON-path String output. Native local walk/matcher/output logic is deleted. One Bytes3 profile carries actual document, ordered parsed paths and mode/Unicode escape/pattern; only observed SQLNULL uses the existing NULL recipe. No-hit NULL is computed by the main worker. Original coercion/error order, all-path parsing before one-mode search, no scalar-array autowrap and String metadata remain intact.

The existing shared matcher gains an explicit PrefixLiteral policy preserving native JSON_SEARCH's dangling-escape behavior, including a remaining target suffix. Normal LIKE policies are unchanged; no duplicate matcher, collation getter or new PB/catalog/legacy admission is introduced.

**Retained compatibility limitation:** prior INTDIV raw-empty coefficient lhs divided by1 used to yield exact zero; shared math rejects it (infallible SDK panic, evaluated infrastructure failure). No native fallback conceals it; arbitrary invalid-raw mathematical parity is unclaimed.

## Validation and evidence

[Evidence](evidence/json-search-checkpoint.md), [exact commands/hashes](logs/json-search-summary.txt), [manifest](checkpoint.json), [cumulative ledger](migration-progress.json).

Seven exclusive write owners plus bounded admission review;15 Rust files, one new module,7 added tests. Eight Cargo launches:6 nonzero green,1 retained/resolved new SQL expectation RED and1 unchanged old full-expression failure. No compile failure, interrupted or zero-match run.

CPP pattern6/core-and-wrapper2/local325+1ignored; native JSON_SEARCH4/LIKE61+9ignored and SQLretry2 pass (overlapping filters). SQL covers28 values/NULLs across both modes, String/VarString(-1,-1)/CI metadata and warnings,2 execution errors and8 direct zero-slot roots. The first new test confused nominal path-error3143 with the existing EXEC-tier1105 mapping for column-sourced InvalidPath. Source proves1105/evaluation origin; only that new assertion/comment was corrected, not production. Initial RED is retained; all7 new tests eventually pass.

Full expression **1568/4old/94ignored** retains the identical failure section/list after thread-ID normalization. Unistore unchanged/not rerun. CPP98/native353 original test bodies are byte-identical; pinned formatting and both diff checks pass. No dependency/generated/Go/Bazel edits or fixture recording.

Full temporal parsing/types, RAND state capability, password Unicode/lazy policy, lexer/digest and plan codec/proto migration, and JSON_SUM_CRC32 ARRAY admission remain separate. Whole workspace/lint/dev/bazel_prepare, release/performance/zero-copy/physical heap/peak/OOM, exhaustive differential/TiFlash/FIPS, M6/default-NoColumns and earlier compatibility gaps remain deferred. This is neither whole-package transcreation nor PR readiness.

Three Plans agree; manifest pins their hash and paired TiKV commit. TiKV publishes first, then TiDB, without force push or PR. Old untracked client-differential BUILD.bazel stays excluded. Overall goal continues.
