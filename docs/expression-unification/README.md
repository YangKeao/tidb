# Expression unification experiment

Checkpoint-ID: `like-two-48` (previous: `binary-three-47`).
**162/245 functional families, target221; strict final-audited acceptance0.** New families: LIKE/ILIKE. Incomplete, not PR-ready.
Paired branches: [TiDB](https://github.com/YangKeao/tidb/tree/expression-unification-demo) · [TiKV](https://github.com/YangKeao/tikv/tree/expression-unification-demo). Each checkpoint includes the Plan; no force-push or automatic PR.

## Shared implementation
- Existing TiKV wildcard matching stays unchanged. Shared ASCII lowering, alphabetic-escape scanning and const byte-width helpers replace native utility loops; compiled LIKE/ILIKE types and lazy context caches now have a TiKV owner.
- Five closed recipes return actual owned Int/NULL. Three real bytes/bytes/int inputs carry separate typed invocation metadata, not a fake fourth SQL operand. Cache resolution/compilation/matching happens inside the generated wrapper; owner Clone resets and invocation Clone shares the live owner. Known compiled capacities are charged, not opaque headers or temporary allocation peaks.
- AST eager, typed sequential and legacy eager child demands remain distinct. Legacy preserves UTF-8-to-empty, Go simple Unicode lower and Reject trailing escape; modern LIKE retains collation policy and Literal trailing escape, ILIKE stays ASCII-only.
- Scan and SHOW paths forward actual contexts and errors, including SHOW WHERE's scope/execution capability. Public bool/statistics helpers remain pure shared SDK calls, without worker receipts. Wire LikeSig and ordinary PB refusal are unchanged; no new Go vector tier or ILIKE wire signature.

## Validation
| Final gate | Result |
|---|---|
| Shared wildcard / native utility | 5 / 13 passed |
| TiKV LIKE / local | 10 / 287 passed; local1 ignored |
| Native LIKE | 61 passed, 9 ignored |
| SQL / legacy / pushed scan | 2 / 1 / 1 passed |
| NOT LIKE instrumentation | 1 passed; SQL expected NULL unchanged |
| Full expression | **1488 passed, 4 old failures, 94 ignored; exit101** |
| Full unistore | **199 passed, 1 old failure, 13 ignored; exit101** |

15 test-Cargo attempts: 13 nonzero-test runs, two compilation failures, nine final focused green runs. Measured integration RED→GREEN corrected legacy NULL witness selection; a full-run instrumentation RED now expects LIKE then NOT, without changing SQL values. Three recovery retries plus a full-suite confirmation; no zero-match or launch failure. Not all first-pass.
Both final full failure sections equal the published binary checkpoint after numeric panic-thread IDs only. Original wire LIKE test module is byte-identical; original fixtures/SQL expectations remain unchanged. Static source-table/algorithm analysis confirms all 1,112,064 Unicode scalar lower mappings agree with the original native leaf; this is not a Rust runtime or whole Go-package equivalence test.
27 Rust sources (TiKV12/native15), no dependency/manifest/lock changes; pinned formatter/diff checks. Twelve added focused tests.
Exact commands, all15 whole-log hashes and source-tool incidents: [summary](logs/like-two-summary.txt), [evidence](evidence/like-two-checkpoint.md), `checkpoint.json`.

## Remaining work
83 eligible families remain; MOD/DIV are read-only next candidates, IntDIV requires original warning-before-conversion closure. JSON renderer closure, FORMAT/DATE/MICROSECOND, broader request-root integration, physical heap/peak/OOM, differential tests, M6, TiFlash, release, whole workspace and lint remain unverified. Existing GBK/GB18030 ILIKE mapping and ignored Go vector gaps are preserved, not silently repaired. Prior extreme Decimal release-shape exceptions, parser-all E0061 and full-suite failures remain unresolved. No whole Go-package/type-domain or final performance completion claim.
