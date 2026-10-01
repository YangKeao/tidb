# Expression unification experiment

Checkpoint-ID: `week-auth-five-38` (previous: `daynumber-four-37`)
**124/245 frozen families delegate with native evaluator algorithms removed; target 221.** Added: WEEK, WEEKOFYEAR, YEARWEEK, PASSWORD and SM3. Strict final-audited acceptance remains **0**; incomplete and not PR-ready.
Paired branches: [TiDB](https://github.com/YangKeao/tidb/tree/expression-unification-demo) · [TiKV](https://github.com/YangKeao/tikv/tree/expression-unification-demo). Use sibling checkouts; parent fills paired commit/Plan hash before publication and owns root Plan mirrors and paired pushes, not force-push or automatic PRs.

## This checkpoint
- **Week demand:** two complete sequential driver calls, never recursive callbacks or host parsing. First worker returns the original owned text only after full date parsing, or NULL; only success demands mode coercion before the second worker. Actual NULL mode means zero. SQL retains the eager default getter even with explicit mode or bad arity; PB observed NULL never reads that getter. Legacy mode-zero raw-core projection keeps its month/day-zero guard, distinct from year_week. Shared i32/i64 week arithmetic preserves const/domain policies; DATE_FORMAT's consumers remain unchanged.
- **Resource order and protocol:** date preparation → first admission/parse → mode coercion → second admission. Zero slots can precede an as-yet-undemanded mode error. Double parsing, owned-text transport, two leases and two one-shot executions without a context capability are explicit costs, not performance completion. Eight operations use three ordinary OwnBytes and five Int results; no new argument role, argument carrier, result kind or driver. `expr_eval` only extends the existing NullWitness operation arm for WeekNullNative; the TimeCoreBits validator is unchanged.
- **Authentication:** an acyclic pure leaf depends only on sha1, not FIPS. Native auth reexports Sm3 and uses four thin functions, preserving the original Sum input-write/state behavior. Existing wire PASSWORD double-hex/lowercase behavior stays distinct, not silently repaired. Native PASSWORD's original 1681 warning precedes coercion, NULL handling and admission. Parser/public-helper closure is included, not just SQL.
- **Scope:** 23 Rust files (TiKV 12/native 11), five manifests and two Cargo-generated lockfiles. Old package versions/sources/checksums are preserved; only the new leaf and approved expression/parser dependency edges are added. TiKV resolves sha1 0.10.6, native 0.10.7. No old expected values, authentication fixtures, hint sources or aggregate test build script were changed.

## Actual validation
| Run | Result | Compile / run seconds |
|---|---|---|
| TiKV Time | 50 passed, 361 filtered | 2.06 / 0.01 |
| Native CoreTime | 15 passed, 423 filtered | 1.95 / 0.00 |
| Shared auth leaf | 2 passed, 0 filtered | 0.67 / 0.00 |
| Parser unit tests | 6 passed, 729 filtered | 3.98 / 0.06 |
| Isolated parser auth_shared | 20 passed, 0 filtered | 0.49 / 0.17 |
| TiKV local evaluator | 263 passed, 1 existing ignored, 496 filtered | 12.55 / 0.19 |
| TiKV time kernels | 62 passed, 698 filtered | 0.12 / 0.01 |
| TiKV encryption | 11 passed, 749 filtered | 0.12 / 0.00 |
| SQL/lifecycle | 80 passed, 2078 filtered | 33.38 / 1.21 |
| Native dispatch | 3 passed, 1559 filtered | 10.76 / 0.00 |
| Native week vectors | 12 passed, 1550 filtered | 0.12 / 0.00 |
| Native crypto vectors | 15 passed, 1547 filtered | 0.12 / 0.00 |
| Legacy week | 2 passed, 205 filtered | 13.66 / 0.00 |
| Full unistore | **193 passed, 1 old failure, 13 ignored; 207 total; exit 101** | 0.13 / 2.98 |
| Full native expression library | **1464 passed, 4 old failures, 94 ignored; 1562 total; exit 101** | 0.12 / 10.79 |

**16 test Cargo attempts, but 15 actual test runs: 13 green + 2 current old full-suite failures; one compilation failure ran no tests.** Two additional successful `cargo metadata --offline --format-version 1` commands are not tests; receipts: `logs/week-auth-{tikv,native}-metadata.json` (tools paths `../tools` and `../../tools`, respectively). No launch failure/retry or new product-test RED.
The aggregate `-p tidb-parser --test all parser_auth_package_source::` attempt (`week-auth-parser-source.log`) failed with three E0061 errors: unchanged parser_hint_source.rs:112/127/351 call parse_hint with three arguments, but unchanged select/hint.rs:748 requires four. Both sources, original auth fixture and aggregate build script equal 16404402. Parent added an auth_shared Cargo test target using that same fixture; its 20 tests passed. This different target is not a launch retry, and **does not restore aggregate all**.
All 23 pinned formatter checks and both diff checks passed first try (formatter failures 0). One static proof assertion failed: whitespace-only SM3 normalization did not remove an extra `///` introduced when rustfmt split the Sum doc line. Normalizing only that documented reflow made the full SM3 region equal; sha1/password helper bodies and wire PASSWORD body are byte-identical. No algorithm fix or new test RED occurred. Static proof path failures 0; one guide-read path error was non-test. Overall checks did **not** all pass first try. Lock comparison permits only the approved additions above, not an unchanged-lockfile claim.
Both complete failure sections match daynumber after thread-ID normalization alone: expression SHA `80be9bda05e5bf630e9c246ca9523d23f82c122eaaeec6e220b1b00cec436615` (duration.rs:212), unistore SHA `e285bfdba646f2d01f85b485ae317cc07c51b78cf0ae6d664c0fb2bc39259759` (source line 194); no address mapping. Historical unistore 189/1/13 is not this checkpoint's 193/1/13.
Three new SQL tests cover stored dates/default changes/NULL mode zero/mode-3 year-zero sentinel and auth NULL/empty/abc with PASSWORD 1681 per row. SM3 empty checks only 64 lowercase-hex shape, not a new golden; original metadata lengths SM3 40/PASSWORD 41 stay unchanged. All 15 zero-slot cases return Resource: three bad dates retain 1292 and three PASSWORD cases retain 1681, the other nine have none. Three dispatch tests cover both facades, but their observer is first-before/last-after, not cumulative; two legacy tests cover raw Time/columns, demand, extras and consumer resources.
Exact commands and boundaries: [summary](logs/week-auth-summary.txt), [evidence](evidence/week-auth-checkpoint.md), `checkpoint.json`.

## Remaining work
Next read-only candidates, **not credited**: MAKEDATE, FROM_DAYS, MICROSECOND and MAKETIME. Of the first three only MICROSECOND has existing native PB/legacy entries; TiKV wire signatures are not native admission. FROM_DAYS needs its heterogeneous ZeroTime/Bytes and exception boundary locked first. MAKETIME's mixed triple role, FSP/unsigned policies and HMS formatting are postponed pending locks; other temporal work and JSON_LENGTH remain deferred.
Legacy capability propagation, operation-scope coverage, allocation/physical peak, paired differential reruns, release/profile gates, prior duration-panic adaptation, aggregate parser all, whole workspace, `make lint`, M6 and TiFlash remain unfinished. No whole-type/package, full parser-suite, performance, FIPS or OOM-safety completion is claimed; JSON_PRETTY retains its separate locks.
