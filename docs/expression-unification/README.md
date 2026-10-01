# Expression unification experiment

Checkpoint-ID: `weekday-four-36` (previous: `period-format-three-35`)
**115/245 frozen families delegate to TiKV with native evaluator algorithms removed; target 221.** Added: DAYOFWEEK, WEEKDAY, DAYOFYEAR and DAYNAME. Strict final-audited acceptance remains **0**; incomplete and not PR-ready.

Paired branches: [TiDB](https://github.com/YangKeao/tidb/tree/expression-unification-demo) · [TiKV](https://github.com/YangKeao/tikv/tree/expression-unification-demo). Use sibling checkouts.
The parent fills the paired TiKV commit and Plan hash in `checkpoint.json` before publication. Root Plans mirror `/home/agent/tidb/EXPRESSION_UNIFICATION_PLAN.md`; publication uses paired pushes, not force-push or automatic PRs.

## This checkpoint

- **Shared native calendar:** four TiKV Time helpers own wide-i64 forward-civil days, Sunday-first weekday indexing, day-of-year and full weekday names; the full weekday table remains private. Workers parse the complete original native date text, preserving valid year zero and the u32-year domain. Native four-family algorithms, the local DAYS table and unused single_date are deleted. WEEKDAY's shifted-index equivalence is claimed only for fully valid dates.
- **Preserved boundaries:** original ETDatetime casts, context getters/warnings, full string coercion and arity/UTF-8 errors stay before admission. Parsing and NULL results now execute in workers, so resource refusal precedes bad-date NULL. Calendar weekday/day-of-year and full-name leaves share helpers; CoreTime changes only Display and extensions only full-name lookup. Existing weekday/ordinal/day-number policies, abbreviations, chrono/normalization and inverse construction remain outside this migration.
- **Closed protocol:** four operations reuse nullable Bytes→three Int results/one ordinary OwnBytes result. No new role, result kind, metadata, driver, typed diagnostic, NoArgs case, four-column allowance or PB/legacy admission. Scope: 14 Rust files, TiKV 8/native 6. No host precomputed date answer or production fallback remains for these four evaluators.
- **Historical correction:** a MONTHS table still present in native calendar formatting after Round35 is deduplicated now, alongside the full weekday table. The earlier four full-month-table replacements were not proof of repository-wide sole ownership. This correction adds no MONTHNAME or DATE_FORMAT family credit.

## Actual validation

| Run | Result | Compile / run seconds |
|---|---|---|
| TiKV Time | 46 passed, 361 filtered | 2.04 / 0.01 |
| Native CoreTime | 15 passed, 423 filtered | 2.66 / 0.00 |
| TiKV local evaluator | 258 passed, 1 existing ignored, 491 filtered | 8.86 / 0.19 |
| TiKV time kernels | 58 passed, 692 filtered | 0.13 / 0.01 |
| SQL/lifecycle | 75 passed, 2078 filtered | 18.23 / 1.16 |
| Native dispatch, true pipeline | 2 passed, 1554 filtered | 12.12 / 0.00 |
| Native calendar vectors | 1 passed, 1555 filtered | 0.13 / 0.00 |
| Native day-name vectors | 1 passed, 1555 filtered | 0.12 / 0.00 |
| Native formatting vectors | 2 passed, 1554 filtered | 0.14 / 0.00 |
| Full native expression library | **1458 passed, 4 old failures, 94 ignored; 1556 total; exit 101** | 0.12 / 10.39 |

**10 actual test runs / 10 Cargo attempts: 9 green, 1 current baseline non-green.** No new formatter/static-check/launch/compilation/test failure or retry occurred. Pinned formatting for all 14 sources, unchanged-lockfile and diff checks passed their first check this round; the previous checkpoint's formatter failure is not a current event.
The complete expression failure section equals **period-format-expr-full.log** after only thread-ID normalization, SHA-256 `80be9bda05e5bf630e9c246ca9523d23f82c122eaaeec6e220b1b00cec436615`. No address mapping is needed; duration.rs remains at 212:55. Unistore was not rerun: 189/1/13 belongs to historical `hms-three-33`, not the previous period checkpoint or a current gate.
SQL covers six rows × four projections and an explicit year-zero CAST witness, not merely a weekday coinciding with year 2000. All 12 zero-slot calls return Resource/1105/HY000: each of four bad-text cases retains one original 1292 warning; the other eight have none. Two dispatch tests exercise the actual pipeline. Forward-civil relocation matches under whitespace normalization only; nine recorded policy bodies and six whole files are byte-identical. Original calendar MONTHS/WEEKDAYS literal order matches the shared tables; the corrected 1970-epoch comment changes no algorithm. Old expected values and fixtures are unchanged.
Exact commands and boundaries: [summary](logs/weekday-summary.txt), [evidence](evidence/weekday-checkpoint.md), `checkpoint.json`.

## Remaining work

Next read-only candidates, **not credited**: DATEDIFF, TO_DAYS, TO_SECONDS and TIDB_PARSE_TSO_LOGICAL—not physical TSO. DATEDIFF's existing PB/legacy CoreTime policy needs a bridge distinct from SQL text; TO_DAYS/TO_SECONDS require their strict datetime parser and wide day-number policy, not just a civil offset. DAYOFWEEK/DAYOFYEAR passed this grouped functional gate and leave the current deferred list; JSON_LENGTH remains deferred.
Legacy capability propagation, operation-scope coverage, allocation/physical peak, paired differential reruns, release/profile gates, prior duration-panic test adaptation, whole workspace, `make lint`, M6 and TiFlash remain unfinished. MICROSECOND and JSON_PRETTY retain their separate parser/public-helper/PB/legacy and float-format/compact-helper locks. No next-batch, whole-type/package, full datatype/parser-suite, performance or OOM-safety completion is claimed.
