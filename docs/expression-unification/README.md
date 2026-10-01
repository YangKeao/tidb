# Expression unification experiment

Checkpoint-ID: `daynumber-four-37` (previous: `weekday-four-36`)
**119/245 frozen families delegate to TiKV with native evaluator algorithms removed; target 221.** Added: DATEDIFF, TO_DAYS, TO_SECONDS and TIDB_PARSE_TSO_LOGICAL. Strict final-audited acceptance remains **0**; incomplete and not PR-ready.

Paired branches: [TiDB](https://github.com/YangKeao/tidb/tree/expression-unification-demo) · [TiKV](https://github.com/YangKeao/tikv/tree/expression-unification-demo). Use sibling checkouts.
The parent fills the paired TiKV commit and Plan hash in `checkpoint.json` before publication. Root Plans mirror `/home/agent/tidb/EXPRESSION_UNIFICATION_PLAN.md`; publication uses paired pushes, not force-push or automatic PRs.

## This checkpoint

- **Shared algorithms:** TiKV owns complete strict datetime/clock/fraction parsing and day-number arithmetic; native helpers thin-delegate and map the seven-field tuple to the existing private struct. The entire fraction tail is validated before truncation to six characters. One macro preserves original i32/i64 operation widths/order and const behavior; public CoreTime date_diff delegates to the shared raw-core helper, not widened arithmetic followed by truncation. Other parser consumers receive no family credit.
- **Distinct DATEDIFF policies:** SQL retains both ordered text conversions, including right conversion after left NULL, then worker-side calendar parsing/civil subtraction. Existing PB first-observed NULL uses only its witness, without coercing its prefix or reading its suffix. Legacy transports both actual nullable raw cores without clock clearing or Gregorian validation; its former Date/zero-FSP constructor was already always successful. The year-zero witness remains SQL civil result 1 versus legacy day-number result 0.
- **Closed protocol:** six operations return ordinary Int. TimeCoreBits2 is the sole new argument role, carrying two real nullable eight-byte little-endian cores; existing NullWitness rejects Some. The approved `types/expr_eval` changes add one isolated TimeCoreBits2 validator and admit existing NullWitness for MathNullWitnessNative or DateDiffNullNative; IEEE admission is unchanged. No new result, metadata, report, cause, driver, NoArgs case or PB/legacy admission. Scope: 19 Rust files, TiKV 9/native 10.
- **Preparation and precedence:** original ETDatetime/ETInt casts, context warnings, arity and coercion stay native; parsing, NULL/non-positive TSO handling and the logical low-bit result run in workers. No host date answer, physical-TSO/time-zone substitution or production fallback is introduced. Resource refusal precedes worker results; legacy capability coverage remains explicitly incomplete.

## Actual validation

| Run | Result | Compile / run seconds |
|---|---|---|
| TiKV Time | 48 passed, 361 filtered | 1.95 / 0.01 |
| Native CoreTime | 15 passed, 423 filtered | 2.05 / 0.00 |
| TiKV local evaluator | 260 passed, 1 existing ignored, 493 filtered | 9.01 / 0.19 |
| TiKV time kernels | 60 passed, 694 filtered | 0.12 / 0.01 |
| SQL/lifecycle | 77 passed, 2078 filtered | 19.50 / 1.20 |
| Native dispatch | 3 passed, 1556 filtered | 11.77 / 0.00 |
| Native DATEDIFF vectors | 1 passed, 1558 filtered | 0.12 / 0.00 |
| Native serial day/second vectors | 2 passed, 1557 filtered | 0.12 / 0.00 |
| Native logical-TSO vectors | 1 passed, 1558 filtered | 0.13 / 0.00 |
| Legacy DATEDIFF | 2 passed, 203 filtered | 6.87 / 0.00 |
| Full unistore | **191 passed, 1 old failure, 13 ignored; 205 total; exit 101** | 0.12 / 3.01 |
| Full native expression library | **1461 passed, 4 old failures, 94 ignored; 1559 total; exit 101** | 2.78 / 10.56 |

**12 actual test runs / 12 Cargo attempts: 10 green, 2 current baseline non-green; no launch/compilation/new product-test failures or test retries.** Non-test corrections: the proof script initially guessed duration_parse/convert_tz under src instead of src/time_fn (`git show` 128, script 1), then passed after path discovery and rerun, with no source/Cargo change. The first formatter check failed at TiKV batch.rs (exit 1: ToDaysTextNative/TsoLogicalNative return-line wraps), before native/lock/diff checks ran; two whitespace-only wraps fixed it and all 19-source formatting, unchanged-lockfile and diff checks passed. One no-op edit was rejected without changing a file. These are not extra test/Cargo runs; no all-first-pass claim is made.
Complete failure sections match with thread IDs alone: expression versus weekday SHA `80be9bda05e5bf630e9c246ca9523d23f82c122eaaeec6e220b1b00cec436615` (duration.rs:212), unistore versus HMS SHA `e285bfdba646f2d01f85b485ae317cc07c51b78cf0ae6d664c0fb2bc39259759` (unchanged source line 194). Neither needs address mapping. The old unistore 189/1/13 is historical; current is 191/1/13.
SQL covers six rows × four LongLong(20,0)/binary projections and 12 zero-slot Resource/1105 refusals: three bad-text cases retain preceding 1292 warnings, the other nine have none. Three dispatch and two legacy tests cover actual Time/columns, invalid raw fields, ignored clock/extra operands and consumer resource behavior. Two clock bodies match after whitespace/callee-name qualification normalization; strict-datetime parsing preserves its prefix with tuple instead of struct output. Nine old policy bodies and four whole files are byte-identical; old expected values and fixtures are unchanged.
Exact commands and boundaries: [summary](logs/daynumber-summary.txt), [evidence](evidence/daynumber-checkpoint.md), `checkpoint.json`.

## Remaining work

Next read-only candidates, **not credited**: WEEK, WEEKOFYEAR, YEARWEEK, PASSWORD and SM3. Week parsing failure must retain skipped mode reads, default-context access and const/width policies. Authentication work first needs public parser/auth-helper and acyclic shared-leaf ownership locks; migrating only SQL is insufficient. TIMEDIFF/TIMESTAMPDIFF are postponed; JSON_LENGTH remains deferred.
Legacy capability propagation, operation-scope coverage, allocation/physical peak, paired differential reruns, release/profile gates, prior duration-panic test adaptation, whole workspace, `make lint`, M6 and TiFlash remain unfinished. MICROSECOND/JSON_PRETTY retain separate locks. No next-batch, whole-type/package, full datatype/parser-suite, performance or OOM-safety completion is claimed.
