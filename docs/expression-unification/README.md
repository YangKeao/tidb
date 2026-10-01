# Expression unification experiment

Checkpoint-ID: `period-format-three-35` (previous: `month-seconds-two-34`)
**111/245 frozen families delegate to TiKV with native evaluator algorithms removed; target 221.** Added: PERIOD_ADD, PERIOD_DIFF and GET_FORMAT. Strict final-audited acceptance remains **0**; incomplete and not PR-ready.

Paired branches: [TiDB](https://github.com/YangKeao/tidb/tree/expression-unification-demo) · [TiKV](https://github.com/YangKeao/tikv/tree/expression-unification-demo). Use sibling checkouts.
The parent records the paired TiKV commit and Plan hash in `checkpoint.json` before publication. Root Plans mirror `/home/agent/tidb/EXPRESSION_UNIFICATION_PLAN.md`; publication uses paired pushes, not force-push or automatic PRs.

## This checkpoint

- **Period ownership:** TiKV Time owns period validation, wrapping forward/inverse conversion and the GET_FORMAT table; native algorithms are deleted. PERIOD_ADD/DIFF retain the original `int_arg` domain and left-to-right conversions: a left NULL still demands right conversion, either observed NULL suppresses period validation, and conversion errors precede admission. Valid periods have no invented seven-digit limit; signed/unsigned wrapping and the year pivot remain unchanged.
- **Typed diagnostics:** `PeriodAddIncorrectArguments` and `PeriodDiffIncorrectArguments` have exact paired typed causes/receipts. Only authenticated failures restore the original `EvalError::IncorrectArguments` messages and SQL 1210; the host does not validate periods. Resource refusal now precedes worker-side invalid-period diagnostics, while original arity/coercion errors remain earlier.
- **GET_FORMAT demand:** the values path retains raw bytes, exact uppercase selector matching and ASCII-case-insensitive location matching; when both arguments are non-NULL, unknown combinations return non-NULL empty bytes. Only actual first-argument NULL selects the single-Bytes witness, without reading/coercing location or fabricating a second NULL. Otherwise both actual optional bytes are sent, including for unknown selectors. The independent AST evaluates location once, retains strict `coerce_str`, and transports the real DATE/TIME/DATETIME selector—not a host-computed format.
- **Closed protocol:** four operations for three families reuse Int2→Int, Bytes2→Bytes and actual-NULL Bytes→Bytes; the witness rejects Some. All NULL paths invoke workers. No new role, result kind, computed metadata, module, driver, NoArgs allowance, four-column allowance or PB/legacy admission. Scope: 13 Rust files, TiKV 8/native 5; no new flags, panic policy or profile changes.

## Actual validation

| Run | Result | Compile / run seconds |
|---|---|---|
| TiKV Time | 45 passed, 361 filtered | 2.19 / 0.01 |
| TiKV local evaluator | 257 passed, 1 existing ignored, 489 filtered | 9.42 / 0.19 |
| TiKV time kernels | 56 passed, 691 filtered | 0.12 / 0.01 |
| SQL/lifecycle | 73 passed, 2078 filtered | 22.20 / 1.14 |
| Native dispatch, true pipeline | 3 passed, 1551 filtered | 12.96 / 0.00 |
| Native period vectors | 2 passed, 1552 filtered | 0.12 / 0.00 |
| Native GET_FORMAT table vectors | 1 passed, 1553 filtered | 0.12 / 0.00 |
| Full native expression library | **1456 passed, 4 old failures, 94 ignored; 1554 total; exit 101** | 0.12 / 10.65 |

**8 actual test runs / 8 Cargo attempts: 7 green, 1 current baseline non-green; no launch failures, retries, Rust compilation failures or new red test run.** The first final formatter check failed (exit 1) on a long `PeriodDiffNative` return in batch.rs; that script stopped at TiKV formatting before native checks/lock checks ran. A whitespace-only wrap fixed it, and the rerun passed all 13 sources, unchanged-lockfile and diff checks. This was not a compilation/test failure or an additional Cargo attempt; not all checks passed first try.
The complete expression failure section equals **month-seconds-expr-full.log** after only thread-ID normalization, SHA-256 `80be9bda05e5bf630e9c246ca9523d23f82c122eaaeec6e220b1b00cec436615`. No address mapping is needed; duration.rs remains at 212:55. Unistore was not rerun this round or last round: its 189/1/13 result belongs to historical `hms-three-33`, not a current gate.
SQL covers five normal rows × three projections, a sixth invalid row with two healthy-resource 1210 calls, and nine zero-slot Resource/1105/HY000 calls with no warnings. Three dispatch tests exercise the real pipeline; the one old GET_FORMAT table test uses a cfg(test) direct datatype lookup and is **not C4 evidence**. The moved GET_FORMAT table body matches under whitespace normalization, not a byte-equality claim; wire period_add/period_diff bodies are byte-identical. Five recorded whole files, int_arg, TIME_FORMAT, unrelated native regions and old tests remain byte-identical; expected values/oracles were not changed.
Exact commands and boundaries: [summary](logs/period-format-summary.txt), [evidence](evidence/period-format-checkpoint.md), `checkpoint.json`.

## Remaining work

Next grouped read-only candidates, **not credited**: DAYOFWEEK, WEEKDAY, DAYOFYEAR and DAYNAME, around shared forward-civil/week-name helpers. DAYOFWEEK and DAYOFYEAR remain on the deferred list: reopening grouped review does not resolve their compatibility gaps. JSON_LENGTH also stays deferred; no next-batch implementation is claimed.
Legacy default-NoColumns/request-root propagation, operation-scope coverage, allocation/physical peak, paired differential reruns, release/profile gates, prior duration-panic test adaptation, whole workspace, `make lint`, M6 and TiFlash remain unfinished. MICROSECOND's parser/public-helper/PB/legacy policy and JSON_PRETTY's float-format/compact-helper boundaries need separate locks. No whole-type/package, full datatype/parser-suite, performance or OOM-safety claim is made.
