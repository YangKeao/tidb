# Expression unification experiment

Checkpoint-ID: `month-seconds-two-34` (previous: `hms-three-33`)
**108/245 frozen families delegate to TiKV with native evaluator algorithms removed; target 221.** Added: MONTHNAME and TIME_TO_SEC. Strict final-audited acceptance remains **0**; incomplete and not PR-ready.

Paired branches: [TiDB](https://github.com/YangKeao/tidb/tree/expression-unification-demo) · [TiKV](https://github.com/YangKeao/tikv/tree/expression-unification-demo). Use sibling checkouts.
The parent records the paired TiKV commit and Plan hash in `checkpoint.json` before publication. Root Plans mirror `/home/agent/tidb/EXPRESSION_UNIFICATION_PLAN.md`; publication uses paired pushes, not force-push or automatic PRs.

## This checkpoint

- **Shared ownership:** existing public `MONTH_NAMES` replaces four duplicate full-name tables; abbreviation tables and lowercase matchers remain outside this change. TiKV `Time::parse_native_duration_text` solely owns the complete seconds/fraction parser; native `duration` thin-delegates for TIME_FORMAT without changing its mask handling or other algorithms. This does not credit TIME_FORMAT, DATE_FORMAT or STR_TO_DATE as whole families.
- **Original domains:** MONTHNAME retains its ETDatetime cast, then sends actual `coerce_str` text for worker date parsing/name selection. TIME_TO_SEC adds no ETDuration cast; its parser retains negative hours/double minus, out-of-range positive seconds from `--900`, positive-hour >838 rejection rather than HMS clamping, arbitrary fraction characters truncated to six, ASCII-last-space date-prefix handling, String/Vec allocation structure and unchecked arithmetic.
- **Closed execution:** two operations reuse nullable Bytes→Bytes/Int, with no new role, result, metadata, module, driver, NoArgs case, four-column allowance or PB/legacy admission. Scope: 13 Rust files, TiKV 7/native 6. Arity/UTF-8/coercion and original cast warnings stay before admission; malformed-text NULL/zero outcomes now run in the worker, so zero-slot Resource refusal takes precedence. NULL also executes the worker.
- **Panic boundary:** in the tested profile, direct-parser and C4 overflow retain the same unwind payload—not an Infrastructure/SQL result. The poisoned old scope is dropped with its worker; a fresh scope in the same execution creates a second worker and returns -7205 normally. This does not make the poisoned scope reusable. Root Cargo profiles govern dependencies; no production checked arithmetic, debug switch, runtime flag or profile change was added. Release/profile gates and adaptation of the panic assertions remain pending.

## Actual validation

| Run | Result | Compile / run seconds |
|---|---|---|
| TiKV Time | 44 passed, 361 filtered | 2.01 / 0.01 |
| Native MySQL Time | 33 passed, 405 filtered | 2.18 / 0.00 |
| Native STR_TO_DATE | 7 passed, 431 filtered | 0.09 / 0.00 |
| TiKV local evaluator | 256 passed, 1 existing ignored, 487 filtered | 8.73 / 0.19 |
| TiKV time kernels | 54 passed, 690 filtered | 0.13 / 0.01 |
| SQL/lifecycle | 71 passed, 2078 filtered | 19.76 / 1.09 |
| Native dispatch, including overflow unwind/recovery | 2 passed, 1549 filtered | 12.56 / 0.00 |
| Native datetime source | 21 passed, 1530 filtered | 0.13 / 0.02 |
| Full native expression library | **1453 passed, 4 old failures, 94 ignored; 1551 total; exit 101** | 0.12 / 10.46 |

**9 actual test runs: 8 green, 1 current baseline non-green; 10 Cargo attempts.** The extra attempt failed at launch (exit 101) in the TiDB root without Cargo.toml: zero compilation/tests; retrying SQL from `rust` passed. Its receipt is `month-seconds-sql-cwd-error.log`. There were no Rust compilation failures, product-test retries or new red phase, but there was this launch retry. Final pinned formatting/checks for 13 sources, lockfiles and diff checks passed without formatting/static-check failures this round.
The complete expression failure section equals **hms-expr-full.log** after only thread-ID normalization, SHA-256 `80be9bda05e5bf630e9c246ca9523d23f82c122eaaeec6e220b1b00cec436615`. No address mapping is needed this round; duration.rs remains at 212:55. Full unistore was **not rerun**: prior 189/1/13 and MONTH/index results are historical, not current gates.
Parser relocation matches after Result→Option, comment/whitespace and Self-qualification normalization—not byte equality. Native datatype changes are confined to shared full-name tables/imports under the recorded comparison; single_date, TIME_FORMAT, sibling algorithms and original tests remain unchanged. SQL covers six rows × two functions plus a numeric query and six zero-slot Resource/1105 calls; only bad-date MONTHNAME adds its original preceding 1292 diagnostic. Expected values/oracles were not changed.
Exact commands and boundaries: [summary](logs/month-seconds-summary.txt), [evidence](evidence/month-seconds-checkpoint.md), `checkpoint.json`.

## Remaining work

Next read-only candidates, **not credited**: PERIOD_ADD, PERIOD_DIFF and GET_FORMAT; lock their original wrapping, 1210 diagnostics and GET_FORMAT's special AST handling first. MICROSECOND's independent parser/public-helper/PB/legacy policy and JSON_PRETTY's float-format/compact-helper boundaries remain separately locked; no next-batch implementation is claimed.
JSON_LENGTH/DAYOFWEEK/DAYOFYEAR stay deferred. Legacy default-NoColumns/request-root propagation, operation-scope coverage, allocation/physical peak, paired differential reruns, release performance, whole workspace, `make lint` and TiFlash remain unfinished. No whole-type/package, full datatype/parser-suite, performance or OOM-safety claim is made.
