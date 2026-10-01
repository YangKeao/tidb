# Expression unification experiment

Checkpoint-ID: `hms-three-33` (previous: `temporal-fields-four-32`)
**106/245 frozen families delegate to TiKV with native evaluator algorithms removed; target 221.** Added: HOUR, MINUTE and SECOND. Strict final-audited acceptance remains **0**; incomplete and not PR-ready.

Paired branches: [TiDB](https://github.com/YangKeao/tidb/tree/expression-unification-demo) · [TiKV](https://github.com/YangKeao/tikv/tree/expression-unification-demo). Use sibling checkouts.
The parent records the paired TiKV commit and Plan hash in `checkpoint.json` before publication. Root Plans mirror `/home/agent/tidb/EXPRESSION_UNIFICATION_PLAN.md`; publication uses paired pushes, not force-push or automatic PRs.

## This checkpoint

- **Single parser owner:** TiKV Time owns native HMS/clamp, date-prefix parsing, component splitting, year pivot, leap-year and month-length helpers; native helpers thin-delegate. Full u32-year input and original String/Vec allocation structure remain. Wire invalid-month length 31 and native length 0 remain distinct policies. Duration's three public const projections share primitives; MICROSECOND, to_secs, Display, FSP and constructors are unchanged.
- **Closed protocol:** six operations reuse three Bytes→Int text signatures and three Int→Int signed-nanosecond signatures, with existing OwnSignedInt. Raw nanos are not precomputed fields and do not construct/round a Duration, infer FSP or apply SQL's clamp. No new role, result, metadata, module, driver, NoArgs case, four-column allowance or PB admission. Scope: 17 Rust files, TiKV 8/native 9.
- **Explicit precedence change:** SQL retains the full original `coerce_str` domain, including typed Duration Display/FSP, without an ETDuration cast. Arity/UTF-8/coercion errors precede admission; malformed text's quiet NULL now comes from the worker, so zero-slot Resource refusal wins over that parse result. All NULL inputs also execute workers.
- **Demand and limits:** PB's first observed NULL reaches the worker without coercing its prefix or reading its suffix. Legacy evaluates duration once and transports optional signed nanos. The default-NoColumns one-shot/request-root capability gap remains; no whole-Time/Duration interchangeability, allocation-peak, performance or package-transcreation claim is made.

## Actual validation

| Run | Result | Compile / run seconds |
|---|---|---|
| TiKV Time | 43 passed, 361 filtered | 2.11 / 0.01 |
| TiKV Duration | 21 passed, 383 filtered | 0.12 / 0.00 |
| Native Duration | 24 passed, 414 filtered | 2.52 / 0.00 |
| TiKV local evaluator | 255 passed, 1 existing ignored, 486 filtered | 8.93 / 0.19 |
| TiKV time kernels | 53 passed, 689 filtered | 0.14 / 0.01 |
| Full unistore | **189 passed, 1 old failure, 13 ignored; 203 total; exit 101** | 11.29 / 2.97 |
| SQL/lifecycle | 69 passed, 2078 filtered | 28.35 / 0.96 |
| Native dispatch | 3 passed, 1546 filtered | 10.18 / 0.00 |
| Native datetime | 21 passed, 1528 filtered | 0.14 / 0.02 |
| Full native expression library | **1451 passed, 4 old failures, 94 ignored; 1549 total; exit 101** | 4.71 / 10.76 |

**10 actual runs: 8 green, 2 current full-suite baseline non-green; no Rust compilation failures, test retries or new red phase.** Existing MONTH/index regressions passed inside full unistore. Six Duration bench smoke cases measured nothing and are not performance evidence. Final pinned formatting/checks, locks and diff checks passed; the initial formatter check needed two return-line wraps, and two static-check scripts needed correction (trailing-comma normalization and the actual `clock_source_tests` module name), not logic changes. Not all checks passed first try.
Unistore's complete failure section remains equal after thread-ID-only normalization. Expression's does **not**: current SHA `80be9bda05e5bf630e9c246ca9523d23f82c122eaaeec6e220b1b00cec436615` differs from prior `0930217d98e0b92d727527dc3c7cb7313f1bbe35e643da60114fa6d78203839b`. Only mapping the import-shifted `duration.rs:212:55` back to `211:55` restores equality; no general line-number stripping is used. The panic statement and MICROSECOND-onward source are unchanged.
Relocated parser algorithms match under explicit symbol/Self, whitespace, optional trailing-comma and const u32→i64 conversion normalization—not byte equality. Five recorded native files and calendar's original test suffix remain byte-identical; expected values/oracles were not changed. SQL covers six varchar rows × three fields, one six-column numeric/TIME query and nine zero-slot Resource/1105 refusals with no warnings, including bad text; three dispatch and two legacy tests passed.
Exact commands and full failure details: [summary](logs/hms-summary.txt), [evidence](evidence/hms-checkpoint.md), `checkpoint.json`.

## Remaining work

Next read-only candidates, **not credited**: MONTHNAME and TIME_TO_SEC. MICROSECOND still needs its complete independent parser, public-helper and PB/legacy policy lock. JSON_PRETTY is a later possibility only after its float-formatting leaf and compact-helper boundaries are locked; do not add a third family merely to fill a batch. No next-batch implementation is claimed.
JSON_LENGTH/DAYOFWEEK/DAYOFYEAR stay deferred. Operation-scope/capability coverage, allocation/physical peak, paired differential reruns, release performance, whole workspace, `make lint` and TiFlash remain unfinished; no full datatype/parser-suite or OOM-safety claim is made.
