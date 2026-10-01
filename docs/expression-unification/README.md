# Expression unification experiment

Checkpoint-ID: `temporal-fields-four-32` (previous: `json-storage-quote-three-31`)
**103/245 frozen families delegate to TiKV with native evaluator algorithms removed; target 221.** Added: YEAR, MONTH, DAYOFMONTH (DAY alias) and QUARTER. Strict final-audited acceptance remains **0**; incomplete and not PR-ready.

Paired branches: [TiDB](https://github.com/YangKeao/tidb/tree/expression-unification-demo) · [TiKV](https://github.com/YangKeao/tikv/tree/expression-unification-demo). Use sibling checkouts.
The parent records the paired TiKV commit and Plan hash in `checkpoint.json` before publication. Root Plans mirror `/home/agent/tidb/EXPRESSION_UNIFICATION_PLAN.md`; publication uses paired pushes, not force-push or automatic PRs.

## This checkpoint

- **Field ownership:** four TiKV Time associated const primitives, plus the quarter instance getter, serve the kernels; native CoreTime's three const getters are thin delegates. Typed invalid fields remain lossless (16383/15/31; quarter 5). Kind, FSP and clock fields are not observed by these four results. Wire YEAR/DAY zero-date warnings remain unchanged; this is not a whole-Time bridge.
- **Closed protocol:** one new nullable `TimeCoreBits` role accepts exactly eight little-endian bytes, isolated from Bytes/IEEE/Int even for NULL. Four fixed CoreNative operations use the closed Bytes-to-Int factory and existing OwnSignedInt; no new result kind, metadata, module, driver, NoArgs case, four-column allowance or PB admission. Scope: 18 Rust files, TiKV 8/native 10.
- **Native/PB behavior:** original ETDatetime casts, context getters and warnings stay intact. MONTH PB sends any observed NULL to the worker without coercing the observed prefix or reading the suffix; non-NULL bad arity stays an error. Tuple/callback compatibility is test-only, not a production fallback.
- **Legacy correction:** a real PI zero-slot regression exposed the index helper's flags=2 downgrade to `Ok` plus warning 1265. A three-line Infrastructure-first guard now returns the error; SQL/InvalidResult flags and messages and public eval/table handling stay unchanged. **DefaultNoColumns one-shot/request-root capability propagation remains unfixed.**

## Actual validation

| Run | Result |
|---|---|
| TiKV datatype | 42 passed |
| TiKV local evaluator | 254 passed, 1 existing ignored |
| TiKV guard | 1 passed |
| TiKV time kernels | 51 passed |
| Native CoreTime | 15 passed |
| Native dispatch | 3 passed |
| Native datetime | 21 passed |
| SQL/lifecycle | 67 passed |
| Legacy index regression, before fix | 0 passed, 1 failed; exit 101; 0.00 s |
| Same exact legacy filter, after fix | 1 passed, 0 failed; exit 0; 0.00 s |
| Full unistore | **187 passed, 1 unchanged failure, 13 ignored; 201 total; exit 101; 2.97 s** |
| Full native expression library | **1448 passed, 4 unchanged failures, 94 ignored; 1546 total; exit 101; 10.47 s** |

**12 actual runs: 9 green, 1 intentional red subsequently green, 2 known full-suite non-green.** Zero compilation failures; no expected/oracle edits. Both complete failure sections match their baselines after thread-ID-only normalization; exact names and hashes are in `checkpoint.json`. All 18 files passed pinned formatting/checks; both locks and the recorded original fixture files/blocks remain unchanged, and diff checks passed. This is not a zero-rerun or first-wave-perfect claim.
SQL coverage includes four rows × five projections with DAY alias, typed zero/setup 1292, four numeric columns, two original cast-1292 cases and nine zero-slot calls (bad YEAR retains pre-admission 1292). Three dispatch tests cover invalid fields, Date clock/FSP0 versus Timestamp/FSP6, AST/typed/PB context and PB(NULL,bad-tail) NULL/refusal.
Exact commands: [summary](logs/temporal-fields-summary.txt). Ownership and corrections: [evidence](evidence/temporal-fields-checkpoint.md).

## Remaining work

Next read-only candidates, **not credited**: HOUR, MINUTE, SECOND. SQL has no ETDuration cast here; even typed Duration passes through Display and `parse_hms_extended`'s special clamp. Future work must share that parser, raw-signed-nanos projection/public const helpers and PB/legacy paths, not substitute direct nanos reads. MICROSECOND, TIME_TO_SEC, other parser policies and MONTHNAME require separate locks; no next-batch completion or interchangeable Time domain is claimed.
JSON_LENGTH/DAYOFWEEK/DAYOFYEAR stay deferred. Operation-scope/capability coverage, allocation/physical peak, paired differential reruns, release performance, whole workspace, `make lint` and TiFlash remain unfinished. No full datatype/parser-suite, whole-Go-package, performance or OOM-safety claim is made.
