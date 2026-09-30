# Expression unification experiment

Checkpoint-ID: `wide-math-decimal-five-25` (previous: `field-make-export-three-24`)

**82/245 frozen families delegate to TiKV with native evaluator algorithms removed; target 221.** This checkpoint adds ABS, CEIL/CEILING, FLOOR, ROUND and TRUNCATE across their complete existing domains. Strict final-audited acceptance remains 0; the experiment is incomplete and not PR-ready.

## Paired repositories

- [YangKeao/tidb, expression-unification-demo](https://github.com/YangKeao/tidb/tree/expression-unification-demo)
- [YangKeao/tikv, expression-unification-demo](https://github.com/YangKeao/tikv/tree/expression-unification-demo)

Use sibling checkouts. `checkpoint.json` pins TiKV and the published Plan hash. Root Plans mirror `/home/agent/tidb/EXPRESSION_UNIFICATION_PLAN.md` at publication. Validated steps push both branches without force-push or automatic PRs.

## This checkpoint

- **Wide Decimal foundation:** controlled coefficient/word bridges preserve hidden storage precision, visible result scale and values beyond 81 digits. C4 consumes and returns actual Decimal vectors, not formatted text or truncated nine-word values. The old native ABS/CEIL/FLOOR/ROUND/TRUNCATE datatype methods and retained-storage rounding delegate to the same shared workers; their duplicated arithmetic is removed. The separate native ceiling-rounding helper remains outside this change, so this is not a whole-Decimal completion claim.
- **Complete math families:** 23 closed recipes preserve full UInt64, raw IEEE/Float32 result policies, integer identity and exact truncation. Native two-argument integer ROUND retains its original f64 round-trip, and native real ROUND keeps Go Pow10 and ties-even rather than silently adopting wire behavior. Decimal target-scale policy and rounding reside in TiKV. CEIL/FLOOR receive a shared checked i64 view and retain Decimal fallback when necessary.
- **Existing PB and legacy paths:** PB ROUND's actual-NULL witness preserves its original demand and error precedence. Legacy ROUND preserves full i128 identity, ties-away real rounding, Decimal rounding before f64 conversion, and original child/error-folding rules. NULL and identity results still require a real worker. Only the native packing return type became generic; the guard and evaluator driver remain shared.
- **Typed failures and resources:** only a sealed ABS invocation that actually returns the explicit signed-overflow cause becomes the existing native 1690 error. Budget, bridge and output failures are never inferred as SQL overflow. Decimal recipes carry the worker's finite remaining budget; logical accounting includes owned spill, initialized NULL backing and result coexistence. This does not establish physical peak/OOM or performance bounds.

Existing wire policies, original unchecked scale arithmetic under the actual workspace profile, SQL coercion and metadata remain distinct and preserved. There is no native fallback, new PB/unistore admission, general graph widening or four-column whitelist expansion.

## Actual validation

| Scope | Result |
|---|---|
| TiKV Decimal-related tests | 98 passed, twice |
| Native Decimal-related tests | 90 passed after a compilation fix |
| TiKV all local evaluator tests | 241 passed, 1 existing ignored |
| TiKV math tests, including original wire cases | 48 passed |
| New native dispatch tests | 3 passed |
| New legacy ROUND tests | 2 passed |
| SQL/lifecycle tests | 53 passed |
| Full native expression library | **1429 passed, 4 unchanged failures, 94 ignored; exit 101** |

New SQL coverage uses five stored rows: four normal rows across 13 function columns, plus a separate ABS(MIN) row asserting the original column-name 1690/22003 rendering. Seventeen direct zero-slot calls verify refusal, including NULL inputs. The entire full-expression failure section is byte-identical to checkpoint24 after replacing only panic-heading thread IDs.

The first native datatype compilation failed with six diagnostics at four new unsigned-word/signed-power-table operations. Four explicit `as u32` conversions fixed compilation without changing values or expectations. The failed log is retained. The second TiKV datatype run verifies ABS reusing the existing shared ABS worker. Both lockfiles and all existing expected values/fixtures remain unchanged.

Ten receipts (nine actual test runs and one compilation-only failure): [summary](logs/wide-math-decimal-summary.txt). Ownership, commands and compatibility: [evidence](evidence/wide-math-decimal-checkpoint.md).

Full unistore, parser-charset, generators, whole workspace and `make lint` were not rerun. Earlier non-green results are not current passing evidence.

## Remaining work

CHAR (frozen ID `char_func`) and CONV are next read-only candidates, not credited. CHAR needs a new shared compatibility core; CONV needs explicit native/legacy policies around existing TiKV primitives, including binary-literal conversion and typed overflow. EXP/LOG10, COMPRESS and UNCOMPRESS retain compatibility work.

Complete operation-scope coverage, allocation/high-water remeasurement, paired differential reruns, full codec-domain equivalence, release performance and TiFlash integration remain unfinished. Kernel reuse is not a complete Go-package transcreation claim. POSITION remains a LOCATE alias; checkpoint20 repaired earlier LOWER/UPPER legacy omissions without extra credit.
