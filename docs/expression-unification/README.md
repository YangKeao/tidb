# Expression unification experiment

Checkpoint-ID: `field-make-export-three-24` (previous: `variadic-oct-elt-four-23`)

**77/245 frozen families delegate to TiKV with native evaluator algorithms removed; target 221.** This checkpoint adds complete FIELD, MAKE_SET and EXPORT_SET. Strict final-audited acceptance remains 0; the experiment is incomplete and not PR-ready.

## Paired repositories

- [YangKeao/tidb, expression-unification-demo](https://github.com/YangKeao/tidb/tree/expression-unification-demo)
- [YangKeao/tikv, expression-unification-demo](https://github.com/YangKeao/tikv/tree/expression-unification-demo)

Use sibling checkouts. `checkpoint.json` pins TiKV and the published Plan hash. Root Plans mirror `/home/agent/tidb/EXPRESSION_UNIFICATION_PLAN.md` at publication. Validated steps push both branches without force-push or automatic PRs.

## This checkpoint

- **FIELD:** separate Bytes/collation, signedness-preserving Int and raw-IEEE recipes. Shared single-candidate probes preserve the original coercion cutoff; the final kernel validates the observed prefix and returns the first-match index. All SQL children remain eager, and all arguments still determine the comparison domain. Real needle conversion remains once before scanning, even with all-NULL candidates; String/Int can leave the needle undemanded. NaN and signed zero retain IEEE equality.
- **MAKE_SET:** shared native selection and wire/native comma-join primitives replace native computation. The original unchecked shift remains governed by the actual workspace overflow-check profile; wire keeps its separate rolling-mask behavior. Full SQL arity includes the mask, including existing value-only mask calls. Selected nullable values and unselected operands remain distinct.
- **EXPORT_SET:** a **new single-owner TiKV compatibility core**, not a claimed pre-existing wire implementation. Native code keeps the value helper's whole-list NULL precheck/lossy conversion and the extension's strict tuple/LTR coercion. An on=NULL value does not skip converting off. True NULL witnesses retain their argument positions; omission differs from NULL and undemanded values. Defaults, count clamping and the original signed bit63 rule now belong to TiKV.

FIELD/MAKE_SET use one physical Bytes column each; EXPORT_SET uses three columns. These are full supported domains, not four-argument SQL subsets or precomputed answers. NULL and empty results require the real evaluator. No generic driver, graph limit, four-column whitelist or PB/unistore admission was widened. Pure preparation errors retain the actual LocalError with phase None, not a fabricated worker phase or SQL overflow.

## Actual validation

| Scope | Result |
|---|---|
| TiKV all local evaluator tests | 236 passed, 1 ignored |
| Original TiKV string tests | 63 passed |
| Native demand/coercion regression tests | 3 passed |
| Native string extension tests | 18 passed |
| SQL/lifecycle tests | 51 passed |
| Full native expression library | **1426 passed, 4 unchanged failures, 94 ignored; exit 101** |

New SQL tests cover five stored rows, nine function columns plus two integer controls, full variadic shapes and 17 direct zero-slot refusals. The complete expression failure block matches checkpoint23 after thread-ID normalization only. A portable MAKE_SET test compares the shared selector with the original expression under the actual test profile; release behavior was not separately executed.

The first native test compilation failed at two new test constructors: Decimal does not implement FromStr. Replacing them with the existing `Decimal::from_literal` API preserved the inputs and expected results; the rerun passed. The initial failure log is retained, and its chained extension/SQL commands did not run. Both lockfiles and all existing expected values/fixtures are unchanged.

Seven exact command receipts (six test runs and one compilation failure): [summary](logs/field-make-export-summary.txt). Ownership and compatibility: [evidence](evidence/field-make-export-checkpoint.md).

Datatype/collation, unistore, parser-charset and generators were not rerun. Their earlier non-green results are not current passing evidence. Whole workspace and `make lint` were not run.

## Remaining work

CHAR and CONV remain string candidates. ABS/CEIL/FLOOR/ROUND/TRUNCATE form a possible next complete batch after a controlled wide-Decimal bridge, real Decimal evaluator carrier and typed SQL-error receipt; migrating only their Real domains would not earn family credit. EXP/LOG10, COMPRESS and UNCOMPRESS retain compatibility/diagnostic work.

Complete operation-scope guards, physical peak/OOM safety, allocation remeasurement, paired differential reruns, full codec-domain equivalence and release performance remain unverified. Packed ownership is not a zero-copy or performance claim. Kernel reuse is not a complete Go-package transcreation claim. POSITION remains a LOCATE alias; checkpoint22 rejected its duplicate credit. Checkpoint20 repaired earlier LOWER/UPPER legacy omissions without extra family credit.
