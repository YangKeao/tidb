# Expression unification experiment

Checkpoint-ID: `variadic-oct-elt-four-23` (previous: `collated-five-22`)

**74/245 frozen families delegate to TiKV with native evaluator algorithms removed; target 221.** This checkpoint adds OCT, CONCAT, CONCAT_WS and full-arity ELT. Strict final-audited acceptance remains 0; the overall experiment is incomplete and not PR-ready.

## Paired repositories

- [YangKeao/tidb, expression-unification-demo](https://github.com/YangKeao/tidb/tree/expression-unification-demo)
- [YangKeao/tikv, expression-unification-demo](https://github.com/YangKeao/tikv/tree/expression-unification-demo)

Use sibling checkouts. `checkpoint.json` pins TiKV and the published Plan hash. Root Plans mirror `/home/agent/tidb/EXPRESSION_UNIFICATION_PLAN.md` at publication. Validated steps push both branches without force-push or automatic PRs.

## This checkpoint

- **CONCAT/CONCAT_WS:** both native join implementations are removed. Value and typed entries share frontend preparation for coercion, packet diagnostics and the demanded prefix. Typed children retain their original lazy stopping points; AST children remain eagerly evaluated before the value helper. The public `concat_values` remains a compatible wrapper around new `concat_values_in`.
- A dedicated opaque payload carries full SQL arity, real nullable operands and `Complete`, `InputNull` or `PacketExceeded { limit }`. One physical Bytes column is not a four-argument SQL limit, a prejoined answer or a tail padded with fake NULLs. TiKV validates the entire prefix/terminal and invokes the shared wire/native join core.
- Packet getters keep their original timing. The failing comparison's actual limit is retained; the original diagnostic handler can read the getter again. WS budgets separator bytes by original operand index, while output separators join surviving values. Strict frontend errors stay errors; successful NULL, empty and warning-suppressed results still require C4.
- **ELT:** one shared selector consumes index plus total SQL arity including the index operand and returns a SQL operand offset. It determines conversion of an already-evaluated value, not which SQL children run. The final three-column recipe revalidates `Value(actual nullable bytes)` versus `Undemanded`. All children still evaluate/wrap eagerly, and any candidate's binary metadata still affects the result.
- **OCT:** the original raw64 integer renderer and decimal-prefix scanner are shared. Native valid UTF8 retains Unicode trimming; malformed UTF8 and wire strings keep ASCII trimming. Empty raw input differs from nonempty whitespace. Int/UInt/Bit/BinaryLiteral paths do not gain generic ETInt or floating-point rounding semantics.

No generic driver, graph limit or four-column whitelist was widened. No new native PB/unistore admission was introduced. Pure input-builder errors retain the actual LocalError with phase None, not a fabricated worker phase or SQL overflow. No existing expected value or fixture was changed.

## Actual validation

| Scope | Result |
|---|---|
| TiKV all local evaluator tests | 232 passed, 1 ignored |
| Original TiKV string tests | 63 passed |
| Native packet/demand regression tests | 3 passed |
| SQL/lifecycle tests | 49 passed |
| Full native expression library | **1421 passed, 4 unchanged failures, 94 ignored; exit 101** |

New SQL tests cover five stored rows with more than four arguments, binary/text metadata, and 16 direct zero-slot refusals. The complete expression failure block matches checkpoint22 after thread-ID normalization only. First compilation and every targeted run passed; both lockfiles are unchanged.

Five exact command/result receipts: [summary](logs/variadic-oct-elt-summary.txt). Detailed ownership and preserved semantics: [evidence](evidence/variadic-oct-elt-checkpoint.md).

Datatype/collation suites, unistore, parser-charset and generators were not rerun this checkpoint. Their earlier results are not current green evidence: the known unistore failure and parser default-registry assertion remain documented in checkpoint21. Whole workspace and `make lint` were not run.

## Remaining work

Next candidates: EXPORT_SET, FIELD and MAKE_SET; CHAR and CONV follow. FIELD needs first-match coercion cutoff and raw IEEE/signedness policies. MAKE_SET must retain the original above-64 shift behavior under the actual overflow-check profile. EXPORT_SET/CHAR may need new single-owner TiKV compatibility cores, explicitly distinguished from reuse of existing wire implementations. EXP/LOG10, COMPRESS and UNCOMPRESS retain documented compatibility/diagnostic work.

Complete operation-scope guards, physical peak/OOM safety, allocation remeasurement, paired differential reruns, full codec-domain equivalence and release performance remain unverified. Packed ownership/copies are not a zero-copy or performance claim. Kernel reuse is not a new complete Go-package transcreation claim. POSITION remains a LOCATE alias; checkpoint22 corrected its tentative duplicate credit before publication. Checkpoint20 repaired the earlier LOWER/UPPER legacy omissions without extra family credit.
