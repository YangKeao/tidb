# Expression unification experiment

Checkpoint-ID: `go-trig-five-27` (previous: `char-conv-two-26`)

**89/245 frozen families delegate to TiKV with native evaluator algorithms removed; target 221.** This checkpoint adds SIN, COS, TAN, COT and ATAN. Both ATAN arities and ATAN2 belong to the single `atan` family. Strict final-audited acceptance remains 0; the experiment is incomplete and not PR-ready.

## Paired repositories

- [YangKeao/tidb, expression-unification-demo](https://github.com/YangKeao/tidb/tree/expression-unification-demo)
- [YangKeao/tikv, expression-unification-demo](https://github.com/YangKeao/tikv/tree/expression-unification-demo)

Use sibling checkouts. `checkpoint.json` pins TiKV and the published Plan hash. Root Plans mirror `/home/agent/tidb/EXPRESSION_UNIFICATION_PLAN.md` at publication. Validated steps push both branches without force-push or automatic PRs.

## This checkpoint

- **One Go-compatible owner:** native trig production code moves to TiKV `impl_math/native_go_trig.rs`. This is compatibility-core relocation, **not** a claim that existing libm implementations are bit-identical. Constants, reduction, polynomial order, special-value branches and the trailing ATAN implementation are preserved. Native `go_trig.rs` retains only test imports, documentation and the original golden block.
- **Preserved evidence:** the original 134-line test block is byte-identical and its three tests pass against the shared provider. The original 400 production lines match after six visibility changes and two formatter-only compact-if layouts; the formatted shared module is 392 lines. Narrow pure exports support the old fixtures, while production evaluators use the closed worker.
- **Existing transport:** eleven private operations—six native-Go and five legacy-libm forms—reuse unary/binary raw IEEE carriers and owned-bit results. No new carrier, SQL failure marker, evaluator driver or four-column allowance is introduced.
- **Separate result policies:** wire and legacy share libm primitives. Wire keeps Real/NULL/COT overflow behavior; legacy retains raw NaN/Inf, including COT at signed zero. Native consumes the computed raw result through its existing `finite_float` packing and preserves COT's expression-based diagnostic renderer.
- **Original operand demand:** ordinary native ATAN2 coerces both operands even when the first is NULL. PB keeps its first-NULL child cutoff with a real NULL-witness invocation. Legacy evaluates the right operand only after a non-NULL left. All existing routes forward their actual context; TAN gains no PB or legacy admission.

No native trig algorithm remains, and no duplicate Go golden fixture is introduced. Existing POW/LOG demand, error receipts, execution pools and unrelated math families are unchanged.

## Actual validation

| Scope | Result |
|---|---|
| TiKV all local evaluator tests | 247 passed, 1 existing ignored |
| TiKV math tests, including original wire cases | 52 passed |
| Original native Go golden tests | 3 passed |
| New native dispatch/PB tests | 3 passed |
| New legacy trig tests | 2 passed |
| SQL/lifecycle tests | 57 passed |
| Full native expression library | **1435 passed, 4 unchanged failures, 94 ignored; exit 101** |

All six targeted Rust runs passed on their first attempt; there were no compilation fixes, assertion corrections or retries. New SQL coverage includes four stored rows across seven call forms, an independent fifth-row COT overflow query and 16 direct zero-slot refusals. Focused PB coverage is representative, not an exhaustive signature matrix.

The entire full-expression failure section matches checkpoint26 byte-for-byte after replacing only panic-heading thread IDs. Both lockfiles and all existing expected values/fixtures remain unchanged.

Seven exact command receipts: [summary](logs/go-trig-summary.txt). Ownership, relocation proof and compatibility: [evidence](evidence/go-trig-checkpoint.md).

## Remaining work

EXP and LOG10 are the next read-only candidates, not credited. Their existing Go-compatible code can likewise move to a shared owner, but EXP's coerced-input diagnostic formatting and LOG10's warning/domain policy must remain intact. The existing EXP baseline failure is an old expectation conflict, not something to silently fix during migration. Compression retains separate compatibility work.

Complete operation-scope coverage, allocation/high-water and physical-peak checks, paired differential reruns, full codec-domain equivalence, release performance, whole workspace, `make lint` and TiFlash integration remain unfinished. Full datatype, unistore and parser-charset suites were not rerun; historical non-green results are not passing evidence. This is not a complete Go-package transcreation claim.
