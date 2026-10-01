# Expression unification experiment

Checkpoint-ID: `exp-log-two-28` (previous: `go-trig-five-27`)

**91/245 frozen families delegate to TiKV with native evaluator algorithms removed; target 221.** This checkpoint adds EXP and LOG10. Strict final-audited acceptance remains 0; the experiment is incomplete and not PR-ready.

## Paired repositories

- [YangKeao/tidb, expression-unification-demo](https://github.com/YangKeao/tidb/tree/expression-unification-demo)
- [YangKeao/tikv, expression-unification-demo](https://github.com/YangKeao/tikv/tree/expression-unification-demo)

Use sibling checkouts. `checkpoint.json` pins TiKV and the published Plan hash. Root Plans mirror `/home/agent/tidb/EXPRESSION_UNIFICATION_PLAN.md` at publication. Validated steps push both branches without force-push or automatic PRs.

## This checkpoint

- **One arithmetic owner:** the existing native compatibility algorithms move to TiKV `impl_math/native_go_exp_log.rs`. All 136 production-body lines remain byte-identical after formatting, including EXP's FMA/nearest-even arithmetic and the existing LOG10/Frexp branches. The license is preserved. Only header commentary is corrected to describe the actual oracle FMA path and avoid a universal Go-CPU claim. The old 170-line native file and module declaration are deleted.
- **Minimal integration:** two closed private unary operations reuse the nullable IEEE carrier, owned-bit result and existing driver. No new argument role, public pure-function bridge, failure kind, graph or four-column allowance is added. These functions had no native PB/legacy admission; none is invented. Existing wire EXP/LOG10 implementations are unchanged.
- **EXP diagnostics:** guarded coercion runs once. A call-local cell retains the coerced input only to format the original diagnostic after an actual computed non-finite result. Thus date-like text still emits 1292 and then reports `exp(2020)`, not a column expression. An impossible missing input returns an explicit existing Unsupported error, not a fabricated value or new typed scope failure.
- **LOG10 policy:** non-positive inputs emit the original 3020 warning before admission, but the real input bits still enter the worker. Packing consumes the computed result before applying domain NULL. NaN and positive infinity retain their original raw Real behavior. Pool refusal retains preceding warnings and never becomes SQL overflow or fabricated NULL.

The change covers twelve Rust paths: five native, including one deletion, and seven TiKV, including one new module. Eleven live sources were pinned-formatted. Existing math vectors, expected values and both lockfiles remain unchanged.

## Actual validation

| Scope | Result |
|---|---|
| TiKV all local evaluator tests | 248 passed, 1 existing ignored |
| TiKV math tests, including original wire cases | 54 passed |
| Original native math/source-vector tests | 20 passed |
| New native dispatch/diagnostic tests | 2 passed |
| SQL/lifecycle tests | 59 passed |
| Full native expression library | **1437 passed, 4 unchanged failures, 94 ignored; exit 101** |

All five targeted Rust runs passed on their first attempt, without compilation fixes, assertion corrections or retries. New SQL tests cover four stored rows across two function columns, two domain-warning rows, a text-coercion/overflow query and six direct zero-slot refusals.

The entire full-expression failure section matches checkpoint27 byte-for-byte after replacing only panic-heading thread IDs. In particular, the old EXP test still expects FloatOverflow while the existing implementation and another source test require DataOutOfRange; migration does not silently change either policy or old expectation.

Six exact command receipts: [summary](logs/exp-log-summary.txt). Ownership, source-copy hashes and compatibility: [evidence](evidence/exp-log-checkpoint.md).

## Remaining work

COMPRESS and UNCOMPRESS are next read-only candidates, not credited. Native COMPRESS requires the existing Go encoder's exact bytes rather than wire zlib output. UNCOMPRESS needs its bounded, complete-stream policy and actual completed result disposition so the host can append original warnings without re-decoding or guessing from inputs. The Plan records these differences and the closed-worker warning constraint.

Complete operation-scope coverage, allocation/high-water and physical-peak checks, paired differential reruns, full codec-domain equivalence, release performance, whole workspace, `make lint` and TiFlash integration remain unfinished. Full datatype, unistore and parser-charset suites were not rerun; historical non-green results are not passing evidence. This is not a complete Go-package transcreation claim.
