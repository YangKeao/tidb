# Expression unification experiment

Checkpoint-ID: `char-conv-two-26` (previous: `wide-math-decimal-five-25`)

**84/245 frozen families delegate to TiKV with native evaluator algorithms removed; target 221.** This checkpoint adds CHAR (frozen ID `char_func`) and CONV. Strict final-audited acceptance remains 0; the experiment is incomplete and not PR-ready.

## Paired repositories

- [YangKeao/tidb, expression-unification-demo](https://github.com/YangKeao/tidb/tree/expression-unification-demo)
- [YangKeao/tikv, expression-unification-demo](https://github.com/YangKeao/tikv/tree/expression-unification-demo)

Use sibling checkouts. `checkpoint.json` pins TiKV and the published Plan hash. Root Plans mirror `/home/agent/tidb/EXPRESSION_UNIFICATION_PLAN.md` at publication. Validated steps push both branches without force-push or automatic PRs.

## This checkpoint

- **CHAR:** a new single-owner TiKV compatibility byte generator, not a claimed pre-existing wire kernel. A closed packed nullable-i64 list supports all existing arities, including value-helper calls with zero numeric items. The original signed shift loop is preserved: zero emits NUL, negative values emit four bytes, and `4294967361` emits `00 00 00 41`. NULL items are skipped; empty/all-NULL lists still compute non-NULL empty bytes through a real worker.
- **Host charset policy:** numeric coercion/1292 warnings and initial charset lookup remain in guarded preparation. Computed bytes then feed the existing decoder, warning1300, conditional strict-mode read and final collation lookup. No additional packet/SQL-mode policy getter is introduced. Original metadata stays unchanged.
- **CONV:** native text, full binary-literal and legacy recipes reuse the existing TiKV prefix/parser/clamp/radix primitives with explicit policies. Native/legacy output sign is recomputed from wrapped u64 bits; wire retains its original sign and wrapping-base behavior. Binary literals keep the entire payload and execute the original two conversion stages in TiKV, including first-stage NULL/overflow precedence.
- **Complete existing paths:** PB preserves its first-NULL child cutoff and forwards the real context for non-NULL calls. Legacy bases retain full i128 in canonical LE16, with original text/from/to demand; out-of-i64 values reach the worker rather than becoming fabricated NULL inputs. Legacy parse overflow remains a kernel-produced NULL.
- **Typed overflow:** only an actual native CONV parse overflow with the sealed operation and invocation receipt exposes its complete sign-stripped digit payload. The original ParseIntError is retained as a source. Native mapping restores the existing 1690 diagnostic; resource failures and ordinary wire/legacy outcomes are never inferred as overflow from code or text.

Four private operations use at most three physical columns and the same driver. Native byte-generation, radix scanning and formatting algorithms are removed; the old prefix test helper is only a shared wrapper. No PB/unistore admission, general graph, four-column whitelist or execution pool is widened.

## Actual validation

| Scope | Result |
|---|---|
| TiKV all local evaluator tests | 245 passed, 1 existing ignored |
| TiKV math tests, including original wire CONV | 50 passed |
| Original TiKV string tests | 63 passed |
| New native dispatch/PB tests | 3 passed |
| New legacy CONV tests | 2 passed |
| SQL/lifecycle tests | 55 passed after correcting one new test assertion |
| Full native expression library | **1432 passed, 4 unchanged failures, 94 ignored; exit 101** |

New SQL coverage includes five stored rows across five main function columns, four UTF8 rows, strict/lenient decoding, an independent overflow query and 16 direct zero-slot refusals. The complete expression failure section matches checkpoint25 byte-for-byte after replacing only panic-heading thread IDs.

The first SQL run had 54 passes and one failure: the new test incorrectly expected the warning name `utf8`. The unchanged decoder maps `utf8` to the canonical diagnostic name `utf8mb4`; source inspection confirmed this before correcting that one new assertion. Result metadata still says `utf8`/Utf8Bin. The rerun passed both strict and lenient cases. The original failure log is retained; no production fix, old expected-value change, fixture regeneration or compile failure occurred.

Eight actual test receipts, including both non-green runs: [summary](logs/char-conv-summary.txt). Ownership and compatibility: [evidence](evidence/char-conv-checkpoint.md). Both lockfiles remain unchanged.

## Remaining work

SIN/COS/TAN/COT/ATAN are five next read-only candidates; ATAN2 belongs to ATAN. Native Go-bit trig differs from TiKV libm even for ordinary inputs, so a future batch must move the Go-compatible implementation to one shared owner while preserving wire/legacy policy, not silently substitute libm. EXP/LOG10 and compression retain separate compatibility work.

Complete operation-scope coverage, allocation/high-water and physical-peak checks, paired differential reruns, full codec-domain equivalence, release performance, whole workspace, `make lint` and TiFlash integration remain unfinished. Full datatype, unistore and parser-charset suites were not rerun; historical non-green results are not passing evidence. Kernel reuse is not a complete Go-package transcreation claim.
