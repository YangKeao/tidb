# Expression unification experiment

Checkpoint-ID: `collated-five-22` (previous: `substring-gb-21`)

**70/245 families delegate to TiKV with native evaluator algorithms removed; target 221.** This checkpoint covers five SQL spellings: STRCMP, LOCATE, INSTR, POSITION and FIND_IN_SET. They add four frozen families: POSITION is already a LOCATE alias, not a fifth credit. The shared collation selector adds no extra family credit. Strict final-audited acceptance remains 0; this is not PR-ready.

## Paired repositories

- [YangKeao/tidb, expression-unification-demo](https://github.com/YangKeao/tidb/tree/expression-unification-demo)
- [YangKeao/tikv, expression-unification-demo](https://github.com/YangKeao/tikv/tree/expression-unification-demo)

Use sibling checkouts. `checkpoint.json` pins TiKV and the published Plan hash. Root Plans mirror `/home/agent/tidb/EXPRESSION_UNIFICATION_PLAN.md` at publication. Validated steps push both branches without force-push or automatic PRs.

## Ownership and preserved policies

Seven closed recipes share TiKV comparison/search/first-match primitives. Native STRCMP keeps value versus derived collation selection. LOCATE covers both arities and the independent extension entry; INSTR retains child order before swapping ready operands. POSITION retains its AST versus rewritten collation distinction, and its helper counts characters even with Binary comparison.

Native text search retains strict frontend UTF8 conversion and collation-aware windows. Only native LOCATE3 applies the existing Go simple lowercase for CI; its unchecked position decrement differs from extension LOCATE3's wrapping decrement, exact matching and Go invalid-byte preparation. Wire lowercase/memmem and original offset policies are not replaced with native behavior. Only `Locate3Native` joins the specific four-column/five-node whitelist; general graph admission and the driver are unchanged.

FIND_IN_SET uses NoPad **key equality**, not comparison. Both dynamic and constant-list paths reach the worker for NULL, empty and nonempty results. TiKV alone builds opaque prepared keys and searches them; native keeps the existing context-once cache owner, child demand, retries and invalidation. Build-time keys remain frozen while lookup samples its current policy. Cached NULL alone permits an undemanded needle; an empty non-NULL cache still converts and keys a non-NULL needle. The opaque owner clones cheaply, but transport still copies into the existing Vec column. Replacing native HashMap lookup with an ordered scan is **not a performance improvement claim**.

`codec::collation::native::NativeCollation` now owns the selector over existing compare/key, pattern, COW and capability primitives. Sixteen checked tags are semantic policies, not wire IDs. GB still uses the shared native compatibility policy; Pinyin remains the original stub. Registry/global-mode resolution stays native, and DerivedBinary LIKE retains rune semantics. Actual pure-key-builder errors retain their typed cause with an unattributed phase, not a fabricated worker Prepare/Invoke phase or SQL overflow.

No new PB or unistore signature admission was introduced. Existing function typing, coercion, diagnostics and output metadata stay frontend-owned. See [current evidence](evidence/collated-five-checkpoint.md) and the preceding [GB foundation](evidence/gb-shared-foundation.md).

## Actual validation

- TiKV collation: 25 passed; all local evaluator tests: 228 passed/1 ignored; original strings: 63 passed.
- Native datatype library: 436 passed; shared collation contract: 14 passed.
- New dispatch/cache checks: 3 passed; extension suite: 16 passed; SQL/lifecycle: 47 passed.
- New SQL coverage: five stored rows × twelve result columns, plus 21 direct zero-slot refusals.
- Full expression: **1418 passed/4 unchanged failures/94 ignored**, 1516 discovered, exit 101. Its complete failure block matches checkpoint21 after thread-ID normalization only.

The initial `local::tests` filter also passed 66 tests; it is a subset, not another 66 tests to add to the full local count. All targeted runs and first compilation passed without runtime repair. Two draft API/source-reading mistakes were corrected before compilation; no old expected value was changed. Both lockfiles remain unchanged. The pre-publication ledger check rejected a proposed extra POSITION family; the frozen alias map corrected the tentative count of 71 to 70 before either repository was published.

Ten exact command/result receipts: [summary](logs/collated-five-summary.txt). Unistore, parser-charset and generators were not rerun this checkpoint. Their earlier results are not current green evidence: the known unistore failure and parser default-registry assertion remain documented in checkpoint21. Whole workspace and `make lint` were not run.

## Next work and exclusions

Next candidates are OCT, CONCAT, CONCAT_WS and full-arity ELT. A dedicated packed variadic carrier or validated demand selector must cover the complete existing domain; a four-argument subset earns no complete-family credit. FIELD, MAKE_SET and CONV need additional compatibility work. EXP/LOG10 retain distinct native Go algorithms; COMPRESS has distinct encoded bytes; UNCOMPRESS needs typed diagnostic outcomes and bounded inflation.

Operation-scope completion, physical peak/OOM safety, allocation remeasurement, paired differential reruns, full codec-domain equivalence and release performance remain unverified. Kernel reuse is not a new complete Go-package transcreation claim. Historical checkpoint18/19 LOWER/UPPER completeness overclaims were corrected and repaired in checkpoint20, without extra family credit or rewritten historical commits.
