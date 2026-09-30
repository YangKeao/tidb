# Expression unification experiment

Checkpoint-ID: `trim-subidx-pad-four-19` (previous: `case-sha2-ord-four-18`)

**59/245 families delegate to TiKV with their native evaluator algorithms removed; target 221.** This checkpoint adds TRIM, SUBSTRING_INDEX, LPAD and RPAD. Strict final-audited acceptance remains 0; this is not PR-ready.

## Paired repositories

- [YangKeao/tidb, expression-unification-demo](https://github.com/YangKeao/tidb/tree/expression-unification-demo)
- [YangKeao/tikv, expression-unification-demo](https://github.com/YangKeao/tikv/tree/expression-unification-demo)

Use sibling checkouts. `checkpoint.json` pins TiKV and the published Plan hash. Root Plans mirror `/home/agent/tidb/EXPRESSION_UNIFICATION_PLAN.md` at publication. Validated steps push both branches without force-push or automatic PRs.

## Trim, split and pad ownership

TRIM keeps AST source-evaluation/coercion before removal-evaluation/coercion, versus typed evaluation of both children before coercion. All three directions, default space, NULL and empty removal enter TiKV. One trim core retains native sequential-right versus wire independent-right overlap behavior.

SUBSTRING_INDEX keeps original string/count preparation but deletes native splitting. Its dedicated ready role preserves signed/unsigned bits, native MIN and forward non-overlapping suffix policy; the wire reverse scanner is unchanged. Actual NULL count stays NULL, distinct from a checked undemanded count lowered to an irrelevant non-NULL representative.

PAD keeps length cast/1292, packet/1301, range, then two-string coercion. Binary selection is source OR pad. Both strings are either real evaluated values or explicitly undemanded together; only NULL count, suppression or out-of-range count permits undemanded strings. Zero/valid width still demands both. Real four-column recipes are whitelisted only for the four pad operations: caller limits are five nodes for pad, four otherwise, with depth three unchanged. Driver, pool and general graph admission are not widened.

One TiKV quotient/remainder core constructs and truncates results. Native keeps empty-pad-growth empty and full16MiB character-width admission; wire keeps NULL and UTF8's *4 limit. Native equality is safe while the original wire nonzero equal-length empty-pad division path remains; original wire SUBSTRING_INDEX MIN abs behavior is also retained. These known wire defects are not silently fixed or recreated with artificial panics.

## Actual validation

- TiKV local: 215 passed/1 ignored; original string tests: 63 passed.
- Native dispatcher retry: 3 passed; SQL/lifecycle: 41 passed.
- Full expression: 1406 passed/4 unchanged failures/94 ignored, 1504 discovered. Complete failure blocks match18 after only thread-ID normalization.

Before testing, review caught and corrected the missing pad-only five-node caller limit. The first native compile then failed on two new tests using nonexistent crate-root Expression paths; only those paths were corrected. That initial chain ran no tests or later SQL/full commands. Old expected values were not changed.

SQL uses four normal stored rows with ten results, stored signedMIN/UIntMAX checks, four text packet refusals, two binary257-byte successes and ten direct zero-slot calls. One native test actually constructs12,582,915 bytes for a4,194,305-character pad result; it checks length/characters/ends/dispatch without a giant fixture. Exact commands: `logs/trim-subidx-pad-four-summary.txt`; boundaries: `evidence/trim-subidx-pad-four-checkpoint.md`.

## Next work and exclusions

LN, LOG(both arities), LOG2, POW/POWER, UNCOMPRESSED_LENGTH and INSERT are the next parallel batch, not yet credited. EXP/LOG10 use distinct native Go algorithms; COMPRESS has distinct encoded bytes; UNCOMPRESS needs a typed diagnostic outcome and bounded inflation. They cannot be claimed through thin wrappers alone.

No new PB/unistore admission was added, and full unistore was not rerun. Operation-scope guards, physical peak/OOM safety, allocator remeasurement, paired differential reruns, full workspace, make lint and release performance remain unverified. Kernel reuse is not complete Go-package transcreation.
