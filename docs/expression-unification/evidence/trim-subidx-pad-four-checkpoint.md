# TRIM, SUBSTRING_INDEX and padding — trim-subidx-pad-four-19

Functional delegation/native-deletion progress: 59/245; final acceptance: 0/245. TRIM, SUBSTRING_INDEX, LPAD and RPAD are four complete functional families on existing admitted surfaces, covered by nine operations. The source cut covers nine TiDB and eight TiKV files, not completion of the overall unification target.

## Changes and domain coverage

TRIM uses real Bytes2 arguments and three fixed directions. AST source evaluation/coercion still precedes remstr evaluation/coercion; the typed entry still evaluates both expressions before coercing either. One TiKV trim core preserves native sequential right-boundary trimming versus wire independent boundaries, including the overlapping-pattern difference. SUBSTRING_INDEX has its own ready role and validated count marker, distinct from actual NULL; UInt bits, MIN and negative overlapping matches retain their policies in one scan core, without a copied native split algorithm.

PAD validates both string demand markers together. Only NULL length, packet suppression or range rejection authorizes Undemanded; its physical Some(empty) is an irrelevant representative, not SQL NULL. Valid length0 still demands both string conversions. The original order remains: length cast/warning1292, packet estimate (negative to u64MAX; text n*4 versus binary n), range check, then both tuple conversions. Either source or pad selects binary mode.

Only the four PAD operations admit four physical columns: three guards whitelist them, two inline ready-value array sites hold four slots, and the caller uses max_nodes5 for PAD versus4 otherwise, retaining depth3. Driver, pool and general graph admission are unchanged. One TiKV quotient/remainder core preserves wire strict `<` truncation, empty-pad growth to NULL and UTF8 n*4 limit, versus native `<=`, empty-pad growth to empty and n limit. Existing wire nonzero equal-length/empty-pad natural0/0 and SUBSTRING_INDEX MIN abs behavior are retained, neither repaired nor simulated with artificial panic branches.

## SQL and actual large-output coverage

The lifecycle source explicitly asserts four normal rows by **ten columns**, not sixteen: five TRIM, one SUBSTRING_INDEX and four PAD expressions. Result widths/collations and raw bytes are checked. A separate three-column query verifies stored MIN/UIntMAX. Two small packet1024 rows (NULL source/n=-1 and non-NULL/n=257) drive four text calls with warning1301 plus NULL; the separate binary257 query checks **two** successful results, source-binary LPAD and pad-binary RPAD. Ten direct zero-slot refusals include NULL length and packet suppression, preserving warning1301 separately from returned typed1105.

The dispatch size test performs one real LPAD('尾',4194305,'你'): 4194305 characters, 12582915 bytes, checked prefix/suffix and one facade/kernel invocation, without a giant expected fixture. This is one LPAD size case, not evidence that all four PAD operations were covered by that test; C/local and SQL coverage exercise the four operations.

## Actual validation and corrections

Parent ran the pinned January TiKV and August TiDB wrappers. [Commands and exact result lines](../logs/trim-subidx-pad-four-summary.txt) retain all six receipts.

- TiKV `local::`: 215 passed/1 ignored/469 filtered.
- TiKV official string tests: 63 passed/622 filtered.
- Initial dispatch attempt: compile exit101, two E0433 errors, zero tests; the chain stopped.
- Dispatch retry: 3 passed/1501 filtered.
- Session lifecycle/SQL filter: 41 passed/2078 filtered.
- Full expression suite: 1406 passed/4 existing failures/94 ignored; 1504 discovered, exit101. Parent compared all four complete failure blocks against case-sha2-ord-four-18: identical after only thread-ID normalization. The suite remains non-green.

Parent corrected the missed PAD-specific factory node allowance during pre-gate review; no failed run is attributed to it. Separately, the recorded dispatch compilation failure was repaired by changing only two new-test paths from `crate::Expression` to `crate::expression::Expression`. That path repair changed neither runtime nor pre-existing expected values. Retry/SQL/full receipts are subsequent runs, not execution by the failed initial chain.

## Review and not verified

Parent reviewed shared-helper policies and native algorithm deletions. No new PB/unistore admission was added; unistore was not rerun in19. Complete business-wrapper operation scopes/guards, allocator remeasurement, physical peak/OOM guarantees, release performance, full-workspace validation, make lint and final acceptance remain open. These receipts do not establish PR readiness.
