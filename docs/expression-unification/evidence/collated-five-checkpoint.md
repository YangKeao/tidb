# Collated search and set lookup — collated-five-22

**Final functional checkpoint: ten parent receipts; the full expression suite remains non-green.**

The batch migrates **five SQL spellings/surfaces**—STRCMP, LOCATE, INSTR, POSITION and FIND_IN_SET—but adds **four frozen families**: `strcmp`, `locate`, `instr` and `find_in_set`. POSITION belongs to `locate`; INSTR remains separate. After the core/native/SQL gates, corrected functional delegation/native-deletion credit advances **66→70/245**. Final acceptance remains **0/245**. The shared native selector adds no evaluated family or completed Go-package claim. No new PB/unistore admission is introduced.

## Pre-publication ledger correction

Parent's publication-ledger membership assertion rejected the duplicate POSITION alias **before any publication**. The frozen [coverage baseline](coverage-baseline.json) maps position→locate at line46; its locate entry at line233 lists both spellings and explicitly normalizes POSITION syntax to LOCATE while keeping INSTR separate. The planned71/245 was therefore corrected to70/245: four added IDs, no new position ID, no change to the frozen denominator, previous ledgers, code or tests. The name **collated-five-22** is retained for five SQL spellings, not five families. This is a failed ledger-validation assertion and its correction, not a compilation failure or an additional Rust runtime-test receipt.

## Source scope and preserved policies

Native call sites use the real execution context and closed C4 recipes. Derived-collation callers remain distinct from value-only collation selection. Nullable/demand records preserve the original coercion order; legal NULL, empty and ordinary answers are not native result shortcuts.

- STRCMP uses its explicit native comparison policy, not FIND's runtime new-collation-mode lookup. FIND uses **NoPad-key equality**, not wire compare(false): with general_ci, a one-space needle in '  , , ,' first matches native field2 rather than wire field1.
- LOCATE/INSTR preserve byte versus UTF8 units and their operand order. Typed UTF8 preparation remains strict; typed three-argument CI search retains Go simple-lower before bounds/comparison. The already-text POSITION helper uses UTF8 units even with Binary comparison, while SQL POSITION grammar rewrites to LOCATE; those entry surfaces must not be conflated.
- The separate extension LOCATE3 path retains wrapping_sub(1), Go per-invalid-byte normalization and exact equality. Its former StrUnits boundary/slice/search algorithm is removed; all three original coercions remain demanded, including after an earlier NULL. It does not acquire the main typed helper's collator algorithm.
- Only **Locate3Native** adds a new four-column recipe in this batch: two byte operands, position and policy. Existing pad/INSERT allowances remain; no generic four-column, graph, driver or pool broadening is claimed.

## FIND preparation, cache ownership and costs

The pure TiKV builder produces an opaque prepared-key owner, **not a SQL answer**, and is not a worker dispatch. Native code retains neither its old HashMap/search/split/key loop nor a raw list to re-key per row. Build-time NoPad keys remain a permanent snapshot, including first-position semantics.

The existing context-id-only cache is retained: evaluate the needle expression first; evaluate/coerce/build a constant list once with Row::empty even if that needle is NULL; do not cache constructor errors; a new context replaces the entry and Clone starts empty. Cached NULL suppresses only needle coercion, not its earlier expression evaluation. An empty non-NULL cache still coerces the needle and, for a non-NULL needle, requests its key. Dynamic FIND still coerces both operands in order, so left NULL does not hide a right coercion error.

Build and probe each resolve get_collator(...).new_collation at their original demand point: Some selects native_policy; None selects Binary for raw NoPad keys. A later probe-mode change does not rebuild cached keys. Real NULL is carried as NULL; only cached-NULL demand permits an Undemanded needle.

PreparedFindInSetKeys uses Arc<Vec<u8>>: cloning the owner is cheap, but the shared-owner path **copies encoded keys into the Vec input column**. This is not zero-copy or allocation-free lookup. Replacing the old HashMap lookup with ordered scanning has **unverified performance**, and no allocator/physical-peak conclusion follows from source review.

The dedicated native pure-builder wrapper keeps genuine LocalError as opaque ExpressionRuntimeFailure with **phase None**. It does not invent worker Prepare/Invoke attribution, a SQL site, or SQL overflow. No native replay/fallback is added after a C4 error.

## Shared native selector

TiKV NativeCollation owns sixteen explicit checked tags, independent of registry/signed wire IDs or the global mode; seven CI identities retain their search policy. The former native algorithm selector is removed, leaving identity/mode preparation in the native facade. Existing shared GB ownership is reused, not duplicated. Pinyin remains its original panic stub, not implemented support or a binary fallback. DerivedBinary LIKE still uses the no-padding UTF8 rune matcher, not Binary's byte matcher.

## Validated native and SQL coverage

The three collation_search_dispatch_ tests passed; the16-test string2 gate includes both new adapter regressions. Both new session tests passed within the47-test lifecycle gate. The main query has **five rows by twelve columns**, including text/binary comparisons, search directions/arities, SQL POSITION, dynamic FIND and a constant-list cache. Original signed integer result metadata is checked: widths [2,2,20,20,20,11,11,20,20,3,3,3], decimal0 and derived collations. The fixtures distinguish e/é, first duplicates, NoPad spaces, empty lists and the typed LOCATE3 simple-lower treatment of ẞ.

The second session test passed **21 direct zero-slot probes**: seven expressions across NULL, ordinary and empty rows, including LOCATE3 and cached FIND. They retain PoolResource/Pool, evaluation-origin1105/HY000, without an Error warning row. Original expected values and fixtures are not changed.

## Ten actual receipts and the non-green full suite

[Exact parent commands and result lines](../logs/collated-five-summary.txt) distinguish all ten receipts:

- TiKV datatype collation:25 passed,370 filtered,0.01s, exit0.
- Native datatype --lib:436 passed,0 filtered,17.11s, exit0; shared-collation integration filter:14 passed,80 filtered,0.00s, exit0.
- TiKV **local::tests subset**:66 passed,0 ignored,632 filtered,0.02s, exit0. This valid extra receipt is neither the complete local scope nor a failed/zero-test attempt.
- Official string:63 passed,635 filtered,0.02s, exit0.
- Separate **local::** run:228 passed,1 ignored,469 filtered,229 discovered,0.18s, exit0. It includes the subset; do not add66 to228.
- Native dispatch:3 passed,1513 filtered,0.00s; extension:16 passed,1500 filtered,0.01s; SQL lifecycle:47 passed,2078 filtered,0.68s; all exit0.
- Full expression:1418 passed,4 failed,94 ignored,0 filtered,1516 discovered,10.39s, **exit101**.

Parent compared the entire current failures section against substring-gb-expr-full.log: byte-identical after **only thread-ID normalization**, with reported normalized-section SHA256 `fb665bb70597cfff204649928faef5a1284edcae0a2f1ecd275295b3b36e7cb3`. This is not a whole-log hash. The four failures remain ifnull pushdown, EXP overflow expectation, vectorized operator duration-FSP panic and partial STR_TO_DATE; their full names/blocks remain in the receipts. The current full suite is not green.

The first January expression compilation completed in10.72s. No actual compilation failure or runtime repair occurred in this round. Two new lifetime-syntax warnings and the existing substring lifetime warning remain in the logs; source was not changed/rebuilt merely to silence them. Parent reports both repositories' git diff --check and both lockfile git diff --exit-code checks exited0: no lock changes or new pins. These checks are not additional test receipts.

## Source-review corrections and unexecuted scope

Parent's proposed CI3 malformed-UTF8 domain expansion was rejected after B checked the original strict coerce_str path and its diff; source was not changed according to that mistaken hypothesis. A's provisional into_int callback name was corrected during read-only review to the existing into_int_datum converter before compilation. Neither event is a fabricated compiler/test failure or a changed old oracle.

Unistore, parser and generators were **not rerun in this round**. Round21's parser7-versus5/poison and unistore1 failures remain historical non-green observations, not new passing gates or proof about this binary. The expression comparison above does use the fresh current run.

Whole workspace, make lint, performance, allocator remeasurement, complete scope/guard coverage, physical peak/OOM guarantees and final acceptance remain unverified. Functional credit is **70/245**, final acceptance **0/245**; this is not PR-ready. Source files remain frozen; this documentation task runs no builds and changes no Plan/index or earlier evidence.
