# SUBSTRING and shared GB ownership — substring-gb-21

Functional delegation/native-deletion progress: **66/245**, up from65 by **one SUBSTRING family**; final acceptance remains **0/245**. SUBSTRING, SUBSTR and MID, both actual arities, four existing PB signatures and four legacy signatures are included. GB ownership deduplication adds no evaluated-family credit and establishes neither another completed Go package nor whole-codec equivalence.

## SUBSTRING ownership and entry policies

One TiKV range/slicing core carries explicit wire, native and legacy policies. Native two arguments mean the true tail operation, not a manufactured MAX length: SUBSTRING('abcd',2) remains 'bcd', while native SUBSTRING('abcd',2,MAX) retains its positive-length overflow-to-empty behavior. Wire saturation/strict UTF8 behavior and legacy unchecked range behavior remain distinct.

Native argument demand preserves its original NULL-before-coercion rule; otherwise casts occur pos, optional length, then source. The two-argument string2 entry keeps NoColumns as its **cast** context while using the actual **execution** context for C4. Enabling the execution capability must not introduce its formerly discarded1292 warning. Native text retains per-invalid-byte Go normalization and original text/binary result tags.

PB preserves its child-NULL stop: earlier evaluated values are still uncoerced and later children remain undemanded. Non-NULL calls keep the original source reader before the native helper's casts. Actual arity2/3, not the nominal PB signature alone, selects the recipe; no new signature-arity lock or PB admission is introduced.

Legacy source/position/length demand remains ordered. A shared boolean needs-length helper authorizes evaluating length but returns **no SQL answer**. Dedicated ready roles transport genuine i128 values as16 little-endian bytes, retaining NULL versus Undemanded rather than narrowing wide values or inventing NULL. Only after demand validation may an undemanded physical operand use an irrelevant zero representative. Legacy position0 and out-of-i64-range values retain NULL, source NULL suppresses position evaluation, and an out-of-range start suppresses length. Rust-grouped lossy UTF8 and the original unchecked addition/natural slice behavior remain. Every admitted two-/three-argument NULL, empty and value result reaches real C4; legacy int/bytes consumers preserve typed infrastructure failures.

The final source uses character iterators for wire/native UTF8 output; only legacy retains a real unit slice for its natural bounds behavior. Parent's adjustment avoids introducing that Vec<char> allocation into wire calls. This was source-reviewed, **not allocation- or performance-measured**. General graph, driver, pool and fixed-four-column admission are not broadened.

## SQL coverage checked from source

The main query has **nine rows by four columns**: text/binary, each at two and three arguments, with source width16 and original collation metadata. It covers NULL, zero/negative/out-of-range positions, zero/negative/NULL lengths and positive-length overflow. A separate four-column row checks SUBSTR/MID aliases. Stored UIntMAX retains native ETInt bits as-1; a latin1 E2 82 41 fixture verifies Go's per-bad-byte normalization rather than the legacy grouped replacement. Two-/three-argument 'bad' positions distinguish silent NoColumns casting from warning1292.

Nine direct zero-slot refusals cover legal NULL, empty, ordinary and overflow-empty results across the aliases. They preserve PoolResource/Pool1105/HY000 and the three-argument1292 diagnostic; returned evaluation-origin1105 is not a warning-buffer Error row. No packet policy or new PB/unistore admission is added.

## GB foundation: shared owner, explicit differences

The frozen [GB foundation evidence](gb-shared-foundation.md) is the authority for canonical hashes, data provenance, generator ownership and source-policy review; it is not edited by this checkpoint. Four former native artifacts (two CI images and two GB18030 lookup maps) are removed through their generators. Four GB compare/key implementations and the GBK/GB18030 encoding leaves now delegate to TiKV; native mode/transform/first-error policies remain outside the shared workers where required.

- All four canonical collation images retain their hashes/bytes. Native GBK LE versus canonical BE CI weights have0/65,536 numeric differences; GB18030 CI has0/1,114,112 differences and is byte-identical. Existing hash/source oracles are retained, not replaced.
- Canonical GB18030 data retains2,103 pairs and its recorded hash. The native2,094-pair subset excludes exactly nine wire-only pairs and uses native codec fallback; only a derived rune index is added, not a second complete mapping table.
- Native encoded-byte BIN comparison is not wire numeric-weight comparison and is not implemented by comparing public keys. The36 differing slots are an **override subdomain**, not a full-Unicode comparison. The19 original PUA key cases retain their key-only trailing NUL, while comparisons omit that added NUL. Euro, malformed grouping, NoPad/PAD SPACE, disabled new-collation mode and the old truncated-prefix MbLen panic keep their policies.
- Native codec alias `encoding_rs =0.8.35` remains distinct from wire git0.8.29 at68e0bc5a72a37a78228d80cd98047326559cf43c. Parent's normal Cargo-tree lock resolution retained all TiDB git pins; TiKV's registry0.8.33→0.8.35 update also reselected small cfg-if reference edges. This is not claimed to have zero incidental resolver changes.

Both parent generator checks exited0. The charset command was **--check-gb**, only a GB check, not a full parser-generator audit. Four dependency-preparation lock logs are separate from the sixteen validation receipts below.

## Actual validation and failure boundaries

[Sixteen receipts and exact parent commands](../logs/substring-gb-summary.txt) retain successes, failures and non-runs. Parent used the pinned January TiKV and August TiDB wrappers; this documentation task ran no tests or builds.

- TiKV collation21/370 filtered, local224/1 ignored/469 filtered, official string63/631 filtered; native datatype --lib436 passed.
- Initial shared-collation invocation selected nonexistent Cargo test target shared_collation_contract: exit101, **zero tests**, subsequent commands in that attempt not reached. This was target selection, not a Rust compilation failure. The --test all filtered retry passed14/80 filtered.
- Full parser-charset filter:9 passed/4 failed. The unchanged default-collation assertion reports supported7 versus defaults5; its lock poisoning causes three subsequent failures. The two new GB tests and original encoding tests passed.
- Isolating that exact old assertion in the **same built artifact** still fails0/1,93 filtered. Skipping just it yields12 passed/82 filtered. This is a diagnostic skip, not a green full parser gate or a changed oracle. Parent/Da7 checked charset.rs, registry tables, original flag initialization and the old assertion as HEAD-unchanged, and found no global writes in the new tests. **No complete old HEAD artifact was run**; none is claimed as a baseline.
- Expression dispatch3/1508 filtered; legacy substring2/191 filtered; SQL lifecycle45/2078 filtered passed.
- Full expression:1413 passed/4 failures/94 ignored,1511 discovered,10.49s, exit101. Full unistore:179 passed/1 failure/13 ignored,193 discovered,2.98s, exit101. Parent compared the complete failures sections against both round20 casing-repair full logs: byte-identical after only thread-ID normalization. Both full suites remain non-green.

There were no product-compilation error retries in this round: do not import the previous round's E0308/E0004 history into these receipts. Existing expected values and fixtures were not changed to pass the gates. Parent froze/pin-formatted the source and reported git diff --check exit0; that check is not runtime validation.

## Not established

Whole-codec cross-domain equivalence, completion of the subsequent search/collation function batch, complete operation scopes/guards, allocator remeasurement, physical peak/OOM guarantees, performance, full-workspace validation, make lint and final acceptance remain unverified. Selected GB gates and one additional function-family credit do not imply PR readiness.
