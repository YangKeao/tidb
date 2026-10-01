# REGEXP shared-foundation checkpoint

**regexp-foundation-44**, following `vector-eight-43`: functional **151/245 unchanged**, final **0/245**. This shares pure algorithms only: **REGEXP_LIKE/SUBSTR/INSTR/REPLACE receive no new evaluator/family credit**. Native callers still invoke shared leaves directly; cache-aware closed-RPN takeover is not implemented. Exact commands/pins and6 whole-log SHA256: [`../logs/regexp-foundation-summary.txt`](../logs/regexp-foundation-summary.txt).

**6 Cargo test attempts =6 actual nonzero runs =4 focused green +2 CURRENT baseline non-green full runs.** Compile/new executed-test RED/zero-match/launch/failed-target retry/supplemental replay/Cargo metadata/lock resolutions all0. **Focused first-pass=true only**; neither all-checks-green nor overall-first-pass success is claimed.

Parent scope **5 Rust files: CPP3 (new regexp_policy.rs, lib.rs, impl_regexp.rs), native2 (regexp.rs, builtin_ext/regexp.rs)**. No manifest/dependency/lock changes. Final5 pinned-format checks and both diff checks passed; formatter/static-proof failures0. Three complete old test modules are byte-identical, old tests/expected values unchanged; only3 hand-derived leaf tests were added, no provider-recorded oracle.

**Source tools were not failure-free:** ordinary lookup failures2 (owner7's wrong native path; A's nonexistent scalar_function/vectorized_eval.rs); observation-precondition edit failures2, one each for7/B, recovered by fresh-read retries2. Not sandbox denials; normal zero-match globs are not errors. Literal mismatch/presentation/ABI-guess corrections0. Round44's compile failures/omitted assertion remain history, not Round45 events.

One shared implementation now owns flags, character trim, nth matching/INSTR position count, single-digit backslash tokens, capture rendering and prefix-inclusive replacement. Six pure helpers/three typed enums; raw occurrence normalization exists only in the leaf (SUBSTR/INSTR max1, negative REPLACE→1). Native validates UTF-8 **after each selected replacement**, before a later capture error; wire retains raw replacement bytes. Actual typed errors map to original native static Unsupported versus wire dynamic diagnostics. The regex-library matcher was already shared; no pretend thin getter is counted as new execution ownership.

Native RegexBuilder keeps its original pattern representation; wire retains HashSet iteration/inline flags and Regex Debug diagnostics. Original empty/UTF-8/NULL/return-option/compile order remains: native positional tuple operands all coerce before their NULL test, and positional trim precedes compilation; wire compiles before positional validation. Context-id/constness success-and-error memoization, replacement cache and clone reset remain unchanged. Public statistics UTF-8/error→None behavior and existing PB/legacy special domains are untouched.

**No new C4 operation, Args/result type, six-slot extension, driver or cache protocol.** This is a necessary foundation, not complete four-family takeover or a whole Go-package/full-domain completion claim; allocation/performance/peak/OOM equivalence remains unproved.

## All raw receipts (`regexp-foundation-` prefix; Finished/test times are not benchmarks)
| Suffix / scope | Passed / failed / ignored; filtered | Finished / test seconds; exit |
| --- | --- | --- |
| tikv.log / focused |7 / 0 / 0;790 |8.03 / 0.04;0 |
| native.log / focused |7 / 0 / 0;1571 |10.82 / 0.06;0 |
| cache.log / focused |1 / 0 / 0;1577 |0.17 / 0.00;0 |
| sql.log / focused |2 / 0 / 0;2171 |23.90 / 0.04;0 |
| **expr-full.log / current baseline** |1480 / **4 old** / 94;0 (1578 discovered) |0.19 / 10.60;**101** |
| **unistore-full.log / current baseline** |195 / **1 old** / 13;0 (209 discovered) |5.69 / 2.99;**101** |

The actual SQL2 are `field_find_in_set_and_regexp_use_the_derived_collation` and `regexp_through_the_chunk_path`: existing REGEXP_LIKE/predicate rows only, **not a new positional-SQL or metadata-assertion gate**. Native focused7 comprise original root-regexp3 +positional4; CPP7 comprise original wire4 +new leaf3. Filtered gates overlap full suites, not additional distinct-test credit.

Parent compared **complete current failure sections to Round44 vector**, normalizing **only panic-heading thread IDs**: Expr SHA256 `25f964887541f45602363f00dc60df4f0af21c74edccc16c38d0804094fb9bff` retains all4 failures including duration.rs:**164:55**; Unistore `e285bfdba646f2d01f85b485ae317cc07c51b78cf0ae6d664c0fb2bc39259759` retains DECIMAL-'abc' at **194:5**. **NO ADDRESS MAPPING; both current full suites remain non-green.** These section hashes are distinct from whole-log hashes.

**Future design gap, not implemented:** actual shared cache state/typed errors with native owner and clone reset; private typed RPN metadata binding/unbinding plus pool-return cleanup; lazy initialization/miss writeback at original demand points. Existing unit-only payload/storage accounting cannot cover this state without a locked budget design. No host callback, fake Bytes or snapshot may stand in for shared state.

FORMAT/DATE/MICROSECOND/larger JSON paths/M6 remain deferred; historical Round39 parser-all E0061 was neither repaired nor rerun here. Whole workspace/Go-package acceptance, lint/release/profile/FIPS, performance/zero-copy/allocator-OOM peak and PR readiness remain unestablished. Writer only read/hashed6 logs and wrote these two docs; no fresh source/build/other audit/Git/index/Plan; B owns separate metadata. Functional **151/245**, final **0/245**.
