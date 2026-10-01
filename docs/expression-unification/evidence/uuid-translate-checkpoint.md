# UUID inspection/conversion and TRANSLATE checkpoint

**uuid-translate-six-41**, following `format-time-three-40`: functional **131→137/245**, final acceptance **0/245**. Six families: IS_UUID, UUID_TO_BIN, BIN_TO_UUID, UUID_VERSION, UUID_TIMESTAMP, TRANSLATE. **11 test-Cargo attempts =10 actual nonzero-test runs (8 green +2 current baseline non-green full runs) +1 compile failure.** New test RED0, zero-match0, launch failures0, failed-target retries1; **all_checks_firstpass=false**. Commands/results and11 whole-log SHA256 values: [`../logs/uuid-translate-summary.txt`](../logs/uuid-translate-summary.txt).

Parent source scope **15 Rust files (TiKV10/native5)**; zero manifest edits, both locks byte-identical to HEAD and metadata commands0. Final pinned --check across15 sources and both repository diff checks passed, **check_failures0**, but this does **not** mean formatting had no failed invocation. Source/gates/proofs are parent-owned; writer read/hashed11 logs and wrote only these two docs, not B's separate metadata deliverables.

**Compile failure retained:** first tikv-local attempt exited101 with E0061 at three new C-test constructors: Bytes3(a,b,c) incorrectly supplied three arguments to the one-array variant Bytes3([a,b,c]). Parent corrected those three constructors only; no expected value or business change. No tests ran in that attempt. One same-target retry then passed272, with1 preexisting ignored test.

**Formatting invocation failure1/retry1:** parent mistyped tidb/rustates/nonexistent.rs; the failed invocation exited1 without touching files. Correct adapter-tests path then succeeded. **Static-proof check failure1:** parent proof script guessed nonexistent Rust func_prop.rs; glob confirmed no such file, and the corrected proof covered only real scalar_function.rs/arg_eval_type.rs. No source was changed to satisfy the proof. These are separate from final --check failures0 and from Cargo launch failures0.

Other disclosed issues: parent/7 ordinary lookup misses2 (baseline path omitted evidence/, guessed local/error.rs), plus writer's earlier RO guessed codec/mysql/json/native.rs miss1: **ordinary lookup total3**. Three precompile interface fixes (B Bytes3 tuple→array; B helper crate datatype→expr; owner7 EvaluateError root→error-module) are distinct from the later three C constructors' single compile-failure attempt. Parent verified the original misc/string2 complete test modules and seven host UUID/v4/v7/hash/coercion bodies byte-identical; old expectations were not changed.

**Nine operations reuse old roles:2 Int +1 Decimal +6 Bytes**, without new input/result DTO, report, driver, module or PB/cop admission. Swap requires actual parsed16 bytes and an actual present flag; NULL uses the genuine existing witness, not dummy bytes. Five typed causes are authenticated against the exact operation, then native rendering borrows the actual input Vec to preserve the old four Unsupported outcomes/1411; it does not infer causes from codes/messages or reparse/replay input.

The shared UUID parser retains32/36-byte,45-byte case-insensitive URN and38-byte arbitrary-shell selection before using the existing uuid decoder; manual hex parsing is removed. Formatting reuses Uuid.hyphenated. Native v1/v6/v7 timestamp math retains signed microseconds, truncation toward zero and six places; other versions return NULL. Wire and native share Decimal shift/round leaves but keep the wire parser/unsigned policy. Native parse/hex/swap/timestamp/decimal-format bodies were removed. Host UUID generation only gains thin common formatter/epoch helpers (two public helpers), **not generator-family credit**.

UUID_TO_BIN returns the real parsed16-byte value from one complete worker call before original flag coercion and a second complete swap call: explicit extra lease/transport, **not double parsing**. BIN_TO_UUID retains flag warnings before NULL. TRANSLATE's sole CPP rune/byte maps preserve first duplicate wins, excess-from deletion and unmapped data; any binary argument selects byte policy, while only arg0 determines binary result metadata. Original strict NULL short-circuits suffix coercion into a real NULL witness, never fabricated three-NULL operands. **Preparation still occurs before the guard/capability boundary; its work is not claimed resource-bounded by worker admission.**

D4 covers all9 actual workers, AST/typed/error paths and zero/closed resources. E3 includes12 direct zero-slot SQL calls, original metadata/exact warnings and Resource1105 versus UUID1411 distinctions, including genuine nonbinary Latin1 FF demand. Added hand-derived literals are explicitly original-policy-derived, not new-provider output or mislabeled old fixtures; direct old tests stay intact. DATE, MICROSECOND and larger JSON-path work remain deferred.

## Current logs (Finished/test timing is not a benchmark)
| `uuid-translate-` suffix | Passed / failed / ignored; filtered | Finished / test seconds; exit |
| --- | --- | --- |
| **tikv-local.log** | compile E0061×3;no tests | unavailable / not run;**101** |
| tikv-local-retry.log | 272 / 0 / 1 old;509 (273 discovered) | 10.36 / 0.19;0 |
| tikv-misc.log | 22 / 0 / 0;760 | 0.13 / 0.00;0 |
| tikv-string.log | 66 / 0 / 0;716 | 0.12 / 0.02;0 |
| native-misc.log | 13 / 0 / 0;1559 | 15.02 / 0.00;0 |
| native-string.log | 1 / 0 / 0;1571 | 0.16 / 0.00;0 |
| native-tables.log | 1 / 0 / 0;1571 | 0.12 / 0.01;0 |
| dispatch.log | 4 / 0 / 0;1568 | 0.15 / 0.00;0 |
| sql.log | 89 / 0 / 0;2078 | 28.41 / 1.42;0 |
| **expr-full.log** | 1474 / **4 old** / 94;0 (1572 discovered) | 0.17 / 10.39;**101** |
| **unistore-full.log** | 195 / **1 old** / 13;0 (209 discovered) | 6.66 / 3.01;**101** |

Parent compared both **complete current failure sections to Round41 with ONLY panic-heading thread IDs normalized**. Expr hash `25f964887541f45602363f00dc60df4f0af21c74edccc16c38d0804094fb9bff` retains the same four failures, including duration.rs:**164:55**. Unistore hash `e285bfdba646f2d01f85b485ae317cc07c51b78cf0ae6d664c0fb2bc39259759` retains DECIMAL-'abc' at source**194:5**. **NO ADDRESS MAPPING THIS ROUND.** Round41→40's old164→212 mapping is history, not this comparison. These are failure-section hashes, not whole-log hashes; both current full suites remain non-green.

Round39 parser-all E0061 remains historically unrepaired and unrun this round, not a present gate. Full workspace/Go-package acceptance, lint, release/profile, allocator/OOM peak, performance/zero-copy, FIPS, M6 and PR readiness remain unestablished. No new writer source audit/build/test/fmt/Git/index/Plan. Functional **137/245**, final **0/245**; no all-first-pass or all-suite-green claim.
