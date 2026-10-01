# SQLCrypt, Vitess hashing and scaled-format checkpoint

**crypt-hash-format-six-42**, following `uuid-translate-six-41`: functional **137→143/245**, final **0/245**; ENCODE, DECODE, TIDB_SHARD, VITESS_HASH, FORMAT_BYTES, FORMAT_NANO_TIME only. **24 raw receipts =22 test-Cargo attempts (all actually ran nonzero tests) +2 successful offline lock resolutions**; test outcomes **19 green (3 pre-expansion +16 final-current),1 new test RED,2 current baseline non-green full runs**. Compile/launch/zero-match failures0, failed-target retries1, metadata commands0; **all_checks_firstpass=false**. Exact commands, pins and24 whole-log SHA256: [`../logs/crypt-hash-format-summary.txt`](../logs/crypt-hash-format-summary.txt).

**New RED retained, not product repair:** first tikv-local failed `crypt_hash_format_dispatch_getters_shapes_and_private_admission`: C's new direct-physical +Inf probe wrongly required success, reaching the original scientific `expect` panic at impl_miscellaneous.rs:272:10. Parent changed only that new test to catch_unwind/assert infinity panic, preserving None/NaN success and actual-invocation assertions; **new test expectation corrections1**, old fixtures/expected unchanged. Production format_scaled/fixed/scientific bodies remained byte-identical to the original native bodies. Original failed log is retained; one retry passed274 +1 old ignore. Do not claim newRED0 or formatter totality over all IEEE values.

Parent scope **22 Rust files (TiKV13, including3 new; native9),2 manifests,2 generated locks**. Final22 pinned-format checks and both diff checks passed; check/static-proof/lookup failures0. Sole source-precontract correction1: parent's Bytes2 array notation corrected to the existing **two-option tuple**, not a compile failure. CPP lock adds cipher0.5.2, crypto-common0.2.2, des0.9.0, hybrid-array0.4.13, inout0.2.2; upgrades typenum1.16.0→1.20.1; digest0.10.7 only disambiguates its old crypto-common0.1.6 edge. Native lock changes no package versions, only util/querycrypto edges. **Actual January5-test leaf gate passed; dependency compatibility is not pending.**

Shared leaf owns original SqlCrypt, RustCrypto DES and normalized MySqlRand. Parent subsequently found native mathutil/rand's third recurrence with **raw setters**: all now share one public **mysql_rand_step(&mut u32,&mut u32)->f64 using wrapping arithmetic**. Normalized CPP inputs retain the original ordinary-arithmetic result; native mutex/time/seed/raw setters/getters stay unchanged, only gen delegates. Three early green leaf receipts predate this expanded closure; three final replays close it successfully and are **not failed-target retries**; native RNG3/SQL RAND2 are extra actual gates. **No RAND-family credit**; native-util's three public thin facades preserve API/tests, not a new whole-Go-package transcreation claim.

Seven closed operations reuse old roles/results; no new argument/result DTO, report, cause, context/driver, PB/native-cop admission. Crypto prepares outside admission, selects one complete worker (not a selected-driver API), preserves genuine NULL witness without fake password, original ENCODE/DECODE direction and new_string metadata. Hashing and %256 execute in CPP; full64-bit results use signed C transport then UInt-bit packing. Formatting uses old nullable LE8 IEEE bits, original units/absolute thresholds/-0/scientific cutoff and nonfinite behavior, including original infinity panic, with no finite filter. **Crypto preparation remains outside guard/capability coverage; hash/format coercion remains inside its guard.** Native business bodies are removed; public utility helpers are thin shared facades.

Parent byte proofs preserve7 old test modules,3 formatter bodies and native RNG bodies other than gen. Added CPP5 kernel +2 local +3 leaf tests, D3 and E3/12 direct zero-slot SQL cases use original literals or clearly labeled hand-derived policy cases, never provider-recorded oracles. New C expectation correction is disclosed separately from unchanged old fixtures. Writer only read/hashed24 logs and wrote these two docs; B owns separate metadata deliverables.

## All raw receipts (`crypt-hash-format-` prefix; Finished/test times are not benchmarks)
| Suffix / phase | Passed / failed / ignored; filtered | Finished / test seconds; exit |
| --- | --- | --- |
| tikv-lock.log / non-test resolution |5 added +1 upgraded; no tests |not applicable;0 |
| native-lock.log / non-test resolution |0 package-version changes; no tests |not applicable;0 |
| tikv-crypto.log / early |5 / 0 / 0;0 |0.85 / 0.00;0 |
| native-crypt-util.log / early |2 / 0 / 0;575 |10.25 / 0.00;0 |
| native-vitess-util.log / early |1 / 0 / 0;576 |0.11 / 0.00;0 |
| tikv-crypto-final.log / final replay |5 / 0 / 0;0 |0.23 / 0.00;0 |
| **tikv-local.log / new RED** |273 / **1 new** / 1 old;514 (275 discovered) |11.96 / 0.19;**101** |
| tikv-local-retry.log / final retry |274 / 0 / 1 old;514 (275 discovered) |4.08 / 0.19;0 |
| tikv-encryption.log / final |13 / 0 / 0;776 |0.12 / 0.00;0 |
| tikv-misc.log / final |25 / 0 / 0;764 |0.12 / 0.00;0 |
| tikv-rand.log / final |4 / 0 / 0;785 |0.12 / 0.10;0 |
| native-crypt-util-final.log / final replay |2 / 0 / 0;575 |3.42 / 0.00;0 |
| native-vitess-util-final.log / final replay |1 / 0 / 0;576 |0.11 / 0.00;0 |
| native-rng-util.log / final |3 / 0 / 0;574 |0.11 / 0.00;0 |
| native-crypto.log / final |15 / 0 / 0;1560 |15.28 / 0.01;0 |
| native-crypto-source.log / final |16 / 0 / 3 old;1556 (19 discovered) |0.12 / 0.01;0 |
| native-misc.log / final |13 / 0 / 0;1562 |0.12 / 0.00;0 |
| native-info.log / final |3 / 0 / 0;1572 |0.14 / 0.00;0 |
| native-info-source.log / final |2 / 0 / 0;1573 |0.12 / 0.00;0 |
| dispatch.log / final |3 / 0 / 0;1572 |0.12 / 0.00;0 |
| sql.log / final |92 / 0 / 0;2078 |32.06 / 1.39;0 |
| sql-rand.log / final |2 / 0 / 0;2168 |0.14 / 0.03;0 |
| **expr-full.log / current baseline** |1477 / **4 old** / 94;0 (1575 discovered) |0.12 / 10.49;**101** |
| **unistore-full.log / current baseline** |195 / **1 old** / 13;0 (209 discovered) |7.98 / 3.01;**101** |

Parent compared **complete current failure sections to Round42 with ONLY panic-heading thread IDs normalized**. Expr SHA256 `25f964887541f45602363f00dc60df4f0af21c74edccc16c38d0804094fb9bff` retains the same4 failures, including duration.rs:**164:55**. Unistore SHA256 `e285bfdba646f2d01f85b485ae317cc07c51b78cf0ae6d664c0fb2bc39259759` retains DECIMAL-'abc' at source**194:5**. **NO ADDRESS MAPPING; both current full suites remain non-green.** These section hashes are not whole-log hashes; historic164→212 mapping is not used.

FORMAT's locale closure, DATE, MICROSECOND and larger JSON-path work remain deferred. Round39 parser-all E0061 remains unrepaired and unrun here, not a present gate. Whole workspace/Go-package acceptance, M6, performance/zero-copy, lint, release/profile, FIPS, allocator/OOM peak and PR readiness remain unestablished. No new writer source audit/build/test/fmt/Git/index/Plan. Functional **143/245**, final **0/245**.
