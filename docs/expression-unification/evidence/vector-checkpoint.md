# Native VECTOR checkpoint

**vector-eight-43**, following `crypt-hash-format-six-42`: functional **143→151/245**, final **0/245**. Eight families: VEC_AS_TEXT, VEC_FROM_TEXT, VEC_DIMS, VEC_L1_DISTANCE, VEC_L2_DISTANCE, VEC_NEGATIVE_INNER_PRODUCT, VEC_COSINE_DISTANCE, VEC_L2_NORM. Exact commands/pins and14 whole-log SHA256: [`../logs/vector-summary.txt`](../logs/vector-summary.txt).

**14 Cargo test attempts =12 actual nonzero runs (10 green:1 early +9 final-current;2 current baseline non-green full runs) +2 compile failures with no tests.** New executed-test RED0, zero-match/launch0; failed-target retries2 (both compile targets), successful supplemental replay1; **all_checks_firstpass=false**. Cargo metadata/lock resolutions0; all14 raw receipts are test commands, not non-test Cargo receipts.

**Both failed-first logs retained:** tikv-datatype E0282/E0283 at vector.rs:136:22 are one ambiguous box_err/map_err closure failure; parent added only `-> crate::codec::Error`. Tikv-local E0004 at local/tests.rs:2102:15 missed new ComputedValue::NativeVector in an old packet-role match; parent added it to the existing panic arm. Neither attempt reached Finished/test execution; each separate retry passed. These are not new test REDs or zero-match runs.

**Parent static-proof failure1/retry1; source_transcription_omission_restored1:** the moved old native datatype test module omitted `assert_eq!(zero.compare(&parsed), Ordering::Less);`. Owner's earlier byte-identical claim was inaccurate; this was **not formatting**. Parent restored the exact HEAD assertion, changing no old expected value/business behavior, then reran native datatype8 successfully. Early native8 had the assertion missing and is **not the final complete gate**; final8 is successful supplemental replay, not a failed-target retry. Final four old test modules (CPP datatype/CPP expr/native datatype/native expr) are byte-identical.

**Source tools were not failure-free:** edit observation-precondition failure1/fresh-read retry1; edit old-literal mismatch1/fresh-read retry1; ordinary lookup failure1 (wrong root-Plan path). These are not sandbox denials or compile/test failures; C's zero-match discovery glob is not an error. Typed inputs/serialized output/transport Eq were locked during RO design; ABI precompile-guess corrections0.

Parent scope **17 Rust files =CPP11 (1 new vector_native.rs) +native6**, plus1 CPP datatype manifest enabling serde_json raw_value. **Both locks remain HEAD-byte-identical**; metadata/resolution commands0. Final17 pinned-format checks and both diff checks passed, formatter errors0. Original const APIs actually passed on the January pin; no speculative const feature gate was added.

Aligned native-policy type/error and complete public helpers now live in CPP; native production exports are aliases, **not aliases to the old packed wire type**. RawValue→f64 range→f32 parsing/16383 cap, finite-only create without dimension cap, init/raw mutation/raw decoding, nonfinite/suffix/size-overflow policies, native formatter/Debug/PartialEq are preserved. Four wire/native metrics share ordered f32 loops; one norm loop keeps native powi(2) versus wire multiplication. Ordinary wire validator/codec/seven getters remain unexpanded.

**Nine private recipes =8 new kernels +original DIMS getter alias.** NativeVector/NativeVector2 tuple roles carry real ScalarValue::VectorFloat32, not disguised ordinary Bytes. FROM_TEXT worker owns strict UTF-8+parse, returns a real standard-LE serialized vector in dedicated OwnNativeVector Bytes; materialization only peeks/deserializes layout, rejects suffix, and checks actual capacity before/after allocation with input/projection and output dual-owner retained charges. No result-vector chunk revalidation, runtime/chunk rewrite or new BE restriction. ComputedNativeVector moves actual aligned storage; only its transport wrapper uses per-f32-bit Eq, while native float PartialEq is unchanged. Borrowed aligned view avoids a copy, but projection/serialization allocate and copy: **no zero-copy/allocator-peak/performance claim**.

Exact **five-op×actual typed NativeVectorError→Caused** authorization produces VectorNative receipts; D renders the borrowed actual error into original Vector/SQL1105, not text classification or host-recomputed dimension errors. **All vector coercions remain guarded**; actual NULL tags select a recipe without changing left-first conversion, left-NULL skipping right conversion or left-error-before-right-NULL. RealNull requires a genuine NULL witness, not fake suffix. FROM_TEXT host only eval_string; five metrics retain NaN→NULL/Inf bits; AS_TEXT keeps new_string/metadata. Eight native expression business bodies removed; cfg(test) two-argument dispatch remains a thin context bridge through the real worker. No new PB/cop admission.

Tests added:3 leaf +3 kernel +2 local +D3/E3, including16 direct zero-slot SQL cases. Original literals and explicitly hand-derived transport/policy cases remain distinguished; no provider-recorded oracle/new SQL golden. CAST/plus and other public-helper consumers receive **no extra family credit**.

## All raw receipts (`vector-` prefix; Finished/test times are not benchmarks)
| Suffix / phase | Passed / failed / ignored; filtered | Finished / test seconds; exit |
| --- | --- | --- |
| **tikv-datatype.log / compile failure** |E0282/E0283; **no tests** |not reached / not run;**101** |
| tikv-datatype-retry.log / final retry |9 / 0 / 0;409 |3.65 / 0.00;0 |
| native-datatype.log / early, assertion missing |8 / 0 / 0;430 |3.27 / 0.00;0 |
| native-datatype-final.log / final supplemental replay |8 / 0 / 0;430 |1.03 / 0.00;0 |
| **tikv-local.log / compile failure** |E0004; **no tests** |not reached / not run;**101** |
| tikv-local-retry.log / final retry |276 / 0 / 1 old;517 (277 discovered) |11.82 / 0.20;0 |
| tikv-kernels.log / final |9 / 0 / 0;785 |0.13 / 0.00;0 |
| native-vec.log / final |3 / 0 / 0;1575 |16.06 / 0.00;0 |
| native-vectorized.log / final |1 / 0 / 0;1577 |0.13 / 0.00;0 |
| dispatch.log / final |3 / 0 / 0;1575 |0.12 / 0.00;0 |
| sql.log / final |95 / 0 / 0;2078 |32.00 / 1.51;0 |
| sql-columns.log / final |1 / 0 / 0;2172 |0.18 / 0.02;0 |
| **expr-full.log / current baseline** |1480 / **4 old** / 94;0 (1578 discovered) |0.12 / 10.74;**101** |
| **unistore-full.log / current baseline** |195 / **1 old** / 13;0 (209 discovered) |8.28 / 2.98;**101** |

Parent actually compared both **complete current failure sections against Round43**, normalizing **only panic-heading thread IDs**. Expr SHA256 `25f964887541f45602363f00dc60df4f0af21c74edccc16c38d0804094fb9bff` retains all4 old failures including duration.rs:**164:55**; Unistore `e285bfdba646f2d01f85b485ae317cc07c51b78cf0ae6d664c0fb2bc39259759` retains DECIMAL-'abc' at **194:5**. **NO ADDRESS MAPPING; both full suites remain non-green.** These section hashes are not whole-log hashes.

FORMAT/DATE/MICROSECOND/larger JSON-path work and M6 remain deferred. Round39 parser-all E0061 remains historical, unrepaired/unrun here. Whole workspace/Go-package acceptance, lint/release/profile, performance/zero-copy, allocator/OOM peak, FIPS and PR readiness remain unestablished. Writer only read/hashed14 logs and wrote these two docs, no fresh source audit/build/test/fmt/Git/index/Plan; B owns separate metadata. Functional **151/245**, final **0/245**.
