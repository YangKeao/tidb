# Generic vector append inlining experiment (2026-10-16)

The M17 profile attributes about10% self to out-of-line owning Decimal vector
append. This candidate changes only ChunkedVecSized<T>::push_data from inline
 to inline(always), allowing the compiler to remove that call boundary and
large value ABI copies. Ownership, bitmap/data append order, NULL backing,
arithmetic, transport, driver and registry semantics are unchanged. The rejected
M18 constructor patch is absent; actual M17 production code is the baseline.

Existing owner/drop/NULL/resource tests cover the unchanged behavior. Four
out-of-line Decimal push_data symbols in the baseline disappear in the candidate;
this confirms code generation changed, not that throughput must improve. ELF
text grows from69430989 to69476641 bytes (45652 bytes); total binary size is not
an application memory or instruction-cache measurement. Keep/reject is decided
by the measured workload/control results below.

## Release results

Dense1024 selected rows, ns/selected-row. Three actual M17/candidate binary pairs
alternate before→after / after→before / before→after. Seven adaptive75ms samples
per cell across all five shapes. Latency is median process median; speedup is
median paired ratio. Both execute batch, not a forced fallback ablation. Frozen
native is a distinct true original production pair with test-only probe added,
freshly timed after each pair over the same data/defaultLongLong schema.

| workload | M17 | candidate | paired speedup | frozen native | native / candidate |
|---|---:|---:|---:|---:|---:|
| any_value_datetime | 10.406 | 10.761 | 0.97x | 122.867 | 11.42x |
| any_value_duration | 15.878 | 14.919 | 1.06x | 122.996 | 8.24x |
| any_value_json | 131.806 | 127.864 | 1.03x | 141.931 | 1.11x |
| any_value_timestamp | 10.427 | 10.731 | 0.97x | 122.730 | 11.44x |
| any_value_vector | 69.690 | 70.339 | 0.99x | 152.986 | 2.17x |
| decimal_add | 158.749 | 149.122 | 1.07x | 70.241 | 0.47x |
| decimal_compare_lt | 102.791 | 94.233 | 1.09x | 166.434 | 1.77x |
| decimal_compare_nulleq | 103.245 | 94.419 | 1.09x | 164.606 | 1.74x |
| integer_add | 13.176 | 13.088 | 1.01x | 21.720 | 1.66x |
| lower | 34.421 | 34.368 | 1.00x | 151.863 | 4.42x |
| real_add | 13.319 | 13.989 | 0.95x | 106.184 | 7.59x |

All2310 primary timing records /165 checksum groups agree, including33 true-native
current dense groups; all batch routes are prepared-only, zero fallback/refusal.
Dense/NULL-every10/sparse/reverse/duplicate raw data and process medians remain
available. Flat/slower cells are retained. CPU2 pinning does not isolate SMT,
boost, thermal or other host effects. No compilation ran during timing. This is
not SQL-server QPS, allocator counting or all-function coverage.

## Independent M15 controls

The actual M15 binary remains a separate reference for Int PLUS, Datetime,
Duration and LOWER. This expands the previous control rather than hiding prior
regressions behind a new baseline. Three pairs reverse initial order, seven75ms
samples over five shapes:840 timing records /60 checksum groups agree and all
routes are prepared-only. No causal attribution is established for slower controls.

| workload | shape | M15 | candidate | paired speedup |
|---|---|---:|---:|---:|
| any_value_datetime | dense | 10.174 | 10.742 | 0.947x |
| any_value_datetime | dense_null10 | 10.386 | 10.957 | 0.948x |
| any_value_datetime | duplicate | 10.367 | 10.966 | 0.945x |
| any_value_datetime | reverse | 10.513 | 10.980 | 0.955x |
| any_value_datetime | sparse_quarter | 16.281 | 16.972 | 0.955x |
| any_value_duration | dense | 14.408 | 14.954 | 0.963x |
| any_value_duration | dense_null10 | 14.019 | 14.157 | 0.993x |
| any_value_duration | duplicate | 14.560 | 15.038 | 0.972x |
| any_value_duration | reverse | 14.675 | 15.248 | 0.961x |
| any_value_duration | sparse_quarter | 20.409 | 21.119 | 0.966x |
| integer_add | dense | 12.839 | 13.051 | 0.984x |
| integer_add | dense_null10 | 12.611 | 12.890 | 0.978x |
| integer_add | duplicate | 13.081 | 13.307 | 0.982x |
| integer_add | reverse | 13.094 | 13.314 | 0.983x |
| integer_add | sparse_quarter | 19.650 | 20.165 | 0.973x |
| lower | dense | 32.991 | 34.108 | 0.967x |
| lower | dense_null10 | 31.067 | 32.245 | 0.959x |
| lower | duplicate | 33.420 | 34.819 | 0.960x |
| lower | reverse | 33.421 | 34.783 | 0.963x |
| lower | sparse_quarter | 42.982 | 44.612 | 0.965x |

## Cohort and validation

- Before M17: TiDB016e8e230b8351eefe31fb44c5b9b2a83e0f09dd / TiKV44befe011baf3082d18398b27428f47030a29be2. Binary SHA256d29d409f280e55e71e38b36f4c0b8c765342cf2f2e15510efc0b08b2bae958d0.
- Candidate binary SHA2569f0910ee69f166a6c46a58a2b24c9fbf68fd0e3e12a48797eff246669cb8167f; source checkpoint is recorded in validation.md and the final decision below.
- M15 reference: TiDB6e46687c0dc1771ca4084f44c935534099b83868 / TiKV8b210d8330eb7afecf973f6ccee0df059e76eb03, binary025aa8b47be1fb0a540662e576c589cb800ed0d2178a873f3e4c28f3c461399d.
- True frozen production: TiDB364aef2bab5cc633ecb76a775ae8f36f86a6687d / TiKV548812e1ef57aef077a2062a9cc356640a6347f5, only M15 test-only probe/registration differs. Exact reused binary91f9c56c4a52ba68fd51976ceb287559ed2929f056a12ba570c5764720dcebe9, freshly timed. Source ../../evidence/original-native-narrow-integer-performance-m15.rs, SHA256325197d64b4059be2981a1c1e08b51114c0da4f143c4edfb01d64fe952abc93d.
- TiDB datatype515 and TiKV datatype565 pass, including existing wide hidden-scale/resource, initialized NULL owners,197056 cell projections and130236 strict/wide exports at six limits. Final release batch28 pass, fullTiDB1379/same4 baseline failures. Fresh TiKV full expression1028/same6 names verified, with no new failure.
- make lint, scoped Rust formatting and both diff checks pass. No fresh server, PD/TiKV, FIPS or full workspace clippy success claimed. Exact commands and cohort receipts are in validation.md.
- Ryzen9 9900X, CPU2, Rust1.97.1/bootstrap, GCC/G++14, empty RUSTFLAGS, release.

## Reproduce

Build matching M17/candidate release libtests as before-bin/after-bin. Preserve
the actual M15 binary as int-reference-bin and original-native production with
only the archived probe as frozen-bin. Binaries are not committed. Run collect.py,
reference-control.py, summarize.py, summarize-reference.py and make-report.py.
Record new hashes; validation.md preserves environment/commands and final source.

## Process repeatability

Ranges of three process medians, not confidence intervals:

| workload | M17 range | candidate range | native range |
|---|---:|---:|---:|
| any_value_datetime | 10.375–10.435 | 10.751–10.768 | 122.652–123.335 |
| any_value_duration | 15.858–15.901 | 14.915–14.989 | 122.485–123.088 |
| any_value_json | 130.911–132.388 | 127.468–128.453 | 141.359–142.177 |
| any_value_timestamp | 10.409–10.563 | 10.722–10.753 | 122.592–122.966 |
| any_value_vector | 69.583–69.734 | 70.253–70.447 | 152.811–153.197 |
| decimal_add | 158.631–161.024 | 148.895–149.175 | 70.164–70.261 |
| decimal_compare_lt | 102.422–103.473 | 94.020–94.248 | 166.146–167.549 |
| decimal_compare_nulleq | 103.103–103.390 | 93.915–94.466 | 164.406–164.650 |
| integer_add | 13.175–13.176 | 13.059–13.113 | 21.533–21.780 |
| lower | 34.337–34.514 | 34.154–34.838 | 150.962–152.090 |
| real_add | 13.290–13.552 | 13.987–13.992 | 106.144–106.407 |

## Decision: retained with measured tradeoffs

Three cohorts consistently improve Decimal PLUS158.749→149.122ns/row (1.07x), LT102.791→94.233 and NULL-safe equality103.245→94.419 (1.09x). Duration15.878→14.919 and JSON131.806→127.864 also improve. The candidate is retained as the one-line append hint, paired with TiKV366723653e14e12b7f3bf2a6f0d40f5e3eb5919d and the TiDB commit containing this report. No new driver, API or semantic mode is introduced.

This is not a universal gain: Real13.319→13.989, Datetime10.406→10.761 and Timestamp10.427→10.731 are slower; Vector is about1% slower and LOWER flat. M15 reference controls still show remaining Int/Time/Duration/LOWER regressions, all retained above. The source change helps the measured Decimal append cost; no established explanation or application-level gain is claimed for all control differences. Decimal PLUS remains about twice true native, so input transport remains an explicit optimization direction.
