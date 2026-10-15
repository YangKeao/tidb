# Decimal fresh construction experiment (2026-10-16)

The M17 cycle profile attributes9.06% self to Decimal::try_new. This candidate
keeps its fraction/span/extent/resource preflights but avoids sending a fresh
nine-cell inline owner through general mutating resize, duplicate result-scale
checks and shrink checks. Wide owners still reserve fallibly. Known zero directly
initializes the same int1/frac0/result0/nonnegative/nine-zero-word state. General
mutating storage resizing, raw status metadata, arithmetic, transport, driver and
registry stay unchanged. This small candidate adds no alternate execution path.

Existing wide/resource/ownership/NULL tests verify the contract; no constructor
implementation-mirroring test is added. This is an experiment: measured gain and
control tradeoffs decide whether to keep it, not the source simplification alone.

## Release results

Dense1024 selected rows, ns/selected-row. Three actual M17/candidate binary pairs
alternate before→after / after→before / before→after. Seven adaptive75ms samples
per cell across all five shapes. Latency is median process median; speedup is
median paired ratio. Both execute batch, not a forced fallback ablation. Frozen
native is a distinct true original production pair with test-only probe added,
freshly timed after each pair over the same data/defaultLongLong schema.

| workload | M17 | candidate | paired speedup | frozen native | native / candidate |
|---|---:|---:|---:|---:|---:|
| any_value_datetime | 10.468 | 10.488 | 1.00x | 123.340 | 11.76x |
| any_value_duration | 15.906 | 15.837 | 1.00x | 123.031 | 7.77x |
| any_value_json | 132.446 | 132.504 | 1.00x | 141.614 | 1.07x |
| any_value_timestamp | 10.453 | 10.519 | 0.99x | 122.607 | 11.66x |
| any_value_vector | 69.765 | 70.806 | 0.99x | 153.483 | 2.17x |
| decimal_add | 156.558 | 158.373 | 0.99x | 70.103 | 0.44x |
| decimal_compare_lt | 100.104 | 102.923 | 0.97x | 167.016 | 1.62x |
| decimal_compare_nulleq | 99.938 | 102.966 | 0.97x | 164.812 | 1.60x |
| integer_add | 13.189 | 13.203 | 1.00x | 21.736 | 1.65x |
| lower | 34.622 | 34.501 | 1.00x | 150.670 | 4.37x |
| real_add | 13.311 | 13.407 | 0.99x | 106.290 | 7.93x |

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
| any_value_datetime | dense | 10.126 | 10.497 | 0.964x |
| any_value_datetime | dense_null10 | 10.322 | 10.714 | 0.965x |
| any_value_datetime | duplicate | 10.301 | 10.621 | 0.970x |
| any_value_datetime | reverse | 10.320 | 10.661 | 0.967x |
| any_value_datetime | sparse_quarter | 16.046 | 16.605 | 0.965x |
| any_value_duration | dense | 14.441 | 15.893 | 0.909x |
| any_value_duration | dense_null10 | 14.028 | 14.825 | 0.946x |
| any_value_duration | duplicate | 14.585 | 15.993 | 0.914x |
| any_value_duration | reverse | 14.635 | 16.042 | 0.913x |
| any_value_duration | sparse_quarter | 20.481 | 21.807 | 0.939x |
| integer_add | dense | 12.785 | 13.230 | 0.965x |
| integer_add | dense_null10 | 12.541 | 13.022 | 0.964x |
| integer_add | duplicate | 13.029 | 13.409 | 0.971x |
| integer_add | reverse | 13.026 | 13.360 | 0.975x |
| integer_add | sparse_quarter | 19.475 | 20.113 | 0.968x |
| lower | dense | 32.894 | 34.563 | 0.951x |
| lower | dense_null10 | 31.082 | 33.399 | 0.931x |
| lower | duplicate | 33.176 | 34.573 | 0.958x |
| lower | reverse | 33.322 | 35.258 | 0.945x |
| lower | sparse_quarter | 43.025 | 46.774 | 0.920x |

## Cohort and validation

- Before M17: TiDB016e8e230b8351eefe31fb44c5b9b2a83e0f09dd / TiKV44befe011baf3082d18398b27428f47030a29be2. Binary SHA256d29d409f280e55e71e38b36f4c0b8c765342cf2f2e15510efc0b08b2bae958d0.
- Candidate binary SHA2564e1fa2f145a82f70382818508c99c59d8b2d7853550b576e110942b1620624e6; source checkpoint is recorded in validation.md and the final decision below.
- M15 reference: TiDB6e46687c0dc1771ca4084f44c935534099b83868 / TiKV8b210d8330eb7afecf973f6ccee0df059e76eb03, binary025aa8b47be1fb0a540662e576c589cb800ed0d2178a873f3e4c28f3c461399d.
- True frozen production: TiDB364aef2bab5cc633ecb76a775ae8f36f86a6687d / TiKV548812e1ef57aef077a2062a9cc356640a6347f5, only M15 test-only probe/registration differs. Exact reused binary91f9c56c4a52ba68fd51976ceb287559ed2929f056a12ba570c5764720dcebe9, freshly timed. Source ../../evidence/original-native-narrow-integer-performance-m15.rs, SHA256325197d64b4059be2981a1c1e08b51114c0da4f143c4edfb01d64fe952abc93d.
- TiDB datatype515 and TiKV datatype565 pass, including existing wide hidden-scale/resource, initialized NULL owners,197056 cell projections and130236 strict/wide exports at six limits. Final release batch28 pass, fullTiDB1379/same4 baseline failures. TiKV full expression1028/same6 is reused M15 evidence, not fresh here.
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
| any_value_datetime | 10.427–10.468 | 10.472–10.502 | 122.376–123.528 |
| any_value_duration | 15.782–15.925 | 15.827–15.837 | 122.987–123.181 |
| any_value_json | 132.064–132.636 | 132.143–133.515 | 141.612–141.847 |
| any_value_timestamp | 10.414–10.471 | 10.490–10.521 | 122.238–123.086 |
| any_value_vector | 69.269–74.022 | 70.754–77.570 | 152.440–153.595 |
| decimal_add | 156.011–160.372 | 158.320–158.846 | 69.986–70.307 |
| decimal_compare_lt | 99.941–103.200 | 102.827–103.444 | 167.016–167.340 |
| decimal_compare_nulleq | 99.835–102.514 | 102.920–103.292 | 164.781–165.424 |
| integer_add | 13.163–13.203 | 13.164–13.224 | 21.714–23.068 |
| lower | 34.586–34.920 | 34.446–34.855 | 150.622–150.928 |
| real_add | 13.289–13.467 | 13.393–13.415 | 106.256–106.419 |

## Decision: rejected and reverted

The candidate does not establish a clear gain. Decimal PLUS is156.558→158.373ns/row (paired ratio about0.99x), LT100.104→102.923 and NULL-safe equality99.938→102.966 (about0.97x). Other controls are flat or slower. M15 reference regressions remain visible. All correctness checks pass, but they do not justify keeping a performance-only source expansion.

The exact candidate.patch is retained for reproduction against TiKV44befe011baf3082d18398b27428f47030a29be2, paired with TiDB016e8e230b8351eefe31fb44c5b9b2a83e0f09dd production code. The patch was reverse-applied after measurement; TiKV git diff/status are clean at that original commit. No candidate production change is pushed. This report-only TiDB commit keeps runtime at the M17 code. Existing M17 validation remains applicable to the exact restored source; no redundant rebuild is claimed. The next small profile-guided experiment targets the generic vector append call, not arithmetic semantics.
