# Single checked Decimal cell input (2026-10-16)

Batch Decimal input now borrows one raw-cell view per column window and feeds
cell bytes through the existing NativeMyDecimal::from_raw_bytes checked decoder
once, then the same logical normalization and word budget. It removes the
intermediate facade/native-cell copy and repeated count/word validation.

The old try_from_native_cell typed entry and MyDecimal input facade method are
deleted after all callers migrate. There is no duplicate decoder, alternate
batch route, raw physical-copy shortcut or changed driver/registry. NULL bytes
are still skipped before decode. Noncanonical padding/leading-zero/negative-zero
and independent visible-scale rules remain exactly the old value projection;
malformed physical bytes follow the existing checked decoder's admission.
The separate value-layer and strict output projection APIs remain required.

## Release results

Dense1024 selected rows, ns/selected-row. Three actual M19/candidate binary pairs
alternate before→after / after→before / before→after. Seven adaptive75ms samples
per cell across all five shapes. Latency is median process median; speedup is
median paired ratio. Both execute batch, not a forced fallback ablation. Frozen
native is a distinct true original production pair with test-only probe added,
freshly timed after each pair over the same data/defaultLongLong schema.

| workload | M19 | candidate | paired speedup | frozen native | native / candidate |
|---|---:|---:|---:|---:|---:|
| any_value_datetime | 11.262 | 10.841 | 1.04x | 123.501 | 11.39x |
| any_value_duration | 15.001 | 15.027 | 1.00x | 124.657 | 8.30x |
| any_value_json | 133.868 | 132.559 | 1.01x | 144.516 | 1.09x |
| any_value_timestamp | 11.255 | 10.863 | 1.04x | 124.124 | 11.43x |
| any_value_vector | 69.907 | 69.314 | 1.01x | 154.703 | 2.23x |
| decimal_add | 149.513 | 132.998 | 1.12x | 70.492 | 0.53x |
| decimal_compare_lt | 94.040 | 78.179 | 1.20x | 167.811 | 2.15x |
| decimal_compare_nulleq | 94.095 | 77.907 | 1.21x | 165.389 | 2.12x |
| integer_add | 13.079 | 13.143 | 1.00x | 21.752 | 1.66x |
| lower | 34.084 | 34.241 | 0.99x | 151.823 | 4.43x |
| real_add | 14.002 | 13.992 | 1.00x | 106.511 | 7.61x |

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
| any_value_datetime | dense | 10.180 | 10.839 | 0.939x |
| any_value_datetime | dense_null10 | 10.388 | 11.020 | 0.942x |
| any_value_datetime | duplicate | 10.321 | 10.962 | 0.941x |
| any_value_datetime | reverse | 10.376 | 10.992 | 0.945x |
| any_value_datetime | sparse_quarter | 16.121 | 16.982 | 0.951x |
| any_value_duration | dense | 14.463 | 14.977 | 0.966x |
| any_value_duration | dense_null10 | 14.026 | 14.084 | 0.996x |
| any_value_duration | duplicate | 14.616 | 14.920 | 0.983x |
| any_value_duration | reverse | 14.643 | 15.127 | 0.972x |
| any_value_duration | sparse_quarter | 20.390 | 20.937 | 0.975x |
| integer_add | dense | 12.925 | 13.114 | 0.984x |
| integer_add | dense_null10 | 12.671 | 12.987 | 0.978x |
| integer_add | duplicate | 13.129 | 13.336 | 0.984x |
| integer_add | reverse | 13.126 | 13.334 | 0.984x |
| integer_add | sparse_quarter | 19.783 | 19.995 | 0.986x |
| lower | dense | 33.073 | 34.199 | 0.967x |
| lower | dense_null10 | 31.088 | 32.129 | 0.968x |
| lower | duplicate | 33.455 | 34.561 | 0.968x |
| lower | reverse | 33.251 | 34.316 | 0.969x |
| lower | sparse_quarter | 42.949 | 44.234 | 0.970x |

## Cohort and validation

- Before M19: TiDB47225cd1297ad04dcc69ab38e83d323d4a9dc200 / TiKV366723653e14e12b7f3bf2a6f0d40f5e3eb5919d. Binary SHA2569f0910ee69f166a6c46a58a2b24c9fbf68fd0e3e12a48797eff246669cb8167f.
- Candidate binary SHA25695264c1c925ceaca9d113ef42092102a0d45d92e921adc1d43240ef14dddb5b9; source checkpoint is recorded in validation.md and the final decision below.
- M15 reference: TiDB6e46687c0dc1771ca4084f44c935534099b83868 / TiKV8b210d8330eb7afecf973f6ccee0df059e76eb03, binary025aa8b47be1fb0a540662e576c589cb800ed0d2178a873f3e4c28f3c461399d.
- True frozen production: TiDB364aef2bab5cc633ecb76a775ae8f36f86a6687d / TiKV548812e1ef57aef077a2062a9cc356640a6347f5, only M15 test-only probe/registration differs. Exact reused binary91f9c56c4a52ba68fd51976ceb287559ed2929f056a12ba570c5764720dcebe9, freshly timed. Source ../../evidence/original-native-narrow-integer-performance-m15.rs, SHA256325197d64b4059be2981a1c1e08b51114c0da4f143c4edfb01d64fe952abc93d.
- TiDB datatype516 and TiKV datatype565 pass, including existing wide hidden-scale/resource, initialized NULL owners,197056 cell projections and130236 strict/wide exports at six limits. Final release batch28 pass, fullTiDB1379/same4 baseline failures. TiKV full expression1028/same6 names were freshly verified in M19 and reused here (this input adapter is consumed by TiDB).
- make lint, scoped Rust formatting and both diff checks pass. No fresh server, PD/TiKV, FIPS or full workspace clippy success claimed. Exact commands and cohort receipts are in validation.md.
- Ryzen9 9900X, CPU2, Rust1.97.1/bootstrap, GCC/G++14, empty RUSTFLAGS, release.

## Reproduce

Build matching M19/candidate release libtests as before-bin/after-bin. Preserve
the actual M15 binary as int-reference-bin and original-native production with
only the archived probe as frozen-bin. Binaries are not committed. Run collect.py,
reference-control.py, summarize.py, summarize-reference.py and make-report.py.
Record new hashes; validation.md preserves environment/commands and final source.

## Process repeatability

Ranges of three process medians, not confidence intervals:

| workload | M19 range | candidate range | native range |
|---|---:|---:|---:|
| any_value_datetime | 10.750–11.264 | 10.831–10.859 | 123.417–124.172 |
| any_value_duration | 14.989–15.006 | 15.027–15.069 | 124.427–128.222 |
| any_value_json | 132.414–134.116 | 132.364–133.551 | 143.686–144.604 |
| any_value_timestamp | 11.246–11.288 | 10.838–10.910 | 123.207–124.142 |
| any_value_vector | 69.888–69.943 | 69.152–69.658 | 154.236–157.822 |
| decimal_add | 149.260–150.427 | 132.981–133.109 | 70.168–70.497 |
| decimal_compare_lt | 93.854–94.200 | 78.009–78.442 | 167.447–168.663 |
| decimal_compare_nulleq | 94.089–94.136 | 77.803–78.149 | 164.927–165.677 |
| integer_add | 13.072–13.102 | 13.137–13.162 | 21.748–21.776 |
| lower | 34.006–34.483 | 34.192–34.319 | 150.903–151.872 |
| real_add | 13.994–14.084 | 13.965–14.043 | 106.427–108.767 |

## Decision: retained, obsolete adapters removed

Three cohorts improve Decimal PLUS149.513→132.998ns/row (1.12x), LT94.040→78.179 (1.20x) and NULL-safe equality94.095→77.907 (1.21x). Other primary controls are flat or improve, apart from small LOWER/Int differences; all values remain visible. M15 references still show old Int/Time/Duration/LOWER regressions and are not reset away. No universal or application-level gain is claimed.

Retained source: report's TiDB commit / TiKV7c427c84ec6f341e04ed516c84b4333333d65b3d. Production searches find no old typed input entry or facade caller. Historical report/profile names remain immutable evidence. The factory reuses the original checked decoder; no new physical parser or semantic mode remains alongside it.
