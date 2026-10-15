# M25: matching unary kernels on the shared batch driver (2026-10-16)

UnaryNotInt, ASIN and ACOS reuse the existing eager driver. NOT preserves
UInt bits and returns signed0/1/NULL. ASIN/ACOS use the same asin_f64/acos_f64
primitive as native raw workers; NaN output becomes NULL and signed zero is
preserved. NaN input remains excluded by the existing Real carrier. This does
not admit SIN/COS/ATAN's native Go/libm mismatch or ROUND's distinct policies.

ASIN/ACOS benchmark data repeats8in-domain values and2outside values per10:
[-1,-.9999,-.5,-0,+0,.5,.9999,1,-1.5,1.5]. Both candidate/fallback and actual
native set the test-only EXPR_PERF_REAL_DOMAIN=inverse_trig; input setup is
outside timing. All old controls retain their original data and schema, with
that flag unset. The flag is benchmark data selection, not a production route
or value admission gate. Raw probe headers record the chosen input domain.
No per-function driver or duplicate mathematical algorithm is added.

The three new workloads compare actual current batch to a clearly labeled
same-current-binary fallback ablation, and separately to true frozen original
native production. That fallback is NOT original native or historical TiKV.
Eleven existing control workloads instead compare distinct actual M24 and M25
batch binaries. No forced singleton route is used as a historical baseline.

Three alternating pairs;7samples75ms,1024selected rows and5selection/NULL shapes.
Latency is median of3process medians; ratios are medians of within-pair ratios.
Actual native dense runs after each pair on identical defaultLongLong inputs.
All2940current timing records/210workload-shape-pair checksum groups match,
including42fresh actual-native/current dense groups. All optimized/control
batch routes are prepared-only with0fallback/refusal. The fallback ablation is
explicitly selected, not an automatic refusal. Full rows remain in summary.json.

## Newly admitted operations

Dense ns/selected-row:

| workload | current fallback ablation | batch | paired ablation/batch | true native | native/batch |
|---|---:|---:|---:|---:|---:|
| integer_not | 826.271 | 8.085 | 101.76x | 47.038 | 5.82x |
| real_acos | 1050.533 | 10.756 | 98.37x | 112.619 | 10.47x |
| real_asin | 1069.012 | 10.686 | 99.89x | 112.748 | 10.55x |

## Existing workload controls

These baseline cells are actual M24 batch code, unlike the new-family ablation
above. Flat and slower results remain visible.

| workload | M24 | M25 | paired M24/M25 | true native |
|---|---:|---:|---:|---:|
| any_value_datetime | 10.451 | 10.236 | 1.020x | 122.387 |
| any_value_duration | 13.703 | 13.571 | 1.010x | 122.589 |
| any_value_json | 126.041 | 121.886 | 1.034x | 137.376 |
| any_value_timestamp | 10.439 | 10.230 | 1.020x | 122.474 |
| any_value_vector | 70.792 | 69.437 | 1.018x | 155.132 |
| decimal_add | 128.795 | 129.236 | 0.997x | 69.386 |
| decimal_compare_lt | 72.924 | 73.976 | 0.984x | 173.493 |
| decimal_compare_nulleq | 73.243 | 74.085 | 0.992x | 169.094 |
| integer_add | 12.920 | 12.876 | 1.003x | 21.101 |
| lower | 32.840 | 32.802 | 1.001x | 153.363 |
| real_add | 13.019 | 13.038 | 0.998x | 106.989 |

## Independent M15 controls

| workload | shape | M15 | M25 | paired M15/M25 |
|---|---|---:|---:|---:|
| any_value_datetime | dense | 10.182 | 10.252 | 0.993x |
| any_value_datetime | dense_null10 | 10.419 | 10.781 | 0.992x |
| any_value_datetime | duplicate | 10.451 | 10.485 | 0.997x |
| any_value_datetime | reverse | 10.416 | 10.511 | 0.991x |
| any_value_datetime | sparse_quarter | 16.272 | 16.372 | 0.994x |
| any_value_duration | dense | 14.449 | 13.520 | 1.067x |
| any_value_duration | dense_null10 | 14.035 | 13.372 | 1.049x |
| any_value_duration | duplicate | 14.602 | 13.549 | 1.079x |
| any_value_duration | reverse | 14.625 | 13.769 | 1.062x |
| any_value_duration | sparse_quarter | 20.548 | 19.520 | 1.050x |
| integer_add | dense | 12.860 | 12.859 | 1.000x |
| integer_add | dense_null10 | 12.629 | 12.616 | 1.000x |
| integer_add | duplicate | 13.098 | 13.095 | 0.997x |
| integer_add | reverse | 13.137 | 13.083 | 1.003x |
| integer_add | sparse_quarter | 19.829 | 19.662 | 0.999x |
| lower | dense | 32.973 | 32.913 | 1.002x |
| lower | dense_null10 | 31.041 | 31.870 | 0.974x |
| lower | duplicate | 33.468 | 33.526 | 1.001x |
| lower | reverse | 33.229 | 34.056 | 0.976x |
| lower | sparse_quarter | 42.955 | 45.049 | 0.955x |

All840records/60checksum groups agree. This older baseline preserves earlier regressions rather than resetting them away.

## Process repeatability

| workload | baseline range | M25 range |
|---|---:|---:|
| any_value_datetime | 10.445–11.116 | 10.234–10.248 |
| any_value_duration | 13.674–13.713 | 13.565–13.597 |
| any_value_json | 126.034–126.795 | 121.856–122.014 |
| any_value_timestamp | 10.419–10.453 | 10.229–10.259 |
| any_value_vector | 70.615–71.502 | 69.395–69.923 |
| decimal_add | 128.458–129.278 | 129.179–129.405 |
| decimal_compare_lt | 72.731–73.871 | 73.949–74.174 |
| decimal_compare_nulleq | 73.055–73.912 | 73.859–74.093 |
| integer_add | 12.894–12.952 | 12.868–12.887 |
| integer_not | 813.564–838.767 | 8.040–8.120 |
| lower | 32.766–33.070 | 32.784–32.822 |
| real_acos | 1046.353–1074.815 | 10.679–10.776 |
| real_add | 13.015–13.073 | 13.026–13.071 |
| real_asin | 1051.353–1078.303 | 10.553–10.702 |

Ranges are3process medians, not confidence intervals. CPU2 pinning does not isolate SMT/boost/thermal/other host effects. No compilation overlaps timing. No application QPS or all-function performance claim is made.

## Decision: retain only the source-matched unary kernels

NOT measures8.085ns/row versus true native47.038 (5.82x). ASIN10.686 versus
112.748 (10.55x) and ACOS10.756 versus112.619 (10.47x), on the declared mixed
normal/out-of-domain dataset. Current fallback ablations826–1069ns are explicitly
separate; their roughly98–102x ratios are not historical-native improvements.

Controls retain tradeoffs: Decimal LT72.924→73.976ns (~2% slower), NULL-safe
comparison73.243→74.085 (~1% slower) and PLUS128.795→129.236 (near flat/slower).
Other controls are near flat or improve1–3%; these are observed build results,
not a new arithmetic algorithm. Independent M15 Int12.860→12.859 and LOWER
32.973→32.913 are flat, Datetime10.182→10.252 slightly slower and Duration
14.449→13.520 improves. All shapes and process ranges remain visible. No claim
that every old regression is permanently fixed, every function optimized or
application QPS improved.

Retained TiKV2b289f9eaa2f4c2620b4e128ec3f5a2b2e08be02 / this TiDB report commit.
Only existing matching kernels are admitted; no new driver, mathematical
algorithm or migration route. Native Go/libm trig and nonidentity ROUND policy
boundaries remain. Dataset selection is confined to test probes, with default
old controls unchanged. Signed/unsigned NOT preserves NULL and all input bits.

## Validation/reproduction

Batch32/fullTiDB1384same4 and freshTiKV1030same6 names verified; profile54pass. Golden inverse-trig bits coverInf, domain edges,±0/endpoints/interior/NULL and2050reverse/duplicate rows; NOT covers UIntMAX/MIN/zero/nonzero/NULL. Actual SQL smoke3prepared. Scoped fmt, make lint and diff checks pass. Datatype516/565 are reused unchanged-core results. No fresh server/PD-TiKV/FIPS/workspace clippy pass. Inventory66/131static signatures is metadata, not SQL coverage percent.

validation.md pins exact source/binary/probe hashes, domain selection and commands. Build current as after-bin, original plus the archived test probe as frozen-bin and actual M24 as before-bin. collect.py/reference-control.py generate raw logs; summarize.py/summarize-reference.py assert counts/checksums and produce JSON; make-report.py regenerates this receipt from those plus decision.md. Binaries stay outside git.
