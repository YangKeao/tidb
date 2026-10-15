# M24: nullable predicates on the shared batch driver (2026-10-16)

Seven IS NULL signatures use the existing eager driver and generated nullable
kernels. NULL inputs produce signed Int1, non-NULL values (including zero/empty)
produce signed Int0. Int permits unsigned bits; other operand/result flag rules
stay unchanged. Datetime/Timestamp share TimeIsNull. No per-function driver,
nullness-only shortcut or execution mode is added. Child phases still execute
before the existing kernel; NULL itself is not propagated as the result.

JSON is explicitly excluded from native SQL batch IS NULL: the existing Go
catalog inserts CastJsonAsString then StringIsNull. Do not replace it with
JsonIsNull or bypass that cast. JSON golden cases verify the existing native
fallback, not prepared coverage. Cast support is a separate shared-contract
extension. No new family-specific migration gate is introduced.

The eight native-type workloads compare actual current batch to a clearly labeled
same-current-binary fallback ablation, and separately to true frozen original
native production. That fallback is NOT original native or historical TiKV.
Eleven existing control workloads instead compare distinct actual M23 and M24
batch binaries. No forced singleton route is used as a historical baseline.

Three alternating pairs;7samples75ms,1024selected rows and5selection/NULL shapes.
Latency is median of3process medians; ratios are medians of within-pair ratios.
Actual native dense runs after each pair on identical defaultLongLong inputs.
All3990current timing records/285workload-shape-pair checksum groups match,
including57fresh actual-native/current dense groups. All optimized/control
batch routes are prepared-only with0fallback/refusal. The fallback ablation is
explicitly selected, not an automatic refusal. Full rows remain in summary.json.

## Newly admitted operations

Dense ns/selected-row:

| workload | current fallback ablation | batch | paired ablation/batch | true native | native/batch |
|---|---:|---:|---:|---:|---:|
| isnull_datetime | 962.788 | 7.989 | 120.29x | 108.466 | 13.58x |
| isnull_decimal | 1018.507 | 36.192 | 28.14x | 151.667 | 4.19x |
| isnull_duration | 953.413 | 10.537 | 90.48x | 108.517 | 10.30x |
| isnull_int | 953.622 | 7.932 | 119.94x | 109.261 | 13.77x |
| isnull_real | 953.213 | 8.009 | 118.51x | 108.750 | 13.58x |
| isnull_string | 970.561 | 11.168 | 87.00x | 116.607 | 10.44x |
| isnull_timestamp | 961.869 | 7.964 | 120.78x | 108.553 | 13.63x |
| isnull_vector | 975.698 | 39.314 | 24.86x | 125.384 | 3.19x |

## Existing workload controls

These baseline cells are actual M23 batch code, unlike the new-family ablation
above. Flat and slower results remain visible.

| workload | M23 | M24 | paired M23/M24 | true native |
|---|---:|---:|---:|---:|
| any_value_datetime | 10.522 | 10.427 | 1.009x | 122.549 |
| any_value_duration | 13.909 | 13.657 | 1.016x | 123.679 |
| any_value_json | 132.979 | 126.083 | 1.054x | 143.004 |
| any_value_timestamp | 10.544 | 10.405 | 1.011x | 122.903 |
| any_value_vector | 70.692 | 70.805 | 0.997x | 153.168 |
| decimal_add | 134.218 | 128.231 | 1.047x | 70.129 |
| decimal_compare_lt | 78.782 | 72.930 | 1.080x | 167.181 |
| decimal_compare_nulleq | 78.577 | 72.633 | 1.082x | 164.792 |
| integer_add | 13.526 | 12.903 | 1.047x | 21.753 |
| lower | 33.716 | 32.703 | 1.024x | 157.226 |
| real_add | 14.068 | 13.024 | 1.081x | 106.374 |

## Independent M15 controls

| workload | shape | M15 | M24 | paired M15/M24 |
|---|---|---:|---:|---:|
| any_value_datetime | dense | 10.149 | 10.435 | 0.971x |
| any_value_datetime | dense_null10 | 10.336 | 10.589 | 0.976x |
| any_value_datetime | duplicate | 10.251 | 10.550 | 0.971x |
| any_value_datetime | reverse | 10.352 | 10.574 | 0.979x |
| any_value_datetime | sparse_quarter | 16.105 | 16.349 | 0.986x |
| any_value_duration | dense | 14.395 | 13.685 | 1.052x |
| any_value_duration | dense_null10 | 13.997 | 13.541 | 1.039x |
| any_value_duration | duplicate | 14.533 | 13.659 | 1.064x |
| any_value_duration | reverse | 14.584 | 13.914 | 1.053x |
| any_value_duration | sparse_quarter | 20.314 | 19.551 | 1.039x |
| integer_add | dense | 12.845 | 12.839 | 1.000x |
| integer_add | dense_null10 | 12.597 | 12.591 | 1.001x |
| integer_add | duplicate | 13.048 | 13.114 | 0.997x |
| integer_add | reverse | 13.054 | 13.106 | 0.996x |
| integer_add | sparse_quarter | 19.703 | 19.550 | 1.007x |
| lower | dense | 32.971 | 32.692 | 1.009x |
| lower | dense_null10 | 30.945 | 30.887 | 1.002x |
| lower | duplicate | 33.147 | 33.113 | 1.004x |
| lower | reverse | 32.988 | 32.888 | 1.004x |
| lower | sparse_quarter | 42.805 | 42.621 | 1.008x |

All840records/60checksum groups agree. This older baseline preserves earlier regressions rather than resetting them away.

## Process repeatability

| workload | baseline range | M24 range |
|---|---:|---:|
| any_value_datetime | 10.521–10.578 | 10.388–10.573 |
| any_value_duration | 13.868–13.954 | 13.656–13.688 |
| any_value_json | 132.935–133.587 | 125.764–126.320 |
| any_value_timestamp | 10.524–10.578 | 10.399–11.077 |
| any_value_vector | 70.485–70.721 | 70.720–71.620 |
| decimal_add | 133.632–135.233 | 128.049–128.507 |
| decimal_compare_lt | 78.650–78.902 | 72.856–73.087 |
| decimal_compare_nulleq | 78.574–78.695 | 72.606–72.633 |
| integer_add | 13.512–13.571 | 12.862–12.966 |
| isnull_datetime | 950.972–963.138 | 7.951–8.004 |
| isnull_decimal | 1014.977–1024.216 | 35.861–36.211 |
| isnull_duration | 950.151–953.816 | 10.483–10.540 |
| isnull_int | 951.120–988.899 | 7.932–7.951 |
| isnull_real | 946.719–962.095 | 7.998–8.043 |
| isnull_string | 968.252–976.722 | 11.129–11.219 |
| isnull_timestamp | 960.519–963.736 | 7.952–7.973 |
| isnull_vector | 971.688–981.424 | 39.253–39.365 |
| lower | 33.503–33.795 | 32.653–32.984 |
| real_add | 14.056–14.123 | 13.018–13.053 |

Ranges are3process medians, not confidence intervals. CPU2 pinning does not isolate SMT/boost/thermal/other host effects. No compilation overlaps timing. No application QPS or all-function performance claim is made.

## Decision: retain the seven-signature nullable family

Actual batch/native dense ns/row: Int7.932/109.261, Real8.009/108.750,
Datetime7.989/108.466, Timestamp7.964/108.553, Duration10.537/108.517,
String11.168/116.607, Decimal36.192/151.667 and Vector39.314/125.384.
That is3.19–13.77x faster than real original native. Explicit same-current
fallback ablations are953–1019ns and remain separate from historical claims.
Retain the family, with JSON cast exclusion and unsigned operand rules intact.

Existing controls are flat/improved in this build except a small Vector change
70.692→70.805ns. Int13.526→12.903 and DecimalPLUS134.218→128.231 improve, as do
comparisons/Real/JSON/time/Duration/LOWER. The source only extends admission;
these unrelated code-generation/layout changes are not attributed to a new
arithmetic algorithm. Independent M15 Int12.845→12.839 and LOWER32.971→32.692
are approximately flat; Duration14.395→13.685 improves, while Datetime10.149→
10.435 remains about3% slower. All shapes and process ranges remain visible,
including earlier checkpoints' slower controls. This cohort does not establish
that all historical regressions are permanently resolved. Decimal PLUS still
trails native. No universal or application QPS claim is made.

Retained TiKVf983e547a148b8bccdf99141b8e32c6de1f503c6 / this TiDB report commit.
No new driver, nullness-only implementation or temporary migration route is
introduced. The existing ordinary kernel receives full typed arguments. JSON
continues through the unchanged native fallback because its actual Go lowering
contains an unadmitted cast. Direct wire JsonIsNull is not substituted to inflate
SQL coverage. The shared cast family remains future work.

## Validation/reproduction

Batch30/fullTiDB1382same4 and freshTiKV1030same6 names verified. Golden NULL/non-NULL, UInt/zero/empty and NULL literals cover2050reverse/duplicate selected rows; JSON remains native fallback. Existing catalog PB tests pin Time/Timestamp and JSON cast signatures. Scoped fmt, make lint and diff checks pass. Datatype516/565 are reused from the unchanged adopted datatype code; no fresh server/PD-TiKV/FIPS/workspace clippy claim. Inventory63admitted/131static signature names is metadata, not SQL coverage percentage.

validation.md pins source/binary/probe hashes and commands. Build current as after-bin, original plus the archived test probe as frozen-bin and actual M23 as before-bin. collect.py/reference-control.py generate logs; summarize.py/summarize-reference.py assert counts/checksums and generate JSON; make-report.py regenerates this receipt from those plus decision.md. Binaries stay outside git.
