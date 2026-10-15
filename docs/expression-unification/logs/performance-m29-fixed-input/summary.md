# M29: common fixed-input importer, three actual implementations (2026-10-16)

M27 is the adopted baseline. Direct temporal raw input (A) preserves the original
Time normalizer and Duration checks, but initial sampling regressed Decimal
comparisons. Common fixed input (B) moves Int/Real/Time/Duration owning import to
one private outlined generic loop with existing per-carrier decoders. NULL does
not decode backing payload. This is a storage change on the same driver.
No constructor check, normalization, SQL policy or runtime selector is added.
Only one implementation may remain after the decision below.

Three actual release binaries run ABC/BCA/CAB over three cohorts, fourteen
workloads, five selection/NULL shapes, seven samples of75ms and1024rows.
Every route is prepared-only with zero fallback/refusal. All4410current samples
and210checksum groups agree, including42fresh original-native dense groups.
Unsigned schema flags are identical on all four binaries for the two unsigned
identity workloads only. Old controls retain original schema/data. All slower
rows remain in JSON/raw logs. Values are ns/row, median of process medians;
ratios are medians of within-cohort ratios.

| workload | M27 | direct A | common B | M27/B | A/B | actual native |
|---|---:|---:|---:|---:|---:|---:|
| any_value_datetime | 9.548 | 9.098 | 9.459 | 1.009x | 0.962x | 122.699 |
| any_value_duration | 12.906 | 12.537 | 12.235 | 1.054x | 1.026x | 122.972 |
| any_value_int_unsigned | 9.826 | 9.461 | 9.215 | 1.065x | 1.027x | 123.501 |
| any_value_json | 130.321 | 123.301 | 122.592 | 1.060x | 1.003x | 144.450 |
| any_value_timestamp | 9.589 | 9.044 | 9.499 | 1.011x | 0.952x | 122.582 |
| any_value_vector | 71.896 | 70.949 | 71.457 | 1.006x | 0.994x | 163.905 |
| decimal_add | 132.165 | 139.170 | 137.420 | 0.963x | 1.016x | 69.461 |
| decimal_compare_lt | 74.938 | 84.210 | 82.870 | 0.911x | 1.015x | 169.469 |
| decimal_compare_nulleq | 75.054 | 84.238 | 82.473 | 0.910x | 1.020x | 164.896 |
| integer_add | 12.341 | 12.177 | 11.340 | 1.088x | 1.074x | 20.954 |
| lower | 34.444 | 32.959 | 33.387 | 1.032x | 0.986x | 154.593 |
| real_add | 12.458 | 12.326 | 11.556 | 1.081x | 1.069x | 107.327 |
| round_int_signed | 7.402 | 7.239 | 6.824 | 1.079x | 1.065x | 101.658 |
| round_int_unsigned | 7.407 | 7.246 | 6.785 | 1.092x | 1.063x | 101.769 |

## Historical M15 controls

All840records/60checksum groups agree.

| workload | shape | M15 | common B | M15/B |
|---|---|---:|---:|---:|
| any_value_datetime | dense | 10.133 | 9.489 | 1.070x |
| any_value_datetime | dense_null10 | 10.365 | 9.515 | 1.089x |
| any_value_datetime | duplicate | 10.318 | 9.657 | 1.073x |
| any_value_datetime | reverse | 10.356 | 9.667 | 1.073x |
| any_value_datetime | sparse_quarter | 16.143 | 15.715 | 1.019x |
| any_value_duration | dense | 14.425 | 12.256 | 1.175x |
| any_value_duration | dense_null10 | 14.031 | 11.949 | 1.170x |
| any_value_duration | duplicate | 14.564 | 12.581 | 1.155x |
| any_value_duration | reverse | 14.621 | 12.465 | 1.173x |
| any_value_duration | sparse_quarter | 20.359 | 18.824 | 1.081x |
| integer_add | dense | 12.831 | 11.311 | 1.134x |
| integer_add | dense_null10 | 12.647 | 11.190 | 1.131x |
| integer_add | duplicate | 13.098 | 11.538 | 1.133x |
| integer_add | reverse | 13.096 | 11.530 | 1.132x |
| integer_add | sparse_quarter | 19.641 | 18.242 | 1.071x |
| lower | dense | 33.113 | 33.423 | 0.991x |
| lower | dense_null10 | 31.086 | 31.558 | 0.989x |
| lower | duplicate | 33.530 | 34.056 | 0.985x |
| lower | reverse | 33.341 | 33.946 | 0.979x |
| lower | sparse_quarter | 42.968 | 43.707 | 0.983x |

## Process repeatability

| workload | M27 range | direct A range | common B range |
|---|---:|---:|---:|
| any_value_datetime | 9.536–9.555 | 9.034–9.144 | 9.442–9.465 |
| any_value_duration | 12.891–12.909 | 12.523–12.551 | 12.209–12.263 |
| any_value_int_unsigned | 9.811–9.842 | 9.458–9.589 | 9.211–9.229 |
| any_value_json | 129.506–131.275 | 122.919–123.503 | 122.535–122.892 |
| any_value_timestamp | 9.574–9.648 | 9.029–9.048 | 9.487–9.510 |
| any_value_vector | 71.584–76.202 | 70.907–76.047 | 71.348–71.763 |
| decimal_add | 131.927–132.550 | 138.792–139.948 | 136.969–137.518 |
| decimal_compare_lt | 74.920–75.459 | 84.013–84.250 | 82.280–82.972 |
| decimal_compare_nulleq | 75.048–75.345 | 84.094–84.435 | 82.447–82.842 |
| integer_add | 12.251–12.355 | 12.140–12.249 | 11.302–11.382 |
| lower | 34.376–34.501 | 32.932–33.211 | 33.370–33.465 |
| real_add | 12.405–12.575 | 12.220–12.594 | 11.529–11.562 |
| round_int_signed | 7.364–7.476 | 7.236–7.305 | 6.794–6.877 |
| round_int_unsigned | 7.337–7.451 | 7.211–7.267 | 6.785–6.840 |

Ranges are not confidence intervals. CPU2 pinning is not CPU isolation; no compilation/tests overlap timing. No application QPS claim.

## Final decision: reject both input candidates

The direct-only candidate A improves temporal inputs but regresses Decimal
comparisons about12%. The common fixed-input candidate B improves Int/Real
about8–9%, Duration about5% and JSON about6%, yet Decimal PLUS132.165→137.420ns
and LT74.938→82.870 / NULL-safe equality75.054→82.473ns regress about4%/10%.
These losses recur across the three rotated cohorts. Time/Timestamp are nearly
flat against M27. The extra abstraction does not resolve the targeted tradeoff.
No established cause is attributed to code layout, generics or host noise.

Both candidates are fully reverse-applied, including the added candidate test;
production source remains exactly the adopted M27. Their exact patches and
validation/measurement evidence are preserved. No import helper, new route,
constructor hint or weakened check is left in production. Coverage remains67/131
static signature metadata. Candidate binaries must never serve as an adopted
baseline for a later experiment.

## Validation and reproduction

Batch34pass/fullTiDB1386pass with the same4baseline failures; make lint, scoped rustfmt and git diff --check pass. Unchanged core reuses datatype516/565, TiKV1031/same6 and Chunk273 explicitly. No fresh server/PD-TiKV/FIPS/workspace-clippy claim. Coverage67/131 metadata unchanged.

See validation.md for exact commands/source/owner/layout preconditions and binary-hashes.txt for all versions. Candidate patches and original samples are retained; binaries remain outside git. Run collect.py, reference-control.py, summarize.py, summarize-reference.py and make-report.py with pinned binaries. The first direct-only experiment remains in ../performance-m29-temporal-input/.
