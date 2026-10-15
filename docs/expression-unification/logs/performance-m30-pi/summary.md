# M30: pure zero-argument PI on the existing batch driver (2026-10-16)

The shared registry admits exactly Pi()->Real and TiDB removes its blanket
zero-argument rejection. Existing catalog lowering, kernel metadata and explicit
output row count remain in use. No new driver, constant cache or random-state
handling is added. Native pi_raw delegates to the same pi primitive; golden
values are exact bits of3.141592653589793. Stateful RAND stays excluded.

Actual SQL pi() is prepared. No-input2050virtual rows, reverse/duplicate and empty
selection, nested Real PLUS, wrong arity/type and resource refusals pass. Budget
checks apply even without source columns; zero work/retained bytes refuse.
Full TiDB1386/same4failures, TiKV1032/same6, batch34, lint/fmt/diff pass; unchanged
datatype516/565 and Chunk273 reused explicitly. No fresh server/PD-TiKV/FIPS or
workspace-clippy claim.68/131 is metadata signatures, not a SQL coverage rate.

Three alternating pairs, fifteen workloads, five shapes, seven75ms samples,
1024rows. New Pi BEFORE is same-current fallback ablation (not native). Old14
BEFORE workloads use actual adopted M27 release code; AFTER is actual M30.
Original-native production is independently frozen and only its test probe is
extended/rebuilt. All3150current records/225checksumgroups agree, including45
fresh original-native/current dense groups. Prepared routes have zero fallback
and refusal; the separately labeled fallback ablation has no batch-route calls.
Two UIntidentity schemas/data remain identical across every relevant binary;
old controls keep original signed LongLong and Real data. Units ns/row; medians
of3process medians, paired ratios are medians of within-pair ratios.

| workload | before kind | before | M30 | before/M30 | actual native | native/M30 |
|---|---|---:|---:|---:|---:|---:|
| any_value_datetime | actual M27 batch | 9.532 | 9.526 | 1.003x | 122.119 | 12.820x |
| any_value_duration | actual M27 batch | 12.905 | 12.842 | 1.005x | 122.445 | 9.534x |
| any_value_int_unsigned | actual M27 batch | 9.834 | 9.624 | 1.024x | 122.030 | 12.680x |
| any_value_json | actual M27 batch | 129.414 | 121.142 | 1.068x | 144.087 | 1.193x |
| any_value_timestamp | actual M27 batch | 9.566 | 9.463 | 1.013x | 121.926 | 12.897x |
| any_value_vector | actual M27 batch | 72.256 | 72.026 | 1.000x | 161.016 | 2.227x |
| decimal_add | actual M27 batch | 131.835 | 128.860 | 1.023x | 69.214 | 0.537x |
| decimal_compare_lt | actual M27 batch | 75.020 | 74.698 | 1.000x | 169.428 | 2.268x |
| decimal_compare_nulleq | actual M27 batch | 74.899 | 74.979 | 1.003x | 163.955 | 2.194x |
| integer_add | actual M27 batch | 12.236 | 12.237 | 1.000x | 21.014 | 1.717x |
| lower | actual M27 batch | 34.379 | 33.290 | 1.035x | 153.781 | 4.636x |
| pure_pi | current fallback ablation | 630.365 | 4.100 | 153.748x | 67.078 | 16.437x |
| real_add | actual M27 batch | 12.414 | 12.479 | 0.994x | 107.237 | 8.584x |
| round_int_signed | actual M27 batch | 7.421 | 7.334 | 1.013x | 100.388 | 13.721x |
| round_int_unsigned | actual M27 batch | 7.392 | 7.364 | 1.010x | 101.233 | 13.787x |

## M15 historical controls

All840records/60checksumgroups agree; every slower shape remains visible.

| workload | shape | M15 | M30 | paired M15/M30 |
|---|---|---:|---:|---:|
| any_value_datetime | dense | 10.148 | 9.502 | 1.068x |
| any_value_datetime | dense_null10 | 10.344 | 9.618 | 1.075x |
| any_value_datetime | duplicate | 10.481 | 9.709 | 1.080x |
| any_value_datetime | reverse | 10.382 | 9.700 | 1.071x |
| any_value_datetime | sparse_quarter | 16.203 | 15.769 | 1.025x |
| any_value_duration | dense | 14.404 | 12.812 | 1.127x |
| any_value_duration | dense_null10 | 14.003 | 12.597 | 1.110x |
| any_value_duration | duplicate | 14.555 | 12.843 | 1.136x |
| any_value_duration | reverse | 14.611 | 13.076 | 1.117x |
| any_value_duration | sparse_quarter | 20.455 | 18.914 | 1.078x |
| integer_add | dense | 12.864 | 12.272 | 1.048x |
| integer_add | dense_null10 | 12.566 | 12.013 | 1.048x |
| integer_add | duplicate | 13.068 | 12.479 | 1.047x |
| integer_add | reverse | 13.045 | 12.496 | 1.044x |
| integer_add | sparse_quarter | 19.611 | 19.369 | 1.014x |
| lower | dense | 33.007 | 33.269 | 0.990x |
| lower | dense_null10 | 31.140 | 31.544 | 0.991x |
| lower | duplicate | 33.278 | 34.099 | 0.976x |
| lower | reverse | 33.108 | 34.118 | 0.970x |
| lower | sparse_quarter | 43.057 | 44.319 | 0.976x |

## Process repeatability

| workload | before range | M30 range | native range |
|---|---:|---:|---:|
| any_value_datetime | 9.502–9.615 | 9.471–9.539 | 120.248–122.227 |
| any_value_duration | 12.874–12.911 | 12.787–12.843 | 122.362–122.446 |
| any_value_int_unsigned | 9.784–9.855 | 9.587–9.860 | 121.689–122.531 |
| any_value_json | 129.332–129.433 | 119.979–121.381 | 144.011–144.510 |
| any_value_timestamp | 9.561–9.620 | 9.445–9.527 | 121.348–122.040 |
| any_value_vector | 71.831–72.345 | 71.707–72.340 | 157.494–161.070 |
| decimal_add | 131.809–131.959 | 128.858–129.284 | 69.151–69.445 |
| decimal_compare_lt | 74.724–75.470 | 74.674–75.133 | 169.398–169.481 |
| decimal_compare_nulleq | 74.888–75.326 | 74.648–76.303 | 163.563–164.504 |
| integer_add | 12.217–12.289 | 12.237–12.252 | 20.935–21.023 |
| lower | 34.358–34.468 | 33.169–33.587 | 153.773–155.009 |
| pure_pi | 623.835–639.657 | 4.081–4.103 | 66.801–67.710 |
| real_add | 12.404–12.439 | 12.479–12.532 | 107.122–107.421 |
| round_int_signed | 7.401–7.459 | 7.308–7.340 | 100.334–100.628 |
| round_int_unsigned | 7.345–7.440 | 7.316–7.381 | 101.192–101.764 |

Ranges are not confidence intervals. Pinning CPU2 is not CPU isolation; no correctness/build jobs overlap formal timing. This is an in-memory warm evaluator microbenchmark, not server QPS.

## Final decision: retain pure niladic PI

PI dense4.100ns/row versus actual original native67.078ns, median paired ratio
16.437x. The separately labeled current fallback ablation is630.365ns; its
153.75x ratio is not a native comparison. Every shape is bit/checksum-exact.
The change is one shared registry row and removal of a blanket arity guard,
with no new execution implementation or weakened semantic constructor.

Old14actual-code dense controls stay close or improve, except small Real
12.414→12.479ns and NULL-safe Decimal comparison74.899→74.979ns increases.
All shapes and ranges stay in JSON/raw logs; no universal speedup claim. JSON
and LOWER improvements are observations, not a proven causal benefit of PI.
M15 historical controls are separately preserved in the table above.

The requested pass ends at the documented next-work cost boundary. Remaining
63static signatures are explicitly unadmitted, and owning input/intermediate
vectors still limit Decimal throughput. The next clear common storage change
requires lifetime/ownership/budget interface work, while remaining function
families need their distinct semantic contracts. See remaining-work-boundary.md;
this is not a declaration that all functions or all SQL domains are optimized.

## Reproduction

validation.md pins source, probe/binary hashes, contracts, exact commands and limitations. Run collect.py, reference-control.py, summarize.py, summarize-reference.py and make-report.py using the pinned binaries. Raw logs, source patches and scripts are retained; binaries stay outside git. See remaining-work-boundary.md for the explicitly unimplemented signature domains and larger storage work.
