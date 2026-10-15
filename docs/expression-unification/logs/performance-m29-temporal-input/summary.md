# M29: temporal input window access experiment (2026-10-16)

Time/Duration input borrows one raw-cell view per column window, like the
existing Int/Real transport. Time still invokes the original Time::from_go_raw
then go_raw before the shared carrier, preserving packed metadata normalization:
TimestampFSP7 maps to6. It is not blindly retained or rejected. No new calendar
validation is invented. Duration still uses the same i8FSP conversion and
from_nanos checks inside the non-NULL branch; NULL does not decode its payload.
Only repeated byte access/temporary native value wrappers are removed. Existing
native scalar getters remain for their other consumers; no new decoder, driver,
semantic mode or unchecked constructor replaces them.

Distinct actual M27/candidate batch binaries compare all14workloads,3alternating
pairs,7samples75ms/5shapes/1024rows. Both modes use real batch_tikv; no same-binary
fallback ablation. New UInt schema choices are identical current/before/native,
all old controls use their original signed schema. Actual original-native dense
runs follow each pair; source is pinned in validation.md. All2940timings/
210checksum groups, including42native/current dense groups, match. All routes
prepared-only,0fallback/refusal. Latencies are medians of3process medians and
ratios medians of paired ratios. Slow controls remain in all raw/summary rows.

| workload | M27 | candidate | paired M27/candidate | true native |
|---|---:|---:|---:|---:|
| any_value_datetime | 9.585 | 9.021 | 1.064x | 122.458 |
| any_value_duration | 12.900 | 12.555 | 1.027x | 123.320 |
| any_value_int_unsigned | 9.837 | 9.489 | 1.034x | 123.071 |
| any_value_json | 131.648 | 123.105 | 1.069x | 146.428 |
| any_value_timestamp | 9.698 | 9.027 | 1.072x | 123.233 |
| any_value_vector | 71.922 | 70.987 | 1.013x | 161.713 |
| decimal_add | 132.214 | 138.565 | 0.954x | 69.372 |
| decimal_compare_lt | 74.889 | 84.138 | 0.890x | 169.834 |
| decimal_compare_nulleq | 75.041 | 84.504 | 0.888x | 165.037 |
| integer_add | 12.233 | 12.164 | 1.009x | 20.977 |
| lower | 34.498 | 33.013 | 1.044x | 156.006 |
| real_add | 12.577 | 12.291 | 1.023x | 107.402 |
| round_int_signed | 7.439 | 7.265 | 1.024x | 102.053 |
| round_int_unsigned | 7.407 | 7.281 | 1.017x | 102.893 |

## Independent M15 controls

All840records/60checksum groups agree; earlier regressions remain visible.

| workload | shape | M15 | candidate | paired M15/candidate |
|---|---|---:|---:|---:|
| any_value_datetime | dense | 10.172 | 9.038 | 1.125x |
| any_value_datetime | dense_null10 | 10.383 | 9.114 | 1.140x |
| any_value_datetime | duplicate | 10.499 | 9.232 | 1.137x |
| any_value_datetime | reverse | 10.537 | 9.228 | 1.142x |
| any_value_datetime | sparse_quarter | 16.258 | 15.239 | 1.064x |
| any_value_duration | dense | 14.403 | 12.602 | 1.143x |
| any_value_duration | dense_null10 | 14.020 | 12.388 | 1.131x |
| any_value_duration | duplicate | 14.597 | 12.547 | 1.161x |
| any_value_duration | reverse | 14.636 | 12.846 | 1.139x |
| any_value_duration | sparse_quarter | 20.462 | 18.753 | 1.091x |
| integer_add | dense | 12.832 | 12.136 | 1.056x |
| integer_add | dense_null10 | 12.612 | 11.924 | 1.055x |
| integer_add | duplicate | 13.097 | 12.419 | 1.059x |
| integer_add | reverse | 13.101 | 12.414 | 1.055x |
| integer_add | sparse_quarter | 19.679 | 19.144 | 1.028x |
| lower | dense | 33.102 | 33.075 | 1.001x |
| lower | dense_null10 | 31.236 | 31.208 | 1.010x |
| lower | duplicate | 33.528 | 33.627 | 1.005x |
| lower | reverse | 33.585 | 33.456 | 1.010x |
| lower | sparse_quarter | 42.752 | 43.921 | 0.973x |

## Process repeatability

| workload | M27 range | candidate range |
|---|---:|---:|
| any_value_datetime | 9.580–9.662 | 9.007–9.065 |
| any_value_duration | 12.899–12.907 | 12.543–12.575 |
| any_value_int_unsigned | 9.816–9.879 | 9.464–9.645 |
| any_value_json | 131.521–132.756 | 122.675–124.201 |
| any_value_timestamp | 9.561–9.714 | 9.010–9.064 |
| any_value_vector | 71.820–72.262 | 70.949–70.999 |
| decimal_add | 132.028–132.437 | 138.533–139.041 |
| decimal_compare_lt | 74.872–75.040 | 84.097–84.227 |
| decimal_compare_nulleq | 74.871–75.119 | 84.111–84.726 |
| integer_add | 12.222–12.317 | 12.122–12.172 |
| lower | 34.450–34.577 | 32.990–33.135 |
| real_add | 12.415–12.674 | 12.280–12.329 |
| round_int_signed | 7.407–11.902 | 7.250–7.349 |
| round_int_unsigned | 7.367–7.467 | 7.246–7.317 |

Ranges of3process medians are not confidence intervals. CPU2 pinning does not isolate host/SMT/boost effects; no compilation/tests overlap timing. No application QPS or universal performance claim.

## Disposition of initial direct-input experiment

Not adopted on these measurements: temporal dense medians improve about3–7%,
but Decimal PLUS132.214→138.565ns and comparisons74.889→84.138 /
75.041→84.504ns regress. All samples remain; no proven causal explanation is
claimed. This candidate is compared against M27 and a common fixed-input
alternative in ../performance-m29-fixed-input/summary.md. That later three-code
receipt records the final implementation choice; this directory is the initial
experiment, not evidence that the direct path was merged.

## Validation/reproduction

New2050reverse/duplicate-row test verifies all16metadata nibbles against the existing native normalizer, includingFSP7→6 and NULL payload skipping for DateTime/Timestamp. Batch34/fullTiDB1386same4/lintfmt pass. Existing unchanged-core datatype516/565,TiKV1031same6 andChunk273 reused explicitly. No fresh server/PD-TiKV/FIPS/workspace clippy claim. Coverage67/131 unchanged, metadata only.

validation.md preserves source/binary/probe hashes, exact commands and owner/layout preconditions. Run collect.py/reference-control.py/summarize.py/summarize-reference.py/make-report.py with before/after/frozen/int-reference binaries. All patches/raw samples/scripts stay in the receipt; binaries remain outside git.
