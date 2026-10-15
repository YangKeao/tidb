# M21: choose the smaller partial-word optimization (2026-10-16)

Two real code candidates are compared to unchanged M20: full exact reciprocal
math, and a one-line canonical-head bound before the existing remainder. Full
candidate correctness/proof/initial measurement is retained in the sibling
[full-candidate receipt](../performance-m21-decimal-word-powers/summary.md).
The final source/keep decision appears below. Only one production implementation
is selected; neither candidate is a runtime route or semantic mode.

Three distinct binaries execute actual batch_tikv, rotating ABC/BCA/CAB positions
in three cohorts. Seven75ms samples per workload/shape/mode. Latency is median
process median and ratios are medians of within-cohort paired ratios. Native is
a separate true original production pair with only the archived test probe;
it runs after each cohort over identical data/defaultLongLong schema. These are
code comparisons, NOT forced same-binary historical simulations.

Dense1024 selected rows, ns/selected-row:

| workload | M20 | full reciprocal | head bound | M20/head | full/head | frozen native |
|---|---:|---:|---:|---:|---:|---:|
| any_value_datetime | 10.825 | 10.498 | 10.830 | 1.000x | 0.967x | 122.131 |
| any_value_duration | 14.957 | 14.872 | 14.939 | 1.001x | 0.996x | 123.599 |
| any_value_json | 134.307 | 132.862 | 132.357 | 1.017x | 1.004x | 142.973 |
| any_value_timestamp | 10.806 | 10.542 | 10.808 | 0.999x | 0.976x | 122.657 |
| any_value_vector | 69.803 | 69.783 | 69.253 | 1.007x | 1.007x | 154.584 |
| decimal_add | 132.335 | 127.510 | 129.283 | 1.024x | 0.986x | 70.046 |
| decimal_compare_lt | 77.772 | 73.151 | 74.849 | 1.039x | 0.977x | 168.008 |
| decimal_compare_nulleq | 77.754 | 72.964 | 74.977 | 1.037x | 0.976x | 166.913 |
| integer_add | 13.162 | 13.332 | 13.134 | 1.002x | 1.015x | 21.780 |
| lower | 34.163 | 34.155 | 34.128 | 0.999x | 1.001x | 150.186 |
| real_add | 14.060 | 14.071 | 13.988 | 1.004x | 1.006x | 106.315 |

All3465 current timing records /165 workload-shape-cohort checksum groups agree,
including33 true-native/current dense groups. All routes prepared-only with zero
fallback/refusal. Raw/summary records retain all dense/NULL-every10/sparse/reverse/
duplicate samples. Flat/slower controls remain visible. Pinning CPU2 does not
isolate SMT/boost/thermal/host effects; no compilation during timing. This is not
SQL-server QPS, all-function coverage or a statistical confidence interval.

## Independent M15 controls for the head candidate

Three paired cohorts, seven75ms samples, four workloads/five shapes. All840
records /60 checksum groups agree. These retain old regressions rather than
resetting the baseline to the immediately previous implementation.

| workload | shape | M15 | head | paired M15/head |
|---|---|---:|---:|---:|
| any_value_datetime | dense | 10.108 | 10.841 | 0.934x |
| any_value_datetime | dense_null10 | 10.319 | 11.011 | 0.936x |
| any_value_datetime | duplicate | 10.343 | 10.999 | 0.938x |
| any_value_datetime | reverse | 10.435 | 11.018 | 0.943x |
| any_value_datetime | sparse_quarter | 16.054 | 16.941 | 0.947x |
| any_value_duration | dense | 14.387 | 14.925 | 0.964x |
| any_value_duration | dense_null10 | 13.979 | 14.085 | 0.993x |
| any_value_duration | duplicate | 14.506 | 14.870 | 0.975x |
| any_value_duration | reverse | 14.615 | 15.116 | 0.966x |
| any_value_duration | sparse_quarter | 20.386 | 20.967 | 0.972x |
| integer_add | dense | 12.834 | 13.103 | 0.978x |
| integer_add | dense_null10 | 12.587 | 12.941 | 0.973x |
| integer_add | duplicate | 13.014 | 13.343 | 0.978x |
| integer_add | reverse | 13.017 | 13.322 | 0.977x |
| integer_add | sparse_quarter | 19.558 | 20.017 | 0.976x |
| lower | dense | 32.904 | 34.100 | 0.964x |
| lower | dense_null10 | 30.953 | 32.455 | 0.956x |
| lower | duplicate | 33.362 | 35.055 | 0.949x |
| lower | reverse | 33.107 | 35.011 | 0.949x |
| lower | sparse_quarter | 42.792 | 44.744 | 0.956x |

## Validation and reproduction

Head candidate: TiDB datatype516, TiKV datatype565 pass. Release batch28 and
fullTiDB1379/same4 names pass their known baseline. Existing cell/export/resource
oracles cover the one-line bound; no reciprocal helper/table/test remains in
head source. Full candidate separately passed516/566 including1M independent
integer-division comparisons and a mathematical proof. Source byte/strict-value
admission and arithmetic/rounding policies are unchanged. M19 expression1028/
same6 reused; no fresh server, PD/TiKV, FIPS or full workspace clippy pass claimed.
Formatting/diff checks pass; prior lint applies to unchanged lint inputs.

M20 source TiDB835daf31153006a41d5d7f9ae1e7755e18db1f4a /
TiKV7c427c84ec6f341e04ed516c84b4333333d65b3d. Full candidate applies sibling
full-candidate.patch to that pair; head candidate applies head-candidate.patch.
All five binary hashes in binary-hashes.txt, exact test/build/cohort commands
in validation.md. Frozen source/probe and original production pair are pinned
there, not the current fallback ablation. Binaries remain outside git.

Build the three current release libtests as m20-bin/full-bin/head-bin beside
collect.py; keep actual M15 as int-reference-bin and frozen true native as
frozen-bin. after-bin is a byte-identical alias of head-bin only for the reference
script. Run collect.py, reference-control.py, summarize.py,
summarize-reference.py and make-report.py; record new hashes.

## Process repeatability

Ranges of three process medians:

| workload | M20 range | full range | head range |
|---|---:|---:|---:|
| any_value_datetime | 10.823–10.855 | 10.475–10.520 | 10.825–10.930 |
| any_value_duration | 14.851–14.969 | 14.860–14.927 | 14.930–14.948 |
| any_value_json | 134.241–134.736 | 132.611–134.189 | 132.036–132.379 |
| any_value_timestamp | 10.800–10.848 | 10.497–10.573 | 10.796–10.882 |
| any_value_vector | 69.728–69.965 | 69.721–69.842 | 69.211–69.328 |
| decimal_add | 132.173–133.659 | 127.356–127.655 | 129.205–129.689 |
| decimal_compare_lt | 77.614–77.857 | 72.921–73.266 | 74.840–74.882 |
| decimal_compare_nulleq | 77.753–77.790 | 72.821–74.211 | 74.740–75.035 |
| integer_add | 13.146–13.237 | 13.328–13.391 | 13.074–13.135 |
| lower | 34.074–34.250 | 34.096–34.165 | 34.071–34.205 |
| real_add | 13.970–14.066 | 14.060–14.075 | 13.969–14.015 |

## Decision: keep the one-line head bound

The three-mode comparison selects the head-bound-only candidate. Decimal PLUS
132.335→129.283ns/row (1.024x), LT77.772→74.849 (1.039x), NULL-safe equality
77.754→74.977 (1.037x). Full reciprocals save another1.77–2.01ns/row, but their
table/helper/proof surface is not justified for this small additional gain.
That gain is real in these cohorts, not claimed to be absent. The full candidate
is removed completely from production, including its helper, table and unit.
The exact patch and proof remain only as experimental evidence.

Selected TiKV source: 8d143e47eba26e2a33562961a98b4693f1e2f22a; TiDB uses the report commit.
The source changes one predicate: n<d implies n%d=n, so canonical heads skip the
remainder. Noncanonical heads still use the old remainder. Existing independent
cell/export corpora cover all head widths and noncanonical words. No algorithm,
resource policy, ABI or admitted function changes. Remaining Decimal PLUS is
1.85x slower than native70.046ns. Against M15, dense Int/Datetime/Duration/LOWER
remain about2%/7%/4%/4% slower; all shapes remain in the tables. No universal
performance improvement or application-level gain is claimed.
