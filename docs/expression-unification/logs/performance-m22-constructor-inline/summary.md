# M22: Decimal constructor inline experiment (2026-10-16)

The candidate adds only inline(always) to the existing Decimal::try_new.
It preserves all constructor checks, zero initialization and allocation/error
behavior. The M17 profile had about9% self in this function; M21 still contains
a344-byte standalone symbol. This is distinct from rejected M18 check removal.
The candidate patch is experimental; see the final decision below.

Three alternating before/after actual binaries, seven75ms samples,1024 rows,
five selection/NULL shapes and eleven workloads. Latencies are medians of three
process medians, speedups medians of within-pair ratios. Both use actual batch_tikv;
frozen native is a separately built original production pair, not a fallback.
No compilation overlaps timing. CPU2 pinning does not isolate SMT/boost/thermal
or host effects. These are not server QPS or confidence intervals.

| workload | M21 | inline | paired M21/inline | native |
|---|---:|---:|---:|---:|
| any_value_datetime | 10.852 | 10.749 | 1.011x | 123.366 |
| any_value_duration | 14.962 | 15.261 | 0.980x | 123.301 |
| any_value_json | 132.626 | 133.039 | 0.997x | 141.731 |
| any_value_timestamp | 10.914 | 10.764 | 1.014x | 123.517 |
| any_value_vector | 69.979 | 70.275 | 0.997x | 154.214 |
| decimal_add | 129.435 | 130.475 | 0.993x | 70.031 |
| decimal_compare_lt | 74.933 | 75.356 | 0.995x | 167.233 |
| decimal_compare_nulleq | 75.081 | 75.437 | 1.000x | 166.135 |
| integer_add | 13.147 | 13.766 | 0.954x | 21.845 |
| lower | 34.165 | 34.637 | 0.986x | 151.165 |
| real_add | 14.020 | 14.317 | 0.981x | 106.655 |

All2310 current timings/165 checksum groups, including33 fresh native/current groups, agree. All routes prepared-only with zero fallback/refusal. Full per-shape samples/process medians remain in raw logs and summary.json. Slower controls are retained.

## Independent M15 controls

| workload | shape | M15 | inline | paired M15/inline |
|---|---|---:|---:|---:|
| any_value_datetime | dense | 10.159 | 10.746 | 0.945x |
| any_value_datetime | dense_null10 | 10.379 | 11.102 | 0.930x |
| any_value_datetime | duplicate | 10.350 | 11.024 | 0.943x |
| any_value_datetime | reverse | 10.419 | 11.088 | 0.945x |
| any_value_datetime | sparse_quarter | 16.221 | 16.956 | 0.957x |
| any_value_duration | dense | 14.450 | 15.295 | 0.945x |
| any_value_duration | dense_null10 | 14.061 | 14.477 | 0.967x |
| any_value_duration | duplicate | 14.616 | 15.308 | 0.955x |
| any_value_duration | reverse | 14.679 | 15.533 | 0.943x |
| any_value_duration | sparse_quarter | 20.426 | 21.503 | 0.951x |
| integer_add | dense | 12.850 | 13.826 | 0.931x |
| integer_add | dense_null10 | 12.599 | 13.602 | 0.924x |
| integer_add | duplicate | 13.075 | 14.027 | 0.934x |
| integer_add | reverse | 13.043 | 14.014 | 0.931x |
| integer_add | sparse_quarter | 19.755 | 20.737 | 0.953x |
| lower | dense | 33.106 | 34.535 | 0.959x |
| lower | dense_null10 | 31.121 | 33.672 | 0.925x |
| lower | duplicate | 33.480 | 35.117 | 0.955x |
| lower | reverse | 33.427 | 35.883 | 0.932x |
| lower | sparse_quarter | 43.034 | 46.972 | 0.914x |

All840 records/60 checksum groups match.

## Process repeatability

| workload | M21 range | inline range |
|---|---:|---:|
| any_value_datetime | 10.829–10.878 | 10.735–10.779 |
| any_value_duration | 14.958–14.962 | 15.260–15.345 |
| any_value_json | 132.366–132.932 | 132.794–133.726 |
| any_value_timestamp | 10.874–10.984 | 10.723–10.784 |
| any_value_vector | 69.922–70.601 | 70.208–70.544 |
| decimal_add | 129.385–130.570 | 130.312–130.682 |
| decimal_compare_lt | 74.909–75.710 | 75.344–75.370 |
| decimal_compare_nulleq | 74.811–75.568 | 75.085–75.979 |
| integer_add | 13.113–23.124 | 13.748–13.814 |
| lower | 34.142–34.282 | 34.567–34.875 |
| real_add | 13.977–14.170 | 14.293–14.317 |

## Decision: rejected and fully reverted

The constructor hint provides no clear Decimal gain: PLUS129.435→130.475ns,
LT74.933→75.356 and NULL-safe equality75.081→75.437. Int13.147→13.766,
Real14.020→14.317 and Duration14.962→15.261 are slower; time controls improve
about1%. These are recorded tradeoffs, not a universal-gain claim.

One baseline Int cohort spikes23.124ns (others about13ns); all raw samples and
process ranges remain visible. We do not attribute its paired improvement to
the candidate. Even without that outlier, the target Decimal operations do not
justify this performance-only hint. No further cherry-picked sample is used.

Candidate reverse-applied completely. Adopted runtime remains TiKV
8d143e47eba26e2a33562961a98b4693f1e2f22a / TiDB02c3676b94f849db74e3019a5e27233629a96e2e
production; this report commit changes only documentation/evidence. Candidate
binary is explicitly experimental, not the adopted baseline for later work.
The constructor standalone symbol disappears; text grows5184bytes. Symbol
removal alone is insufficient to justify retention. Exact patch and all logs
remain reproducible. No constructor policy or alternate implementation remains.

## Validation and reproduction

See validation.md for exact source pairs, hashes, build/test commands, frozen probe and baseline failure names. collect.py/reference-control.py generate raw logs; summarize.py/summarize-reference.py assert all expected counts/checksums and produce summary JSON; make-report.py regenerates this receipt. Binaries stay outside git.
