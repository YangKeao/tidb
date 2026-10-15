# M28: Duration constructor inline experiment (2026-10-16)

The candidate adds only inline(always) to Duration::from_nanos. check_fsp and
checked_round remain exactly unchanged, including errors, range/precision and
normalization behavior. The recent independent Duration profile measured15–17%
self in this constructor. Its baseline579byte standalone symbol disappears,
but that alone does not establish a performance gain. No new mirror test for a
code-generation hint; complete existing suites cover the unchanged contract.

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
| any_value_datetime | 9.677 | 9.509 | 1.018x | 122.398 |
| any_value_duration | 12.939 | 13.472 | 0.961x | 122.838 |
| any_value_int_unsigned | 9.858 | 9.796 | 1.006x | 122.853 |
| any_value_json | 131.274 | 132.226 | 0.991x | 144.905 |
| any_value_timestamp | 9.703 | 9.529 | 1.023x | 122.305 |
| any_value_vector | 74.343 | 70.709 | 1.051x | 160.385 |
| decimal_add | 131.937 | 132.240 | 0.997x | 69.331 |
| decimal_compare_lt | 74.953 | 74.602 | 1.003x | 169.797 |
| decimal_compare_nulleq | 74.887 | 74.707 | 1.002x | 165.178 |
| integer_add | 12.217 | 12.281 | 0.995x | 20.989 |
| lower | 34.373 | 32.855 | 1.046x | 158.538 |
| real_add | 12.405 | 12.663 | 0.978x | 107.292 |
| round_int_signed | 7.400 | 7.358 | 1.007x | 102.147 |
| round_int_unsigned | 7.422 | 7.334 | 1.011x | 101.587 |

## Independent M15 controls

All840records/60checksum groups agree; earlier regressions remain visible.

| workload | shape | M15 | candidate | paired M15/candidate |
|---|---|---:|---:|---:|
| any_value_datetime | dense | 10.167 | 9.498 | 1.070x |
| any_value_datetime | dense_null10 | 10.372 | 9.658 | 1.074x |
| any_value_datetime | duplicate | 10.483 | 9.692 | 1.078x |
| any_value_datetime | reverse | 10.572 | 9.703 | 1.084x |
| any_value_datetime | sparse_quarter | 16.287 | 15.906 | 1.025x |
| any_value_duration | dense | 14.383 | 13.513 | 1.065x |
| any_value_duration | dense_null10 | 13.998 | 13.150 | 1.064x |
| any_value_duration | duplicate | 14.608 | 13.290 | 1.101x |
| any_value_duration | reverse | 14.659 | 13.724 | 1.070x |
| any_value_duration | sparse_quarter | 20.274 | 20.487 | 0.997x |
| integer_add | dense | 12.837 | 12.270 | 1.047x |
| integer_add | dense_null10 | 12.611 | 12.066 | 1.045x |
| integer_add | duplicate | 13.080 | 12.490 | 1.047x |
| integer_add | reverse | 13.063 | 12.484 | 1.047x |
| integer_add | sparse_quarter | 19.696 | 19.426 | 1.013x |
| lower | dense | 32.999 | 32.804 | 1.005x |
| lower | dense_null10 | 31.429 | 30.950 | 1.019x |
| lower | duplicate | 34.128 | 33.339 | 1.026x |
| lower | reverse | 33.977 | 33.219 | 1.022x |
| lower | sparse_quarter | 42.973 | 43.047 | 0.999x |

## Process repeatability

| workload | M27 range | candidate range |
|---|---:|---:|
| any_value_datetime | 9.660–9.679 | 9.480–9.519 |
| any_value_duration | 12.877–12.961 | 13.460–13.506 |
| any_value_int_unsigned | 9.807–9.875 | 9.745–9.845 |
| any_value_json | 130.981–131.498 | 131.820–132.448 |
| any_value_timestamp | 9.693–9.786 | 9.484–9.558 |
| any_value_vector | 71.855–77.936 | 70.698–70.731 |
| decimal_add | 131.783–132.164 | 132.069–132.799 |
| decimal_compare_lt | 74.785–75.110 | 74.548–74.738 |
| decimal_compare_nulleq | 74.820–74.934 | 74.700–74.955 |
| integer_add | 12.185–12.278 | 12.271–12.290 |
| lower | 34.357–34.506 | 32.755–32.864 |
| real_add | 12.383–12.628 | 12.660–13.006 |
| round_int_signed | 7.388–7.567 | 7.334–7.437 |
| round_int_unsigned | 7.388–7.435 | 7.308–7.357 |

Ranges of3process medians are not confidence intervals. CPU2 pinning does not isolate host/SMT/boost effects; no compilation/tests overlap timing. No application QPS or universal performance claim.

## Decision: rejected and fully reverted

The target Duration workload regresses12.939→13.472ns/row (~4% slower) over
three actual-code cohorts despite elimination of the579byte standalone symbol.
No meaningful constructor benefit justifies the hint. Int12.217→12.281,
Real12.405→12.663 and JSON131.274→132.226 are also slightly slower; Decimal is
near flat. Time improves about2%, LOWER34.373→32.855 and Vector74.343→70.709
about5%; all samples/ranges remain visible. These unrelated gains are not
attributed to a successful constructor optimization. The declared target fails.

Independent M15 controls compare experimental code only: Int12.837→12.270,
Time10.167→9.498, Duration14.383→13.513, LOWER32.999→32.804. They do not reset
the adopted baseline or erase prior receipts. All2940/210 and840/60 groups match.
No universal/QPS claim, no assertion that source inlining guarantees throughput.

The exact one-line hint is reverse-applied. TiKV is clean at
d5817d0bfa15ccfad1fe93bea82e779dadf5e4c9; adopted runtime remains M27
TiDB7f0daa9135330e5d64aef22a2f2819e41fd90c68, with this checkpoint adding
only documents/evidence. Candidatea3b554... remains experimental and MUST NOT
become the next before baseline. All FSP/range/normalization checks remain.

## Validation/reproduction

Fresh TiDBdatatype516/TiKVdatatype565,batch33/fullTiDB1385same4/fullTiKV1031same6 names verified. Scope fmt, make lint and diff checks pass. M27Chunk273 is reused explicitly, no Chunk source changes. No fresh server/PD-TiKV/FIPS/workspace clippy claim. Coverage67/131 unchanged, metadata only.

validation.md pins source/binary/probe hashes and commands. Run collect.py/reference-control.py/summarize.py/summarize-reference.py/make-report.py with before/after/frozen/int-reference bins. All raw logs/scripts and the exact one-line patch remain. Binaries stay outside git.
