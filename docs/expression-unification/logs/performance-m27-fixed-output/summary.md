# M27: shared fixed-output buffer experiment (2026-10-16)

The owned fixed-cell append loop sizes its byte buffer once and fills chunks.
It retains the last scratch element for NULL bytes, true iterator termination,
actual row/byte counts, zero-width cells, and the unchanged shared/frozen scalar
copy-on-write behavior. A fused cloned iterator prevents restart after None;
short/long positive size hints remain safe. No unsafe code or new execution mode.
Value normalization, structural/resource checks and output prevalidation remain.

Distinct actual M26/current batch binaries compare all14workloads,3alternating
pairs,7samples75ms/5shapes/1024rows. Both modes use real batch_tikv; no same-binary
fallback ablation. New UInt schema choices are identical current/before/native,
all old controls use their original signed schema. Actual original-native dense
runs follow each pair; source is pinned in validation.md. All2940timings/
210checksum groups, including42native/current dense groups, match. All routes
prepared-only,0fallback/refusal. Latencies are medians of3process medians and
ratios medians of paired ratios. Slow controls remain in all raw/summary rows.

| workload | M26 | candidate | paired M26/candidate | true native |
|---|---:|---:|---:|---:|
| any_value_datetime | 10.933 | 9.551 | 1.146x | 122.377 |
| any_value_duration | 15.082 | 12.879 | 1.175x | 122.073 |
| any_value_int_unsigned | 10.771 | 9.798 | 1.100x | 122.469 |
| any_value_json | 130.837 | 132.312 | 0.997x | 144.590 |
| any_value_timestamp | 10.918 | 9.542 | 1.144x | 122.882 |
| any_value_vector | 72.206 | 71.647 | 1.008x | 161.425 |
| decimal_add | 131.923 | 131.975 | 0.999x | 69.622 |
| decimal_compare_lt | 76.132 | 75.012 | 1.015x | 169.903 |
| decimal_compare_nulleq | 76.035 | 75.146 | 1.012x | 165.520 |
| integer_add | 13.424 | 12.220 | 1.099x | 21.069 |
| lower | 34.695 | 34.429 | 1.006x | 156.984 |
| real_add | 13.856 | 12.411 | 1.116x | 107.242 |
| round_int_signed | 8.553 | 7.481 | 1.143x | 100.851 |
| round_int_unsigned | 8.487 | 7.483 | 1.124x | 101.535 |

## Independent M15 controls

All840records/60checksum groups agree; earlier regressions remain visible.

| workload | shape | M15 | candidate | paired M15/candidate |
|---|---|---:|---:|---:|
| any_value_datetime | dense | 10.119 | 9.597 | 1.054x |
| any_value_datetime | dense_null10 | 10.318 | 9.611 | 1.072x |
| any_value_datetime | duplicate | 10.310 | 9.732 | 1.061x |
| any_value_datetime | reverse | 10.383 | 9.715 | 1.063x |
| any_value_datetime | sparse_quarter | 16.034 | 15.594 | 1.028x |
| any_value_duration | dense | 14.409 | 12.909 | 1.115x |
| any_value_duration | dense_null10 | 14.036 | 12.629 | 1.113x |
| any_value_duration | duplicate | 14.567 | 12.874 | 1.133x |
| any_value_duration | reverse | 14.641 | 13.068 | 1.120x |
| any_value_duration | sparse_quarter | 20.240 | 18.926 | 1.069x |
| integer_add | dense | 12.899 | 12.235 | 1.053x |
| integer_add | dense_null10 | 12.596 | 11.944 | 1.058x |
| integer_add | duplicate | 13.098 | 12.469 | 1.052x |
| integer_add | reverse | 13.067 | 12.411 | 1.053x |
| integer_add | sparse_quarter | 19.623 | 19.076 | 1.029x |
| lower | dense | 32.997 | 34.384 | 0.960x |
| lower | dense_null10 | 31.095 | 32.472 | 0.956x |
| lower | duplicate | 33.347 | 34.945 | 0.953x |
| lower | reverse | 33.091 | 34.609 | 0.956x |
| lower | sparse_quarter | 42.937 | 44.390 | 0.967x |

## Process repeatability

| workload | M26 range | candidate range |
|---|---:|---:|
| any_value_datetime | 10.852–10.976 | 9.539–9.620 |
| any_value_duration | 15.061–15.221 | 12.841–12.912 |
| any_value_int_unsigned | 10.726–11.100 | 9.791–9.829 |
| any_value_json | 129.666–132.369 | 131.274–132.683 |
| any_value_timestamp | 10.905–10.934 | 9.515–9.624 |
| any_value_vector | 72.179–72.255 | 71.598–71.874 |
| decimal_add | 131.908–131.954 | 131.781–132.481 |
| decimal_compare_lt | 76.043–76.188 | 74.897–75.270 |
| decimal_compare_nulleq | 76.026–76.262 | 74.989–75.202 |
| integer_add | 13.414–13.426 | 12.210–12.256 |
| lower | 34.650–34.905 | 34.347–34.926 |
| real_add | 13.823–13.917 | 12.373–12.415 |
| round_int_signed | 8.378–8.706 | 7.411–7.537 |
| round_int_unsigned | 8.412–8.754 | 7.396–7.597 |

Ranges of3process medians are not confidence intervals. CPU2 pinning does not isolate host/SMT/boost effects; no compilation/tests overlap timing. No application QPS or universal performance claim.

## Decision: retain the shared owned-buffer loop

Three actual-code cohorts improve dense Duration15.082→12.879ns/row (paired
1.17x), Datetime10.933→9.551 and Timestamp10.918→9.542 (~1.14–1.15x),
Int13.424→12.220 (1.10x), Real13.856→12.411 (1.12x), signed ROUND8.553→7.481,
unsigned ROUND8.487→7.483 and UInt ANY_VALUE10.771→9.798 (1.10–1.14x).
These common fixed-output gains justify the safe-Rust storage change.

DecimalPLUS131.923→131.975 is flat; comparisons76.1→75.0 improve only about1%.
JSON130.837→132.312 has a slightly slower aggregate median (within-pair ratios
near flat), LOWER34.695→34.429 and Vector72.206→71.647 are near flat. All raw
processes/shapes remain visible; this is not a universal speedup. Independent
M15 Int12.899→12.235, Datetime10.119→9.597 and Duration14.409→12.909 improve,
while LOWER32.997→34.384 remains about4% slower. Older JSON/varlen losses are
not declared resolved. Decimal PLUS still trails original native. No QPS claim.

Retained source: this TiDB report commit / unchanged TiKV
 d5817d0bfa15ccfad1fe93bea82e779dadf5e4c9. The old ordinary per-row owned append
loop is replaced. The remaining tail handles zero-width or inaccurate iterator
counts; it is not a migration execution route. Shared/frozen buffers retain
their existing ownership semantics. No unchecked constructor, value-policy
change, new driver or function-specific path is introduced.

## Validation/reproduction

Chunk273pass includes independent scalar byte/storage oracle for0/4/8/16/40widths, NULL scratch after reset, bad positive size hints and first-None termination; existing alias/frozen/NULL cases pass. Batch33/fullTiDB1385same4/lintfmt pass. TiKV1031same6 and datatype516/565 reused explicitly (unchanged code). No fresh server/PD-TiKV/FIPS/workspace clippy pass. Coverage67/131 unchanged, metadata only.

validation.md preserves exact source/binary/probe hashes, build/test commands and schema selection. Run collect.py/reference-control.py/summarize.py/summarize-reference.py/make-report.py with before/after/frozen/int-reference binaries. Binaries remain outside git; patch and all raw data/scripts remain in the receipt.
