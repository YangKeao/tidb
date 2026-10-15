# M23: integer bit operations on the shared batch driver (2026-10-16)

AND/OR/XOR/left/right shift now use the same eager driver and generated kernels.
The catalog declares Signed/FirstArgument/Always result flags; these five require
Always UNSIGNED. Operand unsigned permission and required result flags are
separate facts in the one registry. All input64 bits are preserved; shift counts
interpreted as u64 at least64 (including negative i64 bit patterns) return zero.
No new execution mode or per-function driver is introduced. Signed bitwise
result declarations and unsigned comparison results are refused. The older
row profile remains signed-only.

The five new workloads compare actual current batch to a clearly labeled
same-current-binary fallback ablation, and separately to true frozen original
native production. That fallback is NOT original native or historical TiKV.
Eleven existing control workloads instead compare distinct actual M21 and M23
batch binaries. No forced singleton route is used as a historical baseline.

Three alternating pairs;7samples75ms,1024selected rows and5selection/NULL shapes.
Latency is median of3process medians; ratios are medians of within-pair ratios.
Actual native dense runs after each pair on identical defaultLongLong inputs.
All3360current timing records/240workload-shape-pair checksum groups match,
including48fresh actual-native/current dense groups. All optimized/control
batch routes are prepared-only with0fallback/refusal. The fallback ablation is
explicitly selected, not an automatic refusal. Full rows remain in summary.json.

## Newly admitted operations

Dense ns/selected-row:

| workload | current fallback ablation | batch | paired ablation/batch | true native | native/batch |
|---|---:|---:|---:|---:|---:|
| integer_bit_bitand | 1078.955 | 11.944 | 90.80x | 80.104 | 6.71x |
| integer_bit_bitor | 1081.881 | 11.936 | 90.75x | 79.886 | 6.69x |
| integer_bit_bitxor | 1083.818 | 12.027 | 90.12x | 80.606 | 6.70x |
| integer_bit_leftshift | 1080.221 | 11.949 | 89.91x | 79.651 | 6.67x |
| integer_bit_rightshift | 1080.931 | 12.002 | 90.06x | 79.769 | 6.65x |

## Existing workload controls

These baseline cells are actual M21 batch code, unlike the new-family ablation
above. Flat and slower results remain visible.

| workload | M21 | M23 | paired M21/M23 | true native |
|---|---:|---:|---:|---:|
| any_value_datetime | 10.878 | 10.571 | 1.031x | 122.739 |
| any_value_duration | 15.016 | 13.862 | 1.083x | 123.035 |
| any_value_json | 132.822 | 133.079 | 0.998x | 142.387 |
| any_value_timestamp | 10.867 | 10.560 | 1.030x | 122.853 |
| any_value_vector | 70.421 | 70.034 | 1.006x | 153.508 |
| decimal_add | 129.951 | 130.790 | 0.994x | 70.384 |
| decimal_compare_lt | 75.270 | 75.279 | 0.998x | 167.588 |
| decimal_compare_nulleq | 75.240 | 75.502 | 0.999x | 166.201 |
| integer_add | 13.200 | 13.558 | 0.974x | 21.814 |
| lower | 34.216 | 33.773 | 1.013x | 152.146 |
| real_add | 14.005 | 14.135 | 0.991x | 106.452 |

## Independent M15 controls

| workload | shape | M15 | M23 | paired M15/M23 |
|---|---|---:|---:|---:|
| any_value_datetime | dense | 10.142 | 10.539 | 0.962x |
| any_value_datetime | dense_null10 | 10.398 | 10.757 | 0.965x |
| any_value_datetime | duplicate | 10.307 | 10.744 | 0.956x |
| any_value_datetime | reverse | 10.372 | 10.788 | 0.959x |
| any_value_datetime | sparse_quarter | 16.095 | 16.622 | 0.968x |
| any_value_duration | dense | 14.424 | 13.881 | 1.040x |
| any_value_duration | dense_null10 | 13.994 | 13.664 | 1.024x |
| any_value_duration | duplicate | 14.541 | 13.959 | 1.048x |
| any_value_duration | reverse | 14.572 | 14.121 | 1.033x |
| any_value_duration | sparse_quarter | 20.343 | 20.037 | 1.016x |
| integer_add | dense | 12.842 | 13.584 | 0.946x |
| integer_add | dense_null10 | 12.596 | 13.355 | 0.943x |
| integer_add | duplicate | 13.047 | 13.758 | 0.948x |
| integer_add | reverse | 13.068 | 13.779 | 0.948x |
| integer_add | sparse_quarter | 19.589 | 20.486 | 0.956x |
| lower | dense | 33.043 | 33.702 | 0.980x |
| lower | dense_null10 | 31.027 | 32.518 | 0.953x |
| lower | duplicate | 33.272 | 33.847 | 0.985x |
| lower | reverse | 33.273 | 34.519 | 0.964x |
| lower | sparse_quarter | 42.942 | 45.798 | 0.938x |

All840records/60checksum groups agree. This older baseline preserves earlier regressions rather than resetting them away.

## Process repeatability

| workload | baseline range | M23 range |
|---|---:|---:|
| any_value_datetime | 10.865–10.927 | 10.542–10.593 |
| any_value_duration | 15.002–25.317 | 13.853–13.892 |
| any_value_json | 132.679–133.180 | 132.831–139.255 |
| any_value_timestamp | 10.866–10.893 | 10.554–10.575 |
| any_value_vector | 70.212–72.560 | 69.982–70.272 |
| decimal_add | 129.704–130.031 | 130.378–131.447 |
| decimal_compare_lt | 74.921–75.829 | 75.270–75.395 |
| decimal_compare_nulleq | 75.191–75.803 | 75.345–75.588 |
| integer_add | 13.176–13.278 | 13.540–13.570 |
| integer_bit_bitand | 1077.109–1117.832 | 11.883–11.989 |
| integer_bit_bitor | 1081.544–1105.225 | 11.922–11.959 |
| integer_bit_bitxor | 1082.256–1084.995 | 11.996–12.040 |
| integer_bit_leftshift | 1074.280–1081.477 | 11.934–12.033 |
| integer_bit_rightshift | 1074.888–1106.732 | 11.985–12.007 |
| lower | 34.164–34.226 | 33.650–33.780 |
| real_add | 13.986–14.014 | 14.042–14.209 |

Ranges are3process medians, not confidence intervals. CPU2 pinning does not isolate SMT/boost/thermal/other host effects. No compilation overlaps timing. No application QPS or all-function performance claim is made.

## Decision: retain the family and its explicit result contract

The five operations measure11.936–12.027ns/selected-row against true native
79.651–80.606ns, about6.6–6.7x faster. The same-current fallback ablation is
1078.955–1083.818ns and is labeled separately; its roughly90x ratio is not a
historical-native gain. Retain this complete family on the existing driver.

Old controls have tradeoffs: Int13.200→13.558ns (~3% slower), Real14.005→14.135
and DecimalPLUS129.951→130.790 (~1% slower); Decimal comparisons/JSON are near
flat, time/Duration/LOWER improve. All per-shape results and process dispersion
remain visible. The cause of small unrelated changes is not established. M15
independent dense Int12.842→13.584 (~6% slower), Datetime10.142→10.539 (~4%
slower), LOWER33.043→33.702 (~2% slower) remain, while Duration14.424→13.881
improves. Decimal PLUS remains slower than original native; this is not a claim
of universal improvement, all-function optimization or application QPS gain.

Retained TiKV57d3d13d48b425007ed26e0c995b85d5e50101cd / this TiDB report commit.
No temporary driver, alternate bit algorithm or migration execution gate is
introduced. Existing native fallback for functions outside the shared closure
is unchanged; no family-specific fallback remains alongside admitted bitwise
execution. The single registry/catalog and generated kernel selector own the
five signatures. Older row profiles remain unchanged.

## Validation/reproduction

Batch29, fullTiDB1381/same4 and fullTiKV1029/same6 names verified; PB result flags, golden extrema/shift boundaries/NULL and2050duplicate/reverse windows pass. Scoped fmt, make lint and diff checks pass. Datatype516/565 are explicitly reused M22 results (no datatype code changes). No fresh server, PD/TiKV, FIPS or workspace clippy pass claimed. Coverage inventory56admitted/131static signature names is metadata, not an SQL coverage percentage.

validation.md pins exact source/binary/probe hashes and commands. Build current as after-bin, original plus the archived probe as frozen-bin and actual M21 as before-bin. collect.py/reference-control.py generate samples; summarize.py/summarize-reference.py assert data and generate JSON; make-report.py regenerates this receipt using decision.md. Binaries remain outside git.
