# Direct logical Decimal input (2026-10-16)

Batch input now projects checked MyDecimal cells directly into the final shared
logical word owner. It removes the intermediate native coefficient object and
per-digit generation/reparsing from that input boundary. The shared eager driver
and generated kernels are unchanged. The native value bridge remains used by
other native value consumers; no equivalent batch migration path is retained.

This is NOT a physical-cell copy. A physical cell permits noncanonical partial
word padding, leading zeros, zero sign, independent visible scale and inactive
words. The old logical value bridge strips integer-leading zeros, clears negative
zero, drops head/tail/inactive padding, and extends storage to visible scale.
The new projection implements those same value rules before constructing its
final owner. Counts/active words are checked; there is no SQL65/30 cap or new
unchecked layout. Existing per-owner word budget includes nine initialized inline
cells; visible scales beyond the source fraction can allocate a larger owner.
No ASCII buffer is introduced. Chunk still reads checked40-byte MyDecimal cells.

## Release results

Dense1024 selected rows, ns/selected-row. Real before/after production binaries,
three alternating M15→M16 / M16→M15 / M15→M16 cohorts. Each cell has seven adaptive
approximately75ms samples; latency is median process median and speedup is median
paired ratio. This is a true code change comparison, NOT the forced current-fallback
ablation used in separate reports. Nine carrier workloads act as controls;
Decimal LT and NULL-safe equality expose input conversion without Decimal output.

A separate true frozen native production pair runs after every pair. Its version
comparison is distinct from the before/after ratio. All probes use the same
SQL/data, schema/defaultLongLong and output materialization. Setup is excluded;
normal warm evaluation, output reset and input/output transport are measured.

| workload | M15 batch | M16 batch | paired speedup | frozen native | native / M16 |
|---|---:|---:|---:|---:|---:|
| any_value_datetime | 10.156 | 10.564 | 0.96x | 122.356 | 11.58x |
| any_value_duration | 14.418 | 14.750 | 0.98x | 122.708 | 8.32x |
| any_value_json | 133.266 | 133.404 | 1.00x | 141.862 | 1.06x |
| any_value_timestamp | 10.163 | 10.570 | 0.96x | 122.464 | 11.59x |
| any_value_vector | 71.898 | 70.557 | 1.02x | 154.897 | 2.20x |
| decimal_add | 228.438 | 182.680 | 1.25x | 70.089 | 0.38x |
| decimal_compare_lt | 148.866 | 102.485 | 1.45x | 167.289 | 1.63x |
| decimal_compare_nulleq | 148.356 | 102.584 | 1.45x | 164.923 | 1.61x |
| integer_add | 12.878 | 13.569 | 0.95x | 21.791 | 1.61x |
| lower | 32.986 | 33.186 | 0.99x | 152.290 | 4.59x |
| real_add | 13.724 | 14.039 | 0.98x | 106.288 | 7.57x |

All2310 current before/after timing records and165 workload-shape-pair checksum
groups agree, including33 true-native/current dense groups. All measured batch
samples are prepared-only, zero fallback/refusal. Dense, NULL-every10, sparse,
reverse and duplicate sample data are retained in raw logs/summary.json. Flat
or slower controls remain in the table. This is not SQL-server QPS or all-function
coverage. CPU2 pinning does not isolate SMT, boost, thermal or other host effects;
no compilation ran during timing. Decimal output still uses a coefficient bridge,
and this slice alone does not claim closure of the original-native Decimal PLUS gap.

## Source and validation

- Before: TiDB6e46687c0dc1771ca4084f44c935534099b83868 / TiKV8b210d8330eb7afecf973f6ccee0df059e76eb03. Binary SHA256025aa8b47be1fb0a540662e576c589cb800ed0d2178a873f3e4c28f3c461399d.
- After: TiDB commit containing this report / TiKVd38538fbce0e7c97de0d98e598589e24c7529126. Binary SHA25601503bb1bcbec895742d9dcfc7bddb11b443ffb8af6fb46cae6871d21421d64d.
- Frozen native: TiDB364aef2bab5cc633ecb76a775ae8f36f86a6687d / TiKV548812e1ef57aef077a2062a9cc356640a6347f5; only the archived M15 test probe/registration differs. Reused exact binary SHA25691f9c56c4a52ba68fd51976ceb287559ed2929f056a12ba570c5764720dcebe9, freshly timed. Probe ../../evidence/original-native-narrow-integer-performance-m15.rs SHA256325197d64b4059be2981a1c1e08b51114c0da4f143c4edfb01d64fe952abc93d.
- 197056 independent old/new cell projections compare every logical field and initialized word, including count/padding/negative-zero/inactive-word/visible-scale variants and exact resource boundaries. See validation.md for the precise targeted corpus, not a claim of all header values.
- TiDB datatype514 pass; TiKV datatype564 pass. Final release TiDB batch28 pass, full1379 pass/same4 baseline failures. The preceding M15 TiKV expression1028/same6 evidence is reused, not rerun for this new datatype method that TiKV expressions do not call.
- TiDB make lint, scoped rustfmt and both diff checks pass. Rust-only inputs, no bazel_prepare required. No fresh server, PD/TiKV, FIPS or full workspace clippy pass claimed.
- Ryzen9 9900X, CPU2, Rust1.97.1/bootstrap, GCC/G++14, empty RUSTFLAGS and release. machine.txt/toolchain.txt retain host facts.

## Reproduce

Build before/after release libtests into before-bin/after-bin beside collect.py.
Rebuild the frozen production pair with only the archived probe as frozen-bin,
or use the retained exact M15 binary. Binaries are not committed. Run collect.py,
summarize.py and make-report.py; record new hashes. validation.md preserves
commands, environment and version/cohort boundaries.

## Process repeatability

Three process-median ranges (not confidence intervals):

| workload | M15 range | M16 range | native range |
|---|---:|---:|---:|
| any_value_datetime | 10.150–10.167 | 10.560–10.615 | 122.118–122.841 |
| any_value_duration | 14.391–14.598 | 14.728–14.797 | 122.419–122.957 |
| any_value_json | 133.021–133.437 | 133.315–133.534 | 141.821–142.143 |
| any_value_timestamp | 10.158–10.197 | 10.566–10.586 | 122.141–122.475 |
| any_value_vector | 71.732–72.243 | 70.463–70.747 | 154.568–155.658 |
| decimal_add | 228.387–231.943 | 182.209–182.715 | 70.063–70.202 |
| decimal_compare_lt | 148.551–149.285 | 102.283–102.858 | 166.912–167.417 |
| decimal_compare_nulleq | 147.912–149.291 | 102.496–102.818 | 164.780–165.826 |
| integer_add | 12.863–12.916 | 13.530–13.575 | 21.757–21.812 |
| lower | 32.977–33.028 | 33.146–33.321 | 151.890–152.440 |
| real_add | 13.709–13.776 | 14.034–14.070 | 106.178–108.665 |

## Targeted integer regression check

The main cohort shows Int PLUS about5% slower, and Time about4% slower. A separate three-pair Int-only check reverses initial order, uses seven150ms samples per shape, and confirms the Int difference across all shapes. Its210 records and15 checksum groups match with prepared-only routes. These are retained tradeoffs, not universal speedups; no root cause is established by this measurement. The next output-bridge slice will recheck this control. Generic calibration medians are similar before/after.

| shape | before | after | paired speedup |
|---|---:|---:|---:|
| dense | 12.833 | 13.586 | 0.948x |
| dense_null10 | 12.659 | 13.306 | 0.952x |
| duplicate | 13.074 | 13.838 | 0.946x |
| reverse | 13.100 | 13.746 | 0.951x |
| sparse_quarter | 19.637 | 20.395 | 0.964x |
