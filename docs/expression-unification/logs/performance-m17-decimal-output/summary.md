# Direct logical Decimal output (2026-10-16)

Batch Decimal output now projects shared logical words directly into native
MyDecimal cells. It removes the explicit per-value coefficient Vec allocation
and digit generation/reparsing at this boundary. The one shared registry,
generated kernels and eager driver are unchanged. Input uses the M16 logical
cell projection. No duplicate batch route or migration switch is retained;
the generic native coefficient bridge remains needed by other value consumers.

Strict validation is preserved: result scale<=storage, initialized nonempty
active prefix, base-1e9 ranges, canonical head/tail and the old logical byte-span
resource preflight. Integer-leading zeros normalize before the nine-word native
capacity check; wide zero prefixes can therefore fit, while retained wide
fractions cannot. Zero sign clears as in the old native producer. Inactive
output words are zero. There is no new SQL65/30 cap or raw physical/status export.
The converted Vec<Option<MyDecimal>>, its retained accounting, full validation
before output publication and common bulk append remain unchanged.

## Release results

Dense1024 selected rows, ns/selected-row. Three actual M16/M17 binary pairs
alternate before→after / after→before / before→after. Seven adaptive approximately
75ms samples per cell, five shapes. Latency is median process median, speedup is
median paired ratio. This is a real code change comparison, NOT a forced current
fallback ablation. Frozen native is a separate original production pair with only
the archived test probe added; it runs after each pair with the same data/schema.

| workload | M16 | M17 | paired speedup | frozen native | native / M17 |
|---|---:|---:|---:|---:|---:|
| any_value_datetime | 10.569 | 10.434 | 1.01x | 122.540 | 11.74x |
| any_value_duration | 14.735 | 15.918 | 0.93x | 123.451 | 7.76x |
| any_value_json | 133.453 | 132.990 | 1.00x | 142.313 | 1.07x |
| any_value_timestamp | 10.581 | 10.456 | 1.01x | 127.366 | 12.18x |
| any_value_vector | 70.324 | 70.021 | 1.00x | 154.139 | 2.20x |
| decimal_add | 183.142 | 158.790 | 1.15x | 70.171 | 0.44x |
| decimal_compare_lt | 103.994 | 103.119 | 1.01x | 166.966 | 1.62x |
| decimal_compare_nulleq | 103.823 | 102.900 | 1.01x | 165.355 | 1.61x |
| integer_add | 13.532 | 13.161 | 1.03x | 21.773 | 1.65x |
| lower | 33.147 | 34.550 | 0.96x | 150.600 | 4.36x |
| real_add | 14.026 | 13.434 | 1.04x | 107.160 | 7.98x |

All2310 current timing records and165 workload-shape-pair checksum groups agree,
including33 frozen-native/current dense groups. All batch samples use prepared
only with zero fallback/refusal. Every dense/NULL-every10/sparse/reverse/duplicate
sample and process median is retained. Flat/slower controls remain visible. This
is not SQL-server QPS or all-function coverage. CPU2 pinning does not isolate SMT,
boost, thermal or other host effects; no compilation ran during timing.

## Independent M15 regression reference

The M16 checkpoint reported Int/Time regressions relative to M15. A separate
fresh reference retains the actual M15 binary rather than merely changing the
primary baseline to M16. Three pairs reverse the initial order, with seven100ms
samples for Int PLUS and Datetime across all five shapes. All420 timing records /
30 checksum groups agree and all routes remain prepared-only. Values below are
median process medians; ratios are paired medians. Any remaining slower result
is retained without an established causal attribution.

| workload | shape | M15 | M17 | paired speedup |
|---|---|---:|---:|---:|
| any_value_datetime | dense | 10.203 | 10.440 | 0.977x |
| any_value_datetime | dense_null10 | 10.363 | 10.684 | 0.969x |
| any_value_datetime | duplicate | 10.384 | 10.648 | 0.976x |
| any_value_datetime | reverse | 10.513 | 10.655 | 0.987x |
| any_value_datetime | sparse_quarter | 16.164 | 16.482 | 0.987x |
| integer_add | dense | 12.866 | 13.170 | 0.976x |
| integer_add | dense_null10 | 12.602 | 12.955 | 0.972x |
| integer_add | duplicate | 13.051 | 13.432 | 0.974x |
| integer_add | reverse | 13.090 | 13.420 | 0.973x |
| integer_add | sparse_quarter | 19.634 | 20.233 | 0.977x |

## Cohort and validation

- Before M16: TiDB01d879349f13a6c9b862b619283a1182ce4b9034 / TiKVd38538fbce0e7c97de0d98e598589e24c7529126. Binary SHA25601503bb1bcbec895742d9dcfc7bddb11b443ffb8af6fb46cae6871d21421d64d.
- After: report's TiDB commit / paired TiKV listed in validation.md. Binary SHA256d29d409f280e55e71e38b36f4c0b8c765342cf2f2e15510efc0b08b2bae958d0.
- M15 reference: TiDB6e46687c0dc1771ca4084f44c935534099b83868 / TiKV8b210d8330eb7afecf973f6ccee0df059e76eb03. Binary SHA256025aa8b47be1fb0a540662e576c589cb800ed0d2178a873f3e4c28f3c461399d.
- True frozen native: TiDB364aef2bab5cc633ecb76a775ae8f36f86a6687d / TiKV548812e1ef57aef077a2062a9cc356640a6347f5; test-only probe/module registration changes. Reused exact M15 probe binary91f9c56c4a52ba68fd51976ceb287559ed2929f056a12ba570c5764720dcebe9, freshly timed. Archived probe ../../evidence/original-native-narrow-integer-performance-m15.rs (SHA256325197d64b4059be2981a1c1e08b51114c0da4f143c4edfb01d64fe952abc93d).
- 130236 strict/wide values at six limits plus197056 native-cell output cases match old bridge bytes/error categories. TiDB datatype515 pass, TiKV datatype validation described in validation.md; release TiDB batch28 pass and full1379/same4 baseline failures. Reused TiKV expression1028/same6 from M15 is not a fresh M17 full-expression claim.
- make lint, scoped Rust formatting and both diff checks pass. No fresh server, PD/TiKV, FIPS or full workspace clippy success claimed.
- Ryzen9 9900X, CPU2, Rust1.97.1/bootstrap, GCC/G++14, empty RUSTFLAGS and release. machine.txt/toolchain.txt retain host facts.

## Reproduce

Build matching current release libtests as before-bin/after-bin and original-native
with only the archived probe as frozen-bin beside collect.py. Keep the actual M15
libtest as int-reference-bin for the separate control. Binaries are not committed.
Run collect.py, reference-control.py, summarize.py, summarize-reference.py and
make-report.py; record new hashes. validation.md preserves exact tests, build
environment and source boundaries.

## Process repeatability

Three process-median ranges, not confidence intervals:

| workload | M16 range | M17 range | native range |
|---|---:|---:|---:|
| any_value_datetime | 10.560–10.588 | 10.419–10.457 | 122.329–122.623 |
| any_value_duration | 14.726–14.752 | 15.869–15.923 | 123.097–124.300 |
| any_value_json | 132.975–133.543 | 132.453–133.071 | 141.540–143.296 |
| any_value_timestamp | 10.560–10.614 | 10.407–10.465 | 127.119–127.660 |
| any_value_vector | 70.275–73.994 | 69.995–73.550 | 153.911–155.227 |
| decimal_add | 183.083–193.949 | 158.707–159.163 | 70.109–70.322 |
| decimal_compare_lt | 103.318–105.055 | 102.591–103.865 | 166.700–167.392 |
| decimal_compare_nulleq | 103.240–104.282 | 102.089–103.042 | 165.245–166.554 |
| integer_add | 13.522–13.538 | 13.153–13.185 | 21.756–21.869 |
| lower | 33.128–33.521 | 34.529–34.646 | 150.280–154.375 |
| real_add | 14.001–14.040 | 13.408–13.670 | 106.373–108.891 |

## Remaining cost profile

A separate post-timing cycle profile of Decimal PLUS across all five shapes collected3395 samples with no loss. Input cell projection accounts for26.30% self, Decimal vector push9.89%, output cell projection9.20%, Decimal constructor try_new9.06%, native physical decode6.57%, and addition worker4.88%. This is not allocator counting or a dense-only profile. Source inspection finds the fresh constructor reuses repeated general storage validation, providing a small next target before changing arithmetic semantics. Text and command receipts are retained; perf.data stays outside git. The control regression cause remains unestablished.
