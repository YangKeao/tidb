# Exact partial-word power division experiment (2026-10-16)

The experimental candidate uses exact high-product integer
quotients for powers of ten. Reciprocal constants are derived from existing
TEN_POW values at compile time. For n:u32,d<=1e9,B=2^64,M=ceil(B/d), n*d<B
bounds the reciprocal error below the next quotient gap: floor(n*M/B)=floor(n/d).
Power0 returns n directly. Full n*M fits u128; no floating point is involved.
validation.md gives the bound and the independent oracle checks.

Canonical heads already below their power skip reduction; noncanonical heads
still reduce exactly. Fraction truncation and strict padding rejection use the
same exact quotient. Decimal arithmetic, rounding, Fixed/Grow statuses, physical
admission, normalization rules, resource preflights and shared driver stay
unchanged. One private helper serves the format projection; there is no new
function-specific route, parser or value domain.

The core oracle compares native integer division for powers0..9, full-u32
and quotient-boundary edges, and1M deterministic random comparisons. Existing
197056 cell projections and130236 strict/wide output values at six limits
independently cover the complete format paths. Generated input/output methods
have0/0 hardware div instructions versus M20's2/1; this is a codegen receipt,
not by itself proof of throughput improvement.

## Release results

Dense1024 selected rows, ns/selected-row. Three actual M20/candidate binary pairs
alternate before→after / after→before / before→after. Seven adaptive75ms samples
per cell across all five shapes. Latency is median process median; speedup is
median paired ratio. Both execute batch, not a forced fallback ablation. Frozen
native is a distinct true original production pair with test-only probe added,
freshly timed after each pair over the same data/defaultLongLong schema.

| workload | M20 | candidate | paired speedup | frozen native | native / candidate |
|---|---:|---:|---:|---:|---:|
| any_value_datetime | 10.805 | 10.482 | 1.03x | 122.816 | 11.72x |
| any_value_duration | 14.946 | 14.815 | 1.01x | 123.377 | 8.33x |
| any_value_json | 133.678 | 132.427 | 1.01x | 142.206 | 1.07x |
| any_value_timestamp | 10.825 | 10.485 | 1.03x | 123.101 | 11.74x |
| any_value_vector | 71.964 | 69.864 | 1.03x | 153.293 | 2.19x |
| decimal_add | 132.893 | 128.113 | 1.04x | 70.340 | 0.55x |
| decimal_compare_lt | 77.887 | 73.213 | 1.06x | 166.875 | 2.28x |
| decimal_compare_nulleq | 78.109 | 73.153 | 1.07x | 165.003 | 2.26x |
| integer_add | 13.130 | 13.377 | 0.98x | 21.745 | 1.63x |
| lower | 34.181 | 34.110 | 1.00x | 153.551 | 4.50x |
| real_add | 13.954 | 14.089 | 0.99x | 106.499 | 7.56x |

All2310 primary timing records /165 checksum groups agree, including33 true-native
current dense groups; all batch routes are prepared-only, zero fallback/refusal.
Dense/NULL-every10/sparse/reverse/duplicate raw data and process medians remain
available. Flat/slower cells are retained. CPU2 pinning does not isolate SMT,
boost, thermal or other host effects. No compilation ran during timing. This is
not SQL-server QPS, allocator counting or all-function coverage.

## Independent M15 controls

The actual M15 binary remains a separate reference for Int PLUS, Datetime,
Duration and LOWER. This expands the previous control rather than hiding prior
regressions behind a new baseline. Three pairs reverse initial order, seven75ms
samples over five shapes:840 timing records /60 checksum groups agree and all
routes are prepared-only. No causal attribution is established for slower controls.

| workload | shape | M15 | candidate | paired speedup |
|---|---|---:|---:|---:|
| any_value_datetime | dense | 10.154 | 10.483 | 0.968x |
| any_value_datetime | dense_null10 | 10.377 | 10.711 | 0.964x |
| any_value_datetime | duplicate | 10.271 | 10.679 | 0.966x |
| any_value_datetime | reverse | 10.361 | 10.713 | 0.965x |
| any_value_datetime | sparse_quarter | 16.058 | 16.647 | 0.967x |
| any_value_duration | dense | 14.365 | 14.824 | 0.969x |
| any_value_duration | dense_null10 | 13.972 | 14.013 | 0.997x |
| any_value_duration | duplicate | 14.528 | 14.805 | 0.981x |
| any_value_duration | reverse | 14.556 | 15.034 | 0.972x |
| any_value_duration | sparse_quarter | 20.253 | 20.822 | 0.973x |
| integer_add | dense | 12.837 | 13.371 | 0.960x |
| integer_add | dense_null10 | 12.621 | 13.194 | 0.957x |
| integer_add | duplicate | 13.054 | 13.544 | 0.964x |
| integer_add | reverse | 13.067 | 13.547 | 0.964x |
| integer_add | sparse_quarter | 19.553 | 20.055 | 0.970x |
| lower | dense | 32.996 | 34.184 | 0.966x |
| lower | dense_null10 | 31.154 | 32.187 | 0.968x |
| lower | duplicate | 33.286 | 34.632 | 0.961x |
| lower | reverse | 33.178 | 34.495 | 0.961x |
| lower | sparse_quarter | 42.923 | 44.433 | 0.966x |

## Cohort and validation

- Before M20: TiDB835daf31153006a41d5d7f9ae1e7755e18db1f4a / TiKV7c427c84ec6f341e04ed516c84b4333333d65b3d. Binary SHA25695264c1c925ceaca9d113ef42092102a0d45d92e921adc1d43240ef14dddb5b9.
- Candidate binary SHA256cce19d0a0798ef5303803cb14d17fa0196241bdff3fcdd729e29dfc7564826fa; source checkpoint is recorded in validation.md and the final decision below.
- M15 reference: TiDB6e46687c0dc1771ca4084f44c935534099b83868 / TiKV8b210d8330eb7afecf973f6ccee0df059e76eb03, binary025aa8b47be1fb0a540662e576c589cb800ed0d2178a873f3e4c28f3c461399d.
- True frozen production: TiDB364aef2bab5cc633ecb76a775ae8f36f86a6687d / TiKV548812e1ef57aef077a2062a9cc356640a6347f5, only M15 test-only probe/registration differs. Exact reused binary91f9c56c4a52ba68fd51976ceb287559ed2929f056a12ba570c5764720dcebe9, freshly timed. Source ../../evidence/original-native-narrow-integer-performance-m15.rs, SHA256325197d64b4059be2981a1c1e08b51114c0da4f143c4edfb01d64fe952abc93d.
- TiDB datatype516 and TiKV datatype566 pass, including existing wide hidden-scale/resource, initialized NULL owners,197056 cell projections and130236 strict/wide exports at six limits. Final release batch28 pass, fullTiDB1379/same4 baseline failures. TiKV full expression1028/same6 names were freshly verified in M19 and reused here (this input adapter is consumed by TiDB).
- make lint, scoped Rust formatting and both diff checks pass. No fresh server, PD/TiKV, FIPS or full workspace clippy success claimed. Exact commands and cohort receipts are in validation.md.
- Ryzen9 9900X, CPU2, Rust1.97.1/bootstrap, GCC/G++14, empty RUSTFLAGS, release.

## Reproduce

Build matching M20/candidate release libtests as before-bin/after-bin. Preserve
the actual M15 binary as int-reference-bin and original-native production with
only the archived probe as frozen-bin. Binaries are not committed. Run collect.py,
reference-control.py, summarize.py, summarize-reference.py and make-report.py.
Record new hashes; validation.md preserves environment/commands and final source.

## Process repeatability

Ranges of three process medians, not confidence intervals:

| workload | M20 range | candidate range | native range |
|---|---:|---:|---:|
| any_value_datetime | 10.800–10.840 | 10.470–10.508 | 122.725–122.891 |
| any_value_duration | 14.946–15.047 | 14.786–14.869 | 123.043–123.444 |
| any_value_json | 133.618–135.223 | 132.148–133.564 | 141.886–142.682 |
| any_value_timestamp | 10.817–10.840 | 10.475–10.505 | 122.707–123.123 |
| any_value_vector | 69.661–72.417 | 69.769–69.911 | 153.085–153.377 |
| decimal_add | 132.371–134.246 | 128.100–128.316 | 70.307–70.418 |
| decimal_compare_lt | 77.748–78.409 | 73.199–73.311 | 166.578–167.439 |
| decimal_compare_nulleq | 78.081–78.381 | 72.918–73.205 | 164.833–165.141 |
| integer_add | 13.100–13.176 | 13.346–13.402 | 21.741–21.875 |
| lower | 34.166–34.352 | 34.054–34.200 | 152.789–153.897 |
| real_add | 13.952–14.048 | 14.039–14.094 | 106.251–109.506 |

## Decision: full reciprocal candidate superseded

This initial full-candidate run improves PLUS132.893→128.113ns and comparisons
about78→73ns; its correctness/proof and all controls are retained above. A later
three-mode experiment finds a one-line head bound captures most of that gain.
The full candidate still saves another roughly1.8–2ns but is not selected because
of its added maintenance surface. No reciprocal helper/table/test remains in
production. See the [selected head-bound receipt](../performance-m21-head-bound/summary.md)
for the actual three-mode comparison and final source commit.
