import json
from pathlib import Path
root=Path(__file__).resolve().parent
rows=json.loads((root/'summary.json').read_text());dense=[r for r in rows if r['scenario']=='dense']
table='| workload | M20 | candidate | paired speedup | frozen native | native / candidate |\n|---|---:|---:|---:|---:|---:|\n'
for r in dense:table+=f"| {r['workload']} | {r['before_median']:.3f} | {r['after_median']:.3f} | {r['paired_speedup']:.2f}x | {r['native_median']:.3f} | {r['native_median']/r['after_median']:.2f}x |\n"
text='''# Exact partial-word power division experiment (2026-10-16)

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

'''+table+'''
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
'''
for r in json.loads((root/'reference-control-summary.json').read_text()):text+=f"| {r['workload']} | {r['scenario']} | {r['m15_median']:.3f} | {r['current_median']:.3f} | {r['paired_speedup']:.3f}x |\n"
text+='''
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
'''
for r in dense:text+=f"| {r['workload']} | {min(r['before']):.3f}–{max(r['before']):.3f} | {min(r['after']):.3f}–{max(r['after']):.3f} | {min(r['frozen_native']):.3f}–{max(r['frozen_native']):.3f} |\n"
text+='\n## Decision: full reciprocal candidate superseded\n\nThis initial full-candidate run improves PLUS132.893→128.113ns and comparisons\nabout78→73ns; its correctness/proof and all controls are retained above. A later\nthree-mode experiment finds a one-line head bound captures most of that gain.\nThe full candidate still saves another roughly1.8–2ns but is not selected because\nof its added maintenance surface. No reciprocal helper/table/test remains in\nproduction. See the [selected head-bound receipt](../performance-m21-head-bound/summary.md)\nfor the actual three-mode comparison and final source commit.\n'
(root/'summary.md').write_text(text);(root/'dense-table.md').write_text(table)
