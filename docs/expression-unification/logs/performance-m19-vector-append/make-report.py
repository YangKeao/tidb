import json
from pathlib import Path
root=Path(__file__).resolve().parent
rows=json.loads((root/'summary.json').read_text());dense=[r for r in rows if r['scenario']=='dense']
table='| workload | M17 | candidate | paired speedup | frozen native | native / candidate |\n|---|---:|---:|---:|---:|---:|\n'
for r in dense:table+=f"| {r['workload']} | {r['before_median']:.3f} | {r['after_median']:.3f} | {r['paired_speedup']:.2f}x | {r['native_median']:.3f} | {r['native_median']/r['after_median']:.2f}x |\n"
text='''# Generic vector append inlining experiment (2026-10-16)

The M17 profile attributes about10% self to out-of-line owning Decimal vector
append. This candidate changes only ChunkedVecSized<T>::push_data from inline
 to inline(always), allowing the compiler to remove that call boundary and
large value ABI copies. Ownership, bitmap/data append order, NULL backing,
arithmetic, transport, driver and registry semantics are unchanged. The rejected
M18 constructor patch is absent; actual M17 production code is the baseline.

Existing owner/drop/NULL/resource tests cover the unchanged behavior. Four
out-of-line Decimal push_data symbols in the baseline disappear in the candidate;
this confirms code generation changed, not that throughput must improve. ELF
text grows from69430989 to69476641 bytes (45652 bytes); total binary size is not
an application memory or instruction-cache measurement. Keep/reject is decided
by the measured workload/control results below.

## Release results

Dense1024 selected rows, ns/selected-row. Three actual M17/candidate binary pairs
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

- Before M17: TiDB016e8e230b8351eefe31fb44c5b9b2a83e0f09dd / TiKV44befe011baf3082d18398b27428f47030a29be2. Binary SHA256d29d409f280e55e71e38b36f4c0b8c765342cf2f2e15510efc0b08b2bae958d0.
- Candidate binary SHA2569f0910ee69f166a6c46a58a2b24c9fbf68fd0e3e12a48797eff246669cb8167f; source checkpoint is recorded in validation.md and the final decision below.
- M15 reference: TiDB6e46687c0dc1771ca4084f44c935534099b83868 / TiKV8b210d8330eb7afecf973f6ccee0df059e76eb03, binary025aa8b47be1fb0a540662e576c589cb800ed0d2178a873f3e4c28f3c461399d.
- True frozen production: TiDB364aef2bab5cc633ecb76a775ae8f36f86a6687d / TiKV548812e1ef57aef077a2062a9cc356640a6347f5, only M15 test-only probe/registration differs. Exact reused binary91f9c56c4a52ba68fd51976ceb287559ed2929f056a12ba570c5764720dcebe9, freshly timed. Source ../../evidence/original-native-narrow-integer-performance-m15.rs, SHA256325197d64b4059be2981a1c1e08b51114c0da4f143c4edfb01d64fe952abc93d.
- TiDB datatype515 and TiKV datatype565 pass, including existing wide hidden-scale/resource, initialized NULL owners,197056 cell projections and130236 strict/wide exports at six limits. Final release batch28 pass, fullTiDB1379/same4 baseline failures. Fresh TiKV full expression1028/same6 names verified, with no new failure.
- make lint, scoped Rust formatting and both diff checks pass. No fresh server, PD/TiKV, FIPS or full workspace clippy success claimed. Exact commands and cohort receipts are in validation.md.
- Ryzen9 9900X, CPU2, Rust1.97.1/bootstrap, GCC/G++14, empty RUSTFLAGS, release.

## Reproduce

Build matching M17/candidate release libtests as before-bin/after-bin. Preserve
the actual M15 binary as int-reference-bin and original-native production with
only the archived probe as frozen-bin. Binaries are not committed. Run collect.py,
reference-control.py, summarize.py, summarize-reference.py and make-report.py.
Record new hashes; validation.md preserves environment/commands and final source.

## Process repeatability

Ranges of three process medians, not confidence intervals:

| workload | M17 range | candidate range | native range |
|---|---:|---:|---:|
'''
for r in dense:text+=f"| {r['workload']} | {min(r['before']):.3f}–{max(r['before']):.3f} | {min(r['after']):.3f}–{max(r['after']):.3f} | {min(r['frozen_native']):.3f}–{max(r['frozen_native']):.3f} |\n"
text+='\n## Decision: retained with measured tradeoffs\n\nThree cohorts consistently improve Decimal PLUS158.749→149.122ns/row (1.07x), LT102.791→94.233 and NULL-safe equality103.245→94.419 (1.09x). Duration15.878→14.919 and JSON131.806→127.864 also improve. The candidate is retained as the one-line append hint, paired with TiKV366723653e14e12b7f3bf2a6f0d40f5e3eb5919d and the TiDB commit containing this report. No new driver, API or semantic mode is introduced.\n\nThis is not a universal gain: Real13.319→13.989, Datetime10.406→10.761 and Timestamp10.427→10.731 are slower; Vector is about1% slower and LOWER flat. M15 reference controls still show remaining Int/Time/Duration/LOWER regressions, all retained above. The source change helps the measured Decimal append cost; no established explanation or application-level gain is claimed for all control differences. Decimal PLUS remains about twice true native, so input transport remains an explicit optimization direction.\n'
(root/'summary.md').write_text(text);(root/'dense-table.md').write_text(table)
