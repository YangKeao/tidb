import json
from pathlib import Path
root=Path(__file__).resolve().parent
rows=json.loads((root/'summary.json').read_text());dense=[r for r in rows if r['scenario']=='dense']
text='''# M21: choose the smaller partial-word optimization (2026-10-16)

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
'''
for r in dense:text+=f"| {r['workload']} | {r['m20_median']:.3f} | {r['full_median']:.3f} | {r['head_median']:.3f} | {r['m20_over_head']:.3f}x | {r['full_over_head']:.3f}x | {r['native_median']:.3f} |\n"
text+='''
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
'''
for r in json.loads((root/'reference-control-summary.json').read_text()):text+=f"| {r['workload']} | {r['scenario']} | {r['m15_median']:.3f} | {r['current_median']:.3f} | {r['paired_speedup']:.3f}x |\n"
text+='''
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
'''
for r in dense:text+=f"| {r['workload']} | {min(r['m20']):.3f}–{max(r['m20']):.3f} | {min(r['full']):.3f}–{max(r['full']):.3f} | {min(r['head']):.3f}–{max(r['head']):.3f} |\n"
text+='\n## Decision: keep the one-line head bound\n\nThe three-mode comparison selects the head-bound-only candidate. Decimal PLUS\n132.335→129.283ns/row (1.024x), LT77.772→74.849 (1.039x), NULL-safe equality\n77.754→74.977 (1.037x). Full reciprocals save another1.77–2.01ns/row, but their\ntable/helper/proof surface is not justified for this small additional gain.\nThat gain is real in these cohorts, not claimed to be absent. The full candidate\nis removed completely from production, including its helper, table and unit.\nThe exact patch and proof remain only as experimental evidence.\n\nSelected TiKV source: 8d143e47eba26e2a33562961a98b4693f1e2f22a; TiDB uses the report commit.\nThe source changes one predicate: n<d implies n%d=n, so canonical heads skip the\nremainder. Noncanonical heads still use the old remainder. Existing independent\ncell/export corpora cover all head widths and noncanonical words. No algorithm,\nresource policy, ABI or admitted function changes. Remaining Decimal PLUS is\n1.85x slower than native70.046ns. Against M15, dense Int/Datetime/Duration/LOWER\nremain about2%/7%/4%/4% slower; all shapes remain in the tables. No universal\nperformance improvement or application-level gain is claimed.\n'
(root/'summary.md').write_text(text)
