import json
from pathlib import Path
r=Path(__file__).resolve().parent
rows=json.loads((r/'summary.json').read_text())
dense=[x for x in rows if x['scenario']=='dense']
new=[x for x in dense if x['workload'].startswith('integer_bit_')]
controls=[x for x in dense if not x['workload'].startswith('integer_bit_')]
s='''# M23: integer bit operations on the shared batch driver (2026-10-16)

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
'''
for x in new:s+=f"| {x['workload']} | {x['before_median']:.3f} | {x['after_median']:.3f} | {x['paired_speedup']:.2f}x | {x['native_median']:.3f} | {x['native_median']/x['after_median']:.2f}x |\n"
s+='''
## Existing workload controls

These baseline cells are actual M21 batch code, unlike the new-family ablation
above. Flat and slower results remain visible.

| workload | M21 | M23 | paired M21/M23 | true native |
|---|---:|---:|---:|---:|
'''
for x in controls:s+=f"| {x['workload']} | {x['before_median']:.3f} | {x['after_median']:.3f} | {x['paired_speedup']:.3f}x | {x['native_median']:.3f} |\n"
s+='\n## Independent M15 controls\n\n'
s+='| workload | shape | M15 | M23 | paired M15/M23 |\n|---|---|---:|---:|---:|\n'
for x in json.loads((r/'reference-control-summary.json').read_text()):s+=f"| {x['workload']} | {x['scenario']} | {x['m15_median']:.3f} | {x['current_median']:.3f} | {x['paired_speedup']:.3f}x |\n"
s+='\nAll840records/60checksum groups agree. This older baseline preserves earlier regressions rather than resetting them away.\n\n## Process repeatability\n\n'
s+='| workload | baseline range | M23 range |\n|---|---:|---:|\n'
for x in dense:s+=f"| {x['workload']} | {min(x['before']):.3f}–{max(x['before']):.3f} | {min(x['after']):.3f}–{max(x['after']):.3f} |\n"
s+='\nRanges are3process medians, not confidence intervals. CPU2 pinning does not isolate SMT/boost/thermal/other host effects. No compilation overlaps timing. No application QPS or all-function performance claim is made.\n\n'
s+=(r/'decision.md').read_text()
s+='\n## Validation/reproduction\n\nBatch29, fullTiDB1381/same4 and fullTiKV1029/same6 names verified; PB result flags, golden extrema/shift boundaries/NULL and2050duplicate/reverse windows pass. Scoped fmt, make lint and diff checks pass. Datatype516/565 are explicitly reused M22 results (no datatype code changes). No fresh server, PD/TiKV, FIPS or workspace clippy pass claimed. Coverage inventory56admitted/131static signature names is metadata, not an SQL coverage percentage.\n\nvalidation.md pins exact source/binary/probe hashes and commands. Build current as after-bin, original plus the archived probe as frozen-bin and actual M21 as before-bin. collect.py/reference-control.py generate samples; summarize.py/summarize-reference.py assert data and generate JSON; make-report.py regenerates this receipt using decision.md. Binaries remain outside git.\n'
(r/'summary.md').write_text(s)
