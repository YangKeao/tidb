import json
from pathlib import Path
r=Path(__file__).resolve().parent
rows=json.loads((r/'summary.json').read_text())
dense=[x for x in rows if x['scenario']=='dense']
new=[x for x in dense if x['workload'].startswith('isnull_')]
controls=[x for x in dense if not x['workload'].startswith('isnull_')]
s='''# M24: nullable predicates on the shared batch driver (2026-10-16)

Seven IS NULL signatures use the existing eager driver and generated nullable
kernels. NULL inputs produce signed Int1, non-NULL values (including zero/empty)
produce signed Int0. Int permits unsigned bits; other operand/result flag rules
stay unchanged. Datetime/Timestamp share TimeIsNull. No per-function driver,
nullness-only shortcut or execution mode is added. Child phases still execute
before the existing kernel; NULL itself is not propagated as the result.

JSON is explicitly excluded from native SQL batch IS NULL: the existing Go
catalog inserts CastJsonAsString then StringIsNull. Do not replace it with
JsonIsNull or bypass that cast. JSON golden cases verify the existing native
fallback, not prepared coverage. Cast support is a separate shared-contract
extension. No new family-specific migration gate is introduced.

The eight native-type workloads compare actual current batch to a clearly labeled
same-current-binary fallback ablation, and separately to true frozen original
native production. That fallback is NOT original native or historical TiKV.
Eleven existing control workloads instead compare distinct actual M23 and M24
batch binaries. No forced singleton route is used as a historical baseline.

Three alternating pairs;7samples75ms,1024selected rows and5selection/NULL shapes.
Latency is median of3process medians; ratios are medians of within-pair ratios.
Actual native dense runs after each pair on identical defaultLongLong inputs.
All3990current timing records/285workload-shape-pair checksum groups match,
including57fresh actual-native/current dense groups. All optimized/control
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

These baseline cells are actual M23 batch code, unlike the new-family ablation
above. Flat and slower results remain visible.

| workload | M23 | M24 | paired M23/M24 | true native |
|---|---:|---:|---:|---:|
'''
for x in controls:s+=f"| {x['workload']} | {x['before_median']:.3f} | {x['after_median']:.3f} | {x['paired_speedup']:.3f}x | {x['native_median']:.3f} |\n"
s+='\n## Independent M15 controls\n\n'
s+='| workload | shape | M15 | M24 | paired M15/M24 |\n|---|---|---:|---:|---:|\n'
for x in json.loads((r/'reference-control-summary.json').read_text()):s+=f"| {x['workload']} | {x['scenario']} | {x['m15_median']:.3f} | {x['current_median']:.3f} | {x['paired_speedup']:.3f}x |\n"
s+='\nAll840records/60checksum groups agree. This older baseline preserves earlier regressions rather than resetting them away.\n\n## Process repeatability\n\n'
s+='| workload | baseline range | M24 range |\n|---|---:|---:|\n'
for x in dense:s+=f"| {x['workload']} | {min(x['before']):.3f}–{max(x['before']):.3f} | {min(x['after']):.3f}–{max(x['after']):.3f} |\n"
s+='\nRanges are3process medians, not confidence intervals. CPU2 pinning does not isolate SMT/boost/thermal/other host effects. No compilation overlaps timing. No application QPS or all-function performance claim is made.\n\n'
s+=(r/'decision.md').read_text()
s+='\n## Validation/reproduction\n\nBatch30/fullTiDB1382same4 and freshTiKV1030same6 names verified. Golden NULL/non-NULL, UInt/zero/empty and NULL literals cover2050reverse/duplicate selected rows; JSON remains native fallback. Existing catalog PB tests pin Time/Timestamp and JSON cast signatures. Scoped fmt, make lint and diff checks pass. Datatype516/565 are reused from the unchanged adopted datatype code; no fresh server/PD-TiKV/FIPS/workspace clippy claim. Inventory63admitted/131static signature names is metadata, not SQL coverage percentage.\n\nvalidation.md pins source/binary/probe hashes and commands. Build current as after-bin, original plus the archived test probe as frozen-bin and actual M23 as before-bin. collect.py/reference-control.py generate logs; summarize.py/summarize-reference.py assert counts/checksums and generate JSON; make-report.py regenerates this receipt from those plus decision.md. Binaries stay outside git.\n'
(r/'summary.md').write_text(s)
