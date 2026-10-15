import json
from pathlib import Path
r=Path(__file__).resolve().parent
rows=json.loads((r/'summary.json').read_text())
dense=[x for x in rows if x['scenario']=='dense']
new=[x for x in dense if x['workload'] in ['integer_not','real_asin','real_acos']]
controls=[x for x in dense if not x['workload'] in ['integer_not','real_asin','real_acos']]
s='''# M25: matching unary kernels on the shared batch driver (2026-10-16)

UnaryNotInt, ASIN and ACOS reuse the existing eager driver. NOT preserves
UInt bits and returns signed0/1/NULL. ASIN/ACOS use the same asin_f64/acos_f64
primitive as native raw workers; NaN output becomes NULL and signed zero is
preserved. NaN input remains excluded by the existing Real carrier. This does
not admit SIN/COS/ATAN's native Go/libm mismatch or ROUND's distinct policies.

ASIN/ACOS benchmark data repeats8in-domain values and2outside values per10:
[-1,-.9999,-.5,-0,+0,.5,.9999,1,-1.5,1.5]. Both candidate/fallback and actual
native set the test-only EXPR_PERF_REAL_DOMAIN=inverse_trig; input setup is
outside timing. All old controls retain their original data and schema, with
that flag unset. The flag is benchmark data selection, not a production route
or value admission gate. Raw probe headers record the chosen input domain.
No per-function driver or duplicate mathematical algorithm is added.

The three new workloads compare actual current batch to a clearly labeled
same-current-binary fallback ablation, and separately to true frozen original
native production. That fallback is NOT original native or historical TiKV.
Eleven existing control workloads instead compare distinct actual M24 and M25
batch binaries. No forced singleton route is used as a historical baseline.

Three alternating pairs;7samples75ms,1024selected rows and5selection/NULL shapes.
Latency is median of3process medians; ratios are medians of within-pair ratios.
Actual native dense runs after each pair on identical defaultLongLong inputs.
All2940current timing records/210workload-shape-pair checksum groups match,
including42fresh actual-native/current dense groups. All optimized/control
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

These baseline cells are actual M24 batch code, unlike the new-family ablation
above. Flat and slower results remain visible.

| workload | M24 | M25 | paired M24/M25 | true native |
|---|---:|---:|---:|---:|
'''
for x in controls:s+=f"| {x['workload']} | {x['before_median']:.3f} | {x['after_median']:.3f} | {x['paired_speedup']:.3f}x | {x['native_median']:.3f} |\n"
s+='\n## Independent M15 controls\n\n'
s+='| workload | shape | M15 | M25 | paired M15/M25 |\n|---|---|---:|---:|---:|\n'
for x in json.loads((r/'reference-control-summary.json').read_text()):s+=f"| {x['workload']} | {x['scenario']} | {x['m15_median']:.3f} | {x['current_median']:.3f} | {x['paired_speedup']:.3f}x |\n"
s+='\nAll840records/60checksum groups agree. This older baseline preserves earlier regressions rather than resetting them away.\n\n## Process repeatability\n\n'
s+='| workload | baseline range | M25 range |\n|---|---:|---:|\n'
for x in dense:s+=f"| {x['workload']} | {min(x['before']):.3f}–{max(x['before']):.3f} | {min(x['after']):.3f}–{max(x['after']):.3f} |\n"
s+='\nRanges are3process medians, not confidence intervals. CPU2 pinning does not isolate SMT/boost/thermal/other host effects. No compilation overlaps timing. No application QPS or all-function performance claim is made.\n\n'
s+=(r/'decision.md').read_text()
s+='\n## Validation/reproduction\n\nBatch32/fullTiDB1384same4 and freshTiKV1030same6 names verified; profile54pass. Golden inverse-trig bits coverInf, domain edges,±0/endpoints/interior/NULL and2050reverse/duplicate rows; NOT covers UIntMAX/MIN/zero/nonzero/NULL. Actual SQL smoke3prepared. Scoped fmt, make lint and diff checks pass. Datatype516/565 are reused unchanged-core results. No fresh server/PD-TiKV/FIPS/workspace clippy pass. Inventory66/131static signatures is metadata, not SQL coverage percent.\n\nvalidation.md pins exact source/binary/probe hashes, domain selection and commands. Build current as after-bin, original plus the archived test probe as frozen-bin and actual M24 as before-bin. collect.py/reference-control.py generate raw logs; summarize.py/summarize-reference.py assert counts/checksums and produce JSON; make-report.py regenerates this receipt from those plus decision.md. Binaries stay outside git.\n'
(r/'summary.md').write_text(s)
