import json
from pathlib import Path
r=Path(__file__).resolve().parent
rows=json.loads((r/'summary.json').read_text())
s="""# M28: Duration constructor inline experiment (2026-10-16)

The candidate adds only inline(always) to Duration::from_nanos. check_fsp and
checked_round remain exactly unchanged, including errors, range/precision and
normalization behavior. The recent independent Duration profile measured15–17%
self in this constructor. Its baseline579byte standalone symbol disappears,
but that alone does not establish a performance gain. No new mirror test for a
code-generation hint; complete existing suites cover the unchanged contract.

Distinct actual M27/candidate batch binaries compare all14workloads,3alternating
pairs,7samples75ms/5shapes/1024rows. Both modes use real batch_tikv; no same-binary
fallback ablation. New UInt schema choices are identical current/before/native,
all old controls use their original signed schema. Actual original-native dense
runs follow each pair; source is pinned in validation.md. All2940timings/
210checksum groups, including42native/current dense groups, match. All routes
prepared-only,0fallback/refusal. Latencies are medians of3process medians and
ratios medians of paired ratios. Slow controls remain in all raw/summary rows.

| workload | M27 | candidate | paired M27/candidate | true native |
|---|---:|---:|---:|---:|
"""
for x in rows:
 if x['scenario']=='dense':s+=f"| {x['workload']} | {x['before_median']:.3f} | {x['after_median']:.3f} | {x['paired_speedup']:.3f}x | {x['native_median']:.3f} |\n"
s+='\n## Independent M15 controls\n\nAll840records/60checksum groups agree; earlier regressions remain visible.\n\n| workload | shape | M15 | candidate | paired M15/candidate |\n|---|---|---:|---:|---:|\n'
for x in json.loads((r/'reference-control-summary.json').read_text()):s+=f"| {x['workload']} | {x['scenario']} | {x['m15_median']:.3f} | {x['current_median']:.3f} | {x['paired_speedup']:.3f}x |\n"
s+='\n## Process repeatability\n\n| workload | M27 range | candidate range |\n|---|---:|---:|\n'
for x in rows:
 if x['scenario']=='dense':s+=f"| {x['workload']} | {min(x['before']):.3f}–{max(x['before']):.3f} | {min(x['after']):.3f}–{max(x['after']):.3f} |\n"
s+='\nRanges of3process medians are not confidence intervals. CPU2 pinning does not isolate host/SMT/boost effects; no compilation/tests overlap timing. No application QPS or universal performance claim.\n\n'+(r/'decision.md').read_text()
s+='\n## Validation/reproduction\n\nFresh TiDBdatatype516/TiKVdatatype565,batch33/fullTiDB1385same4/fullTiKV1031same6 names verified. Scope fmt, make lint and diff checks pass. M27Chunk273 is reused explicitly, no Chunk source changes. No fresh server/PD-TiKV/FIPS/workspace clippy claim. Coverage67/131 unchanged, metadata only.\n\nvalidation.md pins source/binary/probe hashes and commands. Run collect.py/reference-control.py/summarize.py/summarize-reference.py/make-report.py with before/after/frozen/int-reference bins. All raw logs/scripts and the exact one-line patch remain. Binaries stay outside git.\n'
(r/'summary.md').write_text(s)
