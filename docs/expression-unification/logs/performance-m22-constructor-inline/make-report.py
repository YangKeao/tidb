import json
from pathlib import Path
r=Path(__file__).resolve().parent
rows=json.loads((r/'summary.json').read_text())
s="""# M22: Decimal constructor inline experiment (2026-10-16)

The candidate adds only inline(always) to the existing Decimal::try_new.
It preserves all constructor checks, zero initialization and allocation/error
behavior. The M17 profile had about9% self in this function; M21 still contains
a344-byte standalone symbol. This is distinct from rejected M18 check removal.
The candidate patch is experimental; see the final decision below.

Three alternating before/after actual binaries, seven75ms samples,1024 rows,
five selection/NULL shapes and eleven workloads. Latencies are medians of three
process medians, speedups medians of within-pair ratios. Both use actual batch_tikv;
frozen native is a separately built original production pair, not a fallback.
No compilation overlaps timing. CPU2 pinning does not isolate SMT/boost/thermal
or host effects. These are not server QPS or confidence intervals.

| workload | M21 | inline | paired M21/inline | native |
|---|---:|---:|---:|---:|
"""
for x in rows:
 if x['scenario']=='dense':s+=f"| {x['workload']} | {x['before_median']:.3f} | {x['after_median']:.3f} | {x['paired_speedup']:.3f}x | {x['native_median']:.3f} |\n"
s+="\nAll2310 current timings/165 checksum groups, including33 fresh native/current groups, agree. All routes prepared-only with zero fallback/refusal. Full per-shape samples/process medians remain in raw logs and summary.json. Slower controls are retained.\n\n## Independent M15 controls\n\n"
refs=json.loads((r/'reference-control-summary.json').read_text())
s+='| workload | shape | M15 | inline | paired M15/inline |\n|---|---|---:|---:|---:|\n'
for x in refs:s+=f"| {x['workload']} | {x['scenario']} | {x['m15_median']:.3f} | {x['current_median']:.3f} | {x['paired_speedup']:.3f}x |\n"
s+='\nAll840 records/60 checksum groups match.\n\n## Process repeatability\n\n'
s+='| workload | M21 range | inline range |\n|---|---:|---:|\n'
for x in rows:
 if x['scenario']=='dense':s+=f"| {x['workload']} | {min(x['before']):.3f}–{max(x['before']):.3f} | {min(x['after']):.3f}–{max(x['after']):.3f} |\n"
s+='\n'+(r/'decision.md').read_text()
s+='\n## Validation and reproduction\n\nSee validation.md for exact source pairs, hashes, build/test commands, frozen probe and baseline failure names. collect.py/reference-control.py generate raw logs; summarize.py/summarize-reference.py assert all expected counts/checksums and produce summary JSON; make-report.py regenerates this receipt. Binaries stay outside git.\n'
r.joinpath('summary.md').write_text(s)
