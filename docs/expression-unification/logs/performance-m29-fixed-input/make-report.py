import json
from pathlib import Path
r=Path(__file__).resolve().parent
rows=json.loads((r/'summary.json').read_text())
s="""# M29: common fixed-input importer, three actual implementations (2026-10-16)

M27 is the adopted baseline. Direct temporal raw input (A) preserves the original
Time normalizer and Duration checks, but initial sampling regressed Decimal
comparisons. Common fixed input (B) moves Int/Real/Time/Duration owning import to
one private outlined generic loop with existing per-carrier decoders. NULL does
not decode backing payload. This is a storage change on the same driver.
No constructor check, normalization, SQL policy or runtime selector is added.
Only one implementation may remain after the decision below.

Three actual release binaries run ABC/BCA/CAB over three cohorts, fourteen
workloads, five selection/NULL shapes, seven samples of75ms and1024rows.
Every route is prepared-only with zero fallback/refusal. All4410current samples
and210checksum groups agree, including42fresh original-native dense groups.
Unsigned schema flags are identical on all four binaries for the two unsigned
identity workloads only. Old controls retain original schema/data. All slower
rows remain in JSON/raw logs. Values are ns/row, median of process medians;
ratios are medians of within-cohort ratios.

| workload | M27 | direct A | common B | M27/B | A/B | actual native |
|---|---:|---:|---:|---:|---:|---:|
"""
for x in rows:
 if x['scenario']=='dense':s+=f"| {x['workload']} | {x['baseline_median']:.3f} | {x['direct_median']:.3f} | {x['fixed_median']:.3f} | {x['baseline_over_fixed']:.3f}x | {x['direct_over_fixed']:.3f}x | {x['native_median']:.3f} |\n"
s+='\n## Historical M15 controls\n\nAll840records/60checksum groups agree.\n\n| workload | shape | M15 | common B | M15/B |\n|---|---|---:|---:|---:|\n'
for x in json.loads((r/'reference-control-summary.json').read_text()):s+=f"| {x['workload']} | {x['scenario']} | {x['m15_median']:.3f} | {x['current_median']:.3f} | {x['paired_speedup']:.3f}x |\n"
s+='\n## Process repeatability\n\n| workload | M27 range | direct A range | common B range |\n|---|---:|---:|---:|\n'
for x in rows:
 if x['scenario']=='dense':s+='| '+x['workload']+' | '+' | '.join(f"{min(x[m]):.3f}–{max(x[m]):.3f}" for m in ['baseline','direct','fixed'])+' |\n'
s+='\nRanges are not confidence intervals. CPU2 pinning is not CPU isolation; no compilation/tests overlap timing. No application QPS claim.\n\n'+(r/'decision.md').read_text()
s+='\n## Validation and reproduction\n\nBatch34pass/fullTiDB1386pass with the same4baseline failures; make lint, scoped rustfmt and git diff --check pass. Unchanged core reuses datatype516/565, TiKV1031/same6 and Chunk273 explicitly. No fresh server/PD-TiKV/FIPS/workspace-clippy claim. Coverage67/131 metadata unchanged.\n\nSee validation.md for exact commands/source/owner/layout preconditions and binary-hashes.txt for all versions. Candidate patches and original samples are retained; binaries remain outside git. Run collect.py, reference-control.py, summarize.py, summarize-reference.py and make-report.py with pinned binaries. The first direct-only experiment remains in ../performance-m29-temporal-input/.\n'
(r/'summary.md').write_text(s)
