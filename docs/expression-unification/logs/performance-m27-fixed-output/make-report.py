import json
from pathlib import Path
r=Path(__file__).resolve().parent
rows=json.loads((r/'summary.json').read_text())
s="""# M27: shared fixed-output buffer experiment (2026-10-16)

The owned fixed-cell append loop sizes its byte buffer once and fills chunks.
It retains the last scratch element for NULL bytes, true iterator termination,
actual row/byte counts, zero-width cells, and the unchanged shared/frozen scalar
copy-on-write behavior. A fused cloned iterator prevents restart after None;
short/long positive size hints remain safe. No unsafe code or new execution mode.
Value normalization, structural/resource checks and output prevalidation remain.

Distinct actual M26/current batch binaries compare all14workloads,3alternating
pairs,7samples75ms/5shapes/1024rows. Both modes use real batch_tikv; no same-binary
fallback ablation. New UInt schema choices are identical current/before/native,
all old controls use their original signed schema. Actual original-native dense
runs follow each pair; source is pinned in validation.md. All2940timings/
210checksum groups, including42native/current dense groups, match. All routes
prepared-only,0fallback/refusal. Latencies are medians of3process medians and
ratios medians of paired ratios. Slow controls remain in all raw/summary rows.

| workload | M26 | candidate | paired M26/candidate | true native |
|---|---:|---:|---:|---:|
"""
for x in rows:
 if x['scenario']=='dense':s+=f"| {x['workload']} | {x['before_median']:.3f} | {x['after_median']:.3f} | {x['paired_speedup']:.3f}x | {x['native_median']:.3f} |\n"
s+='\n## Independent M15 controls\n\nAll840records/60checksum groups agree; earlier regressions remain visible.\n\n| workload | shape | M15 | candidate | paired M15/candidate |\n|---|---|---:|---:|---:|\n'
for x in json.loads((r/'reference-control-summary.json').read_text()):s+=f"| {x['workload']} | {x['scenario']} | {x['m15_median']:.3f} | {x['current_median']:.3f} | {x['paired_speedup']:.3f}x |\n"
s+='\n## Process repeatability\n\n| workload | M26 range | candidate range |\n|---|---:|---:|\n'
for x in rows:
 if x['scenario']=='dense':s+=f"| {x['workload']} | {min(x['before']):.3f}–{max(x['before']):.3f} | {min(x['after']):.3f}–{max(x['after']):.3f} |\n"
s+='\nRanges of3process medians are not confidence intervals. CPU2 pinning does not isolate host/SMT/boost effects; no compilation/tests overlap timing. No application QPS or universal performance claim.\n\n'+(r/'decision.md').read_text()
s+='\n## Validation/reproduction\n\nChunk273pass includes independent scalar byte/storage oracle for0/4/8/16/40widths, NULL scratch after reset, bad positive size hints and first-None termination; existing alias/frozen/NULL cases pass. Batch33/fullTiDB1385same4/lintfmt pass. TiKV1031same6 and datatype516/565 reused explicitly (unchanged code). No fresh server/PD-TiKV/FIPS/workspace clippy pass. Coverage67/131 unchanged, metadata only.\n\nvalidation.md preserves exact source/binary/probe hashes, build/test commands and schema selection. Run collect.py/reference-control.py/summarize.py/summarize-reference.py/make-report.py with before/after/frozen/int-reference binaries. Binaries remain outside git; patch and all raw data/scripts remain in the receipt.\n'
(r/'summary.md').write_text(s)
