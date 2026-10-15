import json
from pathlib import Path
r=Path(__file__).resolve().parent
rows=json.loads((r/'summary.json').read_text())
s="""# M29: temporal input window access experiment (2026-10-16)

Time/Duration input borrows one raw-cell view per column window, like the
existing Int/Real transport. Time still invokes the original Time::from_go_raw
then go_raw before the shared carrier, preserving packed metadata normalization:
TimestampFSP7 maps to6. It is not blindly retained or rejected. No new calendar
validation is invented. Duration still uses the same i8FSP conversion and
from_nanos checks inside the non-NULL branch; NULL does not decode its payload.
Only repeated byte access/temporary native value wrappers are removed. Existing
native scalar getters remain for their other consumers; no new decoder, driver,
semantic mode or unchecked constructor replaces them.

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
s+='\n## Validation/reproduction\n\nNew2050reverse/duplicate-row test verifies all16metadata nibbles against the existing native normalizer, includingFSP7→6 and NULL payload skipping for DateTime/Timestamp. Batch34/fullTiDB1386same4/lintfmt pass. Existing unchanged-core datatype516/565,TiKV1031same6 andChunk273 reused explicitly. No fresh server/PD-TiKV/FIPS/workspace clippy claim. Coverage67/131 unchanged, metadata only.\n\nvalidation.md preserves source/binary/probe hashes, exact commands and owner/layout preconditions. Run collect.py/reference-control.py/summarize.py/summarize-reference.py/make-report.py with before/after/frozen/int-reference binaries. All patches/raw samples/scripts stay in the receipt; binaries remain outside git.\n'
(r/'summary.md').write_text(s)
