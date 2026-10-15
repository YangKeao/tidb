import json,statistics
from pathlib import Path
r=Path(__file__).resolve().parent
rows=json.loads((r/'summary.json').read_text())
s='''# M30: pure zero-argument PI on the existing batch driver (2026-10-16)

The shared registry admits exactly Pi()->Real and TiDB removes its blanket
zero-argument rejection. Existing catalog lowering, kernel metadata and explicit
output row count remain in use. No new driver, constant cache or random-state
handling is added. Native pi_raw delegates to the same pi primitive; golden
values are exact bits of3.141592653589793. Stateful RAND stays excluded.

Actual SQL pi() is prepared. No-input2050virtual rows, reverse/duplicate and empty
selection, nested Real PLUS, wrong arity/type and resource refusals pass. Budget
checks apply even without source columns; zero work/retained bytes refuse.
Full TiDB1386/same4failures, TiKV1032/same6, batch34, lint/fmt/diff pass; unchanged
datatype516/565 and Chunk273 reused explicitly. No fresh server/PD-TiKV/FIPS or
workspace-clippy claim.68/131 is metadata signatures, not a SQL coverage rate.

Three alternating pairs, fifteen workloads, five shapes, seven75ms samples,
1024rows. New Pi BEFORE is same-current fallback ablation (not native). Old14
BEFORE workloads use actual adopted M27 release code; AFTER is actual M30.
Original-native production is independently frozen and only its test probe is
extended/rebuilt. All3150current records/225checksumgroups agree, including45
fresh original-native/current dense groups. Prepared routes have zero fallback
and refusal; the separately labeled fallback ablation has no batch-route calls.
Two UIntidentity schemas/data remain identical across every relevant binary;
old controls keep original signed LongLong and Real data. Units ns/row; medians
of3process medians, paired ratios are medians of within-pair ratios.

| workload | before kind | before | M30 | before/M30 | actual native | native/M30 |
|---|---|---:|---:|---:|---:|---:|
'''
for x in rows:
 if x['scenario']=='dense':
  ratio=statistics.median([n/a for n,a in zip(x['frozen_native'],x['after'])])
  kind='current fallback ablation' if x['workload']=='pure_pi' else 'actual M27 batch'
  s+=f"| {x['workload']} | {kind} | {x['before_median']:.3f} | {x['after_median']:.3f} | {x['paired_speedup']:.3f}x | {x['native_median']:.3f} | {ratio:.3f}x |\n"
s+='\n## M15 historical controls\n\nAll840records/60checksumgroups agree; every slower shape remains visible.\n\n| workload | shape | M15 | M30 | paired M15/M30 |\n|---|---|---:|---:|---:|\n'
for x in json.loads((r/'reference-control-summary.json').read_text()):s+=f"| {x['workload']} | {x['scenario']} | {x['m15_median']:.3f} | {x['current_median']:.3f} | {x['paired_speedup']:.3f}x |\n"
s+='\n## Process repeatability\n\n| workload | before range | M30 range | native range |\n|---|---:|---:|---:|\n'
for x in rows:
 if x['scenario']=='dense':s+='| '+x['workload']+' | '+' | '.join(f'{min(x[k]):.3f}–{max(x[k]):.3f}' for k in ['before','after','frozen_native'])+' |\n'
s+='\nRanges are not confidence intervals. Pinning CPU2 is not CPU isolation; no correctness/build jobs overlap formal timing. This is an in-memory warm evaluator microbenchmark, not server QPS.\n\n'+(r/'decision.md').read_text()
s+='\n## Reproduction\n\nvalidation.md pins source, probe/binary hashes, contracts, exact commands and limitations. Run collect.py, reference-control.py, summarize.py, summarize-reference.py and make-report.py using the pinned binaries. Raw logs, source patches and scripts are retained; binaries stay outside git. See remaining-work-boundary.md for the explicitly unimplemented signature domains and larger storage work.\n'
(r/'summary.md').write_text(s)
