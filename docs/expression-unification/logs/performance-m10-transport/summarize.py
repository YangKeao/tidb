import collections, json, statistics
from pathlib import Path
root = Path(__file__).resolve().parent
records = collections.defaultdict(list)
checks = collections.defaultdict(set)
for pair in range(1, 4):
 for mode in ['before','after']:
  count=0
  for line in (root/f'pair-{pair}-{mode}.log').read_text().splitlines():
   if not line.startswith('EXPR_BATCH_PERF '): continue
   d=dict(x.split('=',1) for x in line.split()[1:])
   assert d['route']=='tikv_batch' and d['live_fallback']=='0' and d['live_refusals']=='0'
   records[(d['name'],d['scenario'],pair,mode)].append(float(d['ns_per_row']))
   checks[(d['name'],d['scenario'],pair)].add(d['checksum'])
   count+=1
  assert count==315, (pair,mode,count)
assert len(checks)==135 and all(len(x)==1 for x in checks.values())
summary=[]
for name, scenario in sorted({(k[0],k[1]) for k in records}):
 before=[statistics.median(records[(name,scenario,p,'before')]) for p in range(1,4)]
 after=[statistics.median(records[(name,scenario,p,'after')]) for p in range(1,4)]
 summary.append(dict(workload=name,scenario=scenario,before=before,after=after,before_median=statistics.median(before),after_median=statistics.median(after),paired_speedup=statistics.median([b/a for b,a in zip(before,after)])))
(root/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
for row in summary:
 if row['scenario']=='dense':print(row['workload'],round(row['before_median'],3),round(row['after_median'],3),round(row['paired_speedup'],2))
print('1890 timing records / 135 checksum groups match; prepared only.')
