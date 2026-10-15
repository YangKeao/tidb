import collections,json,statistics
from pathlib import Path
r=Path(__file__).resolve().parent
v=collections.defaultdict(list);checks=collections.defaultdict(set)
for pair in range(1,4):
 for mode in ['before','after']:
  count=0
  for l in (r/f'regression-{pair}-{mode}.log').read_text().splitlines():
   if not l.startswith('EXPR_BATCH_PERF '):continue
   d=dict(x.split('=',1) for x in l.split()[1:]);count+=1
   assert d['mode']=='batch_tikv' and d['route']=='tikv_batch' and d['live_refusals']==d['live_fallback']=='0'
   v[d['name'],d['scenario'],pair,mode].append(float(d['ns_per_row']))
   checks[d['name'],d['scenario'],pair].add(d['checksum'])
  assert count==105,(pair,mode,count)
assert len(checks)==45 and all(len(x)==1 for x in checks.values()) and all(len(x)==7 for x in v.values())
rows=[]
for name,scenario in sorted({(k[0],k[1]) for k in v}):
 before=[statistics.median(v[name,scenario,p,'before']) for p in range(1,4)]
 after=[statistics.median(v[name,scenario,p,'after']) for p in range(1,4)]
 row=dict(workload=name,scenario=scenario,before=before,after=after,before_median=statistics.median(before),after_median=statistics.median(after),paired_speedup=statistics.median([a/b for a,b in zip(before,after)]));rows.append(row)
 print(name,scenario,row['before_median'],row['after_median'],row['paired_speedup'])
(r/'regression-summary.json').write_text(json.dumps(rows,indent=2)+'\n')
print('630 timings / 45 checksum groups agree; actual M25/M26 code, longer150ms samples, reverse initial order.')
