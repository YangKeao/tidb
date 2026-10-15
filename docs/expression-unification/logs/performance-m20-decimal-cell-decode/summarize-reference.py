import json,statistics
from pathlib import Path
from collections import defaultdict
root=Path(__file__).resolve().parent
x=defaultdict(list);checks=defaultdict(set);count=0
for pair in range(1,4):
 for mode in ['int-reference','after']:
  for line in (root/f'reference-control-{pair}-{mode}.log').read_text().splitlines():
   if not line.startswith('EXPR_BATCH_PERF '):continue
   d=dict(t.split('=',1) for t in line.split()[1:]);count+=1
   assert d['live_fallback']=='0' and d['live_refusals']=='0' and d['route']=='tikv_batch'
   x[(d['name'],d['scenario'],pair,mode)].append(float(d['ns_per_row']));checks[(d['name'],d['scenario'],pair)].add(d['checksum'])
assert count==840 and len(checks)==60 and all(len(v)==1 for v in checks.values())
rows=[]
for name,shape in sorted({k[:2] for k in x}):
 b=[statistics.median(x[(name,shape,p,'int-reference')]) for p in range(1,4)];a=[statistics.median(x[(name,shape,p,'after')]) for p in range(1,4)]
 rows.append(dict(workload=name,scenario=shape,m15_reference=b,current_after=a,m15_median=statistics.median(b),current_median=statistics.median(a),paired_speedup=statistics.median(bb/aa for bb,aa in zip(b,a))))
(root/'reference-control-summary.json').write_text(json.dumps(rows,indent=2)+'\n')
for r in rows:print(r['workload'],r['scenario'],r['m15_median'],r['current_median'],r['paired_speedup'])
