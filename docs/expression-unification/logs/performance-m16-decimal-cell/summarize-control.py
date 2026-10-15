import json,statistics
from pathlib import Path
from collections import defaultdict
root=Path(__file__).resolve().parent
x=defaultdict(list);checks=defaultdict(set);count=0
for pair in range(1,4):
 for mode in ['before','after']:
  for line in (root/f'int-control-{pair}-{mode}.log').read_text().splitlines():
   if not line.startswith('EXPR_BATCH_PERF '):continue
   d=dict(t.split('=',1) for t in line.split()[1:]);count+=1
   assert d['live_fallback']=='0' and d['live_refusals']=='0' and d['route']=='tikv_batch'
   x[(d['scenario'],pair,mode)].append(float(d['ns_per_row']));checks[(d['scenario'],pair)].add(d['checksum'])
assert count==210 and len(checks)==15 and all(len(a)==1 for a in checks.values())
rows=[]
for shape in sorted({k[0] for k in x}):
 b=[statistics.median(x[(shape,p,'before')]) for p in range(1,4)];a=[statistics.median(x[(shape,p,'after')]) for p in range(1,4)]
 rows.append(dict(scenario=shape,before=b,after=a,before_median=statistics.median(b),after_median=statistics.median(a),paired_speedup=statistics.median(bb/aa for bb,aa in zip(b,a))))
(root/'int-control-summary.json').write_text(json.dumps(rows,indent=2)+'\n')
for r in rows:print(r['scenario'],r['before_median'],r['after_median'],r['paired_speedup'])
