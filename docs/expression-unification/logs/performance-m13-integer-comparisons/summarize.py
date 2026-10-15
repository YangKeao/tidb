import collections,json,statistics
from pathlib import Path
root=Path(__file__).resolve().parent
records=collections.defaultdict(list);checks=collections.defaultdict(set);native=collections.defaultdict(list)
for pair in range(1,4):
 for mode in ['current_fallback_ablation','batch_tikv']:
  n=0
  for line in (root/f'pair-{pair}-{mode}.log').read_text().splitlines():
   if not line.startswith('EXPR_BATCH_PERF '):continue
   d=dict(x.split('=',1) for x in line.split()[1:]);n+=1
   assert d['mode']==mode and d['live_refusals']=='0' and d['live_fallback']=='0'
   if mode=='batch_tikv':assert d['route']=='tikv_batch' and int(d['live_prepared'])>0
   records[(d['name'],d['scenario'],pair,mode)].append(float(d['ns_per_row']))
   checks[(d['name'],d['scenario'],pair)].add(d['checksum'])
  assert n==245,(pair,mode,n)
 frozen=collections.defaultdict(list)
 for line in (root/f'frozen-native-{pair}.log').read_text().splitlines():
  if not line.startswith('ORIGINAL_NATIVE_PERF '):continue
  d=dict(x.split('=',1) for x in line.split()[1:])
  if not d['name'].startswith('integer_compare_'):continue
  frozen[d['name']].append(float(d['ns_per_row']))
  checks[(d['name'],'dense',pair)].add(d['checksum'])
 for name,values in frozen.items():native[name].append(statistics.median(values))
 assert len(frozen)==7 and all(len(v)==7 for v in frozen.values())
assert len(checks)==105 and all(len(v)==1 for v in checks.values())
rows=[]
for name,shape in sorted({(k[0],k[1]) for k in records}):
 f=[statistics.median(records[(name,shape,p,'current_fallback_ablation')]) for p in range(1,4)]
 b=[statistics.median(records[(name,shape,p,'batch_tikv')]) for p in range(1,4)]
 row=dict(workload=name,scenario=shape,current_fallback=f,batch=b,fallback_median=statistics.median(f),batch_median=statistics.median(b),paired_ablation_speedup=statistics.median([x/y for x,y in zip(f,b)]))
 if shape=='dense':row.update(frozen_native=native[name],native_median=statistics.median(native[name]))
 rows.append(row)
(root/'summary.json').write_text(json.dumps(rows,indent=2)+'\n')
for r in rows:
 if r['scenario']=='dense':print(r['workload'],round(r['fallback_median'],3),round(r['batch_median'],3),round(r['paired_ablation_speedup'],2),round(r['native_median'],3))
print('1470 current timing records / 105 checksum groups agree, including 21 native/current dense groups.')
