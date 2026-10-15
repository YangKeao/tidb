import collections,json,statistics
from pathlib import Path
root=Path(__file__).resolve().parent
records=collections.defaultdict(list);checks=collections.defaultdict(set);native=collections.defaultdict(list)
for pair in range(1,4):
 for kind in ['tiny','short','int24','long']:
  for name in ['integer_add','integer_compare_lt']:
   for mode in ['current_fallback_ablation','batch_tikv']:
    n=0
    for line in (root/f'pair-{pair}-{kind}-{name}-{mode}.log').read_text().splitlines():
     if not line.startswith('EXPR_BATCH_PERF '):continue
     d=dict(x.split('=',1) for x in line.split()[1:]);n+=1
     assert d['name']==name and d['mode']==mode and d['live_fallback']=='0' and d['live_refusals']=='0'
     if mode=='batch_tikv':assert d['route']=='tikv_batch' and int(d['live_prepared'])>0
     records[(kind,name,d['scenario'],pair,mode)].append(float(d['ns_per_row']))
     checks[(kind,name,d['scenario'],pair)].add(d['checksum'])
    assert n==35
   values=[]
   for line in (root/f'frozen-{pair}-{kind}-{name}.log').read_text().splitlines():
    if not line.startswith('ORIGINAL_NATIVE_PERF '):continue
    d=dict(x.split('=',1) for x in line.split()[1:]);assert d['name']==name
    values.append(float(d['ns_per_row']));checks[(kind,name,'dense',pair)].add(d['checksum'])
   assert len(values)==7
   native[(kind,name)].append(statistics.median(values))
assert len(checks)==120 and all(len(v)==1 for v in checks.values())
rows=[]
for kind,name,shape in sorted({k[:3] for k in records}):
 f=[statistics.median(records[(kind,name,shape,p,'current_fallback_ablation')]) for p in range(1,4)]
 b=[statistics.median(records[(kind,name,shape,p,'batch_tikv')]) for p in range(1,4)]
 row=dict(integer_type=kind,workload=name,scenario=shape,current_fallback=f,batch=b,fallback_median=statistics.median(f),batch_median=statistics.median(b),paired_ablation_speedup=statistics.median([x/y for x,y in zip(f,b)]))
 if shape=='dense':row.update(frozen_native=native[(kind,name)],native_median=statistics.median(native[(kind,name)]))
 rows.append(row)
(root/'summary.json').write_text(json.dumps(rows,indent=2)+'\n')
for r in rows:
 if r['scenario']=='dense':print(r['integer_type'],r['workload'],round(r['fallback_median'],3),round(r['batch_median'],3),round(r['native_median'],3))
print('1680 current timing records; 120 checksum groups including 24 native/current dense groups agree.')
