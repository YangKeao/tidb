import collections,json,statistics
from pathlib import Path
root=Path(__file__).resolve().parent
records=collections.defaultdict(list);checks=collections.defaultdict(set);native=collections.defaultdict(list)
for cohort in range(1,4):
 for mode in ['m20','full','head']:
  n=0
  for line in (root/f'cohort-{cohort}-{mode}.log').read_text().splitlines():
   if not line.startswith('EXPR_BATCH_PERF '):continue
   d=dict(x.split('=',1) for x in line.split()[1:]);n+=1
   assert d['route']=='tikv_batch' and d['live_fallback']=='0' and d['live_refusals']=='0' and int(d['live_prepared'])>0
   records[(d['name'],d['scenario'],cohort,mode)].append(float(d['ns_per_row']));checks[(d['name'],d['scenario'],cohort)].add(d['checksum'])
  assert n==385
  assert all(len(v)==7 for k,v in records.items() if k[2]==cohort and k[3]==mode)
 samples=collections.defaultdict(list)
 for line in (root/f'frozen-native-{cohort}.log').read_text().splitlines():
  if not line.startswith('ORIGINAL_NATIVE_PERF '):continue
  d=dict(x.split('=',1) for x in line.split()[1:]);samples[d['name']].append(float(d['ns_per_row']));checks[(d['name'],'dense',cohort)].add(d['checksum'])
 assert len(samples)==11 and all(len(v)==7 for v in samples.values())
 for name,v in samples.items():native[name].append(statistics.median(v))
assert len(checks)==165 and all(len(v)==1 for v in checks.values())
rows=[]
for name,shape in sorted({k[:2] for k in records}):
 med={mode:[statistics.median(records[(name,shape,c,mode)]) for c in range(1,4)] for mode in ['m20','full','head']}
 r=dict(workload=name,scenario=shape,**med,**{mode+'_median':statistics.median(v) for mode,v in med.items()})
 for left,right in [('m20','full'),('m20','head'),('full','head')]:r[left+'_over_'+right]=statistics.median(x/y for x,y in zip(med[left],med[right]))
 if shape=='dense':r.update(frozen_native=native[name],native_median=statistics.median(native[name]))
 rows.append(r)
(root/'summary.json').write_text(json.dumps(rows,indent=2)+'\n')
for r in rows:
 if r['scenario']=='dense':print(r['workload'],round(r['m20_median'],3),round(r['full_median'],3),round(r['head_median'],3),round(r['m20_over_head'],3),round(r['full_over_head'],3))
print('3465 timing records / 165 checksum groups including 33 native/current dense groups agree; prepared only.')
