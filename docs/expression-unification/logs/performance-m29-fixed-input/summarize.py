import collections,json,statistics
from pathlib import Path
r=Path(__file__).resolve().parent
modes=['baseline','direct','fixed'];records=collections.defaultdict(list);checks=collections.defaultdict(set);native=collections.defaultdict(list)
for pair in range(1,4):
 for mode in modes:
  count=0
  for l in (r/f'cohort-{pair}-{mode}.log').read_text().splitlines():
   if not l.startswith('EXPR_BATCH_PERF '):continue
   d=dict(x.split('=',1) for x in l.split()[1:]);count+=1
   assert d['mode']=='batch_tikv' and d['route']=='tikv_batch' and d['live_fallback']==d['live_refusals']=='0'
   records[d['name'],d['scenario'],pair,mode].append(float(d['ns_per_row']));checks[d['name'],d['scenario'],pair].add(d['checksum'])
  assert count==490,(pair,mode,count)
 vals=collections.defaultdict(list)
 for l in (r/f'frozen-native-{pair}.log').read_text().splitlines():
  if not l.startswith('ORIGINAL_NATIVE_PERF '):continue
  d=dict(x.split('=',1) for x in l.split()[1:]);vals[d['name']].append(float(d['ns_per_row']));checks[d['name'],'dense',pair].add(d['checksum'])
 assert len(vals)==14 and all(len(x)==7 for x in vals.values())
 for name,v in vals.items():native[name].append(statistics.median(v))
assert len(checks)==210 and all(len(x)==1 for x in checks.values()) and all(len(x)==7 for x in records.values())
rows=[]
for name,scenario in sorted({(x[0],x[1]) for x in records}):
 row=dict(workload=name,scenario=scenario)
 for m in modes:
  row[m]=[statistics.median(records[name,scenario,p,m]) for p in range(1,4)];row[m+'_median']=statistics.median(row[m])
 for left,right in [('baseline','direct'),('baseline','fixed'),('direct','fixed')]:row[left+'_over_'+right]=statistics.median([a/b for a,b in zip(row[left],row[right])])
 if scenario=='dense':row['native']=native[name];row['native_median']=statistics.median(native[name]);print(name,*[round(row[m+'_median'],3) for m in modes],round(row['baseline_over_fixed'],3))
 rows.append(row)
(r/'summary.json').write_text(json.dumps(rows,indent=2)+'\n')
print('4410 timings/210 checksum groups incl42 actual-native dense groups agree; all3 real-code modes prepared only.')
