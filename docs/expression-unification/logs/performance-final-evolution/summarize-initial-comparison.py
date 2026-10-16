import collections,json,statistics
from pathlib import Path
r=Path(__file__).resolve().parent
rec=collections.defaultdict(list);checks=collections.defaultdict(set)
for p in range(1,4):
 for mode in ['initial','current']:
  count=0
  for line in (r/f'initial-compare-{p}-{mode}.log').read_text().splitlines():
   if not line.startswith('EXPR_BATCH_PERF '):continue
   d=dict(x.split('=',1) for x in line.split()[1:]);count+=1
   if d['scenario']!='dense':continue
   assert d['physical_rows']==d['selected_rows']=='1024'
   rec[d['name'],p,mode].append(float(d['ns_per_row']));checks[d['name'],p].add(d['checksum'])
  assert count==(175 if mode=='initial' else 91),(p,mode,count)
assert len(checks)==39 and all(len(v)==1 for v in checks.values()) and all(len(v)==7 for v in rec.values())
rows=[]
for n in sorted({x[0] for x in rec}):
 row=dict(workload=n)
 for m in ['initial','current']:
  row[m]=[statistics.median(rec[n,p,m]) for p in range(1,4)];row[m+'_median']=statistics.median(row[m])
 row['ratio']=statistics.median(a/b for a,b in zip(row['current'],row['initial']));rows.append(row)
(r/'initial-comparison-summary.json').write_text(json.dumps(rows,indent=2)+'\n')
for x in rows:print(x['workload'],round(x['initial_median'],3),round(x['current_median'],3),round(x['ratio'],3))
print('798total timings /546dense timings /39dense checksum groups match; same selected payloads, initial schema6/current12 columns explicitly disclosed.')
