import collections,json,statistics
from pathlib import Path
r=Path(__file__).resolve().parent
cells=[tuple(x) for x in json.loads((r/'cells.json').read_text())]
routes=json.loads((r/'routes.json').read_text());records=collections.defaultdict(list);checks=collections.defaultdict(set)
for pair in range(1,4):
 for mode in ['current','native']:
  count=0;size=None
  for line in (r/f'pair-{pair}-{mode}.log').read_text().splitlines():
   if 'EXPR_BATCH_CONFIG ' in line:assert 'dense_sizes=1,8,64,256,1024' in line
   if line.startswith('ORIGINAL_NATIVE_BATCH_SIZE '):size=int(dict(x.split('=',1) for x in line.split()[1:])['rows'])
   if mode=='current' and line.startswith('EXPR_BATCH_PERF '):
    d=dict(x.split('=',1) for x in line.split()[1:]);size=int(d['physical_rows'])
    assert d['scenario']=='dense' and int(d['selected_rows'])==size and d['mode']=='batch_tikv'
    assert d['route']==routes[d['name']]
    if d['route']=='tikv_batch':assert d['live_fallback']==d['live_refusals']=='0' and int(d['live_prepared'])>0
    else:assert d['live_prepared']=='0' and int(d['live_fallback'])>0 and int(d['live_refusals'])>0
   elif mode=='native' and line.startswith('ORIGINAL_NATIVE_PERF '):d=dict(x.split('=',1) for x in line.split()[1:])
   else:continue
   assert (d['name'],size) in cells
   records[d['name'],size,pair,mode].append(float(d['ns_per_row']));checks[d['name'],size,pair].add(d['checksum']);count+=1
  assert count==812,(pair,mode,count)
assert len(records)==696 and all(len(v)==7 for v in records.values())
assert len(checks)==348 and all(len(v)==1 for v in checks.values())
batch=[]
for name,size in cells:
 row=dict(workload=name,batch_size=size,route=routes[name])
 for mode in ['current','native']:
  row[mode]=[statistics.median(records[name,size,p,mode]) for p in range(1,4)]
  row[mode+'_median']=statistics.median(row[mode])
 row['paired_current_over_native']=[a/b for a,b in zip(row['current'],row['native'])]
 row['ratio']=statistics.median(row['paired_current_over_native']);batch.append(row)
width=collections.defaultdict(list);labels=collections.defaultdict(set)
for pair in range(1,4):
 for binary in ['current','native']:
  count=0
  for line in (r/f'width-{pair}-{binary}.log').read_text().splitlines():
   if not line.startswith('EXPR_LANE_PERF '):continue
   d=dict(x.split('=',1) for x in line.split()[1:]);mode=d['mode'];assert mode in (['one_shot','lane_cache'] if binary=='current' else ['frozen_native'])
   width[d['tier'],d['name'],pair,mode].append(float(d['ns_per_eval']));labels[d['tier'],d['name'],pair].add(d['result']);count+=1
  assert count==(216 if binary=='current' else 108),(pair,binary,count)
assert len(labels)==36 and all(len(v)==1 for v in labels.values()) and all(len(v)==9 for v in width.values())
wr=[]
for tier,name in sorted({(k[0],k[1]) for k in width}):
 row=dict(tier=tier,workload=name)
 for mode in ['frozen_native','one_shot','lane_cache']:
  row[mode]=[statistics.median(width[tier,name,p,mode]) for p in range(1,4)];row[mode+'_median']=statistics.median(row[mode])
 for mode in ['one_shot','lane_cache']:row[mode+'_ratio']=statistics.median(a/b for a,b in zip(row[mode],row['frozen_native']))
 wr.append(row)
initial=[]
for pair in range(1,4):
 vals=[];count=0
 for line in (r/f'initial-{pair}.log').read_text().splitlines():
  if not line.startswith('EXPR_BATCH_PERF '):continue
  d=dict(x.split('=',1) for x in line.split()[1:]);count+=1
  assert d['name']=='integer_add' and d['route']=='tikv_numeric'
  if d['scenario']=='dense':
   assert int(d['physical_rows'])==1024;checks['integer_add',1024,pair].add(d['checksum']);vals.append(float(d['ns_per_row']))
 assert count==35 and len(vals)==7
 initial.append(statistics.median(vals))
assert all(len(v)==1 for v in checks.values())
cur=next(x for x in batch if x['workload']=='integer_add' and x['batch_size']==1024)
summary=dict(batch=batch,width=wr,initial_unified_plus=dict(process_medians=initial,median=statistics.median(initial),paired_initial_over_final=statistics.median(a/b for a,b in zip(initial,cur['current']))),counts=dict(batch_timings=4872,batch_checksum_groups=348,width_timings=972,width_result_groups=36,initial_timings=105))
(r/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
for x in batch:
 if x['batch_size']==1024:print(x['workload'],round(x['native_median'],3),round(x['current_median'],3),round(x['ratio'],3),x['route'])
print('4872 batch timings/348checksum groups +972width timings/36result groups +105actual-initial timings validated. Ratios current/native; lower is better.')
