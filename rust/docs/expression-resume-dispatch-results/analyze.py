"""Three-way paired SQL statistics; rejects missing or duplicate measurements."""
from pathlib import Path
import collections,csv,json,math,statistics
BASE=Path(__file__).resolve().parent
MODES=['native','current','retained_b','frozen']
def quantile(values,p):
 assert values and 0<=p<=1
 values=sorted(values);at=(len(values)-1)*p;lo=math.floor(at);hi=math.ceil(at)
 return values[lo]+(values[hi]-values[lo])*(at-lo)
def cell(row,concurrent=False):
 values=[v for worker in row['workers'] for v in worker['latency_ms']] if concurrent else row['latency_ms']
 n=len(values);assert n and row['wall_seconds']>0 and all(v>=0 for v in values)
 return {'queries':n,'p50_ms':quantile(values,.5),'p95_ms':quantile(values,.95),'qps':n/row['wall_seconds'],'cpu_ms_per_query':row['server_cpu_seconds']*1000/n}
def summarize(rows,rounds,concurrent=False):
 by=collections.defaultdict(dict)
 for row in rows:
  key=row['key'] if not concurrent else f"{row['case']}/{row['concurrency']}"
  slot=(row['mode'],row['repeat']);assert slot not in by[key],('duplicate',key,slot)
  by[key][slot]=cell(row,concurrent)
 result=[]
 for key,values in sorted(by.items()):
  assert set(values)=={(mode,repeat) for mode in MODES for repeat in range(rounds)},('incomplete',key)
  out={'key':key,'rounds':rounds}
  for mode in MODES:
   cells=[values[(mode,r)] for r in range(rounds)]
   out[mode]={metric:statistics.median(c[metric] for c in cells) for metric in cells[0]}
   out[mode]['total_queries']=sum(c['queries'] for c in cells)
   out[mode]['p50_round_range_ms']=[min(c['p50_ms'] for c in cells),max(c['p50_ms'] for c in cells)]
  for denominator in ['native','retained_b','frozen']:
   ratios={};ranges={}
   for metric in ['p50_ms','p95_ms','qps','cpu_ms_per_query']:
    cells=[values[('current',r)][metric]/values[(denominator,r)][metric] for r in range(rounds) if values[(denominator,r)][metric]>0]
    ratios[metric]=statistics.median(cells) if cells else None;ranges[metric]=[min(cells),max(cells)] if cells else None
   out['after_over_'+denominator]=ratios;out['after_over_'+denominator+'_range']=ranges
  ratios={};ranges={}
  for metric in ['p50_ms','p95_ms','qps','cpu_ms_per_query']:
   cells=[values[('retained_b',r)][metric]/values[('native',r)][metric] for r in range(rounds)]
   ratios[metric]=statistics.median(cells);ranges[metric]=[min(cells),max(cells)]
  out['retained_b_over_retained_a']=ratios;out['retained_b_over_retained_a_range']=ranges
  result.append(out)
 return result
def selfcheck():
 assert quantile([1,3],.5)==2 and quantile([3,1,2],.5)==2
 rows=[{'key':'x','mode':m,'repeat':r,'latency_ms':[scale,scale*3],'wall_seconds':scale,'server_cpu_seconds':scale} for m,scale in [('native',2),('current',1),('retained_b',2),('frozen',.5)] for r in range(4)]
 out=summarize(rows,4)[0];assert out['after_over_native']['p50_ms']==.5 and out['after_over_frozen']['p50_ms']==2
 for bad in [rows[:-1],rows+rows[:1]]:
  try:summarize(bad,4)
  except AssertionError:pass
  else:raise AssertionError('incomplete/duplicate accepted')
selfcheck()
def main():
 pre=json.loads((BASE/'preflight.json').read_text());assert not pre['failures']
 rows=[json.loads(l) for l in (BASE/'primary.jsonl').read_text().splitlines()]
 eligible={p['key'] for p in pre['pairs'] if p['equivalent']}
 assert len(rows)==len(eligible)*16 and {r['key'] for r in rows}==eligible
 binaries={r['name']:r['binary_sha256'] for r in json.loads((BASE/'build-results.json').read_text())}
 for row in rows:assert row['binary_sha256']==binaries[row['mode']]
 result={'mode_labels':pre['mode_labels'],'primary':summarize(rows,4),'counts':{'primary_cells':len(rows),'primary_queries':sum(len(r['latency_ms']) for r in rows)},'method':'four Williams balanced orders with four process labels; >=4sec AND30 fullfetches per cell, POSIX process CPU clock; identical retained A/B selfcontrol, original99a distinct; median paired ratios and observed ranges, not CI'}
 (BASE/'summary.json').write_text(json.dumps(result,indent=2)+'\n')
 flat=[]
 for row in result['primary']:
  item={'key':row['key']}
  for group in MODES+['after_over_native','after_over_retained_b','after_over_frozen','retained_b_over_retained_a']:
   item.update({group+'_'+k:v for k,v in row[group].items() if not isinstance(v,list)})
  flat.append(item)
 with (BASE/'primary-summary.csv').open('w',newline='') as f:
  writer=csv.DictWriter(f,fieldnames=list(flat[0]));writer.writeheader();writer.writerows(flat)
 print(result['counts'])
if __name__=='__main__':main()
