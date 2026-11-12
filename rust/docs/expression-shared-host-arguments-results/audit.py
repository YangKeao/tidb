"""Reconcile every timed cell, scope, digest and actual binary, independent of reporting."""
from pathlib import Path
import json,collections,math
F=Path(__file__).resolve().parent
def main():
 load=lambda n:json.loads((F/n).read_text())
 pre=load('preflight.json');assert not pre['failures'];specs={v['key']:v['case_spec'] for v in pre['pairs']};assert len(specs)==14
 rows=[json.loads(l) for l in (F/'primary.jsonl').read_text().splitlines()];assert len(rows)==224
 counts=collections.Counter((r['key'],r['mode'],r['repeat']) for r in rows)
 assert set(counts)=={(k,m,i) for k in specs for m in ['native','current','retained_b','frozen'] for i in range(4)} and set(counts.values())=={1}
 binaries={v['name']:v['binary_sha256'] for v in load('build-results.json')}
 for r in rows:
  assert r['binary_sha256']==binaries[r['mode']]
  assert r['server_cpu_clock']=='POSIX process CPU clock via clock_getcpuclockid' and 0<r['server_cpu_clock_resolution_ns']<=1000
  assert r['server_cpu_seconds']==r['server_cpu_nanoseconds']/1e9
  from workloads import CONFIGS
  size,vec=CONFIGS[r['config']];expected=[min(size,32),size,vec]
  assert [int(v) for v in r['session_configuration']]==expected and r['session_configuration']==r['session_configuration_after']
  assert r['sql']==specs[r['key']]['sql'] and r['rows']==specs[r['key']]['rows']
  assert len(r['latency_ms'])>=30 and r['wall_seconds']>=4
  assert all(math.isfinite(x) and x>=0 for x in r['latency_ms'])
  assert 0<=r['server_cpu_seconds']<=r['wall_seconds']*4.1
 summary=load('summary.json');assert summary['counts']['primary_queries']==sum(len(r['latency_ms']) for r in rows)
 from analyze import summarize
 assert summary['primary']==summarize(rows,4)
 assert binaries['frozen']=='99a4c3e566774ba9733bc9d54366cf1e9b85446f63fa9a7418bfef5d3daeddd2'
 assert binaries['native']==binaries['retained_b']
 # Replay each persisted full observation against the strict preflight reference.
 observations=[json.loads(l) for l in (F/'cell-observations.jsonl').read_text().splitlines()]
 assert len(observations)==448
 assert collections.Counter((o['key'],o['mode'],o['repeat'],o['phase']) for o in observations)==collections.Counter((k,m,r,phase) for k in specs for m in ['native','current','retained_b','frozen'] for r in range(4) for phase in ['before','after'])
 refs={(o['mode'],o['key']):o for o in pre['observations']};assert len(refs)==56
 for o in observations:
  ref=refs[o['mode'],o['key']]
  assert o['configuration']==ref['session_configuration']
  for v in o.get('observations',[o.get('observation')]):
   assert all(v[f]==ref[f] for f in ['rows','digest','fields','warnings'])
 assert not load('timing-interference-watch.json')['events']
 result={'identical_retained_ELF_A_B_control':True,'status':'PASS','cells':len(rows),'persisted_cell_observations':len(observations),'cases':len(specs),'queries':summary['counts']['primary_queries'],'rounds':4,'source_snapshots_equal':load('candidate-final-input.json')==load('build-after1.json'),'all_cells_at_least4seconds':True,'failed_queries_used_for_latency':0,'whole_SQL_full_fetch_not_kernel_timing':True,'scope':'all448 before/after exact fields/warnings/result/configuration observations independently matched strict56 preflight; exact EXPLAIN verified only at preflight; not full-invocation callchain profiling'}
 assert result['source_snapshots_equal'];(F/'audit-final.json').write_text(json.dumps(result,indent=2));print(result)
if __name__=='__main__':main()
