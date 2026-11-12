"""Prove only estRows differs before permitting the single owned-storage retry."""
from pathlib import Path
import json,time
P=Path(__file__).resolve().parent

def compare_plan(a,b):
 assert len(a)==len(b)
 changes=[]
 for i,(x,y) in enumerate(zip(a,b)):
  assert len(x)==len(y) and len(x)>1
  for j,(u,v) in enumerate(zip(x,y)):
   if u!=v:
    assert j==1,('non-estRows plan difference',i,j,u,v)
    changes.append({'row':i,'column':j,'reference':u,'other':v})
 return changes
assert compare_plan([['x','2','a']],[['x','3','a']])==[{'row':0,'column':1,'reference':'2','other':'3'}]
for bad in [[['y','2','a']],[['x','2','b']]]:
 try:compare_plan([['x','2','a']],bad)
 except AssertionError:pass
 else:raise AssertionError('non-estRows accepted')
if __name__=='__main__':
 assert not (P/'preflight-initial.json').exists(),'single retry already used'
 assert not (P/'primary.jsonl').exists(),'timing already began; no retry'
 r=json.loads((P/'measurement1.receipt.json').read_text());assert r['state']=='complete' and r['returncode']!=0
 s=json.loads((P/'preflight.json').read_text());assert len(s['observations'])==56 and len(s['pairs'])==14 and s['failures']
 refs={(o['mode'],o['key']):o for o in s['observations']};changes=[]
 for pair in s['pairs']:
  k=pair['key'];a=refs['native',k]
  for m in ['current','retained_b','frozen']:
   b=refs[m,k];assert 'error' not in a and 'error' not in b
   assert a['case_spec']==b['case_spec']
   assert all(a[f]==b[f] for f in ['rows','digest','fields','warnings','session_configuration','result_json_bytes','preview'])
   diff=compare_plan(a['explain'],b['explain'])
   if diff:changes.append({'key':k,'mode':m,'changes':diff})
 assert len(changes)==len(s['failures']) and changes
 out={'status':'PASS only estRows differ; no timing yet; eligible for at most one same-owned-storage restart','checked':time.time(),'failed_comparisons':len(changes),'changed_estRows_cells':sum(len(c['changes']) for c in changes),'retained_B_also_differs':any(c['mode']=='retained_b' for c in changes),'all_other_plan_fields_results_metadata_warnings_configuration_exact':True,'changes':changes,'actual_failed_receipt':r}
 (P/'preflight-estRows-only-proof.json').write_text(json.dumps(out,indent=2)+'\n');print(out['status'],len(changes),out['changed_estRows_cells'])
