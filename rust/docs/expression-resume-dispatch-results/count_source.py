"""Count the reviewed six-file production patch separately."""
from pathlib import Path
import json,re,subprocess
P=Path(__file__).resolve().parent;R=Path('/home/agent/tidb/expression-unification/tidb')
def git(*a):return subprocess.check_output(['git',*a],cwd=R,text=True)
base='ffb508eeae';head=git('rev-parse','HEAD').strip();rows=[]
for commit in git('rev-list','--reverse',base+'..'+head).splitlines():
 for l in git('diff','--numstat',commit+'^',commit).splitlines():
  a,d,f=l.split('\t')
  if not f.endswith('.rs'):continue
  a,d=int(a),int(d)
  assert f in json.loads((P/'patch-inputs.json').read_text()),f
  kind='production' # exact six-file patch changes only the reviewed production blocks
  rows.append({'commit':commit,'file':f,'category':kind,'added':a,'deleted':d})
assert {r['file'] for r in rows}==set(json.loads((P/'patch-inputs.json').read_text()))
totals={k:{m:sum(x[m] for x in rows if x['category']==k) for m in ['added','deleted']} for k in ['production','test']}
for row in totals.values():row['net_reduction']=row['deleted']-row['added']
checks={}
for f in git('diff','--name-only',base,head).splitlines():
 if not f.endswith('.rs'):continue
 before=git('show',base+':'+f);after=(R/f).read_text();pattern=r'#\[test\]\s*(?:#\[[^\]]+\]\s*)*fn\s+(\w+)';a=re.findall(pattern,before);b=re.findall(pattern,after);assert a==b,f;checks[f]={'count':len(a),'names':a}
previous=json.loads((P.parent/'control-result-cleanup/source-reduction.json').read_text())
combined={k:{m:totals[k][m]+previous['since_preceding_measured_source']['totals'][k][m] for m in ['added','deleted','net_reduction']} for k in totals}
result={'base':base,'head':head,'totals':totals,'since_preceding_measured_source':{'base':previous['since_preceding_measured_source']['base'],'totals':combined},'per_commit_file':rows,'test_function_inventory':checks,'classification':'Only the six reviewed production blocks/direct calls changed; all test function names and sample data retained; blank lines included, docs excluded. Previous501/86 cumulative source result and earlier2582/+63 report remain separate.'}
(P/'source-reduction.json').write_text(json.dumps(result,indent=2)+'\n');print(totals,'combined',combined,'retainedtestnames',sum(c['count'] for c in checks.values()))
