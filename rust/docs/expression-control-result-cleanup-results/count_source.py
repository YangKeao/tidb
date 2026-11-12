"""Count this round separately; explicitly classify cfg(test) helper removals."""
from pathlib import Path
import json,re,subprocess
P=Path(__file__).resolve().parent;R=Path('/home/agent/tidb/expression-unification/tidb')
def git(*a):return subprocess.check_output(['git',*a],cwd=R,text=True)
base='85da8c2a85deca927ec4379d4ae52ad5ffb7246f';head=git('rev-parse','HEAD').strip();rows=[]
for commit in git('rev-list','--reverse',base+'..'+head).splitlines():
 for l in git('diff','--numstat',commit+'^',commit).splitlines():
  a,d,f=l.split('\t')
  if not f.endswith('.rs'):continue
  a,d=int(a),int(d)
  if 'tests' in Path(f).stem:kind='test'
  elif f.endswith('/evaluator/batch_ast_source.rs'):kind='test' # one old test closure only
  elif f.endswith(('/tikv/cast_float.rs','/tikv/cast_integer.rs','/tikv/lineage.rs')):kind='test' # cfg(test) dead helpers/import only
  else:kind='production'
  if commit.startswith('391ad') and f.endswith('/scalar_function/math_diagnostic.rs'):
   rows.append({'commit':commit,'file':f,'category':'test','added':1,'deleted':1});a-=1;d-=1
  rows.append({'commit':commit,'file':f,'category':kind,'added':a,'deleted':d})
totals={k:{m:sum(x[m] for x in rows if x['category']==k) for m in ['added','deleted']} for k in ['production','test']}
for row in totals.values():row['net_reduction']=row['deleted']-row['added']
checks={}
for f in git('diff','--name-only',base,head).splitlines():
 if not f.endswith('.rs'):continue
 before=git('show',base+':'+f);after=(R/f).read_text();pattern=r'#\[test\]\s*(?:#\[[^\]]+\]\s*)*fn\s+(\w+)';a=re.findall(pattern,before);b=re.findall(pattern,after);assert a==b,f;checks[f]={'count':len(a),'names':a}
previous=json.loads((P.parent/'unused-private-helpers/source-reduction.json').read_text())
combined={k:{m:totals[k][m]+previous['totals'][k][m] for m in ['added','deleted','net_reduction']} for k in totals}
result={'base':base,'head':head,'totals':totals,'since_preceding_measured_source':{'base':previous['base'],'totals':combined},'per_commit_file':rows,'test_function_inventory':checks,'classification':'Changed inline cfg(test) helper bodies/imports and test assertion/closure changes counted as test; other scoped Rust production; blank lines included; docs excluded. Earlier measured2582/+63 report remains separate.'}
(P/'source-reduction.json').write_text(json.dumps(result,indent=2)+'\n');print(totals,'combined',combined,'retainedtestnames',sum(c['count'] for c in checks.values()))
