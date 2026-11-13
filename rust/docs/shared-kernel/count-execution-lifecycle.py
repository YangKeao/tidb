# Counts production/tests/build/docs against both original and checkpoint5.
# Usage: python3 count-checkpoint-6.py /path/containing/tidb-and-tikv
import subprocess,json,re,sys
from pathlib import Path
root=Path(sys.argv[1])
res={}
for repo,base,previous in [('tidb','364aef2bab5cc633ecb76a775ae8f36f86a6687d','854d47d675a82d242467cc92182476c33b6f94ce'),('tikv','548812e1ef57aef077a2062a9cc356640a6347f5','2781f6775939ea2700bd26563ae7f16cddec0344')]:
 res[repo]={}
 for title,ref in [('original',base),('incremental',previous)]:
  counts={k:[0,0] for k in ['production','tests','build','documents']}
  def git(*args):return subprocess.check_output(['git','-C',str(root/repo),*args],text=True)
  for row in git('diff','--numstat',ref).splitlines():
   a,d,path=row.split('\t'); a,d=int(a),int(d)
   kind='tests' if (path.endswith('/decimal-shared-bench.rs') or '/tests/' in path or '/tests_' in path) else ('production' if path.endswith('.rs') else ('build' if 'Cargo.' in path else 'documents'))
   if path.endswith(('/shared_kernel.rs', '/decimal/compact.rs')) or path in ('components/tidb_query_expr/src/types/expr_eval.rs','components/tidb_query_expr/src/impl_op.rs'):
    old=git('show',ref+':'+path).splitlines() if git('ls-tree',ref,'--',path).strip() else []
    new=(root/repo/path).read_text().splitlines()
    ot=old.index('#[cfg(test)]')+1 if old else 10**9; nt=new.index('#[cfg(test)]')+1
    ta=td=0
    for line in git('diff','-U0',ref,'--',path).splitlines():
     if line.startswith('@@'):
      m=re.match(r'@@ -(\d+)(?:,\d+)? \+(\d+)',line); oi,ni=map(int,m.groups())
     elif line.startswith('+++') or line.startswith('---'):continue
     elif line.startswith('+'):ta+=ni>=nt;ni+=1
     elif line.startswith('-'):td+=oi>=ot;oi+=1
    counts['tests'][0]+=ta;counts['tests'][1]+=td;a-=ta;d-=td
   counts[kind][0]+=a;counts[kind][1]+=d
  res[repo][title]=counts
print(json.dumps(res,indent=2))
