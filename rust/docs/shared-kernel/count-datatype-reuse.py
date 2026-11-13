# Counts all tracked changes, including the standalone benchmark as test code.
# Usage: python3 count-datatype-reuse.py /path/containing/tidb-and-tikv [tidb_previous tikv_previous]
import subprocess,json,re,sys
from pathlib import Path
root=Path(sys.argv[1])
res={}
for repo,base,previous in [('tidb','364aef2bab5cc633ecb76a775ae8f36f86a6687d','72fedb911c2079ca821b66dae276095f3caaf84d'),('tikv','548812e1ef57aef077a2062a9cc356640a6347f5','3ad4dc75eec6af91b77f2c36f3d10ee61073b942')]:
 if len(sys.argv) == 4: previous = sys.argv[2 if repo == "tidb" else 3]
 res[repo]={}
 for title,ref in [('original',base),('incremental',previous)]:
  counts={k:[0,0] for k in ['production','tests','build','documents']}
  def git(*args):return subprocess.check_output(['git','-C',str(root/repo),*args],text=True)
  for row in git('diff','--numstat',ref).splitlines():
   a,d,path=row.split('\t'); a,d=int(a),int(d)
   kind='tests' if path.endswith('/decimal-shared-bench.rs') else ('production' if path.endswith('.rs') else ('build' if 'Cargo.' in path else 'documents'))
   if path.endswith('/shared_kernel.rs') or path in ('components/tidb_query_expr/src/types/expr_eval.rs','components/tidb_query_expr/src/impl_op.rs'):
    old=git('show',ref+':'+path).splitlines() if ref==previous or not path.endswith('/shared_kernel.rs') else []
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
