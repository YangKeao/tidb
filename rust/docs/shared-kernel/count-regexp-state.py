# Counts production/tests/build/docs against original and task46 endpoints.
# Usage: python3 count-regexp-state.py /path/containing/tidb-and-tikv
import subprocess,json,re,sys
from pathlib import Path
root=Path(sys.argv[1])
res={}
for repo,base,previous in [('tidb','364aef2bab5cc633ecb76a775ae8f36f86a6687d','dad1610309f814213c32f98e3d990c43e809aa02'),('tikv','548812e1ef57aef077a2062a9cc356640a6347f5','00106bee21d25fbf9ce18475a0400bebeaf4cdff')]:
 res[repo]={}
 for title,ref in [('original',base),('incremental',previous)]:
  counts={k:[0,0] for k in ['production','tests','build','documents']}
  def git(*args):return subprocess.check_output(['git','-C',str(root/repo),*args],text=True)
  for row in git('diff','--numstat',ref).splitlines():
   a,d,path=row.split('\t'); a,d=int(a),int(d)
   kind='tests' if (path.endswith('/decimal-shared-bench.rs') or '/tests/' in path or '/tests_' in path) else ('production' if path.endswith('.rs') else ('build' if 'Cargo.' in path else 'documents'))
   if path.endswith(('/shared_kernel.rs', '/decimal/compact.rs', '/mysql_time.rs', '/impl_time.rs', '/impl_regexp.rs', '/codec/mysql/time/mod.rs')) or path in ('components/tidb_query_expr/src/types/expr_eval.rs','components/tidb_query_expr/src/impl_op.rs'):
    old=git('show',ref+':'+path).splitlines() if git('ls-tree',ref,'--',path).strip() else []
    new=(root/repo/path).read_text().splitlines()
    def test_lines(lines):
     # The regex counter is cfg(test) outside the final test module.
     cutoff=next((i+1 for i,line in enumerate(lines) if line=='#[cfg(test)]' and i+1<len(lines) and lines[i+1].startswith('mod tests')),10**9)
     marked=set(range(cutoff,len(lines)+1))
     if path.endswith('/impl_regexp.rs'):
      for i,line in enumerate(lines):
       if line.strip()=='#[cfg(test)]' and i+1<len(lines):
        if lines[i+1].startswith('thread_local!'):marked.update(range(i+1,i+3))
        elif lines[i+1].strip()=='if native {':marked.update(range(i+1,i+5))
     return marked
    ot,nt=test_lines(old),test_lines(new)
    ta=td=0
    for line in git('diff','-U0',ref,'--',path).splitlines():
     if line.startswith('@@'):
      m=re.match(r'@@ -(\d+)(?:,\d+)? \+(\d+)',line); oi,ni=map(int,m.groups())
     elif line.startswith('+++') or line.startswith('---'):continue
     elif line.startswith('+'):ta+=ni in nt;ni+=1
     elif line.startswith('-'):td+=oi in ot;oi+=1
    counts['tests'][0]+=ta;counts['tests'][1]+=td;a-=ta;d-=td
   counts[kind][0]+=a;counts[kind][1]+=d
  res[repo][title]=counts
print(json.dumps(res,indent=2))
