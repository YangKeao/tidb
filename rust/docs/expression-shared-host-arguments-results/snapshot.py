"""Read-only input snapshots for the three measured production binaries."""
from pathlib import Path
import hashlib,json,subprocess
BASE=Path(__file__).resolve().parent
MAIN=Path('/home/agent/tidb/expression-unification/tidb')
SDK=MAIN.parent/'tikv'
NATIVE=Path('/home/agent/tidb/bench-original-native/tidb')
def sha(p):return hashlib.file_digest(p.open('rb'),'sha256').hexdigest()
def git(root,*args):return subprocess.check_output(['git',*args],cwd=root,text=True).strip()
def snapshot():
 result={}
 for owner,root in [('main',MAIN),('sdk',SDK),('native',NATIVE)]:
  scope='rust' if owner!='sdk' else '.'
  paths=sorted(set(git(root,'ls-files',scope).splitlines()+git(root,'ls-files','--others','--exclude-standard',scope).splitlines()))
  result[owner]={'head':git(root,'rev-parse','HEAD'),'status':git(root,'status','--short'),'files':{p:sha(root/p) for p in paths if p.endswith(('.rs','.toml','.lock','.json','.tsv')) and (root/p).is_file()}}
 result['protected_BUILD']=sha(MAIN/'rust/third_party/tikv-client-rs/tests/client_go_differential/BUILD.bazel')
 return result
def write(name,data):(BASE/name).write_text(json.dumps(data,indent=2)+'\n')
