#!/usr/bin/env python3
"""Verify retained task47 records and reconstruct selected inputs for each attempt."""
import argparse, hashlib, json, os, subprocess, tempfile
from pathlib import Path
p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--root',type=Path,required=True)
p.add_argument('--evidence',type=Path,required=True)
p.add_argument('--snapshots-only',action='store_true')
a=p.parse_args()
sha=lambda data:hashlib.sha256(data).hexdigest()
if not a.snapshots_only:
 receipt=json.loads(Path(__file__).with_name('regexp-state-validation.json').read_text())
 for record in receipt['records']:
  assert sha((a.evidence/record['name']).read_bytes())==record['sha256'],record['name']
 for name,count in [('native-shared-final',56),('native-original-regexp',24),('independent-sdk-final',436)]:
  log=(a.evidence/(name+'.log')).read_text()
  assert 'test result: ok. %d passed; 0 failed;' % count in log,name
  assert log.rstrip().endswith('CHECK_EXIT=0'),name
 for name in ['main-scoped-clippy','independent-clippy','root-lint','final-format']:
  assert json.loads((a.evidence/(name+'.json')).read_text())['exit']==0,name
 assert 'left: 6\n right: 1' in (a.evidence/'before-prepares.log').read_text()
 assert json.loads((a.evidence/'before-prepares.json').read_text())['exit']==101
 assert json.loads((a.evidence/'after-prepares.json').read_text())['exit']==0
 final=json.loads((a.evidence/'source-snapshot.json').read_text())
 assert final['source_inputs']==receipt['source_inputs']
 assert sha(json.dumps(final['records'],sort_keys=True,separators=(',',':')).encode())==final['source_inputs']['sha256']
 for r in final['records']:assert sha((a.root/r['repo']/r['path']).read_bytes())==r['sha256'],r
blob_hashes={}; checked=0
for snapshot in sorted((a.evidence/'attempts').glob('*/sources.json')):
 sources=json.loads(snapshot.read_text())
 for repo,head in sources['commits'].items():
  cwd=a.root/repo
  with tempfile.TemporaryDirectory() as temp:
   env=dict(os.environ,GIT_INDEX_FILE=str(Path(temp)/'index'))
   def git(*args,**kwargs):return subprocess.check_output(['git',*args],cwd=cwd,env=env,**kwargs)
   git('read-tree',head)
   patch=snapshot.with_name(repo+'.patch')
   if patch.stat().st_size:git('apply','--cached',str(patch.resolve()))
   entries={}
   for row in git('ls-files','--stage','-z').split(b'\0'):
    if row:
     meta,path=row.split(b'\t');entries[path.decode()]=meta.split()[1].decode()
   selected=[r for r in sources['inputs'] if r['repo']==repo]
   missing=sorted({entries[r['path']] for r in selected}-blob_hashes.keys())
   if missing:
    data=git('cat-file','--batch',input=('\n'.join(missing)+'\n').encode());pos=0
    for oid in missing:
     end=data.index(b'\n',pos);header=data[pos:end].split();size=int(header[2]);pos=end+1
     assert header[0].decode()==oid and header[1]==b'blob'
     blob_hashes[oid]=sha(data[pos:pos+size]);pos+=size+1
   for r in selected:assert blob_hashes[entries[r['path']]]==r['sha256'],(snapshot,repo,r)
   checked+=len(selected)
print('Reconstructed %d attempts / %d selected file observations from HEAD plus binary patches.' % (len(list((a.evidence/'attempts').glob('*/sources.json'))),checked))
if not a.snapshots_only:print('Receipt hashes, current selected inputs, final checks and retained baseline failure match.')
