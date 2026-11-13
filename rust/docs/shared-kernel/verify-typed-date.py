#!/usr/bin/env python3
"""Verify task46 retained records and selected inputs, without running products."""
import argparse, hashlib, json
from pathlib import Path
p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--root',type=Path,required=True)
p.add_argument('--evidence',type=Path,required=True)
a=p.parse_args()
receipt=json.loads(Path(__file__).with_name('typed-date-validation.json').read_text())
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
for r in receipt['records']:assert sha(a.evidence/r['name'])==r['sha256'],r['name']
snapshot=json.loads((a.evidence/'source-snapshot.json').read_text())
assert snapshot['source_inputs']==receipt['source_inputs']
assert len(snapshot['records'])==receipt['source_inputs']['count']
encoded=json.dumps(snapshot['records'],sort_keys=True,separators=(',',':')).encode()
assert hashlib.sha256(encoded).hexdigest()==receipt['source_inputs']['sha256']
for r in snapshot['records']:assert sha(a.root/r['repo']/r['path'])==r['sha256'],r
for name,count in [('shared-verified',53),('owner-tests',34),('native-date',21),('native-date-rows',1),('sdk-time-bits',1),('sdk-tests-final',432)]:
 log=(a.evidence/(name+'.log')).read_text()
 assert 'test result: ok. %d passed; 0 failed;' % count in log,name
 assert log.rstrip().endswith('CHECK_EXIT=0'),name
for name in ['before-native-fixed','before-pb-own','scoped-clippy','sdk-all-clippy','null-array-after']:
 assert json.loads((a.evidence/(name+'.json')).read_text())['exit']==0,name
for name in ['before-native','date-tests-compile-fixed','null-array-before']:
 assert json.loads((a.evidence/(name+'.json')).read_text())['exit']==101,name
for name in ['final-root-lint','final-format-diff']:
 assert (a.evidence/(name+'.log')).read_text().rstrip().endswith('CHECK_EXIT=0'),name
print('Verified %d records and %d selected inputs; final checks and retained failures match.' %(len(receipt['records']),receipt['source_inputs']['count']))
