#!/usr/bin/env python3
"""Verify task45 retained evidence and selected source inputs, without rerunning products."""
import argparse
import hashlib
import json
from pathlib import Path

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--evidence', type=Path, required=True)
p.add_argument('--root', type=Path, required=True, help='directory containing tidb and tikv')
a = p.parse_args()
receipt = json.loads(Path(__file__).with_name('execution-lifecycle-validation.json').read_text())
sha = lambda p: hashlib.sha256(p.read_bytes()).hexdigest()
for r in receipt['records']:
    assert sha(a.evidence / r['name']) == r['sha256'], r['name']
for r in json.loads((a.evidence / 'libraries.json').read_text()):
    assert sha(a.evidence / r['path']) == r['sha256'], r['path']
snapshot = json.loads((a.evidence / 'source-snapshot.json').read_text())
assert snapshot['source_inputs'] == receipt['source_inputs']
assert len(snapshot['records']) == receipt['source_inputs']['count']
encoded = json.dumps(snapshot['records'], sort_keys=True, separators=(',', ':')).encode()
assert hashlib.sha256(encoded).hexdigest() == receipt['source_inputs']['sha256']
for r in snapshot['records']:
    assert sha(a.root / r['repo'] / r['path']) == r['sha256'], r
for name, count in [('shared-local-nodes', 47), ('prepared-regression', 8),
                    ('real-context-verified', 1), ('prepared-lifecycle', 1),
                    ('sdk-tests-final', 430)]:
    log = (a.evidence / (name + '.log')).read_text()
    assert 'test result: ok. %d passed; 0 failed;' % count in log, name
    assert log.rstrip().endswith('CHECK_EXIT=0'), name
before = (a.evidence / 'before-witness.log').read_text()
assert 'new execution must restart a constant seed' in before
assert before.rstrip().endswith('CHECK_EXIT=101')
assert json.loads((a.evidence / 'scoped-clippy-verified.json').read_text())['exit'] == 0
for r in json.loads((a.evidence / 'bench-smoke-reviewed.json').read_text()):
    assert r['exit'] == 0 and r['iterations'] == 1
print('Verified %d retained records, %d selected inputs, before failure and final checks.' %
      (len(receipt['records']), receipt['source_inputs']['count']))
