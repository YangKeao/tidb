#!/usr/bin/env python3
"""Verify checkpoint6 source inputs; optionally verify preserved local logs."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--tidb', type=Path, required=True)
parser.add_argument('--tikv', type=Path, required=True)
parser.add_argument('--evidence', type=Path)
args = parser.parse_args()
receipt = json.loads(Path(__file__).with_name('checkpoint-6-validation.json').read_text())
records = []
for name in ('tidb', 'tikv'):
    root = getattr(args, name)
    paths = subprocess.check_output(['git', '-C', str(root), 'ls-files', '-z']).decode().split('\0')
    for path in paths:
        if not path.endswith(('.rs', '.toml', '.lock', '.c', '.cc', '.cpp', '.h', '.proto')):
            continue
        source = root / path
        if source.is_file():
            records.append({'repo': name, 'path': path,
                            'sha256': hashlib.sha256(source.read_bytes()).hexdigest()})
encoded = json.dumps(records, sort_keys=True, separators=(',', ':')).encode()
actual = {'count': len(records), 'sha256': hashlib.sha256(encoded).hexdigest()}
if actual != receipt['source_inputs']:
    raise SystemExit('Source inputs differ: ' + json.dumps(actual))
if args.evidence:
    for log in receipt['logs']:
        observed = hashlib.sha256((args.evidence / log['name']).read_bytes()).hexdigest()
        if observed != log['sha256']:
            raise SystemExit('Log differs: ' + log['name'])
print(json.dumps({'source_inputs_verified': actual, 'logs_verified': bool(args.evidence)}))
