#!/usr/bin/env python3
"""Bounded GNU/Linux observer runner; no inferred success from missing output."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess

parser = argparse.ArgumentParser()
parser.add_argument('--binary', required=True, type=Path)
parser.add_argument('--observer', required=True, type=Path)
parser.add_argument('--output', required=True, type=Path)
args = parser.parse_args()
assert not os.environ.get('LD_PRELOAD') and not os.environ.get('LD_AUDIT')
binary = args.binary.resolve(strict=True)
observer = args.observer.resolve(strict=True)
args.output.mkdir(parents=True, exist_ok=True)
fixture = 'tikv::ready_value::tests::parent_external_observer_actual_pool_owner_arc_new_fixture'
control = 'tikv::runtime_failure::tests::six_outer_variants_keep_distinct_classes_and_fixed_messages'

def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()

cohort = {str(p): digest(p) for p in (binary, observer, Path(__file__).resolve())}
environment = dict(os.environ, LD_PRELOAD=str(observer))
results = []
for index in range(9):
    negative = index == 8
    name = control if negative else fixture
    command = [str(binary), name, '--ignored' if not negative else '--include-ignored',
               '--exact', '--nocapture', '--test-threads=1']
    child = subprocess.Popen(command, env=environment, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE, start_new_session=True)
    timed_out = False
    try:
        stdout, stderr = child.communicate(timeout=20)
    except subprocess.TimeoutExpired:
        timed_out = True
        os.killpg(child.pid, signal.SIGKILL)
        stdout, stderr = child.communicate()
    code = 124 if timed_out else child.returncode
    label = 'missing-markers-negative' if negative else f'sample-{index + 1}'
    (args.output / f'{label}-stdout.log').write_bytes(stdout)
    (args.output / f'{label}-stderr.log').write_bytes(stderr)
    text = stderr.decode('utf-8', errors='strict')
    native_one = b'test result: ok. 1 passed; 0 failed; 0 ignored;' in stdout
    passed = re.findall(r'^ASCII_POOL_OBSERVER PASS (.+)$', text, re.M)
    row = {'label': label, 'command': command, 'exit': code, 'timeout': timed_out,
           'native_one_passed': native_one}
    if negative:
        row['accepted'] = code == 86 and native_one and not passed and 'ASCII_POOL_OBSERVER INVALID' in text
    else:
        assert len(passed) == 1, (label, code, 'missing/duplicate observer PASS')
        fields = dict(part.split('=', 1) for part in passed[0].split())
        row['observer'] = fields
        row['accepted'] = (code == 0 and native_one and fields['faults'] == '0'
                           and fields['markers'] == '14' and fields['events'] == '16'
                           and fields['gap_events'] == '0' and fields['foreign_observations'] == '0'
                           and fields['matching_final_free'] == 'true')
    results.append(row)
    print(label, 'exit', code, 'accepted', row['accepted'], flush=True)
    if not row['accepted']:
        break
unchanged = all(digest(Path(path)) == sha for path, sha in cohort.items())
receipt = {'timeout_seconds': 20, 'separate_process_group': True,
           'timeout_classification': 'inconclusive, killed and reaped',
           'LD_PRELOAD': str(observer), 'cohort': cohort,
           'cohort_unchanged': unchanged, 'results': results}
(args.output / 'receipt.json').write_text(json.dumps(receipt, indent=2) + '\n')
assert unchanged and len(results) == 9 and all(r['accepted'] for r in results)
requests = {r['observer']['actual_owner_requested_bytes'] for r in results if 'observer' in r}
assert len(requests) == 1, ('nonuniform actual request sizes', requests)
print('Eight independently observed constructor requests:', requests,
      '; 64-clone and empty windows zero; paired inherited controls valid; missing-marker negative rejected')
