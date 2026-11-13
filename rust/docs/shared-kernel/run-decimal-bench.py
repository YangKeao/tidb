#!/usr/bin/env python3
"""Alternate fixed Decimal binaries; keep raw rounds and allocator counts."""
import argparse
import csv
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import time

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--before', type=Path, required=True)
p.add_argument('--after', type=Path, required=True)
p.add_argument('--output', type=Path, required=True)
p.add_argument('--iterations', type=int, default=20000)
a = p.parse_args()
assert a.iterations > 0
a.output.mkdir(parents=True, exist_ok=True)
cpu = max(os.sched_getaffinity(0))
os.sched_setaffinity(0, {cpu})
versions = {'before':a.before.resolve(), 'after':a.after.resolve()}
receipt = {'cpu':cpu, 'iterations':a.iterations, 'binary_sha256':{k:hashlib.sha256(v.read_bytes()).hexdigest() for k,v in versions.items()}, 'runs':[]}
expected = {(case,width,phase,route) for case in ('small_mul','wide_mul','wide_cancel','hidden_chain') for width,phase in ((0,'prepare'),(1,'reuse'),(64,'reuse')) for route in ('native','shared')}
rows = []
for round_number in range(4):
    for version in (('before','after') if round_number % 2 == 0 else ('after','before')):
        stem = f'{round_number+1}-{version}'
        started = time.time_ns()
        run = subprocess.run([str(versions[version])], env=dict(os.environ, DECIMAL_BENCH_ITERATIONS=str(a.iterations)), capture_output=True, text=True)
        (a.output/(stem+'.csv')).write_text(run.stdout)
        (a.output/(stem+'.stderr')).write_text(run.stderr)
        receipt['runs'].append({'round':round_number+1,'version':version,'exit':run.returncode,'started_unix_ns':started,'duration_ns':time.time_ns()-started})
        (a.output/'run-receipt.json').write_text(json.dumps(receipt,indent=2)+'\n')
        assert run.returncode == 0, stem
        data = list(csv.DictReader(io.StringIO(run.stdout)))
        assert len(data) == 24 and {(r['case'],int(r['width']),r['phase'],r['route']) for r in data} == expected, stem
        for r in data:
            assert int(r['iterations']) == a.iterations and int(r['alloc_iterations']) == 100 and int(r['nanos']) > 0
            rows.append({'round':round_number+1,'version':version,**r})
        print(stem+' complete', flush=True)
with (a.output/'all-rounds.csv').open('w') as f:
    writer = csv.DictWriter(f, fieldnames=list(rows[0]),lineterminator='\n')
    writer.writeheader();writer.writerows(rows)
