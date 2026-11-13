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
p.add_argument('--before', type=Path)
p.add_argument('--after', type=Path, required=True)
p.add_argument('--candidate', type=Path)
p.add_argument('--output', type=Path, required=True)
p.add_argument('--iterations', type=int, default=20000)
p.add_argument('--allocations', action='store_true')
p.add_argument('--columns', action='store_true', help='run the single real-column complete-entry case')
p.add_argument('--rounds', type=int, default=4)
p.add_argument('--direct-chunk', action='store_true', help='after/candidate binaries additionally expose direct_chunk')
a = p.parse_args()
assert a.iterations > 0
a.output.mkdir(parents=True, exist_ok=True)
cpu = max(os.sched_getaffinity(0))
os.sched_setaffinity(0, {cpu})
assert a.columns or a.before, 'constant comparisons require --before'
versions = {'before':a.before.resolve()} if a.before else {}
versions['after'] = a.after.resolve()
if a.candidate: versions['candidate'] = a.candidate.resolve()
receipt = {'cpu':cpu, 'iterations':a.iterations, 'allocation_mode':a.allocations, 'rounds':a.rounds, 'binary_sha256':{k:hashlib.sha256(v.read_bytes()).hexdigest() for k,v in versions.items()}, 'runs':[]}
expected = {(case,0,'prepare',route) for case in ('inline_mul','spill_mul','small_mul','wide_mul','wide_cancel','hidden_chain') for route in ('native','shared')}
expected |= {(case,width,phase,route) for case in ('inline_mul','spill_mul','small_mul','wide_mul','wide_cancel','hidden_chain') for width in (1,64) for phase,route in (('reuse','native_scalar'),('reuse','native_vector'),('reuse','shared'),('evaluate','native_scalar'),('evaluate','shared'),('write','shared'),('arithmetic','native'),('reuse','shared_scalar'),('evaluate','shared_scalar'))}
if a.columns:
    expected = {('column_mul',64,'reuse',r) for r in ('native_scalar','native_vector','shared','shared_scalar')}
rows = []
for round_number in range(a.rounds):
    for version in (list(versions) if round_number % 2 == 0 else list(reversed(versions))):
        stem = f'{round_number+1}-{version}'
        started = time.time_ns()
        env = dict(os.environ, DECIMAL_BENCH_ITERATIONS=str(a.iterations))
        if a.columns: env['DECIMAL_BENCH_COLUMNS'] = '1'
        run = subprocess.run([str(versions[version])], env=env, capture_output=True, text=True)
        (a.output/(stem+'.csv')).write_text(run.stdout)
        (a.output/(stem+'.stderr')).write_text(run.stderr)
        receipt['runs'].append({'round':round_number+1,'version':version,'exit':run.returncode,'started_unix_ns':started,'duration_ns':time.time_ns()-started})
        (a.output/'run-receipt.json').write_text(json.dumps(receipt,indent=2)+'\n')
        assert run.returncode == 0, stem
        data = list(csv.DictReader(io.StringIO(run.stdout)))
        expected_run = expected | ({('column_mul',64,'reuse','direct_chunk')} if a.direct_chunk and version != 'before' else set())
        assert len(data) == len(expected_run) and {(r['case'],int(r['width']),r['phase'],r['route']) for r in data} == expected_run, stem
        for r in data:
            assert int(r['iterations']) == a.iterations and int(r['alloc_iterations']) == (100 if a.allocations else 0) and int(r['nanos']) > 0
            rows.append({'round':round_number+1,'version':version,**r})
        print(stem+' complete', flush=True)
with (a.output/'all-rounds.csv').open('w') as f:
    writer = csv.DictWriter(f, fieldnames=list(rows[0]),lineterminator='\n')
    writer.writeheader();writer.writerows(rows)
