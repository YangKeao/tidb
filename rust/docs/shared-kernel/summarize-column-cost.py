#!/usr/bin/env python3
"""Recompute the single column_mul case; phases are not an additive breakdown."""
import csv
import hashlib
import json
from pathlib import Path
import re
import statistics
import sys

root = Path(sys.argv[1])
def csv_rows(name):
    with (root / name).open() as stream:
        return list(csv.DictReader(stream))

def stats(values):
    return dict(min=min(values), median=statistics.median(values), max=max(values))

timing = csv_rows('fixed-timing/all-rounds.csv')
allocations = csv_rows('fixed-allocations/all-rounds.csv')
phases = csv_rows('phases.csv')
assert len(timing) == 32 and len(allocations) == 8 and len(phases) == 40
result = {'complete_ns': {}, 'allocations': {}, 'diagnostic_ns': {}, 'paired_ratio': {}}
for route in ('native_vector', 'native_scalar', 'shared', 'shared_scalar'):
    rows = [v for v in timing if v['route'] == route]
    assert len(rows) == 8 and all(int(v['iterations']) == 20000 for v in rows)
    result['complete_ns'][route] = stats([int(v['nanos']) / int(v['iterations']) for v in rows])
    values = {(int(v['alloc_calls']) / 100, int(v['allocated_bytes']) / 100)
              for v in allocations if v['route'] == route}
    assert len(values) == 1
    result['allocations'][route] = dict(zip(('calls', 'bytes'), values.pop()))
for route in ('shared', 'shared_scalar'):
    ratios = []
    for shared in (v for v in timing if v['route'] == route):
        native = next(v for v in timing if v['route'] == 'native_vector'
                      and (v['round'], v['version']) == (shared['round'], shared['version']))
        ratios.append(int(shared['nanos']) / int(native['nanos']))
    result['paired_ratio'][route + '/native_vector'] = stats(ratios)
for phase in ('input', 'arithmetic', 'evaluate', 'write'):
    rows = [v for v in phases if v['phase'] == phase and v['mode'] == 'plain']
    result['diagnostic_ns'][phase] = stats([int(v['nanos']) / int(v['iterations']) for v in rows])
result['profile_self_percent'] = {}
for route in ('shared', 'native_vector'):
    text = (root / (route + '.flat.txt')).read_text()
    assert 'Total Lost Samples: 0' in text
    symbols = [{'percent': float(p), 'symbol': s} for p, s in
               re.findall(r'^\s*([0-9.]+)%\s+decimal-bench\s+\S+\s+\[.\]\s+(.+)$', text, re.M)]
    result['profile_self_percent'][route] = symbols
baseline = json.loads((root / 'baseline-plain/build-receipt.json').read_text())
for mode in ('plain', 'alloc'):
    receipt = json.loads((root / ('diagnostic-' + mode) / 'build-receipt.json').read_text())
    assert receipt['libraries_sha256'] == baseline['libraries_sha256']
    binary = root / ('diagnostic-' + mode) / 'decimal-bench'
    assert hashlib.sha256(binary.read_bytes()).hexdigest() == receipt['binary_sha256']
    run = json.loads((root / ('fixed-timing' if mode == 'plain' else 'fixed-allocations') / 'run-receipt.json').read_text())
    assert set(run['binary_sha256'].values()) == {receipt['binary_sha256']}
    assert all(v['exit'] == 0 for v in run['runs'])
result['interpretation'] = 'Same product and same binary repeated as before/after; no optimized candidate. Phase slices have different ownership and are not additive. Profile percentages are self cycles, not complete timings.'
print(json.dumps(result, indent=2))
