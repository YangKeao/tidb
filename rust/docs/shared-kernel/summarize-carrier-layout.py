#!/usr/bin/env python3
"""Recompute the fixed column experiment; allocation timings are not compared."""
import csv
import json
from pathlib import Path
import statistics
import sys

root = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).parent
timing = list(csv.DictReader((root / 'carrier-layout-timing.csv').open()))
alloc = list(csv.DictReader((root / 'carrier-layout-allocations.csv').open()))
routes = ['native_vector', 'native_scalar', 'shared', 'shared_scalar', 'direct_chunk']
expected = {(v, r) for v in ('before', 'after') for r in routes}
assert len(timing) == 40 and len(alloc) == 10
assert {(r['version'], r['route']) for r in timing} == expected
assert {(r['version'], r['route']) for r in alloc} == expected
assert all(r['case'] == 'column_mul' and int(r['width']) == 64 and r['phase'] == 'reuse' and int(r['round']) == 1 and int(r['iterations']) == 100 for r in alloc)
summary = {}
for version, route in sorted(expected):
    rows = [r for r in timing if (r['version'], r['route']) == (version, route)]
    assert {int(r['round']) for r in rows} == {1, 2, 3, 4} and len(rows) == 4
    assert all(r['case'] == 'column_mul' and r['phase'] == 'reuse' and int(r['width']) == 64 and int(r['iterations']) == 20000 and int(r['alloc_iterations']) == 0 for r in rows)
    times = [int(r['nanos']) / int(r['iterations']) for r in rows]
    counts = [r for r in alloc if (r['version'], r['route']) == (version, route)]
    assert len(counts) == 1 and int(counts[0]['alloc_iterations']) == 100
    summary[version + '/' + route] = dict(ns_min=min(times), ns_median=statistics.median(times), ns_max=max(times), alloc_calls=int(counts[0]['alloc_calls']) / 100, allocated_bytes=int(counts[0]['allocated_bytes']) / 100)

def times(version, route):
    return {int(r['round']): int(r['nanos']) / int(r['iterations']) for r in timing if (r['version'], r['route']) == (version, route)}

paired = {}
for route in routes:
    old, new = times('before', route), times('after', route)
    changes = [(new[i] / old[i] - 1) * 100 for i in old]
    paired[route + '_percent_change'] = [min(changes), statistics.median(changes), max(changes)]
a, b = times('after', 'direct_chunk'), times('after', 'native_vector')
paired['direct_over_native'] = [min(a[i] / b[i] for i in a), max(a[i] / b[i] for i in a)]
print(json.dumps({'routes': summary, 'paired': paired}, indent=2))
