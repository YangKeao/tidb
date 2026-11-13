#!/usr/bin/env python3
"""Recompute paired timing and allocation tables from the 192 raw cells."""
import csv
from pathlib import Path
import statistics
import sys

rows = list(csv.DictReader(Path(sys.argv[1]).open()))
cells = {(r['case'], r['width'], r['phase']) for r in rows}
assert len(rows) == 192 and len(cells) == 12
data = {}
for r in rows:
    key = (r['case'], r['width'], r['phase'], r['route'], r['version'], int(r['round']))
    assert key not in data and int(r['iterations']) == 20000 and int(r['alloc_iterations']) == 100
    data[key] = r

def values(cell, route, version, column):
    result = []
    for n in range(1, 5):
        r = data[(*cell, route, version, n)]
        divisor = int(r['iterations'] if column == 'nanos' else r['alloc_iterations'])
        result.append(int(r[column]) / divisor)
    if column != 'nanos':
        assert len(set(result)) == 1, (cell, route, version, column)
    return result

print('| 场景 / 行数 / 阶段 | 原生前 ns | 原生后 ns | 共享前 ns | 共享后 ns | 共享后/前四轮范围 |')
print('| --- | ---: | ---: | ---: | ---: | ---: |')
for cell in sorted(cells):
    series = [values(cell, route, version, 'nanos') for route in ('native', 'shared') for version in ('before', 'after')]
    ratio = [b/a for a, b in zip(series[2], series[3])]
    print('| ' + '/'.join(cell) + ' | ' + ' | '.join(f'{statistics.median(x):.1f}' for x in series) + f' | {min(ratio):.3f}–{max(ratio):.3f} |')
print('\n| 场景 / 行数 / 阶段 | 原生 次数 / 字节 | 共享前 次数 / 字节 | 共享后 次数 / 字节 |')
print('| --- | ---: | ---: | ---: |')
for cell in sorted(cells):
    parts = []
    for route, version in (('native', 'before'), ('shared', 'before'), ('shared', 'after')):
        parts.append(' / '.join(f'{values(cell, route, version, col)[0]:g}' for col in ('alloc_calls', 'allocated_bytes')))
    for col in ('alloc_calls', 'allocated_bytes'):
        assert values(cell, 'native', 'before', col) == values(cell, 'native', 'after', col)
    print('| ' + '/'.join(cell) + ' | ' + ' | '.join(parts) + ' |')
