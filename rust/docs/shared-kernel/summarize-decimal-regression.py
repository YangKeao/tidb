#!/usr/bin/env python3
"""Recompute three-version, six-case Decimal comparisons from raw CSV files."""
import csv
from pathlib import Path
import statistics
import sys

def read(path, rounds, iterations, allocations):
    rows = list(csv.DictReader(Path(path).open()))
    assert len(rows) == 120 * 3 * rounds
    result = {}
    for r in rows:
        key = (r['case'], int(r['width']), r['phase'], r['route'], r['version'], int(r['round']))
        assert key not in result and int(r['iterations']) == iterations
        assert int(r['alloc_iterations']) == allocations and int(r['nanos']) > 0
        result[key] = r
    return result

timing = read(sys.argv[1], 4, 20000, 0)
allocation = read(sys.argv[2], 1, 1, 100)
cases = ('inline_mul', 'spill_mul', 'small_mul', 'wide_mul', 'wide_cancel', 'hidden_chain')
versions = ('before', 'after', 'candidate')

def times(case, width, phase, route, version):
    return [int(timing[(case, width, phase, route, version, n)]['nanos']) / 20000 for n in range(1, 5)]

def ratio(a, b):
    pairs = [x/y for x, y in zip(a, b)]
    return f'{min(pairs):.3f}–{max(pairs):.3f}'

print('| 场景 / 行数 / 共享模式 | 旧副本 ns | 原复用 ns | 修正后 ns | 修正/原复用范围 | 修正/旧副本范围 |')
print('| --- | ---: | ---: | ---: | ---: | ---: |')
for case in cases:
    for width in (1, 64):
        for route in ('shared', 'shared_scalar'):
            values = [times(case, width, 'reuse', route, v) for v in versions]
            print(f'| {case}/{width}/{route} | ' + ' | '.join(f'{statistics.median(x):.1f}' for x in values) + f' | {ratio(values[2], values[1])} | {ratio(values[2], values[0])} |')

print('\n| 场景 / 64行原生模式 | 旧副本版本 ns | 原复用版本 ns | 修正后 ns |')
print('| --- | ---: | ---: | ---: |')
for case in cases:
    for route in ('native_vector', 'native_scalar'):
        print(f'| {case}/{route} | ' + ' | '.join(f'{statistics.median(times(case, 64, "reuse", route, v)):.1f}' for v in versions) + ' |')

print('\n| 场景 / 行数 / 共享模式 | 旧副本 次数/字节 | 原复用 次数/字节 | 修正后 次数/字节 |')
print('| --- | ---: | ---: | ---: |')
for case in cases:
    for width in (1, 64):
        for route in ('shared', 'shared_scalar'):
            values = [allocation[(case, width, 'reuse', route, v, 1)] for v in versions]
            print(f'| {case}/{width}/{route} | ' + ' | '.join(f'{int(r["alloc_calls"])/100:g}/{int(r["allocated_bytes"])/100:g}' for r in values) + ' |')

print('\n| small_mul / 64行诊断段 | 旧副本版本 ns | 原复用版本 ns | 修正后 ns |')
print('| --- | ---: | ---: | ---: |')
for phase, route in (('arithmetic', 'native'), ('evaluate', 'native_scalar'), ('evaluate', 'shared'), ('write', 'shared')):
    print(f'| {phase}/{route} | ' + ' | '.join(f'{statistics.median(times("small_mul", 64, phase, route, v)):.1f}' for v in versions) + ' |')
