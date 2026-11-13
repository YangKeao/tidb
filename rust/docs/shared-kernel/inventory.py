#!/usr/bin/env python3
"""Build an original-source candidate inventory, never equivalence credit."""
import argparse
import collections
import json
import re
import subprocess
from pathlib import Path

p = argparse.ArgumentParser()
p.add_argument('--families', type=Path, required=True)
p.add_argument('--tikv', type=Path, required=True)
p.add_argument('--output', type=Path, required=True)
a = p.parse_args()
root = Path(__file__).resolve().parents[3]
ledger = json.loads(a.families.read_text())
rows = [r for r in ledger['rows'] if r['in_frozen_denominator']]
assert len(rows) == 245 and len({r['id'] for r in rows}) == 245
exceptions = set(ledger['uncredited_compatibility_families'])
assert len(exceptions) == 5

def sources(base, revision, pattern):
    names = subprocess.check_output(['git', '-C', str(base), 'ls-tree', '-r', '--name-only', revision], text=True).splitlines()
    result = []
    for name in names:
        if Path(name).match(pattern):
            content = subprocess.check_output(['git', '-C', str(base), 'show', revision + ':' + name], text=True)
            result.append((name, content.split('#[cfg(test)]')[0].splitlines()))
    return result

# Path.match does not give ** arbitrary-depth semantics: include all source levels.
names = subprocess.check_output(['git', '-C', str(root), 'ls-tree', '-r', '--name-only', '364aef2bab5cc633ecb76a775ae8f36f86a6687d', '--', 'rust/crates/tidb-expr/src'], text=True).splitlines()
native = [(name, subprocess.check_output(['git', '-C', str(root), 'show', '364aef2bab5cc633ecb76a775ae8f36f86a6687d:' + name], text=True).split('#[cfg(test)]')[0].splitlines()) for name in names if name.endswith('.rs')]
sdk = sources(a.tikv, '548812e1ef57aef077a2062a9cc356640a6347f5', 'components/tidb_query_expr/src/impl_*.rs')

aliases = {'if':'if_condition', 'case':'case_when', 'ifnull':'if_null', 'not':'unary_not',
    'and':'logical_and', 'or':'logical_or', 'xor':'logical_xor',
    'plus':'add', 'minus':'sub', 'mul':'multiply', 'mod':'mod',
    'unaryminus':'unary_minus', 'unaryplus':'unary_plus', 'nulleq':'compare',
    'eq':'compare', 'ne':'compare', 'gt':'compare', 'ge':'compare', 'lt':'compare',
    'le':'compare', 'in':'compare_in', 'ln':'log', 'atan':'atan',
    'cast':'cast', 'dayofmonth':'day_of_month', 'dayofweek':'day_of_week',
    'dayofyear':'day_of_year', 'datediff':'date_diff', 'timediff':'time_diff',
    'timestampdiff':'timestamp_diff', 'from_unixtime':'from_unixtime',
    'weekofyear':'week_of_year', 'yearweek':'year_week', 'isnull':'is_null',
    'istrue':'int_is_true', 'isfalse':'int_is_false', 'regexp_like':'regexp_like',
    'char_func':'char', 'insert_func':'insert', 'json_member_of':'member_of', 'timediff':'duration_duration_time_diff', 'bitand':'bit_and',
    'bitor':'bit_or', 'bitxor':'bit_xor', 'bitneg':'bit_neg',
    'leftshift':'left_shift', 'rightshift':'right_shift', 'makedate':'make_date',
    'maketime':'make_time', 'microsecond':'micro_second', 'monthname':'month_name',
    'dayname':'day_name', 'weekday':'week_day', 'date_add':'add_date',
    'date_sub':'sub_date', 'addtime':'add_datetime', 'subtime':'sub_datetime',
    'mul':'arithmetic', 'div':'arithmetic', 'intdiv':'arithmetic', 'mod':'arithmetic'}
result = []
for r in rows:
    name = r['id']
    needle = aliases.get(name, name)
    hits = []
    for file, lines in sdk:
        for n, line in enumerate(lines, 1):
            if re.search(r'\bfn\s+' + re.escape(needle) + r'(?:\b|_)', line):
                hits.append(f'{file}:{n}')
    native_hits = []
    names = set(r['spellings'] + [name])
    for file, lines in native:
        if '/tests/' in file or file.endswith('_tests.rs') or file.endswith('/tests.rs'):
            continue
        for n, line in enumerate(lines, 1):
            if any('"' + x + '"' in line or re.search(r'\bfn\s+' + re.escape(x) + r'(?:\b|_)', line) for x in names):
                native_hits.append(f'{file}:{n}')
    if name == 'between':
        native_hits = [f'{file}:{n}' for file, lines in native for n, line in enumerate(lines, 1) if 'Expr::Between {' in line]
    # This is an explicit work allocation, not an automatic semantic verdict.
    classification = ('existing_exception' if name in exceptions else
        'direct_reuse_candidate' if name == 'md5' else
        'semantic_revision_candidate' if hits else 'algorithm_migration_candidate')
    result.append(dict(family=name, category=r['category'], spellings=r['spellings'],
        classification=classification, native_anchors=native_hits,
        sdk_candidates=hits, equivalence_verified=False,
        retirement='retain until shared replacement passes differential tests',
        uncertainty='Candidate search is not exhaustive; inspect aliases, generic kernels, coercion, NULL demand, collation and errors before migration.'))
counts = dict(collections.Counter(r['classification'] for r in result))
out = dict(tidb_base='364aef2bab5cc633ecb76a775ae8f36f86a6687d',
    tikv_base='548812e1ef57aef077a2062a9cc356640a6347f5',
    denominator=245, exceptions=sorted(exceptions), counts=counts,
    note='Planning candidates only. No historical functional credit inherited. File anchors are from original-base sources; missing matches do not establish missing algorithms.', rows=result)
a.output.write_text(json.dumps(out, ensure_ascii=False, indent=2) + '\n')
assert sum(counts.values()) == 245
print(json.dumps(counts, sort_keys=True))
