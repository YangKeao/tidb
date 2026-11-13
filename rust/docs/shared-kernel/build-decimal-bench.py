#!/usr/bin/env python3
"""Build an isolated allocation-counting harness against the real shared evaluator."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--repo', type=Path, required=True)
p.add_argument('--cargo', type=Path, required=True)
p.add_argument('--rustc', type=Path, required=True)
p.add_argument('--output', type=Path, required=True)
p.add_argument('--allocations', action='store_true', help='link allocation wrappers; omit for plain timing')
a = p.parse_args()
a.output.mkdir(parents=True, exist_ok=True)
env = dict(os.environ, CARGO_PROFILE_RELEASE_LTO='false', CARGO_PROFILE_RELEASE_CODEGEN_UNITS='16')
command = [str(a.cargo.resolve()), 'build', '--release', '-p', 'tidb-expr', '--features', 'shared-kernel', '--locked', '--offline', '--message-format=json']
with (a.output/'build.jsonl').open('w') as log:
    subprocess.run(command, cwd=a.repo/'rust', env=env, stdout=log, stderr=log, check=True)
required = {'tidb_expr', 'tidb_chunk', 'tidb_datatype', 'tidb_ast', 'tidb_query_expr', 'tidb_query_datatype'}
artifacts = {}
rlibs = {}
search_paths = set()
for line in (a.output/'build.jsonl').read_text().splitlines():
    try:
        message = json.loads(line)
    except json.JSONDecodeError:
        continue
    if message.get('reason') == 'build-script-executed':
        search_paths.update(message.get('linked_paths', []))
    if message.get('reason') == 'compiler-artifact':
        name = message['target']['name']
        if name == 'tikv_alloc':
            assert not message['features'], 'measurement requires the existing System allocator'
        for path in message['filenames']:
            search_paths.add('dependency='+str(Path(path).parent))
            if name in required and path.endswith('.rlib'):
                rlibs[name] = path
            if name in required and path.endswith('.rmeta'):
                assert message['profile']['opt_level'] == '3' and not message['profile']['debug_assertions'], name
                assert name not in artifacts or artifacts[name] == path, name
                artifacts[name] = path
assert set(artifacts) == required, artifacts
source = Path(__file__).resolve().with_name('decimal-shared-bench.rs')
binary = a.output.resolve()/'decimal-bench'
command = [str(a.rustc.resolve()), '--edition=2021', '-C', 'opt-level=3', '-C', 'codegen-units=16', '-C', 'lto=off', str(source), '-o', str(binary)]
for directory in sorted(search_paths):
    command += ['-L', directory]
for name, path in sorted(artifacts.items()):
    command += ['--extern', name+'='+path, '--extern', name+'='+rlibs[name]]
if a.allocations:
    command += ['--cfg', 'allocation_counting']
    for name in ('malloc','calloc','realloc','aligned_alloc','posix_memalign'):
        command += ['-C', 'link-arg=-Wl,--wrap='+name]
# Wrap the existing System allocator; product code and allocator choice stay unchanged.
with (a.output/'harness.log').open('w') as log:
    result = subprocess.run(command, cwd=a.repo/'rust', stdout=log, stderr=log)
    if result.returncode:
        raise SystemExit(f'harness compilation failed ({result.returncode}); see {a.output}/harness.log')
receipt = {'allocations': a.allocations, 'artifacts': artifacts, 'harness_command': command, 'profile': {'opt_level':3, 'lto':False, 'codegen_units':16}, 'binary_sha256':hashlib.sha256(binary.read_bytes()).hexdigest(), 'source_sha256':hashlib.sha256(source.read_bytes()).hexdigest()}
(a.output/'build-receipt.json').write_text(json.dumps(receipt, indent=2)+'\n')
print(binary)
