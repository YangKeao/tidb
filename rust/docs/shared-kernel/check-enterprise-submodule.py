#!/usr/bin/env python3
"""Exercise the actual init-submodule recipe against a local-only Git fixture."""
import argparse
import os
from pathlib import Path
import subprocess
import tempfile

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('repo', type=Path)
parser.add_argument('--expect', choices=('skipped', 'checked-out'), required=True)
args = parser.parse_args()
makefile = (args.repo / 'Makefile').read_text()
recipe = makefile.split('init-submodule:\n', 1)[1].split('\n\n', 1)[0]
assert recipe.startswith('\tgit submodule init && git submodule update ')
module = 'pkg/extension/enterprise'
env = dict(os.environ, GIT_CONFIG_NOSYSTEM='1', GIT_CONFIG_GLOBAL=os.devnull,
           GIT_ALLOW_PROTOCOL='file', GIT_AUTHOR_NAME='Local fixture',
           GIT_COMMITTER_NAME='Local fixture', GIT_AUTHOR_EMAIL='fixture@example.invalid',
           GIT_COMMITTER_EMAIL='fixture@example.invalid')

def run(cwd, *command):
    result = subprocess.run(command, cwd=cwd, env=env, text=True,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    print('$ ' + ' '.join(command), flush=True)
    print(result.stdout, end='', flush=True)
    result.check_returncode()
    return result.stdout.strip()

with tempfile.TemporaryDirectory(prefix='enterprise-submodule-') as temporary:
    root = Path(temporary)
    child, parent, clone = (root / name for name in ('child', 'parent', 'clone'))
    for repo in (child, parent):
        repo.mkdir()
        run(repo, 'git', 'init', '-q')
    (child / 'generate').mkdir()
    (child / 'generate/main.go').write_text('local fixture only\n')
    run(child, 'git', 'add', '.')
    run(child, 'git', 'commit', '-qm', 'Fixture content')
    expected_head = run(child, 'git', 'rev-parse', 'HEAD')
    run(parent, 'git', 'submodule', 'add', '--name', 'extension/enterprise', str(child), module)
    run(parent, 'git', 'config', '-f', '.gitmodules', 'submodule.extension/enterprise.update', 'none')
    (parent / 'Makefile').write_text('.PHONY: init-submodule\ninit-submodule:\n' + recipe + '\n')
    run(parent, 'git', 'add', '.')
    run(parent, 'git', 'commit', '-qm', 'Fixture with actual init recipe')
    run(root, 'git', 'clone', '--no-local', str(parent), str(clone))
    assert not (clone / module / 'generate/main.go').exists()
    run(clone, 'make', 'init-submodule')
    exists = (clone / module / 'generate/main.go').exists()
    assert exists == (args.expect == 'checked-out'), (args.expect, exists)
    if exists:
        assert (clone / module / 'generate/main.go').read_text() == 'local fixture only\n'
        assert run(clone / module, 'git', 'rev-parse', 'HEAD') == expected_head
        run(clone, 'make', 'init-submodule')
    print('PASS: ' + args.expect + '; no enterprise network access; fixture cleaned on exit')
