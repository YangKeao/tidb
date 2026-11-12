"""Verify the Git review evidence using only Python's standard library.

Run in this directory. Reconstruct the exact observation log in a temporary
copy, then rerun the original timing audit without starting any services.
"""
from pathlib import Path
import hashlib,json,shutil,subprocess,sys,tempfile
sys.dont_write_bytecode = True
from observation_storage import decode
P=Path(__file__).resolve().parent
manifest=json.loads((P/'manifest.json').read_text())
for name,record in manifest['files'].items():
    file=P/name
    assert file.is_file(),name
    assert file.stat().st_size==record['bytes'],name
    digest=hashlib.sha256()
    with file.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024),b''):
            digest.update(chunk)
    assert digest.hexdigest()==record['sha256'],name
summary=json.loads((P/'consumers-final/summary.json').read_text())
assert summary['state']=='complete' and summary['total']==1013 and summary['counts']=={'ok':1012,'failed':1}
results=[json.loads(line) for line in (P/'consumers-final/results.jsonl').read_text().splitlines()]
assert len(results)==len({r['name'] for r in results})==1013
assert {r['binary_sha256'] for r in results}=={summary['binary_sha256']}
assert {r['name'] for r in results}==set(json.loads((P/'front-consumer-names.json').read_text()))
inventory={line[:-6] for line in (P/'candidate-test-inventory.txt').read_text().splitlines() if line.endswith(': test')}
assert len(inventory)==2441 and {r['name'] for r in results}<=inventory
logs=[json.loads(line) for line in (P/'unit-logs.jsonl').read_text().splitlines()]
assert len(logs)==1013
by_name={record['name']:record for record in results}
assert {record['name'] for record in logs}==set(by_name)
for record in logs:
    events=[]
    for line in record['stdout'].splitlines():
        try:
            events.append(json.loads(line))
        except ValueError:
            pass
    terminal=[event for event in events if event.get('type')=='test' and event.get('name')==record['name'] and event.get('event') in ['ok','failed','ignored']]
    assert len(terminal)==1 and terminal[0]['event']==by_name[record['name']]['status']
    if terminal[0]['event']=='ok':
        assert by_name[record['name']]['returncode']==0
failed=[r for r in results if r['status']!='ok']
assert len(failed)==1
assert failed[0]['name']=='evaluator::batch::tests::optional_eager_tests::optional_common_profiles_record_original_and_actual_admission'
assert 'OPTIONAL COMMON ORIGINAL 1713/657792/D19F5082E4AF34C5' in failed[0]['failure_excerpt']
assert 'assertion failed: output.status.success()' in failed[0]['failure_excerpt']
source=json.loads((P/'candidate-final-input.json').read_text())
assert source['main']['head']==manifest['source_commit']
published=json.loads((P/'source-publication.json').read_text())
assert published['source_commit']==published['remote_head_verified']==manifest['source_commit']
for name in ['front-build1','server-build1','lint1']:
    receipt=json.loads((P/(name+'.receipt.json')).read_text())
    assert receipt['state']=='complete' and receipt['returncode']==0,name
assert json.loads((P/'format-final.json').read_text())['returncode']==0
assert json.loads((P/'cleanup.json').read_text())['all_owned_services_stopped']
baseline=json.loads((P/'baseline-functional-summary.json').read_text())
assert baseline['state']=='complete' and baseline['total']==1013 and baseline['counts']==summary['counts']
assert set(json.loads((P/'baseline-consumer-names.json').read_text()))==set(by_name)
publication=json.loads((P/'baseline-git-publication.json').read_text())
assert publication['commit']==publication['remote_verified']=='2897e0289dc697678b8e558604a79cdd9b0c8d1c'
assert publication['source_commit']=='6765b4efa267025ab916d05f9040fab2ffe09b1c'
before=json.loads((P/'baseline-input.json').read_text())
assert before['main']['head']==publication['commit']
for name in ['sdk','native','protected_BUILD']:assert before[name]==source[name]
old_files=before['main']['files'];new_files=source['main']['files']
assert old_files.keys()==new_files.keys()
delta=json.loads((P/'input-delta.json').read_text())
assert {name for name in old_files if old_files[name]!=new_files[name]}==set(delta['changed_files'])
assert set(delta['changed_files'])==set(json.loads((P/'patch-inputs.json').read_text()))
assert len(delta['changed_files'])==2 and not delta['added_files'] and not delta['removed_files']
assert source==json.loads((P/'unit-after.json').read_text())==json.loads((P/'after-service-build.json').read_text())

counts=json.loads((P/'source-reduction.json').read_text())
assert counts['totals']['production']=={'added':2,'deleted':8,'net_reduction':6}
assert counts['totals']['test']=={'added':0,'deleted':0,'net_reduction':0}
completed=[json.loads(f.read_text()) for f in P.glob('measurement*.receipt.json') if json.loads(f.read_text()).get('returncode')==0]
assert len(completed)==1 and completed[0]['state']=='complete'
initial=json.loads((P/'measurement1.receipt.json').read_text())
if initial['returncode']!=0:
    from check_preflight_retry import compare_plan
    first=json.loads((P/'preflight-initial.json').read_text())
    proof=json.loads((P/'preflight-estRows-only-proof.json').read_text())
    assert proof['actual_failed_receipt']==initial and initial['state']=='complete'
    assert len(first['observations'])==56 and len(first['pairs'])==14
    refs={(r['mode'],r['key']):r for r in first['observations']};changes=[]
    for pair in first['pairs']:
        a=refs['native',pair['key']]
        for mode in ['current','retained_b','frozen']:
            b=refs[mode,pair['key']]
            assert 'error' not in a and 'error' not in b
            assert all(a[f]==b[f] for f in ['case_spec','rows','digest','fields','warnings','session_configuration','result_json_bytes','preview'])
            difference=compare_plan(a['explain'],b['explain'])
            if difference:changes.append(difference)
    assert len(changes)==len(first['failures'])==proof['failed_comparisons'] and changes
    assert sum(map(len,changes))==proof['changed_estRows_cells']
    restart=json.loads((P/'restart1.receipt.json').read_text())
    assert restart['state']=='complete' and restart['returncode']==0

# A packaging guard stopped attempt2 before it could open a frontend. Verify
# this separately rather than counting it as a semantic preflight or hiding it.
interruption=json.loads((P/'retry-interruption-proof.json').read_text())
blocked=json.loads((P/'measurement2.receipt.json').read_text())
assert interruption['blocked_receipt']==blocked and blocked['state']=='complete' and blocked['returncode']==1
stderr=(P/'measurement2.stderr.txt').read_bytes()
assert hashlib.sha256(stderr).hexdigest()==interruption['stderr_sha256']
assert b"assert not out.exists();prepare_environment()" in stderr and b'AssertionError' in stderr
preserved=(P/'preflight-initial.json').read_bytes()
assert preserved==(P/'preflight-interrupted-unchanged.json').read_bytes()
assert hashlib.sha256(preserved).hexdigest()==interruption['preserved_preflight_sha256']
from datetime import datetime, timezone
history=[json.loads(line) for line in (P/'runtime/process-history.jsonl').read_text().splitlines()]
assert not [h for h in history if h['name'] in ['native','current','retained_b','frozen'] and blocked['started']-1 <= datetime.strptime(h['started_utc'],'%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc).timestamp() <= blocked['ended']]
restart=json.loads((P/'restart2.receipt.json').read_text())
assert restart['state']=='complete' and restart['returncode']==0
assert json.loads((P/'measurement3.receipt.json').read_text())==completed[0]

from result_tables import render,directional
performance=json.loads((P/'summary.json').read_text())
assert (P/'all-result-tables.md').read_text()==render(performance)+'\n'
assert json.loads((P/'all-directional-results.json').read_text())==directional(performance)
raw=decode(json.loads((P/'cell-observations-deduplicated.json').read_text()))
assert hashlib.sha256(raw).hexdigest()==manifest['original_observation_log_sha256']
with tempfile.TemporaryDirectory(prefix='expression-results-') as temp:
    target=Path(temp)
    for name in ['audit.py','analyze.py','workloads.py','cases.py','preflight.json','primary.jsonl','summary.json','build-results.json','timing-interference-watch.json','candidate-final-input.json','build-after1.json']:
        shutil.copy2(P/name,target/name)
    (target/'cell-observations.jsonl').write_bytes(raw)
    subprocess.run([sys.executable,str(target/'audit.py')],cwd=target,check=True)
print('Git evidence hashes, 1013 current test records and fixed1013 baseline selection, build receipts, exact semantic observations and all timing cells PASS')
