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
    assert hashlib.file_digest(file.open('rb'),'sha256').hexdigest()==record['sha256'],name
summary=json.loads((P/'consumers-final/summary.json').read_text())
assert summary['state']=='complete' and summary['total']==995 and summary['counts']=={'ok':994,'failed':1}
results=[json.loads(line) for line in (P/'consumers-final/results.jsonl').read_text().splitlines()]
assert len(results)==len({r['name'] for r in results})==995
assert {r['binary_sha256'] for r in results}=={summary['binary_sha256']}
logs=[json.loads(line) for line in (P/'unit-logs.jsonl').read_text().splitlines()]
assert len(logs)==995
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
assert source['main']['head']=='43ba91c201769b0d7d9930e2da3488870e95ca6f'
for name in ['front-build2','server-build1','lint1']:
    receipt=json.loads((P/(name+'.receipt.json')).read_text())
    assert receipt['state']=='complete' and receipt['returncode']==0,name
assert json.loads((P/'front-build1.receipt.json').read_text())['returncode']==101
assert json.loads((P/'format-final.json').read_text())['returncode']==0
assert json.loads((P/'cleanup.json').read_text())['all_owned_services_stopped']
from result_tables import render,directional
performance=json.loads((P/'summary.json').read_text())
assert (P/'all-result-tables.md').read_text()==render(performance)+'\n'
assert json.loads((P/'all-directional-results.json').read_text())==directional(performance)
raw=decode(json.loads((P/'cell-observations-deduplicated.json').read_text()))
with tempfile.TemporaryDirectory(prefix='expression-results-') as temp:
    target=Path(temp)
    for name in ['audit.py','analyze.py','workloads.py','cases.py','preflight.json','primary.jsonl','summary.json','build-results.json','timing-interference-watch.json','candidate-final-input.json','build-after1.json']:
        shutil.copy2(P/name,target/name)
    (target/'cell-observations.jsonl').write_bytes(raw)
    subprocess.run([sys.executable,str(target/'audit.py')],cwd=target,check=True)
print('Git evidence hashes, 995 test records, build receipts, exact semantic observations and all timing cells PASS')
