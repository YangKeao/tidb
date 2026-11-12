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
for name in ['front-build1','front-build2','server-build1','lint1','measurement2','restart1']:
    receipt=json.loads((P/(name+'.receipt.json')).read_text())
    assert receipt['state']=='complete' and receipt['returncode']==0,name
assert json.loads((P/'format-final.json').read_text())['returncode']==0
assert json.loads((P/'cleanup.json').read_text())['all_owned_services_stopped']
initial=json.loads((P/'measurement1.receipt.json').read_text())
assert initial['state']=='complete' and initial['returncode']==1
from check_preflight_retry import compare_plan
first=json.loads((P/'preflight-initial.json').read_text())
refs={(o['mode'],o['key']):o for o in first['observations']}
changes=[]
for pair in first['pairs']:
    a=refs['native',pair['key']]
    for mode in ['current','retained_b','frozen']:
        b=refs[mode,pair['key']]
        assert 'error' not in a and 'error' not in b
        assert all(a[f]==b[f] for f in ['case_spec','rows','digest','fields','warnings','session_configuration','result_json_bytes','preview'])
        difference=compare_plan(a['explain'],b['explain'])
        if difference:changes.append(difference)
assert len(changes)==len(first['failures'])==39 and sum(map(len,changes))==114
# Independently inspect the preserved intermediate checkpoint and added baselines.
intermediate=P/'intermediate85'
old=json.loads((intermediate/'consumers-final/summary.json').read_text())
assert old['state']=='complete' and old['total']==995 and old['counts']=={'ok':994,'failed':1}
for directory,logname,count in [('consumers-final','unit-logs.jsonl',995),('extra-baseline/results','extra-baseline-unit-logs.jsonl',16)]:
    records=[json.loads(line) for line in (intermediate/directory/'results.jsonl').read_text().splitlines()]
    rawlogs=[json.loads(line) for line in (intermediate/logname).read_text().splitlines()]
    assert len(records)==len(rawlogs)==count
    named={r['name']:r for r in records}
    assert len(named)==count and {r['name'] for r in rawlogs}==set(named)
    for record in rawlogs:
        events=[]
        for line in record['stdout'].splitlines():
            try: events.append(json.loads(line))
            except ValueError: pass
        terminal=[e for e in events if e.get('type')=='test' and e.get('name')==record['name'] and e.get('event') in ['ok','failed','ignored']]
        assert len(terminal)==1 and terminal[0]['event']==named[record['name']]['status']
        if terminal[0]['event']=='ok': assert named[record['name']]['returncode']==0
    assert {r['name'] for r in records if r['status']!='ok'}==({failed[0]['name']} if count==995 else set())
assert json.loads((intermediate/'candidate-final-input.json').read_text())==json.loads((intermediate/'build-after1.json').read_text())
for name in ['front-build1','front-build2','server-build1','lint1']:
    receipt=json.loads((intermediate/(name+'.receipt.json')).read_text())
    assert receipt['state']=='complete' and receipt['returncode']==0
for index,record in enumerate(json.loads((P/'rewriter-baseline/summary.json').read_text())):
    assert record['returncode']==0
    events=[json.loads(line) for line in (P/('rewriter-baseline/'+str(index)+'.stdout')).read_text().splitlines() if line.startswith('{')]
    assert any(e.get('type')=='test' and e.get('name')==record['name'] and e.get('event')=='ok' for e in events)
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
print('Git evidence hashes, 1013 current and995 intermediate test records, build receipts, exact semantic observations and all timing cells PASS')
