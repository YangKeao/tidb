"""Bind new checkpoint evidence to its own source, tests and service binary."""
from pathlib import Path
import json,hashlib,subprocess,shutil
from snapshot import snapshot,write
P=Path(__file__).resolve().parent
OLD=P.parent/'shared-host-arguments/overnight'
load=lambda name:json.loads((P/name).read_text())
for name in ['front-build2','server-build1','lint1']:
 r=load(name+'.receipt.json');assert r['state']=='complete' and r['returncode']==0,(name,r)
s=load('consumers-final/summary.json')
assert s['state']=='complete' and s['total']==1013 and s['counts']=={'ok':1012,'failed':1},s
records=[json.loads(l) for l in (P/'consumers-final/results.jsonl').read_text().splitlines()]
assert len(records)==len({r['name'] for r in records})==1013
failed=[r for r in records if r['status']!='ok']
old_failed=[json.loads(l) for l in (OLD/'consumers-final/results.jsonl').read_text().splitlines() if json.loads(l)['status']!='ok']
assert len(failed)==len(old_failed)==1 and failed[0]['name']==old_failed[0]['name']
for r in [failed[0],old_failed[0]]:
 assert 'OPTIONAL COMMON ORIGINAL 1713/657792/D19F5082E4AF34C5' in r['failure_excerpt']
 assert 'assertion failed: output.status.success()' in r['failure_excerpt']
assert load('format-final.json')['returncode']==0
actual=snapshot();assert actual==load('candidate-final-input.json')
base=json.loads((P.parent/'unused-private-helpers/baseline-input.json').read_text())
for key in ['sdk','native','protected_BUILD']:assert actual[key]==base[key],key
write('build-after1.json',actual)
root=Path('/home/agent/tidb/expression-unification/tidb')
subprocess.run(['git','diff','--check'],cwd=root,check=True)
pinned=P/'tidb-server-candidate';assert not pinned.exists();shutil.copy2(root.parent/'target-tidb/release/tidb-server',pinned)
assert hashlib.sha256(pinned.read_bytes()).hexdigest()==load('service-built.json')['sha256']
from cluster import BIN
binaries=[{'name':name,'binary':str(BIN[name]),'binary_sha256':hashlib.sha256(BIN[name].read_bytes()).hexdigest()} for name in ['native','retained_b','current','frozen','tikv']]
assert binaries[0]['binary_sha256']==binaries[1]['binary_sha256']=='783723c09121c6e64512586a2fbe43b8fa9aa30a1d7983e7ce15973fb1048629'
write('build-results.json',binaries)
proof=json.loads((OLD/'sdk-inheritance-proof.json').read_text());assert proof['tested_sdk_files_equal_current'];write('sdk-inheritance-proof.json',proof)
write('functional-gates-summary.json',{'state':'complete','source_commit':actual['main']['head'],'main_consumers':s['counts'],'main_test_inventory':2441,'full2441_NOT_executed':True,'unit_source_equals_server_source':True,'unit_binary_sha256':s['binary_sha256'],'SDK_newly_tested':False,'SDK_inherited_exact1574inputs':'sdk-inheritance-proof.json','format':'edition2021 check0','lint':'make lint actual0','diffcheck':0,'allocation_failure_injection_NOT_run':True,'known_failure':failed[0],'additional_baseline_tests':{'AST_control_diagnostics':16,'arithmetic_rewriter':2,'all_pass_on_pinned85':True},'performance_acceptance':'pending cumulative14-scene comparison against pinned43ba and frozen original'})
print('Cumulative functional gates complete; performance comparison pending')
