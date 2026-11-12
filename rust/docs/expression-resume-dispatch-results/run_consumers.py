from pathlib import Path
import json,subprocess,hashlib,shutil,sys
p=Path(__file__).resolve().parent
assert json.loads((p/'front-build1.receipt.json').read_text())['returncode']==0
artifacts=[json.loads(l) for l in (p/'front-build1.stdout.log').read_text().splitlines() if l.startswith('{')];a=[x for x in artifacts if x.get('reason')=='compiler-artifact' and x.get('executable') and x['target']['name']=='tidb_expr'];assert len(a)==1
binary=p/'front-unit-candidate';assert not binary.exists();shutil.copy2(a[0]['executable'],binary);sha=hashlib.file_digest(binary.open('rb'),'sha256').hexdigest();(p/'front-unit-candidate.json').write_text(json.dumps({'artifact':a[0],'binary':str(binary),'sha256':sha},indent=2))
listed=subprocess.check_output([str(binary),'--list'],text=True);(p/'candidate-test-inventory.txt').write_text(listed);names=[l.removesuffix(': test') for l in listed.splitlines() if l.endswith(': test')];old=[l.removesuffix(': test') for l in (p/'baseline-test-inventory.txt').read_text().splitlines() if l.endswith(': test')];print('inventory',len(names),'new',set(names)-set(old),'missing',set(old)-set(names),flush=True);assert len(names)==2441 and set(old)<=set(names)
selected=set(json.loads((p/'front-consumer-names.json').read_text()));assert selected<=set(names),selected-set(names)
subprocess.run([sys.executable,str(p/'front_consumers.py'),str(binary),str(p/'consumers-final'),sha],check=True)
