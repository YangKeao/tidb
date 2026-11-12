"""Resume a same-binary diagnostic with bounded independent test processes."""
import concurrent.futures as futures,collections,hashlib,json,os,pathlib,signal,subprocess,sys,threading,time
binary=pathlib.Path(sys.argv[1]).resolve();out=pathlib.Path(sys.argv[2]).resolve();expected=sys.argv[3]
assert hashlib.file_digest(binary.open('rb'),'sha256').hexdigest()==expected
out.mkdir(exist_ok=True);lock=threading.Lock();active={};started=time.time()
def birth(pid):
 try:return pathlib.Path(f'/proc/{pid}/stat').read_text().split(') ',1)[1].split()[19]
 except FileNotFoundError:return None
def save_active():
 temp=out/'active.tmp';temp.write_text(json.dumps({'binary_sha256':expected,'updated':time.time(),'processes':list(active.values())},indent=2));temp.replace(out/'active.json')
def cancelled(sig,frame):
 with lock:
  for record in active.values():
   if birth(record['pid'])==record['birth']:
    try:os.killpg(record['pid'],signal.SIGKILL)
    except ProcessLookupError:pass
  (out/'parallel-interrupted.json').write_text(json.dumps({'signal':sig,'time':time.time(),'active':list(active.values())},indent=2))
 os._exit(128+sig)
signal.signal(signal.SIGTERM,cancelled);signal.signal(signal.SIGINT,cancelled)
listed=subprocess.run([str(binary),'--list'],capture_output=True,text=True,check=True).stdout
names=[line.removesuffix(': test') for line in listed.splitlines() if line.endswith(': test')];assert names and len(names)==len(set(names))
names=[name for name in names if name in set(json.loads((pathlib.Path(__file__).parent/'front-consumer-names.json').read_text()))]
assert len(names)==1013
results=out/'results.jsonl';previous=[json.loads(l) for l in results.read_text().splitlines()] if results.exists() else []
assert len({d['index'] for d in previous})==len(previous)
for d in previous:assert names[d['index']]==d['name']
completed={d['index'] for d in previous};counts=collections.Counter(d['status'] for d in previous)
serial=out/'progress.json'
if serial.exists() and not (out/'serial-progress.json').exists():(out/'serial-progress.json').write_text(serial.read_text())
BIG_MEMORY={'tikv::digest_value_tests::digest_actual_original_scalar_observations','tikv::eager_scalar_value_tests::eager_value_kernel_matches_frozen_observations'}
def one(index):
 name=names[index];memory_limit=(4<<30) if name in BIG_MEMORY else (768<<20);log=out/f'{index:04d}.jsonl';err=out/f'{index:04d}.stderr'
 for path in [log,err]:
  if path.exists():path.replace(path.with_name(path.name+'.serial-interrupted'))
 begin=time.time();timed_out=False
 with log.open('wb') as stdout,err.open('wb') as stderr:
  child=subprocess.Popen(['/usr/bin/prlimit',f'--as={memory_limit}:{memory_limit}','--core=0:0','--',str(binary),'--exact',name,'--test-threads=1','--format=json','-Z','unstable-options'],stdin=subprocess.DEVNULL,stdout=stdout,stderr=stderr,start_new_session=True)
  with lock:active[index]={'index':index,'name':name,'pid':child.pid,'birth':birth(child.pid)};save_active()
  try:code=child.wait(timeout=1800)
  except subprocess.TimeoutExpired:timed_out=True;os.killpg(child.pid,signal.SIGKILL);code=child.wait()
  finally:
   with lock:active.pop(index,None);save_active()
 events=[]
 for line in log.read_text(errors='replace').splitlines():
  try:events.append(json.loads(line))
  except json.JSONDecodeError:pass
 event=next((e for e in events if e.get('type')=='test' and e.get('name')==name and e.get('event') in ['ok','failed','ignored']),None)
 suite=next((e for e in reversed(events) if e.get('type')=='suite' and e.get('event') in ['ok','failed']),None)
 status='timeout' if timed_out else ('aborted' if code<0 else (event['event'] if event else 'no_terminal_event'))
 item={'index':index,'name':name,'returncode':code,'status':status,'wall_seconds':time.time()-begin,'suite':suite,'log':str(log),'stderr':str(err),'diagnostic_workers':8,'test_only_memory_limit_bytes':memory_limit,'test_only_timeout_seconds':1800,'binary_sha256':expected}
 if event and event.get('stdout'):item['failure_excerpt']=event['stdout'][:6000]
 return item
with futures.ThreadPoolExecutor(max_workers=8) as pool:
 pending={pool.submit(one,i):i for i in range(len(names)) if i not in completed}
 for future in futures.as_completed(pending):
  item=future.result();assert item['index'] not in completed;completed.add(item['index']);counts[item['status']]+=1
  with results.open('a') as f:f.write(json.dumps(item)+'\n')
  progress={'completed':len(completed),'total':len(names),'counts':dict(counts),'started':started,'updated':time.time(),'last':item['name'],'binary':str(binary),'binary_sha256':expected,'test_only_memory_limit_bytes':768<<20,'test_only_timeout_seconds':1800,'large_native_corpus_memory_overrides':{name:4<<30 for name in sorted(BIG_MEMORY)},'workers':8,'scope':'Explicit scoped frontend consumer list, each from one pinned binary in its own memory-bounded process. Timeouts or absent terminal events are failures of verification, never passes.'}
  (out/'progress.json').write_text(json.dumps(progress,indent=2))
assert len(completed)==len(names)
progress.update(state='complete',ended=time.time());(out/'summary.json').write_text(json.dumps(progress,indent=2));print(json.dumps(progress))
