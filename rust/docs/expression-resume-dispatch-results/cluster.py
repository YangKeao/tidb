from pathlib import Path
import subprocess,os,json,socket,time,hashlib,sys
BASE=Path(__file__).resolve().parent
RUNTIME=BASE/'runtime'
RUNTIME.mkdir(exist_ok=True)
PIDFILE=RUNTIME/'owned-processes.json'
PORTS={'pd':26379,'pd_peer':26380,'tikv':27160,'tikv_status':27180,'go':26000,'go_status':26080,'native':26001,'current':26002,'frozen':26003,'retained_b':26004}
BIN={
 'pd':Path('/home/agent/tidb/.scratch/statement-playground/tiup/components/pd/v9.0.0-beta.2.pre-nightly/pd-server'),
 'tikv':Path('/home/agent/tidb/expression-unification/target-tikv/release/tikv-server'),
 'go':Path('/home/agent/tidb/expression-unification/cluster-smoke/bin/tidb-server'),
 'native':Path('/home/agent/.slock/agents/02fef0a9-76ce-4afe-9bce-73eebe82ff7d/evidence/path-unification/control-result-cleanup/tidb-server-candidate'),
 'retained_b':Path('/home/agent/.slock/agents/02fef0a9-76ce-4afe-9bce-73eebe82ff7d/evidence/path-unification/control-result-cleanup/tidb-server-candidate'),
 'frozen':Path('/home/agent/tidb/expression-unification/target-perf-baseline/release/tidb-server'),
 'current':BASE/'tidb-server-candidate',
}
def records():return json.loads(PIDFILE.read_text()) if PIDFILE.exists() else {}
def birth(pid):
 try:
  stat=Path(f'/proc/{pid}/stat').read_text().split(') ',1)[1].split()
  return None if stat[0]=='Z' else stat[19]
 except FileNotFoundError:return None
def launch(name,args):
 owned=records()
 assert name not in owned or birth(owned[name]['pid'])!=owned[name]['birth'],name+' already live'
 binary=BIN[name];assert binary.is_file()
 stdout=(RUNTIME/(name+'.stdout.log')).open('ab')
 env=os.environ.copy();env['RUST_BACKTRACE']='1'
 p=subprocess.Popen([str(binary),*args],cwd=RUNTIME,stdout=stdout,stderr=subprocess.STDOUT,env=env,start_new_session=True);stdout.close()
 owned[name]={'pid':p.pid,'birth':birth(p.pid),'command':[str(binary),*args],'binary_sha256':hashlib.sha256(binary.read_bytes()).hexdigest(),'started_utc':time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime())}
 PIDFILE.write_text(json.dumps(owned,indent=2)+'\n')
 with (RUNTIME/'process-history.jsonl').open('a') as f:f.write(json.dumps({'name':name,**owned[name]})+'\n')
 return p

def free_ports(names):
 for name in names:
  s=socket.socket();s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1);s.bind(('127.0.0.1',PORTS[name]));s.close()

def wait_port(name,p,timeout=180):
 deadline=time.monotonic()+timeout
 while time.monotonic()<deadline:
  if p.poll() is not None:raise RuntimeError(name+' exited '+str(p.returncode))
  try:
   with socket.create_connection(('127.0.0.1',PORTS[name]),timeout=1):return
  except OSError:time.sleep(.25)
 raise TimeoutError(name)

def stop(name):
 import signal
 owned=records();item=owned.get(name)
 if not item:return
 pid=item['pid']
 if birth(pid)!=item['birth']:return
 os.kill(pid,signal.SIGTERM)
 for _ in range(100):
  if birth(pid)!=item['birth']:return
  time.sleep(.1)
 if birth(pid)==item['birth']:
  os.kill(pid,signal.SIGKILL)

def start_storage():
 free_ports(['pd','pd_peer','tikv','tikv_status','go','go_status','native','current','retained_b','frozen'])
 pdurl=f'http://127.0.0.1:{PORTS["pd"]}';peer=f'http://127.0.0.1:{PORTS["pd_peer"]}'
 p=launch('pd',['--name','expr-detailed-pd','--data-dir',str(RUNTIME/'pd-data'),'--client-urls',pdurl,'--advertise-client-urls',pdurl,'--peer-urls',peer,'--advertise-peer-urls',peer,'--initial-cluster','expr-detailed-pd='+peer,'--log-file',str(RUNTIME/'pd.log')]);wait_port('pd',p)
 config=RUNTIME/'tikv.toml';config.write_text('[server]\ngrpc-concurrency = 4\n[storage.block-cache]\ncapacity = "256MB"\n[readpool.unified]\nmin-thread-count = 2\nmax-thread-count = 4\n')
 p=launch('tikv',['--addr',f'127.0.0.1:{PORTS["tikv"]}','--advertise-addr',f'127.0.0.1:{PORTS["tikv"]}','--status-addr',f'127.0.0.1:{PORTS["tikv_status"]}','--pd',f'127.0.0.1:{PORTS["pd"]}','--data-dir',str(RUNTIME/'tikv-data'),'--config',str(config),'--log-file',str(RUNTIME/'tikv.log')]);wait_port('tikv',p)
 print('own loopback PD/TiKV ready; no timing started')

def bootstrap():
 import pymysql
 from cases import SCHEMA,ROWS,row,DATABASE
 free_ports(['go','go_status'])
 config=RUNTIME/'go.toml';config.write_text('[performance]\nstats-lease = "0"\n')
 p=launch('go',['--store','tikv','--path',f'127.0.0.1:{PORTS["pd"]}','--host','127.0.0.1','-P',str(PORTS['go']),'--status',str(PORTS['go_status']),'--lease','1s','--config',str(config),'--log-file',str(RUNTIME/'go.log')]);wait_port('go',p,300)
 conn=None
 for _ in range(300):
  if p.poll() is not None:raise RuntimeError('bootstrap Go exited')
  try:
   conn=pymysql.connect(host='127.0.0.1',port=PORTS['go'],user='root',password='',autocommit=True,charset='utf8mb4');break
  except pymysql.MySQLError:time.sleep(1)
 assert conn is not None
 with conn.cursor() as cursor:
  for stmt in SCHEMA.split(';'):
   if stmt.strip():cursor.execute(stmt)
  sql=f'INSERT INTO {DATABASE}.t VALUES ('+','.join(['%s']*12)+')'
  for start in range(0,ROWS,512):cursor.executemany(sql,[row(i) for i in range(start,min(start+512,ROWS))])
  cursor.execute(f'SELECT COUNT(*),SUM(dense),COUNT(nullable),COUNT(sparse) FROM {DATABASE}.t');counts=cursor.fetchone();assert counts[0]==ROWS
  cursor.execute(f"SELECT TIDB_TABLE_ID FROM information_schema.tables WHERE table_schema='{DATABASE}' AND table_name='t'");tableid=cursor.fetchone()[0]
 (RUNTIME/'fixture.json').write_text(json.dumps({'database':DATABASE,'table':'t','rows':ROWS,'counts':[str(x) for x in counts],'table_id':tableid,'generator_sha256':hashlib.sha256((BASE/'cases.py').read_bytes()).hexdigest()},indent=2)+'\n');conn.close();stop('go');print('own SQL fixture created and Go bootstrap stopped:',ROWS,'rows')

def start_frontend(name):
 from cases import DATABASE,TABLE
 assert name in ('native','current','retained_b','frozen')
 free_ports([name])
 config=RUNTIME/(name+'.toml');config.write_text('[performance]\nstats-lease = "0"\n')
 p=launch(name,['--path',f'127.0.0.1:{PORTS["pd"]}','--store','tikv','--host','127.0.0.1','-P',str(PORTS[name]),'--cluster-session','--load-privileges','--no-auto-tls','--affinity-cpus','2,3,4,5','--max-connections','64','--connection-timeout-ms','120000','--max-topn-rows','16384']);wait_port(name,p,180)
 print(name,'Rust SQL endpoint ready; release endpoint ready')

if __name__=='__main__':
 if sys.argv[1]=='storage':start_storage()
 elif sys.argv[1]=='bootstrap':bootstrap()
 elif sys.argv[1] in ('native','current','retained_b','frozen'):start_frontend(sys.argv[1])
 elif sys.argv[1]=='stop':
  for name in ['native','current','retained_b','frozen','go','tikv','pd']:stop(name)
 else:raise ValueError(sys.argv[1])
