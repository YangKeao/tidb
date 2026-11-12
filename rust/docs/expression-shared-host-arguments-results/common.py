"""Shared measurement primitives, with no work at import for spawned clients."""
from pathlib import Path
import os,json,time,hashlib,subprocess
from cluster import BASE,records,birth,start_frontend,stop
from functional import encode,metadata,connect
from workloads import CONFIGS
HZ=os.sysconf('SC_CLK_TCK')
SERVER_CORES=[2,3,4,5]
def write(name,value):(BASE/name).write_text(json.dumps(value,indent=2,ensure_ascii=False)+'\n')
from process_cpu_clock import cpu_ns,resolution_ns
def cpu(pid):return cpu_ns(pid)/1e9
def memory(pid):
 status={k:v.strip() for l in Path(f'/proc/{pid}/status').read_text().splitlines() if ':' in l for k,v in [l.split(':',1)]}
 return {k:int(status.get(k,'0 kB').split()[0]) for k in ['VmRSS','VmHWM','VmSwap']}
def cores():
 return {f[0]:(sum(map(int,f[1:9])),int(f[4])+int(f[5])) for l in Path('/proc/stat').read_text().splitlines() if (f:=l.split())[0] in {f'cpu{v}' for v in SERVER_CORES}}
def busy(before,after):return {k:1-(after[k][1]-v[1])/(after[k][0]-v[0]) if after[k][0]>v[0] else None for k,v in before.items()}
def pin(name,allowed):
 item=records()[name];assert birth(item['pid'])==item['birth']
 for tid in Path(f'/proc/{item["pid"]}/task').iterdir():
  try:os.sched_setaffinity(int(tid.name),set(allowed))
  except ProcessLookupError:pass
 actual={}
 for tid in Path(f'/proc/{item["pid"]}/task').iterdir():
  try:actual[tid.name]=sorted(os.sched_getaffinity(int(tid.name)))
  except ProcessLookupError:pass
 assert actual and all(set(v)<=set(allowed) for v in actual.values()),('affinity mismatch',name,actual)
 with (BASE/'runtime/affinity-history.jsonl').open('a') as log:
  log.write(json.dumps({'mode':name,'pid':item['pid'],'birth':item['birth'],'time':time.time(),'allowed':allowed,'actual':actual})+'\n')
def guard():
 from snapshot import snapshot
 expected=json.loads((BASE/'build-after1.json').read_text())
 assert snapshot()==expected,'measured source/status changed'
 procs=subprocess.check_output(['ps','-eo','comm,args'],text=True).splitlines()
 assert not any(l.split()[0] in ['rustc','cargo','cc1','cc1plus','lto1'] for l in procs if l.split()),'compiler active'
 assert not any(l.split()[0].startswith(('tidb_expr-','tidb_query_expr-','tidb_executor-','tidb_unistore-','front-build','sdk-build','front-unit','sdk-unit')) for l in procs if l.split()),'test binary active'
 assert not any(l.split()[0]=='perf' for l in procs if l.split()),'perf active during timing'
 for item in json.loads((BASE/'build-results.json').read_text()):
  with Path(item['binary']).open('rb') as f:assert hashlib.file_digest(f,'sha256').hexdigest()==item['binary_sha256']
def prepare_environment():
 guard();pin('tikv',[6,7]);pin('pd',[8,9]);os.sched_setaffinity(0,{10})
def start(mode,settle=3):
 for peer in ['native','current','retained_b','frozen']:stop(peer)
 t=time.monotonic();start_frontend(mode);time.sleep(settle)
 pin(mode,SERVER_CORES)
 return records()[mode]['pid'],time.monotonic()-t
def configure(cursor,config):
 size,vec=CONFIGS[config];initial=min(size,32)
 for sql in [f'SET tidb_init_chunk_size={initial}',f'SET tidb_max_chunk_size={size}',f'SET tidb_enable_vectorized_expression={vec}',"SET time_zone='+00:00'"]:cursor.execute(sql)
 return read_configuration(cursor,config)
def read_configuration(cursor,config):
 size,vec=CONFIGS[config];initial=min(size,32)
 cursor.execute('SELECT @@tidb_init_chunk_size,@@tidb_max_chunk_size,@@tidb_enable_vectorized_expression')
 actual=cursor.fetchone();assert [int(v) for v in actual]==[initial,size,vec],('session configuration did not take effect',actual,size,vec)
 return encode(actual)
def digest(data):return hashlib.sha256(json.dumps(encode(data),ensure_ascii=False,separators=(',',':')).encode()).hexdigest()
def observe(cursor,case):
 cursor.execute(case['sql']);data=cursor.fetchall();fields=metadata(cursor)
 assert case['rows'] is None or len(data)==case['rows'],(case,len(data))
 cursor.execute('SHOW WARNINGS');warnings=cursor.fetchall()
 return {'rows':len(data),'digest':digest(data),'fields':fields,'warnings':encode(warnings),'result_json_bytes':len(json.dumps(encode(data),ensure_ascii=False).encode()),'preview':encode(data[:2])}
def clock_cell(pid,execute,minimum_seconds=2.0,minimum_queries=30):
 before=cores();used0=cpu_ns(pid);client0=time.process_time();m0=memory(pid);t0=time.perf_counter();latency=[]
 while len(latency)<minimum_queries or time.perf_counter()-t0<minimum_seconds:
  begin=time.perf_counter_ns();execute();latency.append((time.perf_counter_ns()-begin)/1e6)
 wall=time.perf_counter()-t0;used=cpu_ns(pid)-used0
 return {'latency_ms':latency,'wall_seconds':wall,'server_cpu_seconds':used/1e9,'server_cpu_nanoseconds':used,'server_cpu_clock':'POSIX process CPU clock via clock_getcpuclockid','server_cpu_clock_resolution_ns':resolution_ns(pid),'client_cpu_seconds':time.process_time()-client0,'actual_core_busy_fraction':busy(before,cores()),'memory_before_kib':m0,'memory_after_kib':memory(pid),'time_utc':time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime())}
def binary(mode):return records()[mode]['binary_sha256']
