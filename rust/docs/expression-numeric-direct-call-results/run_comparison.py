"""Continue after terminal gates; never profile/build during paired timing."""
from pathlib import Path
import json,time,subprocess,sys,os
from cluster import BASE,records,birth,stop,free_ports
F=Path(__file__).resolve().parent
def state(phase,**extra):(F/'comparison-state.json').write_text(json.dumps({'phase':phase,'pid':os.getpid(),'updated':time.time(),**extra},indent=2))
state('require completed functional gates and fresh load observations with no concurrent build/test/profiler')
try:
 assert json.loads((F/'functional-gates-summary.json').read_text())['state']=='complete'
 quiet=json.loads((F/'quiet-window-observation.json').read_text());assert quiet['measurement_permitted'] and time.time()-quiet['checked']<300,'need a fresh actual load/directive check'
 from common import guard
 guard();state('PREFLIGHT')
 subprocess.run([sys.executable,str(F/'compare.py'),'preflight'],check=True)
 state('FOURLABEL224')
 import threading
 os.sched_setaffinity(0,{10})
 watch_stop=threading.Event();interference=[]
 def watch():
  while not watch_stop.wait(1):
   found=[]
   for d in Path('/proc').iterdir():
    if not d.name.isdigit():continue
    try:comm=(d/'comm').read_text().strip()
    except (FileNotFoundError,ProcessLookupError,PermissionError):continue
    if comm in {'go','pytest','cargo','rustc','cc1','cc1plus','lto1','perf','compile','asm','clang','clang++','gcc','g++'} or comm.startswith(('front-unit','sdk-unit','tidb_expr-','tidb_query_expr-','tidb_query_datat')):found.append({'pid':d.name,'comm':comm})
   if found:interference.append({'time':time.time(),'processes':found})
 watcher=threading.Thread(target=watch);watcher.start()
 try:subprocess.run([sys.executable,str(F/'compare.py'),'measure'],check=True)
 finally:
  watch_stop.set();watcher.join();(F/'timing-interference-watch.json').write_text(json.dumps({'events':interference,'scope':'one-second sampling of known build/test/perf process names; idle/core controls still required, not a guarantee against every possible external activity'},indent=2))
 assert not interference,'external build/test/perf observed during timing; preserve raw data, do not accept'

 state('ANALYZE')
 subprocess.run([sys.executable,str(F/'analyze.py')],check=True)
 from audit import main as audit
 audit();state('COMPARISON COMPLETE')
except BaseException as error:
 state('FAILED',error=repr(error));raise
finally:
 for mode in ['native','current','retained_b','frozen','go','tikv','pd']:stop(mode)
 free_ports(['pd','pd_peer','tikv','tikv_status','go','go_status','native','current','retained_b','frozen'])
 (F/'services-stopped.json').write_text(json.dumps({'all_owned_services_stopped':True,'ports_free':True,'checked':time.time(),'records':records()},indent=2))
