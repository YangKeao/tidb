"""Machine observations; requires an agent's fresh directive read, never treats clock alone as quiet."""
from pathlib import Path
import json,time,os,sys,argparse
P=Path(__file__).resolve().parent
parser=argparse.ArgumentParser();parser.add_argument('--latest-read-message',required=True);args=parser.parse_args()
assert json.loads((P/'functional-gates-summary.json').read_text())['state']=='complete'
def cores():
 return {f[0]:(sum(map(int,f[1:9])),int(f[4])+int(f[5])) for l in Path('/proc/stat').read_text().splitlines() if (f:=l.split())[0] in {f'cpu{v}' for v in [2,3,4,5]}}
def processes():
 out={}
 for d in Path('/proc').iterdir():
  if not d.name.isdigit():continue
  try:
   stat=(d/'stat').read_text().split(') ',1)[1].split();comm=(d/'comm').read_text().strip();out[(d.name,stat[19])]={'comm':comm,'ticks':int(stat[11])+int(stat[12])}
  except (FileNotFoundError,ProcessLookupError,PermissionError):continue
 return out
started=time.time();a=cores();old=processes();samples=[]
for _ in range(16):
 time.sleep(1);now=processes();bad=[v['comm'] for v in now.values() if v['comm'] in {'go','pytest','cargo','rustc','cc1','cc1plus','lto1','perf','compile','asm','clang','clang++','gcc','g++'} or v['comm'].startswith(('front-unit','sdk-unit','tidb_expr-','tidb_query_expr-','tidb_query_datat'))];samples.append({'time':time.time(),'known_build_test_perf_processes':bad})
b=cores();new=processes();hz=os.sysconf('SC_CLK_TCK');busy={k:1-(b[k][1]-v[1])/(b[k][0]-v[0]) for k,v in a.items()};active=[{'pid':k[0],'birth':k[1],'comm':v['comm'],'cpu_seconds':(v['ticks']-old[k]['ticks'])/hz} for k,v in new.items() if k in old and (v['ticks']-old[k]['ticks'])/hz>.5]
result={'started':started,'checked':time.time(),'latest_directive_read_message':args.latest_read_message,'constraint_source':'843fd7fa and 16415fea: record environment noise and finish final comparison; no compiler, tests or profiler during timing','server_core_busy_fraction':busy,'globally_active_processes_over_half_CPU_second':active,'samples':samples,'scope':'16-second observations plus fresh directive read; ongoing timing still needs interference guards and scan controls, not a guarantee against future bursts.'}
result['quiet_observed']=all(v<=.05 for v in busy.values()) and not active and not any(s['known_build_test_perf_processes'] for s in samples)
result['measurement_permitted']=not any(s['known_build_test_perf_processes'] for s in samples)
(P/'quiet-window-observation.json').write_text(json.dumps(result,indent=2)+'\n');assert result['measurement_permitted'],result
print('Load recorded; quiet:',result['quiet_observed'],'no build/test/profiler observed; comparison permitted by latest directive')
