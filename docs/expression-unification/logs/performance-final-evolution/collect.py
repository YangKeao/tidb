import os,subprocess,json
from pathlib import Path
r=Path(__file__).resolve().parent
workloads=json.loads((r/'workloads.json').read_text());names=[n for n,sql in workloads]
core=['integer_add','integer_add_literal','abs','lazy_if','coalesce_nullif','strcmp','lower','upper','concat','substring','decimal_add','json_type','regexp_like']
assert set(core)<=set(names)
cells=[(n,size) for n in core for size in [1,8,64,256,1024]]+[(n,1024) for n in names if n not in core]
assert len(cells)==116
(r/'cells.json').write_text(json.dumps(cells,indent=2)+'\n')
env=dict(os.environ,RUST_MIN_STACK='33554432',EXPR_BATCH_PERF_SAMPLES='7',EXPR_BATCH_PERF_TARGET_NS='75000000',EXPR_BATCH_PERF_SCENARIO='dense',EXPR_BATCH_PERF_MODE='batch_tikv',EXPR_BATCH_PERF_INT_TYPE='longlong')
for k in ['EXPR_BATCH_PERF_INT_UNSIGNED','EXPR_PERF_REAL_DOMAIN','EXPR_BATCH_PERF_WORKLOAD','EXPR_NATIVE_PERF_WORKLOAD','EXPR_LANE_PERF_ITERATION_MULTIPLIER','EXPR_LANE_PERF_ITERATION_DIVISOR']:env.pop(k,None)
for pair in range(1,4):
 order=['current','native'] if pair%2 else ['native','current']
 with (r/f'pair-{pair}-current.log').open('w') as cur,(r/f'pair-{pair}-native.log').open('w') as nat:
  for i,(name,size) in enumerate(cells):
   e=dict(env,EXPR_BATCH_PERF_BATCH_SIZE=str(size))
   if name.endswith('_unsigned'):e['EXPR_BATCH_PERF_INT_UNSIGNED']='1'
   if name in ['real_asin','real_acos']:e['EXPR_PERF_REAL_DOMAIN']='inverse_trig'
   for mode in order:
    flag='EXPR_BATCH_PERF_WORKLOAD' if mode=='current' else 'EXPR_NATIVE_PERF_WORKLOAD'
    test='lane_cache_batch_performance::lane_cache_batch_release_performance_probe' if mode=='current' else 'original_native_all_carriers_performance::original_native_all_carriers_release_performance_probe'
    subprocess.run(['taskset','-c','2',str(r/f'{mode}-bin'),'tests::'+test,'--ignored','--exact','--nocapture','--test-threads=1'],env=dict(e,**{flag:name}),stdout=cur if mode=='current' else nat,stderr=subprocess.STDOUT,check=True)
   if (i+1)%20==0:print(f'pair{pair} batch {i+1}/{len(cells)}',flush=True)
 for mode in order:
  test='lane_cache_performance' if mode=='current' else 'final_native_width_one_performance'
  with (r/f'width-{pair}-{mode}.log').open('w') as out:
   subprocess.run(['taskset','-c','2',str(r/f'{mode}-bin'),f'tests::{test}::lane_cache_release_performance_probe','--ignored','--exact','--nocapture','--test-threads=1'],env=dict(env,EXPR_LANE_PERF_ITERATION_MULTIPLIER='16' if mode=='native' else '1'),stdout=out,stderr=subprocess.STDOUT,check=True)
 with (r/f'initial-{pair}.log').open('w') as out:
  subprocess.run(['taskset','-c','2',str(r/'initial-bin'),'tests::lane_cache_batch_performance::lane_cache_batch_release_performance_probe','--ignored','--exact','--nocapture','--test-threads=1'],env=dict(env,EXPR_BATCH_PERF_BATCH_SIZE='1024',EXPR_BATCH_PERF_WORKLOAD='integer_add'),stdout=out,stderr=subprocess.STDOUT,check=True)
 print(f'completed pair {pair}:116batch cells, width-one, actual initial unified PLUS',flush=True)
