import os,subprocess,json
from pathlib import Path
r=Path(__file__).resolve().parent
core=['integer_add','integer_add_literal','abs','lazy_if','coalesce_nullif','strcmp','lower','upper','concat','substring','decimal_add','json_type','regexp_like']
env=dict(os.environ,RUST_MIN_STACK='33554432',EXPR_BATCH_PERF_SAMPLES='7',EXPR_BATCH_PERF_TARGET_NS='75000000',EXPR_BATCH_PERF_SCENARIO='dense',EXPR_BATCH_PERF_MODE='batch_tikv',EXPR_BATCH_PERF_INT_TYPE='longlong',EXPR_BATCH_PERF_BATCH_SIZE='1024')
for k in ['EXPR_BATCH_PERF_INT_UNSIGNED','EXPR_PERF_REAL_DOMAIN','EXPR_NATIVE_PERF_WORKLOAD']:env.pop(k,None)
for pair in range(1,4):
 with (r/f'initial-compare-{pair}-initial.log').open('w') as before,(r/f'initial-compare-{pair}-current.log').open('w') as after:
  for name in core:
   for mode in (['initial','current'] if pair%2 else ['current','initial']):
    subprocess.run(['taskset','-c','2',str(r/('initial-matched-bin' if mode=='initial' else 'current-bin')),'tests::lane_cache_batch_performance::lane_cache_batch_release_performance_probe','--ignored','--exact','--nocapture','--test-threads=1'],env=dict(env,EXPR_BATCH_PERF_WORKLOAD=name),stdout=before if mode=='initial' else after,stderr=subprocess.STDOUT,check=True)
 print(f'initial/final comparison pair {pair} complete',flush=True)
