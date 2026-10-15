import os, subprocess
from pathlib import Path
r=Path(__file__).resolve().parent
env=dict(os.environ,EXPR_BATCH_PERF_INT_TYPE='longlong',EXPR_BATCH_PERF_BATCH_SIZE='1024',EXPR_BATCH_PERF_SAMPLES='7',EXPR_BATCH_PERF_TARGET_NS='150000000',EXPR_BATCH_PERF_MODE='batch_tikv',RUST_MIN_STACK='33554432')
for key in ['EXPR_BATCH_PERF_INT_UNSIGNED','EXPR_PERF_REAL_DOMAIN']:env.pop(key,None)
for pair in range(1,4):
 for mode in (['after','before'] if pair%2 else ['before','after']):
  with (r/f'regression-{pair}-{mode}.log').open('w') as out:
   for name in ['any_value_datetime','any_value_duration','any_value_json']:
    subprocess.run(['taskset','-c','2',str(r/f'{mode}-bin'),'tests::lane_cache_batch_performance::lane_cache_batch_release_performance_probe','--ignored','--exact','--nocapture','--test-threads=1'],env=dict(env,EXPR_BATCH_PERF_WORKLOAD=name),stdout=out,stderr=subprocess.STDOUT,check=True)
 print(f'completed longer control pair {pair}',flush=True)
