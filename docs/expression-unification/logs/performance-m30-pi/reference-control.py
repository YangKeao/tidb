import os,subprocess
from pathlib import Path
root=Path(__file__).resolve().parent
env=dict(os.environ,EXPR_BATCH_PERF_INT_TYPE='longlong',EXPR_BATCH_PERF_BATCH_SIZE='1024',EXPR_BATCH_PERF_SAMPLES='7',EXPR_BATCH_PERF_TARGET_NS='75000000',RUST_MIN_STACK='33554432')
env.pop('EXPR_BATCH_PERF_INT_UNSIGNED',None)
env.pop('EXPR_PERF_REAL_DOMAIN',None)
for pair in range(1,4):
 for mode in (['after','int-reference'] if pair%2 else ['int-reference','after']):
  with (root/f'reference-control-{pair}-{mode}.log').open('w') as out:
   for name in ['integer_add','any_value_datetime','any_value_duration','lower']:
    subprocess.run(['taskset','-c','2',str(root/f'{mode}-bin'),'tests::lane_cache_batch_performance::lane_cache_batch_release_performance_probe','--ignored','--exact','--nocapture','--test-threads=1'],env=dict(env,EXPR_BATCH_PERF_WORKLOAD=name),stdout=out,stderr=subprocess.STDOUT,check=True)
