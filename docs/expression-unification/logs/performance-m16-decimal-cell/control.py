import os,subprocess
from pathlib import Path
root=Path(__file__).resolve().parent
env=dict(os.environ,EXPR_BATCH_PERF_WORKLOAD='integer_add',EXPR_BATCH_PERF_BATCH_SIZE='1024',EXPR_BATCH_PERF_SAMPLES='7',EXPR_BATCH_PERF_TARGET_NS='150000000',RUST_MIN_STACK='33554432')
for pair in range(1,4):
 for mode in (['after','before'] if pair%2 else ['before','after']):
  with (root/f'int-control-{pair}-{mode}.log').open('w') as out:
   subprocess.run(['taskset','-c','2',str(root/f'{mode}-bin'),'tests::lane_cache_batch_performance::lane_cache_batch_release_performance_probe','--ignored','--exact','--nocapture','--test-threads=1'],env=env,stdout=out,stderr=subprocess.STDOUT,check=True)
