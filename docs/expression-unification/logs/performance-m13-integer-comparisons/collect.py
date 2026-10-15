import os, subprocess
from pathlib import Path
root=Path(__file__).resolve().parent
names=[f'integer_compare_{op}' for op in ['lt','le','gt','ge','eq','ne','nulleq']]
env=dict(os.environ,EXPR_BATCH_PERF_BATCH_SIZE='1024',EXPR_BATCH_PERF_SAMPLES='7',EXPR_BATCH_PERF_TARGET_NS='75000000',RUST_MIN_STACK='33554432')
for pair in range(1,4):
 for mode in (['current_fallback_ablation','batch_tikv'] if pair%2 else ['batch_tikv','current_fallback_ablation']):
  env['EXPR_BATCH_PERF_MODE']=mode
  with (root/f'pair-{pair}-{mode}.log').open('w') as out:
   for name in names:
    env['EXPR_BATCH_PERF_WORKLOAD']=name
    subprocess.run(['taskset','-c','2',str(root/'current-bin'),'tests::lane_cache_batch_performance::lane_cache_batch_release_performance_probe','--ignored','--exact','--nocapture','--test-threads=1'],env=env,stdout=out,stderr=subprocess.STDOUT,check=True)
 # This is a real frozen production build with only the retained test probe added.
 with (root/f'frozen-native-{pair}.log').open('w') as out:
  subprocess.run(['taskset','-c','2',str(root/'frozen-bin'),'tests::original_native_all_carriers_performance::original_native_all_carriers_release_performance_probe','--ignored','--exact','--nocapture','--test-threads=1'],env=dict(os.environ,RUST_MIN_STACK='33554432'),stdout=out,stderr=subprocess.STDOUT,check=True)
 print(f'completed pair {pair} and frozen cohort',flush=True)
