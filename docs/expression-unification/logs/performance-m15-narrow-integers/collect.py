import os,subprocess
from pathlib import Path
root=Path(__file__).resolve().parent
cases=[(kind,op) for kind in ['tiny','short','int24','long'] for op in ['integer_add','integer_compare_lt']]
base=dict(os.environ,RUST_MIN_STACK='33554432',EXPR_BATCH_PERF_BATCH_SIZE='1024',EXPR_BATCH_PERF_SAMPLES='7',EXPR_BATCH_PERF_TARGET_NS='75000000')
for pair in range(1,4):
 for mode in (['current_fallback_ablation','batch_tikv'] if pair%2 else ['batch_tikv','current_fallback_ablation']):
  for kind,name in cases:
   with (root/f'pair-{pair}-{kind}-{name}-{mode}.log').open('w') as out:
    subprocess.run(['taskset','-c','2',str(root/'current-bin'),'tests::lane_cache_batch_performance::lane_cache_batch_release_performance_probe','--ignored','--exact','--nocapture','--test-threads=1'],env=dict(base,EXPR_BATCH_PERF_INT_TYPE=kind,EXPR_BATCH_PERF_WORKLOAD=name,EXPR_BATCH_PERF_MODE=mode),stdout=out,stderr=subprocess.STDOUT,check=True)
 for kind,name in cases:
  with (root/f'frozen-{pair}-{kind}-{name}.log').open('w') as out:
   subprocess.run(['taskset','-c','2',str(root/'frozen-bin'),'tests::original_native_all_carriers_performance::original_native_all_carriers_release_performance_probe','--ignored','--exact','--nocapture','--test-threads=1'],env=dict(base,EXPR_BATCH_PERF_INT_TYPE=kind,EXPR_NATIVE_PERF_WORKLOAD=name),stdout=out,stderr=subprocess.STDOUT,check=True)
 print(f'completed pair {pair}',flush=True)
