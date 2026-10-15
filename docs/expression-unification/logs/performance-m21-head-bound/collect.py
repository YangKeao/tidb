import os,subprocess
from pathlib import Path
root=Path(__file__).resolve().parent
names=['integer_add','real_add','decimal_add','lower','any_value_datetime','any_value_timestamp','any_value_duration','any_value_json','any_value_vector','decimal_compare_lt','decimal_compare_nulleq']
env=dict(os.environ,EXPR_BATCH_PERF_MODE='batch_tikv',EXPR_BATCH_PERF_BATCH_SIZE='1024',EXPR_BATCH_PERF_SAMPLES='7',EXPR_BATCH_PERF_TARGET_NS='75000000',RUST_MIN_STACK='33554432')
for cohort,modes in enumerate([['m20','full','head'],['full','head','m20'],['head','m20','full']],1):
 for mode in modes:
  with (root/f'cohort-{cohort}-{mode}.log').open('w') as out:
   for name in names:
    subprocess.run(['taskset','-c','2',str(root/f'{mode}-bin'),'tests::lane_cache_batch_performance::lane_cache_batch_release_performance_probe','--ignored','--exact','--nocapture','--test-threads=1'],env=dict(env,EXPR_BATCH_PERF_WORKLOAD=name),stdout=out,stderr=subprocess.STDOUT,check=True)
 with (root/f'frozen-native-{cohort}.log').open('w') as out:
  for name in names:
   subprocess.run(['taskset','-c','2',str(root/'frozen-bin'),'tests::original_native_all_carriers_performance::original_native_all_carriers_release_performance_probe','--ignored','--exact','--nocapture','--test-threads=1'],env=dict(os.environ,RUST_MIN_STACK='33554432',EXPR_NATIVE_PERF_WORKLOAD=name),stdout=out,stderr=subprocess.STDOUT,check=True)
 print(f'completed cohort {cohort}',flush=True)
