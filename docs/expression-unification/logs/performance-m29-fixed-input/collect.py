import os,subprocess
from pathlib import Path
r=Path(__file__).resolve().parent
names=['round_int_signed','round_int_unsigned','any_value_int_unsigned','integer_add','real_add','decimal_add','lower','any_value_datetime','any_value_timestamp','any_value_duration','any_value_json','any_value_vector','decimal_compare_lt','decimal_compare_nulleq']
modes=['baseline','direct','fixed']
env=dict(os.environ,EXPR_BATCH_PERF_INT_TYPE='longlong',EXPR_BATCH_PERF_BATCH_SIZE='1024',EXPR_BATCH_PERF_SAMPLES='7',EXPR_BATCH_PERF_TARGET_NS='75000000',EXPR_BATCH_PERF_MODE='batch_tikv',RUST_MIN_STACK='33554432')
for k in ['EXPR_BATCH_PERF_INT_UNSIGNED','EXPR_PERF_REAL_DOMAIN']:env.pop(k,None)
for pair in range(1,4):
 order=modes[pair-1:]+modes[:pair-1]
 for mode in order:
  with (r/f'cohort-{pair}-{mode}.log').open('w') as out:
   for name in names:
    callenv=dict(env,EXPR_BATCH_PERF_WORKLOAD=name,**({'EXPR_BATCH_PERF_INT_UNSIGNED':'1'} if name.endswith('_unsigned') else {}))
    subprocess.run(['taskset','-c','2',str(r/f'{mode}-bin'),'tests::lane_cache_batch_performance::lane_cache_batch_release_performance_probe','--ignored','--exact','--nocapture','--test-threads=1'],env=callenv,stdout=out,stderr=subprocess.STDOUT,check=True)
 with (r/f'frozen-native-{pair}.log').open('w') as out:
  for name in names:
   callenv=dict(env,EXPR_NATIVE_PERF_WORKLOAD=name,**({'EXPR_BATCH_PERF_INT_UNSIGNED':'1'} if name.endswith('_unsigned') else {}))
   subprocess.run(['taskset','-c','2',str(r/'frozen-bin'),'tests::original_native_all_carriers_performance::original_native_all_carriers_release_performance_probe','--ignored','--exact','--nocapture','--test-threads=1'],env=callenv,stdout=out,stderr=subprocess.STDOUT,check=True)
 print(f'completed three-code cohort {pair}',flush=True)
