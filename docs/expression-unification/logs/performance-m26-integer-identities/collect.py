import os, subprocess
from pathlib import Path
root=Path(__file__).resolve().parent
new=['round_int_signed','round_int_unsigned','any_value_int_unsigned']
controls=['integer_add','real_add','decimal_add','lower','any_value_datetime','any_value_timestamp','any_value_duration','any_value_json','any_value_vector','decimal_compare_lt','decimal_compare_nulleq']
env=dict(os.environ,EXPR_BATCH_PERF_INT_TYPE='longlong',EXPR_BATCH_PERF_BATCH_SIZE='1024',EXPR_BATCH_PERF_SAMPLES='7',EXPR_BATCH_PERF_TARGET_NS='75000000',RUST_MIN_STACK='33554432')
env.pop('EXPR_PERF_REAL_DOMAIN',None)
env.pop('EXPR_BATCH_PERF_INT_UNSIGNED',None)
for pair in range(1,4):
 for mode in (['before','after'] if pair%2 else ['after','before']):
  with (root/f'pair-{pair}-{mode}.log').open('w') as out:
   for name in new+controls:
    baseline_ablation=mode=='before' and name in new
    binary='after-bin' if baseline_ablation else f'{mode}-bin'
    callenv=dict(env,EXPR_BATCH_PERF_WORKLOAD=name,EXPR_BATCH_PERF_MODE='current_fallback_ablation' if baseline_ablation else 'batch_tikv')
    if name.endswith('_unsigned'): callenv['EXPR_BATCH_PERF_INT_UNSIGNED']='1'
    subprocess.run(['taskset','-c','2',str(root/binary),'tests::lane_cache_batch_performance::lane_cache_batch_release_performance_probe','--ignored','--exact','--nocapture','--test-threads=1'],env=callenv,stdout=out,stderr=subprocess.STDOUT,check=True)
 with (root/f'frozen-native-{pair}.log').open('w') as out:
  for name in new+controls:
   subprocess.run(['taskset','-c','2',str(root/'frozen-bin'),'tests::original_native_all_carriers_performance::original_native_all_carriers_release_performance_probe','--ignored','--exact','--nocapture','--test-threads=1'],env=dict(env,EXPR_NATIVE_PERF_WORKLOAD=name,**({'EXPR_BATCH_PERF_INT_UNSIGNED':'1'} if name.endswith('_unsigned') else {})),stdout=out,stderr=subprocess.STDOUT,check=True)
 print(f'completed pair {pair} and actual frozen native',flush=True)
