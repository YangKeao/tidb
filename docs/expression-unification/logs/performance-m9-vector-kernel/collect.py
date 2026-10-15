import os, subprocess
from pathlib import Path
root = Path(__file__).resolve().parent
names = ['integer_add', 'real_add', 'decimal_add', 'lower', 'any_value_datetime', 'any_value_timestamp', 'any_value_duration', 'any_value_json', 'any_value_vector']
env = dict(os.environ, EXPR_BATCH_PERF_BATCH_SIZE='1024', EXPR_BATCH_PERF_SAMPLES='7', EXPR_BATCH_PERF_TARGET_NS='75000000', RUST_MIN_STACK='33554432')
for pair in range(1,4):
 for mode in (['before', 'after'] if pair % 2 else ['after','before']):
  with (root/f'pair-{pair}-{mode}.log').open('w') as out:
   for name in names:
    env['EXPR_BATCH_PERF_WORKLOAD'] = name
    subprocess.run(['taskset','-c','2', str(root/f'{mode}-bin'), 'tests::lane_cache_batch_performance::lane_cache_batch_release_performance_probe', '--ignored','--exact','--nocapture','--test-threads=1'], env=env, stdout=out, stderr=subprocess.STDOUT, check=True)
 print(f'completed pair {pair}', flush=True)
