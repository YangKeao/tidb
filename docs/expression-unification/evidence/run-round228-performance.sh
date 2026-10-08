#!/usr/bin/env bash
# Reproduce round228 width-one and real-Chunk performance collection.
set -euo pipefail

: "${CURRENT_BIN:?set CURRENT_BIN to the current release libtest binary}"
: "${FROZEN_BIN:?set FROZEN_BIN to the frozen release libtest binary}"
: "${OUT:?set OUT to an existing output directory}"

# This receipt is intentionally fixed to CPU2; environment capture below also
# monitors its topology sibling CPU14.
CPU=2
PAIRS=${PAIRS:-5}
BATCH_TARGET_NS=${BATCH_TARGET_NS:-75000000}
mkdir -p "$OUT/perf"
rm -f "$OUT/perf/"*.csv
: >"$OUT/width-one.log"
: >"$OUT/batch.log"
: >"$OUT/environment.log"

snapshot() {
  local label=$1
  {
    printf 'SNAPSHOT label=%s utc=%s\n' "$label" "$(date -u +%FT%TZ)"
    printf 'loadavg '; cat /proc/loadavg
    printf 'cpu_pressure '; tr '\n' ' ' </proc/pressure/cpu; echo
    printf 'memory_pressure '; tr '\n' ' ' </proc/pressure/memory; echo
    printf 'io_pressure '; tr '\n' ' ' </proc/pressure/io; echo
    awk '$1 == "cpu2" || $1 == "cpu14" { print "proc_stat " $0 }' /proc/stat
    for cpu in 2 14; do
      printf 'cpu_frequency cpu=%s khz=' "$cpu"
      cat "/sys/devices/system/cpu/cpu${cpu}/cpufreq/scaling_cur_freq"
    done
    for sensor in 1 3 4; do
      printf 'k10temp sensor=%s millidegree=' "$sensor"
      cat "/sys/class/hwmon/hwmon2/temp${sensor}_input"
    done
  } >>"$OUT/environment.log"
}

run_one() {
  local suite=$1 revision=$2 run=$3 binary=$4 log test_name=$5
  local multiplier=1
  log="$OUT/${suite}.log"
  if [[ "$suite" == width-one && "$revision" == frozen ]]; then
    multiplier=16
  fi
  snapshot "${suite}-${revision}-${run}-before"
  printf 'PERF_PROCESS_RUN suite=%s revision=%s run=%s order_cpu=%s\n' \
    "$suite" "$revision" "$run" "$CPU" | tee -a "$log"
  if [[ "$suite" == width-one ]]; then
    env EXPR_LANE_PERF_ITERATION_MULTIPLIER="$multiplier" \
      perf stat -x, -o "$OUT/perf/${suite}-${revision}-${run}.csv" \
      -e task-clock,cycles,ref-cycles,instructions,context-switches,cpu-migrations,page-faults,major-faults \
      taskset -c "$CPU" "$binary" "$test_name" \
      --ignored --exact --nocapture --test-threads=1 2>&1 | tee -a "$log"
  else
    env EXPR_BATCH_PERF_TARGET_NS="$BATCH_TARGET_NS" EXPR_BATCH_PERF_SAMPLES=7 \
      perf stat -x, -o "$OUT/perf/${suite}-${revision}-${run}.csv" \
      -e task-clock,cycles,ref-cycles,instructions,context-switches,cpu-migrations,page-faults,major-faults \
      taskset -c "$CPU" "$binary" "$test_name" \
      --ignored --exact --nocapture --test-threads=1 2>&1 | tee -a "$log"
  fi
  snapshot "${suite}-${revision}-${run}-after"
}

run_suite() {
  local suite=$1 test_name=$2
  for run in $(seq 1 "$PAIRS"); do
    if (( run % 2 == 1 )); then
      run_one "$suite" current "$run" "$CURRENT_BIN" "$test_name"
      run_one "$suite" frozen "$run" "$FROZEN_BIN" "$test_name"
    else
      run_one "$suite" frozen "$run" "$FROZEN_BIN" "$test_name"
      run_one "$suite" current "$run" "$CURRENT_BIN" "$test_name"
    fi
  done
}

snapshot session-start
run_suite width-one tests::lane_cache_performance::lane_cache_release_performance_probe
run_suite batch tests::lane_cache_batch_performance::lane_cache_batch_release_performance_probe
snapshot session-end
