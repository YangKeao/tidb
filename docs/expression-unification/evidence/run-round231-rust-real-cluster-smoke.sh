#!/usr/bin/env bash
# Provision a real PD/TiKV cluster, then query it through the Rust tidb-server.
set -Eeuo pipefail

: "${PD_BIN:?set PD_BIN}"
: "${TIKV_BIN:?set TIKV_BIN}"
: "${BOOTSTRAP_TIDB_BIN:?set BOOTSTRAP_TIDB_BIN}"
: "${RUST_TIDB_BIN:?set RUST_TIDB_BIN}"
: "${OUT:?set OUT}"

PD_CLIENT_PORT="${PD_CLIENT_PORT:-13379}"
PD_PEER_PORT="${PD_PEER_PORT:-13380}"
TIKV_PORT="${TIKV_PORT:-21160}"
TIKV_STATUS_PORT="${TIKV_STATUS_PORT:-21180}"
BOOTSTRAP_TIDB_PORT="${BOOTSTRAP_TIDB_PORT:-15000}"
BOOTSTRAP_STATUS_PORT="${BOOTSTRAP_STATUS_PORT:-15080}"
RUST_TIDB_PORT="${RUST_TIDB_PORT:-15001}"
ROWS="${ROWS:-4096}"
PROJECTION_QUERIES="${PROJECTION_QUERIES:-20}"
NESTED_QUERIES="${NESTED_QUERIES:-20}"
PERF_RUNS="${PERF_RUNS:-3}"

mkdir -p "$OUT"
RUN_DIR="$(mktemp -d "${TMPDIR:-/tmp}/round231-rust-real-cluster.XXXXXX")"
PD_PID="" TIKV_PID="" BOOTSTRAP_PID="" RUST_PID=""

terminate_all() {
  local pid
  for pid in "$RUST_PID" "$BOOTSTRAP_PID" "$TIKV_PID" "$PD_PID"; do
    [[ -n "$pid" ]] && kill "$pid" 2>/dev/null || true
  done
  sleep 3
  for pid in "$RUST_PID" "$BOOTSTRAP_PID" "$TIKV_PID" "$PD_PID"; do
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      kill -KILL "$pid" 2>/dev/null || true
    fi
  done
  for pid in "$RUST_PID" "$BOOTSTRAP_PID" "$TIKV_PID" "$PD_PID"; do
    [[ -n "$pid" ]] && wait "$pid" 2>/dev/null || true
  done
}

cleanup() {
  local status=$? cleanup_ok=1
  trap - EXIT INT TERM
  terminate_all
  {
    for port in "$PD_CLIENT_PORT" "$PD_PEER_PORT" "$TIKV_PORT" "$TIKV_STATUS_PORT" "$BOOTSTRAP_TIDB_PORT" "$BOOTSTRAP_STATUS_PORT" "$RUST_TIDB_PORT"; do
      if ss -H -ltn "sport = :$port" | grep -q .; then
        echo "CLEANUP port=$port free=false"
        cleanup_ok=0
      else
        echo "CLEANUP port=$port free=true"
      fi
    done
  } >"$OUT/cleanup.log"
  rm -rf "$RUN_DIR"
  [[ "$status" -eq 0 && "$cleanup_ok" -ne 1 ]] && status=1
  exit "$status"
}
trap cleanup EXIT INT TERM

for port in "$PD_CLIENT_PORT" "$PD_PEER_PORT" "$TIKV_PORT" "$TIKV_STATUS_PORT" "$BOOTSTRAP_TIDB_PORT" "$BOOTSTRAP_STATUS_PORT" "$RUST_TIDB_PORT"; do
  if ss -H -ltn "sport = :$port" | grep -q .; then
    echo "port $port is already in use" >&2
    exit 1
  fi
done

wait_http() {
  local name=$1 url=$2 pid=$3
  for _ in $(seq 1 180); do
    curl -sf "$url" >/dev/null && return 0
    kill -0 "$pid" 2>/dev/null || { echo "$name exited before readiness" >&2; return 1; }
    sleep 1
  done
  echo "$name readiness timed out" >&2
  return 1
}

wait_mysql() {
  local port=$1 pid=$2 name=$3
  for _ in $(seq 1 180); do
    mysql --protocol=tcp -h127.0.0.1 -P"$port" -uroot --connect-timeout=2 -Nse 'SELECT 1' >/dev/null 2>&1 && return 0
    kill -0 "$pid" 2>/dev/null || { echo "$name exited before MySQL readiness" >&2; return 1; }
    sleep 1
  done
  echo "$name MySQL readiness timed out" >&2
  return 1
}

BOOTSTRAP_MYSQL=(mysql --protocol=tcp -h127.0.0.1 -P"$BOOTSTRAP_TIDB_PORT" -uroot --connect-timeout=5 --batch --raw --skip-column-names)
RUST_MYSQL=(mysql --protocol=tcp -h127.0.0.1 -P"$RUST_TIDB_PORT" -uroot --connect-timeout=5 --batch --raw --skip-column-names)

{
  echo "ROUND231_CONFIG rows=$ROWS projection_queries=$PROJECTION_QUERIES nested_queries=$NESTED_QUERIES perf_runs=$PERF_RUNS"
  echo "ROUND231_SOURCE tidb=${TIDB_SOURCE_REVISION:-unknown} tikv=${TIKV_SOURCE_REVISION:-unknown}"
  sha256sum "$PD_BIN" "$TIKV_BIN" "$BOOTSTRAP_TIDB_BIN" "$RUST_TIDB_BIN"
  "$RUST_TIDB_BIN" -V 2>&1 | sed 's/^/RUST_TIDB_VERSION /'
  "$TIKV_BIN" --version 2>&1 | sed 's/^/TIKV_VERSION /'
} >"$OUT/environment.log"

"$PD_BIN" --name=pd-round231 --data-dir="$RUN_DIR/pd" \
  --client-urls="http://127.0.0.1:$PD_CLIENT_PORT" --advertise-client-urls="http://127.0.0.1:$PD_CLIENT_PORT" \
  --peer-urls="http://127.0.0.1:$PD_PEER_PORT" --advertise-peer-urls="http://127.0.0.1:$PD_PEER_PORT" \
  --initial-cluster="pd-round231=http://127.0.0.1:$PD_PEER_PORT" --log-file="$OUT/pd.log" \
  >"$OUT/pd.stdout.log" 2>&1 &
PD_PID=$!
wait_http PD "http://127.0.0.1:$PD_CLIENT_PORT/pd/api/v1/version" "$PD_PID"

"$TIKV_BIN" --addr="127.0.0.1:$TIKV_PORT" --advertise-addr="127.0.0.1:$TIKV_PORT" \
  --status-addr="127.0.0.1:$TIKV_STATUS_PORT" --advertise-status-addr="127.0.0.1:$TIKV_STATUS_PORT" \
  --pd-endpoints="127.0.0.1:$PD_CLIENT_PORT" --data-dir="$RUN_DIR/tikv" --log-file="$OUT/tikv.log" \
  >"$OUT/tikv.stdout.log" 2>&1 &
TIKV_PID=$!
wait_http TiKV "http://127.0.0.1:$TIKV_STATUS_PORT/status" "$TIKV_PID"

# The Go process is provisioning only: it bootstraps TiDB metadata and writes the fixture,
# then stops before any measured/correctness query is sent to the Rust endpoint.
"$BOOTSTRAP_TIDB_BIN" --store=tikv --path="127.0.0.1:$PD_CLIENT_PORT" --host=127.0.0.1 \
  --advertise-address=127.0.0.1 -P "$BOOTSTRAP_TIDB_PORT" --status="$BOOTSTRAP_STATUS_PORT" \
  --log-file="$OUT/bootstrap-tidb.log" >"$OUT/bootstrap-tidb.stdout.log" 2>&1 &
BOOTSTRAP_PID=$!
wait_mysql "$BOOTSTRAP_TIDB_PORT" "$BOOTSTRAP_PID" "bootstrap Go TiDB"

python3 - "$ROWS" "$OUT" <<'PY'
import sys
from pathlib import Path
rows=int(sys.argv[1]); out=Path(sys.argv[2]); strings=['Alpha','beta','abcdef','TiDB','vectorized','z']
records=[]
for n in range(rows):
    records.append((n, None if n%10==0 else n%97-48, n%13+1, None if n%10==0 else strings[n%len(strings)]))
def q(v):
    if v is None: return 'NULL'
    if isinstance(v,int): return str(v)
    return "'"+v.replace("'","''")+"'"
with (out/'setup.sql').open('w') as f:
    f.write('DROP DATABASE IF EXISTS expr_round231;\nCREATE DATABASE expr_round231;\nUSE expr_round231;\n')
    f.write('CREATE TABLE t (id BIGINT PRIMARY KEY, i BIGINT, j BIGINT NOT NULL, s VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin);\n')
    for start in range(0,rows,256):
        vals=['('+','.join(q(v) for v in r)+')' for r in records[start:start+256]]
        f.write('INSERT INTO t VALUES '+','.join(vals)+';\n')
with (out/'expected-projection.tsv').open('w') as f:
    for _,i,j,_ in records: f.write('NULL\n' if i is None else f'{i+j}\n')
with (out/'expected-nested.tsv').open('w') as f:
    for _,i,j,_ in records: f.write('NULL\n' if i is None else f'{i+j+i+11}\n')
selected=[r for r in records if r[0] in (0,1,2,3,10,11,1023,2048,4095) and r[0] < rows]
with (out/'expected-correctness.tsv').open('w') as f:
    f.write(f'{rows}\t{sum(r[1] is not None for r in records)}\t{sum(r[1]+r[2] for r in records if r[1] is not None)}\n')
    for ident,i,j,s in selected:
        vals=[str(ident),'NULL' if i is None else str(i+j),'NULL' if i is None else str(i+j+i+11),'NULL' if s is None else s.lower()]
        f.write('\t'.join(vals)+'\n')
(out/'correctness.sql').write_text("""USE expr_round231;
SELECT COUNT(*), COUNT(i), SUM(i+j) FROM t;
SELECT id, i+j, (i+j)+(i+11), LOWER(s) FROM t WHERE id IN (0,1,2,3,10,11,1023,2048,4095) ORDER BY id;
""")
(out/'projection-query.sql').write_text('USE expr_round231; SELECT i+j FROM t ORDER BY id;\n')
(out/'nested-query.sql').write_text('USE expr_round231; SELECT (i+j)+(i+11) FROM t ORDER BY id;\n')
PY

"${BOOTSTRAP_MYSQL[@]}" <"$OUT/setup.sql" >"$OUT/setup.out"
kill "$BOOTSTRAP_PID" 2>/dev/null || true
sleep 3
kill -0 "$BOOTSTRAP_PID" 2>/dev/null && kill -KILL "$BOOTSTRAP_PID" 2>/dev/null || true
wait "$BOOTSTRAP_PID" 2>/dev/null || true
BOOTSTRAP_PID=""

"$RUST_TIDB_BIN" --path "127.0.0.1:$PD_CLIENT_PORT" --port "$RUST_TIDB_PORT" \
  --cluster-session --load-privileges --lease-ms 1000 --no-auto-tls \
  >"$OUT/rust-tidb.log" 2>&1 &
RUST_PID=$!
wait_mysql "$RUST_TIDB_PORT" "$RUST_PID" "Rust TiDB"
for _ in $(seq 1 180); do
  grep -F '"event":"cluster_session_node_ready"' "$OUT/rust-tidb.log" >/dev/null && break
  kill -0 "$RUST_PID" 2>/dev/null || { echo "Rust TiDB exited before cluster-session readiness" >&2; exit 1; }
  sleep 1
done
grep -F '"event":"cluster_session_node_ready"' "$OUT/rust-tidb.log" >"$OUT/readiness.txt"

"${RUST_MYSQL[@]}" <"$OUT/correctness.sql" >"$OUT/correctness.tsv"
cmp "$OUT/expected-correctness.tsv" "$OUT/correctness.tsv"
"${RUST_MYSQL[@]}" <"$OUT/projection-query.sql" >"$OUT/projection.tsv"
cmp "$OUT/expected-projection.tsv" "$OUT/projection.tsv"
"${RUST_MYSQL[@]}" <"$OUT/nested-query.sql" >"$OUT/nested.tsv"
cmp "$OUT/expected-nested.tsv" "$OUT/nested.tsv"

{
  echo 'USE expr_round231;'
  echo 'EXPLAIN SELECT i+j FROM t ORDER BY id;'
  echo 'EXPLAIN SELECT (i+j)+(i+11) FROM t ORDER BY id;'
} | "${RUST_MYSQL[@]}" >"$OUT/explain.tsv"

{
  echo 'USE expr_round231;'
  for _ in $(seq 1 "$PROJECTION_QUERIES"); do echo 'SELECT i+j FROM t ORDER BY id;'; done
} >"$OUT/bench-projection.sql"
{
  echo 'USE expr_round231;'
  for _ in $(seq 1 "$NESTED_QUERIES"); do echo 'SELECT (i+j)+(i+11) FROM t ORDER BY id;'; done
} >"$OUT/bench-nested.sql"
"${RUST_MYSQL[@]}" <"$OUT/projection-query.sql" >/dev/null
"${RUST_MYSQL[@]}" <"$OUT/nested-query.sql" >/dev/null
: >"$OUT/performance.log"
for run in $(seq 1 "$PERF_RUNS"); do
  if ((run%2)); then workloads=(projection nested); else workloads=(nested projection); fi
  for workload in "${workloads[@]}"; do
    queries="$PROJECTION_QUERIES"; expected="$OUT/expected-projection.tsv"
    [[ "$workload" == nested ]] && { queries="$NESTED_QUERIES"; expected="$OUT/expected-nested.tsv"; }
    /usr/bin/time -f "PERF workload=$workload run=$run queries=$queries rows_per_query=$ROWS elapsed_seconds=%e user_seconds=%U system_seconds=%S max_rss_kib=%M" \
      -a -o "$OUT/performance.log" "${RUST_MYSQL[@]}" <"$OUT/bench-$workload.sql" >"$OUT/bench-$workload-$run.tsv"
    python3 - "$expected" "$OUT/bench-$workload-$run.tsv" "$queries" <<'PY'
import sys
from pathlib import Path
expected=Path(sys.argv[1]).read_bytes(); actual=Path(sys.argv[2]).read_bytes(); count=int(sys.argv[3])
assert actual == expected * count, (len(actual),len(expected),count)
PY
  done
done

{
  echo "RUST_READY true"
  echo "CORRECTNESS exact_diff=pass rows=$ROWS"
  cat "$OUT/performance.log"
} >"$OUT/summary.txt"
