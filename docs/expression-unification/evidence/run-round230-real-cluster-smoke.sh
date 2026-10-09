#!/usr/bin/env bash
# Start one real PD + TiKV + TiDB process set, execute a narrow SQL smoke, and clean up.
set -Eeuo pipefail

: "${PD_BIN:?set PD_BIN}"
: "${TIKV_BIN:?set TIKV_BIN}"
: "${TIDB_BIN:?set TIDB_BIN}"
: "${OUT:?set OUT}"

PD_CLIENT_PORT="${PD_CLIENT_PORT:-12379}"
PD_PEER_PORT="${PD_PEER_PORT:-12380}"
TIKV_PORT="${TIKV_PORT:-20160}"
TIKV_STATUS_PORT="${TIKV_STATUS_PORT:-20180}"
TIDB_PORT="${TIDB_PORT:-14000}"
TIDB_STATUS_PORT="${TIDB_STATUS_PORT:-14080}"
ROWS="${ROWS:-4096}"
INTEGER_QUERIES="${INTEGER_QUERIES:-50}"
MIXED_QUERIES="${MIXED_QUERIES:-20}"
PERF_RUNS="${PERF_RUNS:-3}"

mkdir -p "$OUT"
RUN_DIR="$(mktemp -d "${TMPDIR:-/tmp}/round230-real-cluster.XXXXXX")"
PD_PID=""
TIKV_PID=""
TIDB_PID=""

cleanup() {
  local status=$? cleanup_ok=1
  trap - EXIT INT TERM
  for pid in "$TIDB_PID" "$TIKV_PID" "$PD_PID"; do
    if [[ -n "$pid" ]]; then
      kill "$pid" 2>/dev/null || true
    fi
  done
  # TiDB may wait indefinitely for store shutdown after SIGTERM. Give all three
  # processes a short grace period, then guarantee teardown for this test run.
  sleep 3
  for pid in "$TIDB_PID" "$TIKV_PID" "$PD_PID"; do
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      kill -KILL "$pid" 2>/dev/null || true
    fi
  done
  for pid in "$TIDB_PID" "$TIKV_PID" "$PD_PID"; do
    if [[ -n "$pid" ]]; then
      wait "$pid" 2>/dev/null || true
    fi
  done
  {
    for port in "$PD_CLIENT_PORT" "$PD_PEER_PORT" "$TIKV_PORT" "$TIKV_STATUS_PORT" "$TIDB_PORT" "$TIDB_STATUS_PORT"; do
      if ss -H -ltn "sport = :$port" | grep -q .; then
        echo "CLEANUP port=$port free=false"
        cleanup_ok=0
      else
        echo "CLEANUP port=$port free=true"
      fi
    done
  } >"$OUT/cleanup.log"
  rm -rf "$RUN_DIR"
  if [[ "$status" -eq 0 && "$cleanup_ok" -ne 1 ]]; then
    status=1
  fi
  exit "$status"
}
trap cleanup EXIT INT TERM

for port in "$PD_CLIENT_PORT" "$PD_PEER_PORT" "$TIKV_PORT" "$TIKV_STATUS_PORT" "$TIDB_PORT" "$TIDB_STATUS_PORT"; do
  if ss -H -ltn "sport = :$port" | grep -q .; then
    echo "port $port is already in use" >&2
    exit 1
  fi
done

wait_http() {
  local name=$1 url=$2 pid=$3
  for _ in $(seq 1 180); do
    if curl -sf "$url" >/dev/null; then
      return 0
    fi
    if ! kill -0 "$pid" 2>/dev/null; then
      echo "$name exited before readiness" >&2
      return 1
    fi
    sleep 1
  done
  echo "$name readiness timed out: $url" >&2
  return 1
}

wait_mysql() {
  for _ in $(seq 1 180); do
    if mysql --protocol=tcp -h127.0.0.1 -P"$TIDB_PORT" -uroot --connect-timeout=2 -Nse 'SELECT 1' >/dev/null 2>&1; then
      return 0
    fi
    if ! kill -0 "$TIDB_PID" 2>/dev/null; then
      echo "TiDB exited before MySQL readiness" >&2
      return 1
    fi
    sleep 1
  done
  echo "TiDB MySQL readiness timed out" >&2
  return 1
}

MYSQL=(mysql --protocol=tcp -h127.0.0.1 -P"$TIDB_PORT" -uroot --connect-timeout=5 --batch --raw --skip-column-names)

{
  echo "ROUND230_CONFIG rows=$ROWS integer_queries=$INTEGER_QUERIES mixed_queries=$MIXED_QUERIES perf_runs=$PERF_RUNS"
  echo "ROUND230_PORTS pd=$PD_CLIENT_PORT pd_peer=$PD_PEER_PORT tikv=$TIKV_PORT tikv_status=$TIKV_STATUS_PORT tidb=$TIDB_PORT tidb_status=$TIDB_STATUS_PORT"
  echo "ROUND230_HOST kernel=$(uname -srmo) cpu=$(nproc)"
  echo "ROUND230_SOURCE tidb=${TIDB_SOURCE_REVISION:-unknown} tikv=${TIKV_SOURCE_REVISION:-unknown}"
  sha256sum "$PD_BIN" "$TIKV_BIN" "$TIDB_BIN"
  "$PD_BIN" --version 2>&1 | sed 's/^/PD_VERSION /'
  "$TIKV_BIN" --version 2>&1 | sed 's/^/TIKV_VERSION /'
  "$TIDB_BIN" -V 2>&1 | sed 's/^/TIDB_VERSION /'
} >"$OUT/environment.log"

"$PD_BIN" \
  --name=pd-round230 \
  --data-dir="$RUN_DIR/pd" \
  --client-urls="http://127.0.0.1:$PD_CLIENT_PORT" \
  --advertise-client-urls="http://127.0.0.1:$PD_CLIENT_PORT" \
  --peer-urls="http://127.0.0.1:$PD_PEER_PORT" \
  --advertise-peer-urls="http://127.0.0.1:$PD_PEER_PORT" \
  --initial-cluster="pd-round230=http://127.0.0.1:$PD_PEER_PORT" \
  --log-file="$OUT/pd.log" \
  >"$OUT/pd.stdout.log" 2>&1 &
PD_PID=$!
wait_http PD "http://127.0.0.1:$PD_CLIENT_PORT/pd/api/v1/version" "$PD_PID"

"$TIKV_BIN" \
  --addr="127.0.0.1:$TIKV_PORT" \
  --advertise-addr="127.0.0.1:$TIKV_PORT" \
  --status-addr="127.0.0.1:$TIKV_STATUS_PORT" \
  --advertise-status-addr="127.0.0.1:$TIKV_STATUS_PORT" \
  --pd-endpoints="127.0.0.1:$PD_CLIENT_PORT" \
  --data-dir="$RUN_DIR/tikv" \
  --log-file="$OUT/tikv.log" \
  >"$OUT/tikv.stdout.log" 2>&1 &
TIKV_PID=$!
wait_http TiKV "http://127.0.0.1:$TIKV_STATUS_PORT/status" "$TIKV_PID"

"$TIDB_BIN" \
  --store=tikv \
  --path="127.0.0.1:$PD_CLIENT_PORT" \
  --host=127.0.0.1 \
  --advertise-address=127.0.0.1 \
  -P "$TIDB_PORT" \
  --status="$TIDB_STATUS_PORT" \
  --log-file="$OUT/tidb.log" \
  >"$OUT/tidb.stdout.log" 2>&1 &
TIDB_PID=$!
wait_mysql

python3 - "$ROWS" "$OUT" <<'PY'
import json
import sys
from decimal import Decimal
from pathlib import Path

rows = int(sys.argv[1])
out = Path(sys.argv[2])
strings = ["Alpha", "beta", "abcdef", "TiDB", "vectorized", "z"]
decimals = [Decimal("123.45"), Decimal("-7.25"), Decimal("0.00"), Decimal("999.99")]
jsons = ['{"a":1}', '[1,2]', '"text"', '7']
records = []
for n in range(rows):
    nullable = n % 10 == 0
    records.append({
        "id": n,
        "i": None if nullable else n % 97 - 48,
        "j": n % 13 + 1,
        "s": None if nullable else strings[n % len(strings)],
        "d": None if nullable else decimals[n % len(decimals)],
        "js": None if nullable else jsons[n % len(jsons)],
    })

def sql_value(value):
    if value is None:
        return "NULL"
    if isinstance(value, (int, Decimal)):
        return str(value)
    return "'" + str(value).replace("\\", "\\\\").replace("'", "''") + "'"

with (out / "setup.sql").open("w") as f:
    f.write("DROP DATABASE IF EXISTS expr_round230;\nCREATE DATABASE expr_round230;\nUSE expr_round230;\n")
    f.write("CREATE TABLE t (id BIGINT PRIMARY KEY, i BIGINT, j BIGINT NOT NULL, s VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin, d DECIMAL(20,2), js JSON);\n")
    for start in range(0, rows, 256):
        values = []
        for r in records[start:start + 256]:
            values.append("(" + ",".join(sql_value(r[k]) for k in ("id", "i", "j", "s", "d", "js")) + ")")
        f.write("INSERT INTO t VALUES " + ",".join(values) + ";\n")
    f.write("ANALYZE TABLE t;\n")

int_rows = [r for r in records if r["i"] is not None]
str_rows = [r for r in records if r["s"] is not None]
dec_rows = [r for r in records if r["d"] is not None]
line1 = [len(records), len(int_rows), sum(r["i"] + r["j"] for r in int_rows), sum(r["i"] * r["j"] for r in int_rows), sum(abs(r["i"]) for r in int_rows)]
line2 = [
    sum(r["i"] if r["i"] is not None and r["i"] > 0 else r["j"] for r in records),
    sum(r["i"] if r["i"] not in (None, 0) else r["j"] for r in records),
    sum((-1 if r["s"] < "beta" else 1 if r["s"] > "beta" else 0) for r in str_rows),
    sum(len(r["s"].lower()) for r in str_rows),
    sum(len(r["s"] + "-x") for r in str_rows),
]
decimal_sum = sum((r["d"] + Decimal("0.55") for r in dec_rows), Decimal(0))
type_counts = {"ARRAY": 0, "INTEGER": 0, "OBJECT": 0, "STRING": 0}
for r in records:
    if r["js"] is None:
        continue
    value = json.loads(r["js"])
    kind = "OBJECT" if isinstance(value, dict) else "ARRAY" if isinstance(value, list) else "STRING" if isinstance(value, str) else "INTEGER"
    type_counts[kind] += 1
regex_count = sum(r["s"].lower().startswith("a") for r in str_rows)
prepared_sum = sum(r["i"] + 7 for r in int_rows)
with (out / "expected.tsv").open("w") as f:
    f.write("\t".join(map(str, line1)) + "\n")
    f.write("\t".join(map(str, line2)) + "\n")
    f.write(f"{decimal_sum:.2f}\n")
    for kind in sorted(type_counts):
        f.write(f"{kind}\t{type_counts[kind]}\n")
    f.write(f"{regex_count}\n")
    f.write("900150983cd24fb0d6963f7d28e17f72\t4\tabc\tABC\tbcd\tab\n")
    f.write(f"{prepared_sum}\n")

(out / "correctness.sql").write_text("""USE expr_round230;
SELECT COUNT(*), COUNT(i), SUM(i+j), SUM(i*j), SUM(ABS(i)) FROM t;
SELECT SUM(IF(i>0,i,j)), SUM(COALESCE(NULLIF(i,0),j)), SUM(STRCMP(s,'beta')), SUM(CHAR_LENGTH(LOWER(s))), SUM(CHAR_LENGTH(CONCAT(s,'-x'))) FROM t;
SELECT CAST(SUM(d+CAST(0.55 AS DECIMAL(10,2))) AS CHAR) FROM t;
SELECT JSON_TYPE(js), COUNT(*) FROM t WHERE js IS NOT NULL GROUP BY JSON_TYPE(js) ORDER BY JSON_TYPE(js);
SELECT COUNT(*) FROM t WHERE REGEXP_LIKE(s,'^a','i');
SELECT MD5('abc'), LENGTH('TiDB'), LOWER('AbC'), UPPER('AbC'), SUBSTRING('abcdef',2,3), CONCAT('a','b');
PREPARE round230_stmt FROM 'SELECT SUM(i + ?) FROM t';
SET @p=7;
EXECUTE round230_stmt USING @p;
DEALLOCATE PREPARE round230_stmt;
""")

integer = "SELECT SQL_NO_CACHE SUM(i+j), COUNT(*) FROM expr_round230.t WHERE i+j>0;\n"
mixed = "SELECT SQL_NO_CACHE SUM(IF(i>0,i,j)), CAST(SUM(d+CAST(0.55 AS DECIMAL(10,2))) AS CHAR), COUNT(*) FROM expr_round230.t WHERE REGEXP_LIKE(s,'^[at]','i');\n"
# The shell appends the configured query counts after generation.
(out / "integer-query.sql").write_text(integer)
(out / "mixed-query.sql").write_text(mixed)
PY

"${MYSQL[@]}" <"$OUT/setup.sql" >"$OUT/setup.out"
"${MYSQL[@]}" <"$OUT/correctness.sql" >"$OUT/correctness.tsv"
diff -u "$OUT/expected.tsv" "$OUT/correctness.tsv" >"$OUT/correctness.diff"

{
  echo "USE expr_round230;"
  echo "SET SESSION tidb_opt_agg_push_down=1;"
  echo "EXPLAIN FORMAT='brief' SELECT SUM(i+j), COUNT(*) FROM t WHERE i+j>0;"
  echo "EXPLAIN FORMAT='brief' SELECT SUM(IF(i>0,i,j)), SUM(d+CAST(0.55 AS DECIMAL(10,2))), COUNT(*) FROM t WHERE REGEXP_LIKE(s,'^[at]','i');"
  echo "SELECT CAST('not-a-number' AS SIGNED);"
  echo "SHOW WARNINGS;"
} | "${MYSQL[@]}" >"$OUT/explain-and-warning.tsv"

{
  echo "SET SESSION tidb_opt_agg_push_down=1;"
  for _ in $(seq 1 "$INTEGER_QUERIES"); do cat "$OUT/integer-query.sql"; done
} >"$OUT/bench-integer.sql"
{
  echo "SET SESSION tidb_opt_agg_push_down=1;"
  for _ in $(seq 1 "$MIXED_QUERIES"); do cat "$OUT/mixed-query.sql"; done
} >"$OUT/bench-mixed.sql"

# One untimed warmup for both statements.
"${MYSQL[@]}" <"$OUT/integer-query.sql" >/dev/null
"${MYSQL[@]}" <"$OUT/mixed-query.sql" >/dev/null
: >"$OUT/performance.log"
for run in $(seq 1 "$PERF_RUNS"); do
  if (( run % 2 == 1 )); then workloads=(integer mixed); else workloads=(mixed integer); fi
  for workload in "${workloads[@]}"; do
    queries="$INTEGER_QUERIES"
    [[ "$workload" == mixed ]] && queries="$MIXED_QUERIES"
    /usr/bin/time -f "PERF workload=$workload run=$run queries=$queries elapsed_seconds=%e user_seconds=%U system_seconds=%S max_rss_kib=%M" \
      -a -o "$OUT/performance.log" "${MYSQL[@]}" <"$OUT/bench-$workload.sql" >"$OUT/bench-$workload-$run.tsv"
  done
done

{
  echo "READY pd=$(curl -sf "http://127.0.0.1:$PD_CLIENT_PORT/pd/api/v1/version" | tr -d '\n')"
  echo "READY tikv=$(curl -sf "http://127.0.0.1:$TIKV_STATUS_PORT/status" | tr -d '\n')"
  echo "READY tidb=$(curl -sf "http://127.0.0.1:$TIDB_STATUS_PORT/status" | tr -d '\n')"
  echo "SQL_STATUS $(${MYSQL[@]} -Nse 'SELECT VERSION(), @@version_comment, DATABASE()' | tr '\t' '|')"
  echo "CORRECTNESS exact_diff=pass rows=$ROWS"
  cat "$OUT/performance.log"
} >"$OUT/summary.txt"
