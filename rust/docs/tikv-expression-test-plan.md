# Local TiKV expression validation: scope and commands

This inventory supports `tikv-expression-coverage-execplan.md`. It is not a completed Go-package transcreation or blanket passing-test claim. Commands below are reproducible templates; the observed results are reported by the parent running the guarded jobs. The inventory/Session-harness agent did not launch Cargo, Go tests, or builds while the parent held the global heavy-job slot.

## Observed results so far

| Suite | Parent-reported outcome | What is established |
| --- | --- | --- |
| `tidb-expr`, feature `tikv-expr` | Library **1,200 passed**, aggregate `all` **39 passed** | Native regressions plus explicit adapter tests; feature compilation alone does not make every test an engine test. |
| `tidb-executor`, feature `tikv-expr` | **353 passed, 184 ignored** | Includes explicit SQL/backend execution tests; ignored cases remain gaps. |
| `query_diff`, `table_diff`, `catalog_diff` | **31 passed** combined | Native Rust regression; no backend-counter evidence supplied for these targets. |
| `integration_diff` enrolled mysql fixtures | **Pre-existing red: 142 divergences over 10,251 replay statements** in native mode; copying and borrowed have the **identical discrepancy set**, reported MD5 `7b6445a8493f445641a9d07d787f0cba` | Actual engine execution verified: copying **12,447 expression-row evaluations across 1,555 statements**; borrowed mode **10,670 borrowed expression-row evaluations**. This demonstrates no new recorded discrepancy set in this replay, not complete SQL parity or an all-green ratchet. |
| `join_shape` | **Pre-existing stale ratchet**: native = copying `(269,223,97,93,4)`, expected `(246,168,90,86,5)` | Identical observed metrics with and without explicit copying; retain failure and investigate baseline separately. |
| `expr_diff` | **Pre-existing red: two `EXPORT_SET` cases**, also red with the feature disabled | Native Rust/Go-oracle discrepancy, not demonstrated TiKV execution. |
| `table_diff` | **Pre-existing red: 7 of 1,942 in-domain statements**, identical with the feature disabled | Native Rust/Go-oracle discrepancy, not demonstrated TiKV execution. |
| Go `pkg/expression` | **PASS**, failpoint-enabled, **16.2 s** | Original Go oracle/native regression only. |
| Go `pkg/expression/integration_test` | **PASS**, failpoint-enabled, **49.2 s**, peak RSS 10,489 MiB | The first attempt hit an 8 GiB address-space OOM (goroutine dump, not an assertion); rerunning with 16 GiB AS and `GOMEMLIMIT=6GiB` passed all 59 live tests. |
| Go expression subpackages | **PASS**: `aggregation`, `exprctx`, `expropt`, `exprstatic`, `sessionexpr`, `test/constantpropagation`, `test/multivaluedindex` | Original Go oracle/native regression only; `generator` has no test files. |
| `tidb-session`, feature `tikv-expr` | **1726 + 338 passed**, 209 ignored | Requires `RUST_MIN_STACK=2097152`; without it a concurrency test fails to spawn a thread under the 8 GiB AS guard. |

These are parent-reported receipts, not independently rerun by this agent. The complete 257-topic fixture tree was inventoried; the reported replay covers the enrolled subset, not every original script. No result here converts ignored, skipped, or unaligned cases into coverage.

## What each suite proves

| Surface | Exact location/target | Interpretation |
| --- | --- | --- |
| Go scalar/vectorized expression tests | `pkg/expression/*_test.go`; child packages `aggregation`, `exprctx`, `expropt`, `exprstatic`, `generator`, `sessionexpr`, `test/constantpropagation`, `test/multivaluedindex` | Original Go implementation/oracle regression, **not** execution of this Rust local adapter. |
| Go expression SQL integration | `pkg/expression/integration_test/{integration_test.go,main_test.go,README.md,BUILD.bazel}` | Live checkout has **59** top-level tests in `integration_test.go`, plus `TestMain`. Includes vectors, JSON, time, comparison, planner/session/DDL behavior. `TestMain` configures failpoints, timezone and goleak. Do not add new tests to this already-large package. |
| Rust expression regressions | `cargo test -p tidb-expr --lib --test all` | Native/source-port regression unless individual tests explicitly enable a backend. `autotests=false`; `scripts/aggregate-tests.rs` collects test modules into `all`, so `--test tikv_adapter` is not a valid target. |
| Direct engine-adapter tests | `tidb-expr/tests/{tikv_adapter,tikv_borrowed,tikv_coverage}.rs` and `src/tikv/lowering*` tests | Feature-gated adapter calls; runtime admission and executed rows, not enum/inventory membership, establish coverage. |
| SQL/executor engine tests | `tidb-executor/tests/{tikv_borrowed_expression,tikv_expression_coverage}.rs`, target `all` | Parser/rewrite/planner/executor path with explicit `StmtContext` opt-in. Isolated projections compare values, metadata and warnings and assert engine rows or intentional zero-row native fallback. |
| Session opt-in regression | `tidb-session/tests/tikv_expression_session.rs`, target `all` | Default-native SQL under either feature configuration; feature-on copying/borrowed execution, cumulative counters, disabling and peer isolation. |
| Rust constant-expression golden comparison | `difftest-result-tests --test expr_diff`, `rust/difftests/corpus/expr/` | Existing Go golden oracle; native Rust `tidb_expr::eval`, even if the TiKV feature is compiled. `ERR` and `SKIP:*` entries are explicitly excluded. Not an adapter coverage claim. |
| mysql-tester SQL fixture replay | `difftest-result-tests --test integration_diff` | Reads original `tests/integrationtest/t/*.test` and corresponding `r/*.result` recursively through Rust `Session`. Default native; explicit test-only backend opt-in described below. Keeps existing named skips and divergence ratchets. |
| mysql script/connection harness units | `mysqltest_script.rs`, `mysqltest_connections.rs` **modules** inside explicit difftest targets | Not standalone `--test mysql_test`/`--test mysqltest_script` Cargo targets. Connection tests verify backend propagation and retained counts after disconnect/replacement. |
| Original mysql-tester executable harness | `tests/integrationtest/run-tests.sh`, `mysql_tester` | Against Go `tidb-server`, this is a Go SQL/oracle regression, not proof of the Rust adapter. Do not confuse `-store tikv` remote storage with in-process Rust expression reuse. |

There is no literal `mysql_test` target in this checkout. The two actual mysql-tester surfaces above are the executable harness and the Rust fixture replay.

### Original fixture inventory before selecting scope

`tests/integrationtest/t` contains **257** `.test` scripts. All script groups are: root-level scripts, `bindinfo`, `ddl`, `executor` (including `jointest` and `partition`), `expression`, `globalindex`, `infoschema`, `parser`, `planner` (including `cardinality`, `cascades`, `core`, `funcdep`), `privilege`, `session`, `sessionctx`, `sessiontxn`, `statistics`, `table`, `types`, and `util`. Matching recorded results are under `r/`; plan recordings are not exact-row-result oracles. The authoritative enrolled subset is `rust/difftests/result-tests/tests/enrolled_topics.rs::TOPICS`, not all 257 scripts.

All **16 expression topics** (relative to `t/`, without `.test`) are:

```
expression/builtin              expression/cast
expression/charset_and_collation expression/constant_fold
expression/enum_set             expression/explain
expression/format               expression/issues
expression/json                 expression/misc
expression/multi_valued_index    expression/noop_functions
expression/plan_cache           expression/time
expression/uuid                 expression/vitess_hash
```

Other directly relevant groups include `types/{const,json_binary_functions,time}`; root `collation_agg_func`, `collation_check_use_collation`, `collation_misc`, `collation_pointget`, `common_collation`, `new_character_set`, `new_character_set_builtin`, `new_character_set_invalid`, `generated_columns`, `null_rejected`, `select`, `subquery`, and `window_function`; executor aggregate/window/charset and planner folding/filter/join tests exercise expression consumers. These are additional compatibility surfaces, not evidence that every statement is supported. The separate `tests/integrationtest2` contains BR, dumpling/import, and TiCDC scenarios, not an interchangeable expression suite; its external-system requirements are outside this local embedding change. Cluster and RealTiKV harnesses likewise need their own deployment lifecycle.

Existing Rust integration-source carriers include `compare_time_builtin_rows_source`, `json_merge_patch_integration_source`, `advisory_get_lock_integration_source`, `helper_current_timestamp_source`, and `filter_extract_dnf_source` under `tidb-expr/src/tests`. The compare/time carrier explicitly covers literal rows, not table-column or timezone-mutation subtests. Historical `testport/receipts/b076.md` is partial seed evidence. `expression_integration_test.md` describes **newer Go master** `5e8a1a2` with 63 tests and four additional embedding tests; those four are absent from the live pinned Go file. Do not report that historical count as today's executed count. Rust `ilike_info_cast_source.rs` retains ignored `EMBED_TEXT` gaps. Preserve ignored tests and report their count/reason; do not make an all-green claim from skipped source carriers.

## Explicit Session/replay configuration

`tidb-session` feature `tikv-expr` forwards the optional expression/executor dependency features. A new `Session` remains native. Call `with_tikv_expression_backend(TikvExpressionBackend::{Copying,Borrowed})`, or `set_tikv_expression_backend(Some(...))`; `None` disables it. There is **no environment-variable lookup in production Session code**.

Enabled Session statement contexts share explicitly owned atomic counters through `StmtContext::with_tikv_expression_counters`. `Session::tikv_expression_rows()` counts successful **expression-row evaluations**, not physical SQL rows or attempts; multiple projections may count the same row. `tikv_borrowed_expression_rows()` distinguishes actual borrowed execution from copying fallback. Disabling preserves the accumulated counts. Ordinary standalone statement contexts retain separate default counters.

Only the test replay interprets `INTEGRATION_TIKV_BACKEND=native|copying|borrowed`. An absent value means native; invalid values are rejected. Copying/borrowed requests without the Cargo feature are rejected instead of silently replaying natively. Every connection receives the selection. Topic receipts include selected mode, engine rows, borrowed rows and the number of statements whose execution increased the engine counter. Disconnected/replaced sessions remain included. The enrolled-topic gate requires positive total engine rows for an explicit engine selection and zero for native. A requested borrowed mode is not a claim that all evaluations were borrowed.

The existing ignored `replay_one_topic_from_env` is **diagnostic**, not a correctness gate: it prints divergences/unaligned topics and can exit successfully despite them. Its logs are useful for un-enrolled expression topics, but cannot be called passing SQL parity tests. The enrolled-topic gate is a ratchet with pre-existing skips/divergences, not a declaration of full fixture equivalence. Always report matched rows, side effects, plan properties, skips, divergences and actual engine rows separately.

## Resource guard and environment

Run **one heavy command globally** across both checkouts, coordinating with the parent. The guard has no global lock. Keep build/native workers and test threads at one. Repository `rust/.cargo/config.toml` defaults to jobs=12 and `-Zthreads=8`; override them. From repository root `/home/agent/tidb/expression-reuse/tidb`:

```bash
export CARGO_HOME=/home/agent/tidb/expression-reuse/cargo-home
export RUSTUP_HOME=/home/agent/tidb/expression-reuse/rustup-home
export RUSTUP_TOOLCHAIN=nightly-2026-08-22
export PATH="$CARGO_HOME/bin:$PATH"
export CARGO_BUILD_JOBS=1 CMAKE_BUILD_PARALLEL_LEVEL=1 NUM_JOBS=1 RUST_TEST_THREADS=1
# Empty override suppresses repository -Zthreads=8; retain rustc's default.
export RUSTFLAGS=''
unset CARGO_ENCODED_RUSTFLAGS
export RUST_MIN_STACK=2097152 MALLOC_ARENA_MAX=2
export CMAKE_POLICY_VERSION_MINIMUM=3.5
export CXXFLAGS='-include cstdint -std=c++17'
export CARGO_TARGET_DIR=/home/agent/tidb/expression-reuse/target-tidb
limited() {
  python3 /home/agent/tidb/expression-reuse/tools/limited-run.py \
    --rss-mib 6144 --as-mib 8192 --min-available-mib 8192 -- "$@"
}
```

The smaller test stack and allocator-arena setting above are required by worker-pool topics in this constrained host; the parent observed `EAGAIN` thread-spawn failures without them. The deep-stack SQL replay helper still supplies its own deliberately larger parsing stack.

For the current unpublished engine edits, define the parent's actual development arguments in the same Bash shell:

```bash
local_engine=(--offline --config 'patch."https://github.com/YangKeao/tikv.git".tidb_query_expr.path="/home/agent/tidb/expression-reuse/tikv/components/tidb_query_expr"')
```

After publication, set `local_engine=()` and validate against the reviewed exact reachable fork revision without that local patch. Use the same build-profile environment as the main report to avoid recompiling the entire graph unnecessarily. A Session dependency-list change requires a reviewed Cargo.lock update before `--locked` validation. Do not silently validate against the old facade while the working code uses the expanded API.

### Rust gates (working directory: `rust/`)

Each command below is separate and serial. After selecting the correct dependency and updating the lockfile intentionally:

```bash
limited cargo test "${local_engine[@]}" -p tidb-expr --features tikv-expr --lib --test all -j1 --locked -- --test-threads=1
limited cargo test "${local_engine[@]}" -p tidb-executor --features tikv-expr --test all -j1 --locked -- --test-threads=1
limited cargo test "${local_engine[@]}" -p tidb-session --features tikv-expr --test all -j1 --locked tikv_expression_session -- --test-threads=1
limited cargo test "${local_engine[@]}" -p tidb-expr --lib --test all -j1 --locked -- --test-threads=1
limited cargo test "${local_engine[@]}" -p tidb-session --test all -j1 --locked tikv_expression_session -- --test-threads=1
limited cargo test "${local_engine[@]}" -p difftest-result-tests --test expr_diff -j1 --locked -- --test-threads=1
limited cargo test "${local_engine[@]}" -p difftest-result-tests --test query_diff --test table_diff --test catalog_diff -j1 --locked -- --test-threads=1
INTEGRATION_TIKV_BACKEND=native limited cargo test "${local_engine[@]}" \
  -p difftest-result-tests --features tikv-expr --test join_shape -j1 --locked \
  -- --test-threads=1 --nocapture
# Repeat join_shape with INTEGRATION_TIKV_BACKEND=copying for the baseline comparison.
```

Full enrolled mysql-tester replay, preserving the same fixtures and ratchets in each mode:

```bash
(
  failed=0
  for backend in native copying borrowed; do
    if INTEGRATION_TIKV_BACKEND="$backend" limited cargo test "${local_engine[@]}" \
      -p difftest-result-tests --features tikv-expr --test integration_diff -j1 --locked \
      -- --test-threads=1 --nocapture; then
      printf '%s: PASS\n' "$backend"
    else
      result=$?
      printf '%s: FAILED, exit=%s (retain and compare the discrepancy log)\n' "$backend" "$result"
      failed=1
    fi
  done
  exit "$failed"
)
# Feature-off harness/default behavior, with no ambient engine selection:
INTEGRATION_TIKV_BACKEND=native limited cargo test "${local_engine[@]}" \
  -p difftest-result-tests --test integration_diff -j1 --locked \
  -- --test-threads=1 --nocapture
```

Diagnostic replay for a non-enrolled topic (not a pass gate):

```bash
INTEGRATION_TIKV_BACKEND=copying INTEGRATION_TOPIC=expression/json \
INTEGRATION_SHOW_DIVERGENCES=1 limited cargo test "${local_engine[@]}" \
  -p difftest-result-tests --features tikv-expr --test integration_diff -j1 --locked \
  replay_one_topic_from_env -- --ignored --exact --test-threads=1 --nocapture
```

Run corresponding native/borrowed diagnostics for all 16 expression topics and the relevant ancillary topics above when expanding the scope; retain alignment failures instead of dropping topics. Do not use the ignored all-topic survey as a green gate: it is an inventory tool with child-process timeouts and documented engine/harness gaps.

### Original Go expression/oracle gates (working directory: repository root)

The actual existing configuration is `/home/agent/tidb/goenv.sh`: GOPATH `/home/agent/tidb/.gopath`, module cache under it, GOCACHE `.gocache`, GOTMPDIR `.gotmp`, and GOFLAGS `-mod=mod`. The unsourced shell instead points at `/home/agent/go` and a different build cache. `/usr/bin/go` reports `go1.26.5-X:nodwarf5`; `go.mod` requires 1.25.12. Avoid accidental toolchain downloads.

```bash
source /home/agent/tidb/goenv.sh
export GOTOOLCHAIN=local GOMAXPROCS=1 GOMEMLIMIT=3GiB
export GOFLAGS='-mod=mod -p=1'
export MAKEFLAGS=-j1
limited ./tools/check/failpoint-go-test.sh pkg/expression \
  -p=1 -parallel=1 -count=1 -timeout=30m
# Explicit parent-controlled retry budget after the 8 GiB AS exhaustion:
python3 /home/agent/tidb/expression-reuse/tools/limited-run.py \
  --rss-mib 6144 --as-mib 16384 --min-available-mib 8192 -- \
  ./tools/check/failpoint-go-test.sh pkg/expression/integration_test \
  -p=1 -parallel=1 -count=1 -timeout=30m
# Broader requested package coverage, including children; no parallel invocations:
limited ./tools/check/failpoint-go-test.sh pkg/expression \
  -p=1 -parallel=1 -count=1 -timeout=30m ./...
```

This preserves the parent's actual existing writable module mode; inspect and report any `go.mod`/`go.sum` change and apply the repository's Bazel preparation rule if necessary. Use `-mod=readonly -p=1` instead when module changes must be prohibited, reporting missing dependencies rather than silently fixing them. `tools/bin/failpoint-ctl` was absent at inventory time; the wrapper's `make failpoint-enable` target builds it, then enables failpoints, runs `go test` with default tags `intest,deadlock`, and disables them on exit. Tags alone do not enable failpoints. This mutates source temporarily: serialize the entire operation with other Go readers/builds. If the resource guard kills the process group before cleanup completes, verify/restore failpoint state with guarded `make failpoint-disable` before proceeding. Keep the guard enabled; any budget change must be explicit and recorded. The integration retry above changes only the per-process address-space ceiling to the parent's stated 16 GiB budget, rather than removing the RSS/host-reserve safeguards.

For a narrower diagnostic integration run, use the same wrapper and limits with `-run 'TestCompareBuiltin|TestTimeBuiltin|TestBuiltinFuncJSONMergePatch|TestVectorFunctions|TestCastJSONTimeDuration'`; this does not replace the requested broad integration run.

### Original executable mysql-tester harness

At inventory time `tests/integrationtest/mysql_tester`, `tests/integrationtest/integrationtest_tidb-server`, and `bin/tidb-server` were absent. This harness needs compatible binaries, `unzip` and statistics fixture `s.zip`, available ports, and storage setup. Building or downloading them is a separate guarded task, not part of read-only inventory. `-b n` is not sufficient proof that no build occurs: the script can build a missing server. Check prerequisites first.

With an explicitly provisioned test server and tester (not a user's live database), from `tests/integrationtest/`, using the `limited` function and Go environment above:

```bash
test -x ./mysql_tester && test -x ./integrationtest_tidb-server && \
  TIDB_TEST_STORE_NAME=unistore TIKV_PATH='' NEXT_GEN='' \
  limited ./run-tests.sh -b n -s ./integrationtest_tidb-server -t expression/builtin
```

Repeat `-t` for the complete selected expression topic set. **Never use `-r` for validation**: it rewrites recorded oracles. The runner sets `TZ=Asia/Shanghai` and handles new/old collation recordings; do not compare a different collation recording accidentally. Original Go/server success does not prove Session opt-in or TiKV engine execution in Rust.

## Remaining acceptance work and reporting

The parent reported baseline failures during this work: the native enrolled integration ratchet has **142 divergences**, and `expr_diff` fails on `export_set`. These are not a green baseline and must be compared with explicit-backend runs, not erased or attributed to this adapter without a minimized reproduction. Consult the parent's final logs for exact receipts; the Session/harness agent did not independently execute these heavy runs.

For every run record exact command/environment, dependency revision, exit status, passed/failed/ignored counts, guard peak/reason, elapsed time, and backend row receipts. Preserve metadata, signed/unsigned boundaries, decimal scale, collation/raw bytes, timezone/FSP, SQL NULL versus JSON null, vector shape, warnings and lazy-error exclusions. Engine selection does not excuse differences from the original Go oracle.

No source-port ignore, out-of-domain skip, feature-only compilation, diagnostic survey success, or enum dispatch candidate may be counted as demonstrated adapter parity. Root `make -j1 lint` is required before readiness for code changes; it has not been run by the Session/harness agent. No Go/Bazel files changed in this subtask, so it does not itself trigger `make bazel_prepare`; module or Go-source changes during subsequent work would change that decision. No original `.test` or `.result` oracle was edited.
