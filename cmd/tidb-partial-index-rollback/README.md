# TiDB partial-index rollback tool

`tidb-partial-index-rollback` is a fixed-purpose maintenance command for one
upgrade path only:

- source: `origin/partial-index-planner` commit
  `2cf9f0e14b157373c034a90c88c2534427d99654`, bootstrap version 251;
- target: official TiDB v8.5.2 commit
  `f43a13324440f92209e2a9f04c0bbe9cf763978d`, bootstrap version 220;
- deployment: Dedicated/API-v1 TiKV, without keyspaces.

The command does not start or stop TiDB and does not modify customer schemas or
customer data. A customer partial index is a blocker that must be removed or
converted manually. After all blockers are resolved, `apply` rolls back only
the reviewed `mysql.*` changes and normalizes SQL/KV bootstrap metadata.

## Build

Build from this repository revision:

```bash
go build -o tidb-partial-index-rollback ./cmd/tidb-partial-index-rollback
```

## Check

The flags intentionally match common `tidb-server` connection flags:

```bash
./tidb-partial-index-rollback \
  --mode check \
  --path '<pd-1>:2379,<pd-2>:2379,<pd-3>:2379' \
  --host '<tidb-host>' \
  -P 4000 \
  --user root
```

Set the SQL password through `TIDB_ROLLBACK_PASSWORD` instead of putting it in
shell history. For TLS clusters, use `--cluster-ca`, `--cluster-cert`, and
`--cluster-key` for PD/TiKV and `--sql-ca`, `--sql-cert`, and `--sql-key` for
the TiDB SQL endpoint.

`check` is read-only. It prints:

- SQL and KV bootstrap versions;
- `ACTION` lines for fixed `mysql.*` changes that `apply` will perform;
- `BLOCKER` lines for customer partial indexes, active work, target-schema
  constraints, unsupported binaries, or unexpected metadata.

Exit status is non-zero when blockers exist. Resolve every blocker and rerun
`check` until it prints `RESULT: READY`.

## Apply

Before running `apply`, the operator must satisfy all of the following external
requirements. The command cannot verify them:

- create and review a usable backup or snapshot, and retain its evidence;
- freeze all schema DDL for the entire rollback window;
- freeze new `IMPORT INTO` submissions and independently confirm that no import
  job is still executing; the command intentionally does not infer completion
  from `mysql.tidb_import_jobs.end_time`;
- freeze TiDB start, restart, scale-out, scale-in, and binary replacement;
- review the branch-only `mysql.*` objects and accept that those objects and
  the system data stored only in them will be deleted.

Normal business DML may continue. After these requirements are in place, run:

```bash
./tidb-partial-index-rollback \
  --mode apply \
  --path '<pd-1>:2379,<pd-2>:2379,<pd-3>:2379' \
  --host '<tidb-host>' \
  -P 4000 \
  --user root
```

The command requires `mysql.tidb_global_task` to be empty and blocks PITR rows
that cannot satisfy the v8.5.2 three-column primary key. It does not check
whether `IMPORT INTO` jobs have finished and does not replace the external
requirements above.

`--force` is valid only with `--mode apply`. It still runs and prints the full
preflight report, but continues after reported blockers:

```bash
./tidb-partial-index-rollback \
  --mode apply \
  --force \
  --path '<pd-1>:2379,<pd-2>:2379,<pd-3>:2379' \
  --host '<tidb-host>' \
  -P 4000 \
  --user root
```

Use it only after every printed blocker has been reviewed and accepted. It does
not bypass errors that prevent preflight from completing, rollback SQL errors,
the post-system-schema check, or the final bootstrap version guards.

The v245 widening of `mysql.bind_info.original_sql` and `bind_sql` to
`LONGTEXT` is intentionally retained. Official v8.5.2 is compatible with the
wider columns, so no binding-length blocker or narrowing DDL is required.

`apply` uses no local progress file and stores no per-operation state. One full
preflight validates global blockers, then every invocation runs the complete
manifest from v251 through v239. `DROP ... IF EXISTS` and `DELETE` are replayed
directly. PITR primary-key operations inspect their immediate target condition
and become no-ops when it is already satisfied. Any SQL error
stops with instructions to rerun `check` and the same `apply` command. One full
system-schema check verifies the complete target schema before the bootstrap
update. After that update, only SQL/KV bootstrap version `220/220` is checked.

On restart, the same manifest starts again from its first operation. Every
operation is idempotent either through SQL syntax or a local guard, including
the three PITR mutations. Objects removed by v8.5.2 are dropped by name
regardless of their source definition or current existence; the post-SQL check
verifies only the required target system-schema state.

The final SQL/KV update accepts only values 251 and 220. The KV write checks
the current value and calls `meta.Mutator.FinishBootstrap(220)` in one TiKV
transaction. On an uncertain commit result, KV readback determines whether the
SQL row is kept at 220 or restored to 251.

## Verify

After `apply`, and again after restarting an official v8.5.2 TiDB node, run:

```bash
./tidb-partial-index-rollback \
  --mode verify \
  --path '<pd-1>:2379,<pd-2>:2379,<pd-3>:2379' \
  --host '<tidb-host>' \
  -P 4000 \
  --user root
```

Success requires official v8.5.2 Git hashes, no partial-index metadata, the
reviewed v8.5.2 `mysql.*` target invariants, and SQL/KV bootstrap version
`220/220`.
