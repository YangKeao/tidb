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

The command blocks active distributed/import work, binding text that cannot fit
in v8.5.2, and PITR rows that cannot satisfy the v8.5.2 three-column primary
key. It does not replace the external requirements above.

`apply` uses no local progress file and stores no per-operation state. One full
preflight validates global blockers, then every invocation runs the complete
manifest from v251 through v239. `DROP ... IF EXISTS`, `DELETE`, and binding
`MODIFY COLUMN` are replayed directly. PITR primary-key operations inspect their
immediate target condition and become no-ops when it is already satisfied. Any SQL error
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

# 中文说明

`tidb-partial-index-rollback` 是一个仅用于特定回退路径的维护工具：

- 源版本是 `origin/partial-index-planner` 的
  `2cf9f0e14b157373c034a90c88c2534427d99654`，bootstrap version 为 `251`；
- 目标版本是官方 TiDB v8.5.2 的
  `f43a13324440f92209e2a9f04c0bbe9cf763978d`，bootstrap version 为 `220`；
- 仅支持 Dedicated/API-v1 TiKV 集群，不支持 keyspace。

工具不会启停 TiDB，也不会自动修改用户 schema 或用户数据。如果用户表中仍有
partial index，`check` 会将其报告为 blocker，必须由操作方自行删除或转换。工具只
回滚经过审查的 `mysql.*` 变更，并归一化 SQL 和 KV 中的 bootstrap metadata。

## 构建

在本仓库对应 revision 下执行：

```bash
go build -o tidb-partial-index-rollback ./cmd/tidb-partial-index-rollback
```

SQL 密码通过环境变量提供，避免出现在 shell history 中：

```bash
export TIDB_ROLLBACK_PASSWORD='<password>'
```

## 只读检查

```bash
./tidb-partial-index-rollback \
  --mode check \
  --path '<pd-1>:2379,<pd-2>:2379,<pd-3>:2379' \
  --host '<tidb-host>' \
  -P 4000 \
  --user root
```

`check` 会读取 SQL/KV bootstrap version、在线 TiDB 版本、DDL 状态、TiKV schema
metadata、partial index 和相关系统表状态。存在问题时会输出 `BLOCKER` 并返回非零
退出码。只有输出 `RESULT: READY` 后才能执行 `apply`。

TLS 集群使用 `--cluster-ca/--cluster-cert/--cluster-key` 连接 PD/TiKV，使用
`--sql-ca/--sql-cert/--sql-key` 连接 TiDB SQL endpoint。

## 执行前提

工具无法验证以下外部条件。执行 `apply` 前，操作方必须逐项确认：

- 已创建并验证可用的备份或快照，并保留证据；
- 整个回退窗口内冻结所有 schema DDL；
- 冻结 TiDB 启停、扩缩容和二进制替换；
- 已检查 branch-only `mysql.*` 对象，并接受这些对象及其专有系统数据被删除；
- `check`、`apply` 和 `verify` 始终连接同一个 PD 集群。

正常业务 DML 可以继续，但不能执行 schema DDL。

## 执行回退

```bash
./tidb-partial-index-rollback \
  --mode apply \
  --path '<pd-1>:2379,<pd-2>:2379,<pd-3>:2379' \
  --host '<tidb-host>' \
  -P 4000 \
  --user root
```

`apply` 会先执行完整 preflight，然后从头到尾执行固定、幂等的系统表回滚清单。
系统表达到 v8.5.2 目标状态后，工具才会把 SQL/KV bootstrap version 从 `251`
归一化为 `220/220`。工具不保存本地进度或单步状态；如果中途失败，应重新运行
`check`，再使用相同参数重新运行 `apply`，不要手工猜测补偿 SQL。

## 最终验证

`apply` 完成后，以及官方 v8.5.2 TiDB 节点重启后，分别运行：

```bash
./tidb-partial-index-rollback \
  --mode verify \
  --path '<pd-1>:2379,<pd-2>:2379,<pd-3>:2379' \
  --host '<tidb-host>' \
  -P 4000 \
  --user root
```

成功条件包括：所有在线 TiDB 都是指定的官方 v8.5.2 build，不存在 partial-index
metadata，`mysql.*` 系统表满足 v8.5.2 目标结构，并且 SQL/KV bootstrap version
均为 `220`。
