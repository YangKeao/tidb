# TTL Index Scan Optimization

## Background

TTL scan tasks split and paginate by **primary key** order:

```sql
SELECT LOW_PRIORITY SQL_NO_CACHE `id` FROM `test`.`t`
WHERE `id` >= ? AND `id` < ? AND `created_time` < FROM_UNIXTIME(?)
ORDER BY `id` ASC LIMIT ?;
```

When the optimizer uses a secondary index on the TTL column, this query structure causes redundant scans: each subtask (default 64) independently scans the full TTL-column index range, and every pagination page (default 500 rows) re-sorts by primary key. For tables with many expired rows, the total index scans can exceed a full table scan.

## Goals

- Use a physical index whose first column is the TTL column as the scan ordering and split boundary.
- Provide a global variable to enable/disable the optimization.
- Fall back to PK-based scan when no suitable index exists.

## Non-goals

- Composite indexes where the TTL column is not the prefix.
- Per-table or per-column configuration.
- Dynamic cost-based switching.

## Design

### Global Variable

| Variable | Scope | Default | Description |
|---|---|---|---|
| `tidb_ttl_enable_index_scan` | Global | `ON` | Enable index-based TTL scan. |

### Index Selection

A usable index must be an ordinary public, visible, full-column physical index whose first column is the TTL column. This includes secondary indexes and nonclustered primary indexes. A clustered primary key remains on the PK scan path because it is the table path itself. When its first column is the TTL column, temporal Region boundaries can split that table path directly; pagination still follows the complete clustered-key order. Pagination follows the selected physical path's order instead of always assuming that the table key immediately follows the TTL column. Here, the table key means the clustered primary key, or `_tidb_rowid` for a table without a clustered primary key.

The supported pagination layouts are:

| Index shape | Pagination tuple | Additional requirement |
|---|---|---|
| Non-unique `(ttl)` | `(ttl, table_key...)` | The planner must be able to expose the implicit table-key suffix in index order. |
| Unique `(ttl)` | `(ttl)` | None. The expiration predicate excludes `NULL` TTL values. |
| Non-unique `(ttl, index_columns...)` containing the complete table key | The declared index columns | The table-key columns may appear in any declared order. |
| Non-unique `(ttl, index_columns...)` containing no table-key column | The declared index columns followed by the complete table key | The planner must be able to expose the implicit table-key suffix in index order. Nullable declared columns are supported. |
| Unique `(ttl, index_columns...)` | The declared index columns | Every non-TTL index column must be `NOT NULL`. The index may contain all, part, or none of the table key. |

For a non-unique index, the cursor needs the complete table key unless the declared index tuple already contains it and is therefore row-unique. When the cursor relies on the implicit physical table-key suffix, the following handle layouts are supported:

- `_tidb_rowid`;
- a signed integer clustered primary key;
- a common handle without prefix primary-key columns, except a version-0 common handle containing non-binary string columns when the new collation framework is enabled.

An unsigned integer clustered primary key cannot be used as an implicit suffix because its physical suffix order cannot satisfy the SQL order currently exposed by the planner. This restriction does not apply when the full key is explicitly present among the declared index columns.

Cursor literals must also preserve the selected path's physical order. `ENUM` is supported by writing its ordinal in range and cursor predicates. If the final pagination tuple contains `SET`, `FLOAT`, or `DOUBLE`, the secondary index is rejected: TiDB cannot currently derive the required ordered range from a `SET` bitmask comparison, while a decimal SQL literal cannot reliably reproduce every scanned floating-point value as a strict cursor frontier. This restriction is based on the final pagination tuple, so it also covers table-key columns appended as an implicit index suffix. A clustered common handle containing `SET` remains correct on the PK path by using its bitmask as the cursor, although the planner may not provide ideal range pushdown.

For an integer `PKIsHandle` table, the primary key is encoded directly in the table record key and normally has no `IndexInfo` entry. It therefore remains on the existing signed/unsigned integer PK scan path. A secondary TTL index on such a table is selected independently according to the implicit-handle-suffix restrictions above. In contrast, a nonclustered primary key has its own physical index KV and is handled as a unique, all-`NOT NULL` index.

For a common-handle clustered primary key whose first column is the TTL column, both a single-column key `(ttl)` and a composite key `(ttl, remaining_pk_columns...)` use temporal PK task boundaries. A Region boundary inside a later common-handle column maps to its leading TTL value. Multiple Regions within the same TTL value may therefore collapse into one SQL range; this can reduce split balance but does not create gaps or overlap.

The following cases are unsupported and fall back to the existing PK scan:

- the TTL column is not the first index column;
- clustered primary, invisible, non-public, global, multi-valued, columnar, or conditional indexes;
- prefix index columns or hidden expression columns;
- a unique composite index with a nullable non-TTL column, because SQL permits multiple rows with the same tuple through `NULL` values;
- a non-unique composite index containing only part of the table key, because the current cursor layout cannot represent the remaining implicit suffix in physical order;
- a non-unique index that needs one of the unsupported implicit handle suffixes described above.
- an index whose final pagination tuple contains `SET`, `FLOAT`, or `DOUBLE`.

The scheduler calls `PhysicalTable.FindTTLIndex()` at job creation time. If the selected index is dropped later, the worker reports an error for the affected task.

### Scan SQL

**PK scan (existing behavior):**
```sql
SELECT LOW_PRIORITY SQL_NO_CACHE `id` FROM `test`.`t` USE INDEX ()
WHERE `id` >= ? AND `id` < ? AND `created_time` < FROM_UNIXTIME(?)
ORDER BY `id` ASC LIMIT ?;
```

**Non-unique index scan for the first page of a split task:**
```sql
SELECT LOW_PRIORITY SQL_NO_CACHE `created_time`, `status`, `id` FROM `test`.`t` FORCE INDEX(`idx_created`)
WHERE `created_time` >= ? AND `created_time` < ? AND `created_time` < FROM_UNIXTIME(?)
ORDER BY `created_time` ASC, `status` ASC, `id` ASC LIMIT ?;
```

**Pagination within the same task:**
```sql
SELECT LOW_PRIORITY SQL_NO_CACHE `created_time`, `status`, `id` FROM `test`.`t` FORCE INDEX(`idx_created`)
WHERE `created_time` = ? AND `status` = ? AND `id` > ?
  AND `created_time` < ? AND `created_time` < FROM_UNIXTIME(?)
ORDER BY `created_time` ASC, `status` ASC, `id` ASC LIMIT ?;
```

The `FORCE INDEX` hint prevents the optimizer from choosing a different access index. Pagination uses a stack of cursor prefixes. For a cursor `(created_time, status, id)`, it first scans `created_time = ? AND status = ? AND id > ?`. When that query returns fewer rows than its limit, the current prefix is exhausted and the generator pops one stack level to scan `created_time = ? AND status > ?`, then finally `created_time > ?`. Each query therefore contains an equality prefix followed by at most one range condition. The supported pagination tuples above match the selected index's physical order, so the planner can use one ordered index range scan without a per-page `TopN`.

Conversely, `USE INDEX ()` in the PK-scan SQL prevents the optimizer from unexpectedly selecting a separate index path and repeatedly sorting it by primary-key order.

Each index task scans one TTL-column range `[start, end)`. The first page applies the lower and upper bounds, and later pages continue from the last index-order tuple while keeping the upper bound. If a fixed cursor prefix contains `NULL`, it uses `IS NULL`; advancing past a `NULL` frontier uses `IS NOT NULL`, matching TiDB's ascending, NULL-first index order. The scan result contains both cursor columns and the table key required by the delete phase.

`ENUM` and `SET` are ordered physically by numeric representation rather than display text. Cursor and task-range predicates therefore write an `ENUM` ordinal or a `SET` bitmask. Delete key equality predicates continue to use the exact SQL value returned by the scan. Secondary indexes requiring a `SET` cursor are not selected for the planner reason described above, but the numeric cursor keeps clustered common-handle PK pagination logically correct.

For a unique index, pages order and seek only by the declared index columns. The TTL predicate excludes a `NULL` TTL value, so a unique single-column TTL index needs no extra cursor suffix even when the TTL column is nullable. A composite unique index is used only when every non-TTL index column is `NOT NULL`; otherwise, multiple rows could share the same tuple through `NULL` values and the table falls back to PK scan. Non-unique indexes remain usable with nullable columns because their pagination tuple includes the table key and the stack conditions follow TiDB's NULL-first index order.

Pages are separate SQL statements and do not share one snapshot. If an indexed value changes while a task is scanning, a row can move across the cursor and be skipped by the current job or observed again. The delete statement rechecks the expiration condition, so this cannot delete a row that is no longer expired; a skipped expired row remains eligible for a later TTL job. The scan therefore provides safe, eventually repeated processing rather than an exactly-once traversal under concurrent index-column updates.

### Task Splitting

- **PK scan:** tasks are split by PK ranges; `split_by` is `NULL`. A clustered PK beginning with the TTL column uses one-column temporal boundaries while retaining full clustered-PK pagination order.
- **Index scan:** tasks are split by the selected physical index's TiKV region distribution; `split_by` stores the selected **index ID** (`bigint`).

The `split_by` column in `mysql.tidb_ttl_task` is added as `bigint DEFAULT NULL`. Workers read it to decide which ordering to use. A non-`NULL` value is interpreted as the index ID; if the index no longer exists when the task runs, the worker returns an error for that task.

In PK scan mode, `scan_range_start` and `scan_range_end` encode primary-key boundaries. For a temporal clustered PK they contain the first PK column as a textual SQL temporal value. This keeps the task compatible with existing PK workers, which already accept string datums as range literals. In index scan mode, the ranges contain the TTL-column boundary decoded from the selected index's Region boundaries. Persisting that temporal datum with `codec.EncodeKey` flattens it to a packed integer, so index task execution uses table metadata to restore the TTL column type before generating SQL.

For index scan splitting, the scheduler locates TiKV regions in the selected index's raw key range from `MinNotNull` to the encoded expire time. The lower bound excludes `NULL` TTL-column entries, which cannot satisfy `ttl_col < expire`; the upper bound is exclusive, so entries with `ttl_col == expire` are not included. For a clustered PK beginning with the TTL column, it similarly locates only the record-key range from the table record prefix to `record_prefix + encoded_expire_time`. A Region that crosses the expire key is retained because it may contain expired rows, while Regions wholly after that key do not participate in subtask grouping.

Region boundaries are arbitrary byte strings and may truncate a temporal datum or encode an invalid calendar value. Both physical paths map each boundary to the greatest legal temporal value whose complete encoded key is no greater than the Region boundary. The resulting SQL ranges stay adjacent even when the physical and SQL split points are not identical. If the store is not TiKV or the Region split cannot produce useful boundaries, the scheduler falls back to one full range on the selected scan path.

### Time Zone Protocol

TTL scan and delete statements execute in a pooled session whose session time zone is UTC for the entire borrowed lifetime. In particular, a `TIMESTAMP` range boundary and pagination cursor is interpreted as one UTC instant; it therefore remains unambiguous when a named global time zone repeats a wall-clock value during a daylight-saving-time fold. Temporal cursor predicates continue to compare the bare column with an ordinary constant, so the planner can retain range seek and index order without a conversion function on the indexed column.

The TTL expiration frontier still follows the global time zone captured for the scan task. `TIMESTAMP` stores an instant, so its predicate uses the corresponding Unix instant in the UTC session. `DATE` and `DATETIME` use wall-clock semantics, so their predicate writes the captured global-time-zone wall clock as a `DATETIME` constant instead of converting that epoch to a UTC wall clock. Scan and delete use the same captured frontier, and both retain the strict `ttl_column < cutoff` predicate; the delete therefore rechecks expiration if a row changes after it was scanned.

The borrowed session's original time zone and other TTL-specific variables are restored exactly before returning it to the shared pool. Setup failure performs best-effort restoration and makes the session non-reusable. Restoration attempts all variables even after one failure, and any failure also makes the session non-reusable, preventing TTL's UTC protocol or execution settings from leaking to another pool user.

## Compatibility

- `tidb_ttl_enable_index_scan` defaults to `ON`. Tables with a suitable TTL-column secondary or nonclustered primary index will use index-ordered scans. Turning it off does not disable temporal splitting on a clustered primary key because that optimization remains on the table's PK path.
- `split_by` defaults to `NULL`, compatible with old tasks.
- Before creating a TTL job, the TTL manager compares the normalized semantic versions of the current TiDB and all TiDB instances registered in server info. Prerelease labels, build metadata, and Git hashes are ignored. The current process's runtime version is always the comparison baseline, even if its etcd entry is temporarily absent. When mixed versions are visible, the manager skips creating the job so an old worker cannot interpret index boundaries as PK boundaries during a rolling upgrade. This is a best-effort compatibility gate: server-info lookup, an empty result, or version parsing failures are logged, and the job is allowed to use only the PK scan path (`split_by` remains `NULL`). Only a successful, consistent version check enables index scan tasks. Temporal clustered-PK splitting does not depend on this gate because it keeps the existing PK task shape and stores old-worker-compatible textual range boundaries. Non-blocking results are cached for 10 seconds to coalesce bursts of job creation; a detected mismatch is cached for one minute to avoid frequent etcd reads while the timer retries the job.
- The UTC worker-session protocol does not add a task field, encoding version, server-info call, or feature gate. During an undetected mixed-version interval, an old worker may leave part of a new task unprocessed, but every delete still checks the expiration predicate and a later TTL job can process the omitted expired rows.
- No TiKV or protocol changes.
