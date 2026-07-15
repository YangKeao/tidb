// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
)

type partialIndex struct {
	database  string
	table     string
	index     string
	predicate string
	unique    bool
	columns   []string
}

type kvInspection struct {
	partial   []partialIndex
	nonPublic []string
}

type persistedTableInfo struct {
	Name    persistedCIStr       `json:"name"`
	Indices []persistedIndexInfo `json:"index_info"`
}

type persistedIndexInfo struct {
	Name             persistedCIStr         `json:"idx_name"`
	Columns          []persistedIndexColumn `json:"idx_cols"`
	Unique           bool                   `json:"is_unique"`
	LegacyCondition  string                 `json:"partial_condition_expr_string"`
	CurrentCondition string                 `json:"condition_expr_string"`
}

type persistedIndexColumn struct {
	Name persistedCIStr `json:"name"`
}

type persistedCIStr struct {
	O string `json:"O"`
}

type columnSignature struct {
	dataType    string
	columnTypes []string
	nullable    bool
	defaultVal  *string
}

type columnDefinition struct {
	dataType   string
	columnType string
	nullable   bool
	defaultVal sql.NullString
}

func (r *runner) check(ctx context.Context) error {
	report, err := r.preflight(ctx)
	if err != nil {
		return err
	}
	report.print(r.out)
	return report.error()
}

func (r *runner) verify(ctx context.Context) error {
	report := &report{}
	if err := r.readBootstrapVersions(ctx, report); err != nil {
		return err
	}
	checkTargetBootstrapVersions(report)
	if err := r.checkClusterState(ctx, report); err != nil {
		return err
	}
	r.checkRollbackBlockers(ctx, report)
	r.checkSystemSchema(ctx, report)
	report.print(r.out)
	return report.error()
}

func (r *runner) verifyBootstrap(ctx context.Context) error {
	report := &report{}
	if err := r.readBootstrapVersions(ctx, report); err != nil {
		return err
	}
	checkTargetBootstrapVersions(report)
	report.print(r.out)
	return report.error()
}

func (r *runner) preflight(ctx context.Context) (*report, error) {
	report := &report{}
	if err := r.readBootstrapVersions(ctx, report); err != nil {
		return nil, err
	}
	checkSupportedBootstrapVersions(report)
	if err := r.checkClusterState(ctx, report); err != nil {
		return nil, err
	}
	r.checkTargetInvariants(ctx, report)
	r.checkRollbackBlockers(ctx, report)
	for _, operation := range r.rollbackOperations() {
		report.action("v%d: %s", operation.version, operation.name)
	}
	return report, nil
}

func (r *runner) readBootstrapVersions(ctx context.Context, report *report) error {
	sqlVersion, err := queryBootstrapVersion(ctx, r.db)
	if err != nil {
		return fmt.Errorf("read SQL bootstrap version: %w", err)
	}
	report.sqlBootstrap = sqlVersion
	kvVersion, err := r.readKVBootstrap()
	if err != nil {
		return fmt.Errorf("read KV bootstrap version: %w", err)
	}
	report.kvBootstrap = kvVersion
	return nil
}

func (r *runner) checkClusterState(ctx context.Context, report *report) error {
	// Read authoritative TiKV schema metadata: non-public objects and
	// partial-index predicates that official v8.5.2 cannot decode.
	kvState, err := r.inspectKVMetadata(ctx)
	if err != nil {
		return fmt.Errorf("inspect TiKV metadata: %w", err)
	}

	// Require every online TiDB node to be the reviewed official v8.5.2 build.
	r.checkTiDBNodes(ctx, report)

	// Ensure no schema DDL is active or queued before system-table mutations.
	r.checkZeroRows(ctx, report, `SELECT COUNT(*) FROM mysql.tidb_ddl_job`,
		"cannot inspect active DDL job table", "mysql.tidb_ddl_job contains %d active or queued DDL job(s)")

	// Catch schema objects left in an intermediate DDL state even if the SQL
	// DDL queue changed between the preceding query and the metadata snapshot.
	for _, item := range kvState.nonPublic {
		report.block("non-public metadata object %s; finish or cancel the DDL first", item)
	}

	// Customer partial indexes cannot be represented safely by v8.5.2 and must
	// be removed or converted manually; this tool never changes them.
	for _, index := range kvState.partial {
		kind := "INDEX"
		if index.unique {
			kind = "UNIQUE INDEX"
		}
		report.block("partial %s %s.%s.%s (%s) WHERE %s; remove or convert it manually", kind, index.database, index.table, index.index, strings.Join(index.columns, ", "), index.predicate)
	}
	return nil
}

func (r *runner) checkRollbackBlockers(ctx context.Context, report *report) {
	// Block work that can race with or make the fixed rollback lossy beyond the
	// accepted branch-only system data.
	r.checkZeroRows(ctx, report, `SELECT COUNT(*) FROM mysql.tidb_global_task`,
		"cannot inspect active distributed tasks", "mysql.tidb_global_task contains %d active distributed task(s)")
	r.checkZeroRows(ctx, report, `SELECT COUNT(*) FROM mysql.tidb_import_jobs WHERE end_time IS NULL`,
		"cannot inspect unfinished IMPORT INTO jobs", "mysql.tidb_import_jobs contains %d unfinished IMPORT INTO job(s)")
	r.checkZeroRows(ctx, report, `SELECT COUNT(*) FROM mysql.bind_info WHERE OCTET_LENGTH(original_sql)>65535 OR OCTET_LENGTH(bind_sql)>65535`,
		"cannot inspect binding text lengths", "mysql.bind_info contains %d binding(s) that cannot fit in TEXT")
	r.checkZeroRows(ctx, report, `SELECT COUNT(*) FROM (SELECT restored_ts, upstream_cluster_id, segment_id FROM mysql.tidb_pitr_id_map GROUP BY restored_ts, upstream_cluster_id, segment_id HAVING COUNT(*)>1) AS duplicate_keys`,
		"cannot inspect PITR target primary-key duplicates", "mysql.tidb_pitr_id_map contains %d duplicate v8.5.2 primary key(s)")
}

func (r *runner) checkSystemSchema(ctx context.Context, report *report) {
	r.checkTargetInvariants(ctx, report)
	r.checkRollbackTarget(ctx, report)
}

func (r *runner) checkTargetInvariants(ctx context.Context, report *report) {
	for _, table := range []string{"tidb_global_task", "tidb_global_task_history", "tidb_import_jobs", "tidb_pitr_id_map", "bind_info", "analyze_jobs", "stats_meta"} {
		exists, err := r.tableExists(ctx, table)
		if err != nil {
			report.block("cannot inspect required mysql.%s table: %v", table, err)
			continue
		}
		if exists {
			continue
		}
		report.block("required v8.5.2 system table mysql.%s is missing", table)
	}
	r.requireIndex(ctx, report, "analyze_jobs", "idx_schema_table_state", true, "table_schema", "table_name", "state")
	r.requireIndex(ctx, report, "analyze_jobs", "idx_schema_table_partition_state", true, "table_schema", "table_name", "partition_name", "state")
	r.requireColumn(ctx, report, "stats_meta", "last_stats_histograms_version",
		columnSignature{"bigint", []string{"bigint unsigned", "bigint(20) unsigned"}, true, nil})
	for _, column := range []string{"sql_digest", "plan_digest"} {
		r.requireColumn(ctx, report, "bind_info", column, columnSignature{"varchar", []string{"varchar(64)"}, true, nil})
	}
}

func (r *runner) checkRollbackTarget(ctx context.Context, report *report) {
	r.checkZeroRows(ctx, report, `SELECT COUNT(*) FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA='mysql' AND (TABLE_NAME, COLUMN_NAME) IN (
  ('tidb_import_jobs','group_key'), ('tidb_global_task','keyspace'),
  ('tidb_global_task_history','keyspace'), ('tidb_pitr_id_map','restore_id'),
  ('user','Max_user_connections'), ('tidb_global_task','extra_params'),
  ('tidb_global_task_history','extra_params'), ('tidb_global_task','max_node_count'),
  ('tidb_global_task_history','max_node_count'), ('tidb_global_task','modify_params'),
  ('tidb_global_task_history','modify_params'))`,
		"cannot inspect removed mysql columns", "%d branch-only mysql column(s) still exist")
	r.checkZeroRows(ctx, report, `SELECT COUNT(*) FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA='mysql' AND (TABLE_NAME, INDEX_NAME) IN (
  ('tidb_import_jobs','idx_group_key'), ('tidb_global_task','idx_keyspace'),
  ('tidb_global_task_history','idx_keyspace'), ('bind_info','digest_index'),
  ('user','i_user'), ('global_priv','i_user'), ('db','i_user'),
  ('tables_priv','i_user'), ('columns_priv','i_user'),
  ('global_grants','i_user'), ('default_roles','i_user'))`,
		"cannot inspect removed mysql indexes", "%d branch-only mysql index column(s) still exist")
	r.checkZeroRows(ctx, report, `SELECT COUNT(*) FROM information_schema.TABLES
WHERE TABLE_SCHEMA='mysql' AND TABLE_NAME IN ('tidb_restore_registry','tidb_workload_values')`,
		"cannot inspect removed mysql tables", "%d branch-only mysql table(s) still exist")

	r.requireColumn(ctx, report, "bind_info", "original_sql", columnSignature{"text", []string{"text"}, false, nil})
	r.requireColumn(ctx, report, "bind_info", "bind_sql", columnSignature{"text", []string{"text"}, false, nil})
	r.requireIndex(ctx, report, "tidb_pitr_id_map", "PRIMARY", false, "restored_ts", "upstream_cluster_id", "segment_id")
	r.checkZeroRows(ctx, report, `SELECT COUNT(*) FROM mysql.tidb WHERE VARIABLE_NAME='cluster_id'`,
		"cannot inspect mysql.tidb cluster_id", "mysql.tidb still contains %d cluster_id row(s)")
}

func (r *runner) requireColumn(ctx context.Context, report *report, table, column string, expected columnSignature) {
	actual, err := r.readColumn(ctx, table, column)
	if err != nil {
		report.block("cannot inspect mysql.%s.%s: %v", table, column, err)
		return
	}
	if actual == nil {
		report.block("required mysql.%s.%s column is missing", table, column)
		return
	}
	if !expected.matches(*actual) {
		report.block("mysql.%s.%s has unexpected definition: %s", table, column, actual.describe())
	}
}

func (r *runner) readColumn(ctx context.Context, table, column string) (*columnDefinition, error) {
	var actual columnDefinition
	var nullable string
	err := r.db.QueryRowContext(ctx, `SELECT DATA_TYPE, COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='mysql' AND TABLE_NAME=? AND COLUMN_NAME=?`, table, column).
		Scan(&actual.dataType, &actual.columnType, &nullable, &actual.defaultVal)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	actual.dataType = strings.ToLower(actual.dataType)
	actual.columnType = strings.ToLower(actual.columnType)
	actual.nullable = strings.EqualFold(nullable, "YES")
	return &actual, nil
}

func (r *runner) requireIndex(ctx context.Context, report *report, table, index string, nonUnique bool, columns ...string) {
	actual, err := r.readIndex(ctx, table, index)
	if err != nil {
		report.block("cannot inspect mysql.%s.%s: %v", table, index, err)
		return
	}
	if actual == nil {
		report.block("required mysql.%s.%s index is missing", table, index)
		return
	}
	if !indexMatches(*actual, nonUnique, columns...) {
		report.block("mysql.%s.%s has unexpected definition", table, index)
	}
}

func (r *runner) checkZeroRows(ctx context.Context, report *report, query, inspectError, nonZeroError string) {
	count, err := queryCount(ctx, r.db, query)
	if err != nil {
		report.block("%s: %v", inspectError, err)
		return
	}
	if count == 0 {
		return
	}
	report.block(nonZeroError, count)
}

func queryCount(ctx context.Context, db *sql.DB, query string, args ...any) (int64, error) {
	var count int64
	err := db.QueryRowContext(ctx, query, args...).Scan(&count)
	return count, err
}

func (r *runner) tableExists(ctx context.Context, table string) (bool, error) {
	count, err := queryCount(ctx, r.db, `SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA='mysql' AND TABLE_NAME=?`, table)
	return count == 1, err
}

func (expected columnSignature) matches(actual columnDefinition) bool {
	if expected.dataType != actual.dataType || expected.nullable != actual.nullable {
		return false
	}
	matchedType := false
	for _, columnType := range expected.columnTypes {
		matchedType = matchedType || strings.EqualFold(columnType, actual.columnType)
	}
	if !matchedType {
		return false
	}
	if expected.defaultVal == nil {
		return !actual.defaultVal.Valid
	}
	return actual.defaultVal.Valid && actual.defaultVal.String == *expected.defaultVal
}

func (actual columnDefinition) describe() string {
	defaultValue := "NULL"
	if actual.defaultVal.Valid {
		defaultValue = fmt.Sprintf("%q", actual.defaultVal.String)
	}
	return fmt.Sprintf("type=%s nullable=%t default=%s", actual.columnType, actual.nullable, defaultValue)
}

func checkSupportedBootstrapVersions(report *report) {
	if !supportedBootstrapPair(report.sqlBootstrap, report.kvBootstrap) {
		report.block("unsupported SQL/KV bootstrap versions %d/%d; only values 251 and 220 are accepted", report.sqlBootstrap, report.kvBootstrap)
		return
	}
	if report.sqlBootstrap != report.kvBootstrap {
		report.action("bootstrap versions are temporarily split as SQL/KV=%d/%d; apply will reconcile only after all other checks pass", report.sqlBootstrap, report.kvBootstrap)
	}
}

func checkTargetBootstrapVersions(report *report) {
	if report.sqlBootstrap != targetBootstrapVersion || report.kvBootstrap != targetBootstrapVersion {
		report.block("bootstrap versions are %d/%d; target is 220/220", report.sqlBootstrap, report.kvBootstrap)
	}
}

func (r *runner) inspectKVMetadata(ctx context.Context) (*kvInspection, error) {
	txn, err := r.store.Begin()
	if err != nil {
		return nil, err
	}
	defer txn.Rollback()
	reader := meta.NewReader(txn)
	state := &kvInspection{}
	dbs, err := reader.ListDatabases()
	if err != nil {
		return nil, err
	}
	for _, dbInfo := range dbs {
		if dbInfo.State != model.StatePublic {
			state.nonPublic = append(state.nonPublic, fmt.Sprintf("database %s state=%s", dbInfo.Name.O, dbInfo.State))
		}
		tables, err := reader.ListTables(ctx, dbInfo.ID)
		if err != nil {
			return nil, err
		}
		for _, tableInfo := range tables {
			if tableInfo.State != model.StatePublic {
				state.nonPublic = append(state.nonPublic, fmt.Sprintf("table %s.%s state=%s", dbInfo.Name.O, tableInfo.Name.O, tableInfo.State))
			}
			for _, indexInfo := range tableInfo.Indices {
				if indexInfo.State != model.StatePublic {
					state.nonPublic = append(state.nonPublic, fmt.Sprintf("index %s.%s.%s state=%s", dbInfo.Name.O, tableInfo.Name.O, indexInfo.Name.O, indexInfo.State))
				}
			}
		}
		pairs, err := reader.GetMetasByDBID(dbInfo.ID)
		if err != nil {
			return nil, err
		}
		for _, pair := range pairs {
			state.partial = append(state.partial, decodePartialIndexes(dbInfo.Name.O, pair.Value)...)
		}
	}
	sort.Slice(state.partial, func(i, j int) bool {
		a, b := state.partial[i], state.partial[j]
		return a.database+"\x00"+a.table+"\x00"+a.index < b.database+"\x00"+b.table+"\x00"+b.index
	})
	sort.Strings(state.nonPublic)
	return state, nil
}

func decodePartialIndexes(database string, value []byte) []partialIndex {
	var tableInfo persistedTableInfo
	if err := json.Unmarshal(value, &tableInfo); err != nil || tableInfo.Name.O == "" {
		return nil
	}
	indexes := make([]partialIndex, 0)
	for _, indexInfo := range tableInfo.Indices {
		condition := indexInfo.LegacyCondition
		if condition == "" {
			condition = indexInfo.CurrentCondition
		}
		if strings.TrimSpace(condition) == "" {
			continue
		}
		columns := make([]string, 0, len(indexInfo.Columns))
		for _, column := range indexInfo.Columns {
			columns = append(columns, column.Name.O)
		}
		indexes = append(indexes, partialIndex{
			database: database, table: tableInfo.Name.O, index: indexInfo.Name.O,
			predicate: condition, unique: indexInfo.Unique, columns: columns,
		})
	}
	return indexes
}

func queryBootstrapVersion(ctx context.Context, db *sql.DB) (int64, error) {
	var value string
	err := db.QueryRowContext(ctx, `SELECT VARIABLE_VALUE FROM mysql.tidb WHERE VARIABLE_NAME='tidb_server_version'`).Scan(&value)
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(value, 10, 64)
}

func supportedBootstrapPair(sqlVersion, kvVersion int64) bool {
	valid := func(version int64) bool {
		return version == sourceBootstrapVersion || version == targetBootstrapVersion
	}
	return valid(sqlVersion) && valid(kvVersion)
}

func (r *runner) checkTiDBNodes(ctx context.Context, report *report) {
	rows, err := r.db.QueryContext(ctx, `
SELECT INSTANCE, VERSION, GIT_HASH
FROM information_schema.CLUSTER_INFO
WHERE TYPE='tidb'
ORDER BY INSTANCE`)
	if err != nil {
		report.block("cannot query TiDB nodes: %v", err)
		return
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var instance, version, hash string
		if err := rows.Scan(&instance, &version, &hash); err != nil {
			report.block("cannot read TiDB node row: %v", err)
			return
		}
		count++
		version = strings.TrimPrefix(strings.TrimSpace(version), "v")
		if version != targetTiDBVersion || !strings.EqualFold(hash, targetTiDBGitHash) {
			report.block("TiDB %s is version=%s git_hash=%s; require official v8.5.2 %s", instance, version, hash, targetTiDBGitHash)
		}
	}
	if err := rows.Err(); err != nil {
		report.block("cannot enumerate TiDB nodes: %v", err)
	}
	if count == 0 {
		report.block("information_schema.CLUSTER_INFO returned no TiDB nodes")
	}
}
