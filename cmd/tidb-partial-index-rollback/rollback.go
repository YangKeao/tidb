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
	"fmt"
	"strings"
)

type indexDefinition struct {
	nonUnique bool
	columns   []string
}

func (r *runner) rollbackOperations() []rollbackOperation {
	var operations []rollbackOperation
	add := func(items ...rollbackOperation) { operations = append(operations, items...) }

	// Reverse the branch bootstrap migrations from 251 down to 239. The comments
	// below record the forward upgrade SQL from source commit 2cf9f0e14b.
	// v251 upgrade executed:
	//   ALTER TABLE mysql.tidb_import_jobs ADD COLUMN `group_key` VARCHAR(256) NOT NULL DEFAULT '' AFTER `created_by`;
	//   ALTER TABLE mysql.tidb_import_jobs ADD INDEX idx_group_key(group_key);
	add(r.sqlOperation(251, "mysql.tidb_import_jobs.idx_group_key",
		`ALTER TABLE mysql.tidb_import_jobs DROP INDEX IF EXISTS idx_group_key`))
	add(r.sqlOperation(251, "mysql.tidb_import_jobs.group_key",
		`ALTER TABLE mysql.tidb_import_jobs DROP COLUMN IF EXISTS group_key`))

	// v250 upgrade executed for both tidb_global_task and tidb_global_task_history:
	//   ALTER TABLE mysql.<table> ADD COLUMN `keyspace` VARCHAR(64) DEFAULT '' AFTER `extra_params`;
	//   ALTER TABLE mysql.<table> ADD INDEX idx_keyspace(keyspace);
	add(r.sqlOperation(250, "mysql.tidb_global_task.idx_keyspace",
		`ALTER TABLE mysql.tidb_global_task DROP INDEX IF EXISTS idx_keyspace`))
	add(r.sqlOperation(250, "mysql.tidb_global_task.keyspace",
		`ALTER TABLE mysql.tidb_global_task DROP COLUMN IF EXISTS keyspace`))
	add(r.sqlOperation(250, "mysql.tidb_global_task_history.idx_keyspace",
		`ALTER TABLE mysql.tidb_global_task_history DROP INDEX IF EXISTS idx_keyspace`))
	add(r.sqlOperation(250, "mysql.tidb_global_task_history.keyspace",
		`ALTER TABLE mysql.tidb_global_task_history DROP COLUMN IF EXISTS keyspace`))

	// v249 upgrade executed:
	//   CREATE TABLE IF NOT EXISTS mysql.tidb_restore_registry (
	//     id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
	//     filter_strings TEXT NOT NULL, filter_hash VARCHAR(64) NOT NULL,
	//     start_ts BIGINT UNSIGNED NOT NULL, restored_ts BIGINT UNSIGNED NOT NULL,
	//     upstream_cluster_id BIGINT UNSIGNED, with_sys_table BOOLEAN NOT NULL DEFAULT TRUE,
	//     status VARCHAR(20) NOT NULL DEFAULT 'running', cmd TEXT,
	//     task_start_time TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6),
	//     last_heartbeat_time TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6),
	//     UNIQUE KEY unique_registration_params
	//       (filter_hash, start_ts, restored_ts, upstream_cluster_id, with_sys_table, cmd(256))
	//   ) AUTO_INCREMENT = 1;
	add(r.sqlOperation(249, "mysql.tidb_restore_registry",
		`DROP TABLE IF EXISTS mysql.tidb_restore_registry`))

	// v248 upgrade executed:
	//   ALTER TABLE mysql.tidb_pitr_id_map ADD COLUMN restore_id BIGINT NOT NULL DEFAULT 0;
	//   ALTER TABLE mysql.tidb_pitr_id_map DROP PRIMARY KEY;
	//   ALTER TABLE mysql.tidb_pitr_id_map
	//     ADD PRIMARY KEY(restore_id, restored_ts, upstream_cluster_id, segment_id);
	// The rollback DROP/ADD PRIMARY KEY statements have immediate guards.
	add(r.dropPITRSourcePrimaryKey())
	add(r.sqlOperation(248, "drop mysql.tidb_pitr_id_map.restore_id",
		`ALTER TABLE mysql.tidb_pitr_id_map DROP COLUMN IF EXISTS restore_id`))
	add(r.addPITRTargetPrimaryKey())

	// v247 upgrade executed:
	//   ALTER TABLE mysql.stats_meta
	//     ADD COLUMN last_stats_histograms_version BIGINT UNSIGNED DEFAULT NULL;
	// It is not rolled back because official v8.5.2 already contains the same column.
	// v246 upgrade executed:
	//   UPDATE mysql.bind_info SET plan_digest=NULL, sql_digest=NULL
	//     WHERE (plan_digest, sql_digest) IN (
	//       SELECT plan_digest, sql_digest FROM mysql.bind_info
	//       GROUP BY plan_digest, sql_digest HAVING COUNT(1) > 1);
	//   ALTER TABLE mysql.bind_info MODIFY COLUMN sql_digest VARCHAR(64) DEFAULT NULL;
	//   ALTER TABLE mysql.bind_info MODIFY COLUMN plan_digest VARCHAR(64) DEFAULT NULL;
	//   ALTER TABLE mysql.bind_info ADD UNIQUE INDEX digest_index(plan_digest, sql_digest);
	// The digest UPDATE cannot be reversed. v8.5.2 already defines both digest
	// columns as nullable: fresh bootstrap omits NOT NULL, and upgrade v104 added
	// them as plain VARCHAR(64). The v246 MODIFY statements only normalize that
	// existing definition, so this tool only removes digest_index.
	add(r.sqlOperation(246, "mysql.bind_info.digest_index",
		`ALTER TABLE mysql.bind_info DROP INDEX IF EXISTS digest_index`))

	// v245 upgrade executed:
	//   ALTER TABLE mysql.bind_info MODIFY COLUMN original_sql LONGTEXT NOT NULL;
	//   ALTER TABLE mysql.bind_info MODIFY COLUMN bind_sql LONGTEXT NOT NULL;
	// This change is intentionally not rolled back. Official v8.5.2 can read and
	// write these columns when they remain LONGTEXT, so keeping the wider type is
	// compatible and avoids imposing a lossy length restriction on bindings.

	// v244 upgrade executed:
	//   ALTER TABLE mysql.user ADD COLUMN IF NOT EXISTS `Max_user_connections`
	//     INT UNSIGNED NOT NULL DEFAULT 0 AFTER `Password_lifetime`;
	add(r.sqlOperation(244, "mysql.user.Max_user_connections",
		`ALTER TABLE mysql.user DROP COLUMN IF EXISTS Max_user_connections`))

	// v243 upgrade executed for both tidb_global_task and tidb_global_task_history:
	//   ALTER TABLE mysql.<table> ADD COLUMN max_node_count INT DEFAULT 0 AFTER `modify_params`;
	//   ALTER TABLE mysql.<table> ADD COLUMN extra_params JSON AFTER max_node_count;
	add(r.sqlOperation(243, "mysql.tidb_global_task.extra_params",
		`ALTER TABLE mysql.tidb_global_task DROP COLUMN IF EXISTS extra_params`))
	add(r.sqlOperation(243, "mysql.tidb_global_task.max_node_count",
		`ALTER TABLE mysql.tidb_global_task DROP COLUMN IF EXISTS max_node_count`))
	add(r.sqlOperation(243, "mysql.tidb_global_task_history.extra_params",
		`ALTER TABLE mysql.tidb_global_task_history DROP COLUMN IF EXISTS extra_params`))
	add(r.sqlOperation(243, "mysql.tidb_global_task_history.max_node_count",
		`ALTER TABLE mysql.tidb_global_task_history DROP COLUMN IF EXISTS max_node_count`))

	// v242 upgrade inserted/updated the cluster ID and executed:
	//   INSERT HIGH_PRIORITY INTO mysql.tidb
	//     VALUES ('cluster_id', <cluster-id>, 'TiDB Cluster ID.')
	//     ON DUPLICATE KEY UPDATE VARIABLE_VALUE=<cluster-id>;
	//   CREATE TABLE IF NOT EXISTS mysql.tidb_workload_values (
	//     id BIGINT(20) NOT NULL AUTO_INCREMENT PRIMARY KEY,
	//     version BIGINT(20) NOT NULL, category VARCHAR(64) NOT NULL,
	//     type VARCHAR(64) NOT NULL, table_id BIGINT(20) NOT NULL, value JSON NOT NULL,
	//     INDEX idx_version_category_type (version, category, type),
	//     INDEX idx_table_id (table_id)
	//   );
	add(r.sqlOperation(242, "mysql.tidb_workload_values",
		`DROP TABLE IF EXISTS mysql.tidb_workload_values`))
	add(r.sqlOperation(242, "delete mysql.tidb cluster_id row",
		`DELETE FROM mysql.tidb WHERE VARIABLE_NAME='cluster_id'`))

	// v241 upgrade executed for every listed privilege table:
	//   ALTER TABLE mysql.<table> ADD INDEX i_user(user);
	add(r.sqlOperation(241, "mysql.user.i_user",
		`ALTER TABLE mysql.user DROP INDEX IF EXISTS i_user`))
	add(r.sqlOperation(241, "mysql.global_priv.i_user",
		`ALTER TABLE mysql.global_priv DROP INDEX IF EXISTS i_user`))
	add(r.sqlOperation(241, "mysql.db.i_user",
		`ALTER TABLE mysql.db DROP INDEX IF EXISTS i_user`))
	add(r.sqlOperation(241, "mysql.tables_priv.i_user",
		`ALTER TABLE mysql.tables_priv DROP INDEX IF EXISTS i_user`))
	add(r.sqlOperation(241, "mysql.columns_priv.i_user",
		`ALTER TABLE mysql.columns_priv DROP INDEX IF EXISTS i_user`))
	add(r.sqlOperation(241, "mysql.global_grants.i_user",
		`ALTER TABLE mysql.global_grants DROP INDEX IF EXISTS i_user`))
	add(r.sqlOperation(241, "mysql.default_roles.i_user",
		`ALTER TABLE mysql.default_roles DROP INDEX IF EXISTS i_user`))

	// v240 upgrade executed:
	//   ALTER TABLE mysql.analyze_jobs
	//     ADD INDEX idx_schema_table_state (table_schema, table_name, state);
	//   ALTER TABLE mysql.analyze_jobs
	//     ADD INDEX idx_schema_table_partition_state
	//       (table_schema, table_name, partition_name, state);
	// These indexes are not rolled back because official v8.5.2 already contains them.
	// v239 upgrade executed for both tidb_global_task and tidb_global_task_history:
	//   ALTER TABLE mysql.<table> ADD COLUMN modify_params JSON AFTER `error`;
	add(r.sqlOperation(239, "mysql.tidb_global_task.modify_params",
		`ALTER TABLE mysql.tidb_global_task DROP COLUMN IF EXISTS modify_params`))
	add(r.sqlOperation(239, "mysql.tidb_global_task_history.modify_params",
		`ALTER TABLE mysql.tidb_global_task_history DROP COLUMN IF EXISTS modify_params`))
	return operations
}

func (r *runner) dropPITRSourcePrimaryKey() rollbackOperation {
	return rollbackOperation{
		version: 248,
		name:    "drop mysql.tidb_pitr_id_map branch primary key",
		apply: func(ctx context.Context) error {
			primary, err := r.readIndex(ctx, "tidb_pitr_id_map", "PRIMARY")
			if err != nil || primary == nil {
				return err
			}
			if indexMatches(*primary, false, "restored_ts", "upstream_cluster_id", "segment_id") {
				return nil
			}
			_, err = r.db.ExecContext(ctx, `ALTER TABLE mysql.tidb_pitr_id_map DROP PRIMARY KEY`)
			return err
		},
	}
}

func (r *runner) addPITRTargetPrimaryKey() rollbackOperation {
	return rollbackOperation{
		version: 248,
		name:    "add mysql.tidb_pitr_id_map v8.5.2 primary key",
		apply: func(ctx context.Context) error {
			primary, err := r.readIndex(ctx, "tidb_pitr_id_map", "PRIMARY")
			if err != nil {
				return err
			}
			if primary != nil && indexMatches(*primary, false, "restored_ts", "upstream_cluster_id", "segment_id") {
				return nil
			}
			if primary != nil {
				return fmt.Errorf("cannot add v8.5.2 primary key while another primary key exists")
			}
			_, err = r.db.ExecContext(ctx, `ALTER TABLE mysql.tidb_pitr_id_map ADD PRIMARY KEY(restored_ts, upstream_cluster_id, segment_id)`)
			return err
		},
	}
}

func (r *runner) sqlOperation(version int, name, statement string) rollbackOperation {
	return rollbackOperation{
		version: version,
		name:    name,
		apply: func(ctx context.Context) error {
			_, err := r.db.ExecContext(ctx, statement)
			return err
		},
	}
}

func (r *runner) readIndex(ctx context.Context, table, index string) (*indexDefinition, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT NON_UNIQUE, COLUMN_NAME FROM information_schema.STATISTICS WHERE TABLE_SCHEMA='mysql' AND TABLE_NAME=? AND INDEX_NAME=? ORDER BY SEQ_IN_INDEX`, table, index)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result *indexDefinition
	for rows.Next() {
		var nonUnique int
		var column sql.NullString
		if err := rows.Scan(&nonUnique, &column); err != nil {
			return nil, err
		}
		if result == nil {
			result = &indexDefinition{nonUnique: nonUnique != 0}
		}
		result.columns = append(result.columns, strings.ToLower(column.String))
	}
	return result, rows.Err()
}

func indexMatches(actual indexDefinition, nonUnique bool, columns ...string) bool {
	if actual.nonUnique != nonUnique || len(actual.columns) != len(columns) {
		return false
	}
	for i, column := range columns {
		if actual.columns[i] != strings.ToLower(column) {
			return false
		}
	}
	return true
}
