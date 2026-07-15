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
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	parsermodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/stretchr/testify/require"
)

func TestParseOptions(t *testing.T) {
	opts, err := parseOptions([]string{
		"--mode", "apply",
		"--path", "127.0.0.1:2379",
		"--host", "127.0.0.2",
		"--port", "4400",
		"--force",
	})
	require.NoError(t, err)
	require.Equal(t, modeApply, opts.mode)
	require.Equal(t, "127.0.0.1:2379", opts.path)
	require.Equal(t, "127.0.0.2", opts.host)
	require.Equal(t, "4400", opts.port)
	require.True(t, opts.force)

	_, err = parseOptions([]string{"--path", "tikv://127.0.0.1:2379?keyspaceName=test"})
	require.ErrorContains(t, err, "Dedicated/API-v1")

	_, err = parseOptions([]string{"--mode", "check", "--force", "--path", "127.0.0.1:2379"})
	require.ErrorContains(t, err, "only valid with --mode apply")
}

func TestAcceptPreflight(t *testing.T) {
	var out bytes.Buffer
	r := newRunner(nil, nil, &out)

	require.NoError(t, r.acceptPreflight(&report{}, false))
	require.Empty(t, out.String())

	blocked := &report{blockers: []string{"test blocker"}}
	require.ErrorContains(t, r.acceptPreflight(blocked, false), "found 1 blocker")
	require.Empty(t, out.String())

	require.NoError(t, r.acceptPreflight(blocked, true))
	require.Equal(t, "WARNING: --force ignores 1 preflight blocker(s); continuing with apply\n", out.String())
}

func TestInspectKVFindsPartialIndex(t *testing.T) {
	store := newMetadataStore(t)
	defer store.Close()

	err := kv.RunInNewTxn(context.Background(), store, true, func(_ context.Context, txn kv.Transaction) error {
		m := meta.NewMutator(txn)
		dbInfo := &model.DBInfo{ID: 100, Name: parsermodel.NewCIStr("app"), State: model.StatePublic}
		if err := m.CreateDatabase(dbInfo); err != nil {
			return err
		}
		tableInfo := &model.TableInfo{
			ID: 200, Name: parsermodel.NewCIStr("orders"), State: model.StatePublic,
			Indices: []*model.IndexInfo{{
				ID: 300, Name: parsermodel.NewCIStr("idx_open"), State: model.StatePublic,
				Columns: []*model.IndexColumn{{Name: parsermodel.NewCIStr("customer_id")}},
			}},
		}
		return m.CreateTableOrView(dbInfo.ID, tableInfo)
	})
	require.NoError(t, err)
	require.NoError(t, injectPartialIndexCondition(store, "idx_open", "`status` = 'open'"))

	r := newRunner(nil, store, nil)
	state, err := r.inspectKVMetadata(context.Background())
	require.NoError(t, err)
	require.Len(t, state.partial, 1)
	require.Equal(t, "app", state.partial[0].database)
	require.Equal(t, "orders", state.partial[0].table)
	require.Equal(t, "idx_open", state.partial[0].index)
	require.Equal(t, "`status` = 'open'", state.partial[0].predicate)
}

func injectPartialIndexCondition(store kv.Storage, indexName, condition string) error {
	return kv.RunInNewTxn(context.Background(), store, true, func(_ context.Context, txn kv.Transaction) error {
		iter, err := txn.Iter(nil, nil)
		if err != nil {
			return err
		}
		defer iter.Close()
		for iter.Valid() {
			value := iter.Value()
			if bytes.Contains(value, []byte(`"idx_name":{"O":"`+indexName+`"`)) {
				var tableInfo map[string]any
				if err := json.Unmarshal(value, &tableInfo); err != nil {
					return err
				}
				indices := tableInfo["index_info"].([]any)
				indices[0].(map[string]any)["partial_condition_expr_string"] = condition
				updated, err := json.Marshal(tableInfo)
				if err != nil {
					return err
				}
				return txn.Set(iter.Key(), updated)
			}
			if err := iter.Next(); err != nil {
				return err
			}
		}
		return nil
	})
}

func TestDecodeLegacyPartialIndex(t *testing.T) {
	legacyJSON := "{" +
		`"name":{"O":"orders","L":"orders"},` +
		`"index_info":[{` +
		`"idx_name":{"O":"idx_open","L":"idx_open"},` +
		`"idx_cols":[{"name":{"O":"customer_id","L":"customer_id"}}],` +
		`"is_unique":true,` +
		`"partial_condition_expr_string":"` + "`active` = 1" + `"}]}`
	indexes := decodePartialIndexes("app", []byte(legacyJSON))
	require.Len(t, indexes, 1)
	require.Equal(t, "idx_open", indexes[0].index)
	require.Equal(t, "`active` = 1", indexes[0].predicate)
	require.True(t, indexes[0].unique)
	require.Equal(t, []string{"customer_id"}, indexes[0].columns)
}

func TestCompareAndSetKVBootstrap(t *testing.T) {
	store := newMetadataStore(t)
	defer store.Close()
	r := newRunner(nil, store, nil)

	require.NoError(t, r.compareAndSetKVBootstrap(context.Background(), sourceBootstrapVersion, targetBootstrapVersion))
	version, err := r.readKVBootstrap()
	require.NoError(t, err)
	require.Equal(t, targetBootstrapVersion, version)
	require.ErrorContains(t,
		r.compareAndSetKVBootstrap(context.Background(), sourceBootstrapVersion, targetBootstrapVersion),
		"expected 251, found 220",
	)

	report := &report{sqlBootstrap: 999, kvBootstrap: targetBootstrapVersion}
	checkSupportedBootstrapVersions(report)
	require.Len(t, report.blockers, 1)
	require.Empty(t, report.actions)
}

func TestManifestDropColumnRunsEveryTime(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	for range 2 {
		mock.ExpectExec(`ALTER TABLE mysql\.tidb_import_jobs DROP COLUMN IF EXISTS group_key`).
			WillReturnResult(sqlmock.NewResult(0, 0))
	}
	operation, found := findRollbackOperation(newRunner(db, nil, nil).rollbackOperations(), "mysql.tidb_import_jobs.group_key")
	require.True(t, found)
	require.NoError(t, operation.apply(context.Background()))
	require.NoError(t, operation.apply(context.Background()))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestDropPITRPrimaryKeyDropsAnyNonTargetKey(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	mock.ExpectQuery("SELECT NON_UNIQUE, COLUMN_NAME").WithArgs("tidb_pitr_id_map", "PRIMARY").
		WillReturnRows(sqlmock.NewRows([]string{"NON_UNIQUE", "COLUMN_NAME"}).
			AddRow(0, "custom_id"))
	mock.ExpectExec("ALTER TABLE mysql.tidb_pitr_id_map DROP PRIMARY KEY").
		WillReturnResult(sqlmock.NewResult(0, 0))
	require.NoError(t, newRunner(db, nil, nil).dropPITRSourcePrimaryKey().apply(context.Background()))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestDropPITRPrimaryKeyKeepsTargetKey(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	mock.ExpectQuery("SELECT NON_UNIQUE, COLUMN_NAME").WithArgs("tidb_pitr_id_map", "PRIMARY").
		WillReturnRows(sqlmock.NewRows([]string{"NON_UNIQUE", "COLUMN_NAME"}).
			AddRow(0, "restored_ts").AddRow(0, "upstream_cluster_id").AddRow(0, "segment_id"))
	require.NoError(t, newRunner(db, nil, nil).dropPITRSourcePrimaryKey().apply(context.Background()))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestAddPITRTargetPrimaryKeyWhenMissing(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	mock.ExpectQuery("SELECT NON_UNIQUE, COLUMN_NAME").WithArgs("tidb_pitr_id_map", "PRIMARY").
		WillReturnRows(sqlmock.NewRows([]string{"NON_UNIQUE", "COLUMN_NAME"}))
	mock.ExpectExec("ALTER TABLE mysql.tidb_pitr_id_map ADD PRIMARY KEY").
		WillReturnResult(sqlmock.NewResult(0, 0))
	require.NoError(t, newRunner(db, nil, nil).addPITRTargetPrimaryKey().apply(context.Background()))
	require.NoError(t, mock.ExpectationsWereMet())
}

func findRollbackOperation(operations []rollbackOperation, name string) (rollbackOperation, bool) {
	for _, operation := range operations {
		if operation.name == name {
			return operation, true
		}
	}
	return rollbackOperation{}, false
}

func TestRollbackManifestIsReverseOrdered(t *testing.T) {
	operations := newRunner(nil, nil, nil).rollbackOperations()
	require.NotEmpty(t, operations)
	versions := make([]int, 0, len(operations))
	for i := 1; i < len(operations); i++ {
		require.GreaterOrEqual(t, operations[i-1].version, operations[i].version)
	}
	var pitrSteps []string
	for _, operation := range operations {
		versions = append(versions, operation.version)
		if operation.version == 248 {
			pitrSteps = append(pitrSteps, operation.name)
		}
	}
	require.NotContains(t, versions, 245)
	require.Equal(t, []string{
		"drop mysql.tidb_pitr_id_map branch primary key",
		"drop mysql.tidb_pitr_id_map.restore_id",
		"add mysql.tidb_pitr_id_map v8.5.2 primary key",
	}, pitrSteps)
}

func TestCompareAndSetSQLBootstrap(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	mock.ExpectExec("UPDATE mysql.tidb").WithArgs(targetBootstrapVersion, sourceBootstrapVersion).
		WillReturnResult(sqlmock.NewResult(0, 1))
	r := newRunner(db, nil, nil)
	require.NoError(t, r.compareAndSetSQLBootstrap(context.Background(), sourceBootstrapVersion, targetBootstrapVersion))
	require.NoError(t, mock.ExpectationsWereMet())
}

func newMetadataStore(t *testing.T) kv.Storage {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	err = kv.RunInNewTxn(context.Background(), store, true, func(_ context.Context, txn kv.Transaction) error {
		return meta.NewMutator(txn).FinishBootstrap(sourceBootstrapVersion)
	})
	require.NoError(t, err)
	return store
}
