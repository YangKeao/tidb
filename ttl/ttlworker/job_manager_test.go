// Copyright 2022 PingCAP, Inc.
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

package ttlworker

import (
	"context"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/ttl/cache"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func newTTLTableIDRow(tableIDs ...int64) []chunk.Row {
	c := chunk.NewChunkWithCapacity([]*types.FieldType{
		types.NewFieldType(mysql.TypeLonglong),
	}, len(tableIDs))
	var rows []chunk.Row

	for _, id := range tableIDs {
		tableId := types.NewDatum(id)
		c.AppendDatum(0, &tableId)
	}

	iter := chunk.NewIterator4Chunk(c)
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		rows = append(rows, row)
	}
	return rows
}

func newTTLTableStatusRows(status ...*cache.TableStatus) []chunk.Row {
	c := chunk.NewChunkWithCapacity([]*types.FieldType{
		types.NewFieldType(mysql.TypeLonglong), // table_id
		types.NewFieldType(mysql.TypeLonglong), // parent_table_id
		types.NewFieldType(mysql.TypeString),   // table_statistics
		types.NewFieldType(mysql.TypeString),   // last_job_id
		types.NewFieldType(mysql.TypeDatetime), // last_job_start_time
		types.NewFieldType(mysql.TypeDatetime), // last_job_finish_time
		types.NewFieldType(mysql.TypeDatetime), // last_job_ttl_expire
		types.NewFieldType(mysql.TypeString),   // last_job_summary
		types.NewFieldType(mysql.TypeString),   // current_job_id
		types.NewFieldType(mysql.TypeString),   // current_job_owner_id
		types.NewFieldType(mysql.TypeString),   // current_job_owner_addr
		types.NewFieldType(mysql.TypeDatetime), // current_job_hb_time
		types.NewFieldType(mysql.TypeDatetime), // current_job_start_time
		types.NewFieldType(mysql.TypeDatetime), // current_job_ttl_expire
		types.NewFieldType(mysql.TypeString),   // current_job_state
		types.NewFieldType(mysql.TypeString),   // current_job_status
		types.NewFieldType(mysql.TypeDatetime), // current_job_status_update_time
	}, len(status))
	var rows []chunk.Row

	for _, s := range status {
		tableId := types.NewDatum(s.TableID)
		c.AppendDatum(0, &tableId)
		parentTableID := types.NewDatum(s.ParentTableID)
		c.AppendDatum(1, &parentTableID)
		if s.TableStatistics == "" {
			c.AppendNull(2)
		} else {
			tableStatistics := types.NewDatum(s.TableStatistics)
			c.AppendDatum(2, &tableStatistics)
		}

		if s.LastJobID == "" {
			c.AppendNull(3)
		} else {
			lastJobID := types.NewDatum(s.LastJobID)
			c.AppendDatum(3, &lastJobID)
		}

		lastJobStartTime := types.NewDatum(types.NewTime(types.FromGoTime(s.LastJobStartTime), mysql.TypeDatetime, types.MaxFsp))
		c.AppendDatum(4, &lastJobStartTime)
		lastJobFinishTime := types.NewDatum(types.NewTime(types.FromGoTime(s.LastJobFinishTime), mysql.TypeDatetime, types.MaxFsp))
		c.AppendDatum(5, &lastJobFinishTime)
		lastJobTTLExpire := types.NewDatum(types.NewTime(types.FromGoTime(s.LastJobTTLExpire), mysql.TypeDatetime, types.MaxFsp))
		c.AppendDatum(6, &lastJobTTLExpire)

		if s.LastJobSummary == "" {
			c.AppendNull(7)
		} else {
			lastJobSummary := types.NewDatum(s.LastJobSummary)
			c.AppendDatum(7, &lastJobSummary)
		}
		if s.CurrentJobID == "" {
			c.AppendNull(8)
		} else {
			currentJobID := types.NewDatum(s.CurrentJobID)
			c.AppendDatum(8, &currentJobID)
		}
		if s.CurrentJobOwnerID == "" {
			c.AppendNull(9)
		} else {
			currentJobOwnerID := types.NewDatum(s.CurrentJobOwnerID)
			c.AppendDatum(9, &currentJobOwnerID)
		}
		if s.CurrentJobOwnerAddr == "" {
			c.AppendNull(10)
		} else {
			currentJobOwnerAddr := types.NewDatum(s.CurrentJobOwnerAddr)
			c.AppendDatum(10, &currentJobOwnerAddr)
		}

		currentJobOwnerHBTime := types.NewDatum(types.NewTime(types.FromGoTime(s.CurrentJobOwnerHBTime), mysql.TypeDatetime, types.MaxFsp))
		c.AppendDatum(11, &currentJobOwnerHBTime)
		currentJobStartTime := types.NewDatum(types.NewTime(types.FromGoTime(s.CurrentJobStartTime), mysql.TypeDatetime, types.MaxFsp))
		c.AppendDatum(12, &currentJobStartTime)
		currentJobTTLExpire := types.NewDatum(types.NewTime(types.FromGoTime(s.CurrentJobTTLExpire), mysql.TypeDatetime, types.MaxFsp))
		c.AppendDatum(13, &currentJobTTLExpire)

		if s.CurrentJobState == "" {
			c.AppendNull(14)
		} else {
			currentJobState := types.NewDatum(s.CurrentJobState)
			c.AppendDatum(14, &currentJobState)
		}
		if s.CurrentJobStatus == "" {
			c.AppendNull(15)
		} else {
			currentJobStatus := types.NewDatum(s.CurrentJobStatus)
			c.AppendDatum(15, &currentJobStatus)
		}

		currentJobStatusUpdateTime := types.NewDatum(types.NewTime(types.FromGoTime(s.CurrentJobStatusUpdateTime), mysql.TypeDatetime, types.MaxFsp))
		c.AppendDatum(16, &currentJobStatusUpdateTime)
	}

	iter := chunk.NewIterator4Chunk(c)
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		rows = append(rows, row)
	}
	return rows
}

var updateStatusSQL = "SELECT table_id,parent_table_id,table_statistics,last_job_id,last_job_start_time,last_job_finish_time,last_job_ttl_expire,last_job_summary,current_job_id,current_job_owner_id,current_job_owner_addr,current_job_owner_hb_time,current_job_start_time,current_job_ttl_expire,current_job_state,current_job_status,current_job_status_update_time FROM mysql.tidb_ttl_table_status"

func TestSyncInfoSchemaAndTTLStatus(t *testing.T) {
	tbl := newMockTTLTbl(t, "t1")

	m := NewJobManager("test-id", newMockSessionPool(t, tbl))
	se := newMockSession(t, tbl)

	// when the info schema cache has a table, but status table doesn't
	sqlCounter := 0
	m.infoSchemaCache.Tables = map[int64]*cache.PhysicalTable{
		tbl.ID: tbl,
	}
	m.tableStatusCache.Tables = make(map[int64]*cache.TableStatus)
	se.executeSQL = func(ctx context.Context, sql string, args ...interface{}) (rows []chunk.Row, err error) {
		if sqlCounter == 0 {
			assert.Equal(t, "INSERT INTO mysql.tidb_ttl_table_status (table_id,parent_table_id) VALUES (0, 0)", sql)
			rows = []chunk.Row{}
			err = nil
		} else if sqlCounter == 1 {
			assert.Equal(t, updateStatusSQL, sql)
			rows = newTTLTableStatusRows(&cache.TableStatus{TableID: tbl.ID})
			err = nil
		} else {
			assert.Fail(t, "shouldn't spawn three SQL")
		}

		sqlCounter += 1
		return
	}
	assert.NoError(t, m.syncInfoSchemaAndTTLStatus(context.Background(), se))
	assert.Equal(t, 1, len(m.tableStatusCache.Tables))

	// when the info schema cache and status table cache are consistent
	m.infoSchemaCache.Tables = map[int64]*cache.PhysicalTable{
		tbl.ID: tbl,
	}
	m.tableStatusCache.Tables = map[int64]*cache.TableStatus{
		tbl.ID: {},
	}
	se.executeSQL = func(ctx context.Context, sql string, args ...interface{}) (rows []chunk.Row, err error) {
		assert.Fail(t, "shouldn't spawn any SQL")
		return
	}
	assert.NoError(t, m.syncInfoSchemaAndTTLStatus(context.Background(), se))

	// when the status table cache has more entry than the info schema, do nothing
	sqlCounter = 0
	m.infoSchemaCache.Tables = map[int64]*cache.PhysicalTable{}
	m.tableStatusCache.Tables = map[int64]*cache.TableStatus{
		tbl.ID: {},
	}
	se.executeSQL = func(ctx context.Context, sql string, args ...interface{}) (rows []chunk.Row, err error) {
		assert.Fail(t, "shouldn't spawn three SQL")
		return
	}
	assert.NoError(t, m.syncInfoSchemaAndTTLStatus(context.Background(), se))
	assert.Equal(t, 0, len(m.infoSchemaCache.Tables))

	// if the insert failed, it will return the error directly
	sqlCounter = 0
	m.infoSchemaCache.Tables = map[int64]*cache.PhysicalTable{
		tbl.ID: tbl}
	m.tableStatusCache.Tables = map[int64]*cache.TableStatus{}
	se.executeSQL = func(ctx context.Context, sql string, args ...interface{}) (rows []chunk.Row, err error) {
		return rows, errors.New("test error")
	}
	assert.Equal(t, "test error", m.syncInfoSchemaAndTTLStatus(context.Background(), se).Error())

	// if the first update still make it inconsistent, will retry
	sqlCounter = 0
	m.infoSchemaCache.Tables = map[int64]*cache.PhysicalTable{
		tbl.ID: tbl,
	}
	m.tableStatusCache.Tables = make(map[int64]*cache.TableStatus)
	se.executeSQL = func(ctx context.Context, sql string, args ...interface{}) (rows []chunk.Row, err error) {
		if sqlCounter == 0 {
			assert.Equal(t, "INSERT INTO mysql.tidb_ttl_table_status (table_id,parent_table_id) VALUES (0, 0)", sql)
			rows = []chunk.Row{}
			err = nil
		} else if sqlCounter == 1 {
			assert.Equal(t, updateStatusSQL, sql)
			rows = nil
			err = nil
		} else if sqlCounter == 2 {
			assert.Equal(t, "INSERT INTO mysql.tidb_ttl_table_status (table_id,parent_table_id) VALUES (0, 0)", sql)
			rows = []chunk.Row{}
			err = nil
		} else if sqlCounter == 3 {
			assert.Equal(t, updateStatusSQL, sql)
			rows = newTTLTableStatusRows(&cache.TableStatus{TableID: tbl.ID})
			err = nil
		} else {
			assert.Fail(t, "shouldn't spawn three SQL")
		}

		sqlCounter += 1
		return
	}
	assert.NoError(t, m.syncInfoSchemaAndTTLStatus(context.Background(), se))
	assert.Equal(t, 1, len(m.tableStatusCache.Tables))
}

func TestReadyForNewJobTables(t *testing.T) {
	tbl := newMockTTLTbl(t, "t1")

	m := NewJobManager("test-id", newMockSessionPool(t, tbl))

	cases := []struct {
		name             string
		infoSchemaTables []*cache.PhysicalTable
		tableStatus      []*cache.TableStatus
		shouldSchedule   bool
	}{
		// for a newly inserted table, it'll always be scheduled
		{"newly created", []*cache.PhysicalTable{tbl}, []*cache.TableStatus{{TableID: tbl.ID, ParentTableID: tbl.ID}}, true},
		// table only in the table status cache will not be scheduled
		{"proper subset", []*cache.PhysicalTable{}, []*cache.TableStatus{{TableID: tbl.ID, ParentTableID: tbl.ID}}, false},
		// table whose current job owner id is not empty, and heart beat time is long enough will not be scheduled
		{"current job not empty", []*cache.PhysicalTable{tbl}, []*cache.TableStatus{{TableID: tbl.ID, ParentTableID: tbl.ID, CurrentJobOwnerID: "test-another-id", CurrentJobOwnerHBTime: time.Now()}}, false},
		// table whose current job owner id is not empty, but heart beat time is expired will be scheduled
		{"hb time expired", []*cache.PhysicalTable{tbl}, []*cache.TableStatus{{TableID: tbl.ID, ParentTableID: tbl.ID, CurrentJobOwnerID: "test-another-id", CurrentJobOwnerHBTime: time.Now().Add(-time.Hour)}}, true},
		// if the last finished time is too near, it will also not be scheduled
		{"last finished time too near", []*cache.PhysicalTable{tbl}, []*cache.TableStatus{{TableID: tbl.ID, ParentTableID: tbl.ID, LastJobFinishTime: time.Now()}}, false},
		// if the last finished time is expired, it will be scheduled
		{"last finished time expired", []*cache.PhysicalTable{tbl}, []*cache.TableStatus{{TableID: tbl.ID, ParentTableID: tbl.ID, LastJobFinishTime: time.Now().Add(time.Hour * 2)}}, false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m.infoSchemaCache.Tables = make(map[int64]*cache.PhysicalTable)
			for _, ist := range c.infoSchemaTables {
				m.infoSchemaCache.Tables[ist.ID] = ist
			}
			m.tableStatusCache.Tables = make(map[int64]*cache.TableStatus)
			for _, st := range c.tableStatus {
				m.tableStatusCache.Tables[st.TableID] = st
			}

			tables := m.readyForNewJobTables()
			if c.shouldSchedule {
				assert.Equal(t, 1, len(tables))
				assert.Equal(t, int64(0), tables[0].TableID)
				assert.Equal(t, int64(0), tables[0].ParentTableID)
			} else {
				assert.Equal(t, 0, len(tables))
			}
		})
	}
}

func TestLockNewTable(t *testing.T) {
	now, err := time.Parse(timeFormat, "2022-12-05 17:13:05")
	assert.NoError(t, err)
	maxHBTime := now.Add(-2 * defJobManagerLoopTicker)

	type sqlExecute struct {
		sql string

		rows []chunk.Row
		err  error
	}
	cases := []struct {
		name        string
		tableStatus *cache.TableStatus
		sqls        []sqlExecute
		hasJob      bool
		hasError    bool
	}{
		{"normal lock table", &cache.TableStatus{TableID: 1}, []sqlExecute{
			{
				selectTableStatusWithoutOwner(maxHBTime, 1),
				newTTLTableIDRow(1), nil,
			},
			{
				setTableStatusOwnerSQL(1, now, maxHBTime, "test-id"),
				nil, nil,
			},
			{
				updateStatusSQL,
				newTTLTableStatusRows(&cache.TableStatus{TableID: 1}), nil,
			},
		}, true, false},
		{"select nothing", &cache.TableStatus{TableID: 1}, []sqlExecute{
			{
				selectTableStatusWithoutOwner(maxHBTime, 1),
				nil, nil,
			},
			{
				updateStatusSQL,
				nil, nil,
			},
		}, false, false},
		{"return error", &cache.TableStatus{TableID: 1}, []sqlExecute{
			{
				selectTableStatusWithoutOwner(maxHBTime, 1),
				newTTLTableStatusRows(&cache.TableStatus{TableID: 1}), nil,
			},
			{
				setTableStatusOwnerSQL(1, now, maxHBTime, "test-id"),
				nil, errors.New("test error message"),
			},
		}, false, true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tbl := newMockTTLTbl(t, "t1")

			m := NewJobManager("test-id", newMockSessionPool(t, tbl))
			sqlCounter := 0
			se := newMockSession(t, tbl)
			se.executeSQL = func(ctx context.Context, sql string, args ...interface{}) (rows []chunk.Row, err error) {
				assert.Less(t, sqlCounter, len(c.sqls))
				assert.Equal(t, sql, c.sqls[sqlCounter].sql)

				rows = c.sqls[sqlCounter].rows
				err = c.sqls[sqlCounter].err
				sqlCounter += 1
				return
			}

			job, err := m.lockNewJob(context.Background(), se, c.tableStatus, now)
			if c.hasJob {
				assert.NotNil(t, job)
			} else {
				assert.Nil(t, job)
			}
			if c.hasError {
				assert.NotNil(t, err)
			} else {
				assert.Nil(t, err)
			}
		})
	}
}

func TestUpdateHeartBeat(t *testing.T) {
	tbl := newMockTTLTbl(t, "t1")

	m := NewJobManager("test-id", newMockSessionPool(t, tbl))
	m.runningJobs = []*ttlJob{
		{id: "test-1"},
		{id: "test-2"},
	}
}

//
//func TestScheduleScanTask(t *testing.T) {
//	store, dom := testkit.CreateMockStoreAndDomain(t)
//	sv := server.CreateMockServer(t, store)
//	sv.SetDomain(dom)
//	defer sv.Close()
//	dom.TTLJobManager().Stop()
//	dom.TTLJobManager().WaitStopped(context.Background(), 15*time.Second)
//
//	conn := server.CreateMockConn(t, sv)
//	sctx := conn.Context().Session
//	tk := testkit.NewTestKitWithSession(t, store, sctx)
//	ttlSession := session.NewSession(sctx, tk.Session(), func() {})
//
//	m := ttlworker.NewJobManager("test-id", &mockSessionPool{session: ttlSession})
//	assert.NoError(t, m.syncInfoSchemaAndTTLStatus(context.Background(), ttlSession))
//
//	scanWorker := ttlworker.NewMockScanWorker(t)
//	scanWorker.Start()
//
//	// normally schedule task to the scanWorker
//	tk.MustExec("create table test.t (created_at datetime) TTL=created_at + INTERVAL 5 YEAR")
//	assert.NoError(t, m.updateInfoSchemaCache(ttlSession))
//	assert.NoError(t, m.updateTableStatusCache(ttlSession))
//	assert.NoError(t, m.syncInfoSchemaAndTTLStatus(context.Background(), ttlSession))
//	var task *ttlworker.ScanTask
//	waitScanArrive := make(chan struct{})
//	scanWorker.Scan = func(t *ttlworker.ScanTask) error {
//		task = t
//
//		waitScanArrive <- struct{}{}
//		return nil
//	}
//	m.RescheduleJobs4Test(ttlSession)
//	<-waitScanArrive
//	assert.NotNil(t, task)
//	assert.Equal(t, "t", task.Table().TableInfo.Name.L)
//
//	// test heartbeat
//	task = nil
//	scanWorker.RunningTask = nil
//	tk.MustExec("drop table test.t")
//	tk.MustExec("create table test.t (created_at datetime) TTL=created_at + INTERVAL 5 YEAR")
//	assert.NoError(t, m.updateInfoSchemaCache(ttlSession))
//	assert.NoError(t, m.updateTableStatusCache(ttlSession))
//	assert.NoError(t, m.syncInfoSchemaAndTTLStatus(context.Background(), ttlSession))
//	scanWorker.Scan = func(task *ttlworker.ScanTask) error {
//		waitScanArrive <- struct{}{}
//		return nil
//	}
//	m.RescheduleJobs4Test(ttlSession)
//	assert.NoError(t, m.updateHeartBeat(context.Background(), ttlSession))
//	<-waitScanArrive
//	rows := tk.MustQuery("select current_job_owner_hb_time from mysql.tidb_ttl_table_status").Rows()
//	assert.Equal(t, 1, len(rows))
//	hbTime, err := time.Parse("2006-01-02 15:04:05", rows[0][0].(string))
//	require.NoError(t, err)
//	assert.InDelta(t, time.Now().Nanosecond(), hbTime.Nanosecond(), float64(time.Second.Nanoseconds()))
//
//	// test worker is busy
//	task = nil
//	tk.MustExec("drop table test.t")
//	tk.MustExec("create table test.t (created_at datetime) TTL=created_at + INTERVAL 5 YEAR")
//	assert.NoError(t, m.updateInfoSchemaCache(ttlSession))
//	assert.NoError(t, m.updateTableStatusCache(ttlSession))
//	assert.NoError(t, m.syncInfoSchemaAndTTLStatus(context.Background(), ttlSession))
//	scanWorker.Scan = func(task *ttlworker.ScanTask) error {
//		assert.Fail(t, "should not schedule to this worker")
//		return nil
//	}
//	m.RescheduleJobs4Test(ttlSession)
//	// wait a microsecond to make sure this scan worker is not scheduled
//	time.Sleep(time.Microsecond)
//	assert.Nil(t, task)
//}
//
//func TestReportError(t *testing.T) {
//	store, dom := testkit.CreateMockStoreAndDomain(t)
//	sv := server.CreateMockServer(t, store)
//	sv.SetDomain(dom)
//	defer sv.Close()
//	dom.TTLJobManager().Stop()
//	dom.TTLJobManager().WaitStopped(context.Background(), 15*time.Second)
//
//	conn := server.CreateMockConn(t, sv)
//	sctx := conn.Context().Session
//	tk := testkit.NewTestKitWithSession(t, store, sctx)
//	ttlSession := session.NewSession(sctx, tk.Session(), func() {})
//
//	m := ttlworker.NewJobManager("test-id", &mockSessionPool{session: ttlSession})
//	assert.NoError(t, m.syncInfoSchemaAndTTLStatus(context.Background(), ttlSession))
//
//	scanWorker := ttlworker.NewMockScanWorker(func(task *ttlworker.ScanTask) error {
//		return nil
//	}).(*ttlworker.MockScanWorker)
//	scanWorker.Start()
//	m.SetScanWorkers4Test([]ttlworker.Worker{
//		scanWorker,
//	})
//
//	// as it's not safe to share a session in multiple goroutine and execute statements
//	// to keep the simplicity, use a lock to serialize the test
//	var sessionLock sync.Mutex
//
//	var task *ttlworker.ScanTask
//	waitScanArrive := make(chan struct{})
//	scanWorker.RunningTask = nil
//	tk.MustExec("create table test.t (created_at datetime) TTL=created_at + INTERVAL 5 YEAR")
//	assert.NoError(t, m.updateInfoSchemaCache(ttlSession))
//	assert.NoError(t, m.updateTableStatusCache(ttlSession))
//	assert.NoError(t, m.syncInfoSchemaAndTTLStatus(context.Background(), ttlSession))
//	now := time.Now()
//	scanWorker.Scan = func(t *ttlworker.ScanTask) (err error) {
//		task = t
//		defer func() {
//			sessionLock.Lock()
//			t.Tracker().Done(ttlSession, now, err)
//			sessionLock.Unlock()
//
//			scanWorker.RunningTask = nil
//			waitScanArrive <- struct{}{}
//		}()
//
//		return errors.New("test error")
//	}
//	sessionLock.Lock()
//	m.RescheduleJobs4Test(ttlSession)
//	sessionLock.Unlock()
//	<-waitScanArrive
//	assert.NotNil(t, task)
//	assert.Equal(t, "t", task.Table().TableInfo.Name.L)
//
//	tk.MustQuery("select last_job_finish_time, last_job_ttl_expire, last_job_summary from mysql.tidb_ttl_table_status").
//		Check(testkit.RowsWithSep("|", fmt.Sprintf("%s|<nil>|test error", now.Format("2006-01-02 15:04:05"))))
//}
//
//func TestCancelTask(t *testing.T) {
//	store, dom := testkit.CreateMockStoreAndDomain(t)
//	sv := server.CreateMockServer(t, store)
//	sv.SetDomain(dom)
//	defer sv.Close()
//	dom.TTLJobManager().Stop()
//	dom.TTLJobManager().WaitStopped(context.Background(), 15*time.Second)
//
//	conn := server.CreateMockConn(t, sv)
//	sctx := conn.Context().Session
//	tk := testkit.NewTestKitWithSession(t, store, sctx)
//	ttlSession := session.NewSession(sctx, tk.Session(), func() {})
//
//	m := ttlworker.NewJobManager("test-id", &mockSessionPool{session: ttlSession})
//	assert.NoError(t, m.syncInfoSchemaAndTTLStatus(context.Background(), ttlSession))
//
//	scanWorker := ttlworker.NewMockScanWorker(func(task *ttlworker.ScanTask) error {
//		return nil
//	}).(*ttlworker.MockScanWorker)
//	scanWorker.Start()
//	m.SetScanWorkers4Test([]ttlworker.Worker{
//		scanWorker,
//	})
//
//	// test cancel task
//	now := time.Now()
//	var task *ttlworker.ScanTask
//	waitScanArrive := make(chan struct{})
//	// as it's not safe to share a session in multiple goroutine and execute statements
//	// to keep the simplicity, use a lock to serialize the test
//	var sessionLock sync.Mutex
//
//	scanWorker.RunningTask = nil
//	tk.MustExec("create table test.t (created_at datetime) TTL=created_at + INTERVAL 5 YEAR")
//	assert.NoError(t, m.updateInfoSchemaCache(ttlSession))
//	assert.NoError(t, m.updateTableStatusCache(ttlSession))
//	assert.NoError(t, m.syncInfoSchemaAndTTLStatus(context.Background(), ttlSession))
//	scanWorker.Scan = func(scanTask *ttlworker.ScanTask) (err error) {
//		defer func() {
//			sessionLock.Lock()
//			scanTask.Tracker().Done(ttlSession, now, err)
//			sessionLock.Unlock()
//
//			scanWorker.RunningTask = nil
//			waitScanArrive <- struct{}{}
//		}()
//
//		task = scanTask
//		select {
//		case <-scanTask.Ctx().Done():
//			return errors.New("test error")
//		}
//		return nil
//	}
//	m.RescheduleJobs4Test(ttlSession)
//	// try to select id from status
//	sessionLock.Lock()
//	rows := tk.MustQuery("select current_job_id from mysql.tidb_ttl_table_status").Rows()
//	sessionLock.Unlock()
//	assert.Equal(t, 1, len(rows))
//	jobID := rows[0][0].(string)
//
//	m.CancelJob(jobID)
//	<-waitScanArrive
//	assert.NotNil(t, task)
//	assert.Equal(t, "t", task.Table().TableInfo.Name.L)
//
//	tk.MustQuery("select last_job_finish_time, last_job_ttl_expire, last_job_summary from mysql.tidb_ttl_table_status").
//		Check(testkit.RowsWithSep("|", fmt.Sprintf("%s|<nil>|test error", now.Format("2006-01-02 15:04:05"))))
//}
