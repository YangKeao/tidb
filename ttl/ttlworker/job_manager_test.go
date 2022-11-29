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

//func TestSyncInfoSchemaAndTTLStatus(t *testing.T) {
//	tbl := newMockTTLTbl(t, "t1")
//	m := NewJobManager("test-id", newMockSessionPool(t, tbl))
//	assert.NoError(t, m.SyncInfoSchemaAndTTLStatus(context.Background(), ttlSession))
//
//	tk.MustExec("create table test.t (created_at datetime) TTL=created_at + INTERVAL 5 YEAR")
//	assert.NoError(t, m.UpdateInfoSchemaCache(ttlSession))
//	assert.NoError(t, m.UpdateTableStatusCache(ttlSession))
//	// after updating the cache, the info schema should have contained the new table
//	assert.NoError(t, m.SyncInfoSchemaAndTTLStatus(context.Background(), ttlSession))
//	// then the entry in the mysql.tidb_ttl_table_status should have been created
//	table, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
//	assert.NoError(t, err)
//
//	tk.MustQuery("select table_id,parent_table_id from mysql.tidb_ttl_table_status").Check(testkit.Rows(fmt.Sprintf("%d %d", table.Meta().ID, table.Meta().ID)))
//	// insert a dangling table entry in the mysql.tidb_ttl_table_status, and it will be cleaned after sync
//	tk.MustExec("insert into mysql.tidb_ttl_table_status(table_id, parent_table_id) values (1, 2)")
//	assert.NoError(t, m.UpdateInfoSchemaCache(ttlSession))
//	assert.NoError(t, m.UpdateTableStatusCache(ttlSession))
//	assert.NoError(t, m.SyncInfoSchemaAndTTLStatus(context.Background(), ttlSession))
//	tk.MustQuery("select table_id,parent_table_id from mysql.tidb_ttl_table_status").Check(testkit.Rows(fmt.Sprintf("%d %d", table.Meta().ID, table.Meta().ID)))
//}

//func TestReadyForNewJobTables(t *testing.T) {
//	store, dom := testkit.CreateMockStoreAndDomain(t)
//	sv := server.CreateMockServer(t, store)
//	sv.SetDomain(dom)
//	defer sv.Close()
//	dom.TTLJobManager().Stop()
//	assert.NoError(t, dom.TTLJobManager().WaitStopped(context.Background(), 15*time.Second))
//
//	conn := server.CreateMockConn(t, sv)
//	sctx := conn.Context().Session
//	tk := testkit.NewTestKitWithSession(t, store, sctx)
//	ttlSession := session.NewSession(sctx, tk.Session(), func() {})
//
//	m := ttlworker.NewJobManager("test-id", &mockSessionPool{session: ttlSession})
//	assert.NoError(t, m.SyncInfoSchemaAndTTLStatus(context.Background(), ttlSession))
//
//	tk.MustExec("insert into mysql.tidb_ttl_table_status(table_id, parent_table_id) values (1, 2)")
//	assert.NoError(t, m.UpdateTableStatusCache(ttlSession))
//	tables := m.ReadyForNewJobTables(ttlSession)
//	assert.Equal(t, 1, len(tables))
//	assert.Equal(t, int64(1), tables[0].TableID)
//	assert.Equal(t, int64(2), tables[0].ParentTableID)
//
//	tk.MustExec("update mysql.tidb_ttl_table_status set current_job_owner_id = 'just-random-id', current_job_owner_hb_time = NOW()")
//	assert.NoError(t, m.UpdateTableStatusCache(ttlSession))
//	tables = m.ReadyForNewJobTables(ttlSession)
//	assert.Equal(t, 0, len(tables))
//
//	tk.MustExec("update mysql.tidb_ttl_table_status set current_job_owner_id = NULL, last_job_finish_time = NOW()")
//	assert.NoError(t, m.UpdateTableStatusCache(ttlSession))
//	tables = m.ReadyForNewJobTables(ttlSession)
//	assert.Equal(t, 0, len(tables))
//
//	// set an owner_id, but the heart beat time to be expired
//	tk.MustExec("update mysql.tidb_ttl_table_status set current_job_owner_id = 'just-random-id', current_job_owner_hb_time = ?, last_job_finish_time = NULL",
//		time.Now().Add(-3*ttlworker.GetTiDBTTLJobRunInterval()).Format("2006-01-02 15:04:05"))
//	assert.NoError(t, m.UpdateTableStatusCache(ttlSession))
//	tables = m.ReadyForNewJobTables(ttlSession)
//	assert.Equal(t, 1, len(tables))
//	assert.Equal(t, int64(1), tables[0].TableID)
//	assert.Equal(t, int64(2), tables[0].ParentTableID)
//}

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
//	assert.NoError(t, m.SyncInfoSchemaAndTTLStatus(context.Background(), ttlSession))
//
//	scanWorker := ttlworker.NewMockScanWorker(t)
//	scanWorker.Start()
//
//	// normally schedule task to the scanWorker
//	tk.MustExec("create table test.t (created_at datetime) TTL=created_at + INTERVAL 5 YEAR")
//	assert.NoError(t, m.UpdateInfoSchemaCache(ttlSession))
//	assert.NoError(t, m.UpdateTableStatusCache(ttlSession))
//	assert.NoError(t, m.SyncInfoSchemaAndTTLStatus(context.Background(), ttlSession))
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
//	assert.NoError(t, m.UpdateInfoSchemaCache(ttlSession))
//	assert.NoError(t, m.UpdateTableStatusCache(ttlSession))
//	assert.NoError(t, m.SyncInfoSchemaAndTTLStatus(context.Background(), ttlSession))
//	scanWorker.Scan = func(task *ttlworker.ScanTask) error {
//		waitScanArrive <- struct{}{}
//		return nil
//	}
//	m.RescheduleJobs4Test(ttlSession)
//	assert.NoError(t, m.UpdateHeartBeat(context.Background(), ttlSession))
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
//	assert.NoError(t, m.UpdateInfoSchemaCache(ttlSession))
//	assert.NoError(t, m.UpdateTableStatusCache(ttlSession))
//	assert.NoError(t, m.SyncInfoSchemaAndTTLStatus(context.Background(), ttlSession))
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
//	assert.NoError(t, m.SyncInfoSchemaAndTTLStatus(context.Background(), ttlSession))
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
//	assert.NoError(t, m.UpdateInfoSchemaCache(ttlSession))
//	assert.NoError(t, m.UpdateTableStatusCache(ttlSession))
//	assert.NoError(t, m.SyncInfoSchemaAndTTLStatus(context.Background(), ttlSession))
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
//	assert.NoError(t, m.SyncInfoSchemaAndTTLStatus(context.Background(), ttlSession))
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
//	assert.NoError(t, m.UpdateInfoSchemaCache(ttlSession))
//	assert.NoError(t, m.UpdateTableStatusCache(ttlSession))
//	assert.NoError(t, m.SyncInfoSchemaAndTTLStatus(context.Background(), ttlSession))
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
