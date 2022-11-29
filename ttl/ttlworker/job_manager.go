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
	"fmt"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/ttl/cache"
	"github.com/pingcap/tidb/ttl/session"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

const insertNewTableIntoStatusTemplate = "INSERT INTO mysql.tidb_ttl_table_status (table_id,parent_table_id) VALUES (%d, %d)"
const removeTableFromStatusTemplate = "DELETE FROM mysql.tidb_ttl_table_status WHERE table_id = %d"
const selectTableStatusWithoutOwnerTemplate = "SELECT * FROM mysql.tidb_ttl_table_status WHERE (current_job_owner_id IS NULL OR current_job_owner_hb_time < '%s') AND table_id = %d"
const setTableStatusOwnerTemplate = "UPDATE mysql.tidb_ttl_table_status SET current_job_id = UUID(), current_job_owner_id = '%s',current_job_start_time = '%s',current_job_status = 'waiting',current_job_status_update_time = '%s' WHERE (current_job_owner_id IS NULL OR current_job_owner_hb_time < '%s') AND table_id = %d"

// TODO: update heart beat one by one according to local jobs
const updateHeartBeatTemplate = "UPDATE mysql.tidb_ttl_table_status SET current_job_owner_hb_time = '%s' WHERE table_id = %d AND current_job_owner_id = '%s'"

const timeFormat = "2006-01-02 15:04:05"

func insertNewTableIntoStatusSQL(tableID int64, parentTableID int64) string {
	return fmt.Sprintf(insertNewTableIntoStatusTemplate, tableID, parentTableID)
}

func removeTableFromStatusSQL(tableID int64) string {
	return fmt.Sprintf(removeTableFromStatusTemplate, tableID)
}

func selectTableStatusWithoutOwner(now time.Time, tableID int64) string {
	return fmt.Sprintf(selectTableStatusWithoutOwnerTemplate, now.Format(timeFormat), tableID)
}

func setTableStatusOwnerSQL(tableID int64, now time.Time, id string) string {
	return fmt.Sprintf(setTableStatusOwnerTemplate, id, now.Format(timeFormat), now.Format(timeFormat), now.Format(timeFormat), tableID)
}

func updateHeartBeatSQL(tableID int64, now time.Time, id string) string {
	return fmt.Sprintf(updateHeartBeatTemplate, now.Format(timeFormat), tableID, id)
}

// JobManager schedules and manages the ttl jobs on this instance
type JobManager struct {
	baseWorker

	sessPool session.Pool

	// id is the ddl id of this instance
	id string

	// the workers are shared between the loop goroutine and other sessions (e.g. manually resize workers through
	// setting variables)
	scanWorkers []Worker
	delWorkers  []Worker

	// infoSchemaCache and tableStatusCache are a cache stores the information from info schema and the tidb_ttl_table_status
	// table. They don't need to be protected by mutex, because they are only used in job loop goroutine.
	infoSchemaCache  *cache.InfoSchemaCache
	tableStatusCache *cache.TableStatusCache

	// runningJobs record all ttlJob waiting in local
	// when a job for a table is created, it could spawn several scan tasks. If there are too many scan tasks, and they cannot
	// be fully consumed by local scan workers, their states should be recorded in the runningJobs, so that we could continue
	// to poll scan tasks from the job in the future when there are scan workers in idle.
	runningJobs []*ttlJob

	config jobManagerConfig

	delCh chan *delTask
}

// NewJobManager creates a new ttl job manager
func NewJobManager(id string, sessPool session.Pool) (manager *JobManager) {
	manager = &JobManager{}
	manager.id = id
	manager.sessPool = sessPool
	manager.delCh = make(chan *delTask)
	// TODO: get the config from arguments / system variables
	manager.config = newJobManagerConfig()

	manager.init(manager.jobLoop)
	manager.ctx = logutil.WithKeyValue(manager.ctx, "ttl-Worker", "manager")

	manager.infoSchemaCache = cache.NewInfoSchemaCache(manager.config.updateInfoSchemaCacheInterval)
	manager.tableStatusCache = cache.NewTableStatusCache(manager.config.updateTTLTableStatusCacheInterval)

	manager.registerConfigFunctions()
	return
}

func (m *JobManager) jobLoop() (err error) {
	se, err := session.GetSession(m.sessPool)
	if err != nil {
		return err
	}

	defer func() {
		err = multierr.Combine(err, multierr.Combine(m.resizeScanWorkers(0), m.resizeDelWorkers(0)))
		se.Close()
	}()

	ticker := time.Tick(m.getJobManagerLoopTicker())
	for {
		select {
		case <-m.ctx.Done():
			return nil
		case <-ticker:
			if m.infoSchemaCache.ShouldUpdate() {
				err := m.UpdateInfoSchemaCache(se)
				if err != nil {
					logutil.Logger(m.ctx).Warn("fail to update info schema cache", zap.Error(err))
				}
			}
			if m.tableStatusCache.ShouldUpdate() {
				err := m.UpdateTableStatusCache(se)
				if err != nil {
					logutil.Logger(m.ctx).Warn("fail to update table status cache", zap.Error(err))
				}
			}

			syncSchemaCtx, cancel := context.WithTimeout(m.ctx, m.getSyncInfoSchemaWithTTLTableStatusTimeout())
			err = m.SyncInfoSchemaAndTTLStatus(syncSchemaCtx, se)
			if err != nil {
				logutil.Logger(m.ctx).Warn("fail to synchronize info schema and ttl table", zap.Error(err))
			}
			cancel()

			updateHeartBeatCtx, cancel := context.WithTimeout(m.ctx, m.getUpdateHeartBeatTimeout())
			err = m.UpdateHeartBeat(updateHeartBeatCtx, se)
			if err != nil {
				logutil.Logger(m.ctx).Warn("fail to update heart beat", zap.Error(err))
			}
			cancel()

			m.rescheduleJobs(se)
		}
	}
}

func (m *JobManager) resizeScanWorkers(count int) error {
	m.Lock()
	defer m.Unlock()

	var err error
	m.scanWorkers, err = m.resizeWorkers(m.scanWorkers, count, func() Worker {
		return newScanWorker(m.delCh, m.sessPool)
	})
	return err
}

func (m *JobManager) resizeDelWorkers(count int) error {
	m.Lock()
	defer m.Unlock()

	var err error
	m.delWorkers, err = m.resizeWorkers(m.delWorkers, count, func() Worker {
		return newDelWorker(m.delCh, m.sessPool)
	})
	return err
}

func (m *JobManager) resizeWorkers(workers []Worker, count int, factory func() Worker) ([]Worker, error) {
	if count < len(workers) {
		for _, w := range workers[count:] {
			w.Stop()
		}
		leftWorkers := make([]Worker, 0, len(workers)-count)
		var leftErrs error
		for _, w := range workers[count:] {
			err := <-w.Stopped()
			if err != nil {
				leftWorkers = append(leftWorkers, w)
				leftErrs = multierr.Append(leftErrs, err)
			}
		}

		// remove the existing workers, and keep the left workers
		workers = workers[:count]
		workers = append(workers, leftWorkers...)
		return workers, leftErrs
	}

	if count > len(workers) {
		for i := len(workers); i < len(workers); i++ {
			w := factory()
			w.Start()
			workers = append(workers, w)
		}
		return workers, nil
	}

	return nil, nil
}

func (m *JobManager) rescheduleJobs(se session.Session) {
	idleScanWorkers := m.idleScanWorkers()
	if len(idleScanWorkers) == 0 {
		return
	}

	localJobs := m.localJobs()
	newJobTables := m.ReadyForNewJobTables(se)
	// TODO: also consider the resume tables
	for len(idleScanWorkers) > 0 && (len(newJobTables) > 0 || len(localJobs) > 0) {
		var job *ttlJob
		var err error

		switch {
		case len(localJobs) > 0:
			job = localJobs[0]
			localJobs = localJobs[1:]
		case len(newJobTables) > 0:
			table := newJobTables[0]
			newJobTables = newJobTables[1:]
			job, err = m.lockNewJob(m.ctx, se, table)
			if job != nil {
				m.runningJobs = append(m.runningJobs, job)
			}
		}
		if err != nil {
			logutil.Logger(m.ctx).Warn("fail to create new job", zap.Error(err))
		}
		if job == nil {
			continue
		}
		if job.Finished() {
			m.finishJob(job)
			continue
		}

		for !job.AllSpawned() {
			task := job.PeekScanTask()

			for len(idleScanWorkers) > 0 {
				idleWorker := idleScanWorkers[0]
				idleScanWorkers = idleScanWorkers[1:]

				// the idle scan workers could have been stopped, so this condition is necessary
				if idleWorker.ScheduleTask(task) {
					// TODO: add timeout
					err := job.ChangeStatus(m.ctx, se, cache.JobStatusRunning)
					if err != nil {
						// not a big problem, current logic doesn't depend on the job status to promote
						// the routine, so we could just print a log here
						logutil.Logger(m.ctx).Error("change ttl job status", zap.Error(err), zap.String("id", job.id))
					}
					logArgs := []zap.Field{zap.String("table", task.tbl.TableInfo.Name.L)}
					if task.tbl.PartitionDef != nil {
						logArgs = append(logArgs, zap.String("partition", task.tbl.PartitionDef.Name.L))
					}
					logutil.Logger(m.ctx).Info("ScheduleTTLTask",
						logArgs...)

					job.NextScanTask()
					break
				}
			}

			if len(idleScanWorkers) == 0 {
				break
			}
		}
	}
}

func (m *JobManager) idleScanWorkers() []scanWorker {
	m.Lock()
	defer m.Unlock()
	workers := make([]scanWorker, 0, len(m.scanWorkers))
	for _, w := range m.scanWorkers {
		if w.(scanWorker).Idle() {
			workers = append(workers, w.(scanWorker))
		}
	}
	return workers
}

// SyncInfoSchemaAndTTLStatus synchronizes new tables to the ttl status table
// and removes the dropped table from the ttl status table
func (m *JobManager) SyncInfoSchemaAndTTLStatus(ctx context.Context, se session.Session) error {
	// check whether the information in the info schema cache and the table status cache is consistent
	inconsistent := false
	for tableID := range m.infoSchemaCache.Tables {
		if _, ok := m.tableStatusCache.Tables[tableID]; !ok {
			inconsistent = true
		}
	}
	for id := range m.tableStatusCache.Tables {
		if _, ok := m.infoSchemaCache.Tables[id]; !ok {
			inconsistent = true
		}
	}
	if !inconsistent {
		return nil
	}

	// the cache of info schema and ttl status is not consistent, we'll need to update the cache
	// and try to synchronise them
	err := m.UpdateInfoSchemaCache(se)
	if err != nil {
		return err
	}
	err = m.UpdateTableStatusCache(se)
	if err != nil {
		return err
	}
	shouldUpdate := false
	for tableID, is := range m.infoSchemaCache.Tables {
		if _, ok := m.tableStatusCache.Tables[tableID]; !ok {
			_, err := se.ExecuteSQL(ctx, insertNewTableIntoStatusSQL(tableID, is.TableInfo.ID))
			if err != nil && terror.ErrorNotEqual(err, kv.ErrKeyExists) {
				return err
			}
			shouldUpdate = true
		}
	}

	for id := range m.tableStatusCache.Tables {
		// found an entry exist in table status cache, but not in info schema cache
		if _, ok := m.infoSchemaCache.Tables[id]; !ok {
			_, err := se.ExecuteSQL(ctx, removeTableFromStatusSQL(id))
			if err != nil {
				return err
			}
			shouldUpdate = true
		}
	}

	if shouldUpdate {
		err := m.UpdateInfoSchemaCache(se)
		if err != nil {
			return err
		}
		err = m.UpdateTableStatusCache(se)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *JobManager) localJobs() []*ttlJob {
	return m.runningJobs
}

// ReadyForNewJobTables returns all tables which should spawn a TTL job according to cache
func (m *JobManager) ReadyForNewJobTables(se session.Session) []*cache.TableStatus {
	now := time.Now()

	tables := make([]*cache.TableStatus, 0, len(m.tableStatusCache.Tables))
	for _, table := range m.tableStatusCache.Tables {
		ok, err := m.CouldTrySchedule(se, table, now)
		if err != nil {
			logutil.Logger(m.ctx).Warn("fail to compare lastJobFinishTime with current time", zap.Error(err), zap.Int64("tableId", table.TableID))
			continue
		}
		if ok {
			tables = append(tables, table)
		}
	}

	return tables
}

// CouldTrySchedule returns whether a table should be tried to run TTL
func (m *JobManager) CouldTrySchedule(se session.Session, table *cache.TableStatus, now time.Time) (bool, error) {
	if table.CurrentJobOwnerID != "" {
		// see whether it's heart beat time is expired
		hbTime, err := table.CurrentJobOwnerHBTime.GoTime(se.GetSessionVars().TimeZone)
		if err != nil {
			return false, errors.Trace(err)
		}
		// if the time is not expired, just continue
		if !hbTime.Add(2 * m.config.ttlJobRunInterval).Before(time.Now()) {
			return false, nil
		}
	}

	if table.LastJobFinishTime.IsZero() {
		return true, nil
	}

	finishTime, err := table.LastJobFinishTime.GoTime(se.GetSessionVars().TimeZone)
	if err != nil {
		return false, err
	}

	if finishTime.Add(m.getTTLJobRunInterval()).Before(now) {
		return true, nil
	}

	return false, nil
}

// occupyNewJob tries to occupy a new job in the ttl_table_status table. If it locks successfully, it will create a new
// localJob and return it.
// It could be nil, nil, if the table query doesn't return error but the job has been locked by other instances.
func (m *JobManager) lockNewJob(ctx context.Context, se session.Session, table *cache.TableStatus) (*ttlJob, error) {
	now := time.Now().Add(-2 * m.config.ttlJobRunInterval)

	noTableSelected := false
	err := se.RunInTxn(ctx, func() error {
		rows, err := se.ExecuteSQL(ctx, selectTableStatusWithoutOwner(now, table.TableID))
		if len(rows) == 0 {
			noTableSelected = true
			return nil
		}
		if err != nil {
			return err
		}

		_, err = se.ExecuteSQL(ctx, setTableStatusOwnerSQL(table.TableID, now, m.id))

		return err
	})
	if noTableSelected {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	// successfully update the table status, will need to refresh the cache.
	err = m.UpdateInfoSchemaCache(se)
	if err != nil {
		return nil, err
	}
	err = m.UpdateTableStatusCache(se)
	if err != nil {
		return nil, err
	}
	return m.createNewJob(table.TableID), nil
}

func (m *JobManager) createNewJob(tableID int64) *ttlJob {
	id := m.tableStatusCache.Tables[tableID].CurrentJobID
	ctx, cancel := context.WithCancel(m.ctx)
	return &ttlJob{
		id: id,

		ctx:    ctx,
		cancel: cancel,
		// at least, the info schema cache and table status cache are consistent in table id, so it's safe to get table
		// information from schema cache directly
		tbl: m.infoSchemaCache.Tables[tableID],

		status: cache.JobStatusWaiting,
	}
}

// UpdateHeartBeat updates the heartbeat for all task with current instance as owner
func (m *JobManager) UpdateHeartBeat(ctx context.Context, se session.Session) error {
	now := time.Now()
	for _, job := range m.localJobs() {
		_, err := se.ExecuteSQL(ctx, updateHeartBeatSQL(job.tbl.ID, now, m.id))
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// UpdateInfoSchemaCache updates the cache of information schema
func (m *JobManager) UpdateInfoSchemaCache(se session.Session) error {
	return m.infoSchemaCache.Update(se)
}

// UpdateTableStatusCache updates the cache of table status
func (m *JobManager) UpdateTableStatusCache(se session.Session) error {
	cacheUpdateCtx, cancel := context.WithTimeout(m.ctx, m.getUpdateTTLTableStatusCacheTimeout())
	defer cancel()
	return m.tableStatusCache.Update(cacheUpdateCtx, se)
}

func (m *JobManager) finishJob(finishedJob *ttlJob) {
	for idx, job := range m.runningJobs {
		if job.id == finishedJob.id {
			if idx+1 < len(m.runningJobs) {
				m.runningJobs = append(m.runningJobs[0:idx], m.runningJobs[idx+1:]...)
			} else {
				m.runningJobs = m.runningJobs[0:idx]
			}
			return
		}
	}
}

// CancelJob cancels a job
func (m *JobManager) CancelJob(jobID string) {
	for _, job := range m.runningJobs {
		if job.id == jobID {
			job.Cancel()
		}
	}
}
