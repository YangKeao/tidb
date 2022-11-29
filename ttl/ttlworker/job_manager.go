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
	"golang.org/x/sync/errgroup"
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
const selectTableStatusWithoutOwnerTemplate = "SELECT table_id FROM mysql.tidb_ttl_table_status WHERE (current_job_owner_id IS NULL OR current_job_owner_hb_time < '%s') AND table_id = %d"
const setTableStatusOwnerTemplate = "UPDATE mysql.tidb_ttl_table_status SET current_job_id = UUID(), current_job_owner_id = '%s',current_job_start_time = '%s',current_job_status = 'waiting',current_job_status_update_time = '%s' WHERE (current_job_owner_id IS NULL OR current_job_owner_hb_time < '%s') AND table_id = %d"

// TODO: update heart beat one by one according to local jobs
const updateHeartBeatTemplate = "UPDATE mysql.tidb_ttl_table_status SET current_job_owner_hb_time = '%s' WHERE table_id = %d AND current_job_owner_id = '%s'"

const timeFormat = "2006-01-02 15:04:05"

func insertNewTableIntoStatusSQL(tableID int64, parentTableID int64) string {
	return fmt.Sprintf(insertNewTableIntoStatusTemplate, tableID, parentTableID)
}

func selectTableStatusWithoutOwner(maxHBTime time.Time, tableID int64) string {
	return fmt.Sprintf(selectTableStatusWithoutOwnerTemplate, maxHBTime.Format(timeFormat), tableID)
}

func setTableStatusOwnerSQL(tableID int64, now time.Time, maxHBTime time.Time, id string) string {
	return fmt.Sprintf(setTableStatusOwnerTemplate, id, now.Format(timeFormat), now.Format(timeFormat), maxHBTime.Format(timeFormat), tableID)
}

func updateHeartBeatSQL(tableID int64, now time.Time, id string) string {
	return fmt.Sprintf(updateHeartBeatTemplate, now.Format(timeFormat), tableID, id)
}

// JobManager schedules and manages the ttl jobs on this instance
type JobManager struct {
	// the `runningJobs`, `scanWorkers` and `delWorkers` should be protected by mutex:
	// `runningJobs` will be shared in the state loop and schedule loop
	// `scanWorkers` and `delWorkers` can be modified by setting variables at any time
	baseWorker

	sessPool sessionPool

	// id is the ddl id of this instance
	id string

	// the workers are shared between the loop goroutine and other sessions (e.g. manually resize workers through
	// setting variables)
	scanWorkers []worker
	delWorkers  []worker

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

	delCh         chan *ttlDeleteTask
	notifyStateCh chan interface{}
}

// NewJobManager creates a new ttl job manager
func NewJobManager(id string, sessPool sessionPool) (manager *JobManager) {
	manager = &JobManager{}
	manager.id = id
	manager.sessPool = sessPool
	manager.delCh = make(chan *ttlDeleteTask)
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
	var g errgroup.Group

	g.Go(m.stateLoop)
	g.Go(m.scheduleLoop)

	return g.Wait()
}

func (m *JobManager) stateLoop() error {
	se, err := getSession(m.sessPool)
	if err != nil {
		return err
	}

	defer func() {
		err = multierr.Combine(err, multierr.Combine(m.resizeScanWorkers(m.ctx, 0), m.resizeDelWorkers(m.ctx, 0)))
		se.Close()
	}()

	ticker := time.Tick(m.getJobManagerLoopTicker())
	for {
		select {
		case <-m.ctx.Done():
			return nil
		case state := <-m.notifyStateCh:
			// now, it's the only state change notifier
			scanState := state.(*scanTaskExecEndMsg)
			m.submitScanState(scanState)
		case <-ticker:
			m.removeFinishedJobs(se, time.Now())
		}
	}
}

func (m *JobManager) submitScanState(state *scanTaskExecEndMsg) {
	m.Lock()
	defer m.Unlock()

	for _, job := range m.runningJobs {
		if job.tbl.ID == state.result.task.tbl.ID {
			job.runningScanTask.Add(-1)
			if state.result.err != nil {
				// scanTaskError is not shared between goroutines, we don't need to add lock for it
				job.scanTaskError = multierr.Append(job.scanTaskError, state.result.err)
			}
		}
	}
}

func (m *JobManager) removeFinishedJobs(se session.Session, now time.Time) {
	m.Lock()
	defer m.Unlock()

	for _, job := range m.runningJobs {
		if job.createTime.Add(defTTLJobTimeout).Before(now) {
			err := job.Cancel(context.TODO(), se)
			if err != nil {
				logutil.Logger(m.ctx).Warn("fail to cancel job", zap.String("jobID", job.id), zap.Error(err))
			}
		}
	}

	for _, job := range m.runningJobs {
		if job.Finished() {
			job.finish(se, now)
		}
	}
}

func (m *JobManager) scheduleLoop() error {
	se, err := getSession(m.sessPool)
	if err != nil {
		return err
	}

	defer func() {
		err = multierr.Combine(err, multierr.Combine(m.resizeScanWorkers(m.ctx, 0), m.resizeDelWorkers(m.ctx, 0)))
		se.Close()
	}()

	ticker := time.Tick(m.getJobManagerLoopTicker())
	for {
		select {
		case <-m.ctx.Done():
			return nil
		case <-ticker:
			if m.infoSchemaCache.ShouldUpdate() {
				err := m.updateInfoSchemaCache(se)
				if err != nil {
					logutil.Logger(m.ctx).Warn("fail to update info schema cache", zap.Error(err))
				}
			}
			if m.tableStatusCache.ShouldUpdate() {
				err := m.updateTableStatusCache(se)
				if err != nil {
					logutil.Logger(m.ctx).Warn("fail to update table status cache", zap.Error(err))
				}
			}

			syncSchemaCtx, cancel := context.WithTimeout(m.ctx, defSyncInfoSchemaWithTTLTableStatusTimeout)
			err = m.syncInfoSchemaAndTTLStatus(syncSchemaCtx, se)
			if err != nil {
				logutil.Logger(m.ctx).Warn("fail to synchronize info schema and ttl table", zap.Error(err))
			}
			cancel()

			updateHeartBeatCtx, cancel := context.WithTimeout(m.ctx, defUpdateHeartBeatTimeout)
			err = m.updateHeartBeat(updateHeartBeatCtx, se)
			if err != nil {
				logutil.Logger(m.ctx).Warn("fail to update heart beat", zap.Error(err))
			}
			cancel()

			m.rescheduleJobs(se)
		}
	}
}

func (m *JobManager) resizeScanWorkers(ctx context.Context, count int) error {
	m.Lock()
	defer m.Unlock()

	var err error
	m.scanWorkers, err = m.resizeWorkers(ctx, m.scanWorkers, count, func() worker {
		return newScanWorker(m.delCh, m.notifyStateCh, m.sessPool)
	})
	return err
}

func (m *JobManager) resizeDelWorkers(ctx context.Context, count int) error {
	m.Lock()
	defer m.Unlock()

	var err error
	m.delWorkers, err = m.resizeWorkers(ctx, m.delWorkers, count, func() worker {
		return newDeleteWorker(m.delCh, m.sessPool)
	})
	return err
}

func (m *JobManager) resizeWorkers(ctx context.Context, workers []worker, count int, factory func() worker) ([]worker, error) {
	if count < len(workers) {
		for _, w := range workers[count:] {
			w.Stop()
		}
		var errs error
		for _, w := range workers[count:] {
			err := w.WaitStopped(ctx, 30*time.Second)
			if err != nil {
				logutil.Logger(m.ctx).Warn("fail to stop ttl worker", zap.Error(err))
				errs = multierr.Append(errs, err)
			}
		}

		// remove the existing workers, and keep the left workers
		workers = workers[:count]
		return workers, errs
	}

	if count > len(workers) {
		for i := len(workers); i < len(workers); i++ {
			w := factory()
			w.Start()
			workers = append(workers, w)
		}
		return workers, nil
	}

	return workers, nil
}

func (m *JobManager) rescheduleJobs(se session.Session) {
	m.Lock()
	defer m.Unlock()

	idleScanWorkers := m.idleScanWorkers()
	if len(idleScanWorkers) == 0 {
		return
	}

	now := time.Now()

	localJobs := m.localJobs()
	newJobTables := m.readyForNewJobTables()
	// TODO: also consider to resume tables, but it's fine to left them there, as other nodes will take this job
	// when the heart beat is not sent
	for len(idleScanWorkers) > 0 && (len(newJobTables) > 0 || len(localJobs) > 0) {
		var job *ttlJob
		var err error

		switch {
		case len(localJobs) > 0:
			job = localJobs[0]
			localJobs = localJobs[1:]
			// for a local job, you have to verify the current owner of it is this node, because other nodes could
			// have taken this job
			status := m.tableStatusCache.Tables[job.tbl.ID]
			if status == nil || status.CurrentJobOwnerID != m.id {
				m.removeJob(job)
				continue
			}
		case len(newJobTables) > 0:
			table := newJobTables[0]
			newJobTables = newJobTables[1:]
			job, err = m.lockNewJob(m.ctx, se, table, now)
			if job != nil {
				m.appendJob(job)
			}
		}
		if err != nil {
			logutil.Logger(m.ctx).Warn("fail to create new job", zap.Error(err))
		}
		if job == nil {
			continue
		}
		if job.Finished() {
			continue
		}

		for !job.AllSpawned() {
			task, err := job.peekScanTask(m.ctx, now, se)
			if err != nil {
				logutil.Logger(m.ctx).Warn("fail to generate scan task", zap.Error(err))
				break
			}

			for len(idleScanWorkers) > 0 {
				idleWorker := idleScanWorkers[0]
				idleScanWorkers = idleScanWorkers[1:]

				// the idle scan workers could have been stopped, so this condition is necessary
				err := idleWorker.Schedule(task)
				if err != nil {
					logutil.Logger(m.ctx).Info("fail to schedule task", zap.Error(err))
					continue
				}

				ctx, cancel := context.WithTimeout(m.ctx, defUpdateStateTimeout)
				err = job.changeStatus(ctx, se, cache.JobStatusRunning)
				if err != nil {
					// not a big problem, current logic doesn't depend on the job status to promote
					// the routine, so we could just print a log here
					logutil.Logger(m.ctx).Error("change ttl job status", zap.Error(err), zap.String("id", job.id))
				}
				cancel()

				logArgs := []zap.Field{zap.String("table", task.tbl.TableInfo.Name.L)}
				if task.tbl.PartitionDef != nil {
					logArgs = append(logArgs, zap.String("partition", task.tbl.PartitionDef.Name.L))
				}
				logutil.Logger(m.ctx).Info("ScheduleTTLTask",
					logArgs...)

				job.nextScanTask()
				break
			}

			if len(idleScanWorkers) == 0 {
				break
			}
		}
	}
}

func (m *JobManager) idleScanWorkers() []*ttlScanWorker {
	workers := make([]*ttlScanWorker, 0, len(m.scanWorkers))
	for _, w := range m.scanWorkers {
		if w.(*ttlScanWorker).Idle() {
			workers = append(workers, w.(*ttlScanWorker))
		}
	}
	return workers
}

// syncInfoSchemaAndTTLStatus synchronizes new tables to the ttl status table
// and removes the dropped table from the ttl status table
func (m *JobManager) syncInfoSchemaAndTTLStatus(ctx context.Context, se session.Session) error {
	// entries in the infoSchemaCache should be a subset of tableStatusCache
	// we keep inserting and updating cache until this condition is true
	for {
		// check whether the information in the info schema cache and the table status cache is consistent
		subset := true
		for tableID := range m.infoSchemaCache.Tables {
			if _, ok := m.tableStatusCache.Tables[tableID]; !ok {
				subset = false
			}
		}
		if subset {
			return nil
		}

		// the cache of info schema and ttl status is not consistent, we'll try to synchronise them
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

		if shouldUpdate {
			err := m.updateInfoSchemaCache(se)
			if err != nil {
				return err
			}
			err = m.updateTableStatusCache(se)
			if err != nil {
				return err
			}
		}

		// TODO: add time limit here to avoid too fast dead loop
	}
}

func (m *JobManager) localJobs() []*ttlJob {
	return m.runningJobs
}

// readyForNewJobTables returns all tables which should spawn a TTL job according to cache
func (m *JobManager) readyForNewJobTables() []*cache.TableStatus {
	now := time.Now()

	tables := make([]*cache.TableStatus, 0, len(m.infoSchemaCache.Tables))
	for _, table := range m.infoSchemaCache.Tables {
		// we have made sure the table exists in the info schema cache should also exist in the table status
		status := m.tableStatusCache.Tables[table.ID]
		ok := m.couldTrySchedule(status, now)
		if ok {
			tables = append(tables, status)
		}
	}

	return tables
}

// couldTrySchedule returns whether a table should be tried to run TTL
func (m *JobManager) couldTrySchedule(table *cache.TableStatus, now time.Time) bool {
	if table.CurrentJobOwnerID != "" {
		// see whether it's heart beat time is expired
		hbTime := table.CurrentJobOwnerHBTime
		// if the time is not expired, just continue
		if !hbTime.Add(2 * m.getJobManagerLoopTicker()).Before(time.Now()) {
			return false
		}
	}

	if table.LastJobFinishTime.IsZero() {
		return true
	}

	finishTime := table.LastJobFinishTime

	if finishTime.Add(m.getTTLJobRunInterval()).Before(now) {
		return true
	}

	return false
}

// occupyNewJob tries to occupy a new job in the ttl_table_status table. If it locks successfully, it will create a new
// localJob and return it.
// It could be nil, nil, if the table query doesn't return error but the job has been locked by other instances.
func (m *JobManager) lockNewJob(ctx context.Context, se session.Session, table *cache.TableStatus, now time.Time) (*ttlJob, error) {
	maxHBTime := now.Add(-2 * m.getJobManagerLoopTicker())

	noTableSelected := false
	err := se.RunInTxn(ctx, func() error {
		rows, err := se.ExecuteSQL(ctx, selectTableStatusWithoutOwner(maxHBTime, table.TableID))
		if len(rows) == 0 {
			noTableSelected = true
			return nil
		}
		if err != nil {
			return err
		}

		_, err = se.ExecuteSQL(ctx, setTableStatusOwnerSQL(table.TableID, now, maxHBTime, m.id))

		return err
	})
	if noTableSelected {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	// successfully update the table status, will need to refresh the cache.
	err = m.updateInfoSchemaCache(se)
	if err != nil {
		return nil, err
	}
	err = m.updateTableStatusCache(se)
	if err != nil {
		return nil, err
	}
	return m.createNewJob(now, table.TableID), nil
}

func (m *JobManager) createNewJob(now time.Time, tableID int64) *ttlJob {
	id := m.tableStatusCache.Tables[tableID].CurrentJobID
	ctx, cancel := context.WithCancel(m.ctx)
	return &ttlJob{
		id: id,

		ctx:    ctx,
		cancel: cancel,

		createTime: now,
		// at least, the info schema cache and table status cache are consistent in table id, so it's safe to get table
		// information from schema cache directly
		tbl: m.infoSchemaCache.Tables[tableID],

		status:     cache.JobStatusWaiting,
		statistics: &ttlStatistics{},
	}
}

// updateHeartBeat updates the heartbeat for all task with current instance as owner
func (m *JobManager) updateHeartBeat(ctx context.Context, se session.Session) error {
	now := time.Now()
	for _, job := range m.localJobs() {
		_, err := se.ExecuteSQL(ctx, updateHeartBeatSQL(job.tbl.ID, now, m.id))
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// updateInfoSchemaCache updates the cache of information schema
func (m *JobManager) updateInfoSchemaCache(se session.Session) error {
	return m.infoSchemaCache.Update(se)
}

// updateTableStatusCache updates the cache of table status
func (m *JobManager) updateTableStatusCache(se session.Session) error {
	cacheUpdateCtx, cancel := context.WithTimeout(m.ctx, defUpdateTTLTableStatusCacheTimeout)
	defer cancel()
	return m.tableStatusCache.Update(cacheUpdateCtx, se)
}

func (m *JobManager) removeJob(finishedJob *ttlJob) {
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

func (m *JobManager) appendJob(job *ttlJob) {
	m.runningJobs = append(m.runningJobs, job)
}

// CancelJob cancels a job
func (m *JobManager) CancelJob(ctx context.Context, jobID string) error {
	m.Lock()
	defer m.Unlock()

	se, err := getSession(m.sessPool)
	if err != nil {
		return err
	}

	defer func() {
		err = multierr.Combine(err, multierr.Combine(m.resizeScanWorkers(m.ctx, 0), m.resizeDelWorkers(m.ctx, 0)))
		se.Close()
	}()

	for _, job := range m.runningJobs {
		if job.id == jobID {
			return job.Cancel(ctx, se)
		}
	}

	return errors.Errorf("cannot find the job with id: %s", jobID)
}
