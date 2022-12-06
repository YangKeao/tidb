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
	"github.com/pingcap/tidb/types"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/ttl/cache"
	"github.com/pingcap/tidb/ttl/session"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

const updateJobCurrentStatusTemplate = "UPDATE mysql.tidb_ttl_table_status SET current_job_status = '%s' WHERE table_id = %d AND current_job_status = '%s'"
const finishJobTemplate = "UPDATE mysql.tidb_ttl_table_status SET last_job_id = current_job_id, last_job_start_time = current_job_start_time, last_job_finish_time = '%s', last_job_ttl_expire = current_job_ttl_expire, last_job_summary = '%s', current_job_id = NULL, current_job_owner_id = NULL, current_job_owner_hb_time = NULL, current_job_start_time = NULL, current_job_ttl_expire = NULL, current_job_state = NULL, current_job_status = NULL, current_job_status_update_time = NULL WHERE table_id = %d"

func updateJobCurrentStatusSQL(tableID int64, oldStatus cache.JobStatus, newStatus cache.JobStatus) string {
	return fmt.Sprintf(updateJobCurrentStatusTemplate, newStatus, tableID, oldStatus)
}

func finishJobSQL(tableID int64, finishTime time.Time, summary string) string {
	return fmt.Sprintf(finishJobTemplate, finishTime.Format(timeFormat), summary, tableID)
}

type ttlJob struct {
	id string

	ctx    context.Context
	cancel func()

	createTime time.Time

	tbl *cache.PhysicalTable

	allSpawned bool

	// status is the only field which should be protected by a mutex, as `Cancel` may be called at any time, and will
	// change the status
	statusMutex sync.Mutex
	status      cache.JobStatus

	taskGroup  *taskGroup
	statistics *ttlStatistics
}

// changeStatus updates the state of this job
func (job *ttlJob) changeStatus(ctx context.Context, se session.Session, status cache.JobStatus) error {
	job.statusMutex.Lock()
	defer job.statusMutex.Unlock()

	_, err := se.ExecuteSQL(ctx, updateJobCurrentStatusSQL(job.tbl.ID, job.status, status))
	if err != nil {
		return errors.Trace(err)
	}
	job.status = status

	return nil
}

// peekScanTask returns the next scan task, but doesn't promote the iterator
func (job *ttlJob) peekScanTask(ctx context.Context, now time.Time, se session.Session) (*ttlScanTask, error) {
	expire, err := job.tbl.EvalExpireTime(ctx, se, now)
	if err != nil {
		return nil, err
	}
	return &ttlScanTask{
		tbl:    job.tbl,
		expire: expire,
		// TODO: split and fill in the rangeStart, rangeEnd
		rangeStart: []types.Datum{},
		rangeEnd:   []types.Datum{},
		statistics: job.statistics,
	}, nil
}

// nextScanTask promotes the iterator
func (job *ttlJob) nextScanTask() {
	job.taskGroup.Add()
	job.allSpawned = true
}

// finish will wait until all tasks have stopped, and update the state
func (job *ttlJob) finish(se session.Session) {
	summary := ""
	err := job.taskGroup.Wait()
	if err != nil {
		logutil.Logger(job.ctx).Warn("ttl job failed with error", zap.String("jobID", job.id), zap.Error(err))
		summary = err.Error()
	}
	job.updateFinishState(se, time.Now(), summary)
}

func (job *ttlJob) updateFinishState(se session.Session, now time.Time, summary string) {
	// at this time, the job.ctx may have been canceled (to cancel this job)
	// even when it's canceled, we'll need to update the states, so use another context
	_, err := se.ExecuteSQL(context.TODO(), finishJobSQL(job.tbl.ID, now, summary))
	if err != nil {
		logutil.Logger(job.ctx).Error("fail to finish a ttl job", zap.Error(err), zap.Int64("tableID", job.tbl.ID), zap.String("jobID", job.id))
	}
}

// AllSpawned returns whether all scan tasks have been dumped out
// **This function will be called concurrently, in many workers' goroutine**
func (job *ttlJob) AllSpawned() bool {
	return job.allSpawned
}

// Finished returned whether the job has been finished
func (job *ttlJob) Finished() bool {
	return job.status == cache.JobStatusCancelled ||
		(job.allSpawned && job.taskGroup.Finished())
}

// Cancel canceled the running jobs,
func (job *ttlJob) Cancel(ctx context.Context, se session.Session) error {
	err := job.changeStatus(ctx, se, cache.JobStatusCancelling)
	if err != nil {
		// don't cancel the job, to keep the consistency between the status and job execution
		logutil.Logger(job.ctx).Warn("fail to change status while cancelling the ttl job", zap.String("jobID", job.id), zap.Error(err))
		return err
	}

	job.cancel()

	job.finish(se)
	return nil
}

// JobResultTracker tracks the running task and the result of the job
type JobResultTracker interface {
	Add()
	Done(se session.Session, now time.Time, err error)
}
