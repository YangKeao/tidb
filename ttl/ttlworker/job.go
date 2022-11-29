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
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/ttl/cache"
	"github.com/pingcap/tidb/ttl/session"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/multierr"
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
	sync.Mutex

	id string

	ctx    context.Context
	cancel func()

	tbl *cache.PhysicalTable

	allSpawned bool
	status     cache.JobStatus

	runningTaskCount int
	err              error
}

// ChangeStatus updates the state of this job
func (job *ttlJob) ChangeStatus(ctx context.Context, se session.Session, status cache.JobStatus) error {
	job.Lock()
	defer job.Unlock()

	_, err := se.ExecuteSQL(ctx, updateJobCurrentStatusSQL(job.tbl.ID, job.status, status))
	if err != nil {
		return errors.Trace(err)
	}
	job.status = status

	return nil
}

// PeekScanTask returns the next scan task, but doesn't promote the iterator
func (job *ttlJob) PeekScanTask() *ScanTask {
	job.Lock()
	defer job.Unlock()

	return newScanTask(job)
}

// NextScanTask promotes the iterator
func (job *ttlJob) NextScanTask() {
	job.Lock()
	defer job.Unlock()

	job.runningTaskCount += 1
	job.allSpawned = true
}

func (job *ttlJob) finish(se session.Session, now time.Time, err error) {
	summary := ""
	if err != nil {
		summary = err.Error()
	}
	// at this time, the job.ctx may have been canceled (to cancel this job)
	// even when it's canceled, we'll need to update the states, so use another context
	_, err = se.ExecuteSQL(context.TODO(), finishJobSQL(job.tbl.ID, now, summary))
	if err != nil {
		logutil.Logger(job.ctx).Error("fail to finish a ttl job", zap.Error(err), zap.Int64("tableID", job.tbl.ID), zap.String("jobID", job.id))
	}
}

// AllSpawned returns whether all scan tasks have been dumped out
// **This function will be called concurrently, in many workers' goroutine**
func (job *ttlJob) AllSpawned() bool {
	job.Lock()
	defer job.Unlock()

	return job.allSpawned
}

// Finished returned whether the job has been finished
func (job *ttlJob) Finished() bool {
	job.Lock()
	defer job.Unlock()

	return job.allSpawned && job.runningTaskCount == 0
}

// Done is the callback function called by the worker who just finished a task spawned by this job
func (job *ttlJob) Done(se session.Session, now time.Time, err error) {
	job.Lock()
	defer job.Unlock()

	job.runningTaskCount -= 1
	job.err = multierr.Append(job.err, err)
	if job.allSpawned && job.runningTaskCount == 0 {
		job.finish(se, now, job.err)
	}
}

// Add adds up the running task count
func (job *ttlJob) Add() {
	job.Lock()
	defer job.Unlock()

	job.runningTaskCount += 1
}

func (job *ttlJob) Cancel() {
	job.Lock()
	defer job.Unlock()

	job.cancel()
}

// JobResultTracker tracks the running task and the result of the job
type JobResultTracker interface {
	Add()
	Done(se session.Session, now time.Time, err error)
}
