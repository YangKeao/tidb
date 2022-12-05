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
	"go.uber.org/multierr"
	"sync"
	"sync/atomic"
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

	tbl *cache.PhysicalTable

	allSpawned bool
	status     cache.JobStatus

	scanTaskTracker *scanTaskTracker
	statistics      *ttlStatistics
}

func newTTLJob(ctx context.Context, jobID string, tableInfo *cache.PhysicalTable) *ttlJob {
	ctx, cancel := context.WithCancel(ctx)
	return &ttlJob{
		id: jobID,

		ctx:    ctx,
		cancel: cancel,
		tbl:    tableInfo,

		scanTaskTracker: &scanTaskTracker{},
		statistics:      &ttlStatistics{},

		status: cache.JobStatusWaiting,
	}
}

// ChangeStatus updates the state of this job
func (job *ttlJob) ChangeStatus(ctx context.Context, se session.Session, status cache.JobStatus) error {
	_, err := se.ExecuteSQL(ctx, updateJobCurrentStatusSQL(job.tbl.ID, job.status, status))
	if err != nil {
		return errors.Trace(err)
	}
	job.status = status

	return nil
}

// PeekScanTask returns the next scan task, but doesn't promote the iterator
func (job *ttlJob) PeekScanTask() *ScanTask {
	return newScanTask(job)
}

// NextScanTask promotes the iterator
func (job *ttlJob) NextScanTask() {
	job.scanTaskTracker.Add()
	job.allSpawned = true
}

func (job *ttlJob) finish(se session.Session, now time.Time, summary string) {
	// at this time, the job.ctx may have been canceled (to cancel this job)
	// even when it's canceled, we'll need to update the states, so use another context rather than job.ctx
	_, err := se.ExecuteSQL(context.TODO(), finishJobSQL(job.tbl.ID, now, summary))
	if err != nil {
		logutil.Logger(job.ctx).Error("fail to finish a ttl job", zap.Error(err), zap.Int64("tableID", job.tbl.ID), zap.String("jobID", job.id))
	}

	job.cancel()
}

// AllSpawned returns whether all scan tasks have been dumped out
// **This function will be called concurrently, in many workers' goroutine**
func (job *ttlJob) AllSpawned() bool {
	return job.allSpawned
}

// Finished returned whether the job has been finished, and if finished, the summary of it
func (job *ttlJob) Finished() (bool, string) {
	if !job.AllSpawned() {
		return false, ""
	}

	scanFinished, err := job.scanTaskTracker.Finished()
	if !scanFinished {
		return false, ""
	}
	if err != nil {
		// TODO: tolerate some errors
		return true, err.Error()
	}
	if job.statistics.TotalRows.Load() != job.statistics.SuccessRows.Load()+job.statistics.ErrorRows.Load() {
		// TODO: tolerate some minor difference
		return false, ""
	}
	return true, fmt.Sprintf("Deleted %d rows, %d of them succeed", job.statistics.TotalRows.Load(), job.statistics.SuccessRows.Load())
}

func (job *ttlJob) Cancel() {
	job.cancel()
}

type ttlStatistics struct {
	TotalRows   atomic.Uint64
	SuccessRows atomic.Uint64
	ErrorRows   atomic.Uint64
}

type scanTaskTracker struct {
	sync.Mutex

	counter uint64
	errors  error
}

// Add increases the inside counter
func (stt *scanTaskTracker) Add() {
	stt.Lock()

	stt.counter += 1
}

// Done decreases the counter and record the error
func (stt *scanTaskTracker) Done(err error) {
	stt.Lock()

	stt.counter -= 1
	stt.errors = multierr.Append(stt.errors, err)
}

// Finished returns whether it's finished, and the error inside it
// notice: this error doesn't mean this function failed.
func (stt *scanTaskTracker) Finished() (bool, error) {
	stt.Lock()

	return stt.counter == 0, stt.errors
}
