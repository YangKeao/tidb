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
	"time"

	"github.com/pingcap/tidb/ttl/cache"
	"github.com/pingcap/tidb/ttl/session"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

type scanWorker interface {
	Worker

	Idle() bool
	ScheduleTask(task *ScanTask) bool
}

type scanWorkerImpl struct {
	baseWorker

	runningTask *ScanTask
	del         chan<- *delTask
	sessPool    session.SessionPool
}

func newScanWorker(del chan<- *delTask, sessPool session.SessionPool) scanWorker {
	w := &scanWorkerImpl{
		del:      del,
		sessPool: sessPool,
	}
	w.init(w.scanLoop)
	w.ctx = logutil.WithKeyValue(w.ctx, "ttl-worker", "scan")
	return w
}

// Idle returns whether this worker is idle and doesn't have running task.
func (w *scanWorkerImpl) Idle() bool {
	w.Lock()
	defer w.Unlock()

	return w.runningTask == nil
}

// ScheduleTask schedules a task on this scanWorker, and returns whether it schedules successfully
func (w *scanWorkerImpl) ScheduleTask(task *ScanTask) bool {
	w.Lock()
	defer w.Unlock()

	if w.status != workerStatusRunning {
		return false
	}

	if w.runningTask != nil {
		return false
	}

	select {
	case w.Send() <- task:
		w.runningTask = task
		return true
	default:
		return false
	}
}

func (w *scanWorkerImpl) scanLoop() error {
	for {
		select {
		case <-w.ctx.Done():
			return nil
		case rawTask := <-w.ch:
			task := rawTask.(*ScanTask)
			err := w.scan(task)
			if err != nil {
				logutil.Logger(w.ctx).Warn("fail to run ttl scan task", zap.Error(err))
			}
		}
	}
}

func (w *scanWorkerImpl) scan(task *ScanTask) (err error) {
	se, err := session.GetSession(w.sessPool)
	if err != nil {
		return err
	}
	defer func() {
		task.tracker.Done(se, time.Now(), err)
		w.freeToSchedule()
	}()

	// TODO: implement the scan worker
	return nil
}

func (w *scanWorkerImpl) freeToSchedule() {
	w.Lock()
	defer w.Unlock()

	w.runningTask = nil
}

// ScanTask describes a task to scan the table
type ScanTask struct {
	ctx context.Context

	tbl        *cache.PhysicalTable
	expire     time.Time
	rangeStart []types.Datum
	rangeEnd   []types.Datum

	tracker JobResultTracker
}

func newScanTask(job *ttlJob) *ScanTask {
	// TODO: add more arguments to this function to implement the scan worker
	return &ScanTask{
		tbl:     job.tbl,
		tracker: job,
		ctx:     job.ctx,
	}
}
