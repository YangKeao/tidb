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
	"github.com/pingcap/tidb/ttl/cache"
	"github.com/pingcap/tidb/ttl/session"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// SetScanWorkers4Test sets the scan workers and forget the existing scan workers
func (m *JobManager) SetScanWorkers4Test(scanWorkers []Worker) {
	m.scanWorkers = scanWorkers
}

// SetDelWorkers4Test sets the delete workers and forget the existing del workers
func (m *JobManager) SetDelWorkers4Test(delWorkers []Worker) {
	m.delWorkers = delWorkers
}

// RescheduleJobs4Test schedules the jobs on this manager
func (m *JobManager) RescheduleJobs4Test(se session.Session) {
	m.rescheduleJobs(se)
}

// MockScanWorker is a worker which could specify customized Scan function, to help to test the job manager
type MockScanWorker struct {
	baseWorker

	RunningTask *ScanTask
	Scan        func(task *ScanTask) error
}

// NewMockScanWorker creates a mock scan worker
func NewMockScanWorker(scan func(task *ScanTask) error) scanWorker {
	w := &MockScanWorker{}
	w.Scan = scan
	w.init(w.scanLoop)
	w.ctx = logutil.WithKeyValue(w.ctx, "ttl-Worker", "scan")
	return w
}

// Idle returns whether this mock scan worker is idle
func (w *MockScanWorker) Idle() bool {
	w.Lock()
	defer w.Unlock()

	return w.RunningTask == nil
}

// ScheduleTask schedules a task on the mock scan worker
func (w *MockScanWorker) ScheduleTask(task *ScanTask) bool {
	w.Lock()
	defer w.Unlock()

	if w.status != workerStatusRunning {
		return false
	}

	if w.RunningTask != nil {
		return false
	}

	select {
	case w.Send() <- task:
		w.RunningTask = task
		return true
	default:
		return false
	}
}

func (w *MockScanWorker) scanLoop() error {
	for {
		select {
		case <-w.ctx.Done():
			return nil
		case rawTask := <-w.ch:
			task := rawTask.(*ScanTask)
			err := w.Scan(task)
			if err != nil {
				logutil.Logger(w.ctx).Warn("fail to run ttl scan task", zap.Error(err))
			}
		}
	}
}

// Table returns the corresponding physical table of this task
func (s *ScanTask) Table() *cache.PhysicalTable {
	return s.tbl
}

// Tracker returns the result tracker of this scanTask
func (s *ScanTask) Tracker() JobResultTracker {
	return s.tracker
}

// Ctx returns the context of this scanTask
func (s *ScanTask) Ctx() context.Context {
	return s.ctx
}
