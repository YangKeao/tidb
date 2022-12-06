// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ttlworker

import (
	"context"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/ttl/cache"
	"github.com/pingcap/tidb/ttl/sqlbuilder"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

var (
	scanTaskExecuteSQLMaxRetry      = 5
	scanTaskExecuteSQLRetryInterval = 2 * time.Second
	taskStartCheckErrorRateCnt      = 10000
	taskMaxErrorRate                = 0.4
)

type ttlStatistics struct {
	TotalRows   atomic.Uint64
	SuccessRows atomic.Uint64
	ErrorRows   atomic.Uint64
}

func (s *ttlStatistics) IncTotalRows(cnt int) {
	s.TotalRows.Add(uint64(cnt))
}

func (s *ttlStatistics) IncSuccessRows(cnt int) {
	s.SuccessRows.Add(uint64(cnt))
}

func (s *ttlStatistics) IncErrorRows(cnt int) {
	s.ErrorRows.Add(uint64(cnt))
}

func (s *ttlStatistics) Reset() {
	s.SuccessRows.Store(0)
	s.ErrorRows.Store(0)
	s.TotalRows.Store(0)
}

type ttlScanTask struct {
	tbl        *cache.PhysicalTable
	expire     time.Time
	rangeStart []types.Datum
	rangeEnd   []types.Datum
	statistics *ttlStatistics
	taskGroup  *taskGroup
}

func (t *ttlScanTask) getDatumRows(rows []chunk.Row) [][]types.Datum {
	datums := make([][]types.Datum, len(rows))
	for i, row := range rows {
		datums[i] = row.GetDatumRow(t.tbl.KeyColumnTypes)
	}
	return datums
}

func (t *ttlScanTask) doScan(ctx context.Context, delCh chan<- *ttlDeleteTask, sessPool sessionPool) error {
	rawSess, err := getSession(sessPool)
	if err != nil {
		return err
	}
	defer rawSess.Close()

	origConcurrency := rawSess.GetSessionVars().DistSQLScanConcurrency()
	if _, err = rawSess.ExecuteSQL(ctx, "set @@tidb_distsql_scan_concurrency=1"); err != nil {
		return err
	}

	defer func() {
		_, err = rawSess.ExecuteSQL(ctx, "set @@tidb_distsql_scan_concurrency="+strconv.Itoa(origConcurrency))
		terror.Log(err)
	}()

	sess := newTableSession(rawSess, t.tbl, t.expire)
	generator, err := sqlbuilder.NewScanQueryGenerator(t.tbl, t.expire, t.rangeStart, t.rangeEnd)
	if err != nil {
		return err
	}

	retrySQL := ""
	retryTimes := 0
	var lastResult [][]types.Datum
	for {
		if err = ctx.Err(); err != nil {
			return err
		}

		if total := t.statistics.TotalRows.Load(); total > uint64(taskStartCheckErrorRateCnt) {
			if t.statistics.ErrorRows.Load() > uint64(float64(total)*taskMaxErrorRate) {
				return errors.Errorf("error exceeds the limit")
			}
		}

		sql := retrySQL
		if sql == "" {
			limit := int(variable.TTLScanBatchSize.Load())
			if sql, err = generator.NextSQL(lastResult, limit); err != nil {
				return err
			}
		}

		if sql == "" {
			return nil
		}

		rows, retryable, sqlErr := sess.ExecuteSQLWithCheck(ctx, sql)
		if sqlErr != nil {
			needRetry := retryable && retryTimes < scanTaskExecuteSQLMaxRetry && ctx.Err() == nil
			logutil.BgLogger().Error("execute query for ttl scan task failed",
				zap.String("SQL", sql),
				zap.Int("retryTimes", retryTimes),
				zap.Bool("needRetry", needRetry),
				zap.Error(err),
			)

			if !needRetry {
				return sqlErr
			}
			retrySQL = sql
			retryTimes++
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(scanTaskExecuteSQLRetryInterval):
			}
			continue
		}

		retrySQL = ""
		retryTimes = 0
		lastResult = t.getDatumRows(rows)
		if len(rows) == 0 {
			continue
		}

		// increase the count of task for the delete task
		delTask := &ttlDeleteTask{
			tbl:        t.tbl,
			expire:     t.expire,
			rows:       lastResult,
			statistics: t.statistics,
			taskGroup:  t.taskGroup,
		}
		t.taskGroup.Add()
		select {
		case <-ctx.Done():
			t.taskGroup.Done(nil)
			return ctx.Err()
		case delCh <- delTask:
			t.statistics.IncTotalRows(len(lastResult))
		}
	}
}

type ttlScanWorker struct {
	baseWorker
	curTask       *ttlScanTask
	delCh         chan<- *ttlDeleteTask
	sessionPool   sessionPool
}

func newScanWorker(delCh chan<- *ttlDeleteTask, sessPool sessionPool) *ttlScanWorker {
	w := &ttlScanWorker{
		delCh:       delCh,
		sessionPool: sessPool,
	}
	w.init(w.loop)
	return w
}

func (w *ttlScanWorker) Idle() bool {
	w.Lock()
	defer w.Unlock()
	return w.status == workerStatusRunning && w.curTask == nil
}

func (w *ttlScanWorker) Schedule(task *ttlScanTask) error {
	w.Lock()
	defer w.Unlock()
	if w.status != workerStatusRunning {
		return errors.New("worker is not running")
	}

	if w.curTask != nil {
		return errors.New("a task is running")
	}

	w.curTask = task
	w.baseWorker.ch <- task
	return nil
}

func (w *ttlScanWorker) CurrentTask() *ttlScanTask {
	w.Lock()
	defer w.Unlock()
	return w.curTask
}

func (w *ttlScanWorker) loop() error {
	ctx := w.baseWorker.ctx
	for w.status == workerStatusRunning {
		select {
		case <-ctx.Done():
			return nil
		case msg, ok := <-w.baseWorker.ch:
			if !ok {
				return nil
			}
			switch task := msg.(type) {
			case *ttlScanTask:
				w.handleScanTask(ctx, task)
			default:
				logutil.BgLogger().Warn("unrecognized message for ttlScanWorker", zap.Any("msg", msg))
			}
		}
	}
	return nil
}

func (w *ttlScanWorker) handleScanTask(ctx context.Context, task *ttlScanTask) {
	err := task.doScan(ctx, w.delCh, w.sessionPool)
	task.taskGroup.Done(err)
}
