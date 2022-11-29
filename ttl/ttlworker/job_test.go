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
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/ttl/cache"
	"github.com/pingcap/tidb/ttl/session"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

var _ session.Session = mockSession{}

type mockSession struct {
	sessionctx.Context

	sessionInfoSchema func() infoschema.InfoSchema
	executeSQL        func(context.Context, string, ...interface{}) ([]chunk.Row, error)
	runInTxn          func(context.Context, func() error) error
	close             func()
}

func (m mockSession) SessionInfoSchema() infoschema.InfoSchema {
	return m.SessionInfoSchema()
}

func (m mockSession) ExecuteSQL(ctx context.Context, sql string, args ...interface{}) ([]chunk.Row, error) {
	return m.executeSQL(ctx, sql, args...)
}

func (m mockSession) RunInTxn(ctx context.Context, fn func() error) (err error) {
	return m.runInTxn(ctx, fn)
}

func (m mockSession) Close() {
	m.close()
}

func TestJobTracker(t *testing.T) {
	now := time.Now()
	runSQL := ""

	job := &ttlJob{
		tbl: &cache.PhysicalTable{
			Schema: model.NewCIStr("test"),
			TableInfo: &model.TableInfo{
				ID:   5,
				Name: model.NewCIStr("t"),
			},
			Partition:      model.CIStr{},
			PartitionDef:   nil,
			KeyColumns:     nil,
			KeyColumnTypes: nil,
			TimeColumn:     nil,
		},
	}
	scanTask := job.PeekScanTask()
	job.NextScanTask()
	assert.NotNil(t, scanTask)
	scanTask.tracker.Done(&mockSession{
		executeSQL: func(ctx context.Context, s string, i ...interface{}) ([]chunk.Row, error) {
			runSQL = s
			return nil, nil
		},
	}, now, nil)
	assert.Equal(t, finishJobSQL(5, time.Now(), ""), runSQL)

	// test report error
	job = &ttlJob{
		tbl: &cache.PhysicalTable{
			Schema: model.NewCIStr("test"),
			TableInfo: &model.TableInfo{
				ID:   5,
				Name: model.NewCIStr("t"),
			},
			Partition:      model.CIStr{},
			PartitionDef:   nil,
			KeyColumns:     nil,
			KeyColumnTypes: nil,
			TimeColumn:     nil,
		},
	}
	scanTask = job.PeekScanTask()
	job.NextScanTask()
	assert.NotNil(t, scanTask)
	scanTask.tracker.Done(&mockSession{
		executeSQL: func(ctx context.Context, s string, i ...interface{}) ([]chunk.Row, error) {
			runSQL = s
			return nil, nil
		},
	}, now, errors.New("test error"))
	assert.Equal(t, finishJobSQL(5, time.Now(), "test error"), runSQL)

	// test add counter
	job = &ttlJob{
		tbl: &cache.PhysicalTable{
			Schema: model.NewCIStr("test"),
			TableInfo: &model.TableInfo{
				ID:   5,
				Name: model.NewCIStr("t"),
			},
			Partition:      model.CIStr{},
			PartitionDef:   nil,
			KeyColumns:     nil,
			KeyColumnTypes: nil,
			TimeColumn:     nil,
		},
	}
	runSQL = ""
	scanTask = job.PeekScanTask()
	job.NextScanTask()
	assert.NotNil(t, scanTask)
	scanTask.tracker.Add()
	scanTask.tracker.Done(&mockSession{
		executeSQL: func(ctx context.Context, s string, i ...interface{}) ([]chunk.Row, error) {
			runSQL = s
			return nil, nil
		},
	}, now, nil)
	assert.Equal(t, "", runSQL)
	scanTask.tracker.Add()
	scanTask.tracker.Done(&mockSession{
		executeSQL: func(ctx context.Context, s string, i ...interface{}) ([]chunk.Row, error) {
			runSQL = s
			return nil, nil
		},
	}, now, errors.New("error 1"))
	assert.Equal(t, "", runSQL)
	scanTask.tracker.Done(&mockSession{
		executeSQL: func(ctx context.Context, s string, i ...interface{}) ([]chunk.Row, error) {
			runSQL = s
			return nil, nil
		},
	}, now, errors.New("error 2"))
	assert.Equal(t, finishJobSQL(5, time.Now(), "error 1; error 2"), runSQL)
}
