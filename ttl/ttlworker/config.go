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
)

// TODO: the following functions should be put in the variable pkg to avoid cyclic dependency after adding variables for the TTL
// some of them are only used in test

// ResizeScanWorkers resizes the number of scan workers
var ResizeScanWorkers func(context.Context, int) error

// ResizeDelWorkers resizes the number of delete workers
var ResizeDelWorkers func(context.Context, int) error

func (m *JobManager) registerConfigFunctions() {
	ResizeScanWorkers = m.resizeScanWorkers
	ResizeDelWorkers = m.resizeScanWorkers
}

type jobManagerConfig struct {
	// this ticker is not writable
	jobManagerLoopTicker time.Duration

	updateInfoSchemaCacheInterval     time.Duration
	updateTTLTableStatusCacheInterval time.Duration

	ttlJobRunInterval time.Duration
}

const defJobManagerLoopTicker = 10 * time.Second

const defUpdateInfoSchemaCacheInterval = time.Minute
const defUpdateTTLTableStatusCacheInterval = 10 * time.Minute

const defUpdateTTLTableStatusCacheTimeout = 30 * time.Second
const defSyncInfoSchemaWithTTLTableStatusTimeout = 30 * time.Second
const defUpdateHeartBeatTimeout = 30 * time.Second
const defUpdateStateTimeout = 30 * time.Second
const defTTLJobTimeout = time.Hour

const defTiDBTTLJobRunInterval = 1 * time.Hour

func newJobManagerConfig() jobManagerConfig {
	return jobManagerConfig{
		jobManagerLoopTicker:              defJobManagerLoopTicker,
		updateInfoSchemaCacheInterval:     defUpdateInfoSchemaCacheInterval,
		updateTTLTableStatusCacheInterval: defUpdateTTLTableStatusCacheInterval,
		ttlJobRunInterval:                 defTiDBTTLJobRunInterval,
	}
}

func (m *JobManager) setUpdateInfoSchemaCacheInterval(interval time.Duration) {
	m.Lock()
	defer m.Unlock()

	m.config.updateInfoSchemaCacheInterval = interval
	m.infoSchemaCache.SetInterval(interval)
}

func (m *JobManager) setUpdateTTLTableStatusCacheInterval(interval time.Duration) {
	m.Lock()
	defer m.Unlock()

	m.config.updateTTLTableStatusCacheInterval = interval
	m.tableStatusCache.SetInterval(interval)
}

func (m *JobManager) setTTLJobRunInterval(interval time.Duration) {
	m.Lock()
	defer m.Unlock()

	m.config.ttlJobRunInterval = interval
}

func (m *JobManager) getJobManagerLoopTicker() time.Duration {
	m.Lock()
	defer m.Unlock()

	return m.config.jobManagerLoopTicker
}

func (m *JobManager) getUpdateInfoSchemaCacheInterval() time.Duration {
	m.Lock()
	defer m.Unlock()

	return m.config.updateInfoSchemaCacheInterval
}

func (m *JobManager) getUpdateTTLTableStatusCacheInterval() time.Duration {
	m.Lock()
	defer m.Unlock()

	return m.config.updateTTLTableStatusCacheInterval
}

func (m *JobManager) getTTLJobRunInterval() time.Duration {
	m.Lock()
	defer m.Unlock()

	return m.config.ttlJobRunInterval
}
