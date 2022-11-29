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
	"time"
)

// TODO: the following functions should be put in the variable pkg to avoid cyclic dependency after adding variables for the TTL
// some of them are only used in test

// ResizeScanWorkers resizes the number of scan workers
var ResizeScanWorkers func(int) error

// GetScanWorkerCount returns the count of scan workers
var GetScanWorkerCount func() int

// ResizeDelWorkers resizes the number of delete workers
var ResizeDelWorkers func(int) error

// GetDelWorkerCount returns the count of del workers
var GetDelWorkerCount func() int

// SetTiDBTTLJobRunInterval updates the tidb ttl job run interval
var SetTiDBTTLJobRunInterval func(time.Duration)

// GetTiDBTTLJobRunInterval returns the tidb ttl job run interval
var GetTiDBTTLJobRunInterval func() time.Duration

// SetUpdateInfoSchemaCacheInterval updates the interval of updating info schema cache
var SetUpdateInfoSchemaCacheInterval func(time.Duration)

// SetUpdateInfoSchemaCacheInterval returns the interval of updating info schema cache
var GetUpdateInfoSchemaCacheInterval func() time.Duration

// SetUpdateTTLTableStatusCacheInterval updates the interval of updating ttl table status table cache
var SetUpdateTTLTableStatusCacheInterval func(time.Duration)

// GetUpdateTTLTableStatusCacheInterval returns the interval of updating ttl table status table cache
var GetUpdateTTLTableStatusCacheInterval func() time.Duration

func (m *JobManager) registerConfigFunctions() {
	ResizeScanWorkers = m.resizeScanWorkers
	GetScanWorkerCount = m.getScanWorkerCount
	ResizeDelWorkers = m.resizeScanWorkers
	GetDelWorkerCount = m.getDelWorkerCount

	SetTiDBTTLJobRunInterval = m.setTTLJobRunInterval
	GetTiDBTTLJobRunInterval = m.getTTLJobRunInterval

	SetUpdateInfoSchemaCacheInterval = m.setUpdateInfoSchemaCacheInterval
	GetUpdateInfoSchemaCacheInterval = m.getUpdateInfoSchemaCacheInterval

	SetUpdateTTLTableStatusCacheInterval = m.setUpdateTTLTableStatusCacheInterval
	GetUpdateTTLTableStatusCacheInterval = m.getUpdateTTLTableStatusCacheInterval
}

type jobManagerConfig struct {
	// this ticker is not writable
	jobManagerLoopTicker time.Duration

	updateInfoSchemaCacheInterval     time.Duration
	updateTTLTableStatusCacheInterval time.Duration

	updateTTLTableStatusCacheTimeout        time.Duration
	syncInfoSchemaWithTTLTableStatusTimeout time.Duration
	updateHeartBeatTimeout                  time.Duration

	heartBeatTimeout time.Duration

	ttlJobRunInterval time.Duration
}

const defJobManagerLoopTicker = 10 * time.Second

const defUpdateInfoSchemaCacheInterval = time.Minute
const defUpdateTTLTableStatusCacheInterval = 10 * time.Minute

const defUpdateTTLTableStatusCacheTimeout = 30 * time.Second
const defSyncInfoSchemaWithTTLTableStatusTimeout = 30 * time.Second
const defUpdateHeartBeatTimeout = 30 * time.Second

const defTiDBTTLJobRunInterval = 1 * time.Hour

func newJobManagerConfig() jobManagerConfig {
	return jobManagerConfig{
		jobManagerLoopTicker:                    defJobManagerLoopTicker,
		updateInfoSchemaCacheInterval:           defUpdateInfoSchemaCacheInterval,
		updateTTLTableStatusCacheInterval:       defUpdateTTLTableStatusCacheInterval,
		updateTTLTableStatusCacheTimeout:        defUpdateTTLTableStatusCacheTimeout,
		syncInfoSchemaWithTTLTableStatusTimeout: defSyncInfoSchemaWithTTLTableStatusTimeout,
		updateHeartBeatTimeout:                  defUpdateHeartBeatTimeout,
		ttlJobRunInterval:                       defTiDBTTLJobRunInterval,
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

func (m *JobManager) setUpdateTTLTableStatusCacheTimeout(timeout time.Duration) {
	m.Lock()
	defer m.Unlock()

	m.config.updateTTLTableStatusCacheTimeout = timeout
}

func (m *JobManager) setSyncInfoSchemaWithTTLTableStatusTimeout(timeout time.Duration) {
	m.Lock()
	defer m.Unlock()

	m.config.syncInfoSchemaWithTTLTableStatusTimeout = timeout
}

func (m *JobManager) setTTLJobRunInterval(interval time.Duration) {
	m.Lock()
	defer m.Unlock()

	m.config.ttlJobRunInterval = interval
}

func (m *JobManager) setUpdateHeartBeatTimeout(interval time.Duration) {
	m.Lock()
	defer m.Unlock()

	m.config.updateHeartBeatTimeout = interval
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

func (m *JobManager) getUpdateTTLTableStatusCacheTimeout() time.Duration {
	m.Lock()
	defer m.Unlock()

	return m.config.updateTTLTableStatusCacheTimeout
}

func (m *JobManager) getSyncInfoSchemaWithTTLTableStatusTimeout() time.Duration {
	m.Lock()
	defer m.Unlock()

	return m.config.syncInfoSchemaWithTTLTableStatusTimeout
}

func (m *JobManager) getTTLJobRunInterval() time.Duration {
	m.Lock()
	defer m.Unlock()

	return m.config.ttlJobRunInterval
}

func (m *JobManager) getScanWorkerCount() int {
	m.Lock()
	defer m.Unlock()

	return len(m.scanWorkers)
}

func (m *JobManager) getDelWorkerCount() int {
	m.Lock()
	defer m.Unlock()

	return len(m.delWorkers)
}

func (m *JobManager) getUpdateHeartBeatTimeout() time.Duration {
	m.Lock()
	defer m.Unlock()

	return m.config.updateHeartBeatTimeout
}
