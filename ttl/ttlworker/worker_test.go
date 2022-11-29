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
	"github.com/pingcap/errors"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBaseWorker(t *testing.T) {
	testValue := 0
	testCh := make(chan struct{})
	worker := baseWorker{}
	worker.init(func() error {
		testValue = 1
		testCh <- struct{}{}
		return nil
	})
	assert.Equal(t, 0, testValue)
	assert.Equal(t, workerStatusCreated, worker.status)
	worker.Start()
	assert.Equal(t, workerStatusRunning, worker.status)
	<-testCh
	assert.Equal(t, 1, testValue)
	<-worker.Stopped()
	assert.Equal(t, workerStatusStopped, worker.status)

	// test return error
	testError := errors.Errorf("test error")
	worker = baseWorker{}
	worker.init(func() error {
		return testError
	})
	worker.Start()
	err := <-worker.Stopped()
	assert.ErrorIs(t, testError, err)

	// test stop
	worker = baseWorker{}
	worker.init(func() error {
		<-worker.ctx.Done()
		return testError
	})
	worker.Start()
	worker.Stop()
	err = <-worker.Stopped()
	assert.ErrorIs(t, testError, err)
}
