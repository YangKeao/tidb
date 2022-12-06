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
	"go.uber.org/multierr"
	"sync"
)

type taskGroup struct {
	sync.Mutex
	counter int32
	err     error

	doneCh chan struct{}
}

func newTaskGroup() *taskGroup {
	return &taskGroup{}
}

func (g *taskGroup) Add() {
	g.Lock()
	defer g.Unlock()

	g.counter += 1
}

func (g *taskGroup) Done(err error) {
	g.Lock()
	defer g.Unlock()

	g.counter -= 1
	g.err = multierr.Append(g.err, err)
	if g.counter == 0 {
		g.doneCh <- struct{}{}
		close(g.doneCh)
	}
}

func (g *taskGroup) Finished() bool {
	return g.counter == 0
}

func (g *taskGroup) Wait() error {
	<-g.doneCh

	// after all tasks have been finished, nothing will modify the g.err, so return the value without lock
	return g.err
}
