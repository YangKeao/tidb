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

import "github.com/pingcap/tidb/ttl/session"

type delWorker struct {
	baseWorker
	sessPool   session.SessionPool
	taskNotify <-chan *delTask
}

func newDelWorker(taskNotify <-chan *delTask, sessPool session.SessionPool) Worker {
	w := &delWorker{
		taskNotify: taskNotify,
		sessPool:   sessPool,
	}
	w.init(w.delLoop)
	return w
}

func (w *delWorker) delLoop() error {
	for {
		select {
		case <-w.ctx.Done():
			return nil
		}
	}
}

type delTask struct {
}
