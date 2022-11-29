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
	"sync"
)

type workerStatus int

const (
	workerStatusCreated workerStatus = iota
	workerStatusRunning
	workerStatusStopping
	workerStatusStopped
)

// Worker is the interface of background running goroutine
type Worker interface {
	Start()
	Stop()
	Status() workerStatus
	Send() chan<- interface{}
	Stopped() <-chan error
}

type baseWorker struct {
	// the `.status` is shared between the owner of this object and the Worker goroutine
	// so the `sync.Mutex` is here to protect it
	sync.Mutex

	ctx    context.Context
	cancel func()

	ch chan interface{}

	loopFunc func() error

	status  workerStatus
	stopped chan error
}

// init should be called by the struct inherits the baseWorker, to set the loop function and initialize channels
func (w *baseWorker) init(loop func() error) {
	w.ctx, w.cancel = context.WithCancel(context.Background())
	w.status = workerStatusCreated
	w.loopFunc = loop
	w.ch = make(chan interface{})
	w.stopped = make(chan error, 1)
}

// Start spawns the goroutine to run the loop specified in `init` function
// it changes the state from "Created" to "Running"
func (w *baseWorker) Start() {
	w.Lock()
	defer w.Unlock()
	if w.status != workerStatusCreated {
		return
	}

	go w.loop()
	w.status = workerStatusRunning
}

// Stop uses the cancel (corresponding with the context) to stop the running loop
// it changes the state from "Created" or "Running" to "Stopping"
func (w *baseWorker) Stop() {
	w.Lock()
	defer w.Unlock()
	switch w.status {
	case workerStatusCreated:
		w.cancel()
		w.toStopped(nil)
	case workerStatusRunning:
		w.cancel()
		w.status = workerStatusStopping
	}
}

// Status returns the current status of the Worker
func (w *baseWorker) Status() workerStatus {
	w.Lock()
	defer w.Unlock()
	return w.status
}

// Send returns the work dispatch channel for this Worker
func (w *baseWorker) Send() chan<- interface{} {
	return w.ch
}

func (w *baseWorker) loop() {
	err := w.loopFunc()

	w.Lock()
	defer w.Unlock()
	w.toStopped(err)
}

func (w *baseWorker) toStopped(err error) {
	w.status = workerStatusStopped
	w.stopped <- err

	close(w.ch)
	close(w.stopped)
}

// Stopped returns the channel used to wait until the Worker stop and get the error message
func (w *baseWorker) Stopped() <-chan error {
	return w.stopped
}
