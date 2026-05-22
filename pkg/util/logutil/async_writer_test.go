// Copyright 2026 PingCAP, Inc.
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

package logutil

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type recordingAsyncWriter struct {
	mu sync.Mutex

	writes []string
	events []string

	writeErr error

	writeStarted     chan struct{}
	writeStartedOnce sync.Once
	releaseWrite     chan struct{}
}

func (w *recordingAsyncWriter) Write(p []byte) (int, error) {
	if w.writeStarted != nil {
		w.writeStartedOnce.Do(func() {
			close(w.writeStarted)
		})
	}
	if w.releaseWrite != nil {
		<-w.releaseWrite
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	w.writes = append(w.writes, string(p))
	w.events = append(w.events, "write:"+string(p))
	if w.writeErr != nil {
		return 0, w.writeErr
	}
	return len(p), nil
}

func (w *recordingAsyncWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.events = append(w.events, "close")
	return nil
}

func (w *recordingAsyncWriter) Rotate() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.events = append(w.events, "rotate")
	return nil
}

func (w *recordingAsyncWriter) recordedWrites() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.writes...)
}

func (w *recordingAsyncWriter) recordedEvents() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.events...)
}

func waitAsyncWriterTest(t *testing.T, f func() bool) {
	t.Helper()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	require.True(t, f())
}

func TestAsyncDropWriterCopiesBeforeEnqueue(t *testing.T) {
	writer := &recordingAsyncWriter{}
	asyncWriter := NewAsyncDropWriter(writer, AsyncDropWriterConfig{
		QueueSize:      1,
		EnqueueTimeout: time.Second,
		CloseTimeout:   time.Second,
	})

	payload := []byte("audit-entry")
	n, err := asyncWriter.Write(payload)
	require.NoError(t, err)
	require.Equal(t, len(payload), n)

	for i := range payload {
		payload[i] = 'x'
	}

	waitAsyncWriterTest(t, func() bool {
		return len(writer.recordedWrites()) == 1
	})
	require.Equal(t, []string{"audit-entry"}, writer.recordedWrites())
	require.NoError(t, asyncWriter.Close())
}

func TestAsyncDropWriterWaitsBrieflyThenDropsWhenQueueIsFull(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	drops := make(chan AsyncDropWriterDrop, 1)
	writer := &recordingAsyncWriter{
		writeStarted: started,
		releaseWrite: release,
	}
	asyncWriter := NewAsyncDropWriter(writer, AsyncDropWriterConfig{
		QueueSize:      1,
		EnqueueTimeout: 20 * time.Millisecond,
		CloseTimeout:   time.Second,
		OnDrop: func(drop AsyncDropWriterDrop) {
			drops <- drop
		},
	})

	_, err := asyncWriter.Write([]byte("blocked"))
	require.NoError(t, err)
	<-started

	_, err = asyncWriter.Write([]byte("queued"))
	require.NoError(t, err)

	start := time.Now()
	n, err := asyncWriter.Write([]byte("dropped"))
	require.NoError(t, err)
	require.Equal(t, len("dropped"), n)
	require.GreaterOrEqual(t, time.Since(start), 10*time.Millisecond)

	select {
	case drop := <-drops:
		require.Equal(t, AsyncDropReasonQueueFull, drop.Reason)
		require.Equal(t, len("dropped"), drop.Bytes)
	case <-time.After(time.Second):
		t.Fatal("expected one dropped log entry")
	}

	close(release)
	require.NoError(t, asyncWriter.Close())
	require.Equal(t, []string{"blocked", "queued"}, writer.recordedWrites())
}

func TestAsyncDropWriterSerializesRotateWithWrites(t *testing.T) {
	writer := &recordingAsyncWriter{}
	asyncWriter := NewAsyncDropWriter(writer, AsyncDropWriterConfig{
		QueueSize:      4,
		EnqueueTimeout: time.Second,
		CloseTimeout:   time.Second,
	})

	_, err := asyncWriter.Write([]byte("before"))
	require.NoError(t, err)
	require.NoError(t, asyncWriter.Rotate())
	_, err = asyncWriter.Write([]byte("after"))
	require.NoError(t, err)
	require.NoError(t, asyncWriter.Close())

	require.Equal(t, []string{"write:before", "rotate", "write:after", "close"}, writer.recordedEvents())
}

func TestAsyncDropWriterCloseTimesOutWhenInnerWriterStalls(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	writer := &recordingAsyncWriter{
		writeStarted: started,
		releaseWrite: release,
	}
	asyncWriter := NewAsyncDropWriter(writer, AsyncDropWriterConfig{
		QueueSize:      1,
		EnqueueTimeout: time.Second,
		CloseTimeout:   20 * time.Millisecond,
	})

	_, err := asyncWriter.Write([]byte("blocked"))
	require.NoError(t, err)
	<-started

	err = asyncWriter.Close()
	require.ErrorIs(t, err, ErrAsyncDropWriterTimeout)

	close(release)
	waitAsyncWriterTest(t, func() bool {
		return len(writer.recordedEvents()) == 2
	})
	require.Equal(t, []string{"write:blocked", "close"}, writer.recordedEvents())
}

func TestAsyncDropWriterReportsInnerWriteErrors(t *testing.T) {
	writeErr := errors.New("disk write failed")
	errorsCh := make(chan struct {
		op  string
		err error
	}, 1)
	writer := &recordingAsyncWriter{writeErr: writeErr}
	asyncWriter := NewAsyncDropWriter(writer, AsyncDropWriterConfig{
		QueueSize:      1,
		EnqueueTimeout: time.Second,
		CloseTimeout:   time.Second,
		OnError: func(op string, err error) {
			errorsCh <- struct {
				op  string
				err error
			}{op: op, err: err}
		},
	})

	n, err := asyncWriter.Write([]byte("entry"))
	require.NoError(t, err)
	require.Equal(t, len("entry"), n)

	select {
	case reported := <-errorsCh:
		require.Equal(t, "write", reported.op)
		require.ErrorIs(t, reported.err, writeErr)
	case <-time.After(time.Second):
		t.Fatal("expected write error to be reported")
	}
	require.NoError(t, asyncWriter.Close())
}
