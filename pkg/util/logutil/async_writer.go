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
	"io"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

const (
	// DefaultAsyncDropWriterQueueSize is the default buffered queue size.
	DefaultAsyncDropWriterQueueSize = 4096
	// DefaultAsyncDropWriterEnqueueTimeout is the default max time to wait when the queue is full.
	DefaultAsyncDropWriterEnqueueTimeout = 10 * time.Millisecond
	// DefaultAsyncDropWriterCloseTimeout is the default max time to wait for close/rotate control ops.
	DefaultAsyncDropWriterCloseTimeout = time.Second
	// DefaultAsyncDropWriterMaxPooledBufferSize is the largest copied record buffer retained in the pool.
	DefaultAsyncDropWriterMaxPooledBufferSize = 64 * 1024
)

var (
	// ErrAsyncDropWriterTimeout is returned when a control op cannot finish before the configured timeout.
	ErrAsyncDropWriterTimeout = errors.New("timeout waiting for async drop writer")
	// ErrAsyncDropWriterUnsupportedRotate is returned when Rotate is called on a writer that does not support it.
	ErrAsyncDropWriterUnsupportedRotate = errors.New("async drop writer inner writer does not support rotate")
)

// AsyncDropReason identifies why an async writer dropped a log entry.
type AsyncDropReason string

// AsyncDropReasonQueueFull means the queue stayed full after the enqueue timeout.
const AsyncDropReasonQueueFull AsyncDropReason = "queue_full"

// AsyncDropWriterDrop describes one dropped log entry.
type AsyncDropWriterDrop struct {
	Reason AsyncDropReason
	Bytes  int
}

// AsyncDropWriterConfig configures an AsyncDropWriter.
type AsyncDropWriterConfig struct {
	QueueSize           int
	EnqueueTimeout      time.Duration
	CloseTimeout        time.Duration
	MaxPooledBufferSize int
	OnDrop              func(AsyncDropWriterDrop)
	OnError             func(op string, err error)
}

// DefaultAsyncDropWriterConfig returns the default config for a generic async drop writer.
func DefaultAsyncDropWriterConfig() AsyncDropWriterConfig {
	sampledLogger := SampleLoggerFactory(30*time.Second, 1)
	return AsyncDropWriterConfig{
		QueueSize:           DefaultAsyncDropWriterQueueSize,
		EnqueueTimeout:      DefaultAsyncDropWriterEnqueueTimeout,
		CloseTimeout:        DefaultAsyncDropWriterCloseTimeout,
		MaxPooledBufferSize: DefaultAsyncDropWriterMaxPooledBufferSize,
		OnDrop: func(drop AsyncDropWriterDrop) {
			sampledLogger().Warn("async log entry dropped",
				zap.String("reason", string(drop.Reason)),
				zap.Int("bytes", drop.Bytes),
			)
		},
		OnError: func(op string, err error) {
			sampledLogger().Error("async log writer error",
				zap.String("op", op),
				zap.Error(err),
			)
		},
	}
}

type asyncDropWriterOpType int

const (
	asyncDropWriterOpWrite asyncDropWriterOpType = iota
	asyncDropWriterOpRotate
	asyncDropWriterOpClose
)

type asyncDropWriterOp struct {
	tp    asyncDropWriterOpType
	buf   []byte
	reply chan error
}

// AsyncDropWriter writes log bytes through a bounded background queue.
type AsyncDropWriter struct {
	inner io.Writer
	cfg   AsyncDropWriterConfig
	ch    chan asyncDropWriterOp
	done  chan struct{}

	bufferPool sync.Pool
	closeOnce  sync.Once
	closed     atomic.Bool
	stateMu    sync.RWMutex
}

// NewAsyncDropWriter creates a fail-open async writer.
func NewAsyncDropWriter(inner io.Writer, cfg AsyncDropWriterConfig) *AsyncDropWriter {
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = DefaultAsyncDropWriterQueueSize
	}
	if cfg.EnqueueTimeout < 0 {
		cfg.EnqueueTimeout = 0
	}
	if cfg.CloseTimeout <= 0 {
		cfg.CloseTimeout = DefaultAsyncDropWriterCloseTimeout
	}
	if cfg.MaxPooledBufferSize <= 0 {
		cfg.MaxPooledBufferSize = DefaultAsyncDropWriterMaxPooledBufferSize
	}

	w := &AsyncDropWriter{
		inner: inner,
		cfg:   cfg,
		ch:    make(chan asyncDropWriterOp, cfg.QueueSize),
		done:  make(chan struct{}),
	}
	go w.run()
	return w
}

// Inner returns the wrapped writer.
func (w *AsyncDropWriter) Inner() io.Writer {
	return w.inner
}

// Write copies p, enqueues it, and returns fail-open success to the caller.
func (w *AsyncDropWriter) Write(p []byte) (int, error) {
	if w.closed.Load() {
		return len(p), nil
	}

	buf := w.copyBuffer(p)
	op := asyncDropWriterOp{
		tp:  asyncDropWriterOpWrite,
		buf: buf,
	}

	w.stateMu.RLock()
	if w.closed.Load() {
		w.stateMu.RUnlock()
		w.releaseBuffer(buf)
		return len(p), nil
	}
	enqueued := w.enqueueWrite(op)
	w.stateMu.RUnlock()

	if !enqueued {
		w.releaseBuffer(buf)
		w.reportDrop(AsyncDropWriterDrop{
			Reason: AsyncDropReasonQueueFull,
			Bytes:  len(p),
		})
	}
	return len(p), nil
}

// Rotate forwards a rotate request to the background writer when the inner writer supports it.
func (w *AsyncDropWriter) Rotate() error {
	w.stateMu.Lock()
	defer w.stateMu.Unlock()
	if w.closed.Load() {
		return io.ErrClosedPipe
	}
	return w.controlLocked(asyncDropWriterOpRotate)
}

// Close drains queued writes and closes the inner writer when it supports Close.
func (w *AsyncDropWriter) Close() error {
	w.stateMu.Lock()
	defer w.stateMu.Unlock()
	if w.closed.Swap(true) {
		return nil
	}

	err := w.controlLocked(asyncDropWriterOpClose)
	if errors.Is(err, ErrAsyncDropWriterTimeout) {
		w.closeOnce.Do(func() {
			close(w.ch)
		})
	}
	return err
}

func (w *AsyncDropWriter) enqueueWrite(op asyncDropWriterOp) bool {
	if w.cfg.EnqueueTimeout == 0 {
		select {
		case w.ch <- op:
			return true
		default:
			return false
		}
	}

	timer := time.NewTimer(w.cfg.EnqueueTimeout)
	defer timer.Stop()
	select {
	case w.ch <- op:
		return true
	case <-timer.C:
		return false
	}
}

func (w *AsyncDropWriter) controlLocked(tp asyncDropWriterOpType) error {
	reply := make(chan error, 1)
	op := asyncDropWriterOp{
		tp:    tp,
		reply: reply,
	}

	timer := time.NewTimer(w.cfg.CloseTimeout)
	defer timer.Stop()
	select {
	case w.ch <- op:
	case <-timer.C:
		return ErrAsyncDropWriterTimeout
	}

	select {
	case err := <-reply:
		return err
	case <-timer.C:
		return ErrAsyncDropWriterTimeout
	}
}

func (w *AsyncDropWriter) run() {
	defer close(w.done)
	for op := range w.ch {
		switch op.tp {
		case asyncDropWriterOpWrite:
			w.write(op.buf)
		case asyncDropWriterOpRotate:
			op.reply <- w.reportControlError("rotate", w.rotateInner())
		case asyncDropWriterOpClose:
			op.reply <- w.reportControlError("close", w.closeInner())
			return
		}
	}
	w.reportControlError("close", w.closeInner())
}

func (w *AsyncDropWriter) write(buf []byte) {
	n, err := w.inner.Write(buf)
	if err == nil && n != len(buf) {
		err = io.ErrShortWrite
	}
	if err != nil {
		w.reportError("write", err)
	}
	w.releaseBuffer(buf)
}

func (w *AsyncDropWriter) rotateInner() error {
	rotator, ok := w.inner.(interface {
		Rotate() error
	})
	if !ok {
		return ErrAsyncDropWriterUnsupportedRotate
	}
	return rotator.Rotate()
}

func (w *AsyncDropWriter) closeInner() error {
	closer, ok := w.inner.(io.Closer)
	if !ok {
		return nil
	}
	return closer.Close()
}

func (w *AsyncDropWriter) reportControlError(op string, err error) error {
	if err != nil {
		w.reportError(op, err)
	}
	return err
}

func (w *AsyncDropWriter) reportDrop(drop AsyncDropWriterDrop) {
	if w.cfg.OnDrop != nil {
		w.cfg.OnDrop(drop)
	}
}

func (w *AsyncDropWriter) reportError(op string, err error) {
	if w.cfg.OnError != nil {
		w.cfg.OnError(op, err)
	}
}

func (w *AsyncDropWriter) copyBuffer(p []byte) []byte {
	if len(p) > w.cfg.MaxPooledBufferSize {
		return append([]byte(nil), p...)
	}

	if pooled := w.bufferPool.Get(); pooled != nil {
		buf := pooled.([]byte)
		if cap(buf) >= len(p) {
			buf = buf[:len(p)]
			copy(buf, p)
			return buf
		}
	}

	buf := make([]byte, len(p))
	copy(buf, p)
	return buf
}

func (w *AsyncDropWriter) releaseBuffer(buf []byte) {
	if cap(buf) > w.cfg.MaxPooledBufferSize {
		return
	}
	w.bufferPool.Put(buf[:0])
}
